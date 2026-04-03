"""
Brokerage Data Pipeline  (Polars + SQLAlchemy)
Cleans and loads clients, instruments, and daily trade files into PostgreSQL.

Cleaning steps applied to trades:
  1. Whitespace stripping + case normalisation
  2. Numeric parsing  (comma-separated strings → Float64)
  3. Side validation  (must be BUY | SELL)
  4. Price / quantity validation  (must be > 0 and not null)
  5. Referential integrity  (client_id and instrument_id must exist in master)
  6. Deduplication  (last record per trade_id wins — "late update" semantics)
  7. Rejected rows are written to the `rejected_trades` audit table
"""

import glob
import json
import logging
import sys
from pathlib import Path

import polars as pl
from sqlalchemy import text

from .database import get_engine, create_schema
from .config import settings

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── Cleaning helpers ───────────────────────────────────────────────────────────
def strip_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Strip leading/trailing whitespace from every String column."""
    str_cols = [c for c, t in zip(df.columns, df.dtypes) if t == pl.String]
    return df.with_columns([pl.col(c).str.strip_chars() for c in str_cols])


def parse_numeric_col(col: str) -> pl.Expr:
    """
    Remove comma thousand-separators then cast to Float64.
    Unparseable values (including bare 'NaN' strings) become null.
    e.g. "1,950" -> 1950.0,  "NaN" -> null
    """
    return (
        pl.col(col).str.replace_all(",", "").cast(pl.Float64, strict=False).alias(col)
    )


def parse_timestamp_col(col: str) -> pl.Expr:
    """
    Parse ISO-8601 timestamps that end with a literal 'Z'.
    e.g. "2026-03-09T09:15:00Z" -> Datetime[us, UTC]
    """
    return (
        pl.col(col)
        .str.strip_chars_end("Z")
        .str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S",
            strict=False,
            time_unit="us",
        )
        .dt.replace_time_zone("UTC")
        .alias(col)
    )


# ── Per-dataset cleaners ───────────────────────────────────────────────────────
def clean_clients(df: pl.DataFrame) -> pl.DataFrame:
    df = strip_strings(df)
    df = df.with_columns(
        [
            pl.col("client_id").str.to_uppercase(),
            pl.col("kyc_status").str.to_uppercase(),
            pl.col("country").str.to_uppercase(),
        ]
    )
    # Keep last record if the same client_id appears more than once
    df = df.unique(subset=["client_id"], keep="last")
    log.info("Clients clean: %d rows", len(df))
    return df


def clean_instruments(df: pl.DataFrame) -> pl.DataFrame:
    df = strip_strings(df)
    df = df.with_columns(
        [
            pl.col("instrument_id").str.to_uppercase(),
            pl.col("asset_class").str.to_uppercase(),
            pl.col("currency").str.to_uppercase(),
        ]
    )
    # Keep last record if the same instrument_id appears more than once
    df = df.unique(subset=["instrument_id"], keep="last")
    log.info("Instruments clean: %d rows", len(df))
    return df


def clean_trades(
    df: pl.DataFrame,
    valid_client_ids: set,
    valid_instrument_ids: set,
    source_file: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Clean and validate the trades DataFrame.

    Returns:
        (clean_df, rejected_df)
        rejected_df holds raw rejected rows plus a rejection_reason string.
    """
    raw = df  # keep untouched original for the audit trail

    # ── 1. Normalise strings ───────────────────────────────────────────────
    df = strip_strings(df)
    df = df.with_columns(
        [
            pl.col("trade_id").str.to_uppercase(),
            pl.col("side").str.to_uppercase(),
            pl.col("client_id").str.to_uppercase(),
            pl.col("instrument_id").str.to_uppercase(),
            pl.col("status").str.to_uppercase(),
        ]
    )

    # ── 2. Parse numerics (comma-safe) ────────────────────────────────────
    df = df.with_columns(
        [
            parse_numeric_col("price"),
            parse_numeric_col("quantity"),
            parse_numeric_col("fees"),
        ]
    )

    # ── 3. Parse timestamps ────────────────────────────────────────────────
    df = df.with_columns(parse_timestamp_col("trade_time"))

    # ── 4. Tag source file ─────────────────────────────────────────────────
    df = df.with_columns(pl.lit(source_file).alias("source_file"))

    # ── 5. Build rejection-reason columns (one per rule) ──────────────────
    #
    # Each rule adds a nullable String column: value = reason text | null.
    # Rows with at least one non-null reason column are rejected.
    # concat_str(..., ignore_nulls=True) joins all fired reasons into one
    # human-readable string ("invalid_side; unknown_client_id", etc.)

    _REASON_COLS = ["_r_side", "_r_price", "_r_qty", "_r_client", "_r_instr"]

    df = df.with_columns(
        [
            pl.when(~pl.col("side").is_in(settings.VALID_SIDES))
            .then(pl.lit("invalid_side"))
            .otherwise(None)
            .alias("_r_side"),
            pl.when(pl.col("price").is_null() | (pl.col("price") <= 0))
            .then(pl.lit("invalid_price"))
            .otherwise(None)
            .alias("_r_price"),
            pl.when(pl.col("quantity").is_null() | (pl.col("quantity") <= 0))
            .then(pl.lit("invalid_quantity"))
            .otherwise(None)
            .alias("_r_qty"),
            pl.when(~pl.col("client_id").is_in(list(valid_client_ids)))
            .then(pl.lit("unknown_client_id"))
            .otherwise(None)
            .alias("_r_client"),
            pl.when(~pl.col("instrument_id").is_in(list(valid_instrument_ids)))
            .then(pl.lit("unknown_instrument_id"))
            .otherwise(None)
            .alias("_r_instr"),
        ]
    )

    df = df.with_columns(
        pl.concat_str(_REASON_COLS, separator="; ", ignore_nulls=True).alias(
            "_rejection_reason"
        )
    )

    is_rejected = pl.col("_rejection_reason").str.len_chars() > 0

    # ── 6. Build audit rows for rejected records ───────────────────────────
    rejected_meta = df.filter(is_rejected).select(["trade_id", "_rejection_reason"])
    reason_lookup: dict[str, str] = dict(
        zip(
            rejected_meta["trade_id"].to_list(),
            rejected_meta["_rejection_reason"].to_list(),
        )
    )

    # Normalise trade_id in the raw frame so lookup works
    raw_with_tid = raw.with_columns(
        pl.col("trade_id").str.strip_chars().str.to_uppercase().alias("_tid_norm")
    )
    raw_rejected = raw_with_tid.filter(
        pl.col("_tid_norm").is_in(list(reason_lookup.keys()))
    ).drop("_tid_norm")

    audit_rows: list[dict] = []
    for row in raw_rejected.to_dicts():
        tid = str(row.get("trade_id", "")).strip().upper()
        audit_rows.append(
            {
                "trade_id": tid,
                "rejection_reason": reason_lookup.get(tid, "unknown"),
                "raw_data": json.dumps(row, default=str),
                "source_file": source_file,
            }
        )

    rejected_df = (
        pl.DataFrame(audit_rows)
        if audit_rows
        else pl.DataFrame(
            schema={
                "trade_id": pl.String,
                "rejection_reason": pl.String,
                "raw_data": pl.String,
                "source_file": pl.String,
            }
        )
    )

    for row in audit_rows:
        log.warning(
            "REJECTED trade_id=%s  reason=%s",
            row["trade_id"],
            row["rejection_reason"],
        )

    # ── 7. Isolate clean rows ──────────────────────────────────────────────
    clean = df.filter(~is_rejected).drop(_REASON_COLS + ["_rejection_reason"])

    # ── 8. Deduplication — last trade_time per trade_id wins ──────────────
    # Duplicate trade_ids can stem from amended updates, keep the latest timestamped record.
    dupe_ids = (
        clean.filter(pl.col("trade_id").is_duplicated())["trade_id"].unique().to_list()
    )
    if dupe_ids:
        log.warning("Duplicate trade_ids (keeping last by trade_time): %s", dupe_ids)

    clean = clean.sort("trade_time").unique(
        subset=["trade_id"], keep="last", maintain_order=False
    )

    log.info(
        "Trades '%s': %d raw -> %d clean, %d rejected",
        source_file,
        len(df),
        len(clean),
        len(rejected_df),
    )
    return clean, rejected_df


# ── Upsert helpers ─────────────────────────────────────────────────────────────
def upsert_master(df: pl.DataFrame, table: str, pk: str, engine) -> None:
    """
    Upsert master/reference data using ON CONFLICT DO UPDATE.
    Safe to run multiple times with the same data (idempotent).
    """
    if df.is_empty():
        return

    cols = df.columns
    update_cols = [c for c in cols if c != pk]
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = text(
        f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk})
        DO UPDATE SET {updates}
    """
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dicts())

    log.info("Upserted %d rows into '%s'.", len(df), table)


def upsert_trades(df: pl.DataFrame, engine) -> None:
    """
    Insert or update trades using ON CONFLICT (trade_id) DO UPDATE.
    Safe to run multiple times with the same file (idempotent).
    """
    if df.is_empty():
        log.info("No clean trades to upsert.")
        return

    cols = df.columns
    update_cols = [c for c in cols if c != "trade_id"]
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = text(
        f"""
        INSERT INTO trades ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (trade_id)
        DO UPDATE SET {updates}, loaded_at = NOW()
    """
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dicts())

    log.info("Upserted %d trades.", len(df))


def insert_rejected(df: pl.DataFrame, engine) -> None:
    if df.is_empty():
        return

    sql = text(
        """
        INSERT INTO rejected_trades (trade_id, rejection_reason, raw_data, source_file)
        VALUES (:trade_id, :rejection_reason, :raw_data, :source_file)
    """
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dicts())

    log.info("Logged %d rejected rows to 'rejected_trades'.", len(df))


def get_processed_files(engine) -> set:
    """Get already processed files by checking distinct source_file values in the trades table."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT source_file FROM trades"))
        return {row[0] for row in result}


# ── Main pipeline ──────────────────────────────────────────────────────────────
def run(engine) -> None:
    # ── Step 1: Schema ────────────────────────────────────────────────────
    create_schema(engine)

    # ── Step 2: Load & clean master data ─────────────────────────────────
    clients_path = settings.INPUT_DIR / "clients.csv"
    instruments_path = settings.INPUT_DIR / "instruments.csv"

    if not clients_path.exists():
        raise FileNotFoundError(f"Missing master file: {clients_path}")
    if not instruments_path.exists():
        raise FileNotFoundError(f"Missing master file: {instruments_path}")

    clients_df = clean_clients(pl.read_csv(clients_path, **settings.CSV_OPTS))
    instruments_df = clean_instruments(
        pl.read_csv(instruments_path, **settings.CSV_OPTS)
    )

    upsert_master(clients_df, "clients", "client_id", engine)
    upsert_master(instruments_df, "instruments", "instrument_id", engine)

    valid_client_ids = set(clients_df["client_id"].to_list())
    valid_instrument_ids = set(instruments_df["instrument_id"].to_list())

    # ── Step 3: Discover and process all trade files ──────────────────────
    trade_files = sorted(glob.glob(str(settings.INPUT_DIR / settings.TRADE_FILE_GLOB)))

    # Filter out already processed files ────────────────────────────
    processed = get_processed_files(engine)
    new_files = [f for f in trade_files if Path(f).name not in processed]

    if not new_files:
        log.info("No new trade files to process.")
        return

    log.info(
        "Found %d new file(s): %s", len(new_files), [Path(f).name for f in new_files]
    )

    for path in new_files:
        source_file = Path(path).name
        log.info("Processing trade file: %s", source_file)
        raw_df = pl.read_csv(path, **settings.CSV_OPTS)
        clean_df, rejected_df = clean_trades(
            raw_df, valid_client_ids, valid_instrument_ids, source_file
        )
        upsert_trades(clean_df, engine)
        insert_rejected(rejected_df, engine)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    engine = get_engine()
    run(engine)
