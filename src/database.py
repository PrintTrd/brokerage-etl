import time
import logging
from sqlalchemy import create_engine, text
from .config import settings

logger = logging.getLogger(__name__)


def get_engine():
    """
    Return a SQLAlchemy engine, retrying on connection failure.
    Handles the Docker startup race between the app and the DB containers.
    """
    last_exc = None
    for attempt in range(1, settings.MAX_RETRIES + 1):
        try:
            # pool_pre_ping = True makes SQLAlchemy check connections before using them
            engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
            with engine.connect() as conn:
                # Test the connection
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established.")
            return engine
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "DB not ready (attempt %d/%d): %s — retrying in %ds…",
                attempt,
                settings.MAX_RETRIES,
                exc,
                settings.RETRY_DELAY,
            )
            time.sleep(settings.RETRY_DELAY)

    raise RuntimeError(
        f"Could not connect to the database after {settings.MAX_RETRIES} attempts"
    ) from last_exc


def create_schema(engine) -> None:
    """Create tables if they do not already exist (idempotent)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS clients (
        client_id   VARCHAR(20)  PRIMARY KEY,
        client_name VARCHAR(120),
        country     VARCHAR(10),
        kyc_status  VARCHAR(20),
        created_at  DATE
    );

    CREATE TABLE IF NOT EXISTS instruments (
        instrument_id VARCHAR(20) PRIMARY KEY,
        symbol        VARCHAR(20),
        asset_class   VARCHAR(30),
        currency      VARCHAR(10),
        exchange      VARCHAR(30)
    );

    CREATE TABLE IF NOT EXISTS trades (
        trade_id      VARCHAR(20)  PRIMARY KEY,
        trade_time    TIMESTAMPTZ,
        client_id     VARCHAR(20)  REFERENCES clients(client_id),
        instrument_id VARCHAR(20)  REFERENCES instruments(instrument_id),
        side          VARCHAR(4),
        quantity      NUMERIC(20, 8),
        price         NUMERIC(20, 8),
        fees          NUMERIC(20, 8),
        status        VARCHAR(20),
        source_file   VARCHAR(120),
        loaded_at     TIMESTAMPTZ  DEFAULT NOW()
    );

    -- Audit / governance table for rows that failed validation
    CREATE TABLE IF NOT EXISTS rejected_trades (
        id               SERIAL PRIMARY KEY,
        trade_id         VARCHAR(20),
        rejection_reason VARCHAR(200),
        raw_data         JSONB,
        source_file      VARCHAR(120),
        rejected_at      TIMESTAMPTZ  DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Schema ready.")
