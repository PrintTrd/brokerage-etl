# Brokerage ETL Pipeline

A containerised data pipeline that ingests daily brokerage CSV drops, applies
data quality rules, and loads clean data into PostgreSQL ready for analytics
and reporting.

---

## Project Structure

```
brokerage-etl/
├── data/
│   └── input/                      # Drop CSV files here
│       ├── clients.csv
│       ├── instruments.csv
│       ├── trades_2026-03-09.csv   # Pattern: trades_YYYY-MM-DD.csv
│       └── trades_2026-04-02.csv
├── src/
│   └── etl.py                      # Full ETL pipeline
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- No local Python installation required

---

## How to Start Everything

```bash
# 1. Clone / copy this repo, then enter the directory
cd brokerage-etl

# 2. Build the ETL image and start the database
docker compose up -d db

# 3. Wait for Postgres to be healthy (usually ~5 s), then run the ETL once
docker compose run --rm etl

# 4. (Optional) Start the periodic scheduler and pgAdmin
docker compose up -d scheduler pgadmin
```

---

## How to Trigger a Run

### On-demand

Drop a new trade file (e.g. `data/input/trades_2026-03-10.csv`) then run:

```bash
docker compose run --rm etl
```

The pipeline is **idempotent** — re-running with the same file produces the
same result (ON CONFLICT DO UPDATE ensures no duplicates).

### Periodic (automatic)

Start the `scheduler` service:

```bash
docker compose up -d scheduler
```

It runs the ETL every 5 minutes via cron. Change the schedule inside
`docker-compose.yml` (`*/5 * * * *`) to any cron expression you need.

---

## How to Confirm Results

Connect to Postgres:

```bash
docker compose exec db psql -U myuser -d brokerage_data
```

Or open pgAdmin at **http://localhost:5050** (For the purpose of this technical assessment, default credentials are provided in the docker-compose and config files to ensure a seamless 'plug-and-play' experience for the reviewer. In a production environment, these would be managed via Secret Management Tools and never committed to version control.)

### Useful verification queries

```sql
-- Row counts per table
SELECT 'clients'        AS tbl, COUNT(*) FROM clients
UNION ALL
SELECT 'instruments',            COUNT(*) FROM instruments
UNION ALL
SELECT 'trades',                 COUNT(*) FROM trades
UNION ALL
SELECT 'rejected_trades',        COUNT(*) FROM rejected_trades;

-- Rejected rows with reasons (data governance audit)
SELECT trade_id, rejection_reason, source_file, rejected_at
FROM   rejected_trades
ORDER  BY rejected_at DESC;

Example output:
 trade_id |   rejection_reason    |      source_file      |          rejected_at
----------+-----------------------+-----------------------+-------------------------------
 T0003    | unknown_client_id     | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0005    | invalid_quantity      | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0006    | invalid_side          | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0007    | invalid_price         | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0025    | unknown_instrument_id | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0026    | invalid_quantity      | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
 T0030    | invalid_price         | trades_2026-03-09.csv | 2026-04-02 11:19:27.885178+00
(7 rows)

-- Clean trades with full client and instrument context
SELECT t.trade_id,
       t.trade_time,
       c.client_name,
       c.kyc_status,
       i.symbol,
       i.asset_class,
       t.side,
       t.quantity,
       t.price,
       ROUND(t.quantity * t.price, 2) AS notional,
       t.fees,
       t.status
FROM   trades      t
JOIN   clients     c ON c.client_id     = t.client_id
JOIN   instruments i ON i.instrument_id = t.instrument_id
ORDER  BY t.trade_time;

-- Total notional traded per asset class
SELECT i.asset_class,
       COUNT(*)                            AS trade_count,
       ROUND(SUM(t.quantity * t.price), 2) AS total_notional
FROM   trades      t
JOIN   instruments i ON i.instrument_id = t.instrument_id
GROUP  BY i.asset_class
ORDER  BY total_notional DESC;

-- BUY vs SELL breakdown per client
SELECT c.client_name,
       t.side,
       COUNT(*)                            AS trades,
       ROUND(SUM(t.quantity * t.price), 2) AS notional
FROM   trades  t
JOIN   clients c ON c.client_id = t.client_id
GROUP  BY c.client_name, t.side
ORDER  BY c.client_name, t.side;
```

---

## Data Cleaning Rules Applied

| Check | Rule | Action on failure |
|---|---|---|
| Whitespace | All string columns stripped | In-place fix |
| Case normalisation | `side`, `client_id`, `instrument_id`, `status` uppercased | In-place fix |
| Numeric parsing | Comma-separated strings (e.g. `"1,950"`) converted to float | In-place fix |
| Side validation | Must be `BUY` or `SELL` | Row rejected |
| Price validation | Must be present and `> 0` | Row rejected |
| Quantity validation | Must be present and `> 0` | Row rejected |
| Client integrity | `client_id` must exist in `clients` master | Row rejected |
| Instrument integrity | `instrument_id` must exist in `instruments` master | Row rejected |
| Deduplication | Duplicate `trade_id` → keep record with latest `trade_time` | Earlier copy dropped |

Rejected rows (with reasons) are written to `rejected_trades` for audit.

---

## Design Decisions & Trade-offs

**Upsert over append** — Both master data and trades use `INSERT … ON CONFLICT
DO UPDATE` rather than `to_sql(if_exists='append')`. This makes every run
fully idempotent: dropping the same file twice is safe and produces the same
final state.

**Late-update semantics for duplicates** — When `trade_id` appears more than
once (e.g. an amended booking arriving after the original), the record with the
latest `trade_time` wins. The earlier copy is silently discarded (not flagged
as rejected, since it is a legitimate amendment pattern).

**Master data loaded first** — `clients` and `instruments` are upserted before
trade processing so foreign-key checks work correctly even on a cold database.

**Rejected rows to a table, not a file** — Storing rejections in
`rejected_trades` keeps the audit trail queryable alongside production data and
survives container restarts.

**Connection retry loop** — `get_engine()` retries up to 10 times with a 3 s
delay. This eliminates the "app starts before DB is ready" race condition in
Docker without relying on `wait-for-it` scripts or sleep hacks.

**Scheduler via Alpine cron + Docker socket** — A lightweight Alpine container
runs `crond` and re-triggers `docker compose run --rm etl` on a schedule. The
tradeoff is that it requires the Docker socket to be mounted. Alternatively,
replace this service with an Airflow DAG, a Kubernetes CronJob, or a simple
host-level cron.

**`fees` may be NULL** — Some trade records arrive without a fees value
(e.g. CANCELLED trades). `fees` is kept nullable in the schema; downstream
aggregations should use `COALESCE(fees, 0)`.

---
**Data Masking Awareness**: In this project, I treated `client_name` as potentially sensitive information. Although these are mock records, the pipeline is designed to handle such fields with care.
