# Brokerage ETL Pipeline

A containerised data pipeline that ingests daily brokerage CSV drops, applies
data quality rules, and loads clean data into PostgreSQL ready for analytics
and reporting.



## Project Structure

```
brokerage-etl/
├── data/
│   └── input/                      # Drop CSV files here
│       ├── clients.csv
│       ├── instruments.csv
│       ├── trades_2026-03-09.csv
│       ├── trades_2026-04-02.csv   # Include these files for test
│       └── trades_2026-XX-XX.csv   # Pattern: trades_YYYY-MM-DD.csv (globbed by ETL)
├── src/
│   ├── config.py                   # Configuration
│   ├── database.py
│   └── etl.py                      # Full ETL pipeline
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```


## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- No local Python installation required


## How to Start Everything
For ease of review, this project runs **out-of-the-box** with safe default credentials. No configuration required! See [Configuration & Security](#configuration--security) for details.

```bash
# 1. Clone and enter the repository
git clone https://github.com/PrintTrd/brokerage-etl.git
cd brokerage-etl

# 2. (Optional) Configure environment
cp .env.example .env
# Edit .env with your desired PostgreSQL and pgAdmin passwords

# 3. Start Docker
# Mac/Windows: Open Docker Desktop and wait for the engine to show a green "Running" status
# Linux (Docker Engine): Ensure the Docker daemon is active (e.g., sudo systemctl start docker)

# 4. Build the ETL image and start the database
docker compose up -d db

# 5. Wait for Postgres to be healthy (usually ~5 s), then run the ETL once
docker compose run --rm etl

# 6. Connect to Postgres, then can run queries
docker compose exec db psql -U myuser -d brokerage_data

# 7. (Optional) Start the periodic scheduler and pgAdmin
docker compose up -d scheduler pgadmin
```
This starts:
- **PostgreSQL** (port 5432) – stores cleaned data
- **Scheduler** – runs ETL every 5 minutes
- **pgAdmin** (port 5050) – optional UI to query the database

## How to Trigger a Run

### On-Demand Execution

Drop a new trade file (e.g. `data/input/trades_2026-XX-XX.csv`) then run:

```bash
docker compose run --rm etl
```

The pipeline is **idempotent** — re-running with the same file produces the same result (ON CONFLICT DO UPDATE ensures no duplicates).

### Periodic (Automatic) Execution

The scheduler service automatically runs the ETL every 5 minutes:


```bash
# Already started with `docker compose up -d`
# Monitor it with:
docker compose logs -f scheduler
```

To change the schedule, edit `docker-compose.yml` and modify the cron expression (`*/5 * * * *`) to your needs.


## How to Confirm Results

### Verify Results

```bash
# Check scheduler logs
docker compose logs scheduler

# Connect to Postgres and run queries
docker compose exec db psql -U myuser -d brokerage_data

# Or query directly without interactive shell
docker compose exec db psql -U myuser -d brokerage_data -c "SELECT COUNT(*) FROM clients;"

# Or open pgAdmin UI at http://localhost:5050 (admin@local.dev / admin_local_dev)
```

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
### Cleanup

```bash
# Stop all services and remove volumes
docker compose down -v
```

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

### Configuration & Security

**Dynamic Settings via Environment Variables** — The application uses a centralized `Settings` class

**Security Trade-off (Ease vs. Safety)** — This project prioritizes reviewer convenience by including hardcoded default credentials (`dev_password_local_only` for PostgreSQL, `admin_local_dev` for pgAdmin). This enables immediate `docker compose up` execution without pre-configuration steps. However, in a production environment, credentials must be managed via dedicated Secret Management Tools. These should never be hardcoded or committed to version control.

**Environment Isolation** — PostgreSQL and the application are isolated via Docker Compose network. The database is reachable only from within the container network (using the `db` hostname), preventing accidental public exposure. External access is strictly controlled via mapped ports.

### Data Pipeline Robustness

**Connection Retry Logic** — The `get_engine()` function implements retry logic (up to 10 attempts with 3-second delays) to handle the race condition where the application container starts before PostgreSQL is fully initialized. This eliminates the need for external `wait-for-it` scripts while gracefully handling temporary connection failures.

**Validation & Data Integrity** —
- **Data Consistency**: Trades are normalized (whitespace stripped, case uppercased, numeric formats standardized) to ensure consistency across runs.
- **Referential Integrity**: Invalid foreign key references (`client_id`, `instrument_id`) are rejected before insertion, ensuring clean, queryable datasets.
- **Audit Trail**: Rejected rows with detailed rejection reasons are stored in `rejected_trades` table, providing governance and debugging capabilities.

**Idempotent Runs** — Both master data and trades use Upsert `INSERT … ON CONFLICT DO UPDATE` semantics rather than `to_sql(if_exists='append')`. Re-running the ETL with the same input files produces identical final state, making the pipeline safe for periodic execution and recovery from failures.

### Scalability & Future-proofing

**Incremental Processing** — The pipeline uses glob patterns (`trades_*.csv`) to automatically discover and process new trade files added to `data/input/` without code changes. Combined with the scheduler, this enables seamless horizontal scaling of data ingestion.

**Late-update Semantics** — When duplicate `trade_id` values appear (e.g., amended trades), the record with the latest `trade_time` is retained, silently discarding older versions. This legitimate amendment pattern is not flagged as rejection, supporting real-world trading workflows.

**Master Data Precedence** — `clients` and `instruments` are loaded before trades, ensuring foreign key validation works correctly even on a cold database. This design supports various initialization sequences and partial restarts.

**Pluggable Scheduling** — The current implementation uses Alpine cron + Docker socket. This is lightweight but requires socket mounting. For larger deployments, this can be replaced with:
- Apache Airflow (workflow orchestration)
- Kubernetes CronJobs (Kubernetes-native scheduling)
- AWS Lambda or Google Cloud Functions (serverless)
- Host-level cron with appropriate permissions

**Nullable Fees** — Some trades arrive without fees (e.g., CANCELLED status). The schema keeps `fees` nullable; downstream queries should use `COALESCE(fees, 0)` for aggregations.

**Data Masking Awareness** — `client_name` are treated as sensitive information requiring careful handling in actual production deployments. The pipeline is designed to support future data masking or PII anonymization without structural changes.

---
