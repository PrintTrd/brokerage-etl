# Brokerage ETL Pipeline

A containerised data pipeline that ingests daily brokerage CSV drops, applies
data quality rules, and loads clean data into PostgreSQL ready for analytics
and reporting.

**Highlights:**
- ⚡ **Polars** for blazingly fast, vectorized data cleaning.
- 🐘 **SQLAlchemy** for idempotent, upsert-based database loading.
- 🐳 **Docker Compose** for seamless, out-of-the-box orchestration.

## Project Structure

```
brokerage-etl/
├── data/
│   └── input/                    # Drop CSV files here
│       ├── clients.csv
│       ├── instruments.csv
│       ├── trades_2026-03-09.csv
│       └── trades_YYYY-MM-DD.csv # CSV Pattern (globbed by ETL)
├── src/
│   ├── config.py
│   ├── database.py
│   └── etl.py                    # Full ETL pipeline
├── .env.example                  # start script will copy to create env file
├── docker-compose.yml
├── Dockerfile
├── README.md
├── requirements.txt
├── scheduler.sh                  # For Periodic (Automatic) Execution
├── start_mac.sh                  # Set env for Mac/Linux
└── start_window.bat              # Set env for Windows
```


## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- No local Python installation required

## How to Start Everything
For ease of review, this project runs **out-of-the-box** with safe default credentials. No configuration required! See [Configuration & Security](#configuration--security) for details.

1. Clone and enter the repository
```bash
git clone https://github.com/PrintTrd/brokerage-etl.git
cd brokerage-etl
```

2. Configure environment

For **Mac / Linux**: run
```bash
chmod +x start_mac.sh
./start_mac.sh
```
For **Window**: double click `start_window.bat` in the folder or run
```powershell
  .\start_window.bat
```
>(Optional) Edit .env with your desired PostgreSQL and pgAdmin passwords

3. Start Docker

**Mac / Windows**: Open Docker Desktop and wait for the engine to show a green "Running" status

**Linux** (Docker Engine): Ensure the Docker daemon is active (e.g., sudo systemctl start docker)

4. Build the ETL image and start the database and pgadmin
```bash
docker compose up -d db pgadmin
```
This starts:
- **PostgreSQL** – stores cleaned data
- **pgAdmin** – optional UI to query the database
>pgAdmin UI at http://localhost:5050

5. Wait for Postgres to be healthy (~5 s), then run the ETL once
```bash
docker compose run --build --rm etl
```
- `--build`: ensures the container uses the latest version of the code (optional)
- `--rm`: cleans up the container after the job finishes

6. Connect to Postgres, then can run queries
```bash
docker compose exec db psql -U myuser -d brokerage_data -c "SELECT trade_id, rejection_reason FROM rejected_trades LIMIT 5;"
```

## How to Trigger a Run

### On-Demand Execution

Drop a new trade file (e.g. `data/input/trades_2026-XX-XX.csv`) then run:

```bash
docker compose run --build --rm etl
```

The pipeline is **idempotent** — re-running with the same file produces the same result (ON CONFLICT DO UPDATE ensures no duplicates).

### Periodic (Automatic) Execution

The scheduler service automatically runs the ETL every 2 minutes:
>To change the schedule, edit `docker-compose.yml` and modify the cron expression (`*/2 * * * *`) to your needs.

```bash
docker compose up -d scheduler
# Monitor the Scheduler (watch it trigger every 2 minutes)
docker compose logs -f scheduler
```

## How to Confirm Results

### Verify Results

```bash
# Connect to Postgres and run queries
docker compose exec db psql -U myuser -d brokerage_data

# Or query directly without interactive shell
docker compose exec db psql -U myuser -d brokerage_data -c "SELECT COUNT(*) FROM clients;"
```
Or see in pgAdmin

### Useful verification queries

```sql
-- Row counts per table
SELECT 'clients' AS tbl,  COUNT(*) FROM clients
UNION ALL
SELECT 'instruments',     COUNT(*) FROM instruments
UNION ALL
SELECT 'trades',          COUNT(*) FROM trades
UNION ALL
SELECT 'rejected_trades', COUNT(*) FROM rejected_trades;

       tbl       | count 
-----------------+-------
 clients         |    15
 instruments     |    20
 trades          |    28
 rejected_trades |    20
(4 rows)

-- Rejected rows with reasons (data governance audit)
SELECT trade_id, rejection_reason, source_file, rejected_at
FROM   rejected_trades
ORDER  BY rejected_at DESC;

Example output:
 trade_id |          rejection_reason           |      source_file      |          rejected_at
----------+-------------------------------------+-----------------------+-------------------------------
 T0003    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0004    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0005    | invalid_quantity; unknown_client_id | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0006    | invalid_side                        | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0007    | invalid_price; unknown_client_id    | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0012    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0014    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0017    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0022    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0023    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0025    | unknown_instrument_id               | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0026    | invalid_quantity; unknown_client_id | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0028    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0030    | invalid_price                       | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0031    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0036    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0037    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0041    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0043    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
 T0046    | unknown_client_id                   | trades_2026-03-09.csv | 2026-04-20 02:14:34.561563+00
(20 rows)

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
> Data Masking Awareness — `client_name` are treated as sensitive information requiring careful handling in actual production deployments.

**Environment Isolation** — PostgreSQL and the application are isolated via Docker Compose network. The database is reachable only from within the container network (using the `db` hostname), preventing accidental public exposure. External access is strictly controlled via mapped ports.

### Data Pipeline Robustness

**Connection Retry Logic** — The `get_engine()` function implements retry logic (up to 5 attempts with 3-second delays) to handle the race condition where the application container starts before PostgreSQL is fully initialized. This eliminates the need for external `wait-for-it` scripts while gracefully handling temporary connection failures.

**Validation & Data Integrity** —
- **Data Consistency**: Trades are normalized (whitespace stripped, case uppercased, numeric formats standardized) to ensure consistency across runs.
- **Referential Integrity**: Invalid foreign key references (`client_id`, `instrument_id`) are rejected before insertion, ensuring clean, queryable datasets.
- **Audit Trail**: Rejected rows with detailed rejection reasons are stored in `rejected_trades` table, providing governance and debugging capabilities.

**Idempotent Runs** — Both master data and trades use Upsert `INSERT … ON CONFLICT DO UPDATE` semantics rather than `to_sql(if_exists='append')`. Re-running the ETL with the same input files produces identical final state, making the pipeline safe for periodic execution and recovery from failures.

### Scalability & Future-proofing

**Incremental Processing** — The pipeline uses glob patterns (`trades_*.csv`) to automatically discover and process new trade files added to `data/input/` without code changes. Combined with the scheduler, this enables seamless horizontal scaling of data ingestion.

**Late-update Semantics** — When duplicate `trade_id` values appear (e.g., amended trades), the pipeline retains the record with the latest `trade_time`, silently discarding older versions. I consider this a legitimate workflow (Trade Amendment) rather than a data quality error, hence it is NOT logged in the `rejected_trades` audit table.

**Master Data Precedence** — `clients` and `instruments` are loaded before trades, ensuring foreign key validation works correctly even on a cold database. This design supports various initialization sequences and partial restarts.

**Pluggable Scheduling** — The current implementation uses Alpine cron + Docker socket. This is lightweight but requires socket mounting. For larger deployments, this can be replaced with:
- Apache Airflow (workflow orchestration)
- Kubernetes CronJobs (Kubernetes-native scheduling)
- AWS Lambda or Google Cloud Functions (serverless)
- Host-level cron with appropriate permissions

**Nullable Fees** — Some trades arrive without fees (e.g., CANCELLED status). The schema keeps `fees` nullable; downstream queries should use `COALESCE(fees, 0)` for aggregations.

---
