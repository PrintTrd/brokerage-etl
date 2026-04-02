import os
from pathlib import Path


class Settings:
    DB_USER = os.getenv("POSTGRES_USER", "myuser")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "db_pass_2026")
    DB_HOST = os.getenv("POSTGRES_HOST", "db")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "brokerage_data")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # handle Windows/Linux path
    BASE_DIR = Path(__file__).resolve().parent.parent
    INPUT_DIR = Path(os.getenv("INPUT_DIR", BASE_DIR / "data" / "input"))
    TRADE_FILE_GLOB = os.getenv("TRADE_FILE_GLOB", "trades_*.csv")
    VALID_SIDES = os.getenv("VALID_SIDES", "BUY,SELL").split(",")
    CSV_OPTS = {"infer_schema_length": 0}

    # Retry Logic for Connection Failure
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", 5))  # seconds


settings = Settings()
