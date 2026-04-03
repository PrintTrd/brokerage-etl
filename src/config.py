import os
from pathlib import Path


class Settings:
    def __init__(self):
        # ── 1. Infrastructure & Secrets ──
        self.DB_USER = os.getenv("POSTGRES_USER", "myuser")
        self.DB_PASS = os.getenv("POSTGRES_PASSWORD", "dev_password_local_only")
        self.DB_HOST = os.getenv("POSTGRES_HOST", "db")
        self.DB_PORT = os.getenv("POSTGRES_PORT", "5432")
        self.DB_NAME = os.getenv("POSTGRES_DB", "brokerage_data")
        self.DATABASE_URL = f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        # Retry Logic for Connection Failure (seconds)
        self.MAX_RETRIES = 5
        self.RETRY_DELAY = 3

        # ── 2. Business Logic & App Config ──
        # Dynamic path handling (supports both Docker '/app' and Local Windows/Linux)
        self.BASE_DIR = Path(__file__).resolve().parent.parent
        self.INPUT_DIR = Path(os.getenv("INPUT_DIR", self.BASE_DIR / "data" / "input"))

        self.TRADE_FILE_GLOB = "trades_*.csv"
        self.VALID_SIDES = {"BUY", "SELL"}
        # CSV parsing options for Polars
        # Read all as strings initially to prevent inference errors on messy numbers
        self.CSV_OPTS = {"infer_schema_length": 0, "separator": ",", "has_header": True}


settings = Settings()
