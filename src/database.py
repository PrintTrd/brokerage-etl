import time
from sqlalchemy import create_engine
from config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    retries = 0
    while retries < settings.MAX_RETRIES:
        try:
            engine = create_engine(settings.DATABASE_URL)
            # connection test
            with engine.connect() as conn:
                return engine
        except Exception as e:
            retries += 1
            logger.error(
                f"Database connection failed. Retry {retries}/{settings.MAX_RETRIES} in {settings.RETRY_DELAY}s..."
            )
            time.sleep(settings.RETRY_DELAY)

    raise Exception("Could not connect to database after several retries.")
