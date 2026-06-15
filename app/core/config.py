import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://webhook_user:webhook_pass@db:5432/webhook_db",
)

WORKER_COUNT = int(os.getenv("WORKER_COUNT", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
INITIAL_TIMEOUT = int(os.getenv("INITIAL_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))
DLQ_RETENTION_DAYS = int(os.getenv("DLQ_RETENTION_DAYS", "30"))

PERMANENT_ERROR_CODES = {400, 401, 403, 404, 405, 410, 422}

BACKOFF_BASE = 2
BACKOFF_MAX_SECONDS = 3600