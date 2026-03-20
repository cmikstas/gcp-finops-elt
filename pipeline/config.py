"""
Shared configuration for the FinOps Beam pipeline.
Mirrors the routing logic from billing-data-loader-v3.py.
"""

import os

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
BQ_PROJECT = os.environ.get("BQ_PROJECT", "cur-sql-practice")
BQ_DATASET = os.environ.get("BQ_DATASET", "finops_lab")
BQ_FULL_DATASET = f"{BQ_PROJECT}.{BQ_DATASET}"

# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------
UPLOAD_BUCKET = os.environ.get("DESTINATION_BUCKET", "cur-billing-data-upload")

# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "cur-sql-practice")
TOPIC_ID = os.environ.get("TOPIC_ID", "mock-server-metrics")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "mock-server-metrics-beam-sub")

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
VALID_TOKENS = set(filter(None, [
    os.environ.get("PRIMARY_TOKEN"),
    os.environ.get("DEMO_TOKEN"),
]))

# ---------------------------------------------------------------------------
# File handling
# ---------------------------------------------------------------------------
SUPPORTED_EXTENSIONS = ('.xlsx', '.csv', '.json', '.parquet')
MAX_FILE_SIZE_MB = 100
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

MIME_TYPES = {
    '.csv': 'text/csv',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.json': 'application/json',
    '.parquet': 'application/octet-stream',
}

# ---------------------------------------------------------------------------
# Routing: filename prefix → (table_name, write_disposition)
# ---------------------------------------------------------------------------
ROUTE_CONFIG = {
    "cur_": {
        "table": "cur_billing_data",
        "write_disposition": "WRITE_APPEND",
    },
    "account_map_": {
        "table": "account_map",
        "write_disposition": "WRITE_TRUNCATE",
    },
}

# Streaming metrics table
METRICS_TABLE = "server_metrics"

# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------
RETENTION_MONTHS = 9

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://storage.googleapis.com",
    # Will add demo.mikstasapps.com later
]
