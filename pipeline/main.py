"""
FinOps Pipeline v2 — Beam on Cloud Run

Single HTTP service that:
  1. Receives file uploads → runs Beam batch pipeline → BigQuery (Looker)
  2. Receives streaming metrics → publishes to Pub/Sub
     (background Beam streaming pipeline reads Pub/Sub → BigQuery → Grafana)
  3. Serves a /health endpoint for Cloud Run probes
"""

import hmac
import json
import os
import logging
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import storage, pubsub_v1
from werkzeug.utils import secure_filename

from config import (
    VALID_TOKENS, UPLOAD_BUCKET, PROJECT_ID, TOPIC_ID,
    SUPPORTED_EXTENSIONS, MAX_FILE_SIZE_BYTES, MAX_FILE_SIZE_MB,
    MIME_TYPES, ALLOWED_ORIGINS,
)
from pipelines.batch_pipeline import run_batch_pipeline
from pipelines.stream_pipeline import start_streaming_pipeline, is_streaming_healthy

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Reuse clients across warm invocations
storage_client = storage.Client()
publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(PROJECT_ID, TOPIC_ID)


# ---------------------------------------------------------------------------
# Start the Beam streaming pipeline in a background thread at import time.
# This means it starts when the Cloud Run container boots.
# ---------------------------------------------------------------------------
start_streaming_pipeline()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cors_headers() -> dict:
    """Build CORS headers based on request origin."""
    origin = request.headers.get("Origin", "")
    headers = {
        "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Max-Age": "3600",
    }
    if "*" in ALLOWED_ORIGINS:
        headers["Access-Control-Allow-Origin"] = "*"
    elif origin in ALLOWED_ORIGINS:
        headers["Access-Control-Allow-Origin"] = origin
    return headers


def json_ok(body: dict, status: int = 200):
    headers = cors_headers()
    headers["Content-Type"] = "application/json"
    return (json.dumps(body, default=str), status, headers)


def json_error(message: str, status: int):
    headers = cors_headers()
    headers["Content-Type"] = "application/json"
    return (json.dumps({"error": message}), status, headers)


def get_file_extension(filename: str) -> str:
    dot_index = filename.rfind('.')
    if dot_index == -1:
        return ""
    return filename[dot_index:].lower()


def authenticate():
    """Validate bearer token. Returns None if OK, or error response tuple."""
    if not VALID_TOKENS:
        logger.error("FATAL: No VALID_TOKENS configured")
        return json_error("Internal server error", 500)
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "", 1)
    if not any(hmac.compare_digest(token.encode(), t.encode()) for t in VALID_TOKENS):
        logger.warning(f"AUTH DENIED: origin={request.remote_addr}")
        return json_error("Unauthorized", 401)
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.before_request
def handle_preflight():
    """Handle CORS preflight for all routes."""
    if request.method == "OPTIONS":
        return ("", 204, cors_headers())


@app.route('/health', methods=['GET'])
def health():
    """Health check for Cloud Run. Reports streaming pipeline status."""
    streaming_ok = is_streaming_healthy()
    return json_ok({
        "status": "healthy",
        "streaming_pipeline": "running" if streaming_ok else "stopped",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route('/stream', methods=['POST'])
def stream():
    """
    Receive a JSON metric and publish to Pub/Sub.
    The background Beam streaming pipeline reads from the subscription
    and writes to BigQuery for Grafana.
    """
    auth_err = authenticate()
    if auth_err:
        return auth_err

    try:
        data = request.get_json()
        if not data:
            return json_error("Invalid JSON payload", 400)

        # Ensure timestamp exists
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(timezone.utc).isoformat()

        message_bytes = json.dumps(data).encode("utf-8")
        future = publisher_client.publish(topic_path, data=message_bytes)
        message_id = future.result()

        logger.info(f"STREAM OK: Published metric {message_id}")
        return json_ok({"status": "success", "message_id": message_id})

    except Exception as e:
        logger.error(f"STREAM ERROR: {e}")
        return json_error("Internal server error", 500)


@app.route('/upload', methods=['POST'])
def upload():
    """
    Receive a file upload, save to GCS, then run the Beam batch pipeline
    to load it into BigQuery.
    """
    auth_err = authenticate()
    if auth_err:
        return auth_err

    # --- File presence ---
    if 'file' not in request.files:
        return json_error("No file part in the request", 400)

    uploaded_file = request.files['file']
    if not uploaded_file.filename:
        return json_error("No file selected", 400)

    # --- Filename sanitization ---
    original_ext = get_file_extension(uploaded_file.filename)
    clean_filename = secure_filename(uploaded_file.filename)
    if not clean_filename:
        return json_error("Invalid filename", 400)

    ext = get_file_extension(clean_filename)
    if not ext and original_ext:
        ext = original_ext
        clean_filename = f"{clean_filename}{ext}"

    if ext not in {e for e in SUPPORTED_EXTENSIONS}:
        return json_error(
            f"File type '{ext}' not allowed. Accepted: {', '.join(sorted(SUPPORTED_EXTENSIONS))}",
            400,
        )

    # --- File size checks ---
    uploaded_file.seek(0, 2)
    actual_size = uploaded_file.tell()
    uploaded_file.seek(0)

    if actual_size > MAX_FILE_SIZE_BYTES:
        return json_error(f"File exceeds {MAX_FILE_SIZE_MB}MB limit", 413)
    if actual_size == 0:
        return json_error("File is empty", 400)

    # --- Upload to GCS ---
    try:
        bucket = storage_client.bucket(UPLOAD_BUCKET)
        blob = bucket.blob(clean_filename)
        blob.content_type = MIME_TYPES.get(ext, 'application/octet-stream')
        blob.upload_from_file(uploaded_file, rewind=True)

        size_kb = actual_size / 1024
        logger.info(f"GCS OK: {clean_filename} ({size_kb:.1f}KB)")

    except Exception as e:
        logger.error(f"GCS ERROR: {clean_filename}: {e}")
        return json_error("Failed to upload file to storage", 500)

    # --- Run Beam batch pipeline ---
    try:
        # Download the bytes we just uploaded (Beam needs them in-memory)
        uploaded_file.seek(0)
        file_bytes = uploaded_file.read()

        result = run_batch_pipeline(clean_filename, file_bytes)

        logger.info(f"BEAM OK: {result}")
        return json_ok({
            "status": "success",
            "file": clean_filename,
            "size_bytes": actual_size,
            "pipeline": result,
        })

    except Exception as e:
        logger.error(f"BEAM ERROR: {clean_filename}: {e}")
        # File is already in GCS, so it's not lost — just pipeline failed
        return json_error(f"File uploaded but pipeline failed: {str(e)}", 500)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
