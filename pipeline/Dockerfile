# FinOps Beam Pipeline — Cloud Run container
# Uses gunicorn to serve the Flask app, with the Beam streaming
# pipeline running in a background daemon thread.

FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr (important for Cloud Run logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps needed by some Beam/pyarrow/openpyxl packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Cloud Run injects PORT env var (default 8080)
ENV PORT=8080

# Use gunicorn with a single worker (DirectRunner manages its own threads).
# Timeout set high because batch pipelines may take 30+ seconds for large files.
CMD exec gunicorn \
    --bind :$PORT \
    --workers 1 \
    --threads 4 \
    --timeout 300 \
    main:app
