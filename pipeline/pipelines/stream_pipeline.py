"""
Streaming pipeline: reads JSON metrics from a Pub/Sub subscription,
windows them into 10-second batches, and writes to BigQuery.

Runs in a background thread inside the Cloud Run container.
"""

import json
import logging
import threading
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.window import FixedWindows

from config import PROJECT_ID, SUBSCRIPTION_ID, BQ_FULL_DATASET, METRICS_TABLE

logger = logging.getLogger(__name__)

# Full subscription path for Beam's ReadFromPubSub
SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
METRICS_TABLE_ID = f"{BQ_FULL_DATASET}.{METRICS_TABLE}"

# BigQuery schema for server metrics
METRICS_SCHEMA = {
    'fields': [
        {'name': 'timestamp',        'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'server_id',        'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cpu_usage',        'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'memory_usage',     'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'disk_io_mbps',     'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'network_in_mbps',  'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': '_ingested_at',     'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


class ParseMetricJson(beam.DoFn):
    """Parse Pub/Sub message bytes into a metric dict."""

    def process(self, message_bytes):
        from datetime import datetime, timezone
        try:
            record = json.loads(message_bytes.decode('utf-8'))
            # Add ingestion timestamp
            record['_ingested_at'] = datetime.now(timezone.utc).isoformat()
            # Ensure all values are strings (bronze layer consistency)
            yield {k: str(v) if v is not None else '' for k, v in record.items()}
        except Exception as e:
            logger.error(f"Failed to parse metric message: {e}")


def _run_streaming_pipeline():
    """Blocking call that runs the streaming pipeline until termination."""
    logger.info(f"Starting streaming pipeline: {SUBSCRIPTION_PATH} → {METRICS_TABLE_ID}")

    options = PipelineOptions(
        runner='DirectRunner',
        streaming=True,
        project=PROJECT_ID,
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # Read from Pub/Sub subscription (yields raw bytes)
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION_PATH
            )

            # Parse JSON bytes into dicts
            | 'ParseJSON' >> beam.ParDo(ParseMetricJson())

            # Window into 10-second fixed windows for batched BQ writes
            | 'Window' >> beam.WindowInto(FixedWindows(10))

            # Write to BigQuery using streaming inserts
            | 'WriteToBQ' >> WriteToBigQuery(
                table=METRICS_TABLE_ID,
                schema=METRICS_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


# ---------------------------------------------------------------------------
# Background thread management
# ---------------------------------------------------------------------------

_stream_thread = None


def start_streaming_pipeline():
    """
    Launch the streaming pipeline in a daemon thread.
    Called once at container startup.
    """
    global _stream_thread
    if _stream_thread is not None and _stream_thread.is_alive():
        logger.info("Streaming pipeline already running")
        return

    _stream_thread = threading.Thread(
        target=_run_streaming_pipeline,
        name="beam-streaming-pipeline",
        daemon=True,  # Dies when main process exits
    )
    _stream_thread.start()
    logger.info("Streaming pipeline thread started")


def is_streaming_healthy() -> bool:
    """Check if the streaming background thread is alive."""
    return _stream_thread is not None and _stream_thread.is_alive()
