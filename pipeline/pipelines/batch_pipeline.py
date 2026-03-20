"""
Batch pipeline: reads a file from GCS, cleans it, routes to the correct
BigQuery table, and archives the source file.

Called synchronously per upload from the HTTP handler.
"""

import re
import logging
import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.cloud import bigquery, storage
from datetime import datetime, timezone

from config import (
    BQ_FULL_DATASET, ROUTE_CONFIG, UPLOAD_BUCKET,
    RETENTION_MONTHS,
)
from transforms.clean import CleanColumns, AddMetadata
from transforms.readers import ReadFileRows

logger = logging.getLogger(__name__)


def resolve_route(file_name: str) -> tuple:
    """Returns (table_name, route_config) or (None, None)."""
    base = file_name.split('/')[-1].lower()
    for prefix, config in ROUTE_CONFIG.items():
        if base.startswith(prefix):
            return config["table"], config
    return None, None


def sanitize_table_name(raw_name: str) -> str:
    """Fallback: derive table name from filename."""
    for ext in ('.xlsx', '.csv', '.json', '.parquet'):
        if raw_name.lower().endswith(ext):
            raw_name = raw_name[:-len(ext)]
            break
    base = raw_name.split('/')[-1]
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', base)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    return sanitized.lower()


def run_batch_pipeline(file_name: str, file_bytes: bytes) -> dict:
    """
    Execute a Beam batch pipeline for a single uploaded file.

    Returns a result dict with status info.
    """
    now_utc = datetime.now(timezone.utc)

    # --- Resolve target table and write disposition ---
    routed_table, route = resolve_route(file_name)
    if routed_table:
        table_name = routed_table
        write_disp = route.get("write_disposition", "WRITE_APPEND")
    else:
        table_name = sanitize_table_name(file_name)
        write_disp = "WRITE_APPEND"

    table_id = f"{BQ_FULL_DATASET}.{table_name}"

    # Map string disposition to Beam enum
    bq_write_disp = (
        BigQueryDisposition.WRITE_TRUNCATE
        if write_disp == "WRITE_TRUNCATE"
        else BigQueryDisposition.WRITE_APPEND
    )

    logger.info(f"Batch pipeline: {file_name} → {table_id} ({write_disp})")

    # --- Pre-read the file to discover columns for explicit schema ---
    # We need to know column names before the pipeline runs so we can
    # build an all-STRING schema. SCHEMA_AUTODETECT infers types from
    # data content (e.g. "2026-03-15" becomes DATE), which conflicts
    # with our bronze-layer strings-first approach.
    import io as _io
    preview_buf = _io.BytesIO(file_bytes)
    lower_name = file_name.lower()
    if lower_name.endswith('.csv'):
        preview_df = pd.read_csv(preview_buf, dtype=str, nrows=1)
    elif lower_name.endswith('.xlsx'):
        preview_df = pd.read_excel(preview_buf, dtype=str, nrows=1)
    elif lower_name.endswith('.json'):
        preview_df = pd.read_json(preview_buf, orient='records', dtype=str)
    elif lower_name.endswith('.parquet'):
        preview_df = pd.read_parquet(preview_buf).astype(str).head(1)
    else:
        preview_df = pd.DataFrame()

    # Clean column names the same way the pipeline will
    raw_columns = [
        c.lower().replace(' ', '_').replace('(', '').replace(')', '')
        for c in preview_df.columns
    ]
    # Add the metadata columns the pipeline appends
    all_columns = raw_columns + ['_ingested_at', '_source_file']

    # Build explicit all-STRING schema
    bq_schema = {
        'fields': [
            {'name': col, 'type': 'STRING', 'mode': 'NULLABLE'}
            for col in all_columns
        ]
    }

    logger.info(f"Schema: {len(all_columns)} STRING columns")

    # --- Build and run the Beam pipeline ---
    options = PipelineOptions(
        runner='DirectRunner',
        streaming=False,
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # Start with a single element: the file content
            | 'CreateInput' >> beam.Create([(file_name, file_bytes)])

            # Parse file bytes into individual row dicts
            | 'ReadRows' >> beam.ParDo(ReadFileRows())

            # Clean column names (lowercase, strip special chars)
            | 'CleanColumns' >> beam.ParDo(CleanColumns())

            # Add bronze metadata (_ingested_at, _source_file)
            | 'AddMetadata' >> beam.ParDo(AddMetadata(source_file=file_name))

            # Write to BigQuery via FILE_LOADS with explicit STRING schema.
            # FILE_LOADS supports WRITE_TRUNCATE (needed for account_map).
            | 'WriteToBQ' >> WriteToBigQuery(
                table=table_id,
                write_disposition=bq_write_disp,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                schema=bq_schema,
                custom_gcs_temp_location=f"gs://{UPLOAD_BUCKET}/beam_temp",
            )
        )

    # --- Post-pipeline: archive the source file in GCS ---
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(UPLOAD_BUCKET)
        blob = bucket.blob(file_name)

        if blob.exists():
            date_folder = now_utc.strftime('%Y/%m/%d')
            base_name = file_name.split('/')[-1]
            archive_path = f"archive/{date_folder}/{base_name}"

            bucket.copy_blob(blob, bucket, archive_path)
            archive_ref = bucket.blob(archive_path)
            if archive_ref.exists():
                blob.delete()
                logger.info(f"Archived: {file_name} → {archive_path}")
            else:
                logger.warning(f"Archive copy not confirmed: {archive_path}")
    except Exception as e:
        logger.warning(f"Archive step failed (non-fatal): {e}")

    # --- Post-pipeline: retention enforcement for CUR data ---
    if route and table_name == "cur_billing_data":
        try:
            bq_client = bigquery.Client()
            retention_query = f"""
            DELETE FROM `{table_id}`
            WHERE PARSE_DATE('%Y-%m-%d', usagestartdate)
                  < DATE_SUB(CURRENT_DATE(), INTERVAL {RETENTION_MONTHS} MONTH)
            """
            job = bq_client.query(retention_query)
            job.result()
            logger.info(f"Retention: cleaned rows older than {RETENTION_MONTHS} months")
        except Exception as e:
            logger.warning(f"Retention enforcement failed (non-fatal): {e}")

    # --- Get row count for response ---
    try:
        bq_client = bigquery.Client()
        table_ref = bq_client.get_table(table_id)
        row_count = table_ref.num_rows
    except Exception:
        row_count = "unknown"

    return {
        "status": "success",
        "table": table_id,
        "write_disposition": write_disp,
        "total_rows": row_count,
        "file": file_name,
    }
