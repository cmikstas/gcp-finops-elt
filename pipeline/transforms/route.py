"""
Beam DoFn for routing rows to the correct BigQuery table
based on the source filename prefix.
"""

import apache_beam as beam
from config import ROUTE_CONFIG, BQ_FULL_DATASET


class RouteToTable(beam.DoFn):
    """
    Reads _source_file from each row, matches against ROUTE_CONFIG prefixes,
    and yields a tuple of (table_id, row) for downstream WriteToBigQuery.

    Falls back to a sanitized table name for unrecognized prefixes (legacy behavior).
    """

    def process(self, row: dict):
        import re
        source_file = row.get('_source_file', '')
        base_name = source_file.split('/')[-1].lower()

        table_name = None
        for prefix, config in ROUTE_CONFIG.items():
            if base_name.startswith(prefix):
                table_name = config["table"]
                break

        if table_name is None:
            # Legacy fallback: sanitize filename into a table name
            raw = base_name
            for ext in ('.xlsx', '.csv', '.json', '.parquet'):
                if raw.endswith(ext):
                    raw = raw[:-len(ext)]
                    break
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', raw)
            table_name = re.sub(r'_+', '_', table_name).strip('_').lower()

        table_id = f"{BQ_FULL_DATASET}.{table_name}"
        yield (table_id, row)
