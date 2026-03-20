"""
Beam DoFn to read uploaded files from GCS bytes into dicts.
Supports CSV, JSON, XLSX, and Parquet.
"""

import io
import apache_beam as beam
import pandas as pd


class ReadFileRows(beam.DoFn):
    """
    Takes a tuple of (filename, file_bytes) and yields one dict per row.
    All values are read as strings (bronze layer).
    """

    def process(self, element):
        file_name, file_bytes = element
        buf = io.BytesIO(file_bytes)
        lower_name = file_name.lower()

        if lower_name.endswith('.csv'):
            df = pd.read_csv(buf, dtype=str)
        elif lower_name.endswith('.xlsx'):
            df = pd.read_excel(buf, dtype=str)
        elif lower_name.endswith('.json'):
            df = pd.read_json(buf, orient='records').astype(str)
        elif lower_name.endswith('.parquet'):
            df = pd.read_parquet(buf).astype(str)
        else:
            raise ValueError(f"Unsupported file type: {file_name}")

        # Replace pandas null artifacts with empty string
        df = df.replace(['nan', 'None', '<NA>', 'nan.0'], '')

        # Yield each row as a dict
        for record in df.to_dict(orient='records'):
            yield record
