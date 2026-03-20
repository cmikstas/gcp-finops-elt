"""
Beam DoFn transforms for column cleaning and metadata enrichment.
Bronze-layer: everything stays as strings, type casting happens in BQ views.
"""

import apache_beam as beam
from datetime import datetime, timezone


class CleanColumns(beam.DoFn):
    """
    Lowercase column names, strip spaces and parens.
    Input:  dict with raw column names
    Output: dict with cleaned column names
    """

    def process(self, row: dict):
        cleaned = {}
        for key, value in row.items():
            clean_key = (
                key.lower()
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
            )
            # Bronze layer: coerce everything to string, replace null-like values
            if value is None or str(value) in ('nan', 'None', '<NA>', 'nan.0', ''):
                cleaned[clean_key] = ''
            else:
                cleaned[clean_key] = str(value)
        yield cleaned


class AddMetadata(beam.DoFn):
    """
    Add bronze-layer metadata columns: _ingested_at and _source_file.
    """

    def __init__(self, source_file: str):
        self._source_file = source_file

    def process(self, row: dict):
        row['_ingested_at'] = datetime.now(timezone.utc).isoformat()
        row['_source_file'] = self._source_file
        yield row
