"""Normalizes raw JSON data from the API."""
import re
from datetime import datetime

class Normalizer:
    def __init__(self):
        pass

    def _to_snake_case(self, key):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def normalize_record(self, record):
        normalized_record = {}

        if 'sys_ingested_at' not in record:
            normalized_record['sys_ingested_at'] = datetime.now()
        else:
            normalized_record['sys_ingested_at'] = record['sys_ingested_at']

        for key, value in record.items():
            if key == 'sys_ingested_at':
                continue
            
            standard_key = self._to_snake_case(key)
            
            if isinstance(value, str):
                cleaned_value = value.strip()
            else:
                cleaned_value = value
                
            normalized_record[standard_key] = cleaned_value

        return normalized_record

    def normalize_batch(self, batch):
        return [self.normalize_record(rec) for rec in batch]
