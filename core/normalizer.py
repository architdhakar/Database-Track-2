"""
Takes the raw json data from the api and cleans it.
"""
import re
from datetime import datetime

class Normalizer:
    def __init__(self):
        # Map of "Bad Key" -> "Good Key"
        
        # No Key Ambiguity: We do not hardcode mappings anymore.
        pass

    def _to_snake_case(self, key):
        """
        Converts CamelCase or random casing to snake_case.
        """
        # Insert underscore before capital letters (except the first one)
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
        # Handle cases like 'IPAddress' -> 'ip_address'
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

        return normalized_record

    def normalize_record(self, record):
        """
        Takes a single raw JSON record and returns a normalized dictionary.
        """
        normalized_record = {}

        # 1. Generate Server Timestamp (sys_ingested_at)
        if 'sys_ingested_at' not in record:
            normalized_record['sys_ingested_at'] = datetime.now().isoformat()
        else:
            normalized_record['sys_ingested_at'] = record['sys_ingested_at']

        for key, value in record.items():
            if key == 'sys_ingested_at':
                continue
            
            # 2. Key Standardization (Soft resolution only)
            # Handles generic "userName" -> "user_name"
            standard_key = self._to_snake_case(key)
            
            # 3. Value Cleaning
            # Strip whitespace from strings to avoid "London" vs "London " mismatch
            if isinstance(value, str):
                cleaned_value = value.strip()
            else:
                cleaned_value = value
                
            normalized_record[standard_key] = cleaned_value

        return normalized_record

    def normalize_batch(self, batch):
        return [self.normalize_record(rec) for rec in batch]


if __name__ == "__main__":
    test_batch = [
        {"userName": "alice", "IP": "192.168.1.1", "t_stamp": "2026-01-28 10:00:00"},
        {"User_Name": "bob", "ip": "192.168.1.2", "timestamp": "2026-01-28 10:05:00"},
        {"deviceID": 101, "Status": "Active"}
    ]
    
    norm = Normalizer()
    clean_batch = norm.normalize_batch(test_batch)
    
    import json
    print(json.dumps(clean_batch, indent=2))
