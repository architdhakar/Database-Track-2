"""Analyzes field statistics from incoming data."""
import copy
import threading
from datetime import datetime

class Analyzer:
    def __init__(self):
        self.field_stats = {}
        self.total_records_processed = 0
        self.lock = threading.Lock()

    def analyze_batch(self, batch):
        if not batch:
            return

        with self.lock:
            self.total_records_processed += len(batch)

            for record in batch:
                for key, value in record.items():
                    if key not in self.field_stats:
                        self.field_stats[key] = {
                            "count": 0,
                            "types": set(),  
                            "is_nested": False,
                            "unique_values": set(),
                            "base_unique_count": 0,
                            "_unique_capped": False
                        }

                    self.field_stats[key]["count"] += 1
                    current_type = type(value).__name__
                    self.field_stats[key]["types"].add(current_type)

                    if isinstance(value, (dict, list)):
                        self.field_stats[key]["is_nested"] = True
                    else:
                        if len(self.field_stats[key]["unique_values"]) < 1000:
                            self.field_stats[key]["unique_values"].add(value)

    def get_schema_stats(self):
        with self.lock:
            summary = {}

            for key, stats in self.field_stats.items():
                freq_ratio = 0.0
                if self.total_records_processed > 0:
                    freq_ratio = stats["count"] / self.total_records_processed

                unique_types = list(stats["types"])
                is_stable = (len(unique_types) == 1)
                detected_type = unique_types[0] if is_stable else "mixed"

                session_unique_count = len(stats["unique_values"])
                total_unique_count = stats.get("base_unique_count", 0) + session_unique_count
                
                unique_ratio = 0.0
                if stats["count"] > 0:
                    unique_ratio = total_unique_count / stats["count"]
                
                if session_unique_count >= 1000 or stats.get("_unique_capped"):
                    pass

                summary[key] = {
                    "frequency_ratio": freq_ratio,
                    "type_stability": "stable" if is_stable else "unstable",
                    "detected_type": detected_type,
                    "is_nested": stats["is_nested"],
                    "unique_ratio": unique_ratio,
                    "count": stats["count"]
                }

            return summary

    def export_stats(self):
        with self.lock:
            export_data = {
                "total_records_processed": self.total_records_processed,
                "field_stats": {}
            }
            
            for key, stats in self.field_stats.items():
                export_stats = copy.deepcopy(stats)
                export_stats["types"] = list(export_stats["types"])
                
                total_unique = stats["base_unique_count"] + len(stats["unique_values"])
                export_stats["base_unique_count"] = total_unique
                
                if len(stats["unique_values"]) > 20:
                    export_stats["unique_values"] = []
                    if len(stats["unique_values"]) >= 1000:
                        stats["_unique_capped"] = True
                        export_stats["_unique_capped"] = True
                else:
                    export_stats["unique_values"] = [
                        v.isoformat() if isinstance(v, datetime) else v 
                        for v in stats["unique_values"]
                    ]
                
                export_data["field_stats"][key] = export_stats
                    
            return export_data

    def load_stats(self, loaded_data):
        if "field_stats" in loaded_data:
            self.total_records_processed = loaded_data.get("total_records_processed", 0)
            data_stats = loaded_data["field_stats"]
        else:
            data_stats = loaded_data
            self.total_records_processed = 0

        with self.lock:
            self.field_stats = {}
            for key, stats in data_stats.items():
                self.field_stats[key] = stats
                stats["types"] = set(stats["types"])
                
                is_capped = stats.get("_unique_capped", False)
                restored_set = set(stats.get("unique_values", []))
                stats["unique_values"] = restored_set
                
                total_from_json = stats.get("base_unique_count", 0)
                
                if is_capped:
                    # If it was already capped, we trick the logic to keep it capped
                    if len(restored_set) < 1000:
                        stats["unique_values"] = set(range(1000))
                    stats["base_unique_count"] = total_from_json - 1000
                else:
                    stats["base_unique_count"] = total_from_json - len(restored_set)