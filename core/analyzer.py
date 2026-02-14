"""
Analysis of json data
"""
import copy
import threading
from datetime import datetime

class Analyzer:
    def __init__(self):
        """
        Initializes the Analyzer to track field statistics across multiple batches.
        """
        self.field_stats = {}
        self.total_records_processed = 0
        self.lock = threading.Lock()

    def analyze_batch(self, batch):
        """
        Updates internal statistics based on a new batch of normalized records.
        """
        if not batch:
            return

        with self.lock:
            self.total_records_processed += len(batch)

            for record in batch:
                for key, value in record.items():
                    # 1. Initialize stats for new field
                    if key not in self.field_stats:
                        self.field_stats[key] = {
                            "count": 0,
                            "types": set(),  
                            "is_nested": False,
                            "unique_values": set(), # Current session unique values
                            "base_unique_count": 0, # Historical unique count from previous sessions
                            "_unique_capped": False # Flag to keep state across exports
                        }

                    # 2. Update Frequency Count
                    self.field_stats[key]["count"] += 1

                    # 3. Analyze Type
                    current_type = type(value).__name__
                    self.field_stats[key]["types"].add(current_type)

                    # 4. Check for Nesting
                    if isinstance(value, (dict, list)):
                        self.field_stats[key]["is_nested"] = True
                    else:
                         # 5. Track Uniqueness (HyperLogLog-ish approximation via set capping)
                        if len(self.field_stats[key]["unique_values"]) < 1000:
                            self.field_stats[key]["unique_values"].add(value)

    def get_schema_stats(self):
        """
        Returns a summary for the Classifier. 
        Calculates percentages and stability.
        """
        with self.lock:
            summary = {}

            for key, stats in self.field_stats.items():
                freq_ratio = 0.0
                if self.total_records_processed > 0:
                    freq_ratio = stats["count"] / self.total_records_processed

                # Convert set to list for stability check
                unique_types = list(stats["types"])
                is_stable = (len(unique_types) == 1)
                
                detected_type = unique_types[0] if is_stable else "mixed"

                # Calculate Uniqueness
                # If we hit the cap (1000), we assume it's high cardinality
                session_unique_count = len(stats["unique_values"])
                total_unique_count = stats.get("base_unique_count", 0) + session_unique_count
                
                unique_ratio = 0.0
                if stats["count"] > 0:
                    unique_ratio = total_unique_count / stats["count"]
                
                # If we capped it, we no longer blindly set it to 1.0. 
                # Instead, we let the ratio decrease as the total count grows.
                # However, we still need a heuristic to identify TRULY unique fields (like UUIDs) 
                # even if we only track the first 1000.
                if session_unique_count >= 1000 or stats.get("_unique_capped"):
                     # If it's capped, we keep the ratio as is (base_unique_count / count)
                     # or maybe we should have a different marker. 
                     # For now, let's just ensure it's not hardcoded to 1.0.
                     pass

                summary[key] = {
                    "frequency_ratio": freq_ratio,
                    "type_stability": "stable" if is_stable else "unstable",
                    "detected_type": detected_type,
                    "is_nested": stats["is_nested"],
                    "unique_ratio": unique_ratio
                }

            return summary

    def export_stats(self):
        """
        Prepares stats for JSON saving by converting Sets -> Lists.
        Optimized: Does not save large value sets to JSON to keep file small.
        """
        with self.lock:
            export_data = {
                "total_records_processed": self.total_records_processed,
                "field_stats": {}
            }
            
            for key, stats in self.field_stats.items():
                # Deep copy the field's data for manipulation
                export_stats = copy.deepcopy(stats)
                export_stats["types"] = list(export_stats["types"])
                
                # Total unique is the historical baseline + current session uniques
                total_unique = stats["base_unique_count"] + len(stats["unique_values"])
                export_stats["base_unique_count"] = total_unique
                
                # Optimization: Only save values IF it's a small set (categories)
                if len(stats["unique_values"]) > 20:
                    export_stats["unique_values"] = []
                    # If we hit the 1000 cap, mark it in the REAL state too
                    if len(stats["unique_values"]) >= 1000:
                        stats["_unique_capped"] = True
                        export_stats["_unique_capped"] = True
                else:
                    # Fix: Handle datetime objects in unique_values for JSON serialization
                    export_stats["unique_values"] = [
                        v.isoformat() if isinstance(v, datetime) else v 
                        for v in stats["unique_values"]
                    ]
                
                export_data["field_stats"][key] = export_stats
                    
            return export_data

    def load_stats(self, loaded_data):
        """
        Loads stats from JSON and converts Lists -> Sets.
        Fixed: Avoids double-counting restored sets.
        """
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
                
                # Restore unique_values
                is_capped = stats.get("_unique_capped", False)
                restored_set = set(stats.get("unique_values", []))
                stats["unique_values"] = restored_set
                
                # CRITICAL: base_unique_count in JSON is the TOTAL.
                # Internal base_unique_count should be Total - len(set)
                total_from_json = stats.get("base_unique_count", 0)
                
                if is_capped:
                    # If it was already capped, we trick the logic to keep it capped
                    if len(restored_set) < 1000:
                        stats["unique_values"] = set(range(1000))
                    stats["base_unique_count"] = total_from_json - 1000
                else:
                    stats["base_unique_count"] = total_from_json - len(restored_set)