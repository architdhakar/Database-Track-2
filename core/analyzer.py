"""
Analysis of json data
"""
import copy

import threading

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
                            "unique_values": set() # For cardinality tracking
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
                unique_count = len(stats["unique_values"])
                unique_ratio = 0.0
                if stats["count"] > 0:
                    unique_ratio = unique_count / stats["count"]
                
                # If we capped it, treat it as very unique (1.0) for heuristic purposes if count is large
                if unique_count >= 1000:
                     unique_ratio = 1.0

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
        """
        with self.lock:
            # Deep copy so we don't mess up the running analyzer
            export_data = {
                "total_records_processed": self.total_records_processed,
                "field_stats": copy.deepcopy(self.field_stats)
            }
            
            for key, stats in export_data["field_stats"].items():
                # JSON can't save sets, so we make them lists
                stats["types"] = list(stats["types"])
                
                # Optimization: Don't save massive lists of unique values if they are capped
                if len(stats["unique_values"]) >= 1000:
                    stats["unique_values"] = [] 
                    stats["_unique_capped"] = True
                else:
                    stats["unique_values"] = list(stats["unique_values"])
                    
            return export_data

    def load_stats(self, loaded_data):
        """
        Loads stats from JSON and converts Lists -> Sets.
        """
        # Handle backward compatibility (if file didn't have total_records_processed)
        if "field_stats" in loaded_data:
            self.total_records_processed = loaded_data.get("total_records_processed", 0)
            self.field_stats = loaded_data["field_stats"]
        else:
             # Legacy format support (just the dict)
            self.field_stats = loaded_data
            self.total_records_processed = 0 # Can't recover this unfortunately, unless we guess

        with self.lock:
            for key, stats in self.field_stats.items():
                # Convert back to set so analyze_batch works
                stats["types"] = set(stats["types"])
                
                # Handle Capped Optimization
                if stats.get("_unique_capped", False):
                     # Restore a dummy set of 1000 items to trick logic into thinking it's full
                     stats["unique_values"] = set(range(1000)) 
                else:
                    stats["unique_values"] = set(stats.get("unique_values", []))