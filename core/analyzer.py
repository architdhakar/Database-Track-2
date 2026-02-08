"""
Analysis of json data
"""
import copy

class Analyzer:
    def __init__(self):
        """
        Initializes the Analyzer to track field statistics across multiple batches.
        """
        self.field_stats = {}
        self.total_records_processed = 0

    def analyze_batch(self, batch):
        """
        Updates internal statistics based on a new batch of normalized records.
        """
        if not batch:
            return

        self.total_records_processed += len(batch)

        for record in batch:
            for key, value in record.items():
                # 1. Initialize stats for new field
                if key not in self.field_stats:
                    self.field_stats[key] = {
                        "count": 0,
                        "types": set(),  
                        "is_nested": False
                    }

                # 2. Update Frequency Count
                self.field_stats[key]["count"] += 1

                # 3. Analyze Type
                current_type = type(value).__name__
                self.field_stats[key]["types"].add(current_type)

                # 4. Check for Nesting
                if isinstance(value, (dict, list)):
                    self.field_stats[key]["is_nested"] = True

    def get_schema_stats(self):
        """
        Returns a summary for the Classifier. 
        Calculates percentages and stability.
        """
        summary = {}

        for key, stats in self.field_stats.items():
            freq_ratio = 0.0
            if self.total_records_processed > 0:
                freq_ratio = stats["count"] / self.total_records_processed

            # Convert set to list for stability check
            unique_types = list(stats["types"])
            is_stable = (len(unique_types) == 1)
            
            detected_type = unique_types[0] if is_stable else "mixed"

            summary[key] = {
                "frequency_ratio": freq_ratio,
                "type_stability": "stable" if is_stable else "unstable",
                "detected_type": detected_type,
                "is_nested": stats["is_nested"]
            }

        return summary

    def export_stats(self):
        """
        Prepares stats for JSON saving by converting Sets -> Lists.
        """
        # Deep copy so we don't mess up the running analyzer
        export_data = copy.deepcopy(self.field_stats)
        for key, stats in export_data.items():
            # JSON can't save sets, so we make them lists
            stats["types"] = list(stats["types"]) 
        return export_data

    def load_stats(self, loaded_stats):
        """
        Loads stats from JSON and converts Lists -> Sets so .add() works.
        """
        self.field_stats = loaded_stats
        for key, stats in self.field_stats.items():
            # Convert back to set so analyze_batch works
            stats["types"] = set(stats["types"])