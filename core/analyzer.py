"""
Analysis of json data
"""
class Analyzer:
    def __init__(self):
        """
        Initializes the Analyzer to track field statistics across multiple batches.
        """
        # Dictionary to hold raw stats for every field seen so far
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
                # We store the string name of the type (e.g., 'int', 'str', 'float')
                # If a field has { 'int', 'str' }, it is "Unstable".
                current_type = type(value).__name__
                self.field_stats[key]["types"].add(current_type)

                # 4. Check for Nesting
                # If it's a dictionary or list, mark as nested.
                if isinstance(value, (dict, list)):
                    self.field_stats[key]["is_nested"] = True

    def get_schema_stats(self):
        """
        Returns a summary of the analysis for the Classifier to use.
        Calculates percentage frequency and determines stability.
        """
        summary = {}

        for key, stats in self.field_stats.items():
            # Frequency Calculation (0.0 to 1.0)
            freq_ratio = 0.0
            if self.total_records_processed > 0:
                freq_ratio = stats["count"] / self.total_records_processed

            # Stability Check
            # If we've seen more than 1 type, it's unstable (mixed).
            unique_types = list(stats["types"])
            is_stable = (len(unique_types) == 1)
            
            # Identify the dominant type (or 'mixed')
            # For SQL, we need a specific type. If mixed, we might default to VARCHAR later.
            detected_type = unique_types[0] if is_stable else "mixed"

            summary[key] = {
                "frequency_ratio": freq_ratio,  # e.g., 0.95 (95%)
                "type_stability": "stable" if is_stable else "unstable",
                "detected_type": detected_type,
                "is_nested": stats["is_nested"]
            }

        return summary

if __name__ == "__main__":
    # Simulate a stream where 'age' is stable, but 'score' drifts types
    test_batch = [
        {"user": "alice", "age": 25, "score": 100},
        {"user": "bob",   "age": 30, "score": "A+"},  # Score changes int -> str
        {"user": "charlie", "metadata": {"origin": "US"}} # Nested field
    ]

    analyzer = Analyzer()
    analyzer.analyze_batch(test_batch)
    
    stats = analyzer.get_schema_stats()
    
    import json
    print(json.dumps(stats, indent=2))