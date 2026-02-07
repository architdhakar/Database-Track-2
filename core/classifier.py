"""
Classifier for user events.
Decision Logic
"""
class Classifier:
    def __init__(self, freq_threshold=0.8):
        """
        Initializes the Classifier with decision thresholds.
        :param freq_threshold: Fields appearing in >80% (default) of records are candidates for SQL.
        """
        self.freq_threshold = freq_threshold
        self.common_fields = {'user_name', 't_stamp', 'sys_ingested_at'}

    def decide_schema(self, stats):
        """
        Decides the target backend (SQL vs MongoDB) for each field.
        
        Returns:
            dict: {'field_name': {'target': 'SQL', 'type': 'VARCHAR'}, ...}
        """
        schema_decisions = {}

        for field, metrics in stats.items():
            
            # 1. Handle Mandatory Fields (Always BOTH)
            if field in self.common_fields:
                schema_decisions[field] = {
                    "target": "BOTH",
                    "sql_type": self._map_python_type_to_sql(metrics["detected_type"])
                }
                continue

            # 2. Heuristic Logic
            
            # CRITERIA A: Is it nested? -> MongoDB
            # SQL struggles with deep nesting.
            if metrics["is_nested"]:
                schema_decisions[field] = {"target": "MONGO"}
                continue

            # CRITERIA B: Is it unstable (Type Drifting)? -> MongoDB
            # If it switches between int and string, SQL will fail or requires complex casting.
            if metrics["type_stability"] == "unstable":
                schema_decisions[field] = {"target": "MONGO"}
                continue

            # CRITERIA C: Is it rare (Sparse)? -> MongoDB
            # Creating SQL columns for rare data (NULLs) is inefficient.
            if metrics["frequency_ratio"] < self.freq_threshold:
                schema_decisions[field] = {"target": "MONGO"}
                continue

            # CRITERIA D: High Frequency + Stable + Flat -> SQL
            # This is the ideal candidate for a structured table.
            schema_decisions[field] = {
                "target": "SQL",
                "sql_type": self._map_python_type_to_sql(metrics["detected_type"])
            }

        return schema_decisions

    def _map_python_type_to_sql(self, py_type):
        """
        Helper to map Python types to SQL types for CREATE/ALTER TABLE statements.
        """
        type_map = {
            'int': 'INT',
            'float': 'FLOAT',
            'bool': 'BOOLEAN',
            'str': 'VARCHAR(255)',  
            'NoneType': 'VARCHAR(255)'
        }
        # If unknown, default to TEXT to be safe
        return type_map.get(py_type, 'TEXT')

# --- Testing Block ---
if __name__ == "__main__":
    # Simulate stats coming from Analyzer
    test_stats = {
        "user_name": {"frequency_ratio": 1.0, "type_stability": "stable", "detected_type": "str", "is_nested": False},
        "age":       {"frequency_ratio": 0.95, "type_stability": "stable", "detected_type": "int", "is_nested": False},
        "bio":       {"frequency_ratio": 0.10, "type_stability": "stable", "detected_type": "str", "is_nested": False}, # Rare -> Mongo
        "config":    {"frequency_ratio": 1.0, "type_stability": "stable", "detected_type": "dict", "is_nested": True},   # Nested -> Mongo
        "score":     {"frequency_ratio": 0.9, "type_stability": "unstable", "detected_type": "mixed", "is_nested": False} # Drifting -> Mongo
    }

    classifier = Classifier()
    decisions = classifier.decide_schema(test_stats)
    
    import json
    print(json.dumps(decisions, indent=2))