"""
Classifier for user events.
Decision Logic
"""
class Classifier:
    def __init__(self, lower_threshold=0.75, upper_threshold=0.85):
        """
        Initializes the Classifier with decision thresholds.
        :param lower_threshold: Fields dropping below this frequency revert to Mongo.
        :param upper_threshold: Fields exceeding this frequency promote to SQL.
        """
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.common_fields = {'username', 'timestamp', 'sys_ingested_at'}
        self.previous_decisions = {}

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
            if metrics["is_nested"]:
                schema_decisions[field] = {"target": "MONGO"}
                continue
                
            # CRITERIA B: Is it purely NoneType? -> MongoDB (Prevent empty SQL columns)
            if metrics["detected_type"] == 'NoneType':
                schema_decisions[field] = {"target": "MONGO"}
                continue

            # CRITERIA C: Is it unstable (Type Drifting)? -> MongoDB
            if metrics["type_stability"] == "unstable":
                schema_decisions[field] = {"target": "MONGO"}
                continue

            # CRITERIA D: Frequency & Hysteresis
            freq = metrics["frequency_ratio"]
            previous_target = self.previous_decisions.get(field, {}).get("target", "MONGO")
            
            target = "MONGO" # Default
            
            # Hysteresis Logic:
            if previous_target == "SQL" or previous_target == "BOTH":
                # Only downgrade if it drops drastically (below lower threshold)
                if freq >= self.lower_threshold:
                    target = "SQL"
                else:
                    target = "MONGO"
            else:
                # Upgrade only if it exceeds upper threshold
                if freq >= self.upper_threshold:
                    target = "SQL"
                else:
                    target = "MONGO"

            # Check for Uniqueness (Assignment Requirement)
            # Heuristic: Only strings or UUID-like strings should typically be unique.
            # Numbers (int, float) are often metrics and should rarely be UNIQUE in this context.
            is_unique = metrics.get("unique_ratio", 0) >= 1.0
            if metrics["detected_type"] in ['int', 'float', 'bool']:
                is_unique = False
            
            if target == "SQL":
                schema_decisions[field] = {
                    "target": "SQL",
                    "sql_type": self._map_python_type_to_sql(metrics["detected_type"]),
                    "is_unique": is_unique
                }
            else:
                schema_decisions[field] = {"target": "MONGO"}
        
        # Update state
        self.previous_decisions.update(schema_decisions)
        return schema_decisions

    def _map_python_type_to_sql(self, py_type):
        """
        Helper to map Python types to SQL types for CREATE/ALTER TABLE statements.
        """
        type_map = {
            'int': 'INT',
            'float': 'FLOAT',
            'bool': 'BOOLEAN',
            'str': 'TEXT',  
            'NoneType': 'VARCHAR(255)',
            'datetime': 'DATETIME'
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