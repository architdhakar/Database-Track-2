"""Field classification logic for routing data to SQL or MongoDB."""

class Classifier:
    def __init__(self, lower_threshold=0.75, upper_threshold=0.85, confidence_threshold=1000):
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.confidence_threshold = confidence_threshold
        self.common_fields = {'username', 'timestamp', 'sys_ingested_at'}
        self.previous_decisions = {}

    def decide_schema(self, stats):
        schema_decisions = {}

        for field, metrics in stats.items():
            
            if field in self.common_fields:
                schema_decisions[field] = {
                    "target": "BOTH",
                    "sql_type": self._map_python_type_to_sql(metrics["detected_type"], is_unique=False)
                }
                continue

            if metrics["is_nested"]:
                schema_decisions[field] = {"target": "MONGO"}
                continue
                
            if metrics["detected_type"] == 'NoneType':
                schema_decisions[field] = {"target": "MONGO"}
                continue

            if metrics["type_stability"] == "unstable":
                schema_decisions[field] = {"target": "MONGO"}
                continue

            freq = metrics["frequency_ratio"]
            previous_target = self.previous_decisions.get(field, {}).get("target", "MONGO")
            target = "MONGO"
            
            if previous_target == "SQL" or previous_target == "BOTH":
                if freq >= self.lower_threshold:
                    target = "SQL"
                else:
                    target = "MONGO"
            else:
                if freq >= self.upper_threshold:
                    target = "SQL"
                else:
                    target = "MONGO"

            is_unique = self._is_identifier_field(field, metrics)
            
            if target == "SQL":
                schema_decisions[field] = {
                    "target": "SQL",
                    "sql_type": self._map_python_type_to_sql(metrics["detected_type"], is_unique=is_unique),
                    "is_unique": is_unique
                }
            else:
                schema_decisions[field] = {"target": "MONGO"}
        
        self.previous_decisions.update(schema_decisions)
        return schema_decisions

    def _is_identifier_field(self, field, metrics):
        """Identifies true unique identifier fields vs high-cardinality measurement fields."""
        
        if metrics["detected_type"] in ['int', 'float', 'bool', 'NoneType']:
            return False
        
        if metrics["detected_type"] != 'str' or metrics["type_stability"] != "stable":
            return False
        
        field_count = metrics.get("count", 0)
        if field_count < self.confidence_threshold:
            return False
        
        unique_ratio = metrics.get("unique_ratio", 0)
        if unique_ratio < 0.98:
            return False
        
        field_lower = field.lower()
        identifier_patterns = ['_id', 'uuid', 'email', 'username', 'user_name']
        
        has_identifier_pattern = any(pattern in field_lower for pattern in identifier_patterns)
        
        # CONSERVATIVE DECISION:
        # Only mark as UNIQUE if it has identifier naming pattern
        # This prevents false positives on high-cardinality measurement fields
        # (e.g., purchase_value, ip_address, phone_number)
        return has_identifier_pattern

    def _map_python_type_to_sql(self, py_type, is_unique=False):
        """
        Helper to map Python types to SQL types for CREATE/ALTER TABLE statements.
        """
        if py_type == 'str':
            return 'VARCHAR(255)' if is_unique else 'TEXT'

        type_map = {
            'int': 'INT',
            'float': 'FLOAT',
            'bool': 'BOOLEAN',
            'NoneType': 'VARCHAR(255)',
            'datetime': 'DATETIME'
        }
        return type_map.get(py_type, 'TEXT')

    def export_decisions(self):
        """Export previous decisions for persistence across sessions."""
        import copy
        return copy.deepcopy(self.previous_decisions)

    def load_decisions(self, decisions):
        """Restore previous decisions from persisted metadata."""
        import copy
        if decisions:
            self.previous_decisions = copy.deepcopy(decisions)