"""Field classification logic for routing data to SQL or MongoDB."""

class Classifier:
    def __init__(self, lower_threshold=0.75, upper_threshold=0.85, confidence_threshold=1000):
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.confidence_threshold = confidence_threshold
        self.common_fields = {'username', 'timestamp', 'sys_ingested_at'}
        self.previous_decisions = {}
        self.ai_decision_cache = {}

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
        """Identifies true unique identifier fields vs high-cardinality measurement fields.
        Uses AI-enhanced detection with fallback to local rule-based logic."""
        
        # Try AI decision first (if API available)
        return self._ai_uniqueness_check(field, metrics)

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

    def _ai_uniqueness_check(self, field, metrics):
        """Use Groq AI to intelligently decide if field should be marked UNIQUE."""
        
        # Check cache first (avoid repeated API calls)
        if field in self.ai_decision_cache:
            return self.ai_decision_cache[field]
        
        try:
            from groq import Groq
            import os
            
            api_key = os.getenv('GROQ_API_KEY')
            if not api_key:
                raise ValueError("GROQ_API_KEY not set")
            
            client = Groq(api_key=api_key)
            
            print(f"[AI] Analyzing field '{field}' for UNIQUE constraint...")
            
            # Construct context-aware prompt
            prompt = f"""Database Field Analysis:

Field Name: {field}
Data Type: {metrics['detected_type']}
Frequency: {metrics['frequency_ratio']*100:.1f}% of records contain this field
Uniqueness: {metrics['unique_ratio']*100:.1f}% of values are unique
Sample Size: {metrics['count']} records analyzed

Context: This field will be stored in MySQL. Fields marked as UNIQUE get a UNIQUE constraint.

Identifier Fields (should be UNIQUE):
- User IDs (user_id, customer_id, account_number)
- Email addresses
- Transaction/Order IDs (order_id, transaction_ref, invoice_number)
- Product codes (sku, product_code)
- Usernames

NOT Identifier Fields (should NOT be UNIQUE):
- Measurements (purchase_value, price, amount)
- Contact info without unique constraint (phone, address)
- IP addresses
- Descriptions or content

Question: Should '{field}' be marked with UNIQUE constraint?

Answer ONLY with: YES or NO"""
            
            # Call Groq API with fast model
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=10
            )
            
            answer = response.choices[0].message.content.strip().upper()
            decision = 'YES' in answer
            
            print(f"[AI] Decision for '{field}': {'UNIQUE' if decision else 'NOT UNIQUE'}")
            
            # Cache the result
            self.ai_decision_cache[field] = decision
            
            return decision
            
        except Exception as e:
            # Fallback to local rule-based decision making
            print(f"[AI] API unavailable, using local decision for '{field}'")
            
            # Apply conservative rules locally
            if metrics["detected_type"] in ['int', 'float', 'bool', 'NoneType']:
                self.ai_decision_cache[field] = False
                return False
            
            if metrics["detected_type"] != 'str' or metrics["type_stability"] != "stable":
                self.ai_decision_cache[field] = False
                return False
            
            field_count = metrics.get("count", 0)
            if field_count < self.confidence_threshold:
                self.ai_decision_cache[field] = False
                return False
            
            unique_ratio = metrics.get("unique_ratio", 0)
            if unique_ratio < 0.98:
                self.ai_decision_cache[field] = False
                return False
            
            # Pattern matching for identifier fields
            field_lower = field.lower()
            identifier_patterns = ['_id', 'uuid', 'email', 'username', 'user_name']
            fallback_decision = any(pattern in field_lower for pattern in identifier_patterns)
            
            # Cache fallback decision
            self.ai_decision_cache[field] = fallback_decision
            
            return fallback_decision

    def export_decisions(self):
        """Export previous decisions for persistence across sessions."""
        import copy
        return copy.deepcopy(self.previous_decisions)

    def load_decisions(self, decisions):
        """Restore previous decisions from persisted metadata."""
        import copy
        if decisions:
            self.previous_decisions = copy.deepcopy(decisions)