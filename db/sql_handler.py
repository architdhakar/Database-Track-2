import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

class SQLHandler:
    def __init__(self):
        self.config = {
            'host': os.getenv("SQL_HOST"),
            'port': int(os.getenv("SQL_PORT", 3306)),
            'user': os.getenv("SQL_USER"),
            'password': os.getenv("SQL_PASSWORD"),
            'database': os.getenv("SQL_DB_NAME")
        }
        self.table_name = "structured_data"
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
            self._create_base_table()
            print("[SQL] Connected to Remote Database successfully.")
        except mysql.connector.Error as err:
            print(f"[SQL Error] Connection failed: {err}")

    def _create_base_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255),
            timestamp DATETIME,
            sys_ingested_at DATETIME,
            INDEX (sys_ingested_at),
            INDEX (username)
        )
        """
        self.cursor.execute(query)
        self.conn.commit()
        self._refresh_schema_cache()

    def _refresh_schema_cache(self):
        self.cursor.execute(f"DESCRIBE {self.table_name}")
        self.existing_cols = {row[0] for row in self.cursor.fetchall()}

    def update_schema(self, schema_decisions):
        if not hasattr(self, 'existing_cols'):
            self._refresh_schema_cache()

        for field, decision in schema_decisions.items():
            if decision['target'] in ['SQL', 'BOTH'] and field not in self.existing_cols:
                sql_type = decision.get('sql_type', 'TEXT')
                is_unique = decision.get('is_unique', False)
                
                constraint = " UNIQUE" if is_unique else ""
                print(f"[SQL Handler] Evolving Schema: Adding column '{field}' as {sql_type}{constraint}")
                
                alter_query = f"ALTER TABLE {self.table_name} ADD COLUMN {field} {sql_type}{constraint}"
                try:
                    self.cursor.execute(alter_query)
                    self.existing_cols.add(field)
                except mysql.connector.Error as err:
                    print(f"Failed to add column {field}: {err}")
        
        self.conn.commit()

    def insert_batch(self, records):
        if not records:
            return

        if not hasattr(self, 'existing_cols'):
            self._refresh_schema_cache()

        valid_columns = self.existing_cols

        for record in records:
            filtered_rec = {k: v for k, v in record.items() if k in valid_columns}
            
            if not filtered_rec:
                continue

            columns = ', '.join(filtered_rec.keys())
            placeholders = ', '.join(['%s'] * len(filtered_rec))
            values = list(filtered_rec.values())
            
            sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            
            try:
                self.cursor.execute(sql, values)
            except mysql.connector.Error as err:
                print(f"Insert Error: {err}")
        
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()