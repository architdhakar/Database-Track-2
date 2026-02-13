import time
import queue
import threading

class Router:
    def __init__(self, sql_handler, mongo_handler):
        self.sql_handler = sql_handler
        self.mongo_handler = mongo_handler
        self.previous_decisions = {} 

    def process_batch(self, batch, schema_decisions):
        """
        Routes a batch of records based on schema decisions.
        Also handles migration if a decision changes (SQL <-> Mongo).
        """
        # 1. Check for Schema Drift / Migration Needs
        self._check_and_migrate(schema_decisions)
        
        # 2. Update Persisted Schema Decisions
        self.previous_decisions.update(schema_decisions)

        # 3. Standard Routing (Same as before, but encapsulated here)
        sql_inserts = []
        mongo_inserts = []

        for record in batch:
            sql_rec = {}
            mongo_rec = {}

            # Mandatory Keys -> BOTH
            for key in ['username', 'timestamp', 'sys_ingested_at']:
                if key in record:
                    sql_rec[key] = record[key]
                    mongo_rec[key] = record[key]

            # Dynamic Routing
            for key, value in record.items():
                if key in ['username', 'timestamp', 'sys_ingested_at']:
                    continue
                
                decision = schema_decisions.get(key, {"target": "MONGO"})
                target = decision['target']

                if target == 'SQL':
                    sql_rec[key] = value
                elif target == 'MONGO':
                    mongo_rec[key] = value
                elif target == 'BOTH':
                    sql_rec[key] = value
                    mongo_rec[key] = value

            sql_inserts.append(sql_rec)
            mongo_inserts.append(mongo_rec)

        # 4. Bulk Write
        if sql_inserts:
            self.sql_handler.insert_batch(sql_inserts)
        if mongo_inserts:
            self.mongo_handler.insert_batch(mongo_inserts)

    def _check_and_migrate(self, new_decisions):
        """
        Compares new decisions with previous ones to detect if a field 
        moved from SQL to Mongo (Type Drift) or Mongo to SQL (Stabilization).
        """
        for field, decision in new_decisions.items():
            new_target = decision['target']
            
            # If we haven't seen this field before, just track it
            if field not in self.previous_decisions:
                continue

            old_target = self.previous_decisions[field]['target']

            # Case A: SQL -> Mongo (Drift Detected)
            if old_target == 'SQL' and new_target == 'MONGO':
                print(f"[Router] MIGRATION ALERT: Field '{field}' drifted from SQL to MongoDB. Migrating data...")
                self._migrate_sql_to_mongo(field)

            # Case B: Mongo -> SQL (Stabilization)
            # (Optional Implementation: Usually risky to automate without downtime)
            elif old_target == 'MONGO' and new_target == 'SQL':
                # print(f"[Router] Info: Field '{field}' stabilized. Future data goes to SQL.")
                pass

    def _migrate_sql_to_mongo(self, field):
        """
        Moves data from SQL column to MongoDB documents and drops the SQL column.
        """
        try:
            # 1. Fetch data from SQL (Join Key: sys_ingested_at + username)
            query = f"SELECT username, sys_ingested_at, {field} FROM {self.sql_handler.table_name} WHERE {field} IS NOT NULL"
            self.sql_handler.cursor.execute(query)
            rows = self.sql_handler.cursor.fetchall()

            if not rows:
                print(f"[Router] No existing data in SQL for '{field}'. seamless switch.")
                # Just drop the column
                self.sql_handler.cursor.execute(f"ALTER TABLE {self.sql_handler.table_name} DROP COLUMN {field}")
                self.sql_handler.conn.commit()
                return

            print(f"[Router] Migrating {len(rows)} records for '{field}' from SQL to Mongo...")

            # 2. Update MongoDB
            # note: This is slow for millions of records, but fine for assignment scale.
            for row in rows:
                username, sys_time, value = row
                # We use sys_ingested_at and username as composite key to find the mongo doc
                filter_query = {"username": username, "sys_ingested_at": sys_time.isoformat() if hasattr(sys_time, 'isoformat') else sys_time}
                update_query = {"$set": {field: value}}
                
                self.mongo_handler.collection.update_one(filter_query, update_query, upsert=True)

            # 3. Drop Column from SQL
            print(f"[Router] Dropping column '{field}' from SQL...")
            self.sql_handler.cursor.execute(f"ALTER TABLE {self.sql_handler.table_name} DROP COLUMN {field}")
            self.sql_handler.conn.commit()
            
            # Update local cache in SQL Handler so it knows column is gone
            if hasattr(self.sql_handler, 'existing_cols'):
                self.sql_handler.existing_cols.discard(field)

            print(f"[Router] Migration of '{field}' complete.")

        except Exception as e:
            print(f"[Router] MIGRATION FAILED for '{field}': {e}")
