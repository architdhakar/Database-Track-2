import time
import queue
import threading

class Router:
    def __init__(self, sql_handler, mongo_handler):
        self.sql_handler = sql_handler
        self.mongo_handler = mongo_handler
        self.previous_decisions = {}

    def process_batch(self, batch, schema_decisions):
        self._check_and_migrate(schema_decisions)
        self.previous_decisions.update(schema_decisions)
        sql_inserts = []
        mongo_inserts = []

        for record in batch:
            sql_rec = {}
            mongo_rec = {}

            for key in ['username', 'timestamp', 'sys_ingested_at']:
                if key in record:
                    sql_rec[key] = record[key]
                    mongo_rec[key] = record[key]

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

        if sql_inserts:
            self.sql_handler.insert_batch(sql_inserts)
        if mongo_inserts:
            self.mongo_handler.insert_batch(mongo_inserts)

    def _check_and_migrate(self, new_decisions):
        for field, decision in new_decisions.items():
            new_target = decision['target']
            
            if field not in self.previous_decisions:
                continue

            old_target = self.previous_decisions[field]['target']

            if old_target == 'SQL' and new_target == 'MONGO':
                print(f"[Router] MIGRATION: '{field}' drifted from SQL to MongoDB. Migrating data...")
                self._migrate_sql_to_mongo(field)

            elif old_target == 'MONGO' and new_target == 'SQL':
                pass

    def _migrate_sql_to_mongo(self, field):
        try:
            query = f"SELECT username, sys_ingested_at, {field} FROM {self.sql_handler.table_name} WHERE {field} IS NOT NULL"
            self.sql_handler.cursor.execute(query)
            rows = self.sql_handler.cursor.fetchall()

            if not rows:
                print(f"[Router] No existing data for '{field}'. Dropping column.")
                self.sql_handler.cursor.execute(f"ALTER TABLE {self.sql_handler.table_name} DROP COLUMN {field}")
                self.sql_handler.conn.commit()
                return

            print(f"[Router] Migrating {len(rows)} records...")

            from pymongo import UpdateOne
            bulk_ops = []
            for row in rows:
                username, sys_time, value = row
                filter_query = {
                    "username": username, 
                    "sys_ingested_at": sys_time.isoformat() if hasattr(sys_time, 'isoformat') else sys_time
                }
                bulk_ops.append(UpdateOne(filter_query, {"$set": {field: value}}, upsert=True))

            if bulk_ops:
                self.mongo_handler.collection.bulk_write(bulk_ops)

            print(f"[Router] Dropping column '{field}'...")
            self.sql_handler.cursor.execute(f"ALTER TABLE {self.sql_handler.table_name} DROP COLUMN {field}")
            self.sql_handler.conn.commit()
            
            if hasattr(self.sql_handler, 'existing_cols'):
                self.sql_handler.existing_cols.discard(field)

            print(f"[Router] Migration complete.")

        except Exception as e:
            print(f"[Router] MIGRATION FAILED for '{field}': {e}")

    def export_decisions(self):
        """Export previous decisions for persistence across sessions."""
        import copy
        return copy.deepcopy(self.previous_decisions)

    def load_decisions(self, decisions):
        """Restore previous decisions from persisted metadata."""
        import copy
        if decisions:
            self.previous_decisions = copy.deepcopy(decisions)
