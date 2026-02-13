# Mongo db connection
import pymongo
import os 
from dotenv import load_dotenv
load_dotenv()

class MongoHandler:
    def __init__(self):
        # Fetch from environment
        uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME", "adaptive_db")
        
        if not uri:
            raise ValueError("MONGO_URI not found in .env file")

        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db["unstructured_data"]

    def insert_batch(self, records):
        """
        Inserts a batch of records into MongoDB using bulk write.
        """
        if not records:
            return

        # Ensure we are not inserting empty records (except for mandatory keys)
        valid_records = []
        for rec in records:
            # We only insert if there is 'extra' data beyond the basic join keys.
            valid_records.append(rec)

        if valid_records:
            try:
                self.collection.insert_many(valid_records, ordered=False)
                # print(f"[Mongo Handler] Inserted {len(valid_records)} documents.")
            except pymongo.errors.BulkWriteError as bwe:
                print(f"[Mongo Handler] Bulk Write Error: {bwe.details}")
            except Exception as e:
                print(f"[Mongo Handler] Insert Error: {e}")

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()