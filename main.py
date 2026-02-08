import json
import os
import requests
import sseclient
from datetime import datetime

from core.normalizer import Normalizer
from core.analyzer import Analyzer
from core.classifier import Classifier
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler


BATCH_SIZE = 50
METADATA_FILE = "metadata/schema_map.json"
# FastAPI endpoint: /record/{count}
DATA_STREAM_URL = "http://127.0.0.1:8000/record/5000" 

def load_metadata():
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            # If file is empty or corrupted, return empty dict
            return {}
    return {}

def save_metadata(stats):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, 'w') as f:
        json.dump(stats, f, indent=4) # No custom encoder needed anymore!

def fetch_stream_data(url):
    """
    Connects to the FastAPI SSE stream and yields records one by one.
    """
    print(f"Connecting to data stream at {url}...")
    try:
        response = requests.get(url, stream=True)
        client = sseclient.SSEClient(response)
        
        for event in client.events():
            if event.data:
                yield json.loads(event.data)
                
    except requests.exceptions.ConnectionError:
        print(f"Error: Could not connect to {url}. Is the simulation server running?")
        return

def main():
    print("Starting Adaptive Ingestion Engine...")

    normalizer = Normalizer()
    analyzer = Analyzer()
    classifier = Classifier(freq_threshold=0.8)
    
    # Connect to Databases
    sql_handler = SQLHandler() 
    mongo_handler = MongoHandler()
    
    print("Connecting to storage backends...")
    sql_handler.connect()

    # Load Persistence
    saved_stats = load_metadata()
    if saved_stats:
        analyzer.load_stats(saved_stats)
        print("Loaded existing metadata stats.")

    # Processing Loop
    buffer = []
    
    try:
        # We iterate over the live stream from FastAPI
        for raw_record in fetch_stream_data(DATA_STREAM_URL):
            buffer.append(raw_record)

            # Process only when buffer is full
            if len(buffer) >= BATCH_SIZE:
                print(f"Processing batch of {len(buffer)} records...")

                # Step 1: Cleaning of data
                clean_batch = normalizer.normalize_batch(buffer)

                # Step 2: Analysis
                analyzer.analyze_batch(clean_batch)
                stats = analyzer.get_schema_stats()

                # Step 3: Classification
                schema_decisions = classifier.decide_schema(stats)
                
                # Step 4: Routing & Persistence 
                
                # A. Evolve SQL Schema
                sql_handler.update_schema(schema_decisions)

                # B. Split Records
                sql_inserts = []
                mongo_inserts = []

                for record in clean_batch:
                    sql_rec = {}
                    mongo_rec = {}

                    # Mandatory Keys -> BOTH
                    for key in ['user_name', 't_stamp', 'sys_ingested_at']:
                        if key in record:
                            sql_rec[key] = record[key]
                            mongo_rec[key] = record[key]

                    # Dynamic Routing
                    for key, value in record.items():
                        if key in ['user_name', 't_stamp', 'sys_ingested_at']:
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

                # C. Write
                if sql_inserts:
                    sql_handler.insert_batch(sql_inserts)
                if mongo_inserts:
                    mongo_handler.insert_batch(mongo_inserts)

                # D. Save State
                save_metadata(analyzer.export_stats())
                buffer = []

    except KeyboardInterrupt:
        print("\nStopping engine...")
    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        sql_handler.close()
        mongo_handler.close()
        print("Database connections closed.")

if __name__ == "__main__":
    main()