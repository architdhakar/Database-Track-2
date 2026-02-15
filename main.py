import json
import os
import requests
import sseclient
import threading
import queue
import time
from datetime import datetime

from core.normalizer import Normalizer
from core.analyzer import Analyzer
from core.classifier import Classifier
from core.query_engine import QueryEngine
from core.router import Router
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler

BATCH_SIZE = 50
METADATA_FILE = "metadata/schema_map.json"
DATA_STREAM_URL = "http://127.0.0.1:8000/record/5000"
MAX_QUEUE_SIZE = 1000
STOP_EVENT = threading.Event()

def load_metadata():
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_metadata(stats):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, 'w') as f:
        json.dump(stats, f, indent=4)

def ingest_worker(raw_queue, data_url):
    print(f"[Ingestor] Connecting to data stream at {data_url}...")
    normalizer = Normalizer()
    
    try:
        response = requests.get(data_url, stream=True)
        client = sseclient.SSEClient(response)
        
        for event in client.events():
            if STOP_EVENT.is_set():
                break
            
            if event.data:
                try:
                    raw_record = json.loads(event.data)
                    clean_record = normalizer.normalize_record(raw_record)
                    
                    try:
                        raw_queue.put(clean_record, timeout=1) 
                    except queue.Full:
                        while raw_queue.full() and not STOP_EVENT.is_set():
                            time.sleep(0.1)
                        if not STOP_EVENT.is_set():
                            raw_queue.put(clean_record)

                except json.JSONDecodeError:
                    continue
                    
    except Exception as e:
        print(f"[Ingestor] Error: {e}")
    finally:
        print("[Ingestor] Thread stopping.")

def process_worker(raw_queue, write_queue, analyzer, classifier):
    print("[Processor] Worker started.")
    buffer = []
    
    while not STOP_EVENT.is_set() or not raw_queue.empty():
        try:
            record = raw_queue.get(timeout=1)
            buffer.append(record)
            raw_queue.task_done()
        except queue.Empty:
            pass

        if len(buffer) >= BATCH_SIZE or (STOP_EVENT.is_set() and buffer):
            if not buffer:
                continue
            
            try:
                analyzer.analyze_batch(buffer)
                stats = analyzer.get_schema_stats()
                schema_decisions = classifier.decide_schema(stats)

                payload = {
                    "batch": buffer,
                    "decisions": schema_decisions,
                    "stats": analyzer.export_stats()
                }
                write_queue.put(payload)
            except Exception as e:
                print(f"[Processor] Error: {e}")
            
            buffer = []
    
    print("[Processor] Thread stopping.")

def router_worker(write_queue, router):
    print("[Router] Worker started.")
    
    while not STOP_EVENT.is_set() or not write_queue.empty():
        try:
            payload = write_queue.get(timeout=1)
            batch = payload['batch']
            decisions = payload['decisions']
            
            router.sql_handler.update_schema(decisions)
            router.process_batch(batch, decisions)
            save_metadata(payload['stats'])
            
            write_queue.task_done()
            
        except queue.Empty:
            pass
        except Exception as e:
            print(f"[Router] Error: {e}")

    print("[Router] Thread stopping.")

def main():
    print("="*60)
    print("  ADAPTIVE INGESTION ENGINE")
    print("="*60)
    
    print("\n[1/4] Checking data stream availability...")
    try:
        response = requests.get(DATA_STREAM_URL.replace('/record/5000', '/'), timeout=2)
        print("      ✓ Data stream server is running")
    except requests.exceptions.RequestException:
        print("\n⚠️  WARNING: Simulation server not detected!")
        print("    Start it first: uvicorn simulation_code:app --reload --port 8000\n")
        return

    print("\n[2/4] Initializing components...")
    raw_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
    write_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
    
    analyzer = Analyzer()
    classifier = Classifier(lower_threshold=0.75, upper_threshold=0.85)
    
    sql_handler = SQLHandler() 
    mongo_handler = MongoHandler()
    router = Router(sql_handler, mongo_handler)
    
    print("\n[3/4] Connecting to databases...")
    try:
        sql_handler.connect()
        print("      ✓ MySQL connected")
    except Exception as e:
        print(f"      ✗ MySQL connection failed: {e}")
        return


    saved_stats = load_metadata()
    if saved_stats:
        analyzer.load_stats(saved_stats)
        print(f"      ✓ Loaded metadata ({len(saved_stats.get('field_stats', {}))} fields tracked)")
    else:
        print("      ℹ Starting fresh (no previous metadata)")

    print("\n[4/4] Starting worker threads...")
    t_ingest = threading.Thread(target=ingest_worker, args=(raw_queue, DATA_STREAM_URL))
    t_process = threading.Thread(target=process_worker, args=(raw_queue, write_queue, analyzer, classifier))
    t_router = threading.Thread(target=router_worker, args=(write_queue, router))

    t_ingest.start()
    t_process.start()
    t_router.start()

    query_engine = QueryEngine(analyzer, raw_queue)

    print("\n" + "="*60)
    print("  SYSTEM READY")
    print("="*60)
    print("\nAvailable Commands:")
    print("  • status           - Show system uptime and processing statistics")
    print("  • stats <field>    - Display detailed analysis for a specific field")
    print("  • all_stats        - View statistics for all tracked fields")
    print("  • queue            - Check current queue sizes")
    print("  • help             - Show detailed command help")
    print("  • exit             - Shut down the system gracefully\n")
    
    try:
        while True:
            user_input = input(">> ")
            if user_input.strip().lower() == "exit":
                print("Initiating shutdown...")
                STOP_EVENT.set()
                break
            
            response = query_engine.process_command(user_input)
            print(response)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupt received.")
        STOP_EVENT.set()
    finally:
        print("Stopping worker threads...")
        t_ingest.join()
        t_process.join()
        t_router.join()
        
        sql_handler.close()
        mongo_handler.close()
        print("✓ Shutdown complete.\n")

if __name__ == "__main__":
    main()