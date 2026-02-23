import json
import hashlib
from datetime import datetime, timezone
from config import get_events_collection
from pymongo import UpdateOne

def generate_id(record):
    # Creates a unique ID so you don't get duplicates
    payload_str = json.dumps(record, sort_keys=True)
    return hashlib.sha256(payload_str.encode()).hexdigest()

def bootstrap():
    collection = get_events_collection()
    # List of your 2023 files
    files = {
        "data/bootstrap/orders_2023.json": "historical_order",
        "data/bootstrap/payments_2023.json": "historical_payment"
    }
    
    for path, e_type in files.items():
        with open(path, 'r') as f:
            data = json.load(f)
            ops = []
            for item in data:
                e_id = generate_id(item)
                event = {
                    "event_id": e_id,
                    "event_type": e_type,
                    "payload": item,
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                }
                ops.append(UpdateOne({"event_id": e_id}, {"$set": event}, upsert=True))
            
            if ops:
                collection.bulk_write(ops)
                print(f"Finished {path}")

if __name__ == "__main__":
    bootstrap()