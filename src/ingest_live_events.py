import json
import glob
from config import get_events_collection
from pymongo import UpdateOne

def ingest_live():
    collection = get_events_collection()
    
    # Looks up .jsonl files generated in the live_events folders
    live_files = glob.glob("data/live_events/**/*.jsonl", recursive=True)
    
    for file_path in live_files:
        print(f"Ingesting: {file_path}")
        bulk_ops = []
        
        with open(file_path, 'r') as f:
            for line in f:
                event = json.loads(line)
                bulk_ops.append(
                    UpdateOne({"event_id": event["event_id"]}, {"$set": event}, upsert=True)
                )
        
        if bulk_ops:
            collection.bulk_write(bulk_ops)

if __name__ == "__main__":
    ingest_live()