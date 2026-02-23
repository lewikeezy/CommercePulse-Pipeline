import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_events_collection():  # <--- Make sure this name is EXACTLY this
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    db = client[os.getenv("MONGO_DB", "commercepulse")]
    return db["events_raw"]