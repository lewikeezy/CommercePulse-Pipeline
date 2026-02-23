import pandas as pd
from config import get_events_collection
from datetime import datetime

def normalize_vendor_data(row):
    """
    Logic to handle schema drift across different vendors.
    Maps various field names to a unified standard.
    """
    vendor = row.get('vendor')
    payload = row.get('payload', {})
    
    # Initialize a standard record
    standard = {
        'event_id': row.get('event_id'),
        'event_type': row.get('event_type'),
        'event_time': row.get('event_time'),
        'vendor': vendor,
        'ingested_at': row.get('ingested_at')
    }

    # Unified Mapping Logic
    if vendor == 'vendor_a':
        standard['order_id'] = payload.get('orderRef')
        standard['amount'] = payload.get('total') or payload.get('totalAmount')
        standard['customer_email'] = payload.get('customer', {}).get('email') or payload.get('buyer', {}).get('email')
        
    elif vendor == 'vendor_b':
        standard['order_id'] = payload.get('order_id')
        standard['amount'] = payload.get('totalAmount') or payload.get('amountPaid')
        standard['customer_email'] = payload.get('buyerEmail')

    elif vendor == 'vendor_c':
        # Handles nested objects like {"order": {"id": "..."}}
        order_info = payload.get('order', {})
        standard['order_id'] = order_info.get('id') if isinstance(order_info, dict) else order_info
        standard['amount'] = payload.get('amount') or payload.get('amt')
        standard['customer_email'] = payload.get('email')

    return standard

def run_transformation():
    collection = get_events_collection()
    
    # 1. Load raw data from MongoDB into Pandas
    raw_data = list(collection.find())
    if not raw_data:
        print("No data found in MongoDB to transform.")
        return

    df_raw = pd.DataFrame(raw_data)
    
    # 2. Apply normalization logic
    print("Normalizing vendor schemas...")
    normalized_list = df_raw.apply(normalize_vendor_data, axis=1).tolist()
    df_clean = pd.DataFrame(normalized_list)

    # 3. Data Cleaning
    # Ensure event_time is a proper datetime object
    df_clean['event_time'] = pd.to_datetime(df_clean['event_time'], errors='coerce')
    
    # Remove duplicates based on event_id (Final safety check)
    df_clean = df_clean.drop_duplicates(subset=['event_id'])

    print(f"Transformation complete. {len(df_clean)} records ready for BigQuery.")
    ls
    # For now, let's save to CSV to verify the work
    df_clean.to_csv("data/silver_events_cleaned.csv", index=False)
    print("Cleaned data saved to data/silver_events_cleaned.csv")

if __name__ == "__main__":
    run_transformation()