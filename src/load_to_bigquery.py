import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

def load_silver_to_bigquery():
    # 1. Path to your cleaned data
    file_path = "data/silver_events_cleaned.csv"
    if not os.path.exists(file_path):
        print(" Cleaned CSV not found. Please run 'python src/transform_data.py' first!")
        return
    
    # 2. Load the CSV into a DataFrame
    df = pd.read_csv(file_path)
    
    # 3. Initialize the BigQuery Client
    client = bigquery.Client()
    
    # 4. Set your destination table details
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = "commerce_pulse_silver"
    table_name = "events_cleaned"

    # combines intoformat: project.dataset.table
    destination_table = f"{project_id}.{dataset_id}.{table_name}"

    # 5. Configure the Upload Job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
         # Automatically creates the schema based on csv
        autodetect=True, 
    )

    print(f"🚀 Starting upload: {len(df)} rows to {dataset_id}.{table_name}...")
    
    try:
        # 6. Push to BigQuery
        job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        job.result() 
        print(f" Success! Data is now live in BigQuery Warehouse.")
        
    except Exception as e:
        print(f" Failed to upload: {e}")

if __name__ == "__main__":
    load_silver_to_bigquery()