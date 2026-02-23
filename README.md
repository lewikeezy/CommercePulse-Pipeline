# CommercePulse: End-to-End Data Pipeline

##
CommercePulse Ltd is a rapidly growing e-commerce aggregation company operating across multiple African markets. The company partners with independent online vendors, payment providers, and logistics companies to centralize order processing, payments, refunds, and delivery tracking into a single platform.

CommercePulse does not sell products directly. Instead, it provides vendors with:

    Unified checkout and payments
    Centralized logistics tracking
    Cross-vendor analytics and reporting

As transaction volume has grown, leadership has identified serious gaps in the company’s data infrastructure

## Project Overview
This project demonstrates a robust Data Engineering pipeline designed to handle high-volume, inconsistent e-commerce data from multiple vendors. I have implemented a **Medallion Architecture** to ingest, transform, and load data from local sources into a Cloud Data Warehouse.

### The Challenge
* **Vendor Inconsistency:** Different JSON schemas for Order and Payment events.
* **Data Quality Issues:** Out-of-order arrivals, duplicates, and schema drift.
* **Scale:** Historical bootstrap data combined with live generated JSONL event streams.

---

##  Data Architecture (Medallion Layers)

### 1. Bronze Layer (Raw Ingestion)
* **Source:** Historical JSON arrays (`data/bootstrap/`) and live JSONL events.
* **Storage:** **MongoDB** (`events_raw` collection).
* **Process:** Data is ingested "as-is" to preserve the raw truth before any modification.

### 2. Silver Layer (Transformation)
* **Tool:** **Python (Pandas)**
* **Process:** * Unified inconsistent vendor fields (e.g., mapping `totalAmount`, `amt`, and `total` to a single `amount` field).
    * Removed duplicates based on unique event IDs.
    * Standardized timestamps for cross-vendor analysis.
* **Output:** `data/silver_events_cleaned.csv`

### 3. Gold Layer (Cloud Warehouse)
* **Storage:** **Google BigQuery**
* **Process:** Automated loading of cleaned Silver data into a cloud environment.
* **Outcome:** Analytics-ready tables for SQL querying and business intelligence.

---

##  Setup & Execution

### Prerequisites
* Python 3.x
* MongoDB (Local instance)
* Google Cloud Project (BigQuery enabled)

### Steps to Run the Pipeline
1. **Bootstrap Data:** Ingest historical records into MongoDB.
   `python src/bootstrap_history.py`

2. Generate Live Events: Create simulated daily traffic.
   `python src/live_event_generator.py --events 2000`
3. Transform Data: Clean and normalize the raw MongoDB data
   `python src/transform_data.py`
4. Load to Cloud: Push the cleaned data to BigQuery
   `python src/load_to_bigquery.py`

## Sample Analytics Query

```SELECT 
    vendor, 
    COUNT(*) as total_transactions, 
    SUM(amount) as total_revenue,
    AVG(amount) as average_order_value
FROM `your-project-id.commerce_pulse_silver.events_cleaned`
GROUP BY vendor
ORDER BY total_revenue DESC```

