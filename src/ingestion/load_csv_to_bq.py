import os
import glob
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# ==========================================
# Configuration & Authentication
# ==========================================
# Assumes the script is run from the project root directory
CREDENTIALS_PATH = "credentials/gcp-key.json"
DATA_DIR = "data"
DATASET_ID = "olist_raw"

# Set the environment variable for GCP authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

def load_all_csvs_to_bigquery():
    """Iterates through the data directory and loads all CSVs to BigQuery."""
    
    # Initialize the BigQuery client
    try:
        client = bigquery.Client()
        project_id = client.project
        print(f"Successfully authenticated to GCP Project: {project_id}")
    except Exception as e:
        print(f"Authentication failed. Please check your JSON key path. Error: {e}")
        return

    # Find all CSV files in the data directory
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in '{DATA_DIR}/'. Please ensure the files are unzipped.")
        return

    print(f"Found {len(csv_files)} files. Starting ingestion process...\n")

    # Configure the load job
    # WRITE_TRUNCATE replaces the table if it already exists (idempotent run)
    # autodetect=True tells BigQuery to infer the schema (strings, integers, floats, timestamps)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    for file_path in csv_files:
        # Extract the filename without the extension to use as the table name
        file_name = os.path.basename(file_path)
        table_name = file_name.replace(".csv", "")
        
        # Construct the full BigQuery table ID: project.dataset.table
        table_id = f"{project_id}.{DATASET_ID}.{table_name}"
        
        print(f"Reading {file_name} into pandas...")
        df = pd.read_csv(file_path)
        
        print(f"Uploading {len(df)} rows to BigQuery table: {table_name}...")
        try:
            # Load the dataframe into BigQuery
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the load job to complete
            print(f"✅ Success: {table_name}\n")
            
        except GoogleAPIError as e:
            print(f"❌ Failed to load {table_name}. Error: {e}\n")

if __name__ == "__main__":
    load_all_csvs_to_bigquery()