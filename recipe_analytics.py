from google.cloud import bigquery, storage
from google.oauth2 import service_account
import os

# --- CONFIGURATION ---
project_id = "fir-f8d56"  
bucket_name = "saurav_recipe_backup"  
dataset_id = "recipe_analytics"

# Authenticate
cred = service_account.Credentials.from_service_account_file("serviceaccount.json")
bq_client = bigquery.Client(credentials=cred, project=project_id)
storage_client = storage.Client(credentials=cred, project=project_id)


def upload_and_load():
    print(f" Starting Upload & Load for Project: {project_id}...")

    # --- STEP 1: UPLOAD LOCAL CSVs TO BUCKET ---
    print(f"\n  Step 1: Uploading local files to gs://{bucket_name}...")

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as e:
        print(f" CRITICAL ERROR: Could not find bucket '{bucket_name}'.")
        print("   Make sure the bucket exists in Google Cloud Console.")
        print(f"   Error details: {e}")
        return

    files_map = {
        "users": "users.csv",
        "recipes": "recipe.csv",
        "ingredients": "ingredients.csv",
        "steps": "steps.csv",
        "interactions": "interactions.csv",
    }

    for table_name, filename in files_map.items():
        if not os.path.exists(filename):
            print(f"   Skipping {filename}: File not found on your computer.")
            continue

        blob = bucket.blob(f"backups/{filename}")
        blob.upload_from_filename(filename)
        print(f" Uploaded: {filename}")

    # --- STEP 2: LOAD INTO BIGQUERY ---
    print(f"\n Step 2: Loading into BigQuery Dataset '{dataset_id}'...")

    # Create Dataset
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        bq_client.get_dataset(dataset_ref)
        print(f" Dataset '{dataset_id}' exists.")
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  
        bq_client.create_dataset(dataset)
        print(f"  Created Dataset: {dataset_id}")

    # Job Config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load Tables
    for table_name, filename in files_map.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        uri = f"gs://{bucket_name}/backups/{filename}"

        try:
            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()  # Wait for completion

            table = bq_client.get_table(table_id)
            print(f"  Loaded table '{table_name}': {table.num_rows} rows.")
        except Exception as e:
            print(f"  Failed to load '{table_name}': {e}")

    print("\n SUCCESS: Analytics Data is ready in BigQuery!")


if __name__ == "__main__":
    upload_and_load()
