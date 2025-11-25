import os
from google.cloud import bigquery


def load_to_bigquery(data, context):
    """
    Gen 1 Cloud Function triggered by a change to a Cloud Storage bucket.
    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    """
    file_name = data["name"]
    bucket_name = data["bucket"]

    # Configuration
    PROJECT_ID = os.environ.get("GCP_PROJECT")
    DATASET_ID = "recipe_analytics"

    # Only process files in the 'backups/' folder and CSVs
    if not file_name.startswith("backups/") or not file_name.endswith(".csv"):
        print(f"Skipping file: {file_name}")
        return

    # Determine Table Name
    table_name = os.path.basename(file_name).replace(".csv", "")
    if table_name == "recipe":
        table_name = "recipes"

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    uri = f"gs://{bucket_name}/{file_name}"

    print(f"🚀 Processing {file_name} -> Loading into {table_id}...")

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for job to complete

        table = client.get_table(table_id)
        print(f"✅ Loaded {table.num_rows} rows into {table_id}.")

    except Exception as e:
        print(f"❌ Error loading {table_id}: {e}")
        raise e
