import functions_framework
from google.cloud import firestore  # Use the direct client
from google.cloud import storage
import csv
import io
import os


# Triggered by HTTP request
@functions_framework.http
def run_etl(request):
    # 1. Configuration
    BUCKET_NAME = os.environ.get("BUCKET_NAME")
    PROJECT_ID = os.environ.get("GCP_PROJECT")  # Cloud Functions set this automatically

    if not BUCKET_NAME:
        return "Error: BUCKET_NAME env var missing.", 500

    print(f"🚀 Starting ETL for Project: {PROJECT_ID} -> DB: 'recipe'")

    try:
        # --- CONNECT TO SPECIFIC DATABASE ---
        # This is the critical fix. We explicitly tell the client which DB to use.
        db = firestore.Client(project=PROJECT_ID, database="recipe")
        storage_client = storage.Client()

        # Verify connection by trying to read one document
        print("🔍 Verifying database connection...")
        test_docs = list(db.collection("users").limit(1).stream())
        if not test_docs:
            print("⚠️ Warning: 'users' collection appears empty or inaccessible.")
        else:
            print(f"✅ Connected! Found user: {test_docs[0].id}")

        # --- 2. EXTRACT ---
        print("📥 Extracting data...")
        users_docs = db.collection("users").stream()
        recipes_docs = db.collection("recipes").stream()
        interactions_docs = db.collection("interactions").stream()

        # --- 3. TRANSFORM ---
        print("⚙️ Transforming data...")
        user_rows = []
        recipe_rows = []
        ingredient_rows = []
        step_rows = []
        interaction_rows = []

        # Process Users
        for doc in users_docs:
            data = doc.to_dict()
            user_rows.append(
                {
                    "user_id": data.get("user_id"),
                    "username": data.get("username"),
                    "email": data.get("email"),
                    "created_at": str(data.get("created_at")),
                }
            )

        # Process Recipes
        for doc in recipes_docs:
            data = doc.to_dict()
            r_id = data.get("recipe_id")
            recipe_rows.append(
                {
                    "recipe_id": r_id,
                    "title": data.get("title"),
                    "author_id": data.get("author_id"),
                    "prep_time_minutes": data.get("prep_time_minutes"),
                    "difficulty": data.get("difficulty"),
                    "created_at": str(data.get("created_at")),
                }
            )
            for ing in data.get("ingredients", []):
                ingredient_rows.append(
                    {
                        "recipe_id": r_id,
                        "name": ing.get("name"),
                        "quantity": ing.get("quantity"),
                        "unit": ing.get("unit"),
                    }
                )
            for idx, step in enumerate(data.get("steps", [])):
                step_rows.append(
                    {"recipe_id": r_id, "step_number": idx + 1, "instruction": step}
                )

        # Process Interactions
        for doc in interactions_docs:
            data = doc.to_dict()
            interaction_rows.append(
                {
                    "interaction_id": data.get("interaction_id"),
                    "user_id": data.get("user_id"),
                    "recipe_id": data.get("recipe_id"),
                    "type": data.get("type"),
                    "rating": data.get("rating", ""),
                    "timestamp": str(data.get("timestamp")),
                }
            )

        # --- 4. LOAD ---
        print(f"💾 Uploading {len(recipe_rows)} recipes to {BUCKET_NAME}...")
        bucket = storage_client.bucket(BUCKET_NAME)

        def upload_file(filename, fields, rows):
            mem_file = io.StringIO()
            writer = csv.DictWriter(mem_file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

            blob = bucket.blob(f"backups/{filename}")
            blob.upload_from_string(mem_file.getvalue(), content_type="text/csv")
            print(f"   -> Uploaded {filename}")

        upload_file(
            "users.csv", ["user_id", "username", "email", "created_at"], user_rows
        )
        upload_file(
            "recipe.csv",
            [
                "recipe_id",
                "title",
                "author_id",
                "prep_time_minutes",
                "difficulty",
                "created_at",
            ],
            recipe_rows,
        )
        upload_file(
            "ingredients.csv",
            ["recipe_id", "name", "quantity", "unit"],
            ingredient_rows,
        )
        upload_file("steps.csv", ["recipe_id", "step_number", "instruction"], step_rows)
        upload_file(
            "interactions.csv",
            ["interaction_id", "user_id", "recipe_id", "type", "rating", "timestamp"],
            interaction_rows,
        )

        return (
            f"Success! Processed {len(recipe_rows)} recipes. Files in {BUCKET_NAME}/backups/",
            200,
        )

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        return f"Pipeline Failed: {str(e)}", 500
