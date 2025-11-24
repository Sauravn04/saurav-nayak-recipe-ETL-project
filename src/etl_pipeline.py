import firebase_admin
from firebase_admin import credentials, firestore, storage
import csv
import os

# --- 1. CONFIGURATION ---

bucket_name = "saurav_recipe_backup"
key_file = "serviceaccount.json"

if not firebase_admin._apps:
    cred = credentials.Certificate(key_file)
    firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})

# --- DATABASE CONNECTION ---
db = firestore.client(database_id="recipe")

bucket = storage.bucket()


def run_etl_pipeline():
    print("Starting ETL Pipeline...")

    # --- 2. EXTRACT ---
    print(" Extracting data from Firestore...")

    # STREAMING FROM YOUR SPECIFIC COLLECTIONS
    users_docs = db.collection("users").stream()
    recipes_docs = db.collection("recipes").stream()
    interactions_docs = db.collection(
        "interactions"
    ).stream()  # Matches your seed script

    # --- 3. TRANSFORM ---
    print(" Transforming data...")

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
                "created_at": data.get("created_at"),
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
                "created_at": data.get("created_at"),
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

        for index, step_text in enumerate(data.get("steps", [])):
            step_rows.append(
                {"recipe_id": r_id, "step_number": index + 1, "instruction": step_text}
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
                "timestamp": data.get("timestamp"),
            }
        )

    # --- 4. LOAD (Save CSVs) ---
    print(f"Saving CSV files (Found {len(interaction_rows)} interactions)...")

    files_to_generate = [
        ("users.csv", ["user_id", "username", "email", "created_at"], user_rows),
        (
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
        ),
        ("ingredients.csv", ["recipe_id", "name", "quantity", "unit"], ingredient_rows),
        ("steps.csv", ["recipe_id", "step_number", "instruction"], step_rows),
        (
            "interactions.csv",
            ["interaction_id", "user_id", "recipe_id", "type", "rating", "timestamp"],
            interaction_rows,
        ),
    ]

    files_uploaded = []

    for filename, fields, rows in files_to_generate:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        files_uploaded.append(filename)
        print(f" Generated: {filename}")

    # --- 5. BACKUP ---
    print(f"\n  Uploading backup to Bucket: {bucket_name}...")
    try:
        for filename in files_uploaded:
            blob = bucket.blob(f"backups/{filename}")
            blob.upload_from_filename(filename)
            print(f" Uploaded: backups/{filename}")
    except Exception as e:
        print(f"  Bucket Error: {e}")

    print("\n ETL Pipeline Complete!")


if __name__ == "__main__":
    run_etl_pipeline()
