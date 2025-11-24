import csv
import os
import re
from datetime import datetime

# --- CONFIGURATION ---
INPUT_FILES = {
    "recipes": "recipe.csv",
    "ingredients": "ingredients.csv",
    "steps": "steps.csv",
    "interactions": "interactions.csv",
    "users": "users.csv",
}

OUTPUT_REPORT_FILE = "validation_report.csv"

# --- VALIDATION RULES ---
VALID_DIFFICULTIES = {"Easy", "Medium", "Hard"}
VALID_INTERACTION_TYPES = {"view", "like", "cook_attempt"}

REQUIRED_FIELDS = {
    "recipes": ["recipe_id", "title", "prep_time_minutes", "difficulty"],
    "users": ["user_id", "username", "email"],
    "interactions": ["interaction_id", "user_id", "recipe_id", "type", "timestamp"],
    "ingredients": ["recipe_id", "name", "quantity"],
    "steps": ["recipe_id", "step_number", "instruction"],
}


def load_csv(filename):
    """Reads a CSV file and returns a list of dictionaries."""
    if not os.path.exists(filename):
        print(f"Error: File {filename} not found. Run ETL pipeline first.")
        return []
    with open(filename, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def get_ids_from_list(data_list, id_column):
    """Extracts a set of IDs from a list of dicts."""
    return {row[id_column] for row in data_list}


def check_email(email):
    """Simple regex check for email validity."""
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)


def validate_data():
    print("STARTING DATA QUALITY VALIDATION...\n")

    # 1. LOAD ALL DATA
    data = {name: load_csv(filename) for name, filename in INPUT_FILES.items()}

    if any(not d for d in data.values()):
        print(" One or more files are missing or empty. Stopping.")
        return

    # 2. PREPARE ID SETS
    recipe_ids = get_ids_from_list(data["recipes"], "recipe_id")
    user_ids = get_ids_from_list(data["users"], "user_id")
    recipe_ids_with_ingredients = get_ids_from_list(data["ingredients"], "recipe_id")
    recipe_ids_with_steps = get_ids_from_list(data["steps"], "recipe_id")

    # List to collect all report rows for the CSV output
    full_report_data = []

    # --- VALIDATION HELPER ---
    def run_check(table_name, rows, validator_func):
        print(f" Validating: {table_name}...")
        invalid_count = 0

        for row in rows:
            issues = validator_func(row)
            status = "FAIL" if issues else "PASS"

            # Determine a primary ID for the report
            row_id = (
                row.get("recipe_id")
                or row.get("user_id")
                or row.get("interaction_id")
                or "N/A"
            )

            # Add to CSV Report Data
            full_report_data.append(
                {
                    "Table": table_name,
                    "Record_ID": row_id,
                    "Status": status,
                    "Issues": "; ".join(issues) if issues else "OK",
                    "Validated_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

            if issues:
                invalid_count += 1
                print(f" FAIL [{row_id}]: {', '.join(issues)}")

        return invalid_count

    # --- 3. DEFINE VALIDATORS ---

    def check_recipe(row):
        issues = []
        for f in REQUIRED_FIELDS["recipes"]:
            if not row.get(f):
                issues.append(f"Missing {f}")
        try:
            if float(row.get("prep_time_minutes", 0)) <= 0:
                issues.append("Invalid prep_time")
        except:
            issues.append("Non-numeric prep_time")
        if row.get("difficulty") not in VALID_DIFFICULTIES:
            issues.append(f"Invalid difficulty: {row.get('difficulty')}")
        r_id = row.get("recipe_id")
        if r_id not in recipe_ids_with_ingredients:
            issues.append("No ingredients linked")
        if r_id not in recipe_ids_with_steps:
            issues.append("No steps linked")
        return issues

    def check_user(row):
        issues = []
        for f in REQUIRED_FIELDS["users"]:
            if not row.get(f):
                issues.append(f"Missing {f}")
        if row.get("email") and not check_email(row.get("email")):
            issues.append("Invalid email format")
        return issues

    def check_interaction(row):
        issues = []
        for f in REQUIRED_FIELDS["interactions"]:
            if not row.get(f):
                issues.append(f"Missing {f}")
        if row.get("user_id") not in user_ids:
            issues.append("Orphaned User ID")
        if row.get("recipe_id") not in recipe_ids:
            issues.append("Orphaned Recipe ID")
        if row.get("type") not in VALID_INTERACTION_TYPES:
            issues.append(f"Invalid type: {row.get('type')}")
        return issues

    def check_ingredient(row):
        issues = []
        for f in REQUIRED_FIELDS["ingredients"]:
            if not row.get(f):
                issues.append(f"Missing {f}")
        if row.get("recipe_id") not in recipe_ids:
            issues.append("Links to non-existent recipe")
        try:
            if float(row.get("quantity", 0)) <= 0:
                issues.append("Zero/Negative quantity")
        except:
            issues.append("Non-numeric quantity")
        return issues

    def check_step(row):
        issues = []
        for f in REQUIRED_FIELDS["steps"]:
            if not row.get(f):
                issues.append(f"Missing {f}")
        if row.get("recipe_id") not in recipe_ids:
            issues.append("Links to non-existent recipe")
        return issues

    # --- 4. RUN VALIDATIONS ---
    total_errors = 0
    total_errors += run_check("Users", data["users"], check_user)
    total_errors += run_check("Recipes", data["recipes"], check_recipe)
    total_errors += run_check("Interactions", data["interactions"], check_interaction)
    total_errors += run_check("Ingredients", data["ingredients"], check_ingredient)
    total_errors += run_check("Steps", data["steps"], check_step)

    # --- 5. WRITE REPORT TO CSV ---
    print(f"\nGenerating Report File: {OUTPUT_REPORT_FILE}...")
    fieldnames = ["Table", "Record_ID", "Status", "Issues", "Validated_At"]

    try:
        with open(OUTPUT_REPORT_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(full_report_data)
        print(f"Report generated successfully! ({len(full_report_data)} rows)")
    except Exception as e:
        print(f" Failed to write report CSV: {e}")

    # --- 6. CONSOLE SUMMARY ---
    print("\n" + "=" * 40)
    if total_errors == 0:
        print(" SUCCESS: Data Quality is 100% Clean!")
    else:
        print(f"  WARNING: Found {total_errors} total issues.")
        print(f"   Check '{OUTPUT_REPORT_FILE}' for details.")
    print("=" * 40)


if __name__ == "__main__":
    validate_data()
