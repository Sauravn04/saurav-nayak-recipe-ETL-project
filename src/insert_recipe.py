import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import random
from faker import Faker 

# --- 1. INITIALIZE FIREBASE ---
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceaccount.json")
    firebase_admin.initialize_app(cred)
    
db = firestore.client(database_id="recipe")

fake = Faker()

# --- DATA CONFIGURATION ---

# Your specific Recipe 
my_recipe = {
    "recipe_id": "rec_001_saurav_chicken_gravy",
    "title": "Chicken Gravy for 2 People",
    "author_id": "user_saurav_001",
    "prep_time_minutes": 35,
    "difficulty": "Medium",
    "created_at": datetime.now(),
    "ingredients": [
        {"name": "Chicken (washed)", "quantity": 300, "unit": "grams"},
        {"name": "Onion", "quantity": 2, "unit": "count"},
        {"name": "Tomato", "quantity": 1, "unit": "count"},
        {"name": "Ginger Garlic Paste", "quantity": 1, "unit": "tbsp"},
        {"name": "Salt", "quantity": 1, "unit": "tbsp"},
        {"name": "Chicken Masala", "quantity": 1, "unit": "tbsp"},
        {"name": "Turmeric Powder", "quantity": 0.5, "unit": "tbsp"},
        {"name": "Cumin Powder", "quantity": 0.5, "unit": "tbsp"},
        {"name": "Garam Masala", "quantity": 0.5, "unit": "tbsp"},
        {"name": "Chilli Powder", "quantity": 0.5, "unit": "tbsp"},
        {"name": "Water", "quantity": 250, "unit": "ml"},
        {"name": "Oil", "quantity": 2, "unit": "tbsp"},
    ],
    "steps": [
        "Chop onion and tomato finely.",
        "Heat oil in pan (high flame).",
        "Sauté onions until golden brown.",
        "Add ginger garlic paste, mix 1 min.",
        "Add masalas, mix properly.",
        "Add chicken, cook 5 min to soak water.",
        "Add 250ml water, cover, cook on medium for 15 min.",
        "Turn off flame and serve.",
    ],
}

# Synthetic Data Lists to randomize
food_types = [
    "Pasta",
    "Curry",
    "Salad",
    "Soup",
    "Cake",
    "Stir Fry",
    "Tacos",
    "Sandwich",
]
adjectives = ["Spicy", "Creamy", "Vegan", "Quick", "Homestyle", "Delicious", "Cheesy"]
ingredients_pool = [
    "Salt",
    "Pepper",
    "Olive Oil",
    "Garlic",
    "Tomato",
    "Cheese",
    "Basil",
    "Chicken",
    "Rice",
    "Flour",
]


def generate_synthetic_recipes(count=19):
    recipes = []
    for i in range(count):
        r_id = f"rec_{i+2:03d}_synthetic"
        title = f"{random.choice(adjectives)} {random.choice(food_types)}"

        # Randomize ingredients
        num_ingredients = random.randint(3, 8)
        recipe_ingredients = []
        for _ in range(num_ingredients):
            recipe_ingredients.append(
                {
                    "name": random.choice(ingredients_pool),
                    "quantity": random.randint(1, 500),
                    "unit": random.choice(["grams", "tbsp", "cup", "pcs"]),
                }
            )

        recipes.append(
            {
                "recipe_id": r_id,
                "title": title,
                "author_id": f"user_{random.randint(100, 999)}",
                "prep_time_minutes": random.randint(10, 120),
                "difficulty": random.choice(["Easy", "Medium", "Hard"]),
                "created_at": datetime.now() - timedelta(days=random.randint(1, 365)),
                "ingredients": recipe_ingredients,
                "steps": ["Step 1: Prep", "Step 2: Cook", "Step 3: Serve"],
            }
        )
    return recipes


def generate_interactions(users, recipes, count=50):
    interactions = []
    for i in range(count):
        interactions.append(
            {
                "interaction_id": f"int_{i:04d}",
                "user_id": random.choice(users),
                "recipe_id": random.choice(recipes)["recipe_id"],
                # Logic to create Views and Likes as requested
                "type": random.choice(
                    ["view", "view", "view", "like", "like", "cook_attempt"]
                ),
                "rating": (
                    random.choice([3, 4, 5, 5]) if random.random() > 0.7 else None
                ),
                "timestamp": datetime.now() - timedelta(days=random.randint(0, 30)),
            }
        )
    return interactions

# --- EXECUTION ---

def seed_database():
    print("🚀 Starting Database Seed...")

    # 1. Create Users 
    user_saurav = {
        "user_id": "user_saurav_001",
        "username": "Saurav Nayak",
        "email": "nayakSaurav99@gmail.com",
        "created_at": datetime.now(),
    }
    db.collection("users").document(user_saurav["user_id"]).set(user_saurav)

    synthetic_user_ids = []
    for _ in range(10):
        uid = f"user_{random.randint(1000, 9999)}"
        synthetic_user_ids.append(uid)
        db.collection("users").document(uid).set(
            {
                "user_id": uid,
                "username": fake.name(),
                "email": fake.email(),
                "created_at": datetime.now(),
            }
        )
    print(" Users created.")

    # 2. Create Recipes (Your Chicken Gravy + 19 Synthetic)
    all_recipes = [my_recipe] + generate_synthetic_recipes(19)

    for recipe in all_recipes:
        db.collection("recipes").document(recipe["recipe_id"]).set(recipe)
    print(f" {len(all_recipes)} Recipes created.")

    # 3. Create Interactions
    # Note: This writes to the collection named 'interactions'
    interaction_data = generate_interactions(
        [user_saurav["user_id"]] + synthetic_user_ids, all_recipes, 50
    )
    for interaction in interaction_data:
        db.collection("interactions").document(interaction["interaction_id"]).set(
            interaction
        )
    print(f" {len(interaction_data)} Interactions logged.")

    print(" Database Seeding Complete!")


if __name__ == "__main__":
    seed_database()
