
Recipe Analytics Data Pipeline
==============================

**Author:** Saurav Nayak <br>
**Role:** Data Engineer <br>
**E-mail:** nayaksaurav99@gmail.com <br>
**Project:** Firebase-Based Recipe Analytics Pipeline
---
--------------------
1\. Project Overview
--------------------

This project implements an end-to-end Data Engineering pipeline designed to ingest, transform, and analyze recipe data. The system uses **Firebase Firestore** as a transactional NoSQL source and **Google BigQuery** as the analytical data warehouse.

The goal is to analyze user engagement (views, likes) and recipe complexity (ingredients, steps) to derive business insights.

### System Architecture

The pipeline follows a standard **ELT (Extract, Load, Transform)** pattern:

1.  **Source:** Firestore (Document Store) - *Transactional Data*

2.  **Extraction:** Python ETL Pipeline - *Normalization & Cleaning*

3.  **Staging:** Google Cloud Storage (Bucket: `saurav_recipe_backup`) - *Data Lake*

4.  **Warehouse:** BigQuery (Dataset: `recipe_analytics`) - *Analytics*

__ETL Flow:__
![alt text](pictures/etl_flow.png)

---
2\. Data Model & Schema
-----------------------

The project uses a hybrid data model to handle both semi-structured source data and structured analytical data.

### A. NoSQL Source Schema (Firestore)

The source data is stored in 3 collections in the default database:

-   **`users`**: Stores user profiles.

-   **`recipes`**: Stores recipe details. Uses **nested arrays** for `ingredients` and `steps`.

-   **`interactions`**: A transactional log linking Users to Recipes (Views, Likes, Cook Attempts).

### B. Normalized Relational Schema (BigQuery)

To facilitate SQL analysis, the data is normalized into 5 tables (Star Schema approach):

1.  **`users`**: Dimension table (`user_id`, `username`, `email`).

2.  **`recipes`**: Fact/Dimension table (`recipe_id`, `prep_time`, `difficulty`).

3.  **`ingredients`**: Bridge table exploding the ingredients array (One-to-Many).

4.  **`steps`**: Bridge table exploding the steps array (One-to-Many).

5.  **`interactions`**: Fact table for user events (`view`, `like`, `timestamp`).

ER Diagram:
![alt text](pictures/image-2.png)
---
3\. Setup & Prerequisites
-------------------------

### System Requirements

-   **Python:** 3.8+

-   **Google Cloud Platform:** Project `fir-f8d56` with BigQuery & Storage enabled.

-   **Firebase:** Admin SDK Credentials (`serviceaccount.json`).

### Dependencies

Install the required Python libraries:

```
pip install firebase-admin faker google-cloud-bigquery google-cloud-storage pandas

```
---
4\. Implementation Guide (How to Run)
-------------------------------------

### Step 1: Seed the Database

Populates Firestore with the primary "Chicken Gravy" recipe (sourced from candidate input) and 19 synthetic recipes to simulate a live application.

```
python insert_recipe.py

```

-   **Outcome:** Creates `users`, `recipes`, and `interactions` collections in Firestore.

-   **Verification:**

### Step 2: ETL Pipeline Execution

Extracts data from Firestore, normalizes nested arrays into relational rows, creates local CSV files, and uploads backups to Google Cloud Storage.

```
python etl_pipeline.py

```

-   **Outcome:** Generates 5 CSV files locally and uploads them to `gs://saurav_recipe_backup/backups/`.

-   **Artifacts:**

### Step 3: Data Quality Validation

Runs automated checks for referential integrity (e.g., do interactions point to real users?), missing fields, and data type consistency.

```
python data_validation.py

```

-   **Key Rules Checked:**

    -   **Referential Integrity:** All `user_id` and `recipe_id` in interactions must exist in master tables.

    -   **Logical Validity:** Prep time must be positive; Difficulty must be Easy/Medium/Hard.

    -   **Completeness:** Recipes must have ingredients and steps.

### Step 4: Load to Data Warehouse

Loads the CSV files from the Cloud Storage Bucket into BigQuery tables.

```
python recipe_analytics.py

```

-   **Outcome:** Creates and populates the `recipe_analytics` dataset in BigQuery.
--- 
5\. ETL Process Overview
------------------------

The ETL (Extract, Transform, Load) pipeline is implemented in Python (`etl_pipeline.py`) to bridge the gap between the document-oriented source (Firestore) and the structured analytics destination (BigQuery).

### 1\. Extraction (Extract)

-   **Source:** Firebase Firestore (Default Database).

-   **Method:** Uses the `firebase-admin` SDK to stream documents from three primary collections: `users`, `recipes`, and `interactions`.

-   **Logic:** Streaming (`.stream()`) is used instead of fetching all data at once to ensure memory efficiency and scalability for large datasets.

### 2\. Transformation (Transform)

The raw JSON data from Firestore is semi-structured (nested arrays). The pipeline normalizes this into a **Relational Star Schema** format suitable for SQL analysis.

-   **Normalization of Recipes:**

    -   The `recipes` document contains nested arrays for `ingredients` and `steps`.

    -   **Explosion:** The pipeline iterates through these arrays to create separate rows for the `ingredients.csv` and `steps.csv` files.

    -   **Indexing:** A `step_number` is generated for each cooking step to preserve the correct order in the relational format.

-   **Data Cleaning:**

    -   Timestamps are preserved in ISO format.

    -   Missing optional fields (like `rating` in interactions) are handled gracefully to prevent schema errors.

-   **Schema Mapping:**

    -   JSON fields are mapped to specific CSV columns (e.g., `prep_time_minutes`, `difficulty`).

### 3\. Loading (Load)

-   **Local Staging:** The transformed data is first written to 5 normalized CSV files locally:

    -   `users.csv`

    -   `recipe.csv`

    -   `ingredients.csv`

    -   `steps.csv`

    -   `interactions.csv`

-   **Data Lake Backup:** These files are programmatically uploaded to a Google Cloud Storage bucket (`gs://saurav_recipe_backup`) to serve as a persistent raw data backup.

-   **Warehouse Loading:** A separate process loads these CSVs from Cloud Storage into **BigQuery** tables, utilizing schema auto-detection.

6\. Analytics & Insights Summary
--------------------------------

The following insights were derived using SQL queries on the BigQuery dataset `recipe_analytics`.

### 1\. Top 5 Most Common Ingredients

*Most frequently used ingredients across all recipes.*

```
SELECT name, COUNT(*) as frequency
FROM `recipe_analytics.ingredients`
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

```
Output
| Ingredient            | Frequency |
|-----------------------|-----------|
| Rice                  | 14        |
| Tomato                | 14        |
| Garlic                | 13        |
| Basil                 | 12        |
| Salt                  | 12        |
| Chicken               | 11        |

![alt text](pictures/image-3.png)

### 2\. Average Preparation Time

*Global average time required to cook a dish.*

```
SELECT ROUND(AVG(prep_time_minutes), 1) as avg_time
FROM `recipe_analytics.recipes`
WHERE prep_time_minutes > 0;

```
Output
| Metric            | Value |
|-------------------|-------|
| Average Time (min) | 65.3  |


### 3\. Difficulty Distribution

*Breakdown of recipes by difficulty level.*

```
SELECT difficulty, COUNT(*) as count
FROM `recipe_analytics.recipes`
GROUP BY 1;
```
Output
| Difficulty | Count |
|-----------|--------|
| Easy      | 7      |
| Hard      | 5      |
| Medium    | 8      |

![alt text](pictures/image-8.png)
### 4\. Correlation: Prep Time vs. Likes

*Comparing the prep time of "Liked" recipes vs the global average.*

```
SELECT
    (SELECT ROUND(AVG(prep_time_minutes),1) FROM `recipe_analytics.recipes`) as global_avg,
    (SELECT ROUND(AVG(r.prep_time_minutes),1)
     FROM `recipe_analytics.interactions` i
     JOIN `recipe_analytics.recipes` r ON i.recipe_id = r.recipe_id
     WHERE i.type = 'like') as liked_avg;

```
Output
| global_avg      | liked_avg |
|---------------|--------|
| 65.3  | 69.2    |


### 5\. Most Frequently Viewed Recipe

```
SELECT r.title, COUNT(*) as views
FROM `recipe_analytics.interactions` i
JOIN `recipe_analytics.recipes` r ON i.recipe_id = r.recipe_id
WHERE i.type = 'view'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

```
Output
| Row | Title        | Views |
|-----|--------------|-------|
| 1   | Cheesy Cake  | 6     |

### 6\. Ingredients in High Engagement Recipes

*Ingredients appearing in recipes that received 'Likes'.*

```
SELECT ing.name, COUNT(i.interaction_id) as likes
FROM `recipe_analytics.interactions` i
JOIN `recipe_analytics.ingredients` ing ON i.recipe_id = ing.recipe_id
WHERE i.type = 'like'
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

```
Output
| Row | Name     | Likes |
|-----|----------|-------|
| 1   | Basil    | 13    |
| 2   | Rice     | 13    |
| 3   | Salt     | 12    |
| 4   | Pepper   | 11    |
| 5   | Chicken  | 10    |

![alt text](pictures/image-7.png)
### 7\. Most Active Users

*Users with the highest number of interactions.*

```
SELECT u.username, COUNT(*) as actions
FROM `recipe_analytics.interactions` i
JOIN `recipe_analytics.users` u ON i.user_id = u.user_id
GROUP BY 1 ORDER BY 2 DESC LIMIT 3;

```
Output
| Username          | Actions |
|-------------------|---------|
| Carl Lee          | 7       |
| Amanda Sloan      | 7       |
| Christopher Moss  | 6       |
| Anna Crawford     | 5       |
| Heather Rogers    | 5       |
| Carl Cobb         | 4       |
| Saurav Nayak      | 4       |
| Zachary Lewis     | 4       |
| Ashley Pierce     | 3       |
| Erin Hopkins      | 3       |
| Brenda Wall       | 2       |

![alt text](pictures/image-6.png)
### 8\. Most Complex Recipes

*Recipes with the highest number of cooking steps.*

```
SELECT r.title, COUNT(s.step_number) as steps
FROM `recipe_analytics.recipes` r
JOIN `recipe_analytics.steps` s ON r.recipe_id = s.recipe_id
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

```
Output
| Row | Title                           | Steps |
|-----|----------------------------------|-------|
| 1   | Chicken Gravy for 2 People       | 8     |

### 9\. Average Ingredient Count

```
SELECT ROUND(AVG(cnt),1) as avg_ingredients
FROM (SELECT recipe_id, COUNT(*) as cnt FROM `recipe_analytics.ingredients` GROUP BY recipe_id);

```
Output
| Row | Avg Ingredients |
|-----|-----------------|
| 1   | 6.0             |

### 10\. Views by Difficulty

*Analyzing which difficulty level attracts the most views.*

```
SELECT r.difficulty, COUNT(*) as views
FROM `recipe_analytics.interactions` i
JOIN `recipe_analytics.recipes` r ON i.recipe_id = r.recipe_id
WHERE i.type = 'view'
GROUP BY 1 ORDER BY 2 DESC;

```
Output
| Row | Difficulty | Views |
|-----|------------|-------|
| 1   | Medium     | 9     |
| 2   | Easy       | 9     |
| 3   | Hard       | 8     |

![alt text](pictures/image-9.png)

7\. Known Constraints & Limitations
-----------------------------------

1.  **Synthetic Data:** Aside from the primary recipe ("Chicken Gravy"), the dataset is machine-generated. Ingredient combinations may not be culinarily accurate.

2.  **Batch Latency:** The pipeline is batch-based. Real-time user interactions would require a streaming solution (e.g., Dataflow).

3.  **Schema Rigidity:** The CSV export assumes a consistent schema. Significant changes in the NoSQL document structure would require updates to the Python extraction logic.


