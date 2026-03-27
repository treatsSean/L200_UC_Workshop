# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Setup — Lumina Technologies UC Governance
# MAGIC
# MAGIC **Run this notebook as an instructor before the workshop begins.**
# MAGIC
# MAGIC This notebook provisions the entire `lumina_technologies` catalog with bronze,
# MAGIC silver, and gold layers, registers an MLflow model, creates UC functions, and
# MAGIC sets up access-control fixtures needed by the lab exercises.
# MAGIC
# MAGIC Every cell is **idempotent** — you can re-run the entire notebook safely.
# MAGIC
# MAGIC ### What gets created
# MAGIC | Layer | Objects |
# MAGIC |---|---|
# MAGIC | Catalog | `lumina_technologies` |
# MAGIC | Schemas | `bronze`, `silver`, `gold`, `restricted` |
# MAGIC | Volume | `bronze.raw_files` (managed) |
# MAGIC | Bronze tables | `customers`, `transactions`, `interactions` |
# MAGIC | Silver tables | `cleaned_customers`, `cleaned_transactions`, `cleaned_interactions`, `transaction_totals` |
# MAGIC | Gold tables | `customer_health_scores`, `revenue_summary` |
# MAGIC | ML model | `gold.customer_churn_model` |
# MAGIC | UC function | `gold.score_customer_health` |
# MAGIC | Group | `data_platform_admins` |
# MAGIC | Restricted | `restricted.sensitive_data` (locked down) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

CATALOG = "lumina_technologies"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
RESTRICTED = f"{CATALOG}.restricted"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# Path to CSV files in the workspace repo.
# IMPORTANT: Update this to match where the repo is cloned in your workspace.
# Typical pattern: /Workspace/Users/<your-email>/<repo-name>/workshop/data/output
dbutils.widgets.text("repo_data_path", "/Workspace/Repos/<your-username>/<repo-name>/workshop/data/output", "CSV Source Path")
REPO_DATA_PATH = dbutils.widgets.get("repo_data_path")

# Fail fast if the path doesn't look right
try:
    dbutils.fs.ls(REPO_DATA_PATH)
except Exception:
    raise FileNotFoundError(
        f"CSV source path not found: {REPO_DATA_PATH}\n"
        f"Update the 'repo_data_path' widget at the top of this notebook."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lumina_technologies;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lumina_technologies.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS lumina_technologies.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS lumina_technologies.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Managed Volume and Upload CSV Files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS lumina_technologies.bronze.raw_files;

# COMMAND ----------

# Upload CSV files from the workspace repo into the UC volume.
# If files are already present they will be overwritten (idempotent).
import os

csv_files = ["customers.csv", "transactions.csv", "interactions.csv"]

for f in csv_files:
    src = f"{REPO_DATA_PATH}/{f}"
    dst = f"{VOLUME_PATH}/{f}"
    print(f"Copying {src} -> {dst}")
    dbutils.fs.cp(src, dst)

print("CSV upload complete.")

# COMMAND ----------

# Verify the volume contents
display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Bronze Tables from CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.customers
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/customers.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.transactions
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/transactions.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.interactions
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/interactions.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Silver Tables (Cleaned / Transformed)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_customers AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   street_address,
# MAGIC   city,
# MAGIC   state,
# MAGIC   country,
# MAGIC   region,
# MAGIC   CAST(created_date AS DATE) AS created_date
# MAGIC FROM lumina_technologies.bronze.customers
# MAGIC WHERE customer_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_transactions AS
# MAGIC SELECT
# MAGIC   transaction_id,
# MAGIC   customer_id,
# MAGIC   CAST(amount AS DOUBLE) AS amount,
# MAGIC   currency,
# MAGIC   transaction_type,
# MAGIC   product_category,
# MAGIC   CAST(transaction_date AS DATE) AS transaction_date
# MAGIC FROM lumina_technologies.bronze.transactions
# MAGIC WHERE transaction_id IS NOT NULL
# MAGIC   AND customer_id IS NOT NULL
# MAGIC   AND amount IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_interactions AS
# MAGIC SELECT
# MAGIC   interaction_id,
# MAGIC   customer_id,
# MAGIC   channel,
# MAGIC   interaction_type,
# MAGIC   CAST(sentiment_score AS DOUBLE) AS sentiment_score,
# MAGIC   CAST(interaction_date AS DATE) AS interaction_date
# MAGIC FROM lumina_technologies.bronze.interactions
# MAGIC WHERE interaction_id IS NOT NULL
# MAGIC   AND customer_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregated transaction totals per customer (used in Section 4a multi-table demo)
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.transaction_totals AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*)                    AS transaction_count,
# MAGIC   SUM(amount)                 AS total_amount,
# MAGIC   AVG(amount)                 AS avg_amount,
# MAGIC   MIN(transaction_date)       AS first_transaction_date,
# MAGIC   MAX(transaction_date)       AS last_transaction_date,
# MAGIC   COUNT(DISTINCT product_category) AS distinct_categories
# MAGIC FROM lumina_technologies.silver.cleaned_transactions
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Gold Tables (Aggregated)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Create the UC scoring function first
# MAGIC
# MAGIC The `score_customer_health` function is used by the gold table below, which
# MAGIC creates lineage between the function and the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lumina_technologies.gold.score_customer_health(customer_id STRING)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC   import hashlib
# MAGIC   # Deterministic pseudo-score based on customer_id hash
# MAGIC   hash_val = int(hashlib.md5(customer_id.encode()).hexdigest(), 16)
# MAGIC   return round((hash_val % 1000) / 10.0, 1)
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Gold — Customer Health Scores
# MAGIC
# MAGIC Joins cleaned customers, transactions, and interactions to produce a
# MAGIC per-customer health score. Uses the UC function `score_customer_health()`
# MAGIC to create function-to-table lineage.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.gold.customer_health_scores AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   c.region,
# MAGIC   c.created_date,
# MAGIC   COALESCE(t.transaction_count, 0)   AS transaction_count,
# MAGIC   COALESCE(t.total_amount, 0.0)      AS total_amount,
# MAGIC   COALESCE(i.interaction_count, 0)   AS interaction_count,
# MAGIC   COALESCE(i.avg_sentiment, 0.0)     AS avg_sentiment,
# MAGIC   lumina_technologies.gold.score_customer_health(c.customer_id) AS health_score
# MAGIC FROM lumina_technologies.silver.cleaned_customers c
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*)    AS transaction_count,
# MAGIC     SUM(amount) AS total_amount
# MAGIC   FROM lumina_technologies.silver.cleaned_transactions
# MAGIC   GROUP BY customer_id
# MAGIC ) t ON c.customer_id = t.customer_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*)              AS interaction_count,
# MAGIC     AVG(sentiment_score)  AS avg_sentiment
# MAGIC   FROM lumina_technologies.silver.cleaned_interactions
# MAGIC   GROUP BY customer_id
# MAGIC ) i ON c.customer_id = i.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Gold — Revenue Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.gold.revenue_summary AS
# MAGIC SELECT
# MAGIC   c.region,
# MAGIC   t.product_category,
# MAGIC   DATE_TRUNC('MONTH', t.transaction_date) AS month,
# MAGIC   COUNT(*)          AS transaction_count,
# MAGIC   SUM(t.amount)     AS total_revenue,
# MAGIC   AVG(t.amount)     AS avg_revenue
# MAGIC FROM lumina_technologies.silver.cleaned_transactions t
# MAGIC JOIN lumina_technologies.silver.cleaned_customers c
# MAGIC   ON t.customer_id = c.customer_id
# MAGIC WHERE t.transaction_type = 'purchase'
# MAGIC GROUP BY c.region, t.product_category, DATE_TRUNC('MONTH', t.transaction_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register MLflow Model
# MAGIC
# MAGIC Trains a trivial logistic-regression model and registers it as a UC model.
# MAGIC The model does not need to be accurate — it just needs to exist for
# MAGIC governance demos (lineage, model versioning, access control).

# COMMAND ----------

import mlflow
import numpy as np
from sklearn.linear_model import LogisticRegression
from mlflow.models.signature import infer_signature

mlflow.set_registry_uri("databricks-uc")

model_name = f"{CATALOG}.gold.customer_churn_model"

# Check if model already exists (idempotent — skip if registered)
client = mlflow.tracking.MlflowClient()
existing_versions = client.search_model_versions(f"name='{model_name}'")

if existing_versions:
    latest = max(existing_versions, key=lambda v: int(v.version))
    print(f"Model already registered: {model_name} v{latest.version} — skipping.")
else:
    # Build a small synthetic training set from the gold health scores table
    health_df = spark.table(f"{GOLD}.customer_health_scores").toPandas()

    # Features: transaction_count, total_amount, interaction_count, avg_sentiment
    X = health_df[["transaction_count", "total_amount", "interaction_count", "avg_sentiment"]].fillna(0).values

    # Label: churn = 1 if health_score < 50, else 0 (deterministic from the hash-based score)
    y = (health_df["health_score"] < 50).astype(int).values

    # Train a simple logistic regression
    model = LogisticRegression(max_iter=200, random_state=42)
    model.fit(X, y)

    # Log and register in Unity Catalog
    signature = infer_signature(X, model.predict(X))

    with mlflow.start_run(run_name="customer_churn_setup") as run:
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            input_example=X[:3],
            registered_model_name=model_name,
        )

    print(f"Model registered: {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Group for Ownership Transfer Demo (Section 1)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.groups.create(display_name="data_platform_admins")
    print("Group 'data_platform_admins' created.")
except Exception as e:
    # Group already exists — this is expected on re-runs
    print(f"Group 'data_platform_admins' already exists (or could not be created): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Create Restricted Schema and Table (Access-Request Demo — Section 3)
# MAGIC
# MAGIC This table is intentionally locked down so that attendees cannot access it.
# MAGIC They will use the access-request workflow to request permissions during the lab.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lumina_technologies.restricted;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.restricted.sensitive_data AS
# MAGIC SELECT
# MAGIC   'CONFIDENTIAL' AS classification,
# MAGIC   'This table contains restricted data for the access-request demo.' AS description,
# MAGIC   current_timestamp() AS created_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revoke access so attendees must request it
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA lumina_technologies.restricted FROM `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE ALL PRIVILEGES ON TABLE lumina_technologies.restricted.sensitive_data FROM `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Foreign Connection for Federation Demo (Section 6)
# MAGIC
# MAGIC **This step is manual and must be done before the workshop.**
# MAGIC
# MAGIC Federation requires a foreign connection to an external database (e.g.,
# MAGIC PostgreSQL, MySQL, or SQL Server). This cannot be automated in a notebook
# MAGIC because it requires external infrastructure and credentials.
# MAGIC
# MAGIC ### Steps to configure:
# MAGIC
# MAGIC 1. **Provision an external database** (e.g., a small PostgreSQL instance on
# MAGIC    your cloud provider, or use an existing dev database).
# MAGIC
# MAGIC 2. **Create the foreign connection in Unity Catalog:**
# MAGIC    ```sql
# MAGIC    CREATE CONNECTION IF NOT EXISTS lumina_federation
# MAGIC    TYPE POSTGRESQL
# MAGIC    OPTIONS (
# MAGIC      host '<your-host>',
# MAGIC      port '5432',
# MAGIC      user '<your-user>',
# MAGIC      password '<your-password>'
# MAGIC    );
# MAGIC    ```
# MAGIC
# MAGIC 3. **Create a foreign catalog:**
# MAGIC    ```sql
# MAGIC    CREATE FOREIGN CATALOG IF NOT EXISTS lumina_external
# MAGIC    USING CONNECTION lumina_federation
# MAGIC    OPTIONS (database '<your-database>');
# MAGIC    ```
# MAGIC
# MAGIC 4. **Verify** by querying a table from the foreign catalog:
# MAGIC    ```sql
# MAGIC    SELECT * FROM lumina_external.<schema>.<table> LIMIT 10;
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verify Setup
# MAGIC
# MAGIC Query row counts and print a summary to confirm everything was created.

# COMMAND ----------

tables_to_check = [
    f"{BRONZE}.customers",
    f"{BRONZE}.transactions",
    f"{BRONZE}.interactions",
    f"{SILVER}.cleaned_customers",
    f"{SILVER}.cleaned_transactions",
    f"{SILVER}.cleaned_interactions",
    f"{SILVER}.transaction_totals",
    f"{GOLD}.customer_health_scores",
    f"{GOLD}.revenue_summary",
]

print("=" * 60)
print("SETUP VERIFICATION")
print("=" * 60)

all_ok = True
for table in tables_to_check:
    try:
        count = spark.table(table).count()
        status = "OK" if count > 0 else "WARN (0 rows)"
        if count == 0:
            all_ok = False
        print(f"  [{status}]  {table:55s}  {count:>8,} rows")
    except Exception as e:
        all_ok = False
        print(f"  [FAIL]  {table:55s}  ERROR: {e}")

# restricted.sensitive_data is intentionally inaccessible to non-admin users.
# A PERMISSION_DENIED error here means the REVOKE worked correctly.
print(f"\n  Restricted table ({RESTRICTED}.sensitive_data):")
try:
    count = spark.table(f"{RESTRICTED}.sensitive_data").count()
    print(f"  [OK]    Table exists ({count} rows). You are running as admin — attendees will see PERMISSION_DENIED.")
except Exception as e:
    if "PERMISSION_DENIED" in str(e) or "does not have" in str(e).lower():
        print(f"  [OK]    REVOKE working — non-admin access correctly denied.")
    else:
        all_ok = False
        print(f"  [FAIL]  Unexpected error: {e}")

# COMMAND ----------

# Verify UC function
print("\nUC Function:")
try:
    result = spark.sql(
        "SELECT lumina_technologies.gold.score_customer_health('test-customer-001') AS score"
    ).collect()[0]["score"]
    print(f"  [OK]    lumina_technologies.gold.score_customer_health  ->  {result}")
except Exception as e:
    all_ok = False
    print(f"  [FAIL]  lumina_technologies.gold.score_customer_health  ERROR: {e}")

# Verify MLflow model
print("\nMLflow Model:")
try:
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.tracking.MlflowClient()
    model_versions = client.search_model_versions(f"name='{CATALOG}.gold.customer_churn_model'")
    latest = max(model_versions, key=lambda v: int(v.version))
    print(f"  [OK]    {CATALOG}.gold.customer_churn_model  v{latest.version}")
except Exception as e:
    all_ok = False
    print(f"  [FAIL]  {CATALOG}.gold.customer_churn_model  ERROR: {e}")

# Verify group
print("\nGroup:")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    groups = w.groups.list(filter=f'displayName eq "data_platform_admins"')
    found = any(True for _ in groups)
    if found:
        print("  [OK]    data_platform_admins")
    else:
        all_ok = False
        print("  [FAIL]  data_platform_admins  NOT FOUND")
except Exception as e:
    all_ok = False
    print(f"  [FAIL]  data_platform_admins  ERROR: {e}")

print("\n" + "=" * 60)
if all_ok:
    print("ALL CHECKS PASSED — Workshop environment is ready!")
else:
    print("SOME CHECKS FAILED — Review the output above.")
print("=" * 60)
