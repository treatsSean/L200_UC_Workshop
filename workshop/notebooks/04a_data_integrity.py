# Databricks notebook source

# MAGIC %md
# MAGIC # Section 4a: Data Integrity — ACID Transactions & Foreign Key Constraints
# MAGIC
# MAGIC **Duration:** 9 minutes
# MAGIC
# MAGIC **Purpose:** Demonstrate that Unity Catalog is not just an access control layer — it is the control plane for data integrity on open table formats (Delta and Iceberg). ACID atomicity, Time Travel, and catalog-enforced foreign key constraints give you relational database guarantees on your lakehouse data.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Run silver layer transformations to create cleaned, typed tables
# MAGIC - Verify row counts across the silver layer
# MAGIC - Use Time Travel to inspect table history and verify atomic writes
# MAGIC - Use RESTORE to roll back a table to a previous version
# MAGIC - Add primary key and foreign key constraints between silver tables
# MAGIC - Verify constraints appear in catalog metadata and Catalog Explorer

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Run Silver Layer Transformations
# MAGIC
# MAGIC The bronze layer holds raw, ingested data with minimal transformation. The silver layer applies cleaning rules, type casting, and filtering to produce tables suitable for analytics and downstream use.
# MAGIC
# MAGIC We will create four silver tables:
# MAGIC - `cleaned_customers` — deduplicated, non-null customer records
# MAGIC - `cleaned_transactions` — validated transactions with typed dates and amount checks
# MAGIC - `cleaned_interactions` — customer interaction events
# MAGIC - `transaction_totals` — per-customer aggregates derived from `cleaned_transactions`
# MAGIC
# MAGIC All statements use `CREATE OR REPLACE TABLE` so the notebook is safe to re-run.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_customers AS
# MAGIC SELECT
# MAGIC   customer_id, first_name, last_name, email, phone,
# MAGIC   street_address, city, state, country, region,
# MAGIC   CAST(created_date AS DATE) AS created_date
# MAGIC FROM lumina_technologies.bronze.customers
# MAGIC WHERE customer_id IS NOT NULL AND email IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Now clean the transactions table. We filter out records with null or negative amounts and cast the transaction date to a proper DATE type.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_transactions AS
# MAGIC SELECT
# MAGIC   transaction_id, customer_id, amount, currency,
# MAGIC   transaction_type, product_category,
# MAGIC   CAST(transaction_date AS DATE) AS transaction_date
# MAGIC FROM lumina_technologies.bronze.transactions
# MAGIC WHERE transaction_id IS NOT NULL
# MAGIC   AND customer_id IS NOT NULL
# MAGIC   AND amount IS NOT NULL
# MAGIC   AND amount >= 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Clean the interactions table. We keep all records with a valid customer reference and a non-null interaction type.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.cleaned_interactions AS
# MAGIC SELECT
# MAGIC   interaction_id, customer_id, channel, interaction_type,
# MAGIC   CAST(sentiment_score AS DOUBLE) AS sentiment_score,
# MAGIC   CAST(interaction_date AS DATE) AS interaction_date
# MAGIC FROM lumina_technologies.bronze.interactions
# MAGIC WHERE interaction_id IS NOT NULL
# MAGIC   AND customer_id IS NOT NULL
# MAGIC   AND interaction_type IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Create the `transaction_totals` aggregation table. This derived table holds per-customer totals used in downstream gold layer joins.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.transaction_totals AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*) AS total_transactions,
# MAGIC   SUM(amount) AS total_amount
# MAGIC FROM lumina_technologies.silver.cleaned_transactions
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Silver Tables
# MAGIC
# MAGIC Before proceeding to the transaction demos, confirm that all four silver tables were created and contain data. We use a `UNION ALL` to produce a single result set showing the row count per table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'cleaned_customers'    AS table_name, COUNT(*) AS row_count FROM lumina_technologies.silver.cleaned_customers
# MAGIC UNION ALL
# MAGIC SELECT 'cleaned_transactions' AS table_name, COUNT(*) AS row_count FROM lumina_technologies.silver.cleaned_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'cleaned_interactions' AS table_name, COUNT(*) AS row_count FROM lumina_technologies.silver.cleaned_interactions
# MAGIC UNION ALL
# MAGIC SELECT 'transaction_totals'   AS table_name, COUNT(*) AS row_count FROM lumina_technologies.silver.transaction_totals
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: ACID Atomicity & Time Travel
# MAGIC
# MAGIC Every write to a Delta table — INSERT, UPDATE, DELETE, MERGE — is an atomic transaction recorded in the Delta transaction log. This means each operation either fully succeeds or has no effect. There is no partial write state that readers can observe.
# MAGIC
# MAGIC Delta Lake's **Time Travel** capability lets you query or restore any previous version of a table. This is powered by the transaction log: every committed version is retained and addressable by version number or timestamp.
# MAGIC
# MAGIC In this step we will:
# MAGIC 1. Check the current version of `cleaned_transactions`
# MAGIC 2. Apply a 5% price increase to Cloud Platform purchases
# MAGIC 3. Use Time Travel to query the table **before** the update and confirm the old values are still accessible

# COMMAND ----------

# Capture the current version number before making changes.
# This ensures the Time Travel and RESTORE queries reference the correct version
# regardless of how many times the notebook has been run.
pre_update_version = spark.sql(
    "DESCRIBE HISTORY lumina_technologies.silver.cleaned_transactions LIMIT 1"
).collect()[0]["version"]

print(f"Current version before update: {pre_update_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the current table history before making changes
# MAGIC DESCRIBE HISTORY lumina_technologies.silver.cleaned_transactions LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply a 5% price increase to Cloud Platform purchases
# MAGIC UPDATE lumina_technologies.silver.cleaned_transactions
# MAGIC SET amount = amount * 1.05
# MAGIC WHERE transaction_type = 'purchase' AND product_category = 'Cloud Platform';

# COMMAND ----------

# Compare current vs. previous version using Time Travel
current_df = spark.sql("""
    SELECT 'current' AS version, AVG(amount) AS avg_cloud_platform_amount
    FROM lumina_technologies.silver.cleaned_transactions
    WHERE transaction_type = 'purchase' AND product_category = 'Cloud Platform'
""")

previous_df = spark.sql(f"""
    SELECT 'previous' AS version, AVG(amount) AS avg_cloud_platform_amount
    FROM lumina_technologies.silver.cleaned_transactions VERSION AS OF {pre_update_version}
    WHERE transaction_type = 'purchase' AND product_category = 'Cloud Platform'
""")

display(current_df.union(previous_df))

# COMMAND ----------

# MAGIC %md
# MAGIC The query above shows both the current and previous average amounts side by side. The previous version is unchanged — Delta Lake retains the full history of every committed transaction.
# MAGIC
# MAGIC > **Key point:** Time Travel works on open Delta format tables. Every version is a consistent, atomic snapshot — there is no way to observe a partially written state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Rollback with RESTORE
# MAGIC
# MAGIC Sometimes you need to undo a change entirely — a bad ETL run, an accidental UPDATE, or a data quality issue discovered after the fact. Delta Lake's `RESTORE` command reverts a table to a previous version, and the restore itself is recorded as a new transaction in the log.
# MAGIC
# MAGIC This is a governed operation: only principals with `MODIFY` on the table can execute a RESTORE, and the action is fully auditable through the table history.

# COMMAND ----------

# Restore the table to the version captured before the price increase
spark.sql(f"RESTORE TABLE lumina_technologies.silver.cleaned_transactions TO VERSION AS OF {pre_update_version}")
print(f"Restored to version {pre_update_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the restore: Cloud Platform amounts should be back to their original values
# MAGIC SELECT transaction_type, product_category, AVG(amount) AS avg_amount
# MAGIC FROM lumina_technologies.silver.cleaned_transactions
# MAGIC WHERE transaction_type = 'purchase' AND product_category = 'Cloud Platform'
# MAGIC GROUP BY transaction_type, product_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The history now shows the RESTORE as the latest operation
# MAGIC DESCRIBE HISTORY lumina_technologies.silver.cleaned_transactions LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Add Primary Key and Foreign Key Constraints
# MAGIC
# MAGIC Unity Catalog supports primary key and foreign key constraints on Delta tables. These constraints are **informational** — they are not enforced at write time, but they serve three important purposes:
# MAGIC
# MAGIC 1. **Query optimization:** The query planner uses constraint metadata to eliminate unnecessary joins and improve performance.
# MAGIC 2. **Documentation:** Catalog Explorer displays relationship diagrams derived from FK metadata, making schema relationships visible to consumers.
# MAGIC 3. **Lineage enrichment:** UC uses constraint metadata to enrich lineage graphs with relationship context.
# MAGIC
# MAGIC > **Important:** Unlike a traditional RDBMS, Delta Lake does not reject writes that violate these constraints. Enforcement of data quality rules is handled through expectations in Lakeflow Declarative Pipelines or application-level validation. The constraints here declare the *intended* relationships.
# MAGIC
# MAGIC > **Re-run note:** If you run this notebook more than once, the `ADD CONSTRAINT` statements will fail if the constraints already exist. That is expected — you can ignore the error and continue to Step 6.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, set the primary key on cleaned_customers so it can be referenced
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC ALTER COLUMN customer_id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC ADD CONSTRAINT pk_customer PRIMARY KEY (customer_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now add the foreign key referencing the primary key
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_transactions
# MAGIC ADD CONSTRAINT fk_customer
# MAGIC FOREIGN KEY (customer_id) REFERENCES lumina_technologies.silver.cleaned_customers(customer_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Constraints in Catalog Explorer
# MAGIC
# MAGIC The constraints you just added are visible in Catalog Explorer. Navigate to:
# MAGIC
# MAGIC 1. **Catalog** → `lumina_technologies` → `silver` → `cleaned_customers`
# MAGIC 2. Click the **Schema** tab — you should see `customer_id` marked as **PK**
# MAGIC 3. Navigate to `cleaned_transactions` — you should see the FK relationship to `cleaned_customers`
# MAGIC
# MAGIC You can also verify programmatically:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the constraints defined on cleaned_transactions
# MAGIC SELECT constraint_name, constraint_type, enforced
# MAGIC FROM lumina_technologies.information_schema.table_constraints
# MAGIC WHERE table_schema = 'silver'
# MAGIC ORDER BY table_name, constraint_type;

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the `enforced` column shows `NO` — these constraints are informational. They document the intended relationships and are used by the query optimizer, but writes are not rejected for violations. For write-time enforcement, use Lakeflow Declarative Pipeline expectations or application-level validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint — Key Takeaways
# MAGIC
# MAGIC Take a moment to confirm your understanding before moving to Section 4b.
# MAGIC
# MAGIC **What you demonstrated in this section:**
# MAGIC
# MAGIC | Capability | What happened |
# MAGIC |---|---|
# MAGIC | Silver layer transformations | Cleaned, typed, and filtered bronze data into four silver tables |
# MAGIC | Time Travel | Queried a previous table version to compare pre- and post-update state |
# MAGIC | RESTORE rollback | Reverted the table to a prior version; the restore was recorded as a new transaction |
# MAGIC | PK/FK constraints | Declared primary and foreign keys; verified they appear in catalog metadata |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog manages catalog-level tables in open Delta format. This means:
# MAGIC - ACID guarantees are not tied to a proprietary storage engine — they come from the Delta transaction log coordinated by UC
# MAGIC - Time Travel and RESTORE give you full auditability and the ability to recover from bad writes without backup infrastructure
# MAGIC - PK/FK constraints document relationships in the catalog — the optimizer uses them, Catalog Explorer displays them, and lineage leverages them
# MAGIC - Your schema documentation lives in one place (the catalog) rather than scattered across ER diagrams, wikis, or tribal knowledge
# MAGIC
# MAGIC > **Up next:** Section 4b — Row-level security and column masking as the complement to structural integrity.
