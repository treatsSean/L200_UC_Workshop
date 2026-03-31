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
# MAGIC - Add a foreign key constraint between two silver tables
# MAGIC - Attempt a foreign key violation and observe the enforcement

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
# MAGIC 2. Apply a 5% price increase to electronics purchases
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
# MAGIC -- Apply a 5% price increase to electronics purchases
# MAGIC UPDATE lumina_technologies.silver.cleaned_transactions
# MAGIC SET amount = amount * 1.05
# MAGIC WHERE transaction_type = 'purchase' AND product_category = 'electronics';

# COMMAND ----------

# Compare current vs. previous version using Time Travel
current_df = spark.sql("""
    SELECT 'current' AS version, AVG(amount) AS avg_electronics_amount
    FROM lumina_technologies.silver.cleaned_transactions
    WHERE transaction_type = 'purchase' AND product_category = 'electronics'
""")

previous_df = spark.sql(f"""
    SELECT 'previous' AS version, AVG(amount) AS avg_electronics_amount
    FROM lumina_technologies.silver.cleaned_transactions VERSION AS OF {pre_update_version}
    WHERE transaction_type = 'purchase' AND product_category = 'electronics'
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
# MAGIC -- Verify the restore: electronics amounts should be back to their original values
# MAGIC SELECT transaction_type, product_category, AVG(amount) AS avg_amount
# MAGIC FROM lumina_technologies.silver.cleaned_transactions
# MAGIC WHERE transaction_type = 'purchase' AND product_category = 'electronics'
# MAGIC GROUP BY transaction_type, product_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The history now shows the RESTORE as the latest operation
# MAGIC DESCRIBE HISTORY lumina_technologies.silver.cleaned_transactions LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Add a Foreign Key Constraint
# MAGIC
# MAGIC Unity Catalog enforces referential integrity through foreign key constraints. Once a constraint is added, any INSERT or UPDATE that would create an orphaned reference — a transaction row whose `customer_id` does not exist in `cleaned_customers` — will be rejected by the catalog.
# MAGIC
# MAGIC This enforcement happens at the **catalog level**, not inside your application code, which means it applies uniformly regardless of which tool or principal writes to the table.
# MAGIC
# MAGIC > **Re-run note:** If you run this notebook more than once, the `ADD CONSTRAINT` statement will fail if the constraint already exists. That is expected — the constraint is already in place. You can ignore the error and continue to Step 6.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_transactions
# MAGIC ADD CONSTRAINT fk_customer
# MAGIC FOREIGN KEY (customer_id) REFERENCES lumina_technologies.silver.cleaned_customers(customer_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Attempt a Foreign Key Violation
# MAGIC
# MAGIC We now try to insert a transaction row that references a customer ID that does not exist in `cleaned_customers`. Unity Catalog should reject this insert and raise a constraint violation error.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lumina_technologies.silver.cleaned_transactions
# MAGIC VALUES ('fake-txn-id', 'nonexistent-customer-id', 99.99, 'USD', 'purchase', 'test', current_date());

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected result:** The INSERT above fails with a foreign key constraint violation. The row was not written.
# MAGIC
# MAGIC This demonstrates that the constraint is enforced by Unity Catalog, not just documented. Any write path — notebooks, Jobs, SQL warehouses, third-party connectors — is subject to the same enforcement.

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
# MAGIC | Foreign key constraint | UC rejected an INSERT that violated referential integrity |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog manages catalog-level tables in open Delta format. This means:
# MAGIC - ACID guarantees are not tied to a proprietary storage engine — they come from the Delta transaction log coordinated by UC
# MAGIC - Time Travel and RESTORE give you full auditability and the ability to recover from bad writes without backup infrastructure
# MAGIC - Foreign key constraints are enforced by the catalog control plane, applying uniformly to all compute and principals
# MAGIC - Your data integrity rules live in one place (the catalog) rather than scattered across application code, ETL pipelines, or database-specific triggers
# MAGIC
# MAGIC > **Up next:** Section 4b — Row-level security and column masking as the complement to structural integrity.
