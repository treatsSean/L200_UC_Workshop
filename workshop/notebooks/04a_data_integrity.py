# Databricks notebook source

# MAGIC %md
# MAGIC # Section 4a: Data Integrity — ACID Transactions & Foreign Key Constraints
# MAGIC
# MAGIC **Duration:** 9 minutes
# MAGIC
# MAGIC **Purpose:** Demonstrate that Unity Catalog is not just an access control layer — it is the control plane for data integrity on open table formats (Delta and Iceberg). Multi-table ACID transactions and catalog-enforced foreign key constraints give you relational database guarantees on your lakehouse data.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Run silver layer transformations to create cleaned, typed tables
# MAGIC - Verify row counts across the silver layer
# MAGIC - Execute a successful multi-table ACID transaction (UPDATE + MERGE)
# MAGIC - Observe a failing transaction roll back atomically
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
# MAGIC Create the `transaction_totals` aggregation table. This derived table holds per-customer totals and will be kept in sync with `cleaned_transactions` using a MERGE later in this section.

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
# MAGIC ## Step 3: Successful Multi-Table ACID Transaction
# MAGIC
# MAGIC Delta Lake supports multi-statement transactions via `BEGIN TRANSACTION` / `COMMIT`. When you wrap multiple DML operations in a transaction, **all of them succeed together or none of them persist** — this is the atomicity guarantee.
# MAGIC
# MAGIC In this example we:
# MAGIC 1. Apply a 5% price increase to all electronics purchases in `cleaned_transactions`
# MAGIC 2. Immediately MERGE the updated totals back into `transaction_totals`
# MAGIC
# MAGIC Because both statements are inside the same transaction, readers will never see a state where the transaction amounts have changed but the totals have not yet been updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN TRANSACTION;
# MAGIC
# MAGIC UPDATE lumina_technologies.silver.cleaned_transactions
# MAGIC SET amount = amount * 1.05
# MAGIC WHERE transaction_type = 'purchase' AND product_category = 'electronics';
# MAGIC
# MAGIC MERGE INTO lumina_technologies.silver.transaction_totals AS t
# MAGIC USING (
# MAGIC   SELECT customer_id, COUNT(*) AS total_transactions, SUM(amount) AS total_amount
# MAGIC   FROM lumina_technologies.silver.cleaned_transactions
# MAGIC   GROUP BY customer_id
# MAGIC ) AS s ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.total_transactions = s.total_transactions,
# MAGIC   t.total_amount = s.total_amount;
# MAGIC
# MAGIC COMMIT;

# COMMAND ----------

# MAGIC %md
# MAGIC Both operations committed together. The `transaction_totals` table is now consistent with the updated `cleaned_transactions` amounts. No reader could observe the intermediate state where only one of the two tables had changed.
# MAGIC
# MAGIC > **Key point:** This atomicity guarantee applies to open Delta format tables — it is not a feature of a proprietary storage layer. Unity Catalog coordinates the transaction log entries across both tables within the same commit boundary.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Failing Multi-Table Transaction (Rollback)
# MAGIC
# MAGIC Now we intentionally cause a transaction to fail. The second statement references a column that does not exist (`nonexistent_column`). This will cause an error, and Delta Lake will roll back the entire transaction — including the first UPDATE that would otherwise have succeeded.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN TRANSACTION;
# MAGIC
# MAGIC UPDATE lumina_technologies.silver.cleaned_transactions
# MAGIC SET amount = amount * 1.10
# MAGIC WHERE transaction_type = 'refund';
# MAGIC
# MAGIC UPDATE lumina_technologies.silver.transaction_totals
# MAGIC SET nonexistent_column = 'fail';
# MAGIC
# MAGIC COMMIT;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected result:** The cell above throws an error on the second UPDATE.
# MAGIC
# MAGIC Notice both updates were rolled back. The refund amount change did not persist. Run the verification query below to confirm the refund amounts are unchanged from the committed state after Step 3.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot-check: refund amounts should be unchanged from the Step 3 committed state
# MAGIC SELECT transaction_type, AVG(amount) AS avg_amount
# MAGIC FROM lumina_technologies.silver.cleaned_transactions
# MAGIC GROUP BY transaction_type
# MAGIC ORDER BY transaction_type;

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
# MAGIC | Multi-table ACID (success) | UPDATE + MERGE committed atomically; no intermediate state was visible |
# MAGIC | Multi-table ACID (rollback) | A failing statement caused the entire transaction to roll back |
# MAGIC | Foreign key constraint | UC rejected an INSERT that violated referential integrity |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog manages catalog-level tables in open Delta format. This means:
# MAGIC - ACID guarantees are not tied to a proprietary storage engine — they come from the Delta transaction log coordinated by UC
# MAGIC - Foreign key constraints are enforced by the catalog control plane, applying uniformly to all compute and principals
# MAGIC - Your data integrity rules live in one place (the catalog) rather than scattered across application code, ETL pipelines, or database-specific triggers
# MAGIC
# MAGIC > **Up next:** Section 4b — Row-level security and column masking as the complement to structural integrity.
