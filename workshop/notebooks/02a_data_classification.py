# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2a: Loading Data & AI Classification
# MAGIC
# MAGIC **Duration:** 8 minutes
# MAGIC
# MAGIC **Purpose:** Load raw CSV data into Unity Catalog bronze tables and use AI-powered functions to detect sensitive columns, classify data types, and observe the automated Data Classification engine — all within the Unity Catalog governance boundary.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Load three CSV files from a managed Volume into bronze tables using `read_files`
# MAGIC - Watch an instructor demo: generate AI-authored table and column documentation directly in Catalog Explorer
# MAGIC - Use `ai_gen` to produce column descriptions from SQL
# MAGIC - Use `ai_classify` to categorize columns as PII, non-PII, or quasi-identifier
# MAGIC - Query `system.data_classification.results` to see tags applied by the automated classification engine
# MAGIC
# MAGIC **Recurring theme — AI governance:** The AI functions you call in this section (`ai_gen`, `ai_classify`) are themselves Unity Catalog objects with owners and grants. You are using governed AI to govern data.

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load CSV Data into Bronze Tables
# MAGIC
# MAGIC The raw source files live in a Unity Catalog managed Volume at `/Volumes/lumina_technologies/bronze/raw_files/`. Volumes are the UC-native way to govern file storage — they enforce the same access control model as tables, so only principals with `READ VOLUME` can read these files.
# MAGIC
# MAGIC We use `read_files` with `CREATE OR REPLACE TABLE` to make this cell idempotent: safe to re-run at any time without duplicating data. This pattern is standard for workshop and demo environments where you may need to reset state.
# MAGIC
# MAGIC **Three tables:** `customers`, `transactions`, and `interactions` — the three core entities for the Lumina Technologies customer health scenario used throughout this workshop.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.customers
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/customers.csv',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.transactions
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/transactions.csv',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.bronze.interactions
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/lumina_technologies/bronze/raw_files/interactions.csv',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification: Row Counts
# MAGIC
# MAGIC Confirm all three tables loaded successfully. Expected output: each table should show a non-zero row count matching the source CSV.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers'    AS table_name, COUNT(*) AS row_count FROM lumina_technologies.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'transactions' AS table_name, COUNT(*) AS row_count FROM lumina_technologies.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'interactions' AS table_name, COUNT(*) AS row_count FROM lumina_technologies.bronze.interactions
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSTRUCTOR DEMO: AI-Generated Documentation in Catalog Explorer
# MAGIC
# MAGIC > **Attendees:** Watch the instructor. You do not need to run anything for this step.
# MAGIC
# MAGIC Unity Catalog Catalog Explorer has a built-in feature that uses a foundation model to generate table-level and column-level documentation from metadata alone — no manual writing required.
# MAGIC
# MAGIC **Steps the instructor will perform:**
# MAGIC
# MAGIC 1. Open **Catalog Explorer** from the left navigation bar (the catalog icon).
# MAGIC 2. Navigate to `lumina_technologies` → `bronze` → `customers`.
# MAGIC 3. On the table detail page, locate the **"AI generate"** button (top-right of the Description field) and click it.
# MAGIC 4. Review the generated table comment. Notice it infers purpose from the table name and column names without reading actual data.
# MAGIC 5. Click **Accept** to save the generated description as the table's official comment.
# MAGIC 6. Scroll down to the **Columns** tab. Select a column (e.g., `email`). Click **"AI generate"** next to the column description field.
# MAGIC 7. Review and accept the column-level description.
# MAGIC
# MAGIC **Why this matters:** Table and column comments are first-class metadata in Unity Catalog. They are stored alongside the object definition, versioned, and surfaced in Catalog Explorer, Genie spaces, and downstream tools. AI-generated docs lower the barrier to maintaining a complete data catalog — the hardest part of governance programs is keeping documentation current.
# MAGIC
# MAGIC **Governance angle:** The underlying model endpoint used by the "AI generate" button is itself a governed asset in Unity Catalog. Platform admins can control which models are used for documentation generation the same way they control which models are available for scoring functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: AI-Generated Column Descriptions with `ai_gen`
# MAGIC
# MAGIC Now we move from the point-and-click UI to programmatic documentation generation using the `ai_gen` SQL function. This function calls a UC-managed model endpoint inline within a SQL query — no Python, no external API calls, no credentials to manage.
# MAGIC
# MAGIC We query `lumina_technologies.information_schema.columns` to get column names, then pass each name through a prompt template. The result is a documentation draft for each column, generated at query time.
# MAGIC
# MAGIC **Note:** `ai_gen` is a Unity Catalog function. Your ability to call it is governed by an `EXECUTE` grant — the same privilege model used for every other callable object in UC. If you see a permission error, ask the instructor to verify your group has `EXECUTE` on the function.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   ai_gen(CONCAT('Write a one-sentence description for a database column named "', column_name, '" in a customer data table')) AS generated_description
# MAGIC FROM lumina_technologies.information_schema.columns
# MAGIC WHERE table_schema = 'bronze' AND table_name = 'customers'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: PII Detection with `ai_classify`
# MAGIC
# MAGIC `ai_classify` is a specialized AI function that classifies a text input against a provided set of labels. Here we use it for PII detection: given a column name, classify it as `PII`, `non-PII`, or `quasi-identifier`.
# MAGIC
# MAGIC A **quasi-identifier** is a column that is not PII on its own (e.g., zip code, age, gender) but can be combined with other columns to re-identify an individual. Flagging quasi-identifiers is an important step toward implementing differential privacy controls or data minimization policies.
# MAGIC
# MAGIC This query runs across all columns in the `customers` bronze table. In a production governance workflow, you might persist these results as column tags or feed them into a remediation pipeline.
# MAGIC
# MAGIC **Governance angle:** Like `ai_gen`, `ai_classify` is a UC function governed by grants. The classification labels you provide are part of your governance policy — defining what counts as PII in your organization — and the function applies that policy consistently at scale.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   ai_classify(column_name, ARRAY('PII', 'non-PII', 'quasi-identifier')) AS classification
# MAGIC FROM lumina_technologies.information_schema.columns
# MAGIC WHERE table_schema = 'bronze' AND table_name = 'customers';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Automated Data Classification Engine Results
# MAGIC
# MAGIC In addition to the AI functions you called manually above, Databricks runs an automated **Data Classification engine** in the background. This engine scans table data (not just column names) and applies semantic tags — such as `PERSON_NAME`, `EMAIL`, `US_SSN`, `CREDIT_CARD` — based on pattern matching and ML-based inference.
# MAGIC
# MAGIC The results are written to `system.data_classification.results`, a system table that is part of the Unity Catalog observability surface.
# MAGIC
# MAGIC > **Important:** The classification engine runs **asynchronously**. For a newly created table, results may take several minutes to several hours to appear, depending on table size and workspace load. If this query returns zero rows for your bronze tables, that is expected — the engine has not yet completed its scan. Re-run after a few minutes or check back at the end of the workshop.
# MAGIC
# MAGIC **What the columns mean:**
# MAGIC - `tag_name`: The semantic category detected (e.g., `PERSON_NAME`, `EMAIL`).
# MAGIC - `tag_value`: The specific tag value, if applicable.
# MAGIC - Results are scoped to `catalog_name` and `schema_name` to avoid cross-catalog leakage.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_name, column_name, tag_name, tag_value
# MAGIC FROM system.data_classification.results
# MAGIC WHERE catalog_name = 'lumina_technologies'
# MAGIC   AND schema_name = 'bronze'
# MAGIC ORDER BY table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided Checkpoint
# MAGIC
# MAGIC Take 60 seconds to discuss with a neighbor or reflect on the following:
# MAGIC
# MAGIC **Key takeaway:** AI functions like `ai_gen` and `ai_classify` are Unity Catalog objects — governed by the same privilege model as tables, views, and registered models. You did not call an external API or manage a credential: you issued a SQL query, and UC enforced your organization's access policy before the model was invoked. The Data Classification engine extends this further by applying governed AI automatically, writing its findings to a queryable system table. This is AI governance in practice: AI assets that are auditable, access-controlled, and integrated with the rest of your data platform.
# MAGIC
# MAGIC **Questions to consider:**
# MAGIC - How would you use the `ai_classify` output to automatically apply column-level tags in your organization's catalog?
# MAGIC - What is the difference between the column-name-based classification you ran in Step 3 and the data-based classification the engine runs in the background? When would you trust each?
# MAGIC - If a colleague does not have `EXECUTE` on `ai_classify`, what does that tell you about the governance posture of your platform?
# MAGIC
# MAGIC **Up next — Section 2b:** Data Lineage. You will trace how data flows from bronze through silver to gold, and observe how Unity Catalog captures lineage automatically across SQL transformations and Python notebooks.
