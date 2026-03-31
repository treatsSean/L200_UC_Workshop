# Databricks notebook source

# MAGIC %md
# MAGIC # Section 4b: Row/Column Security & ABAC at Scale
# MAGIC
# MAGIC **Duration:** 11 minutes
# MAGIC
# MAGIC **Purpose:** Demonstrate how Unity Catalog's column masking and row filter functions enable attribute-based access control (ABAC) that scales from a single table all the way to the catalog level — without rewriting policy logic for every new table.
# MAGIC
# MAGIC **The progression:**
# MAGIC - **Table level** — apply a column mask and row filter to one table
# MAGIC - **Schema level** — reuse the same filter function across additional tables in the schema
# MAGIC - **Catalog level** — attach the same functions to a brand-new table in a single step
# MAGIC
# MAGIC This is the ABAC model: policy functions are defined once in the catalog, and any table can reference them. When your access rules change, you update one function and every table governed by it is immediately updated.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Create a reusable column mask function that hides PII from non-admin users
# MAGIC - Apply the mask to multiple columns on `cleaned_customers`
# MAGIC - Create a row filter function that restricts non-admin users to a single region
# MAGIC - Apply the row filter to `cleaned_customers` and verify the effect
# MAGIC - Reuse the same mask function on a second table (`cleaned_interactions`) without any new function code
# MAGIC - Create a new table and immediately attach both policies — demonstrating catalog-wide governance

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Start at the Table
# MAGIC
# MAGIC We begin at the most granular scope: a single table. Unity Catalog column masks are implemented as SQL functions. The function receives the column value and returns either the real value or a redacted version, depending on the caller's group membership.
# MAGIC
# MAGIC The key function is `is_account_group_member()`. It evaluates at query time against the identity of the user running the query — not the identity of whoever defined the function. This makes the policy dynamic: the same function returns different results to different callers.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lumina_technologies.silver.mask_pii(value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_platform_admins') THEN value
# MAGIC   ELSE CONCAT(LEFT(value, 2), '****')
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC ALTER COLUMN email SET MASK lumina_technologies.silver.mask_pii;
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC ALTER COLUMN phone SET MASK lumina_technologies.silver.mask_pii;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, first_name, last_name, email, phone
# MAGIC FROM lumina_technologies.silver.cleaned_customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Notice email and phone are masked because you are not in the `data_platform_admins` group.
# MAGIC
# MAGIC The mask is enforced transparently — the query syntax is identical to a query against an unmasked table. The column mask function intercepts the value at read time and applies the redaction rule. A user in `data_platform_admins` running the same query would see the full values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Scale to the Schema
# MAGIC
# MAGIC Column masks address individual column values. Row filters address which rows a user can see at all. Like column masks, row filter functions are defined once and referenced by any number of tables.
# MAGIC
# MAGIC The function below returns `TRUE` (row is visible) for members of `data_platform_admins`, and restricts all other users to rows where `region = 'NORTH'`. Once attached to a table, this filter is applied automatically — the user never needs to add a `WHERE` clause.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lumina_technologies.silver.filter_by_region(region_val STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_platform_admins') THEN TRUE
# MAGIC   ELSE region_val = 'NORTH'
# MAGIC END;
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC SET ROW FILTER lumina_technologies.silver.filter_by_region ON (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS visible_rows
# MAGIC FROM lumina_technologies.silver.cleaned_customers
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC Only NORTH region rows are visible. The same filter function can be reused across any table with a region column.
# MAGIC
# MAGIC This is the schema-level scaling point: `filter_by_region` is not tied to `cleaned_customers`. Any table in the schema that has a `region` column can reference the same function with a single `ALTER TABLE` statement — no new function definition needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Scale to the Catalog with ABAC
# MAGIC
# MAGIC The `mask_pii` function we created in Part 1 is a catalog-level object. It lives in `lumina_technologies.silver` and can be referenced by any table in the catalog. We do not need to rewrite the masking logic to apply it to a different table.
# MAGIC
# MAGIC Here we apply the same function to the `channel` column in `cleaned_interactions`. The function definition is unchanged — only the `ALTER TABLE` statement is new.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_interactions
# MAGIC ALTER COLUMN channel SET MASK lumina_technologies.silver.mask_pii;

# COMMAND ----------

# MAGIC %md
# MAGIC The `mask_pii` function now governs PII columns across two tables — `cleaned_customers` and `cleaned_interactions` — with a single function definition. If your organization's masking rule changes (for example, showing three characters instead of two before the redaction), you update `mask_pii` once and the change is immediately reflected on every table that references it.
# MAGIC
# MAGIC This is the ABAC scaling model: attributes (group membership) drive policy, policy is expressed in reusable functions, and functions are attached to tables independently of their definition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: The "Aha" Moment
# MAGIC
# MAGIC The most compelling demonstration of catalog-wide ABAC is what happens when you create a **new** table. You do not need to go back and configure security after the fact. You can attach the existing policy functions in the same script — or even immediately after the `CREATE TABLE` — and the new table is governed from its first query.
# MAGIC
# MAGIC In this step we create `new_customer_segment`, a table that did not exist until this moment, and immediately apply both the column mask and the row filter using the functions we defined earlier.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.silver.new_customer_segment AS
# MAGIC SELECT customer_id, email, region, 'segment_a' AS segment
# MAGIC FROM lumina_technologies.silver.cleaned_customers
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ALTER TABLE lumina_technologies.silver.new_customer_segment
# MAGIC ALTER COLUMN email SET MASK lumina_technologies.silver.mask_pii;
# MAGIC ALTER TABLE lumina_technologies.silver.new_customer_segment
# MAGIC SET ROW FILTER lumina_technologies.silver.filter_by_region ON (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lumina_technologies.silver.new_customer_segment LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC This table was just created, and both the column mask and row filter are enforced using the same functions. One function definition governs your entire catalog — no per-table rewrite needed.
# MAGIC
# MAGIC Non-admin users querying `new_customer_segment` will see only NORTH region rows, and the `email` column will be masked. The table inherited these policies by referencing the existing functions — no additional access control setup was required beyond the two `ALTER TABLE` statements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Applying Masks and Filters via the UI
# MAGIC
# MAGIC Everything you did in Parts 1–4 with SQL can also be accomplished through the Catalog Explorer UI. This is useful for data stewards who prefer a visual workflow, or for quick ad-hoc policy changes that don't warrant a notebook.
# MAGIC
# MAGIC **Exercise — apply a column mask via the UI:**
# MAGIC
# MAGIC 1. Open **Catalog Explorer** from the left navigation bar.
# MAGIC 2. Navigate to `lumina_technologies` → `silver` → `cleaned_customers`.
# MAGIC 3. Click on the **Columns** tab.
# MAGIC 4. Click on the `street_address` column.
# MAGIC 5. In the column detail panel, locate the **Mask** section and click **Add mask**.
# MAGIC 6. Select the existing function `lumina_technologies.silver.mask_pii`.
# MAGIC 7. Click **Save**.
# MAGIC
# MAGIC **Exercise — apply a row filter via the UI:**
# MAGIC
# MAGIC 1. Stay on the `cleaned_customers` table detail page.
# MAGIC 2. Click the **Row filter** tab (or locate the row filter section on the Overview tab).
# MAGIC 3. Click **Add row filter**.
# MAGIC 4. Select the existing function `lumina_technologies.silver.filter_by_region`.
# MAGIC 5. Map the function parameter to the `region` column.
# MAGIC 6. Click **Save**.
# MAGIC
# MAGIC **Key point:** Whether you apply masks and filters via SQL or the UI, the result is identical — both write to the same catalog metadata. The UI provides a visual summary of which columns are masked and which row filters are active, making it easy to audit a table's security posture at a glance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint — Key Takeaways
# MAGIC
# MAGIC Take a moment to confirm your understanding before moving to Section 5.
# MAGIC
# MAGIC **What you demonstrated in this section:**
# MAGIC
# MAGIC | Scope | What happened |
# MAGIC |---|---|
# MAGIC | Table — column mask | `mask_pii` hides PII from non-admins on `cleaned_customers` |
# MAGIC | Table — row filter | `filter_by_region` restricts non-admins to NORTH region rows |
# MAGIC | Schema — reuse mask | Same `mask_pii` function applied to `cleaned_interactions` with no new function code |
# MAGIC | Catalog — new table | `new_customer_segment` created and secured in one script using existing functions |
# MAGIC
# MAGIC **The bigger picture — ABAC at scale:**
# MAGIC
# MAGIC Unity Catalog's column masks and row filters are SQL functions stored in the catalog. Because they are catalog objects:
# MAGIC - They are defined once and referenced by any number of tables
# MAGIC - A single update to the function immediately changes behavior across every table that references it
# MAGIC - New tables can inherit existing policies at creation time — security is not an afterthought
# MAGIC - `is_account_group_member()` evaluates against the querying user's identity at runtime, so the same function produces different results for different callers without any per-user configuration
# MAGIC
# MAGIC > **Up next:** Section 5 — Data lineage and audit with Unity Catalog's system tables.
