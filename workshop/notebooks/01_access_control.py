# Databricks notebook source

# MAGIC %md
# MAGIC # Section 1: Access Control & AI Asset Governance
# MAGIC
# MAGIC **Duration:** 10 minutes (2 min refresher + 8 min hands-on)
# MAGIC
# MAGIC **Purpose:** Establish a foundation for the rest of the workshop by exploring how Unity Catalog enforces access control across all data and AI assets — tables, views, functions, and registered models — using a single, consistent privilege model.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Inspect existing table privileges through `information_schema`
# MAGIC - Grant `EXECUTE` on a UC function (a registered model's scoring wrapper)
# MAGIC - Transfer schema ownership to a team group
# MAGIC - Observe what an access-denied scenario looks like
# MAGIC - Query system tables to see compute governance data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Refresher — Three-Level Namespace & Privilege Model
# MAGIC
# MAGIC > **Instructor note:** Read this section aloud (~2 minutes). Attendees should follow along.
# MAGIC
# MAGIC ### Three-Level Namespace
# MAGIC
# MAGIC Unity Catalog organizes all objects in a three-level hierarchy:
# MAGIC
# MAGIC ```
# MAGIC catalog
# MAGIC └── schema
# MAGIC     └── table | view | function | model | volume
# MAGIC ```
# MAGIC
# MAGIC Every object has a fully-qualified name: `catalog.schema.object_name`.
# MAGIC
# MAGIC ### Privilege Cascade
# MAGIC
# MAGIC Privileges flow **downward** but do not automatically inherit upward:
# MAGIC - To read a table, a user needs `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `SELECT` on the table.
# MAGIC - Granting `SELECT` on a table does **not** automatically grant `USE CATALOG` or `USE SCHEMA` — those must be granted separately.
# MAGIC - Granting at the catalog level can cascade to all schemas and objects below it.
# MAGIC
# MAGIC ### Ownership vs. Grants
# MAGIC
# MAGIC - **Owner:** Has full control over an object and can grant privileges to others. Ownership is transferred with `ALTER ... OWNER TO`.
# MAGIC - **Grants:** Fine-grained privileges (`SELECT`, `MODIFY`, `EXECUTE`, etc.) that owners or privileged users assign to principals (users, service principals, groups).
# MAGIC
# MAGIC > **L100 Reference:** If this is new territory, watch the Unity Catalog Fundamentals video in the L100 series before proceeding.

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Inspect Existing Table Privileges
# MAGIC
# MAGIC Before making any changes, it is good practice to understand what privileges already exist. The `information_schema.table_privileges` view provides a queryable record of all grants on tables within a catalog.
# MAGIC
# MAGIC **Why this matters:** In a governed environment, you should always audit before you act. This query gives you a snapshot of who has access to what across the bronze, silver, and gold schemas — the three layers of the Medallion architecture used in this workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lumina_technologies.information_schema.table_privileges
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC ORDER BY table_schema, table_name, grantee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Grant Access to the Registered ML Model
# MAGIC
# MAGIC The ML team has registered a customer churn model (`lumina_technologies.gold.customer_churn_model`). In Unity Catalog, registered models are first-class securables — governed with the same `GRANT` syntax you use for tables and functions.
# MAGIC
# MAGIC **Key concept — AI assets are governed like data assets:** A data scientist cannot load or deploy a registered model unless they have been explicitly granted `SELECT` on it — just as an analyst cannot query a table without `SELECT`. The scoring function that wraps this model (`score_customer_health`) was already granted `EXECUTE` during setup, so the model and its wrapper are now independently governed.
# MAGIC
# MAGIC This uniformity is what makes AI governance tractable: your existing data access-control processes extend naturally to ML models without a separate toolchain.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE lumina_technologies.gold.customer_churn_model TO `data_platform_admins`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transfer Schema Ownership
# MAGIC
# MAGIC Ownership determines who can manage an object going forward — including granting access to others and dropping the object. It is common practice to transfer ownership from an individual service principal (used during provisioning) to a team group so that no single person is a bottleneck.
# MAGIC
# MAGIC Here we transfer ownership of the `gold` schema to the `data_platform_admins` group. This means any member of that group can now manage grants on all objects within `gold`, including the `score_customer_health` function we just updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SCHEMA lumina_technologies.gold OWNER TO `data_platform_admins`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Access Denied — What Happens Without Grants
# MAGIC
# MAGIC This cell demonstrates what a user without the appropriate privileges would experience. The `restricted` schema contains sensitive data that has not been granted to the workshop attendees.
# MAGIC
# MAGIC **Instructor note:** Run this cell and walk through the error message with attendees. Notice that:
# MAGIC - The error clearly identifies which privilege is missing and on which object.
# MAGIC - Unity Catalog does **not** reveal the existence or schema of an object a user has no access to — this is part of its security model.
# MAGIC - The user is not told *why* they lack access (e.g., no one forgot to grant it), only *that* they lack it.
# MAGIC
# MAGIC This behavior is by design. It prevents data enumeration attacks where an attacker probes for object names.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This should fail: try to query without grants
# MAGIC -- (Instructor explains what would happen for a user without permissions)
# MAGIC SELECT * FROM lumina_technologies.restricted.sensitive_data LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Compute Governance via System Tables
# MAGIC
# MAGIC Access control is not limited to data objects. Unity Catalog's system tables expose operational metadata — including compute usage — that you can query with the same SQL interface you use for your own data.
# MAGIC
# MAGIC `system.billing.usage` records DBU consumption per SKU, allowing platform teams to track spending, enforce budgets, and audit which workloads are driving costs. This data is governed by Unity Catalog itself: only users with appropriate access to the `system` catalog can query it.
# MAGIC
# MAGIC The query below summarizes the last 7 days of usage grouped by SKU and unit — a quick operational health check.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   usage_unit,
# MAGIC   SUM(usage_quantity) AS total_usage
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - 7
# MAGIC GROUP BY sku_name, usage_unit
# MAGIC ORDER BY total_usage DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided Checkpoint
# MAGIC
# MAGIC Take 60 seconds to discuss with a neighbor or reflect on the following:
# MAGIC
# MAGIC **Key takeaway:** Unity Catalog enforces a single, consistent security model across every asset type — tables, views, volumes, functions, and registered ML models. Granting `EXECUTE` on a scoring function is structurally identical to granting `SELECT` on a table. This uniformity is what makes AI governance tractable at scale: your data governance processes extend naturally to your AI assets without requiring a separate toolchain.
# MAGIC
# MAGIC **Questions to consider:**
# MAGIC - In your current environment, who owns your ML model artifacts? Is that ownership tracked in a system of record?
# MAGIC - What would it take to audit, right now, every principal that can invoke a production model in your organization?
# MAGIC - How does the privilege cascade interact with dynamic views or row-level security filters applied in the silver layer?
# MAGIC
# MAGIC **Up next — Section 2a:** Loading Data & AI Classification. You will load raw data into bronze tables and use AI functions to detect and classify sensitive information.
