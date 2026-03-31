# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2b: Tagging & Governance Metadata
# MAGIC
# MAGIC **Duration:** 7 minutes
# MAGIC
# MAGIC **Purpose:** Apply Unity Catalog tags to the PII columns you identified in Section 2a. Tags are the bridge between classification (what the AI found) and enforcement (the ABAC policies you will configure in Section 4b). They also enable discovery — anyone querying `system.information_schema.column_tags` can instantly find every sensitive column across the catalog.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Apply PII tags to bronze, silver, and gold table columns based on Section 2a classification results
# MAGIC - Learn the difference between ad-hoc tags and governed tags
# MAGIC - Tag a certification status onto a table so data consumers know it is trustworthy
# MAGIC - Tag the `customer_churn_model` registered model the same way you tag data assets
# MAGIC - Query `system.information_schema.column_tags` to verify your work

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Apply PII Tags to Bronze Layer Columns
# MAGIC
# MAGIC In Section 2a, the AI classification function flagged three columns in `bronze.customers` as containing personally identifiable information. Now we make that classification durable and machine-readable by writing it as a tag directly on each column.
# MAGIC
# MAGIC Two tags are applied together on each column:
# MAGIC - `pii = 'true'` — a boolean flag used by access control policies in Section 4b
# MAGIC - `sensitivity_level = 'high' | 'medium'` — a graduated signal for downstream tooling (masking policies, export controls, audit alerts)
# MAGIC
# MAGIC `email` and `phone` are rated `high` because they are direct identifiers that can be used to contact or authenticate an individual. `street_address` is rated `medium` — it is sensitive in combination with other fields but is less directly exploitable on its own.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.bronze.customers
# MAGIC   ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity_level' = 'high');
# MAGIC ALTER TABLE lumina_technologies.bronze.customers
# MAGIC   ALTER COLUMN phone SET TAGS ('pii' = 'true', 'sensitivity_level' = 'high');
# MAGIC ALTER TABLE lumina_technologies.bronze.customers
# MAGIC   ALTER COLUMN street_address SET TAGS ('pii' = 'true', 'sensitivity_level' = 'medium');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: Apply Tags via Catalog Explorer (UI)
# MAGIC
# MAGIC You just applied tags using SQL — but the same tags can be applied through the Catalog Explorer UI. This is especially useful for data stewards or governance teams who prefer a visual workflow and want to tag assets at scale without writing SQL.
# MAGIC
# MAGIC **Exercise — try it yourself:**
# MAGIC
# MAGIC 1. Open **Catalog Explorer** from the left navigation bar.
# MAGIC 2. Navigate to `lumina_technologies` → `bronze` → `transactions`.
# MAGIC 3. Click the **Tags** tab on the table detail page.
# MAGIC 4. Add a table-level tag: key = `team`, value = `finance`.
# MAGIC 5. Scroll down to the **Columns** section. Click on `customer_id`.
# MAGIC 6. In the column detail panel, add a tag: key = `pii`, value = `true`.
# MAGIC 7. Add a second tag: key = `sensitivity_level`, value = `medium`.
# MAGIC
# MAGIC **Key point:** Whether you apply tags via SQL or the UI, the result is identical — both write to the same underlying Unity Catalog metadata store. Tags applied in the UI are immediately visible in `information_schema.column_tags` queries, and vice versa. Choose whichever workflow fits your team: SQL for automation and bulk operations, the UI for ad-hoc tagging and review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Apply PII Tags to Silver Layer Columns
# MAGIC
# MAGIC The silver `cleaned_customers` table was derived from `bronze.customers`. The ETL pipeline cleaned and standardized the values, but it did not remove the PII — the data is still sensitive. Tags do not propagate automatically through transformations, so we apply them explicitly here.
# MAGIC
# MAGIC **Why this matters:** Without tagging at every layer, a governance policy applied to bronze provides no protection once data flows downstream. Tagging silver closes that gap and ensures your ABAC policies in Section 4b fire correctly regardless of which layer a consumer queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC   ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity_level' = 'high');
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC   ALTER COLUMN phone SET TAGS ('pii' = 'true', 'sensitivity_level' = 'high');
# MAGIC ALTER TABLE lumina_technologies.silver.cleaned_customers
# MAGIC   ALTER COLUMN street_address SET TAGS ('pii' = 'true', 'sensitivity_level' = 'medium');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply PII Tags to Gold Layer Columns
# MAGIC
# MAGIC The `gold.customer_health_scores` table aggregates behavioral metrics per customer. It does not replicate raw PII columns like `email` or `street_address`, but it does retain `customer_id` — a stable identifier that links back to an individual. We tag it as PII with `sensitivity_level = 'medium'` to reflect that risk.
# MAGIC
# MAGIC Columns that are purely derived metrics (`health_score`, `churn_risk`) contain no PII and are intentionally left untagged. Only tag what is actually sensitive — over-tagging dilutes the signal and creates noise for the access control policies that act on these tags.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.gold.customer_health_scores
# MAGIC   ALTER COLUMN customer_id SET TAGS ('pii' = 'true', 'sensitivity_level' = 'medium');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ad-Hoc Tags vs. Governed Tags
# MAGIC
# MAGIC Unity Catalog supports two tagging modes:
# MAGIC
# MAGIC **Ad-hoc tags** — Any user with `APPLY TAG` privilege can create a tag with any key-value pair on the fly. They are useful for lightweight, team-level annotations (ownership, project codes, work-in-progress markers). The trade-off is that ad-hoc tags are free-form: there is nothing preventing one team from writing `team = 'analytics'` and another from writing `team = 'Analytics'` or `team = 'analytics-team'`, leading to inconsistent results in tag-based queries and policies.
# MAGIC
# MAGIC **Governed tags** — Created by a Metastore Admin using a centrally managed tag vocabulary. Each tag name has an optional set of allowed values, and Unity Catalog enforces that only those values can be used. For example, a `sensitivity_level` governed tag might only permit `'low'`, `'medium'`, or `'high'` — preventing typos like `'hgh'` or ad-hoc additions like `'very-high'` from slipping into your policy-critical metadata.
# MAGIC
# MAGIC The `pii` and `sensitivity_level` tags used in this workshop are governed tags set up in advance. The `team` tag below is ad-hoc — created inline and valid only as an organizational convenience.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ad-hoc tag: any user with APPLY TAG privilege can create this
# MAGIC ALTER TABLE lumina_technologies.bronze.customers SET TAGS ('team' = 'analytics');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Certification Status
# MAGIC
# MAGIC Unity Catalog has a built-in concept of **certification status** that communicates data quality and trustworthiness to consumers. Certified tables signal that the data has been validated, is actively maintained, and is appropriate for production use. The counterpart — deprecated status — warns consumers that a table is being retired and should no longer be used for new workloads.
# MAGIC
# MAGIC Certification lifecycle:
# MAGIC 1. **Uncertified (default):** Newly landed or experimental data. No guarantees on quality or continuity.
# MAGIC 2. **Certified:** Validated, documented, and actively maintained. Safe for production dependencies.
# MAGIC 3. **Deprecated:** Scheduled for removal. Consumers should migrate to a replacement before the decommission date.
# MAGIC
# MAGIC Certification is applied as a system tag using the `system.` prefix namespace. The `system.certification_status` tag is recognized by Databricks Data Intelligence Platform UI components (catalog explorer, lineage graph) and displayed as a badge on the table detail page.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.bronze.customers
# MAGIC   SET TAGS ('system.certification_status' = 'certified');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Tag AI Assets
# MAGIC
# MAGIC AI assets — registered models and pipelines — are first-class citizens in Unity Catalog and can be tagged using the same governance metadata as tables. This means your `information_schema` queries and discovery workflows apply uniformly to both data assets and AI assets without a separate governance workflow.
# MAGIC
# MAGIC Here we tag the `customer_churn_model` registered model:
# MAGIC - `pii = 'false'` — the model itself does not store PII (it processes inputs at inference time and returns a score)
# MAGIC - `sensitivity_level = 'low'` — the output (a churn prediction) is not sensitive on its own
# MAGIC - `asset_type = 'ml_model'` — a discovery tag that lets platform teams quickly enumerate all ML models across the catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER REGISTERED MODEL lumina_technologies.gold.customer_churn_model
# MAGIC   SET TAGS ('pii' = 'false', 'sensitivity_level' = 'low', 'asset_type' = 'ml_model');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Search Tagged Assets
# MAGIC
# MAGIC Tags are queryable through `system.information_schema.column_tags` (for column-level tags) and `system.information_schema.table_tags` (for table-level tags). This makes tag-based discovery a standard SQL query — no special tooling required.
# MAGIC
# MAGIC The query below retrieves every column in the `lumina_technologies` catalog tagged with `pii = 'true'`. This is the same query your ABAC policies in Section 4b will use to determine which columns to mask or restrict. Running it now lets you verify that all three layers (bronze, silver, gold) are covered before you build enforcement on top of this metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   catalog_name, schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE catalog_name = 'lumina_technologies'
# MAGIC   AND tag_name = 'pii'
# MAGIC   AND tag_value = 'true'
# MAGIC ORDER BY schema_name, table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided Checkpoint
# MAGIC
# MAGIC Take 60 seconds to discuss with a neighbor or reflect on the following:
# MAGIC
# MAGIC **Key takeaway:** Tags in Unity Catalog are not documentation — they are operational metadata. The `pii = 'true'` tags you just applied will be read by the dynamic views and column masks you build in Section 4b. The certification status badge you set is surfaced directly in the catalog UI. The `asset_type = 'ml_model'` tag is queryable by platform automation scripts. Every tag you write here has a downstream consumer, which is why governed tags (with controlled vocabularies) matter more than they might initially appear.
# MAGIC
# MAGIC **Questions to consider:**
# MAGIC - Who in your organization should own the governed tag vocabulary? A central data governance team, or the owners of each domain?
# MAGIC - How would you handle PII that flows into a table through a join at query time rather than being stored in the table itself?
# MAGIC - What is the correct certification status for a table that passes data quality checks but has no SLA?
# MAGIC
# MAGIC **Up next — Section 3:** Discovery & Domains. You will create a domain in Catalog Explorer, browse the Discovery page, and explore how tags and domains make data assets findable across the organization.
