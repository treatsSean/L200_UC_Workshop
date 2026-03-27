# Databricks notebook source

# MAGIC %md
# MAGIC # Section 5: Metric Views & Lineage
# MAGIC
# MAGIC **Duration:** 15 minutes
# MAGIC
# MAGIC **Purpose:** Build gold-layer aggregation tables, define reusable business metrics with Metric Views, configure data quality monitoring, and explore Unity Catalog's automated lineage tracking — from raw ingestion all the way to column-level PII flow.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Create gold aggregation tables that reference the `score_customer_health()` UC function
# MAGIC - Apply liquid clustering and predictive optimization to the revenue summary table
# MAGIC - Define a Metric View — a YAML-declared, governance-aware metrics layer
# MAGIC - Query the metric view across multiple dimensions without rewriting SQL
# MAGIC - Enable a Lakehouse Monitor on the customer health scores table
# MAGIC - View automated lineage in Catalog Explorer (table-level and column-level)
# MAGIC - Query system tables for programmatic lineage access

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Gold Aggregation Tables
# MAGIC
# MAGIC The gold layer holds business-ready aggregations and enriched records. We create two tables:
# MAGIC
# MAGIC - `customer_health_scores` — one row per customer, enriched with transaction totals, average sentiment, and a computed health score produced by the `score_customer_health()` UC function
# MAGIC - `revenue_summary` — monthly revenue aggregated by region and product category
# MAGIC
# MAGIC Because `customer_health_scores` calls the `score_customer_health()` UC function, Unity Catalog will automatically record **function lineage** — you will be able to trace that the function contributed to this table in the lineage graph.
# MAGIC
# MAGIC > **Re-run note:** Both statements use `CREATE OR REPLACE TABLE`, so the notebook is safe to re-run at any time.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.gold.customer_health_scores AS
# MAGIC SELECT
# MAGIC   c.customer_id, c.first_name, c.last_name, c.region,
# MAGIC   COALESCE(t.total_transactions, 0) AS total_transactions,
# MAGIC   COALESCE(t.total_amount, 0) AS total_spend,
# MAGIC   COALESCE(i.avg_sentiment, 0) AS avg_sentiment,
# MAGIC   lumina_technologies.gold.score_customer_health(c.customer_id) AS health_score
# MAGIC FROM lumina_technologies.silver.cleaned_customers c
# MAGIC LEFT JOIN lumina_technologies.silver.transaction_totals t ON c.customer_id = t.customer_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT customer_id, AVG(sentiment_score) AS avg_sentiment
# MAGIC   FROM lumina_technologies.silver.cleaned_interactions
# MAGIC   GROUP BY customer_id
# MAGIC ) i ON c.customer_id = i.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lumina_technologies.gold.revenue_summary AS
# MAGIC SELECT
# MAGIC   c.region, t.product_category,
# MAGIC   DATE_TRUNC('MONTH', t.transaction_date) AS month,
# MAGIC   COUNT(*) AS transaction_count,
# MAGIC   SUM(t.amount) AS total_revenue,
# MAGIC   AVG(t.amount) AS avg_transaction_value
# MAGIC FROM lumina_technologies.silver.cleaned_transactions t
# MAGIC JOIN lumina_technologies.silver.cleaned_customers c ON t.customer_id = c.customer_id
# MAGIC GROUP BY c.region, t.product_category, DATE_TRUNC('MONTH', t.transaction_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Liquid Clustering & Predictive Optimization
# MAGIC
# MAGIC **Liquid clustering** replaces static `PARTITIONED BY` declarations with a flexible, background-managed clustering strategy. You declare which columns queries typically filter on, and Delta Lake reorganizes data files incrementally — no full table rewrites required. Clusters adapt automatically as query patterns change.
# MAGIC
# MAGIC **Predictive optimization** (enabled at the Unity Catalog level) automates VACUUM and OPTIMIZE runs based on observed workload patterns. Rather than scheduling maintenance jobs manually, UC predicts when optimization will reduce query latency and runs it proactively.
# MAGIC
# MAGIC For `revenue_summary`, filtering by `region` and `product_category` is the expected access pattern, so we cluster on those columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lumina_technologies.gold.revenue_summary
# MAGIC CLUSTER BY (region, product_category);
# MAGIC
# MAGIC OPTIMIZE lumina_technologies.gold.revenue_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Metric View
# MAGIC
# MAGIC A **Metric View** is a YAML-declared object in Unity Catalog that separates *what to measure* (measures) from *how to slice it* (dimensions). Once defined, any principal with SELECT on the view can query any combination of dimensions without writing new SQL aggregations.
# MAGIC
# MAGIC Key properties of Metric Views:
# MAGIC - **Governed** — subject to UC access control and audited like any other securable
# MAGIC - **Reusable** — business definitions (e.g., "Total Revenue") live in one place; dashboards and notebooks reference the same object
# MAGIC - **Lineage-tracked** — queries against a Metric View appear in the lineage graph, linking downstream consumers back to source tables
# MAGIC
# MAGIC The `revenue_metrics` view is defined over `revenue_summary` with three dimensions (Region, Product Category, Month) and three measures.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE METRIC VIEW lumina_technologies.gold.revenue_metrics
# MAGIC AS YAML $$
# MAGIC version: "1.1"
# MAGIC source: lumina_technologies.gold.revenue_summary
# MAGIC dimensions:
# MAGIC   - name: Region
# MAGIC     expr: region
# MAGIC   - name: Product Category
# MAGIC     expr: product_category
# MAGIC   - name: Month
# MAGIC     expr: month
# MAGIC measures:
# MAGIC   - name: Total Revenue
# MAGIC     expr: SUM(total_revenue)
# MAGIC   - name: Average Transaction Value
# MAGIC     expr: AVG(avg_transaction_value)
# MAGIC   - name: Transaction Count
# MAGIC     expr: SUM(transaction_count)
# MAGIC $$

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query the Metric View Across Dimensions
# MAGIC
# MAGIC Metric Views use the `MEASURE()` function syntax. You declare which measures you want, and the engine handles the aggregation. Changing the `GROUP BY` clause instantly changes the granularity — no SQL rewrite needed.
# MAGIC
# MAGIC The two queries below demonstrate the same metric view sliced two different ways:
# MAGIC 1. Revenue and transaction count by **region** (geographic breakdown)
# MAGIC 2. Revenue and average transaction value by **product category and month** (trend analysis)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Region, MEASURE(Total Revenue), MEASURE(Transaction Count)
# MAGIC FROM lumina_technologies.gold.revenue_metrics
# MAGIC GROUP BY Region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product_Category, Month, MEASURE(Total Revenue), MEASURE(Average Transaction Value)
# MAGIC FROM lumina_technologies.gold.revenue_metrics
# MAGIC GROUP BY Product_Category, Month
# MAGIC ORDER BY Month, Product_Category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enable Data Quality Monitoring
# MAGIC
# MAGIC A **Lakehouse Monitor** continuously profiles a table and writes statistics to output tables in a designated schema. Unity Catalog surfaces the results as **health indicator badges** directly on the table detail page in Catalog Explorer — no separate dashboard required.
# MAGIC
# MAGIC The monitor tracks:
# MAGIC - Row counts and null rates per column
# MAGIC - Distribution shifts over time (data drift detection)
# MAGIC - Custom metric thresholds you can define
# MAGIC
# MAGIC After enabling the monitor below, navigate to **Catalog Explorer → lumina_technologies → gold → customer_health_scores** and look for the **Quality** tab to see the profiling results once the first refresh completes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE LAKEHOUSE MONITOR lumina_technologies.gold.customer_health_scores_monitor
# MAGIC ON TABLE lumina_technologies.gold.customer_health_scores
# MAGIC WITH (OUTPUT_SCHEMA_NAME = 'lumina_technologies.gold')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: UI Walkthrough — Open the Lineage Graph
# MAGIC
# MAGIC Unity Catalog automatically captures lineage for every query that reads or writes a UC table. No instrumentation is required — the catalog records it from the SQL execution plan.
# MAGIC
# MAGIC Follow these steps in the Databricks UI:
# MAGIC
# MAGIC 1. Open **Catalog** in the left navigation sidebar.
# MAGIC 2. Expand **lumina_technologies → gold** and click on **customer_health_scores**.
# MAGIC 3. Select the **Lineage** tab on the table detail page.
# MAGIC 4. The graph shows upstream sources on the left and downstream consumers on the right. You should see:
# MAGIC    - **bronze.customers** → **silver.cleaned_customers** → **gold.customer_health_scores**
# MAGIC    - **silver.transaction_totals** → **gold.customer_health_scores**
# MAGIC    - **silver.cleaned_interactions** → **gold.customer_health_scores**
# MAGIC    - The **score_customer_health** UC function node linked to **gold.customer_health_scores**
# MAGIC 5. Click **See column-level lineage** (or select a specific column) to trace individual fields.
# MAGIC    - Click on `health_score` to see that it originates from the `score_customer_health()` function call.
# MAGIC    - Click on `first_name` or `email` to trace PII fields from bronze through silver into gold.
# MAGIC 6. Navigate to **silver.cleaned_customers** and check its lineage to confirm **bronze.customers** appears as the upstream source.
# MAGIC
# MAGIC > **What to look for:** The lineage graph is built from actual query history — every `CREATE TABLE AS SELECT` and `INSERT` you ran in earlier sections contributed edges to this graph automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Query Lineage System Tables
# MAGIC
# MAGIC Unity Catalog writes lineage metadata to system tables under `system.access`. You can query these tables programmatically to build impact analysis tools, compliance reports, or data catalog integrations.
# MAGIC
# MAGIC The query below retrieves all tables that contributed data to `gold.customer_health_scores` in the last 7 days. The `event_type` column distinguishes between direct writes (`CREATE`) and reads that contributed to a downstream write (`READ`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name, event_type
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'lumina_technologies.gold.customer_health_scores'
# MAGIC   AND event_date >= current_date() - 7
# MAGIC ORDER BY event_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Column-Level Lineage — Trace PII Flow
# MAGIC
# MAGIC Column-level lineage tracks which source columns contributed to each target column. This is critical for PII governance: you can prove (or discover) that sensitive fields like `email`, `phone`, and `street_address` flow from raw bronze tables all the way into gold aggregations or downstream applications.
# MAGIC
# MAGIC The query below asks: *In the last 7 days, where did `email`, `phone`, and `street_address` end up?*
# MAGIC
# MAGIC Use this to verify that PII fields are not flowing into tables or schemas where they should not appear — for example, confirming that masked silver columns do not propagate raw values to gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, source_column_name,
# MAGIC   target_table_full_name, target_column_name
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_column_name IN ('email', 'phone', 'street_address')
# MAGIC   AND event_date >= current_date() - 7
# MAGIC ORDER BY source_column_name, target_table_full_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Downstream Impact Analysis
# MAGIC
# MAGIC Lineage also answers the reverse question: *If I change or delete a source table, what downstream assets are affected?*
# MAGIC
# MAGIC The query below finds every table that has read from `bronze.customers` in the last 7 days. This is the **blast radius** of any change to that table — schema changes, row deletions, or access revocations would potentially break all of these downstream consumers.
# MAGIC
# MAGIC Use this pattern before deprecating a table, rotating credentials, or applying a new column mask to understand the impact on downstream pipelines.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT target_table_full_name
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'lumina_technologies.bronze.customers'
# MAGIC   AND event_date >= current_date() - 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint — Key Takeaways
# MAGIC
# MAGIC Take a moment to confirm your understanding before moving to Section 6.
# MAGIC
# MAGIC **What you demonstrated in this section:**
# MAGIC
# MAGIC | Capability | What happened |
# MAGIC |---|---|
# MAGIC | Gold layer tables | Created `customer_health_scores` (with UC function call) and `revenue_summary` (with region JOIN) |
# MAGIC | Liquid clustering | Declared clustering columns on `revenue_summary`; Delta Lake manages file layout automatically |
# MAGIC | Metric Views | Defined `revenue_metrics` in YAML; queried the same measures across two different dimension groupings |
# MAGIC | Data quality monitoring | Enabled a Lakehouse Monitor; health badges appear in Catalog Explorer |
# MAGIC | Automated lineage | Explored the bronze → silver → gold lineage graph in Catalog Explorer — no instrumentation required |
# MAGIC | Programmatic lineage | Queried `system.access.table_lineage` and `system.access.column_lineage` for compliance and impact analysis |
# MAGIC | PII flow tracing | Used column-level lineage to trace `email`, `phone`, and `street_address` across the pipeline |
# MAGIC | Downstream impact | Queried all tables downstream of `bronze.customers` to assess change blast radius |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog captures lineage automatically from the query execution plan — no manual tagging, no separate metadata pipeline. Combined with Metric Views and Lakehouse Monitors, you have a complete picture of *where data comes from*, *what it means*, and *whether it is healthy*, all governed by the same access control layer you configured in earlier sections.
# MAGIC
# MAGIC > **Up next:** Section 6 — AI/BI Dashboards and Genie Spaces for natural language analytics over governed data.
