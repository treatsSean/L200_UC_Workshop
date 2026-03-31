# Databricks notebook source

# MAGIC %md
# MAGIC # Section 6: Sharing & Federation
# MAGIC
# MAGIC **Duration:** 10 minutes (3 min Delta Sharing + 3 min Federation & Iceberg + 4 min BYOL Lineage)
# MAGIC
# MAGIC **Purpose:** Unity Catalog is not just a governance layer for data you own inside a single workspace. It is the control plane for data you share with external partners (Delta Sharing), data you federate from external systems (Lakehouse Federation), open table formats exposed via standard APIs (managed Iceberg), and lineage that crosses system boundaries (BYOL lineage). This section ties all of those capabilities together.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Create a Delta Share and add gold tables to it
# MAGIC - Create a recipient and inspect share contents
# MAGIC - Create a foreign catalog over a federated external source
# MAGIC - Query an external table through Unity Catalog as if it were native
# MAGIC - Understand how UC exposes managed tables via the Iceberg REST Catalog API
# MAGIC - Understand how BYOL lineage completes the provenance graph for external systems
# MAGIC - Explore the real lineage graph built by your workshop queries — in the UI and via system tables
# MAGIC - Trace PII column flow programmatically using `system.access.column_lineage`

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Delta Sharing (3 min)
# MAGIC
# MAGIC **Delta Sharing** is an open protocol for sharing live data across organizations without copying it. The sharing party (provider) creates a *share* — a named collection of tables — and grants access to *recipients*. Recipients query data through their own tools (Databricks, Power BI, pandas, etc.) without ever touching the provider's storage credentials.
# MAGIC
# MAGIC Key points:
# MAGIC - Data stays in the provider's storage — no ETL, no copy
# MAGIC - Share contents are always live (the recipient reads the current version of the table)
# MAGIC - Governed through Unity Catalog: shares, recipients, and grants are first-class UC objects
# MAGIC - Supports Delta tables, views, volumes, and notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Share
# MAGIC
# MAGIC A **share** is a named container that holds references to tables (and optionally, partitions or specific Delta versions of those tables). We will create a share called `lumina_gold_share` and add two gold tables to it.
# MAGIC
# MAGIC > **Re-run note:** Both statements use `IF NOT EXISTS` and `ADD TABLE`, so they are safe to re-run. If the tables are already in the share, `ALTER SHARE ... ADD TABLE` will no-op.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS lumina_gold_share;
# MAGIC ALTER SHARE lumina_gold_share ADD TABLE lumina_technologies.gold.customer_health_scores;
# MAGIC ALTER SHARE lumina_gold_share ADD TABLE lumina_technologies.gold.revenue_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a Recipient
# MAGIC
# MAGIC A **recipient** is the identity that will consume the share. For open sharing (outside Databricks), the recipient receives a bearer token. For Databricks-to-Databricks sharing, the recipient is identified by a metastore sharing ID and no credential exchange is needed.
# MAGIC
# MAGIC We create a basic open-sharing recipient here for demonstration purposes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE RECIPIENT IF NOT EXISTS workshop_partner;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Inspect Share Contents
# MAGIC
# MAGIC `SHOW ALL IN SHARE` returns every object — tables, volumes, notebooks — that has been added to the share, along with partition filters and shared-as aliases if any were set.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE lumina_gold_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Lakehouse Federation & Iceberg (3 min)
# MAGIC
# MAGIC **Lakehouse Federation** lets you query data that lives outside Databricks — in PostgreSQL, MySQL, Snowflake, BigQuery, SQL Server, and others — through Unity Catalog, without moving or copying that data. You register a *connection* (the credentials to the external system), then create a *foreign catalog* on top of it. From that point on, Unity Catalog governs access to external tables with the same privilege model as native tables.
# MAGIC
# MAGIC Benefits:
# MAGIC - Unified governance: one place to manage access to all data, regardless of where it lives
# MAGIC - No ETL required for read-heavy cross-system queries
# MAGIC - Full lineage tracking: queries against foreign tables appear in system lineage tables
# MAGIC - External tables show up in Catalog Explorer alongside native Delta and Iceberg tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create a Foreign Catalog
# MAGIC
# MAGIC A **foreign catalog** is backed by a *connection* — a UC-managed credential object that stores the host, port, and authentication details for an external data system.
# MAGIC
# MAGIC > **Prerequisites note:** The `CREATE FOREIGN CATALOG` statement below requires a pre-configured connection named `workshop_federation_connection`. If that connection does not exist in your workspace, this cell will fail with a "connection not found" error. This is expected — the instructor will demonstrate this step live, or you can skip ahead to Part 3. A workspace admin can create the connection in Catalog Explorer under **External Data → Connections → Add connection**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS external_data
# MAGIC USING CONNECTION workshop_federation_connection;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Query an External Table Through Unity Catalog
# MAGIC
# MAGIC Once the foreign catalog exists, you query it exactly like any native Unity Catalog table. UC translates the query into the external system's dialect and pushes it down for execution. The result is returned to your cluster or SQL warehouse as a standard Spark DataFrame or result set.
# MAGIC
# MAGIC UC treats external tables like native ones: the same `SELECT` privilege model applies, the same audit logs are written, and the same lineage is captured.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM external_data.public.sample_table LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Managed Iceberg via the UC REST Catalog API
# MAGIC
# MAGIC Unity Catalog manages Delta tables natively, but it also exposes those tables as **Iceberg** format via the **Iceberg REST Catalog API**. Any Iceberg-compatible engine — Apache Spark (non-Databricks), Trino, Flink, DuckDB — can connect to UC's REST endpoint, enumerate tables, and read data directly from cloud storage using standard Iceberg metadata.
# MAGIC
# MAGIC This means:
# MAGIC - Your Databricks-managed tables are not locked in. Any Iceberg-aware engine can read them.
# MAGIC - You do not need to maintain a separate Iceberg catalog — UC is the catalog.
# MAGIC - Write-back (external engines writing to UC-managed Iceberg tables) is also supported where the external engine supports the Iceberg REST Catalog write path.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. UC exposes an Iceberg REST Catalog endpoint at `https://<workspace-host>/api/2.1/unity-catalog/iceberg`.
# MAGIC 2. An external engine authenticates with a Databricks OAuth token or PAT.
# MAGIC 3. The engine calls standard Iceberg REST API endpoints (`/v1/namespaces`, `/v1/namespaces/{ns}/tables`, `/v1/namespaces/{ns}/tables/{table}/metadata`) to discover schema and file locations.
# MAGIC 4. The engine reads Parquet/ORC data files from cloud storage directly, using the presigned URLs returned by UC.
# MAGIC
# MAGIC > **No hands-on step here.** This is a discussion point. The instructor will show the UC Iceberg endpoint URL and optionally run a DuckDB or Trino query against it from outside Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: BYOL Lineage, External Engines & Workshop Lineage Review (4 min)
# MAGIC
# MAGIC ### Concept: Bring-Your-Own Lineage (BYOL)
# MAGIC
# MAGIC Unity Catalog automatically captures lineage for every query that runs through Databricks. But data rarely lives in a single system. Your bronze tables may originate from Snowflake, your gold tables may feed Power BI dashboards, and legacy ETL tools may write files directly to cloud storage. UC cannot observe those external hops — so the lineage graph has gaps at the boundaries.
# MAGIC
# MAGIC **Bring-Your-Own Lineage (BYOL)** closes those gaps. It lets you inject lineage relationships into Unity Catalog for data movement that UC cannot instrument directly. Once injected, external nodes appear in the Catalog Explorer lineage graph alongside native UC lineage, with a distinct icon indicating the external system.
# MAGIC
# MAGIC **How it works:**
# MAGIC - BYOL relationships are injected via the Databricks SDK (`w.external_lineage`) or the REST API (`/api/2.0/lineage-tracking/external-lineage`).
# MAGIC - Each relationship consists of a *source* and a *target*, each of which can be a UC table (three-part name) or an external object (system name + URI).
# MAGIC - External lineage is additive — it does not replace or interfere with automatically captured lineage.
# MAGIC
# MAGIC **Example: What a complete lineage graph would look like**
# MAGIC
# MAGIC ```
# MAGIC Snowflake (snowflake.crm.raw_contacts)          ← BYOL injected
# MAGIC   └── bronze.customers                           ← auto-captured
# MAGIC         └── silver.cleaned_customers              ← auto-captured
# MAGIC               └── gold.customer_health_scores     ← auto-captured
# MAGIC               └── gold.revenue_summary            ← auto-captured
# MAGIC                     └── Power BI (revenue_dashboard)  ← BYOL injected
# MAGIC ```
# MAGIC
# MAGIC The middle three hops (bronze → silver → gold) were captured automatically by UC from the queries you ran in earlier sections. Only the external boundary hops — Snowflake at the top and Power BI at the bottom — would require BYOL injection.
# MAGIC
# MAGIC **When to use BYOL vs. when UC captures lineage automatically:**
# MAGIC
# MAGIC | Scenario | Lineage method |
# MAGIC |---|---|
# MAGIC | Databricks notebook or job reads/writes UC tables | Automatic — UC captures it |
# MAGIC | External engine reads via Iceberg REST Catalog API | Automatic — UC captures it |
# MAGIC | External engine reads via Delta Sharing | Automatic — UC captures it |
# MAGIC | Data ingested from Snowflake/Kafka/S3 by a non-UC tool | BYOL injection needed |
# MAGIC | Downstream dashboard (Power BI, Tableau) reads from UC | BYOL injection needed |
# MAGIC | Legacy ETL writes files directly to cloud storage | BYOL injection needed |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Explore the Lineage You Built in This Workshop
# MAGIC
# MAGIC Rather than injecting synthetic external lineage, let's explore the real lineage that UC captured automatically from the queries you ran in earlier sections. This is the lineage your platform team would use for impact analysis, compliance audits, and debugging data quality issues.
# MAGIC
# MAGIC **Exercise — view lineage in Catalog Explorer:**
# MAGIC
# MAGIC 1. Open **Catalog** in the left navigation bar.
# MAGIC 2. Navigate to `lumina_technologies` → `gold` → `customer_health_scores`.
# MAGIC 3. Click the **Lineage** tab.
# MAGIC 4. You should see upstream sources:
# MAGIC    - `silver.cleaned_customers` → `gold.customer_health_scores`
# MAGIC    - `silver.transaction_totals` → `gold.customer_health_scores`
# MAGIC    - `silver.cleaned_interactions` → `gold.customer_health_scores`
# MAGIC    - The `score_customer_health` UC function linked to the table
# MAGIC 5. Click **See column-level lineage** and select `health_score` to confirm it traces back to the UC function.
# MAGIC 6. Navigate to `gold.revenue_summary` and check its lineage to see the join between `silver.cleaned_transactions` and `silver.cleaned_customers`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Query Lineage Programmatically
# MAGIC
# MAGIC The lineage you just viewed in the UI is also queryable via `system.access.table_lineage`. This is what you would use to build automated impact analysis, compliance reports, or data catalog integrations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All upstream sources for the gold customer health scores table
# MAGIC SELECT source_table_full_name, target_table_full_name, entity_type, event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'lumina_technologies.gold.customer_health_scores'
# MAGIC   AND event_date >= current_date() - 7
# MAGIC ORDER BY event_time DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Downstream impact: everything that depends on bronze.customers
# MAGIC SELECT DISTINCT target_table_full_name
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'lumina_technologies.bronze.customers'
# MAGIC   AND event_date >= current_date() - 7

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column-level PII flow: trace where email, phone, and street_address ended up
# MAGIC SELECT source_table_full_name, source_column_name,
# MAGIC        target_table_full_name, target_column_name
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_column_name IN ('email', 'phone', 'street_address')
# MAGIC   AND event_date >= current_date() - 7
# MAGIC ORDER BY source_column_name, target_table_full_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided Checkpoint
# MAGIC
# MAGIC Before wrapping up, take a moment to confirm your understanding of what was covered in this final section.
# MAGIC
# MAGIC **What you demonstrated:**
# MAGIC
# MAGIC | Capability | What happened |
# MAGIC |---|---|
# MAGIC | Delta Sharing | Created `lumina_gold_share`, added two gold tables, created `workshop_partner` recipient |
# MAGIC | Share inspection | Used `SHOW ALL IN SHARE` to enumerate share contents |
# MAGIC | Lakehouse Federation | Created `external_data` foreign catalog over `workshop_federation_connection` |
# MAGIC | External table query | Queried `external_data.public.sample_table` through UC as if it were native |
# MAGIC | Managed Iceberg | Discussed how UC exposes Delta tables via the Iceberg REST Catalog API |
# MAGIC | BYOL lineage (concept) | Understood how external lineage injection completes the provenance graph across system boundaries |
# MAGIC | Workshop lineage review | Explored the real lineage graph built by your queries throughout this workshop — in the UI and via system tables |
# MAGIC | PII flow tracing | Used column-level lineage to trace `email`, `phone`, and `street_address` across the pipeline |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog is the single control plane for data governance across organizational boundaries:
# MAGIC - **Delta Sharing** extends governance to data consumers outside your organization, without copying data.
# MAGIC - **Lakehouse Federation** extends governance to data sources outside Databricks, without moving data.
# MAGIC - **Managed Iceberg** extends interoperability to non-Databricks compute engines, without forking your catalog.
# MAGIC - **BYOL lineage** extends provenance tracking to systems that UC cannot instrument directly, completing the audit trail.
# MAGIC - **Automatic lineage** captures every query that runs through Databricks — the lineage graph you explored was built without any manual instrumentation.
# MAGIC
# MAGIC > **Workshop complete.** You have walked through the full Unity Catalog governance stack: access control, data classification and tagging, discovery and domains, data integrity, row/column security, observability, and now sharing and federation. Every capability operates through the same catalog, the same privilege model, and the same lineage infrastructure.
