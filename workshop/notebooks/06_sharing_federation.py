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
# MAGIC - Inject upstream (Snowflake) and downstream (Power BI) lineage via the BYOL API
# MAGIC - Walk through the end-to-end lineage graph in the UI

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
# MAGIC ## Part 3: BYOL Lineage & External Engine Reads (4 min)
# MAGIC
# MAGIC **Bring-Your-Own Lineage (BYOL)** lets you inject lineage metadata into Unity Catalog for data movement that UC cannot observe directly — for example, a Snowflake transform that produces a table that is later ingested into Databricks, or a Power BI dashboard that reads from a UC gold table. By injecting that metadata, you complete the end-to-end lineage graph so data stewards can trace data from its origin system all the way to its consumer, without gaps.
# MAGIC
# MAGIC BYOL relationships are injected via the Databricks SDK (`w.external_lineage`) or the REST API (`/api/2.0/lineage-tracking/external-lineage`). Each relationship consists of a *source* and a *target*, each of which can be either a UC table (identified by its three-part name) or an external object (identified by its system name and a URI).
# MAGIC
# MAGIC Once injected, external lineage nodes appear in the Catalog Explorer lineage graph alongside native UC lineage, with a distinct icon indicating the external system.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Inject Upstream Lineage (Snowflake → Bronze)
# MAGIC
# MAGIC The `lumina_technologies.bronze.customers` table was originally loaded from a Snowflake CRM system. UC did not observe that ingestion, so the lineage graph currently shows `bronze.customers` with no upstream. We inject that relationship here.
# MAGIC
# MAGIC > **SDK version note:** The `external_lineage` API and its import paths may vary across Databricks SDK versions. The cell uses `try/except` to handle import variations gracefully and falls back to the REST API if the SDK path is unavailable.

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        CreateExternalLineageRequest,
        ExternalLineageObject,
        ExternalLineageObjectType,
    )

    w = WorkspaceClient()

    w.external_lineage.create_external_lineage_relationship(
        source=ExternalLineageObject(
            name="snowflake.crm.raw_contacts",
            system="snowflake",
            object_type=ExternalLineageObjectType.TABLE,
        ),
        target=ExternalLineageObject(
            name="lumina_technologies.bronze.customers",
            object_type=ExternalLineageObjectType.TABLE,
        ),
    )
    print("[OK] Upstream Snowflake → bronze.customers lineage injected via SDK.")

except ImportError:
    # Fall back to the REST API if the SDK does not yet expose this path
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    body = {
        "source": {
            "external_system": "snowflake",
            "external_object": "snowflake://lumina_demo.snowflakecomputing.com/CRM/CRM_SCHEMA/raw_contacts",
        },
        "target": {
            "table_full_name": "lumina_technologies.bronze.customers",
        },
    }
    try:
        w.api_client.do("POST", "/api/2.0/lineage-tracking/external-lineage", body=body)
        print("[OK] Upstream Snowflake → bronze.customers lineage injected via REST API.")
    except Exception as e:
        print(f"[WARN] REST fallback also failed: {e}")

except Exception as e:
    print(f"[WARN] Could not inject upstream lineage: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Inject Downstream Lineage (Gold → Power BI)
# MAGIC
# MAGIC The `lumina_technologies.gold.revenue_summary` table feeds a Power BI dashboard. That consumption is invisible to UC, so the lineage graph currently shows `gold.revenue_summary` with no downstream. We complete the graph by injecting the downstream relationship.

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        CreateExternalLineageRequest,
        ExternalLineageObject,
        ExternalLineageObjectType,
    )

    w = WorkspaceClient()

    w.external_lineage.create_external_lineage_relationship(
        source=ExternalLineageObject(
            name="lumina_technologies.gold.revenue_summary",
            object_type=ExternalLineageObjectType.TABLE,
        ),
        target=ExternalLineageObject(
            name="powerbi.workspace.revenue_dashboard",
            system="powerbi",
            object_type=ExternalLineageObjectType.DASHBOARD,
        ),
    )
    print("[OK] Downstream gold.revenue_summary → Power BI lineage injected via SDK.")

except ImportError:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    body = {
        "source": {
            "table_full_name": "lumina_technologies.gold.revenue_summary",
        },
        "target": {
            "external_system": "powerbi",
            "external_object": "powerbi://app.powerbi.com/groups/lumina-workspace/datasets/revenue_dashboard",
        },
    }
    try:
        w.api_client.do("POST", "/api/2.0/lineage-tracking/external-lineage", body=body)
        print("[OK] Downstream gold.revenue_summary → Power BI lineage injected via REST API.")
    except Exception as e:
        print(f"[WARN] REST fallback also failed: {e}")

except Exception as e:
    print(f"[WARN] Could not inject downstream lineage: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: View the End-to-End Lineage Graph in the UI
# MAGIC
# MAGIC **Instructor walkthrough (~2 min):**
# MAGIC
# MAGIC 1. Open **Catalog** in the left navigation bar.
# MAGIC 2. Navigate to `lumina_technologies` → `gold` → `revenue_summary`.
# MAGIC 3. Click the **Lineage** tab.
# MAGIC 4. You should see the full end-to-end chain:
# MAGIC
# MAGIC ```
# MAGIC Snowflake (snowflake.crm.raw_contacts)
# MAGIC   └── bronze.customers
# MAGIC         └── silver.cleaned_customers
# MAGIC               └── gold.customer_health_scores
# MAGIC               └── gold.revenue_summary
# MAGIC                     └── Power BI (powerbi.workspace.revenue_dashboard)
# MAGIC ```
# MAGIC
# MAGIC 5. Click on the Snowflake node — notice it shows the external system name and object URI, not a UC three-part name.
# MAGIC 6. Click on the Power BI node — same pattern for the downstream consumer.
# MAGIC 7. Point out that the native UC lineage (bronze → silver → gold) was captured automatically; only the external boundary hops required BYOL injection.
# MAGIC
# MAGIC > **Key insight:** The lineage graph is now complete. A data steward auditing `revenue_summary` can trace data provenance all the way back to the originating Snowflake CRM table — and trace forward to every dashboard consuming it — without leaving Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10: External Engine Consistency
# MAGIC
# MAGIC When an external engine (Trino, Flink, a custom Python script, a third-party ETL tool) reads from or writes to a UC-managed table using the Iceberg REST Catalog API or Delta Sharing, Unity Catalog records that access in its audit and lineage systems just as it would for a native Databricks query.
# MAGIC
# MAGIC This means:
# MAGIC - **Audit:** `system.access.audit` captures reads and writes from external engines that authenticate through UC.
# MAGIC - **Lineage:** Read operations from external engines appear as downstream nodes in the lineage graph automatically (no BYOL injection required for engines that go through the UC API).
# MAGIC - **Governance:** Row-level security filters and column masks defined in UC are enforced for external engine reads that go through the Iceberg REST endpoint — the engine receives only the rows and columns it is authorized to see.
# MAGIC
# MAGIC BYOL injection is only needed for external systems that connect directly to cloud storage (bypassing UC entirely) or for systems that have no UC integration — for example, a legacy on-premises ETL tool that writes files directly to S3 before a Databricks job picks them up.

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
# MAGIC | BYOL upstream | Injected Snowflake → `bronze.customers` lineage |
# MAGIC | BYOL downstream | Injected `gold.revenue_summary` → Power BI lineage |
# MAGIC | End-to-end lineage | Traced the full chain from external source to external consumer in the UC UI |
# MAGIC
# MAGIC **The bigger picture:**
# MAGIC
# MAGIC Unity Catalog is the single control plane for data governance across organizational boundaries:
# MAGIC - **Delta Sharing** extends governance to data consumers outside your organization, without copying data.
# MAGIC - **Lakehouse Federation** extends governance to data sources outside Databricks, without moving data.
# MAGIC - **Managed Iceberg** extends interoperability to non-Databricks compute engines, without forking your catalog.
# MAGIC - **BYOL lineage** extends provenance tracking to systems that UC cannot instrument directly, completing the audit trail.
# MAGIC
# MAGIC > **Workshop complete.** You have walked through the full Unity Catalog governance stack: access control, data classification and tagging, discovery and domains, data integrity, row/column security, observability, and now sharing and federation. Every capability operates through the same catalog, the same privilege model, and the same lineage infrastructure.
