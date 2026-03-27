# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Teardown — Lumina Technologies UC Governance
# MAGIC
# MAGIC **Run this notebook as an instructor after the workshop to clean up all provisioned resources.**
# MAGIC
# MAGIC This notebook tears down everything created by `00_setup_workspace.py` and the lab notebooks,
# MAGIC in reverse dependency order. Every step uses `IF EXISTS` / `try-except` for idempotency —
# MAGIC the notebook is safe to re-run even if some objects were already deleted.
# MAGIC
# MAGIC ### Teardown order
# MAGIC | Step | Objects |
# MAGIC |---|---|
# MAGIC | 1 | BYOL lineage entries |
# MAGIC | 2 | Delta Share and recipient |
# MAGIC | 3 | Foreign catalog |
# MAGIC | 4 | Lakehouse Monitor |
# MAGIC | 5 | Metric View |
# MAGIC | 6 | Gold tables |
# MAGIC | 7 | Row filters and column masks on silver tables |
# MAGIC | 8 | Silver tables |
# MAGIC | 9 | Tags on bronze tables |
# MAGIC | 10 | Bronze tables |
# MAGIC | 11 | Volume |
# MAGIC | 12 | UC functions |
# MAGIC | 13 | MLflow model |
# MAGIC | 14 | Schemas |
# MAGIC | 15 | Group |
# MAGIC | 16 | Catalog (CASCADE) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

CATALOG = "lumina_technologies"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

print(f"Tearing down catalog: {CATALOG}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delete BYOL Lineage Entries

# COMMAND ----------

print("=" * 60)
print("STEP 1: Deleting BYOL lineage entries...")
print("=" * 60)

try:
    from databricks.sdk.service.catalog import ExternalLineageObject, LineageDirection

    # Upstream lineage injected on bronze tables (Snowflake → bronze.customers)
    upstream_tables = [
        f"{CATALOG}.bronze.customers",
        f"{CATALOG}.bronze.transactions",
        f"{CATALOG}.bronze.interactions",
    ]
    for table in upstream_tables:
        try:
            entries = list(w.external_lineage.list_external_lineage_relationships(
                object_info=ExternalLineageObject(table_full_name=table),
                lineage_direction=LineageDirection.UPSTREAM,
            ))
            for entry in entries:
                try:
                    w.external_lineage.delete_external_lineage_relationship(entry.id)
                    print(f"  Deleted upstream BYOL entry for {table}")
                except Exception as e:
                    print(f"  SKIP deleting entry for {table}: {e}")
        except Exception as e:
            print(f"  SKIP listing upstream for {table}: {e}")

    # Downstream lineage injected on gold tables (gold.revenue_summary → Power BI)
    downstream_tables = [
        f"{CATALOG}.gold.revenue_summary",
        f"{CATALOG}.gold.customer_health_scores",
    ]
    for table in downstream_tables:
        try:
            entries = list(w.external_lineage.list_external_lineage_relationships(
                object_info=ExternalLineageObject(table_full_name=table),
                lineage_direction=LineageDirection.DOWNSTREAM,
            ))
            for entry in entries:
                try:
                    w.external_lineage.delete_external_lineage_relationship(entry.id)
                    print(f"  Deleted downstream BYOL entry for {table}")
                except Exception as e:
                    print(f"  SKIP deleting entry for {table}: {e}")
        except Exception as e:
            print(f"  SKIP listing downstream for {table}: {e}")

except Exception as e:
    print(f"  SKIP: external_lineage SDK not available: {e}")

print("Step 1 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop Delta Share and Recipient

# COMMAND ----------

print("=" * 60)
print("STEP 2: Dropping Delta Share and recipient...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SHARE IF EXISTS lumina_gold_share;

# COMMAND ----------

print("  Dropped share: lumina_gold_share")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP RECIPIENT IF EXISTS workshop_partner;

# COMMAND ----------

print("  Dropped recipient: workshop_partner")
print("Step 2 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drop Foreign Catalog

# COMMAND ----------

print("=" * 60)
print("STEP 3: Dropping foreign catalog...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS external_data;

# COMMAND ----------

print("  Dropped catalog: external_data")
print("Step 3 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drop Lakehouse Monitor

# COMMAND ----------

print("=" * 60)
print("STEP 4: Dropping Lakehouse Monitor...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP LAKEHOUSE MONITOR IF EXISTS lumina_technologies.gold.customer_health_scores_monitor;

# COMMAND ----------

print("  Dropped monitor: lumina_technologies.gold.customer_health_scores_monitor")
print("Step 4 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Drop Metric View

# COMMAND ----------

print("=" * 60)
print("STEP 5: Dropping Metric View...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP METRIC VIEW IF EXISTS lumina_technologies.gold.revenue_metrics;

# COMMAND ----------

print("  Dropped metric view: lumina_technologies.gold.revenue_metrics")
print("Step 5 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Drop Gold Tables

# COMMAND ----------

print("=" * 60)
print("STEP 6: Dropping gold tables...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lumina_technologies.gold.customer_health_scores;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lumina_technologies.gold.revenue_summary;

# COMMAND ----------

print("  Dropped: lumina_technologies.gold.customer_health_scores")
print("  Dropped: lumina_technologies.gold.revenue_summary")
print("Step 6 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Remove Row Filters and Column Masks from Silver Tables

# COMMAND ----------

print("=" * 60)
print("STEP 7: Removing row filters and column masks from silver tables...")
print("=" * 60)

security_policy_statements = [
    "ALTER TABLE lumina_technologies.silver.cleaned_customers DROP ROW FILTER",
    "ALTER TABLE lumina_technologies.silver.cleaned_customers ALTER COLUMN email DROP MASK",
    "ALTER TABLE lumina_technologies.silver.cleaned_customers ALTER COLUMN phone DROP MASK",
    "ALTER TABLE lumina_technologies.silver.cleaned_interactions ALTER COLUMN channel DROP MASK",
    "ALTER TABLE lumina_technologies.silver.new_customer_segment DROP ROW FILTER",
    "ALTER TABLE lumina_technologies.silver.new_customer_segment ALTER COLUMN email DROP MASK",
]

for stmt in security_policy_statements:
    try:
        spark.sql(stmt)
        print(f"  OK: {stmt}")
    except Exception as e:
        print(f"  SKIP (may not be set): {stmt[:80]}... -> {e}")

print("Step 7 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Drop Silver Tables

# COMMAND ----------

print("=" * 60)
print("STEP 8: Dropping silver tables...")
print("=" * 60)

silver_tables = [
    "new_customer_segment",
    "transaction_totals",
    "cleaned_interactions",
    "cleaned_transactions",
    "cleaned_customers",
]

for table in silver_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {SILVER}.{table}")
        print(f"  Dropped: {SILVER}.{table}")
    except Exception as e:
        print(f"  SKIP: {SILVER}.{table}: {e}")

print("Step 8 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Remove Tags from Bronze Tables

# COMMAND ----------

print("=" * 60)
print("STEP 9: Removing tags from bronze tables...")
print("=" * 60)

tag_statements = [
    # Column-level PII and sensitivity tags
    f"ALTER TABLE {BRONZE}.customers ALTER COLUMN email UNSET TAGS ('pii', 'sensitivity_level')",
    f"ALTER TABLE {BRONZE}.customers ALTER COLUMN phone UNSET TAGS ('pii', 'sensitivity_level')",
    f"ALTER TABLE {BRONZE}.customers ALTER COLUMN street_address UNSET TAGS ('pii', 'sensitivity_level')",
    # Table-level tags
    f"ALTER TABLE {BRONZE}.customers UNSET TAGS ('team', 'system.certification_status')",
]

for stmt in tag_statements:
    try:
        spark.sql(stmt)
        print(f"  OK: {stmt[:90]}...")
    except Exception as e:
        print(f"  SKIP: {stmt[:90]}... -> {e}")

print("Step 9 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Drop Bronze Tables

# COMMAND ----------

print("=" * 60)
print("STEP 10: Dropping bronze tables...")
print("=" * 60)

bronze_tables = ["customers", "transactions", "interactions"]

for table in bronze_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{table}")
        print(f"  Dropped: {BRONZE}.{table}")
    except Exception as e:
        print(f"  SKIP: {BRONZE}.{table}: {e}")

print("Step 10 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Drop Volume

# COMMAND ----------

print("=" * 60)
print("STEP 11: Dropping volume...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS lumina_technologies.bronze.raw_files;

# COMMAND ----------

print("  Dropped volume: lumina_technologies.bronze.raw_files")
print("Step 11 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Drop UC Functions

# COMMAND ----------

print("=" * 60)
print("STEP 12: Dropping UC functions...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lumina_technologies.silver.mask_pii;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lumina_technologies.silver.filter_by_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lumina_technologies.gold.score_customer_health;

# COMMAND ----------

print("  Dropped function: lumina_technologies.silver.mask_pii")
print("  Dropped function: lumina_technologies.silver.filter_by_region")
print("  Dropped function: lumina_technologies.gold.score_customer_health")
print("Step 12 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Delete MLflow Model

# COMMAND ----------

print("=" * 60)
print("STEP 13: Deleting MLflow model...")
print("=" * 60)

try:
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.tracking.MlflowClient()
    model_name = f"{CATALOG}.gold.customer_churn_model"

    # Delete all versions before deleting the registered model
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        for v in versions:
            client.delete_model_version(name=model_name, version=v.version)
            print(f"  Deleted model version: {model_name} v{v.version}")
    except Exception as e:
        print(f"  SKIP deleting versions: {e}")

    client.delete_registered_model(name=model_name)
    print(f"  Deleted registered model: {model_name}")

except Exception as e:
    print(f"  SKIP: {e}")

print("Step 13 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Drop Schemas

# COMMAND ----------

print("=" * 60)
print("STEP 14: Dropping schemas...")
print("=" * 60)

schemas = ["restricted", "gold", "silver", "bronze"]

for schema in schemas:
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
        print(f"  Dropped schema: {CATALOG}.{schema}")
    except Exception as e:
        print(f"  SKIP: {CATALOG}.{schema}: {e}")

print("Step 14 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Delete Group

# COMMAND ----------

print("=" * 60)
print("STEP 15: Deleting group 'data_platform_admins'...")
print("=" * 60)

try:
    groups = list(w.groups.list(filter='displayName eq "data_platform_admins"'))
    if groups:
        for group in groups:
            w.groups.delete(id=group.id)
            print(f"  Deleted group: data_platform_admins (id={group.id})")
    else:
        print("  Group 'data_platform_admins' not found (already deleted?)")
except Exception as e:
    print(f"  SKIP: {e}")

print("Step 15 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Drop Catalog (CASCADE)
# MAGIC
# MAGIC `CASCADE` handles any remaining objects not explicitly dropped above.
# MAGIC Individual drops earlier in this notebook ensure clean logging; this
# MAGIC step is the final safety net.

# COMMAND ----------

print("=" * 60)
print("STEP 16: Dropping catalog lumina_technologies...")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS lumina_technologies CASCADE;

# COMMAND ----------

print("  Dropped catalog: lumina_technologies")
print("Step 16 complete.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown Complete

# COMMAND ----------

print("=" * 60)
print("TEARDOWN COMPLETE")
print("=" * 60)
print()
print("All workshop resources have been removed:")
print("  - BYOL lineage entries")
print("  - Delta Share (lumina_gold_share) and recipient (workshop_partner)")
print("  - Foreign catalog (external_data)")
print("  - Lakehouse Monitor")
print("  - Metric View (revenue_metrics)")
print("  - Gold tables (customer_health_scores, revenue_summary)")
print("  - Silver row filters and column masks")
print("  - Silver tables (5 tables)")
print("  - Bronze column and table tags")
print("  - Bronze tables (customers, transactions, interactions)")
print("  - Volume (bronze.raw_files)")
print("  - UC functions (mask_pii, filter_by_region, score_customer_health)")
print("  - MLflow model (gold.customer_churn_model)")
print("  - Schemas (restricted, gold, silver, bronze)")
print("  - Group (data_platform_admins)")
print("  - Catalog (lumina_technologies)")
print()
print("The workspace is ready for a fresh workshop run.")
print("Re-run 00_setup_workspace.py to provision a new environment.")
