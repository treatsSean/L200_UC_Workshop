# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3: Discovery & Domains
# MAGIC
# MAGIC **Duration:** 5 minutes
# MAGIC
# MAGIC **Purpose:** The tags you applied in Section 2b are only useful if people can find the assets they describe. This section introduces the two mechanisms Unity Catalog provides for that: **Domains** (logical groupings of related assets) and the **Discovery page** (a search and browse interface over catalog metadata). You will create a "Customer Analytics" domain, then use the Discovery page to find tagged assets by domain, by tag, and by asset type — all without knowing the exact three-part name of a table in advance.
# MAGIC
# MAGIC **What you will do:**
# MAGIC - Create a "Customer Analytics" domain via the Python SDK (with UI fallback instructions if the API is unavailable)
# MAGIC - Browse the Discovery page by domain and by tag
# MAGIC - Locate the registered model and UC function created in earlier sections
# MAGIC - Explore a table's detail page (schema, sample data, lineage preview)
# MAGIC - Run a programmatic discovery query against `system.information_schema.column_tags`
# MAGIC - Walk through the "Request access" flow for a restricted table

# COMMAND ----------

# Configuration — set the catalog name used throughout this notebook
CATALOG = "lumina_technologies"

print(f"Working in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Domain via the Python SDK
# MAGIC
# MAGIC **Domains** are logical containers that let you group related Unity Catalog assets — tables, models, functions, volumes — under a single, human-readable label. They are surfaced on the Discovery page as a browse facet, so a new data engineer who does not know the schema layout can navigate to "Customer Analytics" and immediately see every asset that belongs to that domain, regardless of which schema or layer it lives in.
# MAGIC
# MAGIC Domains are distinct from schemas. A schema enforces a namespace boundary; a domain is a metadata-level grouping that can span multiple schemas (and even multiple catalogs). The same table can belong to only one domain at a time.
# MAGIC
# MAGIC The cell below uses the Databricks Python SDK to create the "Customer Analytics" domain and assign the three customer-facing tables to it. If your workspace version does not yet expose the domains API through the SDK, the cell will catch the error and print UI-based fallback instructions.
# MAGIC
# MAGIC **If the SDK call fails, follow these steps in the Catalog Explorer UI:**
# MAGIC 1. Open **Catalog** in the left navigation bar.
# MAGIC 2. Click **Domains** in the left panel (below the catalog tree).
# MAGIC 3. Click **Create domain**.
# MAGIC 4. Enter `Customer Analytics` as the name and an optional description.
# MAGIC 5. Click **Create**.
# MAGIC 6. Inside the new domain, click **Add assets**.
# MAGIC 7. Search for and add: `lumina_technologies.bronze.customers`, `lumina_technologies.silver.cleaned_customers`, and `lumina_technologies.gold.customer_health_scores`.
# MAGIC 8. Click **Save**.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

DOMAIN_NAME = "Customer Analytics"
DOMAIN_DESCRIPTION = (
    "All customer-related assets across bronze, silver, and gold layers — "
    "raw ingestion, cleaned records, and derived health scores."
)

DOMAIN_ASSETS = [
    f"{CATALOG}.bronze.customers",
    f"{CATALOG}.silver.cleaned_customers",
    f"{CATALOG}.gold.customer_health_scores",
]

try:
    # The catalog_domain API is available in SDK >= 0.22 and DBR >= 14.3.
    # If it is not present in your workspace, the except block will fire.
    domain = w.catalog_domains.create(
        name=DOMAIN_NAME,
        comment=DOMAIN_DESCRIPTION,
    )
    print(f"Domain created: {domain.name} (id={domain.id})")

    for asset_name in DOMAIN_ASSETS:
        w.catalog_domains.add_assets(
            domain_id=domain.id,
            assets=[{"full_name": asset_name}],
        )
        print(f"  Added asset: {asset_name}")

    print("\nDomain setup complete. Browse to the Discovery page to see it.")

except AttributeError:
    print(
        "catalog_domains API is not available in this SDK version.\n"
        "Follow the UI fallback instructions in the %md cell above to create\n"
        "the 'Customer Analytics' domain manually in Catalog Explorer."
    )
except Exception as e:
    print(
        f"Domain creation encountered an error: {e}\n\n"
        "Follow the UI fallback instructions in the %md cell above to create\n"
        "the 'Customer Analytics' domain manually in Catalog Explorer."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Browse the Discovery Page
# MAGIC
# MAGIC The Discovery page is Unity Catalog's catalog-wide search and browse surface. It indexes table names, column names, tags, comments, owner information, and asset types — making it possible to find assets based on what they contain rather than where they are stored.
# MAGIC
# MAGIC Follow these steps:
# MAGIC
# MAGIC **Open the Discovery page:**
# MAGIC 1. In the left navigation bar, click **Catalog**.
# MAGIC 2. At the top of the Catalog panel, click the **Discover** tab (or look for a search/magnifying glass icon depending on your workspace version).
# MAGIC 3. You should see a browsable index of assets with filter facets on the left: Asset type, Domain, Tag, Owner.
# MAGIC
# MAGIC **Browse by domain:**
# MAGIC 1. In the **Domain** facet on the left, click **Customer Analytics**.
# MAGIC 2. Confirm that `bronze.customers`, `silver.cleaned_customers`, and `gold.customer_health_scores` all appear in the results.
# MAGIC 3. Notice that assets from different schemas appear together because the domain is a cross-schema grouping.
# MAGIC
# MAGIC **Search by tag:**
# MAGIC 1. Clear the domain filter, then type `pii` into the search bar.
# MAGIC 2. Select the **Tag** filter to scope results to assets tagged with `pii`.
# MAGIC 3. Confirm that the tagged columns from Section 2b appear. You can also filter by `tag_value = 'true'` to narrow the results.
# MAGIC
# MAGIC **Find the registered model and UC function:**
# MAGIC 1. In the **Asset type** facet, select **Models**. Locate the `customer_churn_model` registered in the `lumina_technologies.gold` schema.
# MAGIC 2. Switch **Asset type** to **Functions**. Locate the `score_customer_health` SQL function.
# MAGIC 3. Click through to each asset's detail page and note the tags applied in Section 2b — they are displayed prominently in the metadata panel.
# MAGIC
# MAGIC **Explore a table's detail page:**
# MAGIC 1. Navigate to `lumina_technologies.gold.customer_health_scores` (either through search or the catalog tree).
# MAGIC 2. On the **Overview** tab, review the schema. Confirm `customer_id` is tagged with `pii = 'true'`.
# MAGIC 3. Click the **Sample data** tab to preview rows (this executes a `SELECT * LIMIT 100` using your credentials).
# MAGIC 4. Click the **Lineage** tab. You will see a preview of upstream sources — the full lineage graph is covered in Section 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Programmatic Discovery via `information_schema`
# MAGIC
# MAGIC Everything visible in the Discovery UI is also queryable through SQL. Unity Catalog exposes tag metadata in `system.information_schema.column_tags` and `system.information_schema.table_tags`. Querying these views directly is useful for audits, automated governance checks, and building dashboards that track tagging coverage across the catalog.
# MAGIC
# MAGIC The query below retrieves every column in `lumina_technologies` tagged with `pii`, along with its schema, table, and tag value. Run it to verify that the tags applied in Section 2b are present and complete across all three layers (bronze, silver, gold).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE catalog_name = 'lumina_technologies'
# MAGIC   AND tag_name = 'pii'
# MAGIC ORDER BY schema_name, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: UI Walkthrough — Request Access Flow
# MAGIC
# MAGIC Unity Catalog integrates with access request workflows so that data consumers can signal intent to access assets they currently cannot see or query, without needing to know who to email or which Slack channel to post in.
# MAGIC
# MAGIC Follow these steps to walk through the request flow:
# MAGIC
# MAGIC 1. In the catalog tree or Discovery search, navigate to `lumina_technologies.restricted.sensitive_data`.
# MAGIC    - If the table is not visible in your catalog tree, search for it by name in the Discovery search bar. Tables that you do not have `SELECT` on are still discoverable (visibility is controlled separately from access).
# MAGIC 2. On the table's detail page, click the **Request access** button. It appears near the top of the page next to the table name, or in the **Permissions** tab.
# MAGIC 3. A dialog will appear asking you to describe your use case and select the type of access you need (`SELECT`, `MODIFY`, etc.).
# MAGIC 4. Fill in a brief justification (e.g., "Need read access for churn analysis project") and click **Submit request**.
# MAGIC 5. The table owner will receive a notification (in-product or by email, depending on workspace configuration) and can approve or deny the request from the Catalog UI.
# MAGIC
# MAGIC **Key point:** The ability to *discover* an asset is separate from the ability to *access* it. A user with no privileges on `restricted.sensitive_data` can still find it in Discovery and read its schema and metadata — only the sample data and actual query access are gated. This is the intended behavior: discoverability reduces the need for informal data requests and empowers consumers to self-serve the access process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided Checkpoint
# MAGIC
# MAGIC Take 60 seconds to discuss with a neighbor or reflect on the following:
# MAGIC
# MAGIC **Key takeaway:** Discovery and Domains close the loop on the work you did in Sections 2a and 2b. Tags are not just access control hooks — they are discovery signals. The `pii = 'true'` tag you applied to `bronze.customers.email` now surfaces that column in a tag-based search on the Discovery page. The "Customer Analytics" domain you created means a new team member can browse by domain and immediately understand which tables belong to the customer data product, without needing an onboarding document or a schema diagram.
# MAGIC
# MAGIC **Questions to consider:**
# MAGIC - What is the right granularity for domains in your organization — should a domain map to a team, a subject area, a data product, or something else?
# MAGIC - If discoverability is intentionally separate from access, how do you prevent sensitive metadata (column names, comments) from leaking information you did not intend to share?
# MAGIC - Who should have the ability to create domains and assign assets to them — central governance, domain owners, or anyone with `USE CATALOG`?
# MAGIC
# MAGIC **Up next — Section 4a:** Data Quality & Expectations. You will add Delta Live Tables expectations to the bronze-to-silver pipeline to enforce row-level quality constraints and quarantine bad records before they propagate downstream.
