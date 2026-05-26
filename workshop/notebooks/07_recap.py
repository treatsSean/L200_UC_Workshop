# Databricks notebook source

# MAGIC %md
# MAGIC # Workshop Recap — Unity Catalog Governance
# MAGIC
# MAGIC Congratulations on completing the Lumina Technologies Unity Catalog Governance Workshop! This notebook summarizes what you accomplished across all five sections and provides next steps for deepening your Unity Catalog expertise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Accomplished
# MAGIC
# MAGIC Over the course of this workshop, you built and governed a complete data platform from raw ingestion through gold-layer analytics — using Unity Catalog as the single control plane for access, quality, lineage, and discovery.
# MAGIC
# MAGIC ### Section 1: Access Control & AI Asset Governance
# MAGIC - Inspected table privileges via `information_schema.table_privileges`
# MAGIC - Granted `EXECUTE` on a UC function to demonstrate AI asset governance
# MAGIC - Transferred schema ownership to the `data_platform_admins` group
# MAGIC - Observed access-denied behavior on the restricted schema
# MAGIC
# MAGIC ### Section 2a: Loading Data & AI Classification
# MAGIC - Loaded CSV data into bronze tables from a managed Volume
# MAGIC - Used `ai_gen` to generate column descriptions programmatically
# MAGIC - Used `ai_classify` to detect PII columns
# MAGIC
# MAGIC ### Section 2b: Tagging & Governance Metadata
# MAGIC - Applied PII tags (`pii`, `sensitivity_level`) across bronze, silver, and gold layers
# MAGIC - Understood the difference between ad-hoc and governed tags
# MAGIC - Set certification status on a table
# MAGIC - Queried `information_schema.column_tags` to discover all PII-tagged columns
# MAGIC
# MAGIC ### Section 3: Discovery & Domains
# MAGIC - Created a "Customer Analytics" domain
# MAGIC - Browsed the Discovery page by domain, tag, and asset type
# MAGIC - Explored the "Request access" flow for restricted tables
# MAGIC - Queried `information_schema.column_tags` for programmatic discovery
# MAGIC
# MAGIC ### Section 4a: Data Integrity — ACID Transactions & Constraints
# MAGIC - Built four silver layer tables with cleaning and type-casting transformations
# MAGIC - Used Time Travel to compare pre- and post-update table states
# MAGIC - Used `RESTORE` to roll back a table to a previous version
# MAGIC - Added primary key and foreign key constraints between silver tables
# MAGIC - Verified constraints appear in catalog metadata
# MAGIC
# MAGIC ### Section 4b: Row/Column Security & ABAC at Scale
# MAGIC - Created a reusable `mask_pii` column mask function
# MAGIC - Created a reusable `filter_by_region` row filter function
# MAGIC - Applied masks and filters to multiple tables using the same function definitions
# MAGIC - Created a new table and immediately attached existing policies
# MAGIC
# MAGIC ### Section 5: Metric Views & Lineage
# MAGIC - Created gold aggregation tables with UC function lineage
# MAGIC - Applied liquid clustering and ran `OPTIMIZE`
# MAGIC - Defined a Metric View in YAML with dimensions and measures
# MAGIC - Queried the same metric view across different dimension groupings
# MAGIC - Enabled a Lakehouse Monitor for data quality profiling
# MAGIC - Explored automated lineage in Catalog Explorer (table and column level)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Unity Catalog Governance Model — In One Picture
# MAGIC
# MAGIC Everything you did in this workshop operates through the same catalog, the same privilege model, and the same lineage infrastructure:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                        Unity Catalog                               │
# MAGIC │                                                                     │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
# MAGIC │  │ Access Control│  │  Tags &      │  │  Lineage &               │  │
# MAGIC │  │ GRANT/REVOKE │  │  Classification│  │  Automated Tracking     │  │
# MAGIC │  │ Row Filters  │  │  Domains     │  │  Lakehouse Monitors      │  │
# MAGIC │  │ Column Masks │  │  Discovery   │  │  Metric Views            │  │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
# MAGIC │                                                                     │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
# MAGIC │  │ Delta Tables │  │ UC Functions │  │  ACID Transactions       │  │
# MAGIC │  │ Volumes      │  │ ML Models    │  │  Time Travel & RESTORE   │  │
# MAGIC │  │ Views        │  │ AI Functions │  │  PK/FK Constraints       │  │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps — Continue Your Learning Journey
# MAGIC
# MAGIC ### Deepen Your Unity Catalog Knowledge
# MAGIC
# MAGIC | Topic | Resource |
# MAGIC |---|---|
# MAGIC | Unity Catalog documentation | [docs.databricks.com/en/data-governance/unity-catalog](https://docs.databricks.com/en/data-governance/unity-catalog/) |
# MAGIC | Metric Views | [docs.databricks.com/en/metric-views](https://docs.databricks.com/en/metric-views/) |
# MAGIC | Row filters & column masks | [docs.databricks.com/en/tables/row-and-column-filters](https://docs.databricks.com/en/tables/row-and-column-filters/) |
# MAGIC | Lakehouse Monitors | [docs.databricks.com/en/lakehouse-monitoring](https://docs.databricks.com/en/lakehouse-monitoring/) |
# MAGIC
# MAGIC ### Expand Into Related Areas
# MAGIC
# MAGIC | Topic | Why it matters |
# MAGIC |---|---|
# MAGIC | **Lakeflow Declarative Pipelines** | Automate your bronze → silver → gold transformations with built-in data quality expectations |
# MAGIC | **MLflow on Databricks** | Track experiments, register models in UC, and deploy to serving endpoints — all governed |
# MAGIC | **AI/BI Dashboards & Genie** | Build dashboards and natural language analytics over the governed gold tables you created |
# MAGIC | **Databricks Apps** | Build and deploy full-stack applications that query UC-governed data with OAuth |
# MAGIC | **Databricks Asset Bundles** | Define your UC objects, jobs, and pipelines as code for CI/CD and multi-environment deployment |
# MAGIC
# MAGIC ### Hands-On Practice
# MAGIC
# MAGIC - **Try governed tags at scale:** Create a governed tag vocabulary for your organization and apply it across multiple catalogs
# MAGIC - **Build a real ABAC policy:** Extend the `mask_pii` and `filter_by_region` functions from Section 4b to handle multiple sensitivity levels and regions
# MAGIC - **Implement Metric Views:** Define your organization's core KPIs as Metric Views and connect them to Genie spaces for self-serve analytics
# MAGIC - **Explore tag-based policies:** Use tags to drive which row filters and column masks get applied — this is the path to scalable ABAC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Thank You
# MAGIC
# MAGIC Thank you for participating in the Lumina Technologies Unity Catalog Governance Workshop. The skills you practiced today — access control, classification, tagging, integrity constraints, security policies, lineage tracking, and metric views — are the foundation of a modern data governance program on Databricks.
# MAGIC
# MAGIC If you have questions or feedback about this workshop, please reach out to your instructor.
