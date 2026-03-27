# L200 Unity Catalog Workshop — Instructor Guide

This guide is for the instructor delivering the workshop. It covers timing, pacing, what to say, what to watch for, and how to recover from common problems. It is not a repeat of the notebook content.

---

## Workshop Overview

| | |
|---|---|
| **Total time** | 95 min |
| **Format** | 15 min intro → 75 min hands-on lab → 5 min Q&A |
| **Audience** | Data engineers, data platform admins |
| **Lab format** | Pre-built notebooks; attendees execute cells and observe output |
| **Compute** | Serverless notebooks + serverless SQL warehouse |
| **Scenario** | Lumina Technologies — fictional company, generic vertical |

The Lumina Technologies scenario is intentionally generic. Do not tailor the company description to a specific industry when delivering to a mixed audience.

---

## Pre-Workshop Checklist

Complete these steps before attendees arrive. Allow 20–30 min buffer before the scheduled start.

- [ ] Run `workshop/setup/00_setup_workspace.py` end-to-end and confirm all health checks pass
- [ ] Confirm the serverless SQL warehouse is in a **Running** state
- [ ] Manually run 2–3 SQL queries against the Lumina catalog to confirm access
- [ ] Confirm the foreign connection is configured (if the Section 6 federation demo is in scope)
- [ ] Verify CSVs were uploaded to the volume — the setup script should handle this, but confirm:
  ```sql
  LIST '/Volumes/lumina_technologies/raw/landing/';
  ```
- [ ] Confirm system tables are accessible:
  ```sql
  SELECT * FROM system.billing.usage LIMIT 5;
  SELECT * FROM system.access.table_lineage LIMIT 5;
  ```
- [ ] Open Catalog Explorer in the browser and verify the `lumina_technologies` catalog is visible
- [ ] If demoing lineage in Section 5: confirm the lineage graph is populated in Catalog Explorer for `customer_gold.customer_360`

> **If setup fails:** The most common cause is a missing privilege on `system.billing.usage`. Verify the workspace has system table access enabled in Account Console → Unity Catalog settings.

---

## Timing Reference

| Section | Notebook | Time |
|---------|----------|------|
| Intro / scene-setting | — | 15 min |
| 1: Access Control & AI Asset Governance | `01_access_control.py` | 10 min |
| 2a: Loading Data & AI Classification | `02a_data_classification.py` | 8 min |
| 2b: Tagging & Governance Metadata | `02b_tagging.py` | 7 min |
| 3: Discovery & Domains | `03_discovery_domains.py` | 5 min |
| 4a: Data Integrity | `04a_data_integrity.py` | 9 min |
| 4b: Row/Column Security & ABAC | `04b_abac_security.py` | 11 min |
| 5: Metric Views & Lineage | `05_lineage_metrics.py` | 15 min |
| 6: Sharing & Federation | `06_sharing_federation.py` | 10 min |
| Q&A | — | 5 min |
| **Total** | | **95 min** |

Sections 3 and 4a are the shortest. If you are running behind, those are the best places to tighten. Section 5 is the longest and contains the most UI-heavy demo — protect that time.

---

## Per-Section Guide

### Section 1: Access Control & AI Asset Governance (10 min)

**Notebook:** `01_access_control.py`
**Timing:** 2 min talking points, 8 min hands-on

**Key talking points:**
- Privilege cascade: grants at the catalog level flow down to schemas and tables unless overridden
- Ownership and grants are distinct concepts — owners can always grant, but ownership is not required to receive a grant
- AI assets (registered models, functions, AI/BI dashboards) are first-class UC objects governed by the same GRANT/REVOKE mechanics as tables
- `EXECUTE` is the correct privilege for functions — not `SELECT` — because functions are callable objects, not relations

**UI steps:** None for this section. All interaction is notebook-based.

**Common questions:**

> "Why EXECUTE instead of SELECT on functions?"

Functions are invocable, not queryable. `SELECT` applies to relations (tables, views). `EXECUTE` applies to callable objects (functions, procedures). The analogy is file system execute permission versus read permission.

> "Does granting at the catalog level expose everything in the catalog?"

Yes — `USE CATALOG` and `USE SCHEMA` allow navigation, but `SELECT` must be granted explicitly on the tables. Cascade means the grant can flow down, but you still control what is granted.

**Watch for:** Attendees sometimes confuse the `lumina_technologies` catalog owner with the workshop user. The setup script grants specific privileges rather than making attendees owners — this is intentional to demonstrate the grant model.

**Fallback:** If `system.billing.usage` returns zero rows, note that usage data may take time to populate in new workspaces. The cell will not error — it will just return an empty result. Move on.

---

### Section 2a: Loading Data & AI Classification (8 min)

**Notebook:** `02a_data_classification.py`
**Timing:** 2 min talking points, 6 min hands-on

**Key talking points:**
- `ai_gen()` and `ai_classify()` are SQL functions that call foundation models — no Python, no external API calls, no MLflow required
- AI-generated documentation (table and column descriptions) is stored in the catalog and surfaced in Catalog Explorer — it is not ephemeral
- The Data Classification engine is a separate automated scan — it runs asynchronously on tables and applies sensitivity tags without user-written code

**UI demo (instructor-led, attendees watch):**
1. Navigate to any table in `lumina_technologies` in Catalog Explorer
2. Click the **Generate** button in the Overview tab to generate an AI description
3. Show that the generated text appears in the Description field and can be saved
4. This is a 2-minute demo — do not let it run long. The point is to show the button exists and the output is sensible.

**Common questions:**

> "Is this description stored in the metastore or just displayed?"

It is stored in the Unity Catalog metastore as the table comment. It is queryable via `DESCRIBE TABLE EXTENDED` and visible to all users with `USE SCHEMA` access.

**Fallback:**
- `ai_gen()` and `ai_classify()` require AI functions to be enabled at the workspace level. If they return an error (`AI_FUNCTIONS_DISABLED`), acknowledge the setting and skip to the next cell. The exercise is illustrative — the output is pre-populated for attendees who cannot run it live.
- Data Classification results may be empty on tables created during the workshop. The scan runs on a delay (hours, not minutes). Pre-populate an example if you want a visual during the workshop.

---

### Section 2b: Tagging & Governance Metadata (7 min)

**Notebook:** `02b_tagging.py`
**Timing:** 2 min talking points, 5 min hands-on

**Key talking points:**
- Tags are key-value metadata that can be applied to catalogs, schemas, tables, columns, and AI assets
- Governed tags live in a tag catalog and enforce a controlled vocabulary — only values in the approved list can be used
- Ad-hoc tags are freeform — any value is accepted
- Certification status (`Certified`, `Pending Review`, `Deprecated`) signals data asset quality to consumers without needing a separate data catalog tool
- AI assets such as registered models and dashboards can be tagged the same way as tables

**Common questions:**

> "What's the difference between governed and ad-hoc tags?"

Governed tags are created by a tag admin and define valid values — for example, a `classification` tag may only allow `public`, `internal`, `confidential`, `restricted`. Ad-hoc tags accept any string. Governed tags are better for compliance workflows where you need to ensure consistent vocabulary. Ad-hoc tags are fine for informal annotation.

**Watch for:** Attendees sometimes apply tags incorrectly if they use the wrong object identifier. The notebook uses fully-qualified names (`catalog.schema.table`) consistently — remind attendees not to omit the catalog prefix.

---

### Section 3: Discovery & Domains (5 min)

**Notebook:** `03_discovery_domains.py`
**Timing:** 1 min talking points, 2 min hands-on, 2 min UI walkthrough

**Key talking points:**
- Domains are organizational groupings — they map to teams, business units, or data products, not to technical schemas
- A table can appear in a domain without being moved — domains reference objects, they do not contain them
- The Catalog Explorer Discovery page is a consumer-facing search experience layered on top of the catalog

**UI walkthrough (attendees follow along):**
1. Navigate to **Catalog** → **Discovery** in the sidebar
2. Search for a term (e.g., `customer`) and show that results span catalogs and schemas
3. Filter by tag (e.g., `pii = true`) to show tag-driven discovery
4. Click on a result and show the **Request Access** button — explain that access requests can be routed to the data owner

**Fallback:** The Domain API may not be available in all workspace tiers. If the notebook cell creating a domain via API fails, demonstrate domain creation through the Catalog Explorer UI instead:
- Navigate to **Catalog** → select the catalog → **Domains** tab → **+ Create Domain**

---

### Section 4a: Data Integrity (9 min)

**Notebook:** `04a_data_integrity.py`
**Timing:** 2 min talking points, 7 min hands-on

**Key talking points:**
- Delta Lake provides multi-table ACID transactions on open formats — this is not limited to Databricks-managed storage
- Foreign key constraints are informational in Delta — they are not enforced at write time, but they are respected by the query optimizer and surfaced in Catalog Explorer as schema documentation
- Catalog-managed tables (as opposed to external tables) give Unity Catalog full lifecycle control — including automated cleanup on `DROP TABLE`

**Common questions:**

> "Does this work with Iceberg tables too?"

Yes. UC supports both Delta and Iceberg as managed table formats. The ACID guarantees and FK constraint metadata apply to Iceberg tables under UC management.

> "If FK constraints aren't enforced, what's the point?"

Three things: (1) optimizer hints — the planner can use FK relationships to eliminate joins in certain queries; (2) documentation — Catalog Explorer displays the relationship graph; (3) lineage correlation — UC can use FK metadata to enrich lineage graphs.

**Watch for:** One cell in this notebook deliberately executes a transaction that violates a constraint and throws an error. This is expected behavior — the point is to show that the transaction rolls back cleanly. Alert attendees before they run it so they are not alarmed by a red cell.

Specifically say: *"The next cell will fail — that is the point. Watch how the error message identifies the violation and that the partial write was rolled back."*

---

### Section 4b: Row/Column Security & ABAC (11 min)

**Notebook:** `04b_abac_security.py`
**Timing:** 3 min talking points, 8 min hands-on

**Key talking points:**
- Column masks replace the value of a column at query time based on the calling user's group membership — downstream code does not change, only the returned value changes
- Row filters eliminate rows from results at query time — again, downstream code is unchanged
- `is_account_group_member()` is the standard predicate for both masks and filters — it checks the caller's account-level groups, not workspace-local groups
- ABAC (Attribute-Based Access Control) at scale: instead of granting to individual users, attach a policy to a column and let group membership drive access. Add a user to the group; they get access automatically.

**The "aha" moment:** After applying the mask and filter, walk attendees through the step where a new table inherits both policies because it was created from the original. Ask: "Did you write any new GRANT statements for this table?" The answer is no — the policies followed the data.

**Common questions:**

> "Can I use tags to drive masks automatically?"

This is exactly the ABAC concept. Today you reference columns explicitly when applying a mask. The forward-looking pattern is to define a policy per tag value and have the engine apply the mask to any column tagged with that value — that is on the Unity Catalog roadmap. For now, the group-membership predicate inside the mask function is the standard pattern.

> "What happens if I UNION two masked tables?"

The mask applies to each table independently. The result set will have masked values from both tables. There is no cross-table mask inheritance.

**Watch for:** `is_account_group_member()` checks account-level groups. If attendees are not in the expected group in the demo catalog, the mask will behave as if they are the "restricted" user. This is fine for the demo — it demonstrates the mask is working.

---

### Section 5: Metric Views & Lineage (15 min)

**Notebook:** `05_lineage_metrics.py`
**Timing:** 3 min talking points, 7 min hands-on, 5 min UI demo

This is the longest and highest-value section. Protect the time.

**Key talking points:**
- Metric views define reusable, governed business metrics as YAML — the definition lives in the catalog, not in a BI tool
- The same metric can be queried at any grain by choosing different dimension groupings at query time — no pre-aggregated cubes required
- Lineage is captured automatically from notebooks, pipelines, SQL queries, and dashboards — no annotation required
- Column-level lineage lets you trace a single sensitive field (e.g., `email`) from its origin system through every transformation to every consumer
- PII tracing via lineage is a compliance use case, not just an academic one — you can answer "where does this data go?" without reading every pipeline

**UI demo (instructor-led, attendees watch):**
1. Navigate to **Catalog Explorer** → `lumina_technologies` → `customer_gold` → `customer_360`
2. Click the **Lineage** tab
3. Expand the full graph upstream and downstream
4. Walk the path: bronze source tables → silver cleaned tables → gold `customer_360` → downstream dashboards
5. Switch to **Column lineage** and click on the `email` column
6. Trace upstream to `raw_contacts.email_address` — note the rename in the silver layer was tracked automatically
7. Trace downstream to show the column lands in the AI/BI dashboard

*Talking point for step 6:* "The column was renamed from `email_address` in bronze to `email` in silver. UC tracked that rename automatically — no annotation, no lineage SDK call."

*Talking point for step 7:* "We can see exactly which downstream consumers receive this PII field. For a GDPR right-to-erasure request, this is the map you need."

**Fallback:**
- Lineage system tables (`system.access.table_lineage`, `system.access.column_lineage`) may lag 15–30 minutes after the pipeline runs. If queries return zero rows, use the Catalog Explorer UI graph instead — it updates faster.
- Metric view syntax requires DBR 17.2 or later. If the cell errors with `UNSUPPORTED_FEATURE`, confirm the cluster runtime version. The error message will include the minimum required version.

---

### Section 6: Sharing & Federation (10 min)

**Notebook:** `06_sharing_federation.py`
**Timing:** 2 min talking points, 8 min hands-on

**Key talking points:**
- Delta Sharing is an open protocol — recipients do not need a Databricks account
- Federation (Lakehouse Federation) lets you query external databases (Snowflake, PostgreSQL, MySQL, etc.) from Databricks without moving the data
- BYOL (Bring Your Own Lineage) lets you register lineage edges for systems that don't generate lineage natively — a Snowflake → Databricks → Power BI flow appears in the Catalog Explorer lineage graph
- Managed Iceberg: UC can serve Delta tables as Iceberg to external consumers — consumers see a native Iceberg table, the underlying format is Delta

**The federation demo requires a pre-configured foreign connection.** Confirm during the pre-workshop checklist that the connection exists. If it does not:
- Skip the `CREATE FOREIGN CATALOG` and federation query cells
- Acknowledge verbally: "In a production workspace, this is where you would configure the connection to Snowflake/PostgreSQL. We'll skip execution and walk through the syntax."
- The BYOL lineage cells do not require the connection — those can still be demonstrated

**Common questions:**

> "Does the recipient need to be a Databricks customer?"

No. Delta Sharing is an open REST protocol. Recipients can consume shares using the open-source Delta Sharing Python client, Spark, or any BI tool with a Delta Sharing connector.

> "Does federation move the data into Databricks?"

No. Queries are pushed down to the foreign system where possible. Results are returned to Databricks but not persisted. If you want persistence, use `CREATE TABLE AS SELECT` from the foreign table.

**Watch for:** The BYOL lineage SDK import path (`from databricks.sdk.service.catalog import ...`) may differ between SDK versions. The notebook includes a REST API fallback — if the SDK import fails, run the REST cell instead. Both produce the same result.

---

## Troubleshooting

| Issue | Likely Cause | Resolution |
|-------|-------------|------------|
| Serverless cluster takes 30–60s on first cell | Cold start | Normal — tell attendees this is expected and to wait |
| `system.billing.usage` returns zero rows | Workspace is new or access not granted | Acknowledge and move on; tell attendees data populates over time |
| `system.access.table_lineage` returns zero rows | Lineage propagation lag (15–30 min) | Use Catalog Explorer UI lineage graph instead |
| `ai_gen()` / `ai_classify()` error: `AI_FUNCTIONS_DISABLED` | Workspace setting not enabled | Skip the cell; explain the function exists and the output is illustrative |
| Data Classification results empty | Scan runs asynchronously (hours) | Pre-populate an example result before the workshop if a visual is needed |
| Multi-table transaction cell fails with `TRANSACTION_CONFLICT` | DBR version < 16 | Confirm cluster runtime is DBR 16+; update cluster config if needed |
| Metric view `CREATE` fails with `UNSUPPORTED_FEATURE` | DBR version < 17.2 | Confirm cluster runtime is DBR 17.2+; update cluster config if needed |
| BYOL lineage SDK import fails | SDK version mismatch | Run the REST API fallback cell in the notebook |
| Federation `CREATE FOREIGN CATALOG` fails | Connection not configured | Skip execution; walk through syntax verbally |
| Tags not visible in Catalog Explorer | Propagation delay | Refresh the browser; tags should appear within 60 seconds of the SQL command |
| Attendee cannot see `lumina_technologies` catalog | Missing `USE CATALOG` privilege | Have attendee run: `GRANT USE CATALOG ON CATALOG lumina_technologies TO <user>` — or re-run the setup script |

---

## Q&A Guide

These questions come up frequently. Having prepared answers helps keep Q&A tight.

**"How does Unity Catalog compare to Apache Atlas / Collibra / Alation?"**
UC is the governance layer inside Databricks — access control, lineage, tagging, and discovery are all native. External data catalogs can integrate with UC via APIs (e.g., Collibra has a UC connector). UC is not positioned to replace a standalone enterprise catalog for organizations with multi-cloud, multi-engine estates, but for Databricks-first teams it removes the need for a separate tool.

**"Can I use Unity Catalog with my existing Hive metastore tables?"**
Yes, via upgrade in place or by registering external tables in UC. Tables remain in their existing storage location; UC adds the governance layer on top.

**"Does lineage work for streaming pipelines?"**
Yes. Structured Streaming jobs captured in Delta Live Tables generate lineage. Standalone Structured Streaming jobs outside of DLT also generate lineage as long as they write to UC-managed tables.

**"What's the latency on lineage system tables?"**
Typically 15–30 minutes. For real-time lineage visualization, use Catalog Explorer — it updates faster than the system tables.

---

## Post-Workshop

1. Run `workshop/setup/00_setup_workspace.py` teardown section (or a separate `99_teardown_workspace.py` if available) to remove workshop artifacts from the catalog
2. Confirm the serverless SQL warehouse is stopped to avoid idle charges
3. Collect feedback — a 3-question form works well: (1) what was most useful, (2) what was confusing, (3) what would you add
4. Share the L100 video walkthrough with attendees as a follow-up resource — it covers the lineage demo from a storytelling perspective rather than a hands-on one
5. File any issues found during delivery (broken cells, outdated syntax, timing that ran long) in the workshop repo before the next delivery
