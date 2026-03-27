# Unity Catalog Governance Workshop

A hands-on workshop for learning Unity Catalog governance on Databricks. Uses a fictional company (Lumina Technologies) with synthetic data to walk through access control, data classification, tagging, ACID transactions, row/column security, lineage, metric views, monitoring, Delta Sharing, and Lakehouse Federation.

## Two-Tier Approach

| Tier | Format | Duration | Description |
|------|--------|----------|-------------|
| **L100** | On-demand video | ~18 min | Foundational concepts — three-level namespace, access control model, object types, Catalog Explorer walkthrough |
| **L200** | Live hands-on lab | 95 min | 8 click-through notebook sections covering governance, protection, and sharing |

Attendees watch the L100 video before the live session. The L200 workshop is 100% hands-on.

## Workshop Sections (L200)

| # | Topic | Time |
|---|-------|------|
| 1 | Access control & AI asset governance | 10 min |
| 2a | Data classification with AI functions | 8 min |
| 2b | Tagging & certification | 7 min |
| 3 | Discovery & domains | 5 min |
| 4a | ACID transactions & FK constraints | 9 min |
| 4b | Row filters & column masks (ABAC) | 11 min |
| 5 | Lineage, metric views & monitoring | 15 min |
| 6 | Delta Sharing & federation | 10 min |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Workspace admin permissions (for setup)
- A compute resource (cluster or serverless)

## Quick Start

### 1. Clone the repo into your workspace

Import this repository into your Databricks workspace using Repos.

### 2. Generate synthetic data (if not already present)

```bash
cd workshop/data
python generate_csv_data.py
```

This creates three CSV files in `workshop/data/output/`: `customers.csv` (5,000 rows), `transactions.csv` (25,000 rows), `interactions.csv` (15,000 rows).

### 3. Run the setup notebook

Open `workshop/setup/00_setup_workspace.py` in your workspace:

1. Set the `repo_data_path` widget to match your workspace path (e.g., `/Workspace/Users/<you>/claude_test/workshop/data/output`)
2. Click **Run All**
3. Wait for the verification summary — all checks should say `[OK]`

This creates the `lumina_technologies` catalog with bronze, silver, and gold layers, a UC function, an MLflow model, and access-control fixtures.

### 4. Open the first lab notebook

Navigate to `workshop/notebooks/01_access_control.py` and begin.

## Deploying with Databricks Asset Bundles

This repo includes a `databricks.yml` for deploying via [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html).

```bash
# Validate the bundle
databricks bundle validate

# Deploy to your dev workspace
databricks bundle deploy -t dev

# Run the setup job
databricks bundle run setup_workshop -t dev
```

See `databricks.yml` for available targets and variables.

## Teardown

Run `workshop/setup/99_teardown_workspace.py` to cleanly remove all workshop resources.

## Repo Structure

```
├── databricks.yml              # Asset Bundle definition
├── workshop/
│   ├── data/
│   │   ├── generate_csv_data.py    # Synthetic data generator
│   │   └── output/                 # Generated CSV files
│   ├── docs/
│   │   ├── instructor_guide.md     # Per-section talking points
│   │   ├── intro_deck_content.md   # L200 intro slide content
│   │   ├── l100_deck_content.md    # L100 video slide content
│   │   ├── l100_recording_script.md # Teleprompter script
│   │   └── proposal_tiered_uc_workshop.md
│   ├── notebooks/
│   │   ├── 01_access_control.py
│   │   ├── 02a_data_classification.py
│   │   ├── 02b_tagging.py
│   │   ├── 03_discovery_domains.py
│   │   ├── 04a_data_integrity.py
│   │   ├── 04b_abac_security.py
│   │   ├── 05_lineage_metrics.py
│   │   └── 06_sharing_federation.py
│   └── setup/
│       ├── 00_setup_workspace.py   # Provisions everything
│       └── 99_teardown_workspace.py # Clean removal
├── notebooks/                  # Lineage demo notebooks (separate)
├── LICENSE.md
├── NOTICE.md
└── SECURITY.md
```

## Contributing

This workshop is maintained by the Databricks field team. For issues or suggestions, open an issue in this repository.

## License

See [LICENSE.md](LICENSE.md).
