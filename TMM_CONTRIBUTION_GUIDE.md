# Adding UC-Governance-Workshop to databricks/tmm

## Step 1: Fork the tmm repo

Go to https://github.com/databricks/tmm and click **Fork** (top right). This creates a copy at `https://github.com/treatsSean/tmm`.

## Step 2: Clone your fork locally

```bash
cd ~/Documents/Workspace
git clone https://github.com/treatsSean/tmm.git
cd tmm
```

## Step 3: Create a branch

```bash
git checkout -b add-uc-governance-workshop
```

## Step 4: Copy the workshop into the tmm repo

```bash
# Copy the workshop contents (excluding .git)
rsync -av --exclude='.git' --exclude='TMM_CONTRIBUTION_GUIDE.md' \
  ~/Documents/Workspace/L200_UC_Workshop/ \
  ~/Documents/Workspace/tmm/UC-Governance-Workshop/
```

## Step 5: Update the tmm README

Open `~/Documents/Workspace/tmm/README.md` and add the following line under the "Tech covered" section (alphabetical placement near "Unity Catalog & Governance"):

```
Unity Catalog & Governance — Free edition governance workshop: access control, AI classification, tagging, domains, ACID transactions, row/column security, metric views, monitoring, lineage
```

If there's already a "Unity Catalog & Governance" line, replace it or append this as a sub-bullet.

## Step 6: Commit and push

```bash
cd ~/Documents/Workspace/tmm
git add UC-Governance-Workshop/ README.md
git commit -m "Add UC Governance Workshop (Free edition hands-on lab)"
git push -u origin add-uc-governance-workshop
```

## Step 7: Open a Pull Request

Go to https://github.com/treatsSean/tmm and click the green **"Compare & pull request"** button that appears after pushing.

**Title:**
```
Add UC Governance Workshop
```

**Body:**
```markdown
## Summary
- Adds a hands-on Unity Catalog governance workshop (85 min, 7 notebook sections)
- Covers Free edition features: access control, AI functions, tagging, discovery & domains, data integrity, row/column security, metric views, monitoring, and lineage
- Includes setup/teardown automation, instructor guide, and DAB config for serverless deployment

## Contents
- `UC-Governance-Workshop/` — self-contained workshop directory
  - 7 lab notebooks using medallion architecture (bronze/silver/gold)
  - Idempotent setup and teardown scripts
  - Synthetic data generator (Lumina Technologies scenario)
  - Instructor guide with timing and talking points
  - `databricks.yml` for deployment via Databricks Asset Bundles

## Test plan
- [ ] Run `00_setup_workspace.py` on a Free edition workspace
- [ ] Walk through all 7 notebooks end-to-end
- [ ] Run `99_teardown_workspace.py` to confirm clean removal
```

Set the **base repository** to `databricks/tmm` and **base branch** to `main`.

## Done

After the PR is merged, the workshop will be available at:
`https://github.com/databricks/tmm/tree/main/UC-Governance-Workshop`
