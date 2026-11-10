# Step by step: run the project in Databricks

About 45 minutes for Databricks, then 1.5–2 hours for Power BI.

---

## Step 1: Get a Databricks workspace

**Option A: Databricks Free Edition (recommended for practice, no cost).**
Sign up at databricks.com (Free Edition), using your Gmail. It includes Unity Catalog,
serverless notebooks and jobs, and a serverless SQL warehouse.

**Option B: your Azure Databricks workspace** (Premium tier with Unity Catalog). Serverless
jobs must be enabled, or add a job cluster to `resources/utility_job.yml`.

## Step 2: Put the project in the workspace

**Option A: Git folder (easiest).**
1. Unzip the project and push it to a new GitHub repo, e.g. `shakeelMemon/utility-metrics-lakehouse`.
2. In Databricks: **Workspace → Create → Git folder**, paste the repo URL, and click **Create**.

**Option B: Asset Bundle (shows CI/CD skills).**
```powershell
cd C:\Users\shake\Projects\utility-metrics-lakehouse
databricks auth login --host https://<your-workspace-url>
databricks bundle validate
databricks bundle deploy -t dev
```
This uploads the files to `/Workspace/Users/<you>/.bundle/utility-metrics-lakehouse/dev/files`
and creates the job **[dev] utility-metrics-pipeline**.

## Step 3: Run setup (`notebooks/00_setup`)

1. Open `notebooks/00_setup` and attach **Serverless** compute.
2. Leave `catalog = utility_lab` and click **Run all**.
3. If you see *"Could not create catalog"* (some workspaces don't allow it), set the widget to a
   catalog you already have, such as `workspace`, and run again. Use the **same catalog name**
   everywhere after that.

✅ Check: **Catalog** explorer → `utility_lab` has schemas `landing`, `bronze`, `silver`,
`gold`, `ops`, and volumes `landing.raw` and `landing.checkpoints`.

## Step 4: Generate the source files (`notebooks/01_generate_source_files`)

Run all with `mode = initial`. It writes six months of data (May–Oct 2026) to
`/Volumes/utility_lab/landing/raw`:

| Folder | Files | Rows |
|---|---|---|
| `sap_bp` | 1 full + 2 change files | ~1,300 |
| `sap_premise`, `sap_contract_account`, `sap_meter` | 1 each | 1,200 each |
| `ami_meter_reads` | 27 weekly JSON files | ~216,000 |
| `_sap_system` | `billing_doc.csv`, `payment.csv`: stand-ins for the SAP tables, with SAP's changed-on date `AEDAT` | ~5,900 / ~5,200 |
| `sap_billing_doc`, `sap_payment` | filled by `01b_sap_extract` (only changed rows) | first run = full load |
| `call_center_calls`, `oms_outages` | monthly CSV | ~4,000 / ~75 |

✅ Check: open the volume in Catalog explorer and preview `_sap_system/billing_doc.csv`.
Notice the SAP formats: `BUDAT = 20260603`, `BETRW` amounts, and `R...` reversal documents with `125.40-`.

## Step 5: Run the pipeline job

- **Git folder:** **Workflows → Create job**, add the eight notebook tasks in order
  (`00_setup` → `01b_sap_extract` → `02_bronze_autoloader` → `03_silver` → `04_gold` → `05_metrics` →
  `06_data_quality` → `07_maintenance`), add a job parameter `catalog = utility_lab`, and **Run now**.
  You can also run the notebooks one by one.
- **Bundle:** `databricks bundle run utility_metrics_pipeline -t dev`

Takes about 5–10 minutes on serverless.

✅ Check each step:

| Step | What to look for |
|---|---|
| bronze | "new rows" printed per source; `bronze.ami_meter_reads` has `voltage_v`, `_ingest_ts`, `_source_file` |
| silver | ~855 meter reads and 1 outage **quarantined**, with reasons (`kwh_not_negative`, `meter_not_null`…) |
| gold | 12 tables (5 dimensions, 7 facts) with row counts |
| metrics | 52 metrics in the catalogue; a table of the latest month's values |
| data_quality | "Data-quality gate passed" (a freshness WARN is fine) |

Then run `sql/verify.sql` in the **SQL editor**.

## Step 6: Practise incremental loading (the most realistic part)

1. Run `01_generate_source_files` with `mode = increment`, `increment_date = 2026-11-01`.
   It also changes the SAP tables: one bill is corrected and two payments are added, all with
   `AEDAT = 20261101`. In the job run, `01b_sap_extract` prints the high-water mark, the extract
   window (high-water mark − 1 day) and the linked-server `OPENQUERY` it would send to SAP, and
   lands only the changed rows.
   It adds one day of reads (with a **duplicate** and a **late correction** for Oct 31) and that
   day's calls.
2. Run the job again.
3. Check:
   - bronze loaded **only the new file** (Auto Loader checkpoint)
   - silver processed **only new rows** (`ops.watermarks`)
   - the duplicate didn't create a second row, and the Oct 31 reading for that meter now shows
     `kwh = 31.5` (MERGE updated it)

```sql
SELECT * FROM utility_lab.silver.meter_read_daily
WHERE meter_id = 'M0000002' AND read_date >= '2026-10-30' ORDER BY read_date;
SELECT * FROM utility_lab.ops.watermarks;
```

## Step 7: Power BI

Follow `powerbi/POWERBI_GUIDE.md`.

## Updating from the earlier version

The SAP extract step changed how billing and payment files arrive. If you ran the earlier
version, start clean so silver gets the new `changed_on` column for every row:
`DROP CATALOG utility_lab CASCADE;`, then run `00_setup`, `01_generate_source_files`
(`mode = initial`) and the job again.

## Step 8: Clean up (optional)

```sql
DROP CATALOG utility_lab CASCADE;   -- or drop the schemas if you used an existing catalog
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: utility_lakehouse` | The notebook must stay in the project's `notebooks/` folder, because it loads `../src` |
| `ModuleNotFoundError: yaml` | Rerun the `%pip install pyyaml` cell, then the rest |
| Bronze shows "retrying with the evolved schema" | Normal: Auto Loader found a new column and restarted |
| `CREATE CATALOG` permission error | Use an existing catalog name in the `catalog` widget / job parameter |
| Data-quality gate fails | Open `ops.dq_results` for the latest run to see which CRITICAL check failed |
