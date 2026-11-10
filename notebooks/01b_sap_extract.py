# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Incremental SAP extract (changed-on date)
# MAGIC Pulls **only the SAP records that changed since the last load**, using SAP's own
# MAGIC changed-on date (`AEDAT`), and lands them as one file per run for Auto Loader.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 | **High-water mark** = `max(changed_on)` already in the silver table. No separate watermark table: the loaded data is the watermark. |
# MAGIC | 2 | **Window** = high-water mark minus 1 day. SAP dates have no time part, so a one-day overlap catches late changes. First run: full load. |
# MAGIC | 3 | **Query SAP** for `AEDAT >= window`. In production this is a SQL Server linked server (`OPENQUERY`), ADF's SAP connector or JDBC; the statement is printed below. In this lab the SAP tables are files in `_sap_system/`. |
# MAGIC | 4 | **Land** the changed rows in `sap_billing_doc/` and `sap_payment/`. Bronze (Auto Loader) loads the new file once; silver **MERGEs on the SAP key** (BELNR, OPBEL), newest `changed_on` wins, so the overlap never double-counts. |
# MAGIC
# MAGIC A failed run recovers by itself: the next run starts from what actually reached silver.
# MAGIC Limit: a changed-on filter cannot see hard deletes (use an SAP deletion flag or a periodic key compare).

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")
RAW = f"/Volumes/{CAT}/landing/raw"

import os, sys
sys.path.append(os.path.abspath("../src"))
from datetime import datetime
from utility_lakehouse.pipeline import load_yaml
from utility_lakehouse.sap_extract import SAP_SYSTEM_DIR, extract_source, openquery_sql

SOURCES = load_yaml("sources.yml", os.path.abspath("../config"))
RUN = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

results = []
for name, spec in SOURCES.items():
    cfg = spec.get("sap_extract")
    if not cfg:
        continue
    r = extract_source(spark,
                       sap_csv_path=f"{RAW}/{SAP_SYSTEM_DIR}/{cfg['sap_table']}.csv",
                       target_table=f"{CAT}.silver.{spec['silver_table']}",
                       spec=spec,
                       out_path=f"{RAW}/{name}/extract_{RUN}.csv")
    print(f"{name:18s} high-water mark {r['high_water_mark']}  extract from {r['since'] or 'FULL LOAD'}  rows {r['rows']:,}")
    print("   production query:", openquery_sql("SAP_LINK", cfg["sap_table"].upper(), list(spec["columns"]),
                                                 cfg["change_col"], r["since"]))
    results.append((name, str(r["high_water_mark"]), r["since"] or "FULL", r["rows"]))

# COMMAND ----------

display(spark.createDataFrame(results, "source string, high_water_mark string, extract_from string, rows long"))
