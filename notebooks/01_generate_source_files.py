# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate source files
# MAGIC Writes synthetic utility extracts into the landing volume, as if SAP, the AMI head-end,
# MAGIC the contact centre and the outage system had dropped their files.
# MAGIC
# MAGIC * **mode = initial**: six months of history (May–Oct 2026). Run once.
# MAGIC * **mode = increment**: one extra day of meter reads and calls, including a duplicate and a
# MAGIC   late correction. Use it to practise incremental loads: run it, then run the pipeline job again.

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
dbutils.widgets.dropdown("mode", "initial", ["initial", "increment"], "Mode")
dbutils.widgets.text("increment_date", "2026-11-01", "Increment date (YYYY-MM-DD)")
dbutils.widgets.text("customers", "1200", "Number of customers")

CAT = dbutils.widgets.get("catalog")
MODE = dbutils.widgets.get("mode")
RAW = f"/Volumes/{CAT}/landing/raw"

import os, sys
sys.path.append(os.path.abspath("../src"))
from datetime import date
from utility_lakehouse.generator import generate, generate_increment

# COMMAND ----------

n = int(dbutils.widgets.get("customers"))
if MODE == "initial":
    summary = generate(RAW, start=date(2026, 5, 1), days=184, n_customers=n, seed=42)
else:
    summary = generate_increment(RAW, date.fromisoformat(dbutils.widgets.get("increment_date")), n_customers=n, seed=42)
print(summary)

# COMMAND ----------

display(dbutils.fs.ls(RAW))
