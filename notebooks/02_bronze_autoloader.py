# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingest with Auto Loader
# MAGIC Loads every new file from the landing volume into a bronze Delta table, **exactly as
# MAGIC received** (all columns as strings), plus `_ingest_ts` and `_source_file`.
# MAGIC
# MAGIC * **Incremental:** Auto Loader keeps a checkpoint, so each file is loaded once.
# MAGIC   `trigger(availableNow=True)` processes what's new, then stops (batch-style, cheap).
# MAGIC * **Schema drift:** `addNewColumns` adds new fields (e.g. `voltage_v` in later AMI files)
# MAGIC   instead of losing them. When a new field appears the stream stops once by design;
# MAGIC   the retry loop below restarts it with the new schema.

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")
RAW = f"/Volumes/{CAT}/landing/raw"
CHK = f"/Volumes/{CAT}/landing/checkpoints"

import os, sys, uuid
from datetime import datetime
sys.path.append(os.path.abspath("../src"))
from pyspark.sql import functions as F
from utility_lakehouse.pipeline import load_yaml

SOURCES = load_yaml("sources.yml", os.path.abspath("../config"))
RUN_ID = str(uuid.uuid4())

# COMMAND ----------

def ingest(name: str, fmt: str) -> None:
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{CHK}/{name}/schema")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("cloudFiles.inferColumnTypes", "false"))      # bronze keeps raw strings
    if fmt == "csv":
        reader = reader.option("header", "true")
    stream = (reader.load(f"{RAW}/{name}")
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path")))
    (stream.writeStream
           .option("checkpointLocation", f"{CHK}/{name}/bronze")
           .option("mergeSchema", "true")
           .trigger(availableNow=True)
           .toTable(f"{CAT}.bronze.{name}")
           .awaitTermination())


def audit(name, rows, status, message, started):
    spark.createDataFrame([(RUN_ID, "bronze", f"bronze.{name}", rows, rows, 0, status, message, started, datetime.now())],
                          "run_id string, step string, table_name string, rows_in long, rows_out long, "
                          "rows_quarantined long, status string, message string, started_at timestamp, finished_at timestamp") \
         .write.mode("append").saveAsTable(f"{CAT}.ops.load_audit")

# COMMAND ----------

failures = []
for name, spec in SOURCES.items():
    started = spark.sql("SELECT current_timestamp() AS ts").first().ts
    for attempt in range(1, 4):
        try:
            ingest(name, spec["format"])
            break
        except Exception as e:                       # new column found -> restart with evolved schema
            print(f"{name}: attempt {attempt} stopped ({type(e).__name__}); retrying with the evolved schema")
            if attempt == 3:
                failures.append(name)
                audit(name, 0, "FAILED", str(e)[:500], started)
    if name not in failures:
        rows = spark.table(f"{CAT}.bronze.{name}").where(F.col("_ingest_ts") >= F.lit(started)).count()
        audit(name, rows, "SUCCESS", "", started)
        print(f"{name:22s} new rows: {rows:>9,}")

if failures:
    raise RuntimeError(f"Bronze ingestion failed for: {failures}")
