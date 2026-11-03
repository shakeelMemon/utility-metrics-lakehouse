# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: clean, conform, de-duplicate, keep history
# MAGIC For each source, processes only **new bronze rows** (watermark on `_ingest_ts`), then:
# MAGIC 1. **Conform:** SAP technical names → business names (`VKONT` → `account_id`), SAP dates
# MAGIC    (`yyyyMMdd`), SAP trailing-minus amounts (`125.40-`), `X` flags → booleans.
# MAGIC 2. **Quality rules** from `config/sources.yml`. Failing rows go to `silver.quarantine`
# MAGIC    with the reason. Nothing is deleted.
# MAGIC 3. **De-duplicate** to one row per business key (latest wins).
# MAGIC 4. **MERGE** into the silver table: upsert, so reruns and late-arriving corrections are safe
# MAGIC    (**idempotent**).
# MAGIC 5. **SCD Type 2** for customers: a changed segment or name closes the old row and inserts a new one.

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

import os, sys, uuid
from datetime import datetime
sys.path.append(os.path.abspath("../src"))
from pyspark.sql import functions as F
from utility_lakehouse.pipeline import load_yaml, prepare_silver
from utility_lakehouse.transforms import scd2_versions

SOURCES = load_yaml("sources.yml", os.path.abspath("../config"))
RUN_ID = str(uuid.uuid4())

# COMMAND ----------

def get_watermark(source):
    row = spark.sql(f"SELECT max(last_ingest_ts) AS ts FROM {CAT}.ops.watermarks WHERE source = '{source}'").first()
    return row.ts


def set_watermark(source, ts):
    spark.sql(f"""MERGE INTO {CAT}.ops.watermarks t
                  USING (SELECT '{source}' AS source, TIMESTAMP'{ts}' AS last_ingest_ts) s ON t.source = s.source
                  WHEN MATCHED THEN UPDATE SET t.last_ingest_ts = s.last_ingest_ts, t.updated_at = current_timestamp()
                  WHEN NOT MATCHED THEN INSERT (source, last_ingest_ts, updated_at)
                                        VALUES (s.source, s.last_ingest_ts, current_timestamp())""")


def table_exists(name):
    return spark.catalog.tableExists(name)


def upsert(df, target, keys):
    """Idempotent upsert: insert new keys, update existing keys only with newer data."""
    df.createOrReplaceTempView("_src")
    if not table_exists(target):
        spark.sql(f"CREATE TABLE {target} CLUSTER BY ({', '.join(keys)}) AS SELECT * FROM _src WHERE 1 = 0")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""MERGE INTO {target} t USING _src s ON {on}
                  WHEN MATCHED AND s._ingest_ts >= t._ingest_ts THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


def apply_scd2(df, target, spec):
    """SCD Type 2 with MERGE, one version rank at a time so every change is kept in order."""
    key, change = spec["keys"][0], spec["order_by"]
    versions = scd2_versions(df, spec["keys"], change, spec["scd2_attributes"])
    cols = [c for c in versions.columns if c != "version_rank"]
    versions.createOrReplaceTempView("_versions")
    if not table_exists(target):
        spark.sql(f"""CREATE TABLE {target} CLUSTER BY ({key}) AS
                      SELECT {', '.join(cols)}, {change} AS valid_from, CAST(NULL AS DATE) AS valid_to,
                             true AS is_current FROM _versions WHERE 1 = 0""")
    max_rank = versions.agg(F.max("version_rank")).first()[0] or 0
    insert_cols = ", ".join(cols + ["valid_from", "valid_to", "is_current"])
    insert_vals = ", ".join([f"u.{c}" for c in cols] + [f"u.{change}", "NULL", "true"])
    for r in range(1, max_rank + 1):
        spark.sql(f"""
        MERGE INTO {target} t
        USING (
          SELECT s.{key} AS merge_key, s.* FROM _versions s WHERE s.version_rank = {r}
          UNION ALL
          SELECT NULL AS merge_key, s.* FROM _versions s
          JOIN {target} c ON c.{key} = s.{key} AND c.is_current
                         AND c.attr_hash <> s.attr_hash AND s.{change} > c.valid_from
          WHERE s.version_rank = {r}
        ) u
        ON t.{key} = u.merge_key AND t.is_current
        WHEN MATCHED AND t.attr_hash <> u.attr_hash AND u.{change} > t.valid_from
          THEN UPDATE SET t.is_current = false, t.valid_to = u.{change}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})""")


def audit(table, rows_in, rows_out, rows_bad, status, msg, started):
    spark.createDataFrame([(RUN_ID, "silver", table, rows_in, rows_out, rows_bad, status, msg, started, datetime.now())],
                          "run_id string, step string, table_name string, rows_in long, rows_out long, "
                          "rows_quarantined long, status string, message string, started_at timestamp, finished_at timestamp") \
         .write.mode("append").saveAsTable(f"{CAT}.ops.load_audit")

# COMMAND ----------

for name, spec in SOURCES.items():
    started = spark.sql("SELECT current_timestamp() AS ts").first().ts
    target = f"{CAT}.silver.{spec['silver_table']}"
    wm = get_watermark(name)
    batch = spark.table(f"{CAT}.bronze.{name}")
    if wm is not None:
        batch = batch.where(F.col("_ingest_ts") > F.lit(wm))
    rows_in = batch.count()
    if rows_in == 0:
        print(f"{name:22s} nothing new")
        continue
    new_wm = batch.agg(F.max("_ingest_ts")).first()[0]

    good, bad = prepare_silver(batch, spec)
    rows_bad = bad.count()
    if rows_bad:
        bad.withColumn("quarantined_at", F.current_timestamp()).write.mode("append").saveAsTable(f"{CAT}.silver.quarantine")

    if spec.get("scd2"):
        apply_scd2(good, target, spec)
    else:
        upsert(good, target, spec["keys"])
    rows_out = good.count()
    set_watermark(name, new_wm)
    audit(f"silver.{spec['silver_table']}", rows_in, rows_out, rows_bad, "SUCCESS", "", started)
    print(f"{name:22s} in {rows_in:>8,}  merged {rows_out:>8,}  quarantined {rows_bad:>6,}")

# COMMAND ----------

# MAGIC %md ### Check the quarantine: why were rows rejected?

# COMMAND ----------

display(spark.sql(f"""SELECT target_table, dq_reason, count(*) AS rows
                      FROM {CAT}.silver.quarantine GROUP BY ALL ORDER BY rows DESC"""))
