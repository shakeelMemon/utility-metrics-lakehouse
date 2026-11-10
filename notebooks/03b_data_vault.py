# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · Data Vault: hubs, links, satellites (auditable history)
# MAGIC Builds an **insert-only Data Vault 2.0** layer in schema `vault` from silver.
# MAGIC
# MAGIC | Object | Holds | Loaded how |
# MAGIC |---|---|---|
# MAGIC | **Hub** | one row per business key (`bill_id`, `account_id` …) + hash key | insert new keys only |
# MAGIC | **Link** | one row per relationship (account ↔ bill …) | insert new relationships only |
# MAGIC | **Satellite** | descriptive attributes with full history + `hash_diff` | insert only when `hash_diff` changes |
# MAGIC
# MAGIC Every row carries `load_dts` (when it was loaded) and `record_source` (the exact file it came
# MAGIC from), so any number can be traced back and any past state rebuilt. Nothing is updated or deleted.
# MAGIC Reruns add nothing (idempotent). Notebook 06 reconciles the vault to silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

import os, sys, uuid
from datetime import datetime
sys.path.append(os.path.abspath("../src"))
from utility_lakehouse import vault

RUN_ID, LOAD_TS = str(uuid.uuid4()), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.vault")
V = lambda name: f"{CAT}.vault.{name}"
S = lambda table: spark.table(f"{CAT}.silver.{table}")


def existing(name):
    return spark.table(V(name)) if spark.catalog.tableExists(V(name)) else None


def append(df, name, started):
    n = df.count()
    if n:
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(V(name))
    spark.createDataFrame([(RUN_ID, "vault", f"vault.{name}", n, n, 0, "SUCCESS", "", started, datetime.now())],
                          "run_id string, step string, table_name string, rows_in long, rows_out long, "
                          "rows_quarantined long, status string, message string, started_at timestamp, finished_at timestamp") \
         .write.mode("append").saveAsTable(f"{CAT}.ops.load_audit")
    print(f"{name:24s} +{n:>7,} new rows")

# COMMAND ----------

# MAGIC %md ### Hubs: one row per business key

# COMMAND ----------

for name, (table, keys) in vault.HUBS.items():
    started, hk = datetime.now(), name.replace("hub_", "") + "_hk"
    staged = vault.stage_hub(S(table), keys, hk, LOAD_TS)
    append(vault.new_rows(staged, existing(name), hk), name, started)

# COMMAND ----------

# MAGIC %md ### Links: one row per relationship

# COMMAND ----------

for name, (table, hubs) in vault.LINKS.items():
    started, lk = datetime.now(), name.replace("link_", "") + "_hk"
    staged = vault.stage_link(S(table), hubs, lk, LOAD_TS)
    append(vault.new_rows(staged, existing(name), lk), name, started)

# COMMAND ----------

# MAGIC %md ### Satellites: a new version only when the attributes really change

# COMMAND ----------

for name, (table, hk, keys, attrs, order) in vault.SATS.items():
    started = datetime.now()
    staged = vault.stage_sat(S(table), hk, keys, attrs, order, LOAD_TS)
    append(vault.new_sat_rows(staged, existing(name), hk), name, started)

# COMMAND ----------

# MAGIC %md ### Look at one bill's full history and where each version came from

# COMMAND ----------

display(spark.sql(f"""
  SELECT h.bill_id, s.billed_amount, s.bill_status, s.effective_from, s.load_dts, s.record_source
  FROM {V('hub_bill')} h JOIN {V('sat_bill')} s USING (bill_hk)
  WHERE h.bill_hk IN (SELECT bill_hk FROM {V('sat_bill')} GROUP BY bill_hk HAVING count(*) > 1 LIMIT 3)
  ORDER BY h.bill_id, s.effective_from"""))
