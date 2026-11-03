# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Table maintenance
# MAGIC * **OPTIMIZE:** compacts small files and applies liquid clustering (`CLUSTER BY` on each table).
# MAGIC   The fix for the small-file problem.
# MAGIC * **VACUUM:** removes files no longer referenced by the table (orphan files) after the
# MAGIC   retention period (7 days by default), which keeps storage cost down. Time travel only
# MAGIC   works within that retention window.
# MAGIC
# MAGIC With Unity Catalog managed tables, *predictive optimization* can run these automatically.
# MAGIC Running them yourself is still good practice to understand them.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

# COMMAND ----------

tables = spark.sql(f"""SELECT table_schema, table_name FROM {CAT}.information_schema.tables
                       WHERE table_schema IN ('silver', 'gold') AND table_type <> 'VIEW'""").collect()
for t in tables:
    name = f"{CAT}.{t.table_schema}.{t.table_name}"
    spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name}")
    print("optimized and vacuumed", name)

# COMMAND ----------

# MAGIC %md ### Time travel: see a table's history and query an older version

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CAT}.silver.meter_read_daily LIMIT 10"))
