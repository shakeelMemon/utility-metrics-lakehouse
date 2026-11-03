# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Performance lab: data skew, broadcast joins and AQE
# MAGIC A hands-on practice notebook (not part of the job). It builds a deliberately **skewed**
# MAGIC dataset, one "hot" key with most of the rows, the way one large commercial customer or one
# MAGIC region can dominate utility data. Then it fixes the skew three ways.
# MAGIC
# MAGIC Open the **Spark UI → Stages** after each cell and compare task durations: with skew, one
# MAGIC task runs much longer than the others.

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")
from pyspark.sql import functions as F

# 90% of rows on one meter = skew
usage = spark.table(f"{CAT}.gold.fact_daily_usage")
hot = usage.where("meter_id = 'M0000001'")
skewed = usage.unionByName(hot.crossJoin(spark.range(200).withColumnRenamed("id", "copy")).drop("copy"))
skewed.groupBy(F.col("meter_id") == "M0000001").count().show()

# COMMAND ----------

# MAGIC %md ### 1. Broadcast join: the small side is copied to every executor, so there's no shuffle

# COMMAND ----------

premise = spark.table(f"{CAT}.gold.dim_premise")          # small dimension
joined = skewed.join(F.broadcast(premise.select("premise_id", "premise_type")), "premise_id")
joined.groupBy("premise_type").agg(F.sum("kwh").alias("kwh")).show()
joined.explain()    # look for BroadcastHashJoin

# COMMAND ----------

# MAGIC %md ### 2. Adaptive Query Execution (AQE): Spark splits skewed partitions at runtime
# MAGIC On Databricks these are on by default; shown here so you can explain them.

# COMMAND ----------

for k in ["spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled",
          "spark.sql.adaptive.coalescePartitions.enabled"]:
    try:
        print(k, "=", spark.conf.get(k))
    except Exception:
        print(k, "= (managed by the platform)")

# COMMAND ----------

# MAGIC %md ### 3. Salting: spread one hot key over N sub-keys, aggregate, then combine
# MAGIC Step 1 aggregates by (key, salt), so the hot key is split across N tasks. Step 2 sums the
# MAGIC partial results by key. The total is identical; only the work is spread out.

# COMMAND ----------

N = 16
salted = skewed.withColumn("salt", (F.rand(seed=1) * N).cast("int"))
partial = salted.groupBy("meter_id", "salt").agg(F.sum("kwh").alias("kwh_part"))
result = partial.groupBy("meter_id").agg(F.sum("kwh_part").alias("total_kwh"))

check = skewed.groupBy("meter_id").agg(F.sum("kwh").alias("total_kwh"))
diff = result.join(check, "meter_id").where(F.abs(result.total_kwh - check.total_kwh) > 1e-6).count()
print("Rows where salted result differs from the direct result:", diff)   # expect 0
