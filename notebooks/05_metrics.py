# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Metric layer: every metric defined once
# MAGIC Reads `config/metrics.yml` (the metric catalogue: name, owner, definition, formula) and
# MAGIC writes:
# MAGIC * `gold.metric_catalog`: the business definition of every metric (for a "metric dictionary" page)
# MAGIC * `gold.metric_values`: month × region × metric, plus an `ALL` region row recalculated from detail
# MAGIC * `gold.v_metric_values`: values joined to names and units, ready for Power BI
# MAGIC
# MAGIC Adding metric 51, 52… 100 means adding entries to the YAML file. No report changes are needed.

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

import os, sys
sys.path.append(os.path.abspath("../src"))
from utility_lakehouse.metrics import all_metrics_sql, catalog_rows, validate
from utility_lakehouse.pipeline import load_yaml

METRICS = load_yaml("metrics.yml", os.path.abspath("../config"))["metrics"]
problems = validate(METRICS)
if problems:
    raise ValueError("Metric catalogue has problems:\n" + "\n".join(problems))
print(f"{len(METRICS)} metrics in the catalogue")

# COMMAND ----------

spark.createDataFrame(catalog_rows(METRICS, version="1.0")) \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CAT}.gold.metric_catalog")

spark.sql(f"""CREATE OR REPLACE TABLE {CAT}.gold.metric_values
              CLUSTER BY (metric_id, month)
              AS {all_metrics_sql(METRICS, CAT)}""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CAT}.gold.v_metric_values AS
SELECT v.metric_id, c.metric_name, c.domain, c.unit, c.owner, v.month, v.region, v.value, v.source_rows
FROM {CAT}.gold.metric_values v
JOIN {CAT}.gold.metric_catalog c USING (metric_id)""")

print(spark.table(f"{CAT}.gold.metric_values").count(), "metric values written")

# COMMAND ----------

# MAGIC %md ### Latest month, all regions

# COMMAND ----------

display(spark.sql(f"""
SELECT domain, metric_id, metric_name, unit, round(value, 4) AS value
FROM {CAT}.gold.v_metric_values
WHERE region = 'ALL' AND month = (SELECT max(month) FROM {CAT}.gold.metric_values WHERE metric_id = 'BIL001')
ORDER BY domain, metric_id"""))
