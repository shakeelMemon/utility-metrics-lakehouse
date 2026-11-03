# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: star schema for Power BI
# MAGIC Builds the dimensions and facts from silver. The SQL lives in
# MAGIC `src/utility_lakehouse/gold_sql.py` (the same SQL is tested locally with pytest).
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `dim_date` | one row per day |
# MAGIC | `dim_customer` | one row per customer version (SCD Type 2) |
# MAGIC | `dim_premise`, `dim_account` | one row per premise / contract account |
# MAGIC | `fact_daily_usage` | meter × day |
# MAGIC | `fact_billing` | billing document |
# MAGIC | `fact_payment` | payment |
# MAGIC | `fact_ar_region_month` | month end × region × segment (AR aging snapshot) |
# MAGIC | `fact_contact_call` | call |
# MAGIC | `fact_outage` | outage event |
# MAGIC | `fact_reliability_region_month` | month × region (inputs for SAIDI / SAIFI / CAIDI) |

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

import os, sys, uuid
from datetime import datetime
sys.path.append(os.path.abspath("../src"))
from utility_lakehouse.gold_sql import GOLD_TABLES, render

RUN_ID = str(uuid.uuid4())

# COMMAND ----------

for name, cluster_by, sql in GOLD_TABLES:
    started = datetime.now()
    spark.sql(f"""CREATE OR REPLACE TABLE {CAT}.gold.{name}
                  CLUSTER BY ({', '.join(cluster_by)})
                  AS {render(sql, CAT)}""")
    rows = spark.table(f"{CAT}.gold.{name}").count()
    spark.sql(f"""INSERT INTO {CAT}.ops.load_audit VALUES
                  ('{RUN_ID}', 'gold', 'gold.{name}', NULL, {rows}, 0, 'SUCCESS', '',
                   TIMESTAMP'{started:%Y-%m-%d %H:%M:%S}', current_timestamp())""")
    print(f"gold.{name:30s} {rows:>10,} rows")

# COMMAND ----------

# MAGIC %md ### Quick look: billed revenue by region and month

# COMMAND ----------

display(spark.sql(f"""
SELECT bill_month, region, round(sum(billed_amount), 2) AS billed_revenue, count(*) AS bills
FROM {CAT}.gold.fact_billing WHERE bill_status = 'BILLED'
GROUP BY ALL ORDER BY bill_month, region"""))
