# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Data-quality gate
# MAGIC Runs checks on silver and gold, writes every result to `ops.dq_results`, and **fails the
# MAGIC job** if a CRITICAL check fails, so bad numbers never reach Power BI unnoticed.
# MAGIC
# MAGIC * Completeness: row counts, not-null keys
# MAGIC * Uniqueness: one row per business key
# MAGIC * Validity: accepted values, value ranges
# MAGIC * Freshness: latest data date
# MAGIC * Reconciliation: silver totals = gold totals
# MAGIC * Metric checks: every catalogue metric produced values; percentages between 0 and 1

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
dbutils.widgets.text("freshness_days", "60", "Max data age in days (WARN)")
CAT = dbutils.widgets.get("catalog")

import os, sys, uuid
from datetime import date, datetime
sys.path.append(os.path.abspath("../src"))
from utility_lakehouse import quality as q
from utility_lakehouse.pipeline import load_yaml

SOURCES = load_yaml("sources.yml", os.path.abspath("../config"))
T = lambda name: spark.table(f"{CAT}.{name}")
results = []

# COMMAND ----------

# silver: completeness and uniqueness for every source table
for spec in SOURCES.values():
    t = f"silver.{spec['silver_table']}"
    df = T(t)
    if spec.get("scd2"):
        df = df.where("is_current")
    results += [q.row_count_min(df, t), q.not_null(df, t, spec["keys"]), q.unique(df, t, spec["keys"])]

# validity and freshness
results.append(q.accepted_values(T("silver.billing_document"), "silver.billing_document", "bill_status",
                                 ["BILLED", "REVERSED", "REVERSAL"]))
results.append(q.range_check(T("silver.meter_read_daily"), "silver.meter_read_daily", "kwh >= 0", "kwh >= 0", "CRITICAL"))
results.append(q.freshness(T("silver.meter_read_daily"), "silver.meter_read_daily", "read_date",
                           date.today(), int(dbutils.widgets.get("freshness_days"))))

# gold: conformed dimensions joined (no orphan facts)
results.append(q.not_null(T("gold.fact_billing"), "gold.fact_billing", ["region", "segment"], "WARN"))
results.append(q.not_null(T("gold.fact_daily_usage"), "gold.fact_daily_usage", ["account_id"], "WARN"))

# reconciliation: silver -> gold totals must match
s_bill = spark.sql(f"SELECT sum(billed_amount) v FROM {CAT}.silver.billing_document WHERE bill_status='BILLED'").first().v
g_bill = spark.sql(f"SELECT sum(billed_amount) v FROM {CAT}.gold.fact_billing WHERE bill_status='BILLED'").first().v
results.append(q.reconcile("billed_amount silver vs gold", "gold.fact_billing", s_bill, g_bill))
s_kwh = spark.sql(f"SELECT sum(kwh) v FROM {CAT}.silver.meter_read_daily").first().v
g_kwh = spark.sql(f"SELECT sum(kwh) v FROM {CAT}.gold.fact_daily_usage").first().v
results.append(q.reconcile("kWh silver vs gold", "gold.fact_daily_usage", s_kwh, g_kwh, tolerance=0.5))

# metrics: every catalogue metric has values, percentages are 0..1
missing = spark.sql(f"""SELECT count(*) n FROM {CAT}.gold.metric_catalog c
                        LEFT ANTI JOIN {CAT}.gold.metric_values v USING (metric_id)""").first().n
results.append(q._row("metrics_have_values", "gold.metric_values", "CRITICAL", missing == 0, missing, "0 metrics without values"))
pct = spark.sql(f"""SELECT v.* FROM {CAT}.gold.metric_values v JOIN {CAT}.gold.metric_catalog c USING (metric_id)
                    WHERE c.unit = 'pct'""")
results.append(q.range_check(pct, "gold.metric_values", "value BETWEEN 0 AND 1", "pct metrics 0..1"))

# COMMAND ----------

run_id, run_ts = str(uuid.uuid4()), datetime.now()
out = spark.createDataFrame([{**r, "run_id": run_id, "run_ts": run_ts} for r in results]) \
           .select("run_id", "run_ts", "check", "table_name", "severity", "status", "observed", "expected")
out.write.mode("append").saveAsTable(f"{CAT}.ops.dq_results")
display(out.orderBy("status", "severity"))

failed = [r for r in results if r["status"] == "FAIL" and r["severity"] == "CRITICAL"]
if failed:
    raise RuntimeError(f"{len(failed)} CRITICAL data-quality check(s) failed: " + "; ".join(r["check"] for r in failed))
print(f"Data-quality gate passed: {len(results)} checks, "
      f"{sum(r['status'] == 'FAIL' for r in results)} warnings")
