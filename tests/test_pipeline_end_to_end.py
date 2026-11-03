"""End-to-end test: generate source files -> bronze -> silver -> gold -> metrics, locally.

Runs the same transformation code, gold SQL and metric SQL that the Databricks notebooks
use. Only the Databricks-specific parts (Auto Loader, Delta MERGE) are replaced by plain
batch reads and overwrites.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from utility_lakehouse import quality
from utility_lakehouse.generator import generate
from utility_lakehouse.gold_sql import GOLD_TABLES, render
from utility_lakehouse.metrics import all_metrics_sql, validate
from utility_lakehouse.pipeline import load_yaml, prepare_silver
from utility_lakehouse.transforms import scd2_from_versions

CAT = "spark_catalog"


def save(spark, df, name):
    spark.sql(f"DROP TABLE IF EXISTS {CAT}.{name}")
    df.write.mode("overwrite").saveAsTable(f"{CAT}.{name}")


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    summary = generate(str(landing), start=date(2026, 5, 1), days=92, n_customers=150, seed=7)
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    sources = load_yaml("sources.yml")
    quarantine_counts = {}
    for name, spec in sources.items():
        path = str(landing / name)
        reader = spark.read.option("header", True) if spec["format"] == "csv" else spark.read.option("primitivesAsString", True)
        raw = (reader.csv(path) if spec["format"] == "csv" else reader.json(path))
        bronze = raw.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))
        save(spark, bronze, f"bronze.{name}")
        good, bad = prepare_silver(spark.table(f"{CAT}.bronze.{name}"), spec)
        quarantine_counts[name] = bad.count()
        if spec.get("scd2"):
            good = scd2_from_versions(good, spec["keys"], spec["order_by"], spec["scd2_attributes"])
        save(spark, good, f"silver.{spec['silver_table']}")
    for table, _cluster, sql in GOLD_TABLES:
        save(spark, spark.sql(render(sql, CAT)), f"gold.{table}")
    metrics = load_yaml("metrics.yml")["metrics"]
    save(spark, spark.sql(all_metrics_sql(metrics, CAT)), "gold.metric_values")
    return {"summary": summary, "quarantine": quarantine_counts, "metrics": metrics}


def test_metric_catalogue_is_valid():
    metrics = load_yaml("metrics.yml")["metrics"]
    assert validate(metrics) == []
    assert len(metrics) >= 50


def test_planted_bad_records_are_quarantined_not_lost(spark, built):
    assert built["quarantine"]["ami_meter_reads"] > 0          # negative / null kWh, null meter
    assert built["quarantine"]["oms_outages"] >= 1             # restore before start
    bronze = spark.table(f"{CAT}.bronze.ami_meter_reads").count()
    silver = spark.table(f"{CAT}.silver.meter_read_daily").count()
    assert silver < bronze                                     # duplicates and bad rows removed
    assert quality.unique(spark.table(f"{CAT}.silver.meter_read_daily"), "silver.meter_read_daily",
                          ["meter_id", "read_date"])["status"] == "PASS"


def test_scd2_history_has_one_current_row_per_customer(spark):
    scd = spark.table(f"{CAT}.silver.customer_scd2")
    assert scd.where("is_current").groupBy("partner_id").count().where("count > 1").count() == 0
    assert scd.count() > scd.select("partner_id").distinct().count()   # some customers changed


def test_schema_drift_column_is_kept(spark):
    reads = spark.table(f"{CAT}.silver.meter_read_daily")
    assert reads.where("voltage_v IS NOT NULL").count() > 0
    assert reads.where("voltage_v IS NULL").count() > 0        # early files had no voltage


def test_sap_reversal_amounts_are_negative(spark):
    rev = spark.table(f"{CAT}.gold.fact_billing").where("bill_status = 'REVERSAL'")
    if rev.count():
        assert rev.where("billed_amount >= 0").count() == 0


def test_gold_reconciles_to_silver(spark):
    s = spark.sql(f"SELECT sum(billed_amount) v FROM {CAT}.silver.billing_document WHERE bill_status='BILLED'").first().v
    g = spark.sql(f"SELECT sum(billed_amount) v FROM {CAT}.gold.fact_billing WHERE bill_status='BILLED'").first().v
    assert quality.reconcile("billed_amount", "gold.fact_billing", s, g)["status"] == "PASS"
    assert spark.table(f"{CAT}.gold.fact_billing").where("region IS NULL").count() == 0


def test_every_metric_produces_values(spark, built):
    mv = spark.table(f"{CAT}.gold.metric_values")
    ids = {r.metric_id for r in mv.select("metric_id").distinct().collect()}
    missing = {m["id"] for m in built["metrics"]} - ids
    assert not missing, f"metrics with no rows: {missing}"


def test_all_region_rollup_is_recalculated_not_averaged(spark):
    # SAIDI for ALL must equal total customer-minutes / total customers served
    row = spark.sql(f"""
        SELECT month_start, sum(customer_minutes_interrupted) / sum(customers_served) AS expected
        FROM {CAT}.gold.fact_reliability_region_month GROUP BY month_start ORDER BY month_start LIMIT 1""").first()
    got = spark.sql(f"""SELECT value FROM {CAT}.gold.metric_values
                        WHERE metric_id='REL002' AND region='ALL' AND month = DATE'{row.month_start}'""").first().value
    assert abs(got - float(row.expected)) < 1e-9


def test_metric_values_are_sensible(spark):
    v = {r.metric_id: r.value for r in spark.sql(
        f"SELECT metric_id, avg(value) value FROM {CAT}.gold.metric_values WHERE region='ALL' GROUP BY metric_id").collect()}
    for pct in ("CC003", "CC006", "CC007", "USE003", "PAY003", "BIL005", "AR005"):
        assert 0 <= v[pct] <= 1, pct
    assert v["REL002"] > 0 and v["REL003"] > 0
    assert v["BIL001"] > 0 and v["USE001"] > 0
