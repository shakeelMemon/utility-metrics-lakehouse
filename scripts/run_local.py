"""Run the whole pipeline locally with plain PySpark (no Databricks), for a quick check.

    pip install pyspark==4.0.1 pyyaml
    python scripts/run_local.py --out ./local_run

Uses batch reads instead of Auto Loader and overwrites instead of Delta MERGE; the
transformation code, gold SQL and metric SQL are the same as in the notebooks.
"""
import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from utility_lakehouse.generator import generate  # noqa: E402
from utility_lakehouse.gold_sql import GOLD_TABLES, render  # noqa: E402
from utility_lakehouse.metrics import all_metrics_sql  # noqa: E402
from utility_lakehouse.pipeline import load_yaml, prepare_silver  # noqa: E402
from utility_lakehouse.sap_extract import SAP_SYSTEM_DIR, extract_source  # noqa: E402
from utility_lakehouse.transforms import scd2_from_versions  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--out", default="./local_run")
ap.add_argument("--customers", type=int, default=1200)
args = ap.parse_args()
out = Path(args.out).resolve()

spark = (SparkSession.builder.master("local[*]").config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", str(out / "warehouse")).getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(generate(str(out / "landing"), start=date(2026, 5, 1), days=184, n_customers=args.customers))
for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {s}")


def save(df, name):
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    df.write.mode("overwrite").saveAsTable(name)


for name, spec in load_yaml("sources.yml").items():
    path = str(out / "landing" / name)
    if spec.get("sap_extract"):                      # incremental SAP extract (notebook 01b)
        r = extract_source(spark, str(out / "landing" / SAP_SYSTEM_DIR / f"{spec['sap_extract']['sap_table']}.csv"),
                           f"silver.{spec['silver_table']}", spec, f"{path}/extract_local.csv")
        print(f"{name:22s} SAP extract from {r['since'] or 'FULL'}: {r['rows']:,} rows")
    raw = (spark.read.option("header", True).csv(path) if spec["format"] == "csv"
           else spark.read.option("primitivesAsString", True).json(path))
    save(raw.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path")),
         f"bronze.{name}")
    good, bad = prepare_silver(spark.table(f"bronze.{name}"), spec)
    if spec.get("scd2"):
        good = scd2_from_versions(good, spec["keys"], spec["order_by"], spec["scd2_attributes"])
    save(good, f"silver.{spec['silver_table']}")
    print(f"{name:22s} silver {spark.table('silver.' + spec['silver_table']).count():>8,}  quarantined {bad.count():>5,}")
for table, _c, sql in GOLD_TABLES:
    save(spark.sql(render(sql, "spark_catalog")), f"gold.{table}")
save(spark.sql(all_metrics_sql(load_yaml("metrics.yml")["metrics"], "spark_catalog")), "gold.metric_values")
spark.sql("""SELECT metric_id, round(value, 4) AS value FROM gold.metric_values
             WHERE region = 'ALL' AND month = DATE'2026-09-01' ORDER BY metric_id""").show(60, truncate=False)
