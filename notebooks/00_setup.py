# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the catalog, the medallion schemas, the landing volume and the operations tables.
# MAGIC
# MAGIC | Schema | Purpose |
# MAGIC |---|---|
# MAGIC | `landing` | Unity Catalog volume `raw` where source files arrive, plus `checkpoints` for Auto Loader |
# MAGIC | `bronze` | Raw data exactly as received (all strings) + ingest time and source file |
# MAGIC | `silver` | Cleaned, conformed, de-duplicated; SAP names mapped to business names; SCD Type 2 history |
# MAGIC | `vault` | Data Vault 2.0 audit layer: hubs, links, satellites (insert-only, full history) |
# MAGIC | `gold` | Star schema (facts and dimensions) and the metric layer that Power BI reads |
# MAGIC | `ops` | Load audit, data-quality results and incremental watermarks |

# COMMAND ----------

dbutils.widgets.text("catalog", "utility_lab", "Catalog")
CAT = dbutils.widgets.get("catalog")

# COMMAND ----------

# Free Edition / some workspaces don't allow CREATE CATALOG. If this fails, set the
# 'catalog' widget to a catalog you can use (for example 'workspace') and run again.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CAT}")
except Exception as e:
    print(f"Could not create catalog '{CAT}' ({type(e).__name__}). Using it if it already exists.")

for schema, comment in [
    ("landing", "Source files (volume) and Auto Loader checkpoints"),
    ("bronze", "Raw data as received"),
    ("silver", "Cleaned and conformed data"),
    ("gold", "Business-ready star schema and metrics for Power BI"),
    ("vault", "Data Vault 2.0: insert-only hubs, links and satellites with load date and record source"),
    ("ops", "Audit, data quality and watermarks"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.landing.raw COMMENT 'Landing zone for source extracts'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.landing.checkpoints COMMENT 'Auto Loader schema and checkpoints'")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.ops.load_audit (
  run_id STRING, step STRING, table_name STRING, rows_in BIGINT, rows_out BIGINT,
  rows_quarantined BIGINT, status STRING, message STRING, started_at TIMESTAMP, finished_at TIMESTAMP)
COMMENT 'One row per table per pipeline step'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.ops.dq_results (
  run_id STRING, run_ts TIMESTAMP, check STRING, table_name STRING, severity STRING,
  status STRING, observed STRING, expected STRING)
COMMENT 'Data-quality check results'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.ops.watermarks (
  source STRING, last_ingest_ts TIMESTAMP, updated_at TIMESTAMP)
COMMENT 'Last bronze ingest time processed into silver, per source'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.silver.quarantine (
  target_table STRING, dq_reason STRING, record_json STRING, _source_file STRING,
  _ingest_ts TIMESTAMP, quarantined_at TIMESTAMP)
COMMENT 'Rows that failed data-quality rules, kept for investigation and replay'""")

print(f"Setup complete. Landing folder: /Volumes/{CAT}/landing/raw")
