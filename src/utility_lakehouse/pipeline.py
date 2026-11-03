"""Glue between the source catalogue (config/sources.yml) and the transformations."""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .transforms import conform, dedupe_latest, split_quarantine

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def load_yaml(name: str, config_dir: str | Path | None = None) -> dict:
    path = Path(config_dir or CONFIG_DIR) / name
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def prepare_silver(bronze_df: DataFrame, spec: dict) -> tuple[DataFrame, DataFrame]:
    """Bronze rows -> (clean rows ready to MERGE, quarantined rows).

    1. conform: SAP field names -> business names, safe type casts
    2. quality rules: failing rows go to quarantine with the rule names
    3. de-duplicate to one row per business key (latest wins), except SCD2 sources,
       which keep every version so history can be built
    """
    conformed = conform(bronze_df, spec["columns"])
    good, bad = split_quarantine(conformed, spec.get("checks", {}))
    if not spec.get("scd2"):
        order = [spec["order_by"]] if spec.get("order_by") else []
        good = dedupe_latest(good, spec["keys"], order)
    quarantine = bad.select(
        F.lit(spec["silver_table"]).alias("target_table"),
        F.col("_dq_reason").alias("dq_reason"),
        F.to_json(F.struct(*[F.col(c) for c in bad.columns if c not in ("_dq_reason",)])).alias("record_json"),
        *[F.col(c) for c in ("_source_file", "_ingest_ts") if c in bad.columns],
    )
    return good, quarantine
