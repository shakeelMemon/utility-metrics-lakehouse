"""Incremental SAP extract using SAP's own changed-on date (for example AEDAT).

Pattern
-------
SAP keeps a changed-on date on each record. We pull only the records that changed since
the last load, and we take "the last load" from the **target table itself**:

    high-water mark = max(changed_on) already in silver
    window start    = high-water mark - overlap_days       (SAP dates have no time part)
    SAP query       = ... WHERE AEDAT >= window start
    silver          = MERGE on the SAP key, newest changed_on wins

So no separate watermark table is needed, a failed run recovers on its own (the next run
starts from what actually loaded), and the overlap re-reads a day that MERGE de-duplicates.

In production the query runs against SAP through a SQL Server linked server (OPENQUERY),
ADF's SAP connectors, or JDBC; ``openquery_sql`` builds that statement. In this project
the SAP tables are CSV files under ``_sap_system/`` (written by the generator), and
``select_changes`` applies the same filter with Spark.

Limit: a changed-on filter cannot see hard deletes. Use a deletion flag from SAP or a
periodic full compare of keys for that.
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SAP_SYSTEM_DIR = "_sap_system"          # folder that stands in for the SAP source tables


def window_start(high_water_mark: date | None, overlap_days: int = 1) -> str | None:
    """SAP yyyyMMdd date to extract from, or None for a first (full) load."""
    if high_water_mark is None:
        return None
    if overlap_days < 0:
        raise ValueError("overlap_days must be >= 0")
    return (high_water_mark - timedelta(days=overlap_days)).strftime("%Y%m%d")


def openquery_sql(link: str, sap_table: str, columns: list[str], change_col: str, since: str | None) -> str:
    """T-SQL that a SQL Server linked server would run against SAP.

    OPENQUERY does not accept variables, so the date is written into the query text.
    ``since`` is validated as an 8-digit SAP date, so nothing else can be injected.
    """
    cols = ", ".join(columns)
    inner = f"SELECT {cols} FROM {sap_table}"
    if since is not None:
        if not (len(since) == 8 and since.isdigit()):
            raise ValueError(f"since must be yyyyMMdd, got {since!r}")
        inner += f" WHERE {change_col} >= ''{since}''"
    return f"SELECT * FROM OPENQUERY({link}, '{inner}')"


def high_water_mark(spark, table: str, column: str = "changed_on") -> date | None:
    """Latest changed-on date already loaded into the target table (None if empty or missing)."""
    if not spark.catalog.tableExists(table):
        return None
    if column not in spark.table(table).columns:
        return None
    return spark.table(table).agg(F.max(column)).first()[0]


def select_changes(sap_df: DataFrame, change_col: str, since: str | None) -> DataFrame:
    """Rows of an SAP table changed on or after ``since`` (all rows when since is None).

    SAP dates are yyyyMMdd strings, so a string comparison orders them correctly.
    """
    if since is None:
        return sap_df
    return sap_df.where(F.col(change_col) >= F.lit(since))


def write_extract(df: DataFrame, path: str) -> int:
    """Write the extract as ONE CSV file (header + rows, all strings) and return the row count.

    One plain file per run keeps Auto Loader simple: it sees a new file and loads it once.
    """
    import csv
    import os

    rows = [[("" if v is None else str(v)) for v in r] for r in df.collect()]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows(rows)
    return len(rows)


def extract_source(spark, sap_csv_path: str, target_table: str, spec: dict, out_path: str) -> dict:
    """One incremental extract for one SAP table, end to end.

    1. high-water mark = max(changed_on) in the silver target
    2. read the SAP table and keep rows with change_col >= high-water mark - overlap
    3. write them as one CSV into the landing folder for Auto Loader

    Used by notebook 01b, scripts/run_local.py and the tests, so all three run the same code.
    """
    cfg = spec["sap_extract"]
    change_name = spec["columns"][cfg["change_col"]]["name"]          # AEDAT -> changed_on
    hwm = high_water_mark(spark, target_table, change_name)
    since = window_start(hwm, cfg.get("overlap_days", 1))
    sap_df = spark.read.option("header", True).csv(sap_csv_path)        # all columns as strings
    changes = select_changes(sap_df, cfg["change_col"], since)
    rows = write_extract(changes, out_path)
    return {"high_water_mark": hwm, "since": since, "rows": rows}
