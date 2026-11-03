"""Data-quality checks that return result rows for ops.dq_results.

Each check returns a dict: check, table_name, severity, status (PASS/FAIL), observed,
expected. The notebook writes all results, then fails the job if a CRITICAL check fails.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _row(check, table, severity, passed, observed, expected):
    return {"check": check, "table_name": table, "severity": severity,
            "status": "PASS" if passed else "FAIL", "observed": str(observed), "expected": str(expected)}


def row_count_min(df: DataFrame, table: str, minimum: int = 1, severity: str = "CRITICAL"):
    n = df.count()
    return _row("row_count_min", table, severity, n >= minimum, n, f">= {minimum}")


def not_null(df: DataFrame, table: str, cols: list[str], severity: str = "CRITICAL"):
    agg = df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    nulls = {c: int(agg[c] or 0) for c in cols}
    return _row(f"not_null({','.join(cols)})", table, severity, sum(nulls.values()) == 0, nulls, "0 nulls")


def unique(df: DataFrame, table: str, keys: list[str], severity: str = "CRITICAL"):
    dups = df.groupBy(*keys).count().where("count > 1").count()
    return _row(f"unique({','.join(keys)})", table, severity, dups == 0, dups, "0 duplicate keys")


def accepted_values(df: DataFrame, table: str, col: str, values: list[str], severity: str = "WARN"):
    bad = df.where(~F.col(col).isin(values) | F.col(col).isNull()).count()
    return _row(f"accepted_values({col})", table, severity, bad == 0, bad, f"in {values}")


def freshness(df: DataFrame, table: str, date_col: str, as_of: date, max_age_days: int, severity: str = "WARN"):
    latest = df.select(F.max(F.col(date_col).cast("date")).alias("d")).first()["d"]
    age = (as_of - latest).days if latest else None
    return _row(f"freshness({date_col})", table, severity, age is not None and age <= max_age_days,
                f"latest={latest}, age_days={age}", f"<= {max_age_days} days")


def reconcile(name: str, table: str, left: float, right: float, tolerance: float = 0.01, severity: str = "CRITICAL"):
    diff = abs((left or 0) - (right or 0))
    return _row(f"reconcile({name})", table, severity, diff <= tolerance, f"{left} vs {right} (diff {diff:.4f})",
                f"diff <= {tolerance}")


def range_check(df: DataFrame, table: str, expr: str, label: str, severity: str = "WARN"):
    bad = df.where(f"NOT ({expr})").count()
    return _row(f"range({label})", table, severity, bad == 0, bad, f"all rows satisfy {expr}")
