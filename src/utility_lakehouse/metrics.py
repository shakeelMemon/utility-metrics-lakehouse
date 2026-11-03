"""Metric framework: turn the metric catalogue (config/metrics.yml) into SQL.

Every metric produces rows of (metric_id, month, region, value). GROUPING SETS adds an
'ALL' region row that is recalculated from the detail rows. Ratios such as SAIDI or
abandon rate are therefore correct at every level, never an average of averages.
"""
from __future__ import annotations

import re

REQUIRED = ["id", "name", "domain", "owner", "unit", "description", "source", "date_col", "expression"]


def validate(metrics: list[dict]) -> list[str]:
    """Return a list of problems in the catalogue (empty list = valid)."""
    problems, seen = [], set()
    for m in metrics:
        mid = m.get("id", "<missing id>")
        for f in REQUIRED:
            if not m.get(f):
                problems.append(f"{mid}: missing '{f}'")
        if mid in seen:
            problems.append(f"{mid}: duplicate id")
        seen.add(mid)
        if not re.fullmatch(r"[A-Z]{2,4}\d{3}", str(mid)):
            problems.append(f"{mid}: id must look like ABC123")
        if ";" in str(m.get("expression", "")) or ";" in str(m.get("filter", "")):
            problems.append(f"{mid}: ';' is not allowed in expressions")
    return problems


def metric_sql(m: dict, catalog: str) -> str:
    source = f"{catalog}.{m['source']}"
    region = m.get("region_col", "region")
    where = (m.get("filter") or "true").replace("{cat}", catalog)
    return f"""
SELECT '{m['id']}' AS metric_id,
       _m AS month,
       CASE WHEN grouping(_r) = 1 THEN 'ALL' ELSE _r END AS region,
       CAST({m['expression']} AS DOUBLE) AS value,
       count(*) AS source_rows
FROM (SELECT *,
             trunc(CAST({m['date_col']} AS DATE), 'MM') AS _m,
             coalesce(CAST({region} AS STRING), 'UNKNOWN') AS _r
      FROM {source}
      WHERE {where}) t
GROUP BY GROUPING SETS ((_m, _r), (_m))"""


def all_metrics_sql(metrics: list[dict], catalog: str) -> str:
    return "\nUNION ALL\n".join(metric_sql(m, catalog) for m in metrics)


def catalog_rows(metrics: list[dict], version: str) -> list[dict]:
    """Rows for gold.metric_catalog: the business definition of every metric."""
    return [{
        "metric_id": m["id"], "metric_name": m["name"], "domain": m["domain"], "owner": m["owner"],
        "unit": m["unit"], "description": m["description"], "source_table": m["source"],
        "date_column": m["date_col"], "formula": m["expression"], "filter": m.get("filter") or "",
        "catalog_version": version,
    } for m in metrics]
