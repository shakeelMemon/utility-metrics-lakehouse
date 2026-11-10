"""Incremental SAP extract using SAP's changed-on date (AEDAT)."""
from datetime import date

import pytest

from utility_lakehouse.sap_extract import openquery_sql, select_changes, window_start


def test_window_start_uses_one_day_overlap():
    assert window_start(None) is None                       # first run: full load
    assert window_start(date(2026, 11, 2)) == "20261101"
    assert window_start(date(2026, 11, 2), overlap_days=0) == "20261102"
    with pytest.raises(ValueError):
        window_start(date(2026, 11, 2), overlap_days=-1)


def test_openquery_sql_filters_on_changed_on_date():
    sql = openquery_sql("SAP_LINK", "BILLING_DOC", ["BELNR", "AEDAT"], "AEDAT", "20261101")
    assert sql == "SELECT * FROM OPENQUERY(SAP_LINK, 'SELECT BELNR, AEDAT FROM BILLING_DOC WHERE AEDAT >= ''20261101''')"
    assert "WHERE" not in openquery_sql("SAP_LINK", "BILLING_DOC", ["BELNR"], "AEDAT", None)
    with pytest.raises(ValueError):
        openquery_sql("SAP_LINK", "BILLING_DOC", ["BELNR"], "AEDAT", "2026-11-01'; DROP")


def test_select_changes_keeps_rows_on_or_after_since(spark):
    df = spark.createDataFrame([("1", "20261030"), ("2", "20261031"), ("3", "20261101")], "BELNR string, AEDAT string")
    assert [r.BELNR for r in select_changes(df, "AEDAT", "20261031").orderBy("BELNR").collect()] == ["2", "3"]
    assert select_changes(df, "AEDAT", None).count() == 3
