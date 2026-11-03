from datetime import date

from utility_lakehouse.transforms import conform, dedupe_latest, scd2_from_versions, scd2_versions, split_quarantine


def test_conform_handles_sap_formats_and_missing_columns(spark):
    df = spark.createDataFrame(
        [("4000000001", "20261003", "125.40-", "X", " 12.5 "), ("4000000002", "bad-date", "88.10", "", "abc")],
        ["BELNR", "BUDAT", "BETRW", "ESTIMATED_FLAG", "KWH"])
    cols = {
        "BELNR": {"name": "bill_id", "type": "string"},
        "BUDAT": {"name": "bill_date", "type": "sap_date"},
        "BETRW": {"name": "billed_amount", "type": "sap_amount"},
        "ESTIMATED_FLAG": {"name": "is_estimated", "type": "flag"},
        "KWH": {"name": "billed_kwh", "type": "double"},
        "NEW_FIELD": {"name": "new_field", "type": "int"},         # not in the file (schema drift)
    }
    rows = {r["bill_id"]: r for r in conform(df, cols).collect()}
    assert rows["4000000001"]["bill_date"] == date(2026, 10, 3)
    assert rows["4000000001"]["billed_amount"] == -125.40          # trailing minus -> negative
    assert rows["4000000001"]["is_estimated"] is True
    assert rows["4000000001"]["billed_kwh"] == 12.5
    assert rows["4000000002"]["bill_date"] is None                  # bad value -> NULL, not a crash
    assert rows["4000000002"]["is_estimated"] is False
    assert rows["4000000002"]["billed_kwh"] is None
    assert rows["4000000002"]["new_field"] is None


def test_split_quarantine_keeps_bad_rows_with_reasons(spark):
    df = spark.createDataFrame([("M1", 10.0), (None, 5.0), ("M3", -2.0), ("M4", None)], "meter_id string, kwh double")
    good, bad = split_quarantine(df, {"meter_not_null": "meter_id IS NOT NULL", "kwh_not_negative": "kwh >= 0"})
    assert [r.meter_id for r in good.collect()] == ["M1"]
    reasons = {r.meter_id: r._dq_reason for r in bad.collect()}
    assert reasons[None] == "meter_not_null"
    assert reasons["M3"] == "kwh_not_negative"
    assert reasons["M4"] == "kwh_not_negative"                      # NULL fails the rule too


def test_dedupe_latest_keeps_most_recent(spark):
    df = spark.createDataFrame([("M1", "2026-05-01", 10.0, 1), ("M1", "2026-05-01", 12.0, 2)],
                               "meter_id string, read_date string, kwh double, _ingest_ts int")
    out = dedupe_latest(df, ["meter_id", "read_date"]).collect()
    assert len(out) == 1 and out[0].kwh == 12.0


def test_scd2_keeps_history_and_skips_no_change(spark):
    df = spark.createDataFrame([
        ("100", "RES", date(2026, 4, 30)),
        ("100", "RES", date(2026, 6, 30)),        # same attributes -> not a new version
        ("100", "COM", date(2026, 8, 30)),        # real change -> new version
        ("200", "RES", date(2026, 4, 30)),
    ], "partner_id string, segment string, change_date date")
    v = scd2_versions(df, ["partner_id"], "change_date", ["segment"])
    assert v.where("partner_id = '100'").count() == 2
    scd = {(r.partner_id, r.valid_from): r for r in scd2_from_versions(df, ["partner_id"], "change_date", ["segment"]).collect()}
    old, new = scd[("100", date(2026, 4, 30))], scd[("100", date(2026, 8, 30))]
    assert old.valid_to == date(2026, 8, 30) and old.is_current is False
    assert new.valid_to is None and new.is_current is True and new.segment == "COM"
