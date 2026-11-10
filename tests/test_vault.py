"""Data Vault: hash keys, insert-only hubs/links, change-only satellites."""
from datetime import date

from pyspark.sql import functions as F

from utility_lakehouse.vault import (current_sat, hash_key, new_rows, new_sat_rows, stage_hub, stage_link,
                                     stage_sat)

TS = "2026-11-10 06:30:00"


def bills(spark, rows):
    return spark.createDataFrame(rows, "bill_id string, account_id string, billed_amount double, "
                                       "bill_status string, changed_on date, _source_file string")


def test_hash_key_ignores_case_and_spaces(spark):
    df = spark.createDataFrame([(" r1001 ",), ("R1001",)], "bill_id string")
    assert df.select(hash_key(["bill_id"]).alias("hk")).distinct().count() == 1


def test_hub_is_insert_only_and_keeps_record_source(spark):
    s = bills(spark, [("R1", "A1", 10.0, "BILLED", date(2026, 11, 1), "extract_1.csv")])
    hub = stage_hub(s, ["bill_id"], "bill_hk", TS)
    assert hub.first().record_source == "extract_1.csv"
    again = new_rows(stage_hub(s, ["bill_id"], "bill_hk", TS), hub, "bill_hk")
    assert again.count() == 0                                  # rerun adds nothing


def test_link_one_row_per_relationship(spark):
    s = bills(spark, [("R1", "A1", 10.0, "BILLED", date(2026, 11, 1), "f"),
                      ("R1", "A1", 10.0, "BILLED", date(2026, 11, 1), "f"),
                      ("R2", "A1", 5.0, "BILLED", date(2026, 11, 1), "f")])
    link = stage_link(s, {"account_hk": ["account_id"], "bill_hk": ["bill_id"]}, "account_bill_hk", TS)
    assert link.count() == 2


def test_satellite_adds_row_only_when_attributes_change(spark):
    attrs = ["account_id", "billed_amount", "bill_status"]
    day1 = bills(spark, [("R1", "A1", 10.0, "BILLED", date(2026, 11, 1), "e1")])
    sat = new_sat_rows(stage_sat(day1, "bill_hk", ["bill_id"], attrs, "changed_on", TS), None, "bill_hk")
    assert sat.count() == 1

    same = new_sat_rows(stage_sat(day1, "bill_hk", ["bill_id"], attrs, "changed_on", TS), sat, "bill_hk")
    assert same.count() == 0                                   # unchanged -> no new version

    day2 = bills(spark, [("R1", "A1", 10.5, "BILLED", date(2026, 11, 2), "e2")])
    changed = new_sat_rows(stage_sat(day2, "bill_hk", ["bill_id"], attrs, "changed_on", TS), sat, "bill_hk")
    assert changed.count() == 1                                # corrected amount -> new version

    history = sat.unionByName(changed)
    assert history.count() == 2                                # full history kept
    assert current_sat(history, "bill_hk").first().billed_amount == 10.5
