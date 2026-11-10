"""Data Vault 2.0 integration layer: hubs, links and satellites built from silver.

Why a vault here: it is the most auditable way to keep history. Every row carries
  - a hash key (SHA-256 of the business key)  -> joins without knowing the source system
  - load_dts and record_source                -> when it arrived and exactly which file it came from
  - (satellites) a hash_diff of the attributes -> a new row only when something really changed
Everything is insert-only: nothing is updated or deleted, so any past state can be rebuilt.

Model (kept small on purpose: the core billing entities)
  hub_customer   (partner_id)    hub_account (account_id)   hub_premise (premise_id)   hub_bill (bill_id)
  link_customer_account   link_account_premise   link_account_bill
  sat_customer   (name, segment, region, email ... history)   sat_bill (amount, status, dates ... history)

All functions are pure DataFrame logic, shared by notebook 03b and the local tests.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# hub name -> (silver table, business key columns)
HUBS = {
    "hub_customer": ("customer_scd2", ["partner_id"]),
    "hub_account": ("contract_account", ["account_id"]),
    "hub_premise": ("premise", ["premise_id"]),
    "hub_bill": ("billing_document", ["bill_id"]),
}

# link name -> (silver table, {hash key column: business key columns of that hub})
LINKS = {
    "link_customer_account": ("contract_account", {"customer_hk": ["partner_id"], "account_hk": ["account_id"]}),
    "link_account_premise": ("contract_account", {"account_hk": ["account_id"], "premise_hk": ["premise_id"]}),
    "link_account_bill": ("billing_document", {"account_hk": ["account_id"], "bill_hk": ["bill_id"]}),
}

# satellite name -> (silver table, parent hash key, business key columns, tracked attributes, order column)
SATS = {
    "sat_customer": ("customer_scd2", "customer_hk", ["partner_id"],
                     ["customer_type", "first_name", "last_name", "org_name", "segment", "region", "email"],
                     "valid_from"),
    "sat_bill": ("billing_document", "bill_hk", ["bill_id"],
                 ["account_id", "bill_date", "period_start", "period_end", "billed_kwh", "billed_amount",
                  "due_date", "bill_status", "is_estimated"],
                 "changed_on"),
}


def hash_key(cols: list[str]) -> F.Column:
    """SHA-256 of the normalized business key (trimmed, upper case, '||' between parts)."""
    parts = [F.upper(F.trim(F.coalesce(F.col(c).cast("string"), F.lit("")))) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def hash_diff(cols: list[str]) -> F.Column:
    """SHA-256 of all tracked attributes; '~' marks NULL so NULL and '' differ."""
    parts = [F.coalesce(F.trim(F.col(c).cast("string")), F.lit("~")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def _audit_cols(df: DataFrame, load_ts) -> list[F.Column]:
    source = F.col("_source_file") if "_source_file" in df.columns else F.lit("unknown")
    return [F.lit(load_ts).cast("timestamp").alias("load_dts"), source.alias("record_source")]


def stage_hub(silver: DataFrame, keys: list[str], hk: str, load_ts) -> DataFrame:
    """One row per business key: the first record source that delivered it."""
    order = [F.col("_ingest_ts").asc_nulls_last()] if "_ingest_ts" in silver.columns else [F.lit(1)]
    w = Window.partitionBy(*keys).orderBy(*order)
    first = silver.where(F.concat_ws("", *[F.col(k) for k in keys]) != "") \
                  .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
    return first.select(hash_key(keys).alias(hk), *keys, *_audit_cols(first, load_ts))


def stage_link(silver: DataFrame, hubs: dict[str, list[str]], link_hk: str, load_ts) -> DataFrame:
    """One row per distinct relationship; the link hash key covers all business keys."""
    all_keys = [k for ks in hubs.values() for k in ks]
    rows = silver.where(F.greatest(*[F.col(k).isNull().cast("int") for k in all_keys]) == 0)
    rows = rows.withColumn("_rn", F.row_number().over(Window.partitionBy(*all_keys).orderBy(F.lit(1)))).where("_rn = 1")
    return rows.select(hash_key(all_keys).alias(link_hk),
                       *[hash_key(ks).alias(hk) for hk, ks in hubs.items()],
                       *_audit_cols(rows, load_ts))


def stage_sat(silver: DataFrame, hk: str, keys: list[str], attrs: list[str], order_col: str, load_ts) -> DataFrame:
    """Every version of the attributes, in change order, with its hash_diff."""
    order_c = F.col(order_col) if order_col in silver.columns else F.lit(None)
    return silver.select(hash_key(keys).alias(hk), hash_diff(attrs).alias("hash_diff"),
                         *[F.col(a) for a in attrs], order_c.alias("effective_from"),
                         *_audit_cols(silver, load_ts))


def new_rows(staged: DataFrame, existing: DataFrame | None, key: str) -> DataFrame:
    """Hubs and links are insert-only: keep only keys not already in the target."""
    return staged if existing is None else staged.join(existing.select(key), key, "left_anti")


def new_sat_rows(staged: DataFrame, existing: DataFrame | None, hk: str) -> DataFrame:
    """Insert a satellite row only when the hash_diff differs from the previous version.

    The latest stored row per hash key is put in front of the staged versions; a version
    is kept when its hash_diff differs from the one before it. Reruns therefore add nothing.
    """
    staged = staged.dropDuplicates([hk, "hash_diff", "effective_from"])
    if existing is not None:
        w_last = Window.partitionBy(hk).orderBy(F.col("effective_from").desc_nulls_last(), F.col("load_dts").desc())
        last = (existing.withColumn("_rn", F.row_number().over(w_last)).where("_rn = 1")
                        .select(hk, F.col("hash_diff").alias("_last_diff"), F.col("effective_from").alias("_last_from")))
        staged = staged.join(last, hk, "left")
        # versions older than what is stored were already loaded
        staged = staged.where(F.col("_last_from").isNull() | F.col("effective_from").isNull()
                              | (F.col("effective_from") >= F.col("_last_from")))
    else:
        staged = staged.withColumn("_last_diff", F.lit(None).cast("string"))
    w = Window.partitionBy(hk).orderBy(F.col("effective_from").asc_nulls_first())
    prev = F.coalesce(F.lag("hash_diff").over(w), F.col("_last_diff"))
    out = staged.withColumn("_prev", prev).where(F.col("_prev").isNull() | (F.col("_prev") != F.col("hash_diff")))
    return out.drop("_prev", "_last_diff", *[c for c in ("_last_from",) if c in out.columns])


def current_sat(sat: DataFrame, hk: str) -> DataFrame:
    """The latest version per hash key (what silver should equal)."""
    w = Window.partitionBy(hk).orderBy(F.col("effective_from").desc_nulls_last(), F.col("load_dts").desc())
    return sat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
