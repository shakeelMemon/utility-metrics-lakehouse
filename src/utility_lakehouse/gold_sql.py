"""Gold layer: star schema (dimensions + facts) for Power BI.

Each entry is a plain SELECT using {cat} as the catalog name, plus the columns to
cluster by. The Databricks notebook wraps them in
    CREATE OR REPLACE TABLE {cat}.gold.<name> CLUSTER BY (...) AS <select>
and the local tests run the same SELECT against local tables, so the logic is tested.

Order matters: later tables read earlier gold tables.
"""

GOLD_TABLES: list[tuple[str, list[str], str]] = [

    # ---------------------------------------------------------------- dimensions
    ("dim_date", ["date"], """
SELECT d                                   AS date,
       year(d)                             AS year,
       quarter(d)                          AS quarter,
       month(d)                            AS month_num,
       date_format(d, 'MMM yyyy')          AS month_name,
       trunc(d, 'MM')                      AS month_start,
       last_day(d)                         AS month_end,
       dayofweek(d) IN (1, 7)              AS is_weekend
FROM (SELECT explode(sequence(min_d, max_d, interval 1 day)) AS d
      FROM (SELECT min(read_date) AS min_d, max(read_date) AS max_d FROM {cat}.silver.meter_read_daily))
"""),

    ("dim_customer", ["partner_id"], """
SELECT xxhash64(partner_id, valid_from)                         AS customer_sk,
       partner_id,
       CASE WHEN customer_type = '2' THEN 'ORGANIZATION' ELSE 'PERSON' END AS customer_type,
       coalesce(org_name, concat_ws(' ', first_name, last_name))  AS customer_name,
       segment,
       region,
       valid_from,
       valid_to,
       is_current
FROM {cat}.silver.customer_scd2
"""),

    ("dim_premise", ["premise_id"], """
SELECT p.premise_id, p.street, p.city, p.region, p.premise_type,
       m.meter_id, m.meter_type, m.install_date
FROM {cat}.silver.premise p
LEFT JOIN {cat}.silver.meter m ON m.premise_id = p.premise_id
"""),

    ("dim_region", ["region"], """
SELECT region, count(*) AS premises
FROM {cat}.gold.dim_premise
GROUP BY region
"""),

    ("dim_account", ["account_id"], """
SELECT a.account_id, a.partner_id, a.premise_id, a.rate_category,
       a.move_in_date, a.move_out_date,
       p.region, c.segment, c.customer_name
FROM {cat}.silver.contract_account a
LEFT JOIN {cat}.gold.dim_premise  p ON p.premise_id = a.premise_id
LEFT JOIN {cat}.gold.dim_customer c ON c.partner_id = a.partner_id AND c.is_current
"""),

    # ---------------------------------------------------------------- facts
    ("fact_daily_usage", ["read_date", "region"], """
SELECT r.read_date, r.meter_id, r.premise_id, a.account_id, a.region, a.segment,
       r.kwh, r.read_quality, r.read_quality = 'ESTIMATED' AS is_estimated, r.voltage_v
FROM {cat}.silver.meter_read_daily r
LEFT JOIN {cat}.gold.dim_account a ON a.premise_id = r.premise_id
"""),

    ("fact_billing", ["bill_date", "region"], """
SELECT b.bill_id, b.account_id, a.partner_id, a.region, a.segment, a.rate_category,
       b.bill_date, trunc(b.bill_date, 'MM') AS bill_month,
       b.period_start, b.period_end, b.billed_kwh, b.billed_amount, b.due_date,
       b.bill_status, coalesce(b.is_estimated, false) AS is_estimated
FROM {cat}.silver.billing_document b
LEFT JOIN {cat}.gold.dim_account a ON a.account_id = b.account_id
"""),

    ("fact_payment", ["payment_date", "region"], """
SELECT p.payment_id, p.account_id, p.bill_id, a.region, a.segment,
       p.payment_date, p.payment_amount, p.channel,
       datediff(p.payment_date, b.bill_date)  AS days_to_pay,
       p.payment_date > b.due_date            AS is_late
FROM {cat}.silver.payment p
LEFT JOIN {cat}.gold.dim_account a  ON a.account_id = p.account_id
LEFT JOIN {cat}.silver.billing_document b ON b.bill_id = p.bill_id
"""),

    # AR aging snapshot at every month end, aggregated by region and segment.
    ("fact_ar_region_month", ["month_end", "region"], """
WITH month_ends AS (
  SELECT DISTINCT month_end FROM {cat}.gold.dim_date
  WHERE month_end <= (SELECT max(date) FROM {cat}.gold.dim_date)
),
open_items AS (
  SELECT m.month_end, b.bill_id, b.region, b.segment, b.due_date,
         b.billed_amount - coalesce(sum(CASE WHEN p.payment_date <= m.month_end THEN p.payment_amount END), 0) AS open_amount
  FROM month_ends m
  JOIN {cat}.gold.fact_billing b ON b.bill_date <= m.month_end AND b.bill_status = 'BILLED'
  LEFT JOIN {cat}.gold.fact_payment p ON p.bill_id = b.bill_id
  GROUP BY m.month_end, b.bill_id, b.region, b.segment, b.due_date, b.billed_amount
),
aging AS (
  SELECT month_end, region, segment,
         sum(open_amount)                                                       AS ar_open_balance,
         sum(CASE WHEN datediff(month_end, due_date) <= 0 THEN open_amount END) AS ar_current,
         sum(CASE WHEN datediff(month_end, due_date) > 30 THEN open_amount END) AS ar_over_30,
         sum(CASE WHEN datediff(month_end, due_date) > 60 THEN open_amount END) AS ar_over_60,
         sum(CASE WHEN datediff(month_end, due_date) > 90 THEN open_amount END) AS ar_over_90,
         count(*)                                                               AS open_bills
  FROM open_items WHERE open_amount > 0.005
  GROUP BY month_end, region, segment
),
billed AS (
  SELECT last_day(bill_date) AS month_end, region, segment, sum(billed_amount) AS billed_in_month
  FROM {cat}.gold.fact_billing WHERE bill_status = 'BILLED'
  GROUP BY 1, 2, 3
),
paid AS (
  SELECT last_day(payment_date) AS month_end, region, segment, sum(payment_amount) AS paid_in_month
  FROM {cat}.gold.fact_payment GROUP BY 1, 2, 3
)
SELECT a.month_end, a.region, a.segment, a.ar_open_balance,
       coalesce(a.ar_current, 0) AS ar_current, coalesce(a.ar_over_30, 0) AS ar_over_30,
       coalesce(a.ar_over_60, 0) AS ar_over_60, coalesce(a.ar_over_90, 0) AS ar_over_90,
       a.open_bills,
       coalesce(b.billed_in_month, 0) AS billed_in_month,
       coalesce(p.paid_in_month, 0)   AS paid_in_month
FROM aging a
LEFT JOIN billed b USING (month_end, region, segment)
LEFT JOIN paid   p USING (month_end, region, segment)
"""),

    ("fact_contact_call", ["call_date", "region"], """
SELECT c.call_id, c.partner_id, d.region, d.segment,
       CAST(c.call_start_ts AS DATE) AS call_date, c.call_start_ts,
       c.queue_wait_sec, c.handle_sec, c.call_reason,
       coalesce(c.is_abandoned, false) AS is_abandoned,
       CASE WHEN coalesce(c.is_abandoned, false) THEN NULL ELSE c.is_first_call_resolved END AS is_first_call_resolved,
       NOT coalesce(c.is_abandoned, false) AND c.queue_wait_sec <= 30 AS answered_within_30s
FROM {cat}.silver.contact_call c
LEFT JOIN {cat}.gold.dim_customer d ON d.partner_id = c.partner_id AND d.is_current
"""),

    ("fact_outage", ["outage_date", "region"], """
SELECT outage_id, region, CAST(outage_start_ts AS DATE) AS outage_date,
       outage_start_ts, restore_ts,
       (unix_timestamp(restore_ts) - unix_timestamp(outage_start_ts)) / 60.0D AS duration_min,
       customers_affected,
       customers_affected * (unix_timestamp(restore_ts) - unix_timestamp(outage_start_ts)) / 60.0D AS customer_minutes_interrupted,
       cause, coalesce(is_planned, false) AS is_planned
FROM {cat}.silver.outage
"""),

    # Region x month grid so months with no outages still count customers served.
    ("fact_reliability_region_month", ["month_start", "region"], """
WITH grid AS (
  SELECT DISTINCT d.month_start, p.region, p.customers_served
  FROM {cat}.gold.dim_date d
  CROSS JOIN (SELECT region, count(*) AS customers_served FROM {cat}.gold.dim_premise GROUP BY region) p
),
o AS (
  SELECT trunc(outage_date, 'MM') AS month_start, region,
         count(*)                                                    AS outages,
         sum(CASE WHEN is_planned THEN 1 ELSE 0 END)                 AS planned_outages,
         sum(CASE WHEN NOT is_planned THEN customers_affected END)   AS customer_interruptions,
         sum(CASE WHEN NOT is_planned THEN customer_minutes_interrupted END) AS customer_minutes_interrupted,
         sum(duration_min)                                           AS total_outage_minutes
  FROM {cat}.gold.fact_outage GROUP BY 1, 2
)
SELECT g.month_start, g.region, g.customers_served,
       coalesce(o.outages, 0) AS outages,
       coalesce(o.planned_outages, 0) AS planned_outages,
       coalesce(o.customer_interruptions, 0) AS customer_interruptions,
       coalesce(o.customer_minutes_interrupted, 0) AS customer_minutes_interrupted,
       coalesce(o.total_outage_minutes, 0) AS total_outage_minutes
FROM grid g LEFT JOIN o USING (month_start, region)
"""),
]


def render(sql: str, catalog: str) -> str:
    return sql.replace("{cat}", catalog)
