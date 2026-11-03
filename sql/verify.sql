-- Verification queries: run in the Databricks SQL editor (catalog utility_lab)

-- 1. Bronze: raw strings exactly as received, plus lineage columns
SELECT BELNR, VKONT, BUDAT, BETRW, BILL_STATUS, _source_file, _ingest_ts
FROM utility_lab.bronze.sap_billing_doc WHERE BILL_STATUS = 'REVERSAL' LIMIT 5;

-- 2. Silver: SAP names mapped, types cast, trailing-minus amount now negative
SELECT bill_id, account_id, bill_date, billed_amount, bill_status
FROM utility_lab.silver.billing_document WHERE bill_status = 'REVERSAL' LIMIT 5;

-- 3. Quarantine: rejected rows and why
SELECT target_table, dq_reason, count(*) AS rows
FROM utility_lab.silver.quarantine GROUP BY ALL ORDER BY rows DESC;

-- 4. SCD Type 2: customers with history
SELECT partner_id, segment, last_name, valid_from, valid_to, is_current
FROM utility_lab.silver.customer_scd2
WHERE partner_id IN (SELECT partner_id FROM utility_lab.silver.customer_scd2 GROUP BY partner_id HAVING count(*) > 1)
ORDER BY partner_id, valid_from LIMIT 20;

-- 5. Schema drift: voltage only in later reads
SELECT trunc(read_date, 'MM') AS month, count(*) AS reads, count(voltage_v) AS with_voltage
FROM utility_lab.silver.meter_read_daily GROUP BY ALL ORDER BY month;

-- 6. Gold: AR aging by month
SELECT month_end, round(sum(ar_open_balance)) AS open_ar, round(sum(ar_over_90)) AS over_90
FROM utility_lab.gold.fact_ar_region_month GROUP BY month_end ORDER BY month_end;

-- 7. Metric layer: SAIDI by region, and the ALL row recalculated (not averaged)
SELECT month, region, round(value, 2) AS saidi_minutes
FROM utility_lab.gold.metric_values WHERE metric_id = 'REL002' ORDER BY month, region;

-- 8. Metric dictionary
SELECT metric_id, metric_name, domain, owner, unit, formula FROM utility_lab.gold.metric_catalog ORDER BY metric_id;

-- 9. Ops: latest quality results and audit
SELECT * FROM utility_lab.ops.dq_results
WHERE run_ts = (SELECT max(run_ts) FROM utility_lab.ops.dq_results) ORDER BY status, severity;
SELECT step, table_name, rows_in, rows_out, rows_quarantined, status, finished_at
FROM utility_lab.ops.load_audit ORDER BY finished_at DESC LIMIT 30;

-- 10. Time travel: the silver table before the last MERGE
DESCRIBE HISTORY utility_lab.silver.meter_read_daily;
-- SELECT count(*) FROM utility_lab.silver.meter_read_daily VERSION AS OF 1;
