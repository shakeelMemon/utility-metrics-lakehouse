# Practice exercises

Each exercise matches something an interviewer might ask: "Have you done X?" After each one,
you can answer with a real example.

## 1. Add five new metrics (15 minutes)
Add these to `config/metrics.yml`, then rerun `05_metrics` and `06_data_quality`:

| id | name | hint |
|---|---|---|
| BIL012 | Industrial billed revenue | `fact_billing`, filter `segment = 'IND'` |
| PAY007 | Average payment amount | `fact_payment`, `AVG(payment_amount)` |
| CC011 | Move calls | `fact_contact_call`, reason `MOVE` |
| USE009 | Commercial average daily kWh | `fact_daily_usage`, filter `segment = 'COM'` |
| REL009 | Equipment outages | `fact_outage`, cause `EQUIPMENT` |

**Talking point:** "Each metric has an owner, a definition and a formula in one catalogue.
Adding one is a configuration change, reviewed in a pull request. Reports pick it up automatically."

## 2. Incremental load and late-arriving data (10 minutes)
Follow Step 6 in `STEP_BY_STEP.md`. Then explain how the duplicate was ignored and how the
late correction updated the existing row.

## 3. SCD Type 2 (10 minutes)
Upload a new file to `/Volumes/utility_lab/landing/raw/sap_bp/` named `bp_changes_20261102.csv`
with a changed segment for one customer, for example:
```
PARTNER,BU_TYPE,NAME_FIRST,NAME_LAST,NAME_ORG1,CUST_SEGMENT,REGION,SMTP_ADDR,CHDAT
1000000005,1,Avery,Smith,,COM,EAST,avery.new@example.com,20261102
```
(Check the real values for that partner first and change only one or two fields.)
Run the job, then query `silver.customer_scd2` for that partner: the old row is closed
(`valid_to = 2026-11-02`, `is_current = false`) and a new current row exists.

## 4. Break it on purpose: the quality gate (10 minutes)
Upload a billing file with a bad status (`BILL_STATUS = UNKNOWN`) and a bad date (`BUDAT = 2026AB01`).
Run the job: the rows go to `silver.quarantine` with the reasons `status_valid` and `bill_date_valid`.
Then change a rule severity in `06_data_quality` to see the job fail, and read `ops.dq_results`.

## 5. Performance: skew, broadcast, AQE (15 minutes)
Run `notebooks/08_performance_lab`. Open the **Spark UI** and compare task durations.
Explain the three fixes in your own words.

## 6. Power BI self-service (1–2 hours)
Build the seven pages in `powerbi/POWERBI_GUIDE.md`. On page 7, prove that
`Billed Revenue Difference = 0` (the star schema and the metric layer agree).

## 7. Sprint planning (thinking exercise)
Plan the next 50 metrics as 2-week sprints: group them by domain, write one user story per metric
group with acceptance criteria (definition signed off, reconciled to source, tested, documented),
and estimate them.
