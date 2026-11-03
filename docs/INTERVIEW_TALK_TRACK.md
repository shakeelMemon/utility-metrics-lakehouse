# Interview talk track: utility metrics project

Use your **real work first** (Northleaf: medallion on Databricks, metrics defined once,
20 → 3–4 minute proof of concept). Use this project to show you've also practised the
utility specifics. Say it's a personal practice project.

## 60-second pitch
"I also built a practice project that matches this role closely. Source files land from an
SAP-style billing system, smart meters, the contact centre and the outage system. Auto Loader
loads them into bronze exactly as received. In silver, I map SAP technical fields like VKONT
and BELNR to business names, handle SAP date and amount formats, quarantine bad rows with a
reason, and MERGE so reruns and late data are safe. Customers have SCD Type 2 history. Gold is
a star schema, and on top I have 52 metrics, such as billed revenue, AR over 90 days, abandon
rate, SAIDI and SAIFI, each defined once in a catalogue with an owner and formula. A
data-quality gate runs before Power BI, and the reports read the governed metric layer, so
every page shows the same number. Adding a metric is a config change, which is how you'd
scale to 100."

## Job description → project

| Job description | Where it is in the project | What to say |
|---|---|---|
| Pipelines in Databricks with PySpark and SQL | `03_silver` (PySpark), `gold_sql.py` (SQL) | "PySpark for cleaning and MERGE logic, SQL for the star schema and metrics." |
| Bronze, silver, gold | notebooks 02–04 | "Bronze raw, silver trusted, gold business-ready." |
| Business requirements → models and metrics | `metrics.yml`, `fact_*` tables | "Each metric has an owner, a definition, a formula and a grain." |
| Self-service Power BI | `v_metric_values`, metric dictionary page | "Users pick any metric and see its definition; one certified model." |
| SAP and Azure sources | `sources.yml` mapping | "SAP names and formats are handled in silver, not in reports." |
| Data quality, performance, reliability | `06_data_quality`, `07_maintenance`, `08_performance_lab` | "A quality gate, idempotent MERGE, liquid clustering, OPTIMIZE, skew fixes." |
| Agile | `PRACTICE_EXERCISES.md` #7 | "Metrics are delivered in domain groups per sprint, with a definition of done." |

## Questions they may ask about it

**Why keep bronze as raw strings?** "So nothing is lost or misread on arrival, and I can
always reprocess. Typing and rules happen in silver, where failures are quarantined, not dropped."

**How do you handle a file that arrives twice or late?** "Auto Loader's checkpoint loads each file
once. In silver, MERGE on the business key keeps only the latest version, so duplicates don't double
count and a late correction updates the existing row."

**How do you make sure 100 metrics stay consistent?** "Define once in gold, never in reports. The
`ALL` total is recalculated from detail using GROUPING SETS, so a ratio like SAIDI is never an
average of regional averages. A reconciliation check compares the star schema to the metric layer."

**Explain SAIDI and SAIFI.** "SAIDI is the average outage minutes per customer served; SAIFI is the
average number of interruptions per customer. CAIDI is SAIDI divided by SAIFI, the average restoration
time. I exclude planned outages, which is a common definition, but the business owner signs that off."

**What's the AR aging snapshot?** "For every month end, open balance per bill equals the billed
amount minus payments up to that date, bucketed by days past due: current, over 30, 60 and 90.
DSO and collection rate are built on it."

**What would you do differently in production?** "Use SAP's supported extraction, such as the ADF
SAP CDC connector, instead of files; Lakeflow declarative pipelines with expectations for quality;
row filters in Unity Catalog; incremental gold with Change Data Feed; and CI/CD through the bundle
with a test environment."
