# Utility Metrics Lakehouse

A hands-on practice project for a utility data platform on **Databricks**: source extracts from
SAP and operational systems are ingested into a **medallion architecture** (bronze, silver, gold),
**52 business metrics are defined once** in a metric catalogue, a **data-quality gate** protects
them, and **Power BI** serves automated, self-service reports.

It mirrors a common utility brief: *move from ad hoc, ticket-based reporting to a scalable
platform with self-service Power BI, built out to around 100 metrics in Databricks, in 2-week
Agile sprints.*

> All data is synthetic. SAP field names and formats (`VKONT`, `BELNR`, `yyyyMMdd` dates,
> trailing-minus amounts, `X` flags) are imitated so you can practise SAP-style mapping.

## Architecture

```
 SOURCES (files land in a Unity Catalog volume)          DATABRICKS (Unity Catalog: utility_lab)
 ───────────────────────────────────────────           ─────────────────────────────────────────────────────────
 SAP IS-U  business partner, premise,      ─┐
           contract account, meter,         │   BRONZE            SILVER                    GOLD                    POWER BI
           billing docs, payments  (CSV)    │   Auto Loader       watermark-incremental     star schema             (SQL warehouse)
 AMI head-end  daily meter reads (JSON)    ─┼─▶ raw strings  ───▶ SAP names → business ───▶ dims + facts      ───▶ certified semantic
 Contact centre  calls (CSV)                │   + _ingest_ts      types, dedupe, MERGE      metric_catalog          model, 7 pages,
 Outage mgmt (OMS)  outages (CSV)          ─┘   + _source_file    quarantine, SCD Type 2    metric_values (52)      self-service
                                                schema drift ok                                  │
                                                                         ops: load_audit, dq_results (quality gate), watermarks
 Orchestration: one Databricks job (Asset Bundle): setup → bronze → silver → gold → metrics → data quality → maintenance
```

| Layer | What happens | Key techniques |
|---|---|---|
| **Bronze** | Files loaded exactly as received | Auto Loader, `availableNow`, checkpoints, schema evolution (`voltage_v` appears later), `_metadata.file_path` |
| **Silver** | Clean, conform, de-duplicate, keep history | SAP field mapping in YAML, `try_cast`, quality rules, quarantine table, Delta `MERGE` upserts (idempotent), late-arriving corrections, **SCD Type 2** customers, watermark-based incremental |
| **Gold** | Business-ready star schema | Conformed dimensions, facts at a clear grain, AR aging snapshot, reliability inputs, liquid clustering |
| **Metrics** | 52 metrics defined once (YAML) | Owner, definition, formula, grain; `GROUPING SETS` adds an `ALL` rollup recalculated from detail (ratios are never averaged) |
| **Quality** | Gate before Power BI | Completeness, uniqueness, validity, freshness, silver↔gold reconciliation; job fails on CRITICAL |
| **Power BI** | Automated, self-service reports | Star schema, `Metric Value` measure, metric dictionary page, RLS |

### Metric domains (52 metrics)

| Domain | Examples |
|---|---|
| Billing (11) | Billed revenue, bills issued, estimated bill rate, bill reversal rate, average revenue per kWh |
| Accounts receivable (8) | AR open balance, AR over 30/60/90, share over 90, DSO, collection rate |
| Payments (6) | Payments received, online and pre-authorized share, late payment rate, days to pay |
| Meter and usage (8) | Energy consumed, average daily kWh, actual read rate, active meters |
| Customer operations (10) | Calls offered and answered, abandon rate, AHT, speed of answer, service level, FCR |
| Reliability (8) | SAIDI, SAIFI, CAIDI, outage events, restoration time, weather outages |
| Customers (1) | Move-ins |

Adding metric 53 means adding one entry to `config/metrics.yml`. No new code and no report changes.

## Folders

| Path | Contents |
|---|---|
| `notebooks/` | Databricks notebooks `00`–`08` (Python source format) |
| `src/utility_lakehouse/` | Shared code: generator, transforms, gold SQL, metric engine, quality checks |
| `config/sources.yml` | SAP → business field mapping, types, keys, quality rules |
| `config/metrics.yml` | **The metric catalogue** |
| `resources/`, `databricks.yml` | Databricks Asset Bundle: the pipeline job |
| `powerbi/` | Power BI guide and DAX measures |
| `tests/` | pytest: unit tests and a full local end-to-end run (13 tests) |
| `scripts/run_local.py` | Run everything locally with plain PySpark |
| `docs/` | **Start with `docs/STEP_BY_STEP.md`**; also the interview talk track and practice exercises |
| `sql/verify.sql` | Queries to check each layer |

## Quick start

1. Follow **`docs/STEP_BY_STEP.md`**: Databricks Free Edition or your Azure workspace, import
   the project, run `00` and `01`, then the job.
2. Build the reports with **`powerbi/POWERBI_GUIDE.md`**.
3. Practise with **`docs/PRACTICE_EXERCISES.md`** and **`docs/INTERVIEW_TALK_TRACK.md`**.

Local test run (optional, needs Java 17+):
```bash
pip install -r requirements-dev.txt
pytest -q
```
