# Power BI guide: build the self-service reports

You'll connect Power BI Desktop to the Databricks **gold** schema, build a star-schema
model, add the measures from `measures.dax`, and create seven report pages.

Time: about 1.5–2 hours the first time.

---

## 1. Get the connection details from Databricks

1. In Databricks, open **SQL Warehouses** and click your warehouse (in Free Edition it's
   called *Serverless Starter Warehouse*). Start it if it's stopped.
2. Open the **Connection details** tab and copy:
   - **Server hostname** (like `dbc-xxxx.cloud.databricks.com`)
   - **HTTP path** (like `/sql/1.0/warehouses/xxxx`)
3. Create a token: your profile (top right) → **Settings → Developer → Access tokens →
   Generate new token**. Copy it somewhere safe; you'll only see it once.

Shortcut: **Marketplace / Partner Connect → Power BI** downloads a `.pbids` file that opens
Power BI already connected.

## 2. Connect from Power BI Desktop

1. **Get data → More… → Azure Databricks** (on Azure) or **Databricks** (other clouds,
   Free Edition).
2. Paste the **Server hostname** and **HTTP path**, choose **Import**, and click OK.
3. Sign in with **Personal Access Token** and paste your token.
4. In the navigator, open your catalog (`utility_lab`) → **gold**, and select:

| Table | Why |
|---|---|
| `dim_date` | calendar for every fact |
| `dim_region` | one slicer that filters every fact |
| `dim_account` | rate category and account attributes |
| `dim_customer` | filter it to current rows (step 3) |
| `fact_billing`, `fact_payment`, `fact_daily_usage` | billing, payments, usage detail |
| `fact_ar_region_month` | AR aging snapshot |
| `fact_contact_call` | call-centre detail |
| `fact_outage`, `fact_reliability_region_month` | outages and SAIDI/SAIFI inputs |
| `v_metric_values`, `metric_catalog` | **the governed metric layer** |

Optional: `ops.dq_results` and `ops.load_audit` for a data-quality page.

5. Click **Transform data** and, in Power Query, filter `dim_customer` to `is_current = TRUE`,
   then **Close & Apply**.

## 3. Relationships (Model view)

Create these, all **one-to-many, single direction**, from the dimension (1) to the fact (*):

| From (1) | To (*) |
|---|---|
| `dim_date[date]` | `fact_billing[bill_date]`, `fact_payment[payment_date]`, `fact_daily_usage[read_date]`, `fact_contact_call[call_date]`, `fact_outage[outage_date]`, `fact_ar_region_month[month_end]`, `fact_reliability_region_month[month_start]`, `v_metric_values[month]` |
| `dim_region[region]` | `fact_billing`, `fact_payment`, `fact_daily_usage`, `fact_contact_call`, `fact_outage`, `fact_ar_region_month`, `fact_reliability_region_month` (each on `[region]`) |
| `dim_account[account_id]` | `fact_billing[account_id]`, `fact_payment[account_id]`, `fact_daily_usage[account_id]` |
| `metric_catalog[metric_id]` | `v_metric_values[metric_id]` |

Don't relate `dim_region` to `v_metric_values`. The metric layer has its own `region`
column, including an `ALL` row, so use `v_metric_values[region]` as the slicer on metric pages.

Mark `dim_date` as a date table: **Table tools → Mark as date table → date**.

## 4. Measures

Create an empty table called `_Measures` (**Home → Enter data**, then OK), and add each
measure from `measures.dax` (**Modeling → New measure**).

The important one is **`Metric Value`**. It reads the governed number from the metric layer
and **never adds regions together**, so ratios such as SAIDI, abandon rate and collection rate
stay correct at every level.

## 5. Report pages

| Page | Visuals |
|---|---|
| **1. Executive overview** | KPI cards: Billed Revenue, AR over 90 Share, SAIDI, Abandon Rate (latest month measures). Line chart: `Metric Value` by `month` for `BIL001`. Slicer: `v_metric_values[region]` |
| **2. Billing and revenue** | Column chart: `Billed Revenue` by `dim_date[month_name]`, legend `fact_billing[segment]`. Cards: `Bills Issued`, `Estimated Bill Rate`. Table by `dim_account[rate_category]` |
| **3. Accounts receivable** | Stacked column by `month_end`: `AR Current`, `AR 1-30 Days`, `AR 31-60 Days`, `AR 61-90 Days`, `AR Over 90`. Line: DSO (`Metric Value`, `AR006`). Slicer: `dim_region` |
| **4. Customer operations** | Cards: `Calls Offered`, `Abandon Rate`, `Average Handle Time (sec)`. Bar: calls by `call_reason`. Line: `CC006` service level by month |
| **5. Reliability** | Line: `SAIDI (minutes)` and `SAIFI` by month. Matrix: region × month for SAIDI. Bar: `fact_outage` count by `cause` |
| **6. Metric dictionary and explorer** | Table from `metric_catalog`: name, domain, owner, unit, description, formula. Slicers: `metric_catalog[domain]`, `metric_catalog[metric_name]`. Matrix: rows `v_metric_values[region]`, columns `month`, values `Metric Value (formatted)` |
| **7. Reconciliation and data quality** | Card: `Billed Revenue Difference` (should be 0). Table: latest `ops.dq_results`. Table: `ops.load_audit` |

Page 6 is what makes it **self-service**: a business user picks any metric, sees its
definition and owner, and gets the same number everywhere.

## 6. Row-level security (optional)

**Modeling → Manage roles → New role** `North region`:
- on `dim_region`: `[region] = "NORTH"`
- on `v_metric_values`: `[region] IN { "NORTH" }`

Test it with **View as → North region**. In production you'd map roles to Entra ID groups in
the Power BI service, or apply row filters in Unity Catalog so every tool gets the same security.

## 7. Refresh

Publish to the Power BI service, add the Databricks credentials to the dataset, and schedule a
refresh **after** the Databricks job (the job runs at 06:30, so refresh at 07:30). For large
facts, use **incremental refresh** on the date column.
