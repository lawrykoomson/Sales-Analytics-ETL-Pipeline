# 🛒 Sales Analytics ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi)
![Tests](https://img.shields.io/badge/Tests-26%2F26%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade sales analytics ETL pipeline that processes **25,000 retail transactions**, engineers revenue and profitability KPIs, and loads results into a live **PostgreSQL** warehouse — with a full **dbt analytical layer**, **Power BI dashboard**, **Airflow DAG**, and **Kafka stream simulator**.

Built to mirror real retail analytics workflows used at **Hubtel Ghana**.

---

## 🏗️ System Architecture

```
[Retail Sales Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← Generates 25,000 synthetic retail sales transactions
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← Engineers revenue, profit, and discount KPIs
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (sales_dw schema)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 4 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  Power BI │  ← 4-page live dashboard connected to PostgreSQL
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time sales stream: Producer + 3 Consumer Groups
     └───────────┘
```

---

## ✅ What The Pipeline Does

### Extract
- Generates 25,000 realistic Ghana retail sales transactions
- 7 product categories across 6 Ghana regions
- 4 sales channels: In-Store, Online, Mobile App, Agent
- 4 payment methods: MoMo, Cash, Card, Bank Transfer

### Transform — Business KPI Engineering
Each transaction is enriched with:

| KPI | Formula |
|---|---|
| `gross_revenue_ghs` | unit_price × quantity |
| `discount_amount_ghs` | gross_revenue × discount_pct |
| `net_revenue_ghs` | gross_revenue − discount |
| `cost_ghs` | net_revenue × random cost ratio (45–70%) |
| `gross_profit_ghs` | net_revenue − cost |
| `profit_margin_pct` | gross_profit / net_revenue × 100 |
| `is_high_value` | net_revenue >= GHS 500 |
| `revenue_tier` | Micro / Small / Medium / Large / Premium |

### Load
- Batch upserts into PostgreSQL (sales_dw schema)
- Auto-falls back to CSV if database unavailable

---

## 🔁 dbt Analytical Layer

5 models built on top of PostgreSQL:

| Model | Type | Description |
|---|---|---|
| stg_sales | View | Cleaned transactions + time-of-day segments |
| mart_sales_by_category | Table | Revenue and profit by product category |
| mart_sales_by_region | Table | Revenue and profit by Ghana region |
| mart_sales_by_channel | Table | Revenue and MoMo payments by channel |
| mart_monthly_revenue | Table | 12-month revenue and profitability trend |

```bash
cd dbt
dbt run --profiles-dir .    # Run all 5 models
dbt test --profiles-dir .   # Run 4 data quality tests
```

---

## 📊 Power BI Dashboard — 4 Pages

Connected live to PostgreSQL via dbt mart tables:

| Page | Key Metrics |
|---|---|
| Executive Summary | 25K transactions, GHS 9.42M revenue, GHS 4.01M profit, 42.53% margin |
| Monthly Trends | 12-month revenue trend, 5,135 high value, 6,174 discounted |
| Regional & Channel Analysis | Greater Accra 35.12%, In-Store leads channel revenue |
| Product & Profitability | Groceries leads profit, Electronics leads margin |

---

## 🌊 Kafka Stream Simulator

Real-time sales transaction streaming:

```bash
python kafka_sales_simulator.py
```

```
Topic          : sales.transactions.live
Partitions     : 3
Producer Rate  : 10 sales/sec
Duration       : 60 seconds

Producer        → generates live sales transaction events
RevenueConsumer → tracks high-value transactions (partition 0)
MetricsConsumer → aggregates real-time sales KPIs (partition 1)
AuditConsumer   → logs all events to JSONL file (partition 2)

Final Results:
  Total Sales Produced   : 596
  High Value Sales       : 37
  Discounted Sales       : 56
  Total Revenue Streamed : GHS 66,333.25
  Total Profit Streamed  : GHS 27,710.79
  Top Category           : Electronics
  Top Channel            : In-Store
  Top Region             : Greater Accra
```

---

## 🧪 Unit Tests — 26/26 Passing

```bash
pytest test_sales_pipeline.py -v
# 26 passed in 24.21s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestExtract | 10 | Row count, columns, valid values, uniqueness |
| TestTransform | 11 | Revenue, profit, flags, time features |
| TestIntegration | 5 | End-to-end, revenue totals, top category |

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/sales_pipeline_dag.py`:
- Runs **every day at 03:00 AM UTC**
- 5 tasks: extract, transform, load, dbt refresh, notify
- XCom passes revenue and profit metrics between tasks
- Email alerts on failure with 2 retries

---

## 📊 Sample Pipeline Output

```
====================================================================
   SALES ANALYTICS ETL PIPELINE — RUN SUMMARY
====================================================================
  Total Transactions      : 25,000
  Total Gross Revenue     : GHS 9,689,208.90
  Total Net Revenue       : GHS 9,418,866.60
  Total Gross Profit      : GHS 4,012,302.80
  Avg Profit Margin       : 42.6%
  High Value Transactions : 5,135
  Discounted Transactions : 6,174
--------------------------------------------------------------------
  REVENUE BY CATEGORY:
    Groceries            : GHS 2,420,465.47
    Electronics          : GHS 1,810,851.31
    Clothing             : GHS 1,377,364.66
    Health & Beauty      : GHS 1,143,208.26
    Home & Garden        : GHS 1,140,904.00
    Sports               : GHS   930,156.05
    Automotive           : GHS   595,916.85
--------------------------------------------------------------------
  REVENUE BY REGION:
    Greater Accra        : GHS 3,307,942.96
    Ashanti              : GHS 2,279,243.56
    Western              : GHS 1,423,608.06
    Eastern              : GHS 1,111,636.75
    Northern             : GHS   804,488.67
    Volta                : GHS   491,946.60
====================================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Sales-Analytics-ETL-Pipeline.git
cd Sales-Analytics-ETL-Pipeline

# 2. Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE sales_warehouse;"

# 5. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Run the pipeline
python etl_pipeline.py

# 7. Run unit tests
pytest test_sales_pipeline.py -v

# 8. Run dbt models
cd dbt
set DBT_POSTGRES_PASSWORD=your_password
dbt run --profiles-dir .
dbt test --profiles-dir .

# 9. Run Kafka stream simulator
cd ..
python kafka_sales_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| Pandas | Data extraction and transformation |
| NumPy | Numerical operations and KPI calculation |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| Power BI | Sales analytics dashboard |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] ETL pipeline with PostgreSQL live load
- [x] 26 unit tests — all passing
- [x] dbt analytical layer — 5 models, 4 tests passing
- [x] Apache Airflow DAG — daily scheduled runs
- [x] Power BI dashboard — 4 pages live
- [x] Kafka stream simulator — 3 consumer groups
- [ ] Docker containerisation

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
