# 📊 Automated ETL Pipeline — Real-Time Sales Analytics

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![SQLite](https://img.shields.io/badge/SQLite-Source_DB-003B57?style=flat-square&logo=sqlite)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Warehouse-336791?style=flat-square&logo=postgresql)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A complete 3-layer sales analytics system — data generation, ETL pipeline, and business intelligence reporting — processing **138,057 records** across **50,000 orders** from a realistic Ghana retail database.

This is my **Final Year Project** at the University of Cape Coast, built to mirror production data engineering systems used by retail and fintech companies in Ghana.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────┐
│  LAYER 1 — DATA SOURCE                              │
│  SQLite Database (sales_database.db)                │
│  • 500 customers  • 38 products  • 20 salespersons  │
│  • 50,000 orders  • 87,499 order line items         │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  LAYER 2 — ETL PIPELINE (etl_pipeline.py)           │
│  Extract  → 5 tables from SQLite                    │
│  Transform → join, clean, engineer KPIs, enrich     │
│  Load     → PostgreSQL warehouse or CSV fallback    │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  LAYER 3 — ANALYTICS REPORTS (analytics_report.py) │
│  • Executive Summary    • Monthly Revenue           │
│  • Top 10 Products      • Top Salespersons          │
│  • Regional Revenue     • Payment Methods           │
│  • Returns Analysis                                 │
└─────────────────────────────────────────────────────┘
```

---

## ✅ What The System Does

### Layer 1 — Data Generator (`sales_data_generator.py`)
- Creates a realistic Ghana retail SQLite database with 5 related tables
- 500 Ghanaian customers across 7 regions and 20+ cities
- 38 products across 6 categories (Electronics, Food, Clothing, Home, Health, Stationery)
- 20 named Ghanaian salespersons with regional assignments
- 50,000 orders with payment methods, statuses, and delivery regions
- 87,499 order line items with quantities, unit prices, and discounts

### Layer 2 — ETL Pipeline (`etl_pipeline.py`)
- Extracts all 5 tables from SQLite source
- Joins orders → items → customers → products → salespersons
- Engineers financial KPIs: gross revenue, discounts, net revenue, COGS, gross profit, margin %
- Builds time dimensions: year, month, quarter, day of week, hour, weekend flag
- Flags high-value orders (top 10%), returns, and order size tiers
- Loads into PostgreSQL warehouse or exports 3 CSV files as fallback

### Layer 3 — Analytics Reports (`analytics_report.py`)
Generates 6 business intelligence reports:
- Executive summary with all key metrics
- Monthly revenue breakdown with margins
- Top 10 products by net revenue and margin
- Top salesperson performance rankings
- Revenue and margin by delivery region
- Payment method share analysis
- Returns analysis by region

---

## 📊 Sample System Output

```
======================================================================
   GHANA RETAIL SALES ANALYTICS — EXECUTIVE SUMMARY
======================================================================
  Reporting Period         : 01 Jan 2023 — 31 Dec 2024
  Total Orders             : 50,000
  Completed Orders         : 46,752
  Unique Customers         : 500
  Total Net Revenue        : GHS 78,632,142.16
  Total Gross Profit       : GHS 25,757,758.24
  Overall Profit Margin    : 32.8%
  Average Order Value      : GHS 1,681.90
  Total Discounts Given    : GHS 2,585,393.56
  Returns                  : 2,225 orders (4.5%)
----------------------------------------------------------------------
  TOP SALESPERSON: Esi Tetteh — GHS 4,158,371.64
  TOP PRODUCT:     Laptop (Electronics) — GHS 16,835,804.68
  TOP REGION:      Northern — GHS 11,512,928.68
======================================================================
```

---

## 🚀 How To Run

### 1. Clone the repo
```bash
git clone https://github.com/lawrykoomson/Sales-Analytics-ETL-Pipeline.git
cd Sales-Analytics-ETL-Pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment (optional)
```bash
copy .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 5. Run the full system
```bash
python run_pipeline.py
```

Or run each layer individually:
```bash
python sales_data_generator.py   # Layer 1 — Generate database
python etl_pipeline.py           # Layer 2 — Run ETL
python analytics_report.py       # Layer 3 — Generate reports
```

---

## 📁 Output Files

```
data/
├── raw/
│   └── sales_database.db          ← SQLite source database
├── processed/
│   ├── fact_orders_*.csv          ← 50,000 enriched orders
│   ├── fact_items_*.csv           ← 87,499 line items
│   └── product_performance_*.csv  ← Product KPIs
└── reports/
    ├── monthly_revenue.csv
    ├── top_products.csv
    ├── salesperson_performance.csv
    ├── regional_revenue.csv
    ├── payment_methods.csv
    └── returns_analysis.csv
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core pipeline language |
| Pandas | Data transformation and KPI engineering |
| NumPy | Numerical operations |
| SQLite | Source database (data generation) |
| psycopg2 | PostgreSQL warehouse connector |
| Faker | Realistic Ghana data generation |
| python-dotenv | Environment variable management |

---

## 🔮 Future Improvements
- [ ] Apache Airflow DAG for daily scheduled pipeline runs
- [ ] Power BI dashboard connected to PostgreSQL warehouse
- [ ] Real-time order streaming with Apache Kafka
- [ ] Predictive sales forecasting with Prophet / ARIMA
- [ ] dbt models for analytical transformation layer
- [ ] REST API to expose KPIs for dashboard consumption

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
Final Year Project — 2025
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)