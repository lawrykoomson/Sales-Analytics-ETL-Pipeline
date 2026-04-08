"""
Sales Analytics ETL Pipeline
==============================
Extracts from SQLite source database, transforms and enriches
the data, then loads into a clean PostgreSQL warehouse or CSV.

Pipeline Flow:
    Extract  -> SQLite (customers, products, orders, order_items, salespersons)
    Transform -> join tables, calculate KPIs, clean, enrich
    Load     -> PostgreSQL warehouse or CSV fallback

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "sales_warehouse"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

SQLITE_PATH   = Path("data/raw/sales_database.db")
PROCESSED_PATH = Path("data/processed/")


# ─────────────────────────────────────────────
#  EXTRACT
# ─────────────────────────────────────────────
def extract() -> dict:
    """
    Extract all 5 tables from the SQLite source database.
    Returns a dictionary of DataFrames keyed by table name.
    """
    logger.info(f"[EXTRACT] Connecting to SQLite: {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)

    tables = {}
    for table in ["customers", "products", "salespersons", "orders", "order_items"]:
        tables[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        logger.info(f"[EXTRACT] {table}: {len(tables[table]):,} records loaded")

    conn.close()
    logger.info("[EXTRACT] All tables extracted successfully.")
    return tables


# ─────────────────────────────────────────────
#  TRANSFORM
# ─────────────────────────────────────────────
def transform(tables: dict) -> tuple:
    """
    Join all tables, clean data, engineer KPIs,
    and produce the final fact table and dimension tables.
    """
    logger.info("[TRANSFORM] Starting transformation...")

    orders    = tables["orders"].copy()
    items     = tables["order_items"].copy()
    customers = tables["customers"].copy()
    products  = tables["products"].copy()
    salesreps = tables["salespersons"].copy()

    # ── 1. Parse dates
    orders["order_date"] = pd.to_datetime(orders["order_date"])

    # ── 2. Calculate line item financials
    items["gross_revenue"] = (
        items["unit_price"] * items["quantity"]
    ).round(2)
    items["discount_amount"] = (
        items["gross_revenue"] * items["discount_pct"] / 100
    ).round(2)
    items["net_revenue"] = (
        items["gross_revenue"] - items["discount_amount"]
    ).round(2)

    # Merge cost from products
    items = items.merge(
        products[["product_id", "cost_price", "category"]],
        on="product_id", how="left"
    )
    items["cogs"] = (items["cost_price"] * items["quantity"]).round(2)
    items["gross_profit"] = (items["net_revenue"] - items["cogs"]).round(2)
    items["margin_pct"]   = np.where(
        items["net_revenue"] > 0,
        (items["gross_profit"] / items["net_revenue"] * 100).round(2),
        0
    )

    # ── 3. Aggregate order items to order level
    order_agg = items.groupby("order_id").agg(
        total_items      = ("quantity", "sum"),
        total_gross      = ("gross_revenue", "sum"),
        total_discount   = ("discount_amount", "sum"),
        total_net        = ("net_revenue", "sum"),
        total_cogs       = ("cogs", "sum"),
        total_profit     = ("gross_profit", "sum"),
    ).round(2).reset_index()

    # ── 4. Build master fact table
    fact = orders.merge(order_agg,     on="order_id",     how="left")
    fact = fact.merge(customers[["customer_id","first_name","last_name",
                                  "region","city","gender","age"]],
                      on="customer_id", how="left")
    fact = fact.merge(salesreps[["salesperson_id","full_name"]],
                      on="salesperson_id", how="left")
    fact.rename(columns={"full_name": "salesperson_name",
                          "region": "customer_region"}, inplace=True)

    # ── 5. Time dimension engineering
    fact["order_year"]        = fact["order_date"].dt.year
    fact["order_month"]       = fact["order_date"].dt.month
    fact["order_month_name"]  = fact["order_date"].dt.strftime("%B")
    fact["order_quarter"]     = fact["order_date"].dt.quarter
    fact["order_day_of_week"] = fact["order_date"].dt.day_name()
    fact["order_hour"]        = fact["order_date"].dt.hour
    fact["is_weekend"]        = fact["order_date"].dt.dayofweek >= 5

    # ── 6. Business flags
    fact["is_returned"]    = fact["order_status"] == "Returned"
    fact["is_completed"]   = fact["order_status"] == "Completed"
    fact["is_high_value"]  = fact["total_net"] >= fact["total_net"].quantile(0.90)

    # ── 7. Customer age group
    fact["age_group"] = pd.cut(
        fact["age"],
        bins=[0, 25, 35, 45, 55, 100],
        labels=["18-25", "26-35", "36-45", "46-55", "55+"]
    ).astype(str)

    # ── 8. Order size tier
    fact["order_tier"] = pd.cut(
        fact["total_net"],
        bins=[0, 100, 500, 1000, 5000, float("inf")],
        labels=["Micro", "Small", "Medium", "Large", "Enterprise"]
    ).astype(str)

    # ── 9. Clean nulls
    fact["total_items"]    = fact["total_items"].fillna(0).astype(int)
    fact["total_gross"]    = fact["total_gross"].fillna(0)
    fact["total_discount"] = fact["total_discount"].fillna(0)
    fact["total_net"]      = fact["total_net"].fillna(0)
    fact["total_cogs"]     = fact["total_cogs"].fillna(0)
    fact["total_profit"]   = fact["total_profit"].fillna(0)
    fact["processed_at"]   = datetime.now()

    logger.info(f"[TRANSFORM] Fact table built: {len(fact):,} orders")
    logger.info(f"[TRANSFORM] Total net revenue: GHS {fact['total_net'].sum():,.2f}")
    logger.info(f"[TRANSFORM] Total gross profit: GHS {fact['total_profit'].sum():,.2f}")

    # ── 10. Product performance summary
    product_perf = items.groupby(["product_id","category"]).agg(
        product_name    = ("product_id", "first"),
        units_sold      = ("quantity", "sum"),
        gross_revenue   = ("gross_revenue", "sum"),
        net_revenue     = ("net_revenue", "sum"),
        total_profit    = ("gross_profit", "sum"),
        avg_margin_pct  = ("margin_pct", "mean"),
        num_orders      = ("order_id", "nunique"),
    ).round(2).reset_index()

    product_perf = product_perf.merge(
        products[["product_id","product_name"]], on="product_id", how="left"
    )
    product_perf.drop(columns=["product_name_x"], errors="ignore", inplace=True)
    product_perf.rename(columns={"product_name_y": "product_name"}, inplace=True)

    logger.info(f"[TRANSFORM] Product performance: {len(product_perf)} products analysed")
    return fact, items, product_perf


# ─────────────────────────────────────────────
#  LOAD
# ─────────────────────────────────────────────
def load(fact: pd.DataFrame, items: pd.DataFrame, product_perf: pd.DataFrame):
    """
    Load transformed data into PostgreSQL warehouse.
    Falls back to CSV if database is unavailable.
    """
    logger.info("[LOAD] Attempting PostgreSQL connection...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS sales_dw;

                CREATE TABLE IF NOT EXISTS sales_dw.fact_orders (
                    order_id            INTEGER PRIMARY KEY,
                    customer_id         INTEGER,
                    salesperson_id      INTEGER,
                    order_date          TIMESTAMP,
                    order_status        VARCHAR(20),
                    payment_method      VARCHAR(20),
                    delivery_region     VARCHAR(50),
                    total_items         INTEGER,
                    total_gross         NUMERIC(12,2),
                    total_discount      NUMERIC(12,2),
                    total_net           NUMERIC(12,2),
                    total_cogs          NUMERIC(12,2),
                    total_profit        NUMERIC(12,2),
                    first_name          VARCHAR(50),
                    last_name           VARCHAR(50),
                    customer_region     VARCHAR(50),
                    city                VARCHAR(50),
                    gender              VARCHAR(5),
                    age                 SMALLINT,
                    salesperson_name    VARCHAR(100),
                    order_year          SMALLINT,
                    order_month         SMALLINT,
                    order_month_name    VARCHAR(15),
                    order_quarter       SMALLINT,
                    order_day_of_week   VARCHAR(12),
                    order_hour          SMALLINT,
                    is_weekend          BOOLEAN,
                    is_returned         BOOLEAN,
                    is_completed        BOOLEAN,
                    is_high_value       BOOLEAN,
                    age_group           VARCHAR(10),
                    order_tier          VARCHAR(15),
                    processed_at        TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sales_dw.fact_order_items (
                    item_id         INTEGER PRIMARY KEY,
                    order_id        INTEGER,
                    product_id      INTEGER,
                    quantity        INTEGER,
                    unit_price      NUMERIC(10,2),
                    discount_pct    NUMERIC(5,2),
                    gross_revenue   NUMERIC(12,2),
                    discount_amount NUMERIC(10,2),
                    net_revenue     NUMERIC(12,2),
                    cost_price      NUMERIC(10,2),
                    cogs            NUMERIC(12,2),
                    gross_profit    NUMERIC(12,2),
                    margin_pct      NUMERIC(6,2),
                    category        VARCHAR(50)
                );
            """)
            conn.commit()

        # Load fact orders
        fact_cols = [
            "order_id","customer_id","salesperson_id","order_date","order_status",
            "payment_method","delivery_region","total_items","total_gross",
            "total_discount","total_net","total_cogs","total_profit",
            "first_name","last_name","customer_region","city","gender","age",
            "salesperson_name","order_year","order_month","order_month_name",
            "order_quarter","order_day_of_week","order_hour","is_weekend",
            "is_returned","is_completed","is_high_value","age_group",
            "order_tier","processed_at"
        ]
        records = [tuple(r) for r in fact[fact_cols].itertuples(index=False)]
        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO sales_dw.fact_orders ({','.join(fact_cols)}) VALUES %s "
                f"ON CONFLICT (order_id) DO UPDATE SET "
                f"total_net=EXCLUDED.total_net, processed_at=EXCLUDED.processed_at",
                records, page_size=1000
            )
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Loaded {len(fact):,} orders into PostgreSQL warehouse.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        logger.info("[LOAD] Saving to CSV fallback...")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fact.to_csv(PROCESSED_PATH / f"fact_orders_{ts}.csv", index=False)
        items.to_csv(PROCESSED_PATH / f"fact_items_{ts}.csv", index=False)
        product_perf.to_csv(PROCESSED_PATH / f"product_performance_{ts}.csv", index=False)
        logger.info(f"[LOAD] Saved 3 CSV files to {PROCESSED_PATH}")


if __name__ == "__main__":
    tables = extract()
    fact, items, product_perf = transform(tables)
    load(fact, items, product_perf)