"""
Sales Analytics ETL Pipeline
==============================
Ingests retail sales transaction data, enriches with
business KPIs, and loads into PostgreSQL for analytics.

Targets: Hubtel Ghana (retail analytics)

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "sales_warehouse"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PROCESSED_PATH = Path("data/processed/")

CATEGORIES  = ["Electronics","Groceries","Clothing","Home & Garden","Health & Beauty","Sports","Automotive"]
REGIONS     = ["Greater Accra","Ashanti","Western","Eastern","Northern","Volta"]
CHANNELS    = ["In-Store","Online","Mobile App","Agent"]
PAYMENT_METHODS = ["MoMo","Cash","Card","Bank Transfer"]


def extract() -> pd.DataFrame:
    logger.info("[EXTRACT] Generating synthetic sales transaction data...")
    np.random.seed(42)
    n = 25000

    base_date = datetime(2024, 1, 1)
    timestamps = [
        base_date + timedelta(
            days=int(np.random.randint(0, 365)),
            hours=int(np.random.randint(6, 22)),
            minutes=int(np.random.randint(0, 59))
        ) for _ in range(n)
    ]

    unit_prices = np.abs(np.random.lognormal(4.5, 1.0, n)).round(2)
    quantities  = np.random.choice([1,2,3,4,5,6,7,8,9,10], n,
                                   p=[0.35,0.25,0.15,0.10,0.06,0.04,0.02,0.01,0.01,0.01])
    discounts   = np.random.choice([0,0,0,0.05,0.10,0.15,0.20,0.25], n,
                                   p=[0.40,0.20,0.15,0.10,0.06,0.04,0.03,0.02])

    df = pd.DataFrame({
        "transaction_id":   [f"SALE-{str(i).zfill(9)}" for i in range(1, n+1)],
        "timestamp":        timestamps,
        "customer_id":      [f"CUST{str(np.random.randint(1, 5001)).zfill(6)}" for _ in range(n)],
        "product_id":       [f"PROD{str(np.random.randint(1, 501)).zfill(5)}" for _ in range(n)],
        "product_name":     [f"Product {np.random.randint(1, 501)}" for _ in range(n)],
        "category":         np.random.choice(CATEGORIES, n,
                                p=[0.20,0.25,0.15,0.12,0.12,0.10,0.06]),
        "region":           np.random.choice(REGIONS, n,
                                p=[0.35,0.25,0.15,0.12,0.08,0.05]),
        "channel":          np.random.choice(CHANNELS, n,
                                p=[0.35,0.30,0.25,0.10]),
        "payment_method":   np.random.choice(PAYMENT_METHODS, n,
                                p=[0.45,0.25,0.20,0.10]),
        "unit_price_ghs":   unit_prices,
        "quantity":         quantities,
        "discount_pct":     discounts,
        "salesperson_id":   [f"SP{str(np.random.randint(1, 101)).zfill(4)}" for _ in range(n)],
        "store_id":         [f"STR{str(np.random.randint(1, 51)).zfill(3)}" for _ in range(n)],
    })

    logger.info(f"[EXTRACT] Generated {len(df):,} sales transactions.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Enriching sales data with business KPIs...")

    df["sale_date"]       = df["timestamp"].dt.date
    df["sale_month"]      = df["timestamp"].dt.month
    df["sale_quarter"]    = df["timestamp"].dt.quarter
    df["sale_year"]       = df["timestamp"].dt.year
    df["sale_hour"]       = df["timestamp"].dt.hour
    df["day_of_week"]     = df["timestamp"].dt.day_name()
    df["is_weekend"]      = df["timestamp"].dt.dayofweek >= 5

    df["gross_revenue_ghs"]   = (df["unit_price_ghs"] * df["quantity"]).round(2)
    df["discount_amount_ghs"] = (df["gross_revenue_ghs"] * df["discount_pct"]).round(2)
    df["net_revenue_ghs"]     = (df["gross_revenue_ghs"] - df["discount_amount_ghs"]).round(2)

    cost_pct = np.random.uniform(0.45, 0.70, len(df))
    df["cost_ghs"]        = (df["net_revenue_ghs"] * cost_pct).round(2)
    df["gross_profit_ghs"] = (df["net_revenue_ghs"] - df["cost_ghs"]).round(2)
    df["profit_margin_pct"] = (
        df["gross_profit_ghs"] / df["net_revenue_ghs"].replace(0, np.nan) * 100
    ).round(2).fillna(0)

    df["is_high_value"]   = df["net_revenue_ghs"] >= 500
    df["is_discounted"]   = df["discount_pct"] > 0

    df["revenue_tier"] = pd.cut(
        df["net_revenue_ghs"],
        bins=[-1, 50, 200, 500, 2000, float("inf")],
        labels=["Micro (<50)","Small (50-200)","Medium (200-500)",
                "Large (500-2k)","Premium (2k+)"]
    ).astype(str)

    df["processed_at"] = datetime.now()

    logger.info(f"[TRANSFORM] Complete. Total Net Revenue: GHS {df['net_revenue_ghs'].sum():,.2f}")
    return df


def load(df: pd.DataFrame):
    logger.info("[LOAD] Attempting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS sales_dw;

                CREATE TABLE IF NOT EXISTS sales_dw.fact_sales (
                    transaction_id      VARCHAR(20) PRIMARY KEY,
                    timestamp           TIMESTAMP,
                    sale_date           DATE,
                    sale_month          SMALLINT,
                    sale_quarter        SMALLINT,
                    sale_year           SMALLINT,
                    sale_hour           SMALLINT,
                    day_of_week         VARCHAR(12),
                    is_weekend          BOOLEAN,
                    customer_id         VARCHAR(12),
                    product_id          VARCHAR(10),
                    product_name        VARCHAR(50),
                    category            VARCHAR(30),
                    region              VARCHAR(50),
                    channel             VARCHAR(20),
                    payment_method      VARCHAR(20),
                    salesperson_id      VARCHAR(8),
                    store_id            VARCHAR(8),
                    unit_price_ghs      NUMERIC(12,2),
                    quantity            SMALLINT,
                    discount_pct        NUMERIC(5,2),
                    gross_revenue_ghs   NUMERIC(14,2),
                    discount_amount_ghs NUMERIC(14,2),
                    net_revenue_ghs     NUMERIC(14,2),
                    cost_ghs            NUMERIC(14,2),
                    gross_profit_ghs    NUMERIC(14,2),
                    profit_margin_pct   NUMERIC(6,2),
                    is_high_value       BOOLEAN,
                    is_discounted       BOOLEAN,
                    revenue_tier        VARCHAR(20),
                    processed_at        TIMESTAMP
                );
            """)
            conn.commit()

        load_cols = [
            "transaction_id","timestamp","sale_date","sale_month","sale_quarter",
            "sale_year","sale_hour","day_of_week","is_weekend","customer_id",
            "product_id","product_name","category","region","channel",
            "payment_method","salesperson_id","store_id","unit_price_ghs",
            "quantity","discount_pct","gross_revenue_ghs","discount_amount_ghs",
            "net_revenue_ghs","cost_ghs","gross_profit_ghs","profit_margin_pct",
            "is_high_value","is_discounted","revenue_tier","processed_at"
        ]

        records = [tuple(r) for r in df[load_cols].itertuples(index=False)]

        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO sales_dw.fact_sales ({','.join(load_cols)}) "
                f"VALUES %s ON CONFLICT (transaction_id) DO UPDATE SET "
                f"net_revenue_ghs=EXCLUDED.net_revenue_ghs, "
                f"processed_at=EXCLUDED.processed_at",
                records, page_size=500
            )
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Successfully loaded {len(df):,} records into PostgreSQL.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        fallback = PROCESSED_PATH / f"sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        logger.info(f"[LOAD] Saved to {fallback}")


def print_summary(df: pd.DataFrame):
    print("\n" + "="*68)
    print("   SALES ANALYTICS ETL PIPELINE — RUN SUMMARY")
    print("="*68)
    print(f"  Total Transactions      : {len(df):,}")
    print(f"  Total Gross Revenue     : GHS {df['gross_revenue_ghs'].sum():,.2f}")
    print(f"  Total Net Revenue       : GHS {df['net_revenue_ghs'].sum():,.2f}")
    print(f"  Total Gross Profit      : GHS {df['gross_profit_ghs'].sum():,.2f}")
    print(f"  Avg Profit Margin       : {df['profit_margin_pct'].mean():.1f}%")
    print(f"  High Value Transactions : {df['is_high_value'].sum():,}")
    print(f"  Discounted Transactions : {df['is_discounted'].sum():,}")
    print("-"*68)
    print("  REVENUE BY CATEGORY:")
    cat_rev = df.groupby("category")["net_revenue_ghs"].sum().sort_values(ascending=False)
    for cat, val in cat_rev.items():
        print(f"    {cat:<20} : GHS {val:,.2f}")
    print("-"*68)
    print("  REVENUE BY REGION:")
    reg_rev = df.groupby("region")["net_revenue_ghs"].sum().sort_values(ascending=False)
    for reg, val in reg_rev.items():
        print(f"    {reg:<20} : GHS {val:,.2f}")
    print("-"*68)
    print("  REVENUE BY CHANNEL:")
    ch_rev = df.groupby("channel")["net_revenue_ghs"].sum().sort_values(ascending=False)
    for ch, val in ch_rev.items():
        print(f"    {ch:<20} : GHS {val:,.2f}")
    print("="*68 + "\n")


def run_pipeline():
    logger.info("=" * 62)
    logger.info("  SALES ANALYTICS ETL PIPELINE — STARTED")
    logger.info("=" * 62)
    start = datetime.now()
    df    = extract()
    df    = transform(df)
    load(df)
    print_summary(df)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f"PIPELINE COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()