"""
Sales Analytics Report
=======================
Reads the transformed fact table and generates business KPI reports.
Exports summary reports to data/reports/ as CSV files.

KPI Reports:
    1. Executive Summary
    2. Revenue by Month
    3. Top 10 Products
    4. Top 10 Salespersons
    5. Revenue by Region
    6. Payment Method Breakdown
    7. Customer Demographics
    8. Returns Analysis

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import glob
import os

PROCESSED_PATH = Path("data/processed/")
REPORTS_PATH   = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


def load_fact_table() -> pd.DataFrame:
    """Load the most recent fact_orders CSV from processed folder."""
    files = sorted(glob.glob(str(PROCESSED_PATH / "fact_orders_*.csv")))
    if not files:
        raise FileNotFoundError("No fact_orders CSV found. Run etl_pipeline.py first.")
    latest = files[-1]
    print(f"Loading: {latest}")
    df = pd.read_csv(latest, parse_dates=["order_date"])
    return df


def print_executive_summary(df: pd.DataFrame):
    completed = df[df["order_status"] == "Completed"]
    returned  = df[df["order_status"] == "Returned"]

    total_revenue  = completed["total_net"].sum()
    total_profit   = completed["total_profit"].sum()
    total_orders   = len(completed)
    total_returned = len(returned)
    return_rate    = total_returned / len(df) * 100
    avg_order_val  = completed["total_net"].mean()
    margin_pct     = total_profit / total_revenue * 100 if total_revenue > 0 else 0
    total_discount = completed["total_discount"].sum()
    unique_customers = df["customer_id"].nunique()

    print("\n" + "="*70)
    print("   GHANA RETAIL SALES ANALYTICS — EXECUTIVE SUMMARY")
    print("="*70)
    print(f"  Reporting Period         : {df['order_date'].min().strftime('%d %b %Y')} — {df['order_date'].max().strftime('%d %b %Y')}")
    print(f"  Total Orders             : {len(df):,}")
    print(f"  Completed Orders         : {total_orders:,}")
    print(f"  Unique Customers         : {unique_customers:,}")
    print(f"  Total Net Revenue        : GHS {total_revenue:,.2f}")
    print(f"  Total Gross Profit       : GHS {total_profit:,.2f}")
    print(f"  Overall Profit Margin    : {margin_pct:.1f}%")
    print(f"  Average Order Value      : GHS {avg_order_val:,.2f}")
    print(f"  Total Discounts Given    : GHS {total_discount:,.2f}")
    print(f"  Returns                  : {total_returned:,} orders ({return_rate:.1f}%)")


def print_monthly_revenue(df: pd.DataFrame):
    completed = df[df["order_status"] == "Completed"]
    monthly = completed.groupby(["order_year","order_month","order_month_name"]).agg(
        Orders        = ("order_id", "count"),
        Net_Revenue   = ("total_net", "sum"),
        Gross_Profit  = ("total_profit", "sum"),
        Avg_Order_Val = ("total_net", "mean"),
    ).round(2).reset_index()
    monthly = monthly.sort_values(["order_year","order_month"])
    monthly["Margin_%"] = (monthly["Gross_Profit"] / monthly["Net_Revenue"] * 100).round(1)

    print("\n" + "-"*70)
    print("  MONTHLY REVENUE BREAKDOWN")
    print("-"*70)
    for _, row in monthly.iterrows():
        print(f"  {row['order_month_name'][:3]} {int(row['order_year'])}  |  "
              f"Orders: {int(row['Orders']):>5,}  |  "
              f"Revenue: GHS {row['Net_Revenue']:>12,.2f}  |  "
              f"Margin: {row['Margin_%']:>5.1f}%")

    monthly.to_csv(REPORTS_PATH / "monthly_revenue.csv", index=False)


def print_top_products(df: pd.DataFrame):
    items_files = sorted(glob.glob(str(PROCESSED_PATH / "fact_items_*.csv")))
    if not items_files:
        print("\n  [!] No items file found — skipping product report")
        return

    items = pd.read_csv(items_files[-1])
    # Only completed orders
    completed_ids = df[df["order_status"] == "Completed"]["order_id"].tolist()
    items = items[items["order_id"].isin(completed_ids)]

    product_files = sorted(glob.glob(str(PROCESSED_PATH / "product_performance_*.csv")))
    if product_files:
        prod = pd.read_csv(product_files[-1])
        prod = prod[prod["order_id"].isin(completed_ids) if "order_id" in prod.columns else [True]*len(prod)]

    top_by_revenue = items.groupby(["product_id","category"]).agg(
        Units_Sold    = ("quantity", "sum"),
        Net_Revenue   = ("net_revenue", "sum"),
        Gross_Profit  = ("gross_profit", "sum"),
    ).round(2).reset_index().sort_values("Net_Revenue", ascending=False).head(10)

    print("\n" + "-"*70)
    print("  TOP 10 PRODUCTS BY NET REVENUE")
    print("-"*70)
    for rank, (_, row) in enumerate(top_by_revenue.iterrows(), 1):
        margin = row["Gross_Profit"] / row["Net_Revenue"] * 100 if row["Net_Revenue"] > 0 else 0
        print(f"  {rank:>2}. Product {int(row['product_id']):>3}  [{row['category']:<16}]  "
              f"Units: {int(row['Units_Sold']):>5,}  |  "
              f"Revenue: GHS {row['Net_Revenue']:>10,.2f}  |  "
              f"Margin: {margin:.1f}%")

    top_by_revenue.to_csv(REPORTS_PATH / "top_products.csv", index=False)


def print_top_salespersons(df: pd.DataFrame):
    completed = df[df["order_status"] == "Completed"]
    sales_perf = completed.groupby("salesperson_name").agg(
        Orders        = ("order_id", "count"),
        Net_Revenue   = ("total_net", "sum"),
        Gross_Profit  = ("total_profit", "sum"),
        Avg_Order_Val = ("total_net", "mean"),
        Customers     = ("customer_id", "nunique"),
    ).round(2).reset_index().sort_values("Net_Revenue", ascending=False)

    print("\n" + "-"*70)
    print("  TOP SALESPERSONS BY NET REVENUE")
    print("-"*70)
    for rank, (_, row) in enumerate(sales_perf.head(10).iterrows(), 1):
        print(f"  {rank:>2}. {row['salesperson_name']:<22}  "
              f"Orders: {int(row['Orders']):>4,}  |  "
              f"Revenue: GHS {row['Net_Revenue']:>10,.2f}  |  "
              f"Customers: {int(row['Customers']):>3}")

    sales_perf.to_csv(REPORTS_PATH / "salesperson_performance.csv", index=False)


def print_regional_revenue(df: pd.DataFrame):
    completed = df[df["order_status"] == "Completed"]
    regional = completed.groupby("delivery_region").agg(
        Orders      = ("order_id", "count"),
        Net_Revenue = ("total_net", "sum"),
        Profit      = ("total_profit", "sum"),
        Customers   = ("customer_id", "nunique"),
    ).round(2).reset_index().sort_values("Net_Revenue", ascending=False)

    print("\n" + "-"*70)
    print("  REVENUE BY DELIVERY REGION")
    print("-"*70)
    for _, row in regional.iterrows():
        margin = row["Profit"] / row["Net_Revenue"] * 100 if row["Net_Revenue"] > 0 else 0
        print(f"  {row['delivery_region']:<18}  "
              f"Orders: {int(row['Orders']):>5,}  |  "
              f"Revenue: GHS {row['Net_Revenue']:>11,.2f}  |  "
              f"Margin: {margin:.1f}%")

    regional.to_csv(REPORTS_PATH / "regional_revenue.csv", index=False)


def print_payment_methods(df: pd.DataFrame):
    completed = df[df["order_status"] == "Completed"]
    payment = completed.groupby("payment_method").agg(
        Orders      = ("order_id", "count"),
        Net_Revenue = ("total_net", "sum"),
    ).round(2).reset_index().sort_values("Net_Revenue", ascending=False)
    payment["Revenue_%"] = (payment["Net_Revenue"] / payment["Net_Revenue"].sum() * 100).round(1)

    print("\n" + "-"*70)
    print("  PAYMENT METHOD BREAKDOWN")
    print("-"*70)
    for _, row in payment.iterrows():
        print(f"  {row['payment_method']:<18}  "
              f"Orders: {int(row['Orders']):>5,}  |  "
              f"Revenue: GHS {row['Net_Revenue']:>11,.2f}  |  "
              f"Share: {row['Revenue_%']:>5.1f}%")

    payment.to_csv(REPORTS_PATH / "payment_methods.csv", index=False)


def print_returns_analysis(df: pd.DataFrame):
    returned  = df[df["order_status"] == "Returned"]
    completed = df[df["order_status"] == "Completed"]

    print("\n" + "-"*70)
    print("  RETURNS ANALYSIS")
    print("-"*70)
    print(f"  Total Returned Orders    : {len(returned):,}")
    print(f"  Return Rate              : {len(returned)/len(df)*100:.1f}%")
    print(f"  Revenue Lost to Returns  : GHS {returned['total_net'].sum():,.2f}")

    by_region = returned.groupby("delivery_region")["order_id"].count().sort_values(ascending=False)
    print("\n  Returns by Region:")
    for region, count in by_region.items():
        print(f"    {region:<20} : {count:,} returns")

    returned.to_csv(REPORTS_PATH / "returns_analysis.csv", index=False)


def generate_all_reports():
    print("\n" + "="*70)
    print("   SALES ANALYTICS REPORT ENGINE — STARTED")
    print("="*70)

    df = load_fact_table()

    print_executive_summary(df)
    print_monthly_revenue(df)
    print_top_products(df)
    print_top_salespersons(df)
    print_regional_revenue(df)
    print_payment_methods(df)
    print_returns_analysis(df)

    print("\n" + "="*70)
    print(f"   REPORTS EXPORTED TO: {REPORTS_PATH}")
    print("="*70 + "\n")


if __name__ == "__main__":
    generate_all_reports()