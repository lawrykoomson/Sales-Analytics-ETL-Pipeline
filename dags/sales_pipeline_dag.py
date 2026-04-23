"""
Apache Airflow DAG — Sales Analytics ETL Pipeline
===================================================
Schedules the sales ETL pipeline to run every
day at 03:00 AM UTC (overnight batch processing).

Tasks:
    1. extract_sales      — generate/load sales transactions
    2. transform_sales    — enrich with business KPIs
    3. load_to_postgres   — load into PostgreSQL warehouse
    4. refresh_dbt        — rebuild analytical layer
    5. notify_completion  — log daily sales summary

Author: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="sales_analytics_etl_pipeline",
    default_args=default_args,
    description="Daily sales analytics ETL pipeline for Hubtel Ghana",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sales","etl","hubtel","ghana","data-engineering"],
)


def task_extract(**context):
    from etl_pipeline import extract
    df = extract()
    temp_path = "/tmp/sales_raw.csv"
    df.to_csv(temp_path, index=False)
    context["ti"].xcom_push(key="raw_count", value=len(df))
    context["ti"].xcom_push(key="temp_path", value=temp_path)
    logger.info(f"Extracted {len(df):,} sales transactions")
    return len(df)


def task_transform(**context):
    import pandas as pd
    from etl_pipeline import transform
    temp_path = context["ti"].xcom_pull(task_ids="extract_sales", key="temp_path")
    df = pd.read_csv(temp_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    result = transform(df)
    clean_path = "/tmp/sales_clean.csv"
    result.to_csv(clean_path, index=False)
    context["ti"].xcom_push(key="clean_path",   value=clean_path)
    context["ti"].xcom_push(key="net_revenue",  value=float(result["net_revenue_ghs"].sum()))
    context["ti"].xcom_push(key="total_profit", value=float(result["gross_profit_ghs"].sum()))
    logger.info(f"Transformed {len(result):,} records")
    return len(result)


def task_load(**context):
    import pandas as pd
    from etl_pipeline import transform, load
    temp_path = context["ti"].xcom_pull(task_ids="extract_sales", key="temp_path")
    df = pd.read_csv(temp_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    result = transform(df)
    load(result)
    logger.info("Load to PostgreSQL complete")
    return "success"


def task_notify(**context):
    run_date   = context["ds"]
    raw        = context["ti"].xcom_pull(task_ids="extract_sales",    key="raw_count")
    revenue    = context["ti"].xcom_pull(task_ids="transform_sales",  key="net_revenue")
    profit     = context["ti"].xcom_pull(task_ids="transform_sales",  key="total_profit")
    logger.info("=" * 60)
    logger.info("  SALES PIPELINE — DAILY SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Run Date          : {run_date}")
    logger.info(f"  Transactions      : {raw:,}")
    logger.info(f"  Net Revenue       : GHS {revenue:,.2f}")
    logger.info(f"  Gross Profit      : GHS {profit:,.2f}")
    logger.info("=" * 60)
    return "notified"


start        = EmptyOperator(task_id="pipeline_start",   dag=dag)
extract_task = PythonOperator(task_id="extract_sales",   python_callable=task_extract,   dag=dag)
transform_task = PythonOperator(task_id="transform_sales", python_callable=task_transform, dag=dag)
load_task    = PythonOperator(task_id="load_to_postgres", python_callable=task_load,     dag=dag)
notify_task  = PythonOperator(task_id="notify_completion", python_callable=task_notify,  dag=dag)
end          = EmptyOperator(task_id="pipeline_end",     dag=dag)

start >> extract_task >> transform_task >> load_task >> notify_task >> end