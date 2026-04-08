"""
Master Pipeline Runner
=======================
Runs the complete Sales Analytics system end to end:
    Step 1 — Generate sales database (SQLite)
    Step 2 — Run ETL pipeline (Extract → Transform → Load)
    Step 3 — Generate analytics reports

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("master_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    start = datetime.now()

    logger.info("=" * 65)
    logger.info("  SALES ANALYTICS ETL PIPELINE — FULL SYSTEM RUN")
    logger.info("=" * 65)

    # ── STEP 1: Generate source data
    logger.info("STEP 1/3 — Generating sales database...")
    from sales_data_generator import create_database
    create_database()

    # ── STEP 2: Run ETL pipeline
    logger.info("\nSTEP 2/3 — Running ETL pipeline...")
    from etl_pipeline import extract, transform, load
    tables              = extract()
    fact, items, prods  = transform(tables)
    load(fact, items, prods)

    # ── STEP 3: Generate analytics reports
    logger.info("\nSTEP 3/3 — Generating analytics reports...")
    from analytics_report import generate_all_reports
    generate_all_reports()

    duration = (datetime.now() - start).total_seconds()
    logger.info(f"\n{'='*65}")
    logger.info(f"  FULL SYSTEM COMPLETED in {duration:.2f} seconds")
    logger.info(f"{'='*65}")


if __name__ == "__main__":
    main()