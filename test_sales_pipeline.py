"""
Unit Tests — Sales Analytics ETL Pipeline
==========================================
Run with: pytest test_sales_pipeline.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from etl_pipeline import extract, transform


class TestExtract:

    def test_returns_dataframe(self):
        df = extract()
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = extract()
        assert len(df) == 25000

    def test_required_columns_present(self):
        df = extract()
        required = [
            "transaction_id","timestamp","customer_id","product_id",
            "category","region","channel","payment_method",
            "unit_price_ghs","quantity","discount_pct"
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_transaction_ids_unique(self):
        df = extract()
        assert df["transaction_id"].nunique() == len(df)

    def test_unit_prices_positive(self):
        df = extract()
        assert (df["unit_price_ghs"] > 0).all()

    def test_quantities_positive(self):
        df = extract()
        assert (df["quantity"] > 0).all()

    def test_discounts_valid_range(self):
        df = extract()
        assert df["discount_pct"].between(0, 1).all()

    def test_categories_valid(self):
        df = extract()
        valid = {"Electronics","Groceries","Clothing","Home & Garden",
                 "Health & Beauty","Sports","Automotive"}
        assert set(df["category"].unique()).issubset(valid)

    def test_regions_valid(self):
        df = extract()
        valid = {"Greater Accra","Ashanti","Western","Eastern","Northern","Volta"}
        assert set(df["region"].unique()).issubset(valid)

    def test_channels_valid(self):
        df = extract()
        valid = {"In-Store","Online","Mobile App","Agent"}
        assert set(df["channel"].unique()).issubset(valid)


class TestTransform:

    @pytest.fixture
    def transformed(self):
        df = extract()
        return transform(df)

    def test_revenue_columns_exist(self, transformed):
        for col in ["gross_revenue_ghs","net_revenue_ghs","cost_ghs","gross_profit_ghs"]:
            assert col in transformed.columns

    def test_net_revenue_positive(self, transformed):
        assert (transformed["net_revenue_ghs"] > 0).all()

    def test_net_less_than_gross(self, transformed):
        discounted = transformed[transformed["discount_pct"] > 0]
        assert (discounted["net_revenue_ghs"] < discounted["gross_revenue_ghs"]).all()

    def test_profit_margin_range(self, transformed):
        assert transformed["profit_margin_pct"].between(0, 100).all()

    def test_time_columns_created(self, transformed):
        for col in ["sale_date","sale_month","sale_quarter","sale_hour","is_weekend"]:
            assert col in transformed.columns

    def test_revenue_tier_assigned(self, transformed):
        assert "revenue_tier" in transformed.columns
        assert transformed["revenue_tier"].isna().sum() == 0

    def test_high_value_flag_correct(self, transformed):
        high = transformed[transformed["is_high_value"]]
        assert (high["net_revenue_ghs"] >= 500).all()

    def test_discounted_flag_correct(self, transformed):
        disc = transformed[transformed["is_discounted"]]
        assert (disc["discount_pct"] > 0).all()

    def test_processed_at_exists(self, transformed):
        assert "processed_at" in transformed.columns

    def test_row_count_preserved(self, transformed):
        df = extract()
        assert len(transformed) == len(df)

    def test_no_null_net_revenue(self, transformed):
        assert transformed["net_revenue_ghs"].isna().sum() == 0


class TestIntegration:

    def test_full_pipeline_runs(self):
        df     = extract()
        result = transform(df)
        assert len(result) == len(df)

    def test_total_revenue_positive(self):
        df     = extract()
        result = transform(df)
        assert result["net_revenue_ghs"].sum() > 0

    def test_no_duplicate_transaction_ids(self):
        df     = extract()
        result = transform(df)
        assert result["transaction_id"].duplicated().sum() == 0

    def test_greater_accra_has_most_revenue(self):
        df     = extract()
        result = transform(df)
        top_region = result.groupby("region")["net_revenue_ghs"].sum().idxmax()
        assert top_region == "Greater Accra"

    def test_groceries_or_electronics_top_category(self):
        df     = extract()
        result = transform(df)
        top_cat = result.groupby("category")["net_revenue_ghs"].sum().idxmax()
        assert top_cat in {"Groceries","Electronics"}