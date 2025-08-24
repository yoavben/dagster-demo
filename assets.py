import os

import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, Output, MetadataValue


@asset(owners=["richard.hendricks@hooli.com", "team:data-eng"])
def raw_sales_data():
    """Load raw sales data from CSV file."""
    df = pd.read_csv("sales_data.csv")
    return Output(
        df,
        metadata={
            "source_path": os.path.abspath("sales_data.csv"),
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": MetadataValue.md(df.head(5).to_markdown(index=False)),
        },
    )


@asset
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate sales data."""
    # Remove any rows with missing values
    cleaned = raw_sales_data.dropna()

    # Add calculated fields
    cleaned["total_revenue"] = cleaned["quantity"] * cleaned["price"]
    cleaned["date"] = pd.to_datetime(cleaned["date"])

    return cleaned


@asset
def sales_summary(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    """Create daily sales summary by region."""
    summary = clean_sales_data.groupby(["date", "region"]).agg({
        "quantity": "sum",
        "total_revenue": "sum",
        "product": "nunique"
    }).round(2)

    summary.columns = ["total_quantity", "total_revenue", "unique_products"]
    return summary.reset_index()


@asset
def top_products(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    """Find top-selling products by revenue."""
    product_revenue = clean_sales_data.groupby("product").agg({
        "total_revenue": "sum",
        "quantity": "sum"
    }).sort_values("total_revenue", ascending=False)

    return product_revenue.reset_index()



@asset_check(asset=sales_summary)
def sales_summary_has_data(sales_summary):
    return AssetCheckResult(passed=len(sales_summary) > 0)