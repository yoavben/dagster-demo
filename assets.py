import pandas as pd
from dagster import asset


@asset
def raw_sales_data():
    """Load raw sales data from CSV file."""
    return pd.read_csv("sales_data.csv")
