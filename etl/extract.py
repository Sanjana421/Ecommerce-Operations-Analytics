import pandas as pd
import os

RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

def extract_all():
    """Load all raw CSVs into a dictionary of DataFrames."""
    files = {
        "customers":    "olist_customers_dataset.csv",
        "orders":       "olist_orders_dataset.csv",
        "order_items":  "olist_order_items_dataset.csv",
        "payments":     "olist_order_payments_dataset.csv",
        "reviews":      "olist_order_reviews_dataset.csv",
        "products":     "olist_products_dataset.csv",
        "sellers":      "olist_sellers_dataset.csv",
        "geolocation":  "olist_geolocation_dataset.csv",
        "translations": "product_category_name_translation.csv",
    }

    dataframes = {}
    for name, filename in files.items():
        path = os.path.join(RAW_PATH, filename)
        df = pd.read_csv(path)
        dataframes[name] = df
        print(f"✅ Loaded '{name}': {df.shape[0]:,} rows × {df.shape[1]} columns")

    return dataframes

if __name__ == "__main__":
    extract_all()