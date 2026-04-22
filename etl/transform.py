import pandas as pd

def transform_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Parse dates, calculate delivery time, flag late deliveries."""
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in date_cols:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # Keep only delivered orders for delivery KPIs
    delivered = orders[orders["order_status"] == "delivered"].copy()

    # How many days did delivery actually take?
    delivered["delivery_days_actual"] = (
        delivered["order_delivered_customer_date"] -
        delivered["order_purchase_timestamp"]
    ).dt.days

    # How many days was delivery estimated to take?
    delivered["delivery_days_estimated"] = (
        delivered["order_estimated_delivery_date"] -
        delivered["order_purchase_timestamp"]
    ).dt.days

    # Was the order late? 1 = late, 0 = on time
    delivered["is_late"] = (
        delivered["order_delivered_customer_date"] >
        delivered["order_estimated_delivery_date"]
    ).astype(int)

    # Year-month string for trend charts e.g. "2017-11"
    delivered["order_month"] = (
        delivered["order_purchase_timestamp"].dt.to_period("M").astype(str)
    )
    delivered["order_year"] = delivered["order_purchase_timestamp"].dt.year

    print(f"✅ Transformed orders: {delivered.shape[0]:,} rows")
    print(f"   Late delivery rate: {delivered['is_late'].mean():.2%}")
    return delivered


def transform_order_items(items: pd.DataFrame, products: pd.DataFrame,
                           translations: pd.DataFrame) -> pd.DataFrame:
    """Join items with English product category names."""
    products = products.merge(translations, on="product_category_name", how="left")
    items = items.merge(
        products[["product_id", "product_category_name_english"]],
        on="product_id", how="left"
    )
    items["product_category_name_english"] = (
        items["product_category_name_english"].fillna("unknown")
    )
    print(f"✅ Transformed order_items: {items.shape[0]:,} rows")
    return items


def transform_payments(payments: pd.DataFrame) -> pd.DataFrame:
    """Aggregate payments per order — one order can have multiple payment rows."""
    agg = payments.groupby("order_id").agg(
        total_payment=("payment_value", "sum"),
        num_installments=("payment_installments", "max"),
        payment_type=("payment_type", lambda x: x.mode()[0])
    ).reset_index()
    print(f"✅ Transformed payments: {agg.shape[0]:,} rows")
    return agg


def transform_reviews(reviews: pd.DataFrame) -> pd.DataFrame:
    """Keep only the latest review per order, flag low scores."""
    reviews["review_creation_date"] = pd.to_datetime(
        reviews["review_creation_date"], errors="coerce"
    )
    reviews = reviews.sort_values("review_creation_date").drop_duplicates(
        subset="order_id", keep="last"
    )
    reviews["is_low_score"] = (reviews["review_score"] <= 2).astype(int)
    print(f"✅ Transformed reviews: {reviews.shape[0]:,} rows")
    return reviews


def transform_sellers(sellers: pd.DataFrame) -> pd.DataFrame:
    """Standardize seller state codes."""
    sellers["seller_state"] = sellers["seller_state"].str.upper().str.strip()
    print(f"✅ Transformed sellers: {sellers.shape[0]:,} rows")
    return sellers


def build_master_table(orders, items, payments, reviews, customers, sellers):
    """Join all cleaned tables into one wide analytical table."""
    df = orders.copy()

    # Join payments (revenue data)
    df = df.merge(payments, on="order_id", how="left")

    # Join reviews (satisfaction data)
    df = df.merge(
        reviews[["order_id", "review_score", "is_low_score"]],
        on="order_id", how="left"
    )

    # Join customers (geography data)
    df = df.merge(
        customers[["customer_id", "customer_state", "customer_city"]],
        on="customer_id", how="left"
    )

    # Aggregate items to order level then join
    items_agg = items.groupby("order_id").agg(
        num_items=("order_item_id", "count"),
        total_freight=("freight_value", "sum"),
        product_category=("product_category_name_english", lambda x: x.mode()[0]),
        seller_id=("seller_id", "first")
    ).reset_index()

    df = df.merge(items_agg, on="order_id", how="left")

    # Join sellers (seller location data)
    df = df.merge(
        sellers[["seller_id", "seller_state"]],
        on="seller_id", how="left"
    )

    print(f"✅ Master table built: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"   Columns: {list(df.columns)}")
    return df


def run_all_transforms(raw: dict) -> dict:
    print("\n--- Transforming each table ---")
    orders    = transform_orders(raw["orders"])
    items     = transform_order_items(raw["order_items"], raw["products"], raw["translations"])
    payments  = transform_payments(raw["payments"])
    reviews   = transform_reviews(raw["reviews"])
    sellers   = transform_sellers(raw["sellers"])
    customers = raw["customers"]

    print("\n--- Building master table ---")
    master = build_master_table(orders, items, payments, reviews, customers, sellers)

    return {
        "orders":    orders,
        "items":     items,
        "payments":  payments,
        "reviews":   reviews,
        "sellers":   sellers,
        "customers": customers,
        "master":    master
    }