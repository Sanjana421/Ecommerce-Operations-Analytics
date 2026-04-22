import duckdb
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "supply_chain.duckdb")
OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed")

queries = {

    "kpi_monthly_revenue": """
        SELECT
            order_month,
            COUNT(DISTINCT order_id)        AS total_orders,
            ROUND(SUM(total_payment), 2)    AS total_revenue,
            ROUND(AVG(total_payment), 2)    AS avg_order_value
        FROM master
        WHERE total_payment IS NOT NULL
        GROUP BY order_month
        ORDER BY order_month
    """,

    "kpi_delivery": """
        SELECT
            order_month,
            COUNT(*)                                                    AS total_delivered,
            SUM(is_late)                                                AS late_orders,
            ROUND(100.0 * SUM(is_late) / COUNT(*), 2)                  AS late_rate_pct,
            ROUND(100.0 * (1 - SUM(is_late) * 1.0 / COUNT(*)), 2)     AS on_time_rate_pct,
            ROUND(AVG(delivery_days_actual), 1)                        AS avg_delivery_days
        FROM master
        WHERE delivery_days_actual IS NOT NULL
        GROUP BY order_month
        ORDER BY order_month
    """,

    "kpi_category": """
        SELECT
            product_category,
            COUNT(DISTINCT order_id)        AS total_orders,
            ROUND(SUM(total_payment), 2)    AS total_revenue,
            ROUND(AVG(review_score), 2)     AS avg_review_score
        FROM master
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY total_revenue DESC
        LIMIT 20
    """,

    "kpi_seller": """
        SELECT
            seller_id,
            seller_state,
            COUNT(DISTINCT order_id)                        AS total_orders,
            ROUND(SUM(total_payment), 2)                    AS total_revenue,
            ROUND(AVG(delivery_days_actual), 1)             AS avg_delivery_days,
            ROUND(AVG(review_score), 2)                     AS avg_review_score,
            ROUND(100.0 * SUM(is_late) / COUNT(*), 2)       AS late_rate_pct
        FROM master
        WHERE seller_id IS NOT NULL
        GROUP BY seller_id, seller_state
        ORDER BY total_revenue DESC
    """,

    "kpi_geography": """
        SELECT
            customer_state,
            COUNT(DISTINCT customer_id)     AS unique_customers,
            COUNT(DISTINCT order_id)        AS total_orders,
            ROUND(SUM(total_payment), 2)    AS total_revenue
        FROM master
        GROUP BY customer_state
        ORDER BY total_revenue DESC
    """,

    "kpi_payment": """
        SELECT
            payment_type,
            COUNT(*)                            AS num_orders,
            ROUND(SUM(total_payment), 2)        AS total_revenue,
            ROUND(AVG(num_installments), 1)     AS avg_installments
        FROM master
        WHERE payment_type IS NOT NULL
        GROUP BY payment_type
        ORDER BY num_orders DESC
    """,

    "kpi_summary": """
        SELECT
            COUNT(DISTINCT order_id)                                AS total_orders,
            ROUND(SUM(total_payment), 2)                            AS total_revenue,
            ROUND(AVG(total_payment), 2)                            AS avg_order_value,
            ROUND(AVG(delivery_days_actual), 1)                     AS avg_delivery_days,
            ROUND(100.0 * SUM(is_late) / COUNT(*), 2)              AS late_rate_pct,
            ROUND(100.0 * (1 - SUM(is_late) * 1.0 / COUNT(*)), 2) AS on_time_rate_pct,
            ROUND(AVG(review_score), 2)                             AS avg_review_score,
            COUNT(DISTINCT customer_state)                          AS states_served,
            COUNT(DISTINCT seller_id)                               AS total_sellers
        FROM master
    """
}

def run_kpis():
    con = duckdb.connect(DB_PATH)
    os.makedirs(OUT_PATH, exist_ok=True)

    print("=" * 50)
    print("📊 Running KPI queries...")
    print("=" * 50)

    for name, query in queries.items():
        df = con.execute(query).df()
        out_file = os.path.join(OUT_PATH, f"{name}.csv")
        df.to_csv(out_file, index=False)
        print(f"✅ {name}: {len(df)} rows → saved")

    # Print the summary KPI to terminal so you can see key numbers
    print("\n" + "=" * 50)
    print("🔑 KEY METRICS SUMMARY")
    print("=" * 50)
    summary = con.execute(queries["kpi_summary"]).df()
    for col in summary.columns:
        print(f"   {col}: {summary[col].iloc[0]:,}")

    con.close()
    print("\n✅ All KPI CSVs saved to data/processed/")

if __name__ == "__main__":
    run_kpis()