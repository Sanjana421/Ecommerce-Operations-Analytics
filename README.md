# Ecommerce Operations Analytics Pipeline

An end-to-end data analytics pipeline built on the **Olist Brazilian E-Commerce dataset**, demonstrating production-grade ETL automation, SQL KPI modeling, and interactive business intelligence reporting across finance, supply chain, and operations domains.

---

## Architecture

```
Raw CSVs (9 tables)
      ↓
Python ETL (extract → transform → load)
      ↓
DuckDB Analytical Database
      ↓
SQL KPI Layer (7 business metrics)
      ↓
Power BI Dashboard (4 interactive pages)
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + Pandas | Load and validate raw CSVs |
| Transformation | Pandas | Clean, engineer features, build master table |
| Storage | DuckDB | Lightweight analytical SQL database |
| KPI Layer | SQL (DuckDB) | Pre-aggregated business metrics |
| Visualization | Power BI Desktop | Interactive 4-page dashboard |
| Orchestration | Apache Airflow (DAG defined) | Pipeline scheduling |
| Version Control | Git + GitHub | Code and documentation |

---

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle)

| Table | Rows | Description |
|---|---|---|
| olist_orders_dataset | 99,441 | Order status and timestamps |
| olist_order_items_dataset | 112,650 | Items, prices, freight per order |
| olist_order_payments_dataset | 103,886 | Payment type and installments |
| olist_order_reviews_dataset | 99,224 | Customer review scores |
| olist_customers_dataset | 99,441 | Customer location data |
| olist_sellers_dataset | 3,095 | Seller location data |
| olist_products_dataset | 32,951 | Product categories |
| olist_geolocation_dataset | 1,000,163 | Brazilian zip code coordinates |
| product_category_name_translation | 71 | Portuguese → English category names |

---

## Project Structure

```
Ecommerce-Operations-Analytics/
├── data/
│   ├── raw/               ← Olist CSVs (not tracked in git)
│   └── processed/         ← DuckDB database + KPI CSVs (not tracked in git)
├── etl/
│   ├── extract.py         ← Load all 9 raw CSVs into DataFrames
│   ├── transform.py       ← Clean, engineer features, build 25-column master table
│   ├── load.py            ← Write all tables into DuckDB
│   └── run_pipeline.py    ← Single entry point to run full ETL
├── sql/
│   └── run_kpis.py        ← Execute 7 KPI queries, export to CSV
├── notebooks/
│   └── eda.ipynb          ← Exploratory analysis with 4 saved charts
├── dashboards/
│   └── supply_chain.pbix  ← Power BI 4-page interactive dashboard
├── requirements.txt
└── README.md
```

---

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/Sanjana421/Ecommerce-Operations-Analytics.git
cd Ecommerce-Operations-Analytics
```

### 2. Set up virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Add the raw data
Download the Olist dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all 9 CSV files in `data/raw/`.

### 5. Run the ETL pipeline
```bash
python etl/run_pipeline.py
```

### 6. Generate KPI exports
```bash
python sql/run_kpis.py
```

### 7. Explore the data
```bash
jupyter notebook notebooks/eda.ipynb
```

### 8. Open the dashboard
Open `dashboards/supply_chain.pbix` in Power BI Desktop.

---

## ETL Pipeline

### Extract
Loads all 9 raw CSVs into Pandas DataFrames with validation logging.

### Transform
Key transformations applied to build the 25-column master analytical table:

| Transformation | Detail |
|---|---|
| Date parsing | 5 timestamp columns parsed to datetime |
| Delivery KPIs | `delivery_days_actual`, `delivery_days_estimated` engineered |
| Late flag | `is_late` binary flag (actual > estimated delivery date) |
| Time features | `order_month` (YYYY-MM), `order_year` for trend analysis |
| Payment aggregation | Multi-row payments aggregated to order level |
| Review deduplication | Latest review kept per order |
| Category translation | Portuguese → English via translation table join |
| Multi-table join | Orders + payments + reviews + customers + items + sellers |

### Load
All transformed tables written to a local DuckDB analytical database (`supply_chain.duckdb`).

---

## KPI Framework

| Domain | KPI | Description |
|---|---|---|
| **Finance** | Monthly Revenue | Total revenue per month (2016–2018) |
| **Finance** | Avg Order Value | Average payment per order by month |
| **Supply Chain** | On-Time Delivery Rate | % orders delivered before estimated date |
| **Supply Chain** | Avg Delivery Days | Mean actual delivery time per month |
| **Operations** | Late Rate % | % orders delivered after estimated date |
| **Operations** | Seller Scorecard | Per-seller revenue, delivery, review KPIs |
| **Customer** | Geographic Distribution | Revenue and orders by Brazilian state |

---

## Key Findings

| Metric | Value |
|---|---|
| Total Revenue | R$15,422,461 |
| Total Orders | 96,478 |
| Avg Order Value | R$159.86 |
| Avg Delivery Days | 12.1 days |
| On-Time Delivery Rate | **91.89%** |
| Late Delivery Rate | 8.11% |
| Avg Review Score | 4.16 / 5.0 |
| States Served | 27 |
| Active Sellers | 2,960 |

**Notable Insights:**
- Peak revenue month was **November 2017** — driven by Black Friday, showing 52% MoM revenue spike
- **São Paulo (SP)** accounts for ~40% of all platform revenue, confirming its dominance as Brazil's commercial hub
- **health_beauty** is the top revenue category at R$1.5M+, followed by watches_gifts and bed_bath_table
- **Credit card** dominates at 76.64% of all transactions — key insight for payment infrastructure planning
- Early months (2016) show high delivery variance due to low order volume — normalizes as platform scales

---

## Power BI Dashboard

4-page interactive dashboard with cross-filtering between visuals:

| Page | Visuals | Key Insight |
|---|---|---|
| Executive Overview | 4 KPI cards + revenue trend + order volume | Platform grew 10x from 2016 to 2018 |
| Delivery Performance | 3 KPI cards + on-time trend + delivery days | 91.89% on-time rate, improving over time |
| Product Analysis | Revenue by category + review scores | health_beauty leads both revenue and satisfaction |
| Seller & Geography | State revenue map + seller scorecard + top sellers | SP dominates, high variance in seller performance |

---

## Data Quality Notes

- Delivered orders only used for delivery KPIs (96,478 of 99,441 total orders)
- Duplicate reviews resolved by keeping the most recent review per order
- Multi-payment orders aggregated using sum (payment value) and mode (payment type)
- 1 order in 2016-09 with 54-day delivery excluded from trend analysis as statistical outlier
- Null values in review scores (<2%) handled via left join — orders without reviews retained

---

## Business Recommendations

- Monitor delivery delays as order volume scales → potential carrier bottleneck
- Invest in São Paulo logistics infrastructure → 40% revenue concentration
- Prioritize health & beauty inventory → highest demand category

---

## Future Improvements

- Add Apache Airflow scheduling for automated daily pipeline runs
- Integrate a date dimension table to enable full cross-filtering in Power BI
- Add cohort analysis to track customer retention over time
- Build a seller churn prediction model using scikit-learn
- Migrate DuckDB to cloud storage (S3 + Athena) for scalability

---

## Author

**Sanjana Reddy Nenturi**  
M.S. Intelligent Systems Engineering — Analytics | Indiana University Bloomington  
[LinkedIn](https://linkedin.com/in/sanjana-reddy-nenturi) | [GitHub](https://github.com/Sanjana421)
