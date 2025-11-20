# scripts/generate_data.py
import awswrangler as wr
import pandas as pd
import json

# --------------------------------------------------
# Configuration
# --------------------------------------------------
DB_NAME = "ecommerce_db"
S3_OUTPUT = "s3://ogs-ecommerce-analytics-2025/data-generation/"

print("--- Starting Data Generation for Web Dashboard ---")

def run_query(query_name, sql):
    """Runs Athena SQL and returns a list of dict rows."""
    print(f"Running query: {query_name}...")
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database=DB_NAME,
            s3_output=S3_OUTPUT,
            ctas_approach=False
        )
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"  ERROR running query {query_name}: {e}")
        return []


# --------------------------------------------------
# Queries (OPTION A enabled)
# Each dataset now includes country for proper filtering
# --------------------------------------------------
queries = {

    # ---------- Global KPIs ----------
    "kpis": """
        SELECT
            COUNT(DISTINCT invoiceno) AS total_orders,
            COUNT(DISTINCT customerid) AS unique_customers,
            SUM(total_price)          AS total_revenue,
            AVG(total_price)          AS avg_order_value
        FROM online_retail_cleaned
    """,

    # ---------- Global monthly sales ----------
    "monthly_sales": """
        SELECT
            date_trunc('month', invoicedate) AS sales_month,
            SUM(total_price)                 AS monthly_revenue,
            COUNT(DISTINCT invoiceno)        AS monthly_orders
        FROM online_retail_cleaned
        GROUP BY 1
        ORDER BY 1
    """,

    # ---------- Monthly revenue by country ----------
    "monthly_revenue_by_country": """
        SELECT
            country,
            date_trunc('month', invoicedate) AS month,
            SUM(total_price)                 AS revenue
        FROM online_retail_cleaned
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,

    # ---------- Top products per country ----------
    "top_products": """
        SELECT
            country,
            description,
            SUM(quantity)     AS total_quantity_sold,
            SUM(total_price)  AS revenue
        FROM online_retail_cleaned
        GROUP BY country, description
        ORDER BY revenue DESC
    """,

    # ---------- RFM analysis per country ----------
    "rfm_analysis": """
        SELECT 
            country,
            customerid,
            CAST(date_diff('day', MAX(invoicedate), current_date) AS INTEGER) AS recency,
            COUNT(DISTINCT invoiceno) AS frequency,
            SUM(total_price)          AS monetary
        FROM online_retail_cleaned
        GROUP BY country, customerid
        HAVING COUNT(DISTINCT invoiceno) > 1
    """,

    # ---------- Market basket (unchanged) ----------
    "market_basket": """
        SELECT
            t1.description AS product_a,
            t2.description AS product_b,
            COUNT(*)       AS times_purchased_together
        FROM online_retail_cleaned t1
        JOIN online_retail_cleaned t2
            ON t1.invoiceno = t2.invoiceno
        WHERE t1.stockcode < t2.stockcode
          AND t1.description NOT LIKE '%POSTAGE%'
          AND t2.description NOT LIKE '%POSTAGE%'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 15
    """,

    # ---------- Cohort analysis (FIXED VERSION) ----------
    "cohort_analysis": """
        WITH first_order AS (
            SELECT 
                customerid,
                country,
                date_trunc('month', MIN(invoicedate)) AS cohort_month
            FROM online_retail_cleaned
            GROUP BY customerid, country
        ),

        orders AS (
            SELECT 
                customerid,
                country,
                date_trunc('month', invoicedate) AS order_month
            FROM online_retail_cleaned
            GROUP BY customerid, country, date_trunc('month', invoicedate)
        ),

        joined AS (
            SELECT
                o.customerid,
                o.country,
                f.cohort_month,
                o.order_month,
                date_diff('month', f.cohort_month, o.order_month) AS month_offset
            FROM orders o
            JOIN first_order f 
                ON o.customerid = f.customerid
               AND o.country = f.country
        )

        SELECT
            country,
            cohort_month,
            CAST(month_offset AS INTEGER) AS month_index,
            COUNT(DISTINCT customerid)    AS active_customers
        FROM joined
        GROUP BY country, cohort_month, month_index
        ORDER BY country, cohort_month, month_index
    """,

    # ---------- Revenue per country ----------
    "country_revenue": """
        SELECT
            country,
            SUM(total_price)                       AS revenue,
            COUNT(DISTINCT invoiceno)              AS orders,
            COUNT(DISTINCT customerid)             AS customers,
            CASE
                WHEN COUNT(DISTINCT invoiceno) = 0 THEN 0
                ELSE SUM(total_price) / COUNT(DISTINCT invoiceno)
            END                                    AS avg_order_value
        FROM online_retail_cleaned
        GROUP BY country
        HAVING SUM(total_price) > 0
        ORDER BY revenue DESC
    """
}


# --------------------------------------------------
# Execute all queries
# --------------------------------------------------
final_data = {}
for name, sql in queries.items():
    final_data[name] = run_query(name, sql)

# --------------------------------------------------
# Save to JSON file
# --------------------------------------------------
output_path = "dashboard/data/dashboard-data.json"
with open(output_path, "w") as f:
    json.dump(final_data, f, indent=2, default=str)

print(f"\n✅ Successfully generated and saved data to {output_path}")
print("🚀 Ready to upload to S3!")
