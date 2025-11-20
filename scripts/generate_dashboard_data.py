#!/usr/bin/env python3
"""
Generate static JSON data files from Athena queries for the CloudFront dashboard
Run this script to refresh the dashboard data
"""

import json
import boto3
import time
from datetime import datetime

# Initialize Athena client
athena = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

# Configuration
DATABASE = 'ecommerce_db'
OUTPUT_LOCATION = 's3://ogs-ecommerce-analytics-2025/athena-results/'
QUERIES = {
    'kpis': """
        SELECT
            COUNT(DISTINCT invoiceno) AS total_orders,
            COUNT(DISTINCT customerid) AS unique_customers,
            SUM(total_price) AS total_revenue,
            AVG(total_price) AS avg_order_value
        FROM online_retail_cleaned
    """,
    
    'monthly_sales': """
        SELECT
            date_trunc('month', invoicedate) AS sales_month,
            SUM(total_price) AS monthly_revenue,
            COUNT(DISTINCT invoiceno) AS monthly_orders
        FROM online_retail_cleaned
        GROUP BY 1
        ORDER BY 1
    """,
    
    'monthly_revenue': """
        SELECT 
            DATE_FORMAT(invoicedate, '%Y-%m') as month,
            SUM(total_price) as revenue
        FROM online_retail_cleaned
        GROUP BY DATE_FORMAT(invoicedate, '%Y-%m')
        ORDER BY month
    """,
    
    'monthly_revenue_by_country': """
        SELECT 
            country,
            DATE_FORMAT(invoicedate, '%Y-%m') as month,
            SUM(total_price) as revenue
        FROM online_retail_cleaned
        GROUP BY country, DATE_FORMAT(invoicedate, '%Y-%m')
        ORDER BY country, month
    """,
    
    'country_revenue': """
        SELECT 
            country,
            SUM(total_price) as revenue,
            COUNT(DISTINCT invoiceno) as orders,
            COUNT(DISTINCT customerid) as customers,
            CASE
                WHEN COUNT(DISTINCT invoiceno) = 0 THEN 0
                ELSE SUM(total_price) / COUNT(DISTINCT invoiceno)
            END as avg_order_value
        FROM online_retail_cleaned
        GROUP BY country
        HAVING SUM(total_price) > 0
        ORDER BY revenue DESC
    """,
    
    'top_products': """
        SELECT 
            country,
            description,
            SUM(quantity) AS total_quantity_sold,
            SUM(total_price) as revenue
        FROM online_retail_cleaned
        GROUP BY country, description
        ORDER BY revenue DESC
    """,
    
    'rfm_analysis': """
        SELECT 
            country,
            customerid,
            CAST(date_diff('day', MAX(invoicedate), current_date) AS INTEGER) as recency,
            COUNT(DISTINCT invoiceno) as frequency,
            SUM(total_price) as monetary
        FROM online_retail_cleaned
        GROUP BY country, customerid
        HAVING COUNT(DISTINCT invoiceno) > 1
    """,
    
    'cohort_analysis': """
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
            COUNT(DISTINCT customerid) AS active_customers
        FROM joined
        GROUP BY country, cohort_month, month_index
        ORDER BY country, cohort_month, month_index
    """,
    
    'market_basket': """
        SELECT
            t1.description AS product_a,
            t2.description AS product_b,
            COUNT(*) AS times_purchased_together
        FROM online_retail_cleaned t1
        JOIN online_retail_cleaned t2
            ON t1.invoiceno = t2.invoiceno
        WHERE t1.stockcode < t2.stockcode
          AND t1.description NOT LIKE '%POSTAGE%'
          AND t2.description NOT LIKE '%POSTAGE%'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 15
    """
}

def execute_athena_query(query, query_name):
    """Execute Athena query and return results"""
    print(f"Executing query: {query_name}...")
    
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")
        
        # Wait for query to complete
        while True:
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            print(f"Query status: {status}...")
            time.sleep(2)
        
        if status != 'SUCCEEDED':
            error_msg = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            print(f"Query failed: {error_msg}")
            return None
        
        print(f"Query succeeded! Fetching results...")
        
        # Get query results
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse results
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row in results['ResultSet']['Rows'][1:]:  # Skip header row
            row_data = {}
            for i, col in enumerate(columns):
                value = row['Data'][i].get('VarCharValue', None)
                # Try to convert to number if possible
                if value is not None:
                    try:
                        if '.' in value:
                            row_data[col.lower()] = float(value)
                        else:
                            row_data[col.lower()] = int(value)
                    except ValueError:
                        row_data[col.lower()] = value
                else:
                    row_data[col.lower()] = None
            rows.append(row_data)
        
        print(f"Retrieved {len(rows)} rows")
        return rows
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def generate_dashboard_data():
    """Generate all dashboard data"""
    print("=" * 60)
    print("DASHBOARD DATA GENERATOR")
    print("=" * 60)
    print()
    
    dashboard_data = {}
    
    for query_name, query in QUERIES.items():
        results = execute_athena_query(query, query_name)
        if results is not None:
            dashboard_data[query_name] = results
        print()
    
    # Save to JSON file
    output_file = 'dashboard-data.json'
    with open(output_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    
    print("=" * 60)
    print(f"✅ Dashboard data saved to {output_file}")
    print(f"📊 Total queries executed: {len(dashboard_data)}")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Create a 'data' folder in your dashboard directory")
    print("2. Move dashboard-data.json to the data folder")
    print("3. Upload the entire dashboard to S3:")
    print("   aws s3 sync . s3://ogs-ecommerce-dashboard-2025/ --exclude '.git/*'")
    print("4. Invalidate CloudFront cache:")
    print("   aws cloudfront create-invalidation --distribution-id <ID> --paths '/*'")

if __name__ == "__main__":
    generate_dashboard_data()