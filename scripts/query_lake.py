import duckdb
import os

# Define path to Silver Lake
SILVER_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'silver')

def query_lake():
    print("🦆 Connecting to Silver Lake (Parquet files) via DuckDB...")
    
    # Query multiple parquet files at once
    query = f"""
    SELECT 
        o.order_status,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(p.payment_value) as total_revenue
    FROM read_parquet('{SILVER_DIR}/orders.parquet') o
    JOIN read_parquet('{SILVER_DIR}/order_payments.parquet') p ON o.order_id = p.order_id
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    
    try:
        result = duckdb.query(query).df()
        print("\n📊 Analytical Query Result (from Parquet):")
        print(result)
    except Exception as e:
        print(f"❌ Error querying the lake: {e}")

if __name__ == "__main__":
    query_lake()
