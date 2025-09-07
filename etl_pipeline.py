import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def run_etl(customers_fp, products_fp, orders_fp, out_dir):
    customers = pd.read_csv(customers_fp, dtype=str)
    products = pd.read_csv(products_fp, dtype=str)
    orders = pd.read_csv(orders_fp, dtype=str)
    customers = customers.drop_duplicates()
    customers['customer_id'] = customers['customer_id'].str.strip()
    customers['name'] = customers['name'].str.strip()
    customers['email'] = customers['email'].fillna("").astype(str).str.strip().str.lower()
    customers.loc[customers['email']=="", 'email'] = "unknown@example.com"
    customers['signup_date'] = pd.to_datetime(customers['signup_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    customers = customers[customers['customer_id'].notna() & (customers['customer_id']!="")]
    dim_customers = customers[['customer_id','name','email','city','signup_date']].copy()

    products = products.drop_duplicates()
    products['product_id'] = products['product_id'].str.strip()
    products['name'] = products['name'].str.strip()
    products['category'] = products['category'].fillna("unknown").astype(str).str.strip().str.lower()
    products['price'] = pd.to_numeric(products['price'].astype(str).str.strip().replace("", np.nan), errors='coerce').fillna(0.0)
    dim_products = products[['product_id','name','category','price']].copy()

    orders = orders.drop_duplicates()
    orders['order_id'] = orders['order_id'].astype(str).str.strip()
    orders['customer_id'] = orders['customer_id'].astype(str).str.strip()
    orders['product_id'] = orders['product_id'].astype(str).str.strip()
    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce').fillna(0).astype(int)
    orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    orders = orders[orders['order_id'].notna() & (orders['order_id']!="") & orders['customer_id'].notna() & (orders['customer_id']!="") & orders['order_date'].notna()]
    orders = orders.merge(dim_products[['product_id','price']], on='product_id', how='left')
    orders['price'] = orders['price'].fillna(0.0)
    orders['total_amount'] = orders['quantity'] * orders['price']
    orders['date'] = orders['order_date'].dt.strftime('%Y-%m-%d')
    orders['date_key'] = orders['order_date'].dt.strftime('%Y%m%d').astype(int)
    fact_orders = orders[['order_id','customer_id','product_id','date_key','quantity','price','total_amount','date']].copy()

    dim_date = fact_orders[['date_key','date']].drop_duplicates().copy()
    dim_date['date'] = pd.to_datetime(dim_date['date'])
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['weekday'] = dim_date['date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['weekday'].isin(['Saturday','Sunday'])
    dim_date = dim_date[['date_key','date','year','month','day','weekday','is_weekend']].copy()
    dim_date['date'] = dim_date['date'].dt.strftime('%Y-%m-%d')

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dim_customers.to_csv(out_dir/"dim_customers.csv", index=False)
    dim_products.to_csv(out_dir/"dim_products.csv", index=False)
    dim_date.to_csv(out_dir/"dim_date.csv", index=False)
    fact_orders.to_csv(out_dir/"fact_orders.csv", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_dir", default=".", help="Directory containing customers_raw.csv, products_raw.csv, orders_raw.csv")
    parser.add_argument("--out_dir", default=".", help="Output directory for transformed CSVs")
    args = parser.parse_args()
    run_etl(Path(args.raw_dir)/"customers_raw.csv", Path(args.raw_dir)/"products_raw.csv", Path(args.raw_dir)/"orders_raw.csv", args.out_dir)
