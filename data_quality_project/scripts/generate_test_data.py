import pandas as pd
import numpy as np
import sqlalchemy as sa
import os
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta
from urllib.parse import quote_plus

load_dotenv()

def generate_test_data():
    """Generate test data without duplicate IDs"""
    # Create connection
    db_user = os.getenv('SOURCE_DB_USER')
    db_pass = os.getenv('SOURCE_DB_PASSWORD')
    db_host = os.getenv('SOURCE_DB_HOST', 'localhost')
    db_port = os.getenv('SOURCE_DB_PORT', '5432')
    db_name = os.getenv('SOURCE_DB_NAME', 'source_database')

    # Encode credentials properly
    db_user_quoted = quote_plus(db_user)
    db_pass_quoted = quote_plus(db_pass)

    conn_str = f"postgresql://{db_user_quoted}:{db_pass_quoted}@{db_host}:{db_port}/{db_name}"
    engine = sa.create_engine(conn_str)
    
    # Fetch existing customer IDs
    with engine.connect() as conn:
        existing_customer_ids = pd.read_sql("SELECT customer_id FROM customers", conn)['customer_id'].tolist() if engine.dialect.has_table(conn, "customers") else []
        existing_order_ids = pd.read_sql("SELECT order_id FROM orders", conn)['order_id'].tolist() if engine.dialect.has_table(conn, "orders") else []

    # Set of used IDs to avoid duplicates
    used_order_ids = set(existing_order_ids)
    used_customer_ids = set(existing_customer_ids)

    # Generate new customers if needed
    num_new_customers = max(0, 1000 - len(existing_customer_ids))  # Ensure 1000 total customers
    new_customers = []
    
    for i in range(len(existing_customer_ids) + 1, len(existing_customer_ids) + num_new_customers + 1):
        email = None if random.random() < 0.05 else f"customer{i}@example.com"
        phone = None if random.random() < 0.07 else f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

        new_customers.append({
            'customer_id': i,
            'name': f"Customer {i}",
            'email': email,
            'phone': phone,
            'registration_date': datetime.now() - timedelta(days=random.randint(1, 1000))
        })
        used_customer_ids.add(i)

    # Generate new orders
    num_new_orders = 5000
    new_orders = []
    
    for _ in range(num_new_orders):
        order_id = random.randint(1, 10000)
        while order_id in used_order_ids:  # Ensure unique order_id
            order_id = random.randint(1, 10000)
        used_order_ids.add(order_id)

        customer_id = random.choice(list(used_customer_ids))  # Pick existing customer_id

        order_date = None if random.random() < 0.005 else datetime.now() - timedelta(days=random.randint(1, 365))

        new_orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date,
            'amount': round(random.uniform(10.0, 500.0), 2)
        })

    # Convert to DataFrame
    customers_df = pd.DataFrame(new_customers)
    orders_df = pd.DataFrame(new_orders)

    # Insert new data without replacing existing records
    if not customers_df.empty:
        customers_df.to_sql('customers', engine, if_exists='append', index=False)

    if not orders_df.empty:
        orders_df.to_sql('orders', engine, if_exists='append', index=False)
    
    print(f"Added {len(new_customers)} customers and {len(new_orders)} new orders without duplicates.")

if __name__ == "__main__":
    generate_test_data()
