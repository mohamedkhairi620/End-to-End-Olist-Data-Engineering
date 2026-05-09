import pandas as pd
import time
import os
from datetime import datetime

# ============================================
# Paths
# ============================================

RAW_PATH = "/home/jovyan/work/data/raw"
STREAMING_PATH = "/home/jovyan/work/data/streaming"

os.makedirs(STREAMING_PATH, exist_ok=True)

# ============================================
# Tables & Required Columns
# ============================================

TABLES = {
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ],

    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],

    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ],

    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
}

# ============================================
# Load CSV Files
# ============================================

print("=" * 60)
print("LOADING DATASETS")
print("=" * 60)

dataframes = {}

for table, cols in TABLES.items():

    file_path = f"{RAW_PATH}/olist_{table}_dataset.csv"

    df = pd.read_csv(file_path)

    df.columns = df.columns.str.strip()

    available_cols = [c for c in cols if c in df.columns]

    dataframes[table] = df[available_cols].copy()

    print(f"Loaded {table}: {len(dataframes[table])} rows")

# ============================================
# Streaming Simulation
# ============================================

print("\n" + "=" * 60)
print("STARTING STREAMING SIMULATION")
print("=" * 60)

batch_size = 500

for table, df in dataframes.items():

    for i in range(0, len(df), batch_size):

        chunk = df.iloc[i:i + batch_size].copy()

        chunk["event_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        chunk["source_table"] = table

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_name = f"{STREAMING_PATH}/{table}_{timestamp}_{i}.json"

        chunk.to_json(
            file_name,
            orient="records",
            lines=True
        )

        print(
            f"[{table}] Batch {i // batch_size + 1} "
            f"-> {len(chunk)} records"
        )

        time.sleep(1)

print("\nSTREAMING COMPLETED SUCCESSFULLY")