import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("NorthwindBronzeValidation") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.31.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BRONZE_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/"

TABLES = {
    "orders": "northwind_orders",
    "order_details": "northwind_order_details",
    "customers": "northwind_customers",
    "employees": "northwind_employees",
    "products": "northwind_products",
    "categories": "northwind_categories",
    "suppliers": "northwind_suppliers",
    "shippers": "northwind_shippers",
}

REQUIRED_COLUMNS = {
    # Bronze is raw, so orders still uses the original source column name: Customer key
    "orders": ["OrderID", "Customer key", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate", "ShipVia", "Freight"],
    "order_details": ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"],
    "customers": ["CustomerID", "CompanyName"],
    "employees": ["EmployeeID", "Region"],
    "products": ["ProductID", "ProductName", "SupplierID", "CategoryID"],
    "categories": ["CategoryID", "CategoryName", "Picture"],
    "suppliers": ["SupplierID", "CompanyName"],
    "shippers": ["ShipperID", "CompanyName"],
}

BUSINESS_KEYS = {
    "orders": ["OrderID"],
    "order_details": ["OrderID", "ProductID"],
    "customers": ["CustomerID"],
    "employees": ["EmployeeID"],
    "products": ["ProductID"],
    "categories": ["CategoryID"],
    "suppliers": ["SupplierID"],
    "shippers": ["ShipperID"],
}


def read_table(table_name):
    path = f"{BRONZE_BASE_PATH}{TABLES[table_name]}"
    print(f"\nReading Bronze table: {table_name}")
    print(f"Path: {path}")
    return spark.read.parquet(path)


def is_not_empty(col_name):
    return F.col(col_name).isNotNull() & (F.trim(F.col(col_name).cast("string")) != "")


def check_required_columns(df, table_name):
    print(f"\n--- Required Columns Check: {table_name} ---")

    missing_cols = [
        col for col in REQUIRED_COLUMNS[table_name]
        if col not in df.columns
    ]

    if missing_cols:
        print(f"Missing columns: {missing_cols}")
    else:
        print("All required columns exist.")


def check_empty_columns(df, table_name):
    print(f"\n--- Empty Columns Check: {table_name} ---")

    empty_cols = []

    for col_name in df.columns:
        non_empty_count = df.filter(is_not_empty(col_name)).count()

        if non_empty_count == 0:
            empty_cols.append(col_name)

    if empty_cols:
        print(f"Empty columns found: {empty_cols}")
    else:
        print("No fully empty columns found.")


def check_null_values(df, table_name):
    print(f"\n--- Null / Empty Values Check: {table_name} ---")

    for col_name in REQUIRED_COLUMNS[table_name]:
        if col_name in df.columns:
            null_count = df.filter(~is_not_empty(col_name)).count()

            if null_count > 0:
                print(f"❌ {col_name}: {null_count} null/empty values")
            else:
                print(f"✅ {col_name}: 0 null/empty values")


def check_fully_null_rows(df, table_name):
    print(f"\n--- Fully Null Business Rows Check: {table_name} ---")

    metadata_cols = ["ingestion_time", "source_file", "bronze_ingestion_time"]
    business_cols = [c for c in df.columns if c not in metadata_cols]

    condition = None

    for col_name in business_cols:
        col_condition = is_not_empty(col_name)
        condition = col_condition if condition is None else condition | col_condition

    null_rows = df.filter(~condition).count()

    if null_rows > 0:
        print(f"Fully null business rows found: {null_rows}")
    else:
        print("No fully null business rows found.")


def check_duplicate_business_keys(df, table_name):
    print(f"\n--- Duplicate Business Keys Check: {table_name} ---")

    keys = BUSINESS_KEYS[table_name]

    valid_key_condition = None

    for key in keys:
        key_condition = is_not_empty(key)
        valid_key_condition = key_condition if valid_key_condition is None else valid_key_condition & key_condition

    df_valid_keys = df.filter(valid_key_condition)

    duplicates_df = df_valid_keys.groupBy(keys).count().filter(F.col("count") > 1)
    duplicate_groups = duplicates_df.count()

    duplicate_rows = duplicates_df.agg(
        F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows")
    ).collect()[0]["duplicate_rows"]

    duplicate_rows = duplicate_rows if duplicate_rows is not None else 0

    if duplicate_groups > 0:
        print(f"Duplicate business key groups found: {duplicate_groups}")
        print(f"Duplicate extra rows found: {duplicate_rows}")
    else:
        print("No duplicate business keys found.")


try:
    print("=" * 80)
    print("STARTING NORTHWIND BRONZE VALIDATION")
    print("=" * 80)

    for table_name in TABLES.keys():
        df = read_table(table_name)

        print("\n" + "-" * 80)
        print(f"TABLE: {table_name}")
        print("-" * 80)

        print(f"Rows: {df.count()}")
        print(f"Columns: {df.columns}")

        check_required_columns(df, table_name)
        check_empty_columns(df, table_name)
        check_null_values(df, table_name)
        check_fully_null_rows(df, table_name)
        check_duplicate_business_keys(df, table_name)

    print("\nBronze validation finished successfully.")

except Exception as e:
    print(f"Validation failed: {e}")
    raise e

finally:
    spark.stop()
