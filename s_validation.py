import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("NorthwindSilverValidation") \
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

SILVER_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/silver/"

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

REQUIRED_COLUMNS = {
    "orders": ["OrderID", "CustomerID", "EmployeeID", "OrderDate", "ShipVia"],
    "order_details": ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"],
    "customers": ["CustomerID", "CompanyName"],
    "employees": ["EmployeeID"],
    "products": ["ProductID", "ProductName", "SupplierID", "CategoryID"],
    "categories": ["CategoryID", "CategoryName"],
    "suppliers": ["SupplierID", "CompanyName"],
    "shippers": ["ShipperID", "CompanyName"],
}


def read_silver(table_name):
    return spark.read.parquet(f"{SILVER_BASE_PATH}{TABLES[table_name]}")


def count_nulls(df, cols):
    total = 0

    for col_name in cols:
        if col_name in df.columns:
            total += df.filter(
                F.col(col_name).isNull() |
                (F.trim(F.col(col_name).cast("string")) == "")
            ).count()

    return total


def count_duplicates(df, keys):
    return df.groupBy(keys).count().filter(F.col("count") > 1).count()


def count_fully_null_business_rows(df):
    metadata_cols = ["ingestion_time", "source_file", "bronze_ingestion_time"]

    business_cols = [
        col_name for col_name in df.columns
        if col_name not in metadata_cols
    ]

    condition = None

    for col_name in business_cols:
        col_condition = (
            F.col(col_name).isNotNull() &
            (F.trim(F.col(col_name).cast("string")) != "")
        )

        condition = col_condition if condition is None else condition | col_condition

    if condition is None:
        return 0

    return df.filter(~condition).count()


try:
    print("=" * 80)
    print("STARTING NORTHWIND SILVER VALIDATION")
    print("=" * 80)

    total_errors = 0

    for table_name in TABLES:
        df = read_silver(table_name)

        rows = df.count()
        nulls = count_nulls(df, REQUIRED_COLUMNS[table_name])
        duplicates = count_duplicates(df, BUSINESS_KEYS[table_name])
        fully_null_rows = count_fully_null_business_rows(df)
        empty_table_error = 1 if rows == 0 else 0

        table_errors = nulls + duplicates + fully_null_rows + empty_table_error
        total_errors += table_errors

        print(f"\nTABLE: {table_name}")
        print(f"Rows: {rows}")

        if rows == 0:
            print("Empty table: 0 rows")

        print(f"Required nulls: {nulls}")
        print(f"Fully null business rows: {fully_null_rows}")
        print(f"Duplicate business keys: {duplicates}")

        if table_errors == 0:
            print("Status: CLEAN")
        else:
            print("Status: NEEDS REVIEW")

    print("\n" + "=" * 80)

    if total_errors == 0:
        print("SILVER VALIDATION PASSED: Cleaning checks passed.")
    else:
        print("SILVER VALIDATION FAILED: Some cleaning issues still exist.")

    print("=" * 80)

except Exception as e:
    print(f"Silver validation failed: {e}")
    raise e

finally:
    spark.stop()
