import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("NorthwindBronzeToSilverCleaning") \
    .config("spark.ui.enabled", "false") \
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


def read_bronze(table_name):
    return spark.read.parquet(f"{BRONZE_BASE_PATH}{TABLES[table_name]}")


def write_silver(df, table_name):
    output_path = f"{SILVER_BASE_PATH}{TABLES[table_name]}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"{table_name} written to Silver: {output_path}")


def drop_fully_null_rows(df):
    condition = None

    for col_name in df.columns:
        col_condition = (
            F.col(col_name).isNotNull() &
            (F.trim(F.col(col_name).cast("string")) != "")
        )

        condition = col_condition if condition is None else condition | col_condition

    return df.filter(condition)


def drop_empty_columns(df):
    empty_cols = []

    for col_name in df.columns:
        non_empty_count = df.filter(
            F.col(col_name).isNotNull() &
            (F.trim(F.col(col_name).cast("string")) != "")
        ).count()

        if non_empty_count == 0:
            empty_cols.append(col_name)

    if empty_cols:
        print(f"Dropping empty columns: {empty_cols}")
        df = df.drop(*empty_cols)

    return df


def clean_orders(df):
    print("\nCleaning orders...")
    before = df.count()

    df = drop_fully_null_rows(df)

    # Bronze is raw, so the source customer column name is "Customer key".
    # Silver standardizes it to "CustomerID".
    if "Customer key" in df.columns:
        df = df.withColumnRenamed("Customer key", "CustomerID")

    df = df.withColumn("OrderID", F.col("OrderID").cast("int")) \
           .withColumn("CustomerID", F.trim(F.col("CustomerID"))) \
           .withColumn("EmployeeID", F.col("EmployeeID").cast("int")) \
           .withColumn("ShipVia", F.col("ShipVia").cast("int")) \
           .withColumn("Freight", F.col("Freight").cast("double")) \
           .withColumn("OrderDate", F.to_timestamp("OrderDate", "M/d/yyyy H:mm")) \
           .withColumn("RequiredDate", F.to_timestamp("RequiredDate", "M/d/yyyy H:mm")) \
           .withColumn("ShippedDate", F.to_timestamp("ShippedDate", "M/d/yyyy H:mm"))

    df = df.filter(
        F.col("OrderID").isNotNull() &
        F.col("CustomerID").isNotNull()
    )

    df = df.dropDuplicates(["OrderID"])

    after = df.count()

    print(f"Orders rows before: {before}")
    print(f"Orders rows after : {after}")

    return df


def clean_order_details(df):
    print("\nCleaning order_details...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("OrderID", F.col("OrderID").cast("int")) \
           .withColumn("ProductID", F.col("ProductID").cast("int")) \
           .withColumn("UnitPrice", F.col("UnitPrice").cast("double")) \
           .withColumn("Quantity", F.col("Quantity").cast("int")) \
           .withColumn("Discount", F.col("Discount").cast("double"))

    df = df.filter(
        F.col("OrderID").isNotNull() &
        F.col("ProductID").isNotNull()
    )

    df = df.dropDuplicates(["OrderID", "ProductID"])

    after = df.count()

    print(f"OrderDetails rows before: {before}")
    print(f"OrderDetails rows after : {after}")

    return df


def clean_customers(df):
    print("\nCleaning customers...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("CustomerID", F.trim(F.col("CustomerID"))) \
           .withColumn("CompanyName", F.trim(F.col("CompanyName"))) \
           .withColumn(
                "Region",
                F.when(
                    F.col("Region").isNull() |
                    (F.trim(F.col("Region")) == ""),
                    "Unknown"
                ).otherwise(F.col("Region"))
           )

    df = df.filter(
        F.col("CustomerID").isNotNull() &
        (F.trim(F.col("CustomerID")) != "")
    )

    df = df.dropDuplicates(["CustomerID"])

    after = df.count()

    print(f"Customers rows before: {before}")
    print(f"Customers rows after : {after}")

    return df


def clean_employees(df):
    print("\nCleaning employees...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("EmployeeID", F.col("EmployeeID").cast("int")) \
           .withColumn("ReportsTo", F.col("ReportsTo").cast("int")) \
           .withColumn("BirthDate", F.to_timestamp("BirthDate", "M/d/yyyy H:mm")) \
           .withColumn("HireDate", F.to_timestamp("HireDate", "M/d/yyyy H:mm")) \
           .withColumn(
                "Region",
                F.when(
                    F.col("Region").isNull() |
                    (F.trim(F.col("Region")) == ""),
                    "Unknown"
                ).otherwise(F.col("Region"))
           )

    df = df.filter(F.col("EmployeeID").isNotNull())
    df = df.dropDuplicates(["EmployeeID"])

    after = df.count()

    print(f"Employees rows before: {before}")
    print(f"Employees rows after : {after}")

    return df


def clean_products(df):
    print("\nCleaning products...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("ProductID", F.col("ProductID").cast("int")) \
           .withColumn("SupplierID", F.col("SupplierID").cast("int")) \
           .withColumn("CategoryID", F.col("CategoryID").cast("int")) \
           .withColumn("UnitPrice", F.col("UnitPrice").cast("double")) \
           .withColumn("UnitsInStock", F.col("UnitsInStock").cast("int")) \
           .withColumn("UnitsOnOrder", F.col("UnitsOnOrder").cast("int")) \
           .withColumn("ReorderLevel", F.col("ReorderLevel").cast("int"))

    df = df.filter(F.col("ProductID").isNotNull())
    df = df.dropDuplicates(["ProductID"])

    after = df.count()

    print(f"Products rows before: {before}")
    print(f"Products rows after : {after}")

    return df


def clean_categories(df):
    print("\nCleaning categories...")
    before = df.count()

    df = drop_fully_null_rows(df)

    if "Picture" in df.columns:
        print("Dropping Picture column because it is empty/not needed for analytics.")
        df = df.drop("Picture")

    df = drop_empty_columns(df)

    df = df.withColumn("CategoryID", F.col("CategoryID").cast("int")) \
           .withColumn("CategoryName", F.trim(F.col("CategoryName")))

    df = df.filter(F.col("CategoryID").isNotNull())
    df = df.dropDuplicates(["CategoryID"])

    after = df.count()

    print(f"Categories rows before: {before}")
    print(f"Categories rows after : {after}")

    return df


def clean_suppliers(df):
    print("\nCleaning suppliers...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("SupplierID", F.col("SupplierID").cast("int")) \
           .withColumn("CompanyName", F.trim(F.col("CompanyName"))) \
           .withColumn(
                "Region",
                F.when(
                    F.col("Region").isNull() |
                    (F.trim(F.col("Region")) == ""),
                    "Unknown"
                ).otherwise(F.col("Region"))
           )

    df = df.filter(F.col("SupplierID").isNotNull())
    df = df.dropDuplicates(["SupplierID"])

    after = df.count()

    print(f"Suppliers rows before: {before}")
    print(f"Suppliers rows after : {after}")

    return df


def clean_shippers(df):
    print("\nCleaning shippers...")
    before = df.count()

    df = drop_fully_null_rows(df)

    df = df.withColumn("ShipperID", F.col("ShipperID").cast("int")) \
           .withColumn("CompanyName", F.trim(F.col("CompanyName")))

    df = df.filter(F.col("ShipperID").isNotNull())
    df = df.dropDuplicates(["ShipperID"])

    after = df.count()

    print(f"Shippers rows before: {before}")
    print(f"Shippers rows after : {after}")

    return df


try:
    print("=" * 80)
    print("STARTING NORTHWIND BRONZE TO SILVER CLEANING")
    print("=" * 80)

    write_silver(clean_orders(read_bronze("orders")), "orders")
    write_silver(clean_order_details(read_bronze("order_details")), "order_details")
    write_silver(clean_customers(read_bronze("customers")), "customers")
    write_silver(clean_employees(read_bronze("employees")), "employees")
    write_silver(clean_products(read_bronze("products")), "products")
    write_silver(clean_categories(read_bronze("categories")), "categories")
    write_silver(clean_suppliers(read_bronze("suppliers")), "suppliers")
    write_silver(clean_shippers(read_bronze("shippers")), "shippers")

    print("\nSilver Layer created successfully.")

except Exception as e:
    print(f"Cleaning failed: {e}")
    raise

finally:
    spark.stop()
