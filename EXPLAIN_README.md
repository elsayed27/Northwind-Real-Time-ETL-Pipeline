# Northwind Real-Time ETL Pipeline — Full Project Explanation

## Project Overview

This project implements a complete end-to-end Data Engineering pipeline using:

- Python
- PySpark
- Hadoop HDFS
- Apache Airflow
- Docker
- Snowflake

The project simulates real-time Northwind transactions, processes the data through Bronze, Silver, and Gold layers, then loads the final analytical tables into Snowflake.

---

# Project Architecture

```text
CSV Files
   ↓
simulateRealWorldData.ipynb
   ↓
Landing Zone (JSON batches)
   ↓
e.py
   ↓
Bronze Layer (HDFS)
   ↓
validation.py
   ↓
cleaning.py
   ↓
Silver Layer (HDFS)
   ↓
s_validation.py
   ↓
t.py
   ↓
Gold Star Schema (HDFS)
   ↓
l.py
   ↓
Snowflake Data Warehouse
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| PySpark | Distributed data processing |
| Hadoop HDFS | Distributed storage |
| Airflow | Pipeline orchestration |
| Docker | Containerized environment |
| Snowflake | Cloud Data Warehouse |
| Parquet | Optimized storage format |

---

# Final Result

The project demonstrates:

- Real-time data simulation
- ETL pipeline design
- Bronze/Silver/Gold architecture
- Data validation and cleaning
- Star Schema modeling
- Distributed processing using Spark
- Workflow orchestration using Airflow
- Cloud warehouse loading using Snowflake
