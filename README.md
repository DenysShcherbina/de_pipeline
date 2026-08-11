# Automated Big Data ELT Pipeline & Data Mart Architecture (S3 ➔ PySpark ➔ Greenplum)

## 📌 Overview
This project implements a scalable and fault-tolerant ELT pipeline that automates the ingestion, distributed processing, and analytical storage of large-scale datasets. The architecture decouples storage and compute, implementing a modern **Data Lakehouse** approach where raw data is managed in cloud storage, processed via Spark, and exposed to analytics via federated queries in Greenplum.

## 🏗️ Architecture & Data Flow
```text
[ Raw Data in AWS S3 ] 
       │
       ▼ (Orchestrated by Airflow DAG)
[ PySpark Ingestion & Batch Processing ] ➔ Cleansed & Aggregated Data ➔ [ Processed S3 Bucket ]
                                                                                   │
[ Airflow Sensor ] ➔ Waits for Spark Job Completion ───────────────────────────────┘
       │
       ▼ (Triggered)
[ Greenplum PXF External Tables ] ➔ [ Analytical Data Marts ]
```

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow (DAGs, Custom Sensors, Task Groups)
* **Compute Engine:** PySpark (Spark SQL, DataFrame API)
* **Storage / Lakehouse:** AWS S3 (S3a connector)
* **Analytical DWH:** Greenplum DB
* **Data Integration:** Greenplum PXF (Platform Extension Framework)
* **Environment:** Docker, Docker-Compose

## 🚀 Key Implementation Features
1. **Robust Ingestion & Processing:** PySpark jobs read raw JSON/Parquet files from AWS S3, perform schema enforcement, data deduplication, handling of missing values, and calculate high-level business metrics.
2. **Smart Orchestration:** Airflow utilizes precise **Sensors** to decouple the processing stage from data availability, ensuring downstream tasks only execute once upstream batch windows are completely closed and validated.
3. **High-Performance Data Marts:** Instead of heavy JDBC inserts, the pipeline leverages **Greenplum PXF** to query the processed S3 data directly via external tables, significantly reducing analytical query latency and data movement overhead.
