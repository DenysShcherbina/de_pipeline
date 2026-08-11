# Cloud-Native K8s Spark ELT Pipeline & Data Mart Architecture

## 📌 Overview
This repository contains a production-grade, cloud-native ELT pipeline that automates data ingestion, distributed batch processing, and high-performance data warehousing. The project implements a modern **Data Lakehouse** pattern by decoupling storage (AWS S3) and cloud-native compute (Apache Spark on Kubernetes), exposing prepared data to downstream analytics via federated queries in Greenplum.

## 🏗️ Architecture & Data Flow
The orchestration is entirely managed by Apache Airflow. The process flow is detailed below:

* Ingestion Step: Raw unstructured data arrives in the landing zone of an AWS S3 bucket.
* Orchestration Step: Apache Airflow dynamic DAG triggers the system using programmatic configurations.
* Compute Step: SparkKubernetesOperator provisions temporary Spark Driver and Executor pods inside a Kubernetes (k8s) cluster using custom declarative YAML manifests.
* Processing Step: PySpark jobs perform schema validation, parsing, heavy cleaning, deduplication, and write optimized Parquet files back to the processed layer in S3.
* Monitoring Step: SparkKubernetesSensor dynamically tracks pod lifecycle states, freeing up Airflow worker slots while the distributed job executes.
* Loading & Serving Step: SQLExecuteQueryOperator triggers a parallel data load into Greenplum. Segment nodes of the MPP cluster execute federated reads directly from S3 using Greenplum PXF (Platform Extension Framework) external tables, bypassing the Master node bottleneck to build highly optimized analytical Data Marts.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow (SparkKubernetesOperator, SparkKubernetesSensor, SQLExecuteQueryOperator)
* **Compute Infrastructure:** Kubernetes (k8s), Apache Spark (Spark Operator, Spark History Server), Docker
* **Distributed Engine:** PySpark (Spark SQL, DataFrame API)
* **Cloud Storage:** AWS S3 (S3a Filesystem Connector)
* **Analytical Massively Parallel Database (MPP):** Greenplum DB
* **Federated Data Ingestion:** Greenplum PXF 
* **Configuration:** YAML Declarative Manifests

## 🚀 Key Implementation & Optimization Features
1. **Cloud-Native Resource Allocation:** Utilized custom Kubernetes custom resource definitions (CRD) via YAML to specify compute environments. Configured isolated namespace resources, explicit node selectors, and memory settings.
2. **Advanced Memory Management:** Mitigated Kubernetes `OOMKilled` (Exit Code 137) errors by balancing JVM limits (`spark.executor.memory`) and configuring container overhead thresholds (`spark.kubernetes.memoryOverhead`) to adapt to heavy Spark data mutations.
3. **Efficient Event-Driven Orchestration:** Implemented a smart polling structure using asynchronous scheduling modes in Airflow, drastically cutting infrastructure utilization compared to classic blocking threads.
4. **Parallelized MPP Storage Loads:** Optimized relational DWH data movements by eliminating single-threaded JDBC client patterns. PXF enables concurrent multi-node segment transfers directly from remote cloud object structures.
