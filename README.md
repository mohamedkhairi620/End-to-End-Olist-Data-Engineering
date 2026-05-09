# Big Data ETL Pipeline using Spark, Hadoop, Airflow, and Snowflake

## Project Overview

This project is an end-to-end Big Data ETL pipeline built using:

- Apache Spark
- Hadoop HDFS
- Apache Airflow
- Snowflake
- Docker

The pipeline ingests raw JSON data, processes it through multiple layers (Bronze, Silver, Gold), and loads the final analytical tables into Snowflake for reporting and analytics.

---

# Architecture

Raw JSON Files
↓
HDFS (Bronze Layer)
↓
Spark Transformations (Silver Layer)
↓
Star Schema Modeling (Gold Layer)
↓
Snowflake Data Warehouse
↓
Analytics & Reporting

---

# Technologies Used

- Python
- Apache Spark
- Hadoop HDFS
- Apache Airflow
- Snowflake
- Docker & Docker Compose
- PySpark

---

# Project Structure

```bash
project-root/
│
├── dags/
│   └── olist_data_pipeline.py
│
├── ingestion/
│   └── bronze_ingestion.py
│
├── transformation/
│   ├── feature_engineering.py
│   └── silver_to_gold.py
│
├── loading/
│   └── load_to_snowflake.py
│
├── notebooks/
│
├── jars/
│   ├── spark-snowflake_2.12-2.11.0.jar
│   └── snowflake-jdbc-3.13.33.jar
│
├── data/
│
├── docker-compose.yml
│
└── README.md
