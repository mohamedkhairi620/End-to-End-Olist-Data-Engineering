# Big Data ETL Pipeline using Spark, Hadoop, Airflow, and Snowflake

## Overview

This project is a complete Big Data ETL pipeline designed to process large-scale e-commerce data using modern data engineering tools and technologies.

The pipeline follows a multi-layer architecture (Bronze, Silver, Gold) and automates the entire workflow using Apache Airflow. Data is processed with Apache Spark, stored in Hadoop HDFS, and finally loaded into Snowflake for analytics and reporting.

The project demonstrates practical implementation of:

- Distributed data processing
- Data lake architecture
- ETL orchestration
- Cloud data warehousing
- Scalable big data systems

---

# System Architecture

```text
Raw JSON Data
       │
       ▼
Bronze Layer (HDFS)
       │
       ▼
Silver Layer (Feature Engineering)
       │
       ▼
Gold Layer (Star Schema)
       │
       ▼
Snowflake Data Warehouse
       │
       ▼
Analytics & BI
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| Apache Spark | Distributed data processing |
| Hadoop HDFS | Distributed storage |
| Apache Airflow | Workflow orchestration |
| Snowflake | Cloud data warehouse |
| Docker | Containerization |
| PySpark | Spark API for Python |
| YARN | Cluster resource management |

---

# Project Structure

```bash
Final_Project_bigData/
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
├── logs/
│
├── plugins/
│
├── config/
│
├── docker-compose.yml
│
└── README.md
```

---

# Data Pipeline Layers

## 1. Bronze Layer

The Bronze layer stores raw ingested data directly from JSON files into Hadoop HDFS.

### Responsibilities

- Raw data ingestion
- Schema preservation
- Initial storage in HDFS
- Immutable raw storage

### Script

```bash
ingestion/bronze_ingestion.py
```

---

## 2. Silver Layer

The Silver layer performs data cleaning and feature engineering.

### Responsibilities

- Handle missing values
- Data type conversion
- Feature engineering
- Data standardization
- Data validation

### Script

```bash
transformation/feature_engineering.py
```

---

## 3. Gold Layer

The Gold layer transforms the cleaned data into analytical tables using Star Schema modeling.

### Responsibilities

- Build fact tables
- Build dimension tables
- Create analytical models
- Optimize for BI and reporting

### Script

```bash
transformation/silver_to_gold.py
```

---

# Snowflake Integration

The final Gold layer tables are loaded into Snowflake for cloud analytics and reporting.

---

# Snowflake Warehouse Setup

```sql
CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;
```

---

# Create Database and Schema

```sql
CREATE DATABASE IF NOT EXISTS OLIST_DB;

CREATE SCHEMA IF NOT EXISTS OLIST_DB.GOLD_LAYER;
```

---

# Snowflake Tables

## Dimension Tables

| Table | Description |
|---|---|
| DIM_CUSTOMER | Customer information |
| DIM_PRODUCT | Product details |
| DIM_TIME | Date and time dimension |

---

## Fact Tables

| Table | Description |
|---|---|
| FACT_ORDERS | Main transactional fact table |

---

# Airflow Orchestration

Apache Airflow is used to automate and schedule the entire ETL pipeline.

---

# DAG Workflow

```text
Bronze Ingestion
        │
        ▼
Feature Engineering
        │
        ▼
Silver to Gold
        │
        ▼
Load to Snowflake
        │
        ▼
Data Quality Check
```

---

# Airflow Tasks

| Task ID | Description |
|---|---|
| bronze_ingestion | Load raw JSON data into HDFS |
| feature_engineering | Clean and transform data |
| silver_to_gold | Build Gold analytical tables |
| load_to_snowflake | Load tables into Snowflake |
| check_data_quality | Validate null values |

---

# Docker Environment

The entire project runs inside Docker containers.

---

# Main Services

| Service | Port |
|---|---|
| Airflow Webserver | 18080 |
| Jupyter Notebook | 8899 |
| Hadoop NameNode | 9870 |
| YARN Resource Manager | 8088 |
| Spark UI | 4040 |

---

# Running the Project

## 1. Start Containers

```bash
docker compose up -d
```

---

# Verify Running Containers

```bash
docker ps
```

---

# Access Services

## Airflow

```text
http://localhost:18080
```

Default credentials:

```text
Username: airflow
Password: airflow
```

---

## Jupyter Notebook

```text
http://localhost:8899
```

---

## Hadoop NameNode

```text
http://localhost:9870
```

---

# Upload Data to HDFS

## Copy JSON Files

```bash
docker cp ./streaming/. hadoop-namenode:/tmp/streaming
```

---

## Open Hadoop Container

```bash
docker exec -it hadoop-namenode bash
```

---

## Create HDFS Directory

```bash
hdfs dfs -mkdir -p /user/root/datalake/streaming
```

---

## Upload Files to HDFS

```bash
hdfs dfs -put /tmp/streaming/*.json /user/root/datalake/streaming/
```

---

## Verify Uploaded Files

```bash
hdfs dfs -ls /user/root/datalake/streaming
```

---

# Running Spark Jobs

## Bronze Ingestion

```bash
spark-submit /home/jovyan/work/ingestion/bronze_ingestion.py
```

---

## Feature Engineering

```bash
spark-submit /home/jovyan/work/transformation/feature_engineering.py
```

---

## Gold Layer Creation

```bash
spark-submit /home/jovyan/work/transformation/silver_to_gold.py
```

---

# Loading Data into Snowflake

```bash
spark-submit \
--jars /home/jovyan/work/jars/spark-snowflake_2.12-2.11.0.jar,/home/jovyan/work/jars/snowflake-jdbc-3.13.33.jar \
/home/jovyan/work/loading/load_to_snowflake.py
```

---

# Data Quality Validation

The pipeline performs automated data quality checks after loading data.

## Validation Rules

- Check null values
- Validate schema consistency
- Verify table creation
- Validate successful loading

---

# Example Data Quality Output

```text
================ DATA QUALITY REPORT ================

Table: dim_customer
No null values found

Table: fact_orders
customer_id -> 3 null values
```

---

# Key Features

- Distributed ETL processing using Spark
- Hadoop-based Data Lake
- Automated workflows using Airflow
- Star Schema modeling
- Snowflake cloud integration
- Scalable architecture
- Dockerized environment
- Data quality validation
- Incremental data loading

---

# Future Improvements

- Add Kafka for real-time streaming
- Add CI/CD pipeline
- Add monitoring and alerting
- Add unit and integration tests
- Add Power BI dashboards
- Add Spark Structured Streaming
- Add Delta Lake support

---

# Challenges Solved

- Distributed processing configuration
- HDFS integration
- Spark-YARN connectivity
- Snowflake Spark connector setup
- Airflow orchestration
- Docker networking
- Incremental loading strategy

---

# Author

## Mohamed Khairy

Computer Science Student  
Interested in:
- Data Engineering
- Machine Learning
- Big Data Systems
- Cloud Computing

---

# License

This project is for educational and portfolio purposes.
