# 🏢 Olist E-Commerce Data Pipeline

## Enterprise-Grade Data Engineering Project

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.10.4-green.svg)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0-blue.svg)](https://www.docker.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-29B5E8.svg)](https://www.snowflake.com/)

---

## 📋 **Table of Contents**
- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Pipeline Stages](#pipeline-stages)
- [Star Schema Design](#star-schema-design)
- [Results](#results)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 📖 **Overview**

This project implements a **complete end-to-end data pipeline** for the Olist Brazilian E-Commerce dataset. The pipeline extracts raw JSON data, transforms it through multiple quality stages (Bronze → Silver → Gold), and loads it into Snowflake for business intelligence and analytics.

### **Key Features**
- ✅ **Automated ETL Pipeline** using Apache Airflow DAGs
- ✅ **Distributed Processing** with Apache Spark
- ✅ **Scalable Storage** on HDFS (Hadoop Distributed File System)
- ✅ **Data Quality Checks** at each pipeline stage
- ✅ **Star Schema Design** optimized for analytical queries
- ✅ **Containerized Deployment** with Docker Compose
- ✅ **Cloud Integration** with Snowflake Data Warehouse

---

## 🏗️ **Architecture**
