# 🚦 Traffic Accidents Big Data Pipeline & ML Engine

[![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue.svg)]()
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache_Kafka-red.svg)]()
[![Apache Spark](https://img.shields.io/badge/Engine-PySpark_3.5-orange.svg)]()
[![Apache Hive](https://img.shields.io/badge/Warehouse-Apache_Hive_3.1-yellow.svg)]()
[![Machine Learning](https://img.shields.io/badge/ML-Spark_MLlib-green.svg)]()
[![Docker](https://img.shields.io/badge/Deployment-Docker_Compose-blue.svg)]()

A containerized real-time streaming analytics pipeline and machine learning architecture for **Traffic Accidents Data**. Built with **Kafka**, **PySpark Streaming**, **Apache Hive**, **PostgreSQL Metastore**, and **Spark MLlib** for accident severity prediction — fully orchestrated via Docker Compose.

---
   
## 📑 Table of Contents
* [Overview & Architecture](#-overview--architecture)
* [Tech Stack](#-tech-stack)
* [Prerequisites](#-prerequisites)
* [Quick Start & Setup Guide](#-quick-start--setup-guide)
* [Open the UIs](#-open-the-uis)
* [Project Structure](#-project-structure)
* [Stopping & Resetting Infrastructure](#-stopping--resetting-infrastructure)

---

## 📐 Overview & Architecture

This pipeline simulates real-time traffic accident telemetry events via Kafka producers, processes batch and streaming accident records using PySpark, stores transformed aggregated tables in Apache Hive, and trains predictive Machine Learning models to analyze accident severity patterns.

```text
┌────────────────────────┐
│ Traffic Data Streams   │
│ (producer.py)          │
└───────────┬────────────┘
            │ (Kafka Topics)
            ▼
┌────────────────────────┐
│ PySpark Streaming      │  ◄── [Hive Metastore: thrift://hive-metastore:9083]
│ (streaming_consumer.py)│
└───────────┬────────────┘
            │
            ▼ (Data Warehouse)
┌────────────────────────┐
│ Apache Hive 3.1        │  ◄── [PostgreSQL 13 Metastore DB]
│ (Data Mart & Tables)   │
└───────────┬────────────┘
            │
            ▼ (Feature Engineering & ML)
┌────────────────────────┐
│ Spark MLlib Trainer    │ ──> [Accident Severity Model]
│ (train_model.py)       │
└────────────────────────┘
```
git clone [https://github.com/AliAlbably/Traffic_Accidents_Data_Pipeline.git](https://github.com/AliAlbably/Traffic_Accidents_Data_Pipeline.git)
cd Traffic_Accidents_Data_Pipeline
```
docker compose up -d postgres
docker compose run --rm --entrypoint /opt/hive/bin/schematool hive-metastore -dbType postgres -initSchema
docker compose up -d
```
python producer.py
```
docker exec nycsparkmaster /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  /app/streaming_consumer.py
```
docker exec nycsparkmaster /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/train_model.py
  Traffic_Accidents_Data_Pipeline/
  ```
├── .gitignore                # Ignores dataset, .env, and temporal files
├── README.md                 # Project Architecture & Setup Guide
├── docker-compose.yml        # Docker microservices definitions
├── requirements.txt          # Python dependencies
│
├── producer.py               # Kafka event streaming producer
├── streaming_consumer.py     # PySpark streaming engine & Hive writer
└── train_model.py            # Spark MLlib training script for accident predictions
Traffic_Accidents_Data_Pipeline/
├── .gitignore                # Ignores dataset, .env, and temporal files
├── README.md                 # Project Architecture & Setup Guide
├── docker-compose.yml        # Docker microservices definitions
├── requirements.txt          # Python dependencies
│
├── producer.py               # Kafka event streaming producer
├── streaming_consumer.py     # PySpark streaming engine & Hive writer
└── train_model.py            # Spark MLlib training script for accident predictions
```
# Stop containers gracefully
docker compose down

# Wipe containers and clean volumes
docker compose down -v
  
