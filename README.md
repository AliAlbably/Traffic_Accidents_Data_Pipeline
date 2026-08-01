⚙️ Detailed Configurations
1️⃣ Docker Compose Configuration (docker-compose.yml)
YAML
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  spark-master:
    image: bitnami/spark:3.5.0
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"

  postgres:
    image: postgres:15
    container_name: postgres_db
    environment:
      POSTGRES_USER: taxi_user
      POSTGRES_PASSWORD: taxi_password
      POSTGRES_DB: nyc_taxi_db
    ports:
      - "5432:5432"
2️⃣ PostgreSQL Schema (db_schema.sql)
SQL
CREATE TABLE IF NOT EXISTS nyc_taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
🔄 Data Pipeline Workflow
Ingestion (Producer): Reads raw NYC Taxi trip data using optimized chunking (pd.read_csv(..., chunksize=5000)), serializes records into JSON payloads, and streams them continuously into Kafka.

Stream Processing (Spark Consumer): Consumes streaming batches from Kafka using Spark Structured Streaming, parses JSON schemas, filters corrupted or negative fare records, and executes sliding window aggregations.

Data Sink: Writes transformed metrics directly to PostgreSQL for persistence and downstream reporting.

⚠️ Troubleshooting & Common Issues
🔴 Issue 1: Java Heap Out of Memory (java.lang.OutOfMemoryError)
Cause: Reading massive CSV datasets directly into RAM during streaming generation.

Resolution: Enforce chunk-based streaming inside producer.py:

Python
import pandas as pd
for chunk in pd.read_csv('yellow_tripdata.csv', chunksize=5000):
    # Stream chunk records to Kafka
    pass
🔴 Issue 2: Kafka Connection Refused (NoBrokersAvailable)
Cause: Spark or Producer attempting to connect to Kafka before Zookeeper initialization finishes.

Resolution: Wait 10–15 seconds after running docker-compose up -d. Verify internal listener host names (kafka:29092 for docker containers and localhost:9092 for external scripts).

🔴 Issue 3: Missing Kafka Spark Package (ClassNotFoundException)
Cause: Missing Kafka connector package inside the spark-submit execution context.

Resolution: Ensure the correct version package is supplied during execution:

Bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
🔴 Issue 4: Corrupted or Out-of-Bounds Trip Records
Cause: Negative fares, zero-distance trips, or empty passenger counts in raw data.

Resolution: Implement validation filters in Spark Streaming:

Python
cleaned_df = df.filter(
    (df["fare_amount"] > 0) & 
    (df["passenger_count"] > 0) & 
    (df["trip_distance"] > 0)
)
🚀 Setup & Step-by-Step Execution Guide
Prerequisites
Docker Desktop installed and running.

Python 3.10+ installed locally (for producer execution).

Step 1: Spin Up Microservices
Start all infrastructure containers (Kafka, Zookeeper, Spark Master, PostgreSQL):

Bash
docker-compose up -d
Verify service status:

Bash
docker ps
Step 2: Initialize Database Schema
Create the target analytics table in PostgreSQL:

Bash
docker exec -i postgres_db psql -U taxi_user -d nyc_taxi_db < db_schema.sql
Step 3: Launch Spark Streaming Consumer
Run the PySpark Structured Streaming consumer inside the Spark Master container:
