# 🚕 Real-Time NYC Taxi Data Analytics & Batch Processing Pipeline

An end-to-end distributed Big Data pipeline designed to ingest, process, and analyze large-scale New York City Yellow Taxi transaction datasets. Engineered using **Apache Kafka**, **Apache Spark Streaming**, **PostgreSQL/InfluxDB**, and **Docker**.

---

## 📐 Architecture Overview

```text
[ NYC Taxi Dataset ] ──(Streaming Producer)──> [ Apache Kafka ] ──(Consumer Engine)──> [ Apache Spark ]
                                                                                              │
                                                                                              ├──> [ PostgreSQL / Data Warehouse ]
                                                                                              └──> [ Real-Time Analytics Dashboard ]
Data Ingestion (Producer): Streams taxi trip records continuously into Kafka topics using partitioned data chunking to optimize memory consumption.

Event Broker (Kafka & Zookeeper): Acts as a resilient messaging layer that absorbs high-volume trip ingestion rates with low latency.
├── docker-compose.yml           # Multi-Container Infrastructure Setup
├── producer.py                  # Kafka Ingestion Producer Script
├── consumer_spark.py            # PySpark Streaming & Aggregation Pipeline
├── db_schema.sql                # Analytical Database Schemas
├── requirements.txt             # Python Dependencies
└── README.md                    # Project Documentation
⚡ Key Pipeline Capabilities
High-Throughput Streaming: Ingests and parses thousands of trip events per second seamlessly.

Geospatial & Fare Analytics: Calculates dynamic trip distances, tip rates, high-density pickup zones, and peak revenue windows.

Scalable Architecture: Fully containerized microservices ensuring simple deployment across staging and production environments.

🚀 Quick Setup & Execution
1️⃣ Spin Up Infrastructure
docker-compose up -ddocker exec -it spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  consumer_spark.py
python producer.py



