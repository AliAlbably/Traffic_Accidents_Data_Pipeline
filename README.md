# 🚗 Real-Time US Traffic Accidents Analytics Pipeline

An end-to-end Big Data streaming architecture designed to process, analyze, and visualize real-time traffic accident records across the United States. Powered by **Apache Kafka**, **Apache Spark Streaming**, **InfluxDB**, and **PySpark MLlib**.

---

## 📐 Architecture Overview

```text
[ CSV Dataset ] ──(Chunked Producer)──> [ Apache Kafka ] ──(Consumer Engine)──> [ Apache Spark ]
                                                                                       │
                                                                                       ├──> [ InfluxDB Time-Series ]
                                                      Producer (producer.py): Reads large-scale historical accident data in chunks and streams JSON events directly into Kafka topics at high speed.

Event Messaging (Kafka & Zookeeper): Serves as a high-throughput event buffer, separating ingestion speed from stream processing.

Stream Processing (streaming_consumer.py): Consumes streaming batches using Apache Spark Structured Streaming, parses schema, and executes real-time aggregation.

Time-Series Sink (InfluxDB): Persists processed micro-batches into dynamic measurement structures for real-time dashboarding.

Machine Learning (train_model.py): Leverages PySpark MLlib (Random Forest Classifier) on 1,000,000+ records to predict accident severity.

🛠️ Tech Stack & Tools
Core Frameworks: Apache Kafka, Apache Spark (PySpark), InfluxDB 2.x

Containerization: Docker & Docker Compose

Programming Languages: Python 3.10+

Machine Learning: PySpark MLlib (Random Forest, VectorAssembler)

Monitoring & UI: Kafka UI, InfluxDB Data Explorer, Spark Master UI

📁 Repository Structure
Plaintext
├── docker-compose.yml           # Complete Multi-Container Infrastructure Config
├── producer.py                  # High-Speed Kafka Producer Script
├── streaming_consumer.py        # Spark Streaming & InfluxDB Ingestion Consumer
├── train_model.py               # PySpark ML Model Training Script
├── requirements.txt             # Python Dependencies
└── README.md                    # Project Documentation
⚡ Performance Metrics
Data Throughput: Standardized streaming rate of 12,000+ events/sec with zero memory overflow.

Dataset Processed: Over 7,000,000+ accident events successfully ingested and persisted.

Machine Learning Model: Achieved high multiclass classification accuracy on accident severity prediction.

🚀 Getting Started
Prerequisites
Docker Desktop installed on Windows/Linux.

US Accidents Dataset (US_Accidents_March23.csv) placed inside the ./data folder.

1️⃣ Step 1: Spin Up Infrastructure
Run all services (Zookeeper, Kafka, InfluxDB, Spark Master, Kafka UI) using Docker Compose:

Bash
docker-compose up -d
2️⃣ Step 2: Run Spark Streaming Consumer
Start the consumer first so it remains ready to ingest incoming Kafka micro-batches:

Bash
docker exec -e SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy" -it spark-master \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/streaming_consumer.py
3️⃣ Step 3: Launch High-Speed Data Producer
In a new terminal window, execute the producer to stream accident records into Kafka:

Bash
docker exec -it spark-master python3 /opt/spark/work-dir/producer.py
4️⃣ Step 4: Train PySpark ML Severity Model (Optional)
Train a Distributed Random Forest model on 1,000,000+ accident records:

Bash
docker exec -it spark-master python3 /opt/spark/work-dir/train_model.py
🌐 Web Dashboards & Management UI
Kafka UI: http://localhost:8080

InfluxDB Dashboard: http://localhost:8086 (User: admin | Pass: adminpassword)

Spark Master Web UI: http://localhost:4040                                 └──> [ PySpark ML Model ]
