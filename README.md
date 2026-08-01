# 🚗 Real-Time US Traffic Accidents Analytics Pipeline

[![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue.svg)]()
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache_Kafka-red.svg)]()
[![Apache Spark](https://img.shields.io/badge/Engine-Apache_Spark-orange.svg)]()
[![InfluxDB](https://img.shields.io/badge/Database-InfluxDB-purple.svg)]()
[![Docker](https://img.shields.io/badge/Deployment-Docker-blue.svg)]()

An end-to-end Big Data streaming architecture designed to process, analyze, and visualize real-time traffic accident records across the United States. Powered by **Apache Kafka**, **Apache Spark Streaming**, **InfluxDB**, and **PySpark MLlib**.

---

## 📐 Architecture Overview

```text
[ CSV Dataset ] ──(Chunked Producer)──> [ Apache Kafka ] ──(Consumer Engine)──> [ Apache Spark ]
                                                                                       │
                                                                                       ├──> [ InfluxDB Time-Series ]
                                                                                       └──> [ PySpark ML Model ]
