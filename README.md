# 🚕 NYC Taxi Data Analytics & Real-Time Streaming Pipeline

[![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue.svg)]()
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache_Kafka-red.svg)]()
[![Apache Spark](https://img.shields.io/badge/Engine-Apache_Spark-orange.svg)]()
[![Docker](https://img.shields.io/badge/Deployment-Docker-blue.svg)]()

An end-to-end, enterprise-grade Big Data streaming and batch analytics pipeline designed to ingest, process, transform, and store multi-million record datasets of **New York City Yellow Taxi** trips in real time.

---

## 📑 Table of Contents
* [Overview & Business Context](#-overview--business-context)
* [System Architecture](#-system-architecture)
* [Tech Stack & Infrastructure](#-tech-stack--infrastructure)
* [Detailed Configurations](#-detailed-configurations)
* [Data Pipeline Workflow](#-data-pipeline-workflow)
* [Troubleshooting & Common Issues](#-troubleshooting--common-issues)
* [Setup & Step-by-Step Execution Guide](#-setup--step-by-step-execution-guide)
* [Verification & Testing](#-verification--testing)
* [Repository Structure](#-repository-structure)

---

## 🎯 Overview & Business Context

Processing urban transportation data requires high throughput, low latency, and efficient memory management. 

This project addresses the challenge of processing large-scale NYC Taxi transactions by implementing a fully containerized microservices architecture. It enables real-time monitoring of trip frequencies, high-demand pickup zones, tip percentages, fare dynamics, and system health metrics.

---

## 📐 System Architecture

Below is the end-to-end data processing workflow:

```text
┌────────────────────────┐
│  NYC Taxi CSV Dataset  │
└───────────┬────────────┘
            │ (Chunked Streaming Producer)
            ▼
┌────────────────────────┐
│      Apache Kafka      │  ◄── [Zookeeper Orchestration]
│  (Topic: taxi_trips)   │
└───────────┬────────────┘
            │ (Consumer Micro-batches)
            ▼
┌────────────────────────┐
│ Apache Spark Streaming │  ◄── [PySpark Engine & SQL Rules]
└───────────┬────────────┘
            │
            ├───────────────────────────────┐
            ▼                               ▼
┌────────────────────────┐     ┌────────────────────────┐
│ PostgreSQL Data Warehouse│   │ Real-Time Metrics UI   │
│  (Persisted Storage)   │     │ (Dashboard Engine)     │
└────────────────────────┘     └────────────────────────┘

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
