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

Component,Technology,Role / Description
Event Broker,Apache Kafka,High-throughput streaming message buffer
Cluster Manager,Apache Zookeeper,Service coordination and Kafka state management
Processing Engine,Apache Spark (PySpark),Distributed stream transformations and window aggregations
Storage Layer,PostgreSQL,Relational data warehouse storing structured output
Containerization,Docker & Docker Compose,Infrastructure isolation and multi-container orchestration
Language & Tooling,Python 3.10+ & SQL,"Core scripting, pipeline development, and data modeling"
