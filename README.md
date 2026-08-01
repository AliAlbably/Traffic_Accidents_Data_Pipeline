# 🚕 NYC Taxi Data Analytics & Real-Time Streaming Pipeline

[![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue.svg)]()
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache_Kafka-red.svg)]()
[![Apache Spark](https://img.shields.io/badge/Engine-Apache_Spark-orange.svg)]()
[![Docker](https://img.shields.io/badge/Deployment-Docker-blue.svg)]()

An end-to-end, enterprise-grade Big Data streaming & batch analytics pipeline designed to ingest, process, transform, and analyze multi-million record datasets of **New York City Yellow Taxi** trips in real time.

---

## 📑 Table of Contents
* [Overview & Business Motivation](#-overview--business-motivation)
* [System Architecture](#-system-architecture)
* [Tech Stack & Infrastructure](#-tech-stack--infrastructure)
* [Data Pipeline Workflow](#-data-pipeline-workflow)
* [Key Metrics & Analytics](#-key-metrics--analytics)
* [Repository Structure](#-repository-structure)
* [Setup & Installation Guide](#-setup--installation-guide)
* [Future Roadmap](#-future-roadmap)

---

## 🎯 Overview & Business Motivation

Processing urban transportation data requires high throughput, low latency, and efficient memory management. 

This project solves the problem of analyzing large-scale NYC Taxi transactions by creating a fully containerized microservices architecture. It allows data engineers and analysts to monitor trip frequencies, high-demand pickup zones, tip percentages, and fare spikes live as transactions occur.

---

## 📐 System Architecture

Below is the conceptual flow of the distributed pipeline:

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
│ PostgreSQL / TimeSeries│     │ Analytical Dashboards  │
│  (Persisted Storage)   │     │ (Real-Time Metrics UI) │
└────────────────────────┘     └────────────────────────┘
