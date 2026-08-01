# 🚕 NYC Taxi Big Data Analytics Pipeline

[![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue.svg)]()
[![Apache Spark](https://img.shields.io/badge/ETL-PySpark_3.5-orange.svg)]()
[![Apache Hive](https://img.shields.io/badge/Warehouse-Apache_Hive_3.1-yellow.svg)]()
[![Flask](https://img.shields.io/badge/Dashboard-Flask_--_Plotly.js-green.svg)]()
[![Docker](https://img.shields.io/badge/Deployment-Docker_Compose-blue.svg)]()

A containerized, enterprise-ready Big Data streaming and batch analytics pipeline built with **PySpark**, **Apache Hive**, **PostgreSQL**, and a live **Flask** interactive web dashboard — fully orchestratable via Docker so every team member gets an identical runtime environment with a single command.

---

## 📑 Table of Contents
* [Overview & Architecture](#-overview--architecture)
* [Tech Stack](#-tech-stack)
* [Prerequisites](#-prerequisites)
* [Quick Start & Setup Guide](#-quick-start--setup-guide)
* [Web User Interfaces](#-web-user-interfaces)
* [Project Structure](#-project-structure)
* [Container Networking Rules](#-container-networking-rules)
* [Stopping & Resetting Infrastructure](#-stopping--resetting-infrastructure)

---

## 📐 Overview & Architecture

This pipeline ingests raw **NYC Yellow Taxi Parquet trip records**, performs complex distributed ETL transformations using PySpark, warehouses the clean aggregated data into Apache Hive tables backed by a PostgreSQL Metastore, and serves dynamic interactive visualizations over a custom Flask & Plotly web application.

```text
┌────────────────────────┐
│ Raw Parquet Files      │
│ (Mounted `./dataset/`) │
└───────────┬────────────┘
            │
            ▼ (Batch Transformation)
┌────────────────────────┐
│     PySpark 3.5        │  ◄── [Hive Metastore: thrift://hive-metastore:9083]
│     ETL Pipeline       │
└───────────┬────────────┘
            │
            ▼ (Structured Data Warehouse)
┌────────────────────────┐
│    Apache Hive 3.1     │  ◄── [PostgreSQL 13 Metadata Database]
│  (HiveServer2 Engine)  │
└───────────┬────────────┘
            │
            ▼ (PyHive SQL Interface)
┌────────────────────────┐
│  Flask & Plotly Web UI │ ──> [Interactive Live Dashboard @ localhost:8501]
└────────────────────────┘
