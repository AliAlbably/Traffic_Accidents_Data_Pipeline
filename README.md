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
🛠️ Tech StackLayerTechnologyPurpose / DescriptionContainerizationDocker & Docker ComposeContainer runtime isolating master/worker dependenciesRaw StorageLocal Parquet FilesHigh-performance columnar storage files mounted to container volumesETL EnginePySpark 3.5Distributed data extraction, cleansing, feature enrichment, and loadingData WarehouseApache Hive 3.1Queryable SQL warehouse for multi-dimensional KPI aggregationsMetastore DBPostgreSQL 13Persistence layer for Hive tables metadata & schema structuresDashboard UIFlask & Plotly.jsAsynchronous HTML analytics portal re-rendered dynamically on page load📋 PrerequisitesDocker Desktop installed and actively running.Git CLI installed.🚀 Quick Start & Setup Guide1️⃣ Clone the RepositoryBashgit clone <your-repo-url>
cd big-Data-project
2️⃣ Populate Raw DatasetPlace your .parquet dataset files inside the ./dataset/ directory (create the directory if missing):Plaintextbig-Data-project/
└── dataset/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── yellow_tripdata_2025-03.parquet
3️⃣ Initialize Hive Metastore & Launch ServicesBecause multiple services concurrently query the PostgreSQL database, we initialize the Hive Metastore schema first via the schematool container runner to avoid concurrent write lock issues:Bash# 1. Boot up PostgreSQL database container
docker compose up -d postgres

# 2. Run schema initialization tool for Hive metastore
docker compose run --rm --entrypoint /opt/hive/bin/schematool hive-metastore -dbType postgres -initSchema

# 3. Spin up all remaining infrastructure microservices in background
docker compose up -d
4️⃣ Provision Hive SQL TablesExecute the DDL script inside the HiveServer container to instantiate schema tables:Bashdocker exec nychiveserver beeline \
  -u "jdbc:hive2://localhost:10000/;auth=noSasl" -n hive \
  -f /database_scripts/create_tables.hql
5️⃣ Execute PySpark ETL JobSubmit the PySpark pipeline script to the Spark cluster to cleanse raw records and write directly into Hive tables:Bashdocker exec nycsparkmaster /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/hive/warehouse \
  /app/transform.py
🌐 Web User InterfacesUI ComponentPort / Access URLPurposeFlask Live Dashboardhttp://localhost:8501Real-time interactive glassmorphism analytical dashboardSpark Master Web UIhttp://localhost:8090Apache Spark cluster topology & master node execution statsSpark Worker UIhttp://localhost:8081Worker node core usage, RAM allocations, and executor logsHiveServer2 Status UIhttp://localhost:10002HiveServer2 query monitoring & session management📁 Project StructurePlaintextbig-Data-project/
├── .gitignore               # Ignores dataset/, .env, *.csv, *.parquet files
├── README.md                # Project Architecture & Setup Guide
├── docker-compose.yml       # Complete multi-container microservices definition
│
├── dataset/                 # Raw input Parquet data directory (Git-ignored)
│
├── spark_job/               # PySpark streaming and batch ETL scripts
│   └── transform.py         # Main PySpark extraction & processing script
│
├── hive_setup/              # Data Warehouse definition & Hive configs
│   ├── hive-site.xml        # Centralized Hive Metastore configuration settings
│   └── create_tables.hql    # DDL script creating structured Hive tables
│
└── python_app/              # Flask Visualization Web Application
    ├── Dockerfile
    ├── main.py              # Flask server, handles Hive SQL queries & Plotly JSON
    ├── requirements.txt      # Python runtime dependencies (pyhive, flask, etc.)
    └── templates/
        └── dashboard.html   # Custom Jinja2 Glassmorphism UI template with Plotly.js
📡 Container Networking RulesInside the isolated Docker network, never use localhost to inter-connect backend services. Always reference the target service name defined in docker-compose.yml:Connectivity FlowTarget HostnameDefault PortFlask → Hive Serverhive-server10000Spark → Hive Metastorehive-metastore9083Hive Metastore → PostgreSQLpostgres5432Spark Worker → Spark Masterspark-master7077Code Example (PyHive inside Flask application):Pythonfrom pyhive import hive

conn = hive.connect(
    host="hive-server",   # <-- Use Docker service hostname, NOT localhost
    port=10000,
    database="nyc_taxi",
    auth="NOSASL"
)
🛑 Stopping & Resetting InfrastructureBash# Stop all running containers gracefully (preserves database state & volume data)
docker compose down

# Stop and PURGE all persistent container volumes (Clean reset — requires re-running schematool)
docker compose down -v
