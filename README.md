# spotify-content-management

<div align="center">
  <img src="images/apache kafka.png" alt="Dashboarding" style="max-width: 100%; height: auto;" />
</div>
## Data Pipeline Architecture (Abstract)

A real-time **spotify content management system** built with **Apache Kafka**, **Spark Structured Streaming**, and **HDFS (Hive-partitioned Parquet)**. Simulated high-volume transaction data (`EventSim`) is ingested via Kafka, processed in micro-batches by Spark, and stored in a **Bronze Layer** (raw but structured) in HDFS using efficient Parquet format with Hive partitioning by date/hour .

---

### Kafka 📨  
*High-throughput message broker for temporary storage of simulated events*

- Receives and temporarily stores high-volume simulated data from **EventSim**
- Requires a user-friendly Kafka UI for monitoring and debugging:
  - [AKHQ](https://github.com/tchiotludo/akhq)

---

### 1. Spark ⚡  
*Micro-batch processing with Structured Streaming*

- Pulls data from Kafka in **micro-batches** (e.g., every 1 minute) using **Structured Streaming**
- Performs **light preprocessing**:
  - Convert timestamps
  - Filter corrupted/malformed records
  - **Preserve raw data as much as possible**
- Writes output to **HDFS** in **Parquet** format

---

### 2. HDFS + Hive Partitioning + Parquet 🗄️  
*Bronze Layer: Raw but structured data lake*

- Stores processed data in **Parquet format** on **HDFS**
- Uses **Hive partitioning** by **date/hour** for efficient querying
- This is our **Bronze Layer**:  
  → Raw data, minimally transformed, but **structured and queryable**
- Goal: Clean, organized, and scalable storage for downstream analytics

## Data Lake Architecture: Bronze → Silver → Gold in HDFS + ClickHouse Integration

A **medallion architecture** implemented using **Apache Spark** and **HDFS**, progressing from raw ingestion to curated analytics. Final **Gold-layer aggregates** are exposed via **ClickHouse** for ultra-fast querying — either by direct import or zero-copy access using the **HDFS Engine**.

---

### 1. Bronze Layer 🟤  
*Raw but structured — minimal transformation*

- Ingests **raw data** from **Kafka** via **Spark Structured Streaming**
- Stores in **HDFS** using **Parquet format**
- **Goal**: Preserve original data with **maximum fidelity**
- **Hive partitioning** by **date/hour** (or other keys) for scalability and performance
- No business logic — only light validation and schema enforcement

---

### 2. Silver Layer 🟡  
*Cleaned, enriched, and modeled data*

- Reads from **Bronze Layer**
- Performs **data quality** operations:
  - Remove duplicates & corrupted records
  - Standardize formats
  - Enrich with reference data
- Builds **initial Fact & Dimension tables**
- Uses **Spark SQL** + **dbt on Spark** for modular, version-controlled transformations
- Output stored back in **HDFS (Parquet + partitioned)** — now **clean and query-ready**

---

### 3. Gold Layer 🟨  
*Business-ready aggregates and KPIs*

- Consumes **Silver Layer** data
- Applies **aggregations**, **joins**, and **business logic**
- Produces:
  - Final **Fact tables** (e.g., daily transactions, fraud alerts)
  - Enriched **Dimension tables** (e.g., merchants, customers)
  - Precomputed **KPIs** and **metrics**
- Optimized **Parquet files** in **HDFS**
- Designed for **reporting**, **dashboards**, and **ML features**

---

### ClickHouse Integration 🚀  
*High-speed analytics on Gold data — zero copy when possible*

- ** HDFS Engine (Zero-Copy)**  
  ```sql
  CREATE TABLE gold_facts_hdfs (
      event_date Date,
      merchant_id String,
      total_amount Float64,
      fraud_score Float32
  ) ENGINE = HDFS('hdfs://namenode:8020/path/to/gold/facts/*.parquet', 'Parquet')
