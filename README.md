# 🛒 Olist Data Platform
### A Modern Incremental Lakehouse Pipeline (Local Simulation)

---

## 📌 Overview
This project implements a modern data pipeline architecture inspired by real-world analytics platforms such as Snowflake and BigQuery. The platform is structured using a **Medallion Architecture (Raw → Bronze → Silver → Gold)** to ensure clear data maturity stages and reliable delivery.

We use **PostgreSQL** for the warehouse layer, **Parquet** for the lake, and **dbt** to enforce modular, testable, and incremental transformations — enabling **safe, repeatable, and observable data processing workflows**. The focus is not on infrastructure complexity, but on **correctness, maintainability, and data engineering principles at scale**.

---

## 🎯 Problem Statement
Traditional batch ETL pipelines suffer from:
*   **Full data reloads** on every execution.
*   **Duplicate records** due to lack of idempotency.
*   **Poor auditability** of data lineage.
*   **Tight coupling** between ingestion and transformation.

This project addresses these issues by introducing **incremental ingestion**, **file-level idempotency**, and a **layered data architecture**.

---

## 🏗️ Architecture (Medallion Pattern)

### 🧱 Data Architecture
*   🟤 **Raw Layer (PostgreSQL)**: First ingestion point. Stores normalized raw data with technical metadata for traceability.
*   🥉 **Bronze Layer (Filesystem)**: Immutable archive of original files. Enables full historical reprocessing and acts as the system of record.
*   ⚪ **Silver Layer (Parquet)**: Optimized analytical format (Parquet). Designed for fast, read-heavy workloads and compatible with engines like **DuckDB**.
*   🟡 **Gold Layer (dbt Marts)**: Business-ready datasets, aggregated and modeled for analytics and BI.

---

## ⚙️ Core Design Principles

### 1. Idempotent Ingestion
Each file is processed using a deterministic **SHA-256 hash**.
*   Prevents duplicate ingestion.
*   Allows safe pipeline re-runs.
*   Ensures consistency across executions.

### 2. Incremental Processing
Transformations are executed using **dbt incremental models**:
*   Updates only new or changed data.
*   Avoids full table refreshes.
*   Improves performance and cost efficiency.

### 3. Data Lineage & Auditability
Every record is enriched with technical metadata:
*   `_metadata_ingested_at`: Ingestion timestamp.
*   `_metadata_source_file`: Reference to the original file.
*   `_metadata_file_hash`: Unique identifier for the batch.

### 4. Separation of Concerns
Clear boundaries between **Ingestion** (Python), **Storage** (Postgres + Filesystem), and **Transformation** (dbt).

---

## ⚖️ Trade-offs
| Decision | Trade-off |
| :--- | :--- |
| **File-based idempotency** | Simpler than CDC, but no real-time updates. |
| **PostgreSQL warehouse** | Easy local setup, but not horizontally scalable. |
| **Batch pipeline** | Easier to reason about, but not real-time. |

---

## 📂 Project Structure
```text
data/
  landing/     # Input files (Inbound)
  bronze/      # Immutable archive (System of Record)
  silver/      # Parquet lake (Analytical format)

dbt/           # Transformation Layer
  models/
    staging/   # Silver: Incremental transformations
    marts/     # Gold: Business models

pipeline/      # Engineering Layer
  ingestion.py # Incremental loader
  run_pipeline.py # Master Orchestrator
```

---

## 🚀 How to Run
1.  **Setup:** `pip install -r requirements.txt`
2.  **Execute:** `python -m pipeline.run_pipeline`

---

## 🧠 Core Insight
The goal of this project is to demonstrate how to build reliable data systems using simple primitives while preserving **correctness, idempotency, and observability**. In real-world platforms, the hardest problems are not infrastructure — they are **ensuring reproducibility and maintaining trust in metrics**. This project is built around those constraints.

---
**Data Engineering Case Study** — Focused on Correctness, Incremental Processing, and Data Quality.
