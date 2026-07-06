
```markdown
# 🎵 Music Analytics Data Pipeline
### Enterprise-Grade Music Data Lakehouse — Docker Compose Stack

[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7.1-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-Latest-C72E49?logo=minio)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Latest-FFCC01?logo=clickhouse)](https://clickhouse.com/)

---

## 📐 Architecture


```

Music Streaming API ──────────────────────────┐
User Interaction Logs ────────────────────────►│
│
┌─────────▼─────────┐
│   Apache Kafka     │
│   (Ingestion)      │
└─────────┬──────────┘
│ Spark Streaming
┌─────────▼──────────┐
│   MinIO: BRONZE     │  Raw Data
│  (Parquet + JSON)   │
└─────────┬──────────┘
│ Spark Batch ETL
┌─────────▼──────────┐
│   MinIO: SILVER     │  Cleaned Data
│     (Parquet)       │
└─────────┬──────────┘
│ Spark Analytics
┌─────────▼──────────┐
│   MinIO: GOLD       │  Aggregated
│     (Parquet)       │
└──────┬─────┬───────┘
│     │
┌─────────────▼┐   ┌▼──────────────────┐
│  ClickHouse  │   │  Trend Analysis   │
│  (Analytics) │   │  (Dashboards)     │
└──────────────┘   └───────────────────┘

Orchestration: Apache Airflow
Monitoring:    Prometheus + Grafana

```

---

## 🧱 Services Overview

| # | Service | Purpose | Port |
|---|---------|---------|------|
| 1 | **Kafka** | Real-time streaming of music logs | 9092 |
| 2 | **Spark Master** | Distributed data processing cluster | 8090 |
| 3 | **MinIO** | Object storage for Data Lake (Bronze/Silver/Gold) | 9000 |
| 4 | **ClickHouse** | Columnar database for song analytics | 8123 |
| 5 | **Airflow** | Orchestrating ETL DAGs | 8081 |
| 6 | **Grafana** | Visualizing music trends & system metrics | 3000 |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop & Docker Compose v2.
- 16GB RAM recommended for full stack.

### 1. Launch the Stack
```bash
docker-compose up -d --build

```

### 2. Verify Health

```bash
docker-compose ps

```

---

## 🌐 Access Points

| Interface | URL |
| --- | --- |
| **Kafka UI** | http://localhost:8090 |
| **Grafana** | http://localhost:3000 |
| **Airflow** | http://localhost:8081 |
| **ClickHouse** | http://localhost:8123 |

---

## 📂 Project Structure

```
music-pipeline/
├── dags/               ← Airflow ETL workflows
├── notebooks/          ← PySpark logic for song analytics
├── clickhouse/         ← Gold layer schema
├── docker-compose.yml  ← Infrastructure definition
└── scripts/            ← Data ingestion helpers

```

---

## 📊 Analytics Goals

* **Top Tracks:** Real-time ranking of popular songs.
* **User Insights:** Analyzing user listening preferences.
* **Genre Trends:** Tracking the rise and fall of music genres.

---

## 💡 Performance Tuning & Roadmap

* **Performance:** Optimized partitioning in ClickHouse for fast analytical queries.
* **Roadmap:**
* Future integration with **Apache Iceberg** for full Lakehouse capabilities.
* Implementing automated anomaly detection for streaming logs.



---

*Built for the Music Analytics Graduation Project — Data Engineering Bootcamp 2024*

```


هل تحتاج لأي تعديل إضافي في الجداول أو تفاصيل تقنية معينة؟

```
