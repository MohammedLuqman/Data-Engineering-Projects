# 🎵 Music Analytics Data Pipeline
### Enterprise-Grade Music Data Lakehouse — Docker Compose Stack

[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7.1-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-Latest-C72E49?logo=minio)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Latest-FFCC01?logo=clickhouse)](https://clickhouse.com/)

---

## 📐 Architecture

```text
Music Streaming API + user Profiles ───────────┐
User Interaction Logs ────────────────────────►│
                                               │
                                    ┌──────────▼──────────┐
                                    │    Apache Kafka     │
                                    │    (Ingestion)      │
                                    └──────────┬──────────┘
                                               │
                                       Spark Streaming
                                               │
                                    ┌──────────▼──────────┐
                                    │   MinIO: BRONZE     │
                                    │ Raw Data (Parquet)  │
                                    └──────────┬──────────┘
                                               │
                                       Spark Batch ETL
                                               │
                                    ┌──────────▼──────────┐
                                    │   MinIO: SILVER     │
                                    │  Cleaned Parquet    │
                                    └──────────┬──────────┘
                                               │
                                      Spark Analytics
                                               │
                                    ┌──────────▼──────────┐
                                    │    MinIO: GOLD      │
                                    │ Aggregated Parquet  │
                                    └───────┬─────┬───────┘
                                            │     │
                               ┌────────────▼┐   ┌▼─────────────────┐
                               │ ClickHouse  │   │ Trend Analysis   │
                               │ Analytics   │   │ Dashboards       │
                               └─────────────┘   └──────────────────┘

Orchestration : Apache Airflow
Dashboard    :  Grafana
```

---

## 🧱 Services Overview

| # | Service | Purpose | Port |
|---|---------|---------|------|
| 1 | **Kafka** | Real-time streaming of music logs | 9092 |
| 2 | **Spark Master** | Distributed data processing cluster using ( unified medallion architecture ) | 8090 |
| 3 | **MinIO** | Object storage for Data Lake (Bronze/Silver/Gold) | 9000 |
| 4 | **ClickHouse** | Columnar database for song analytics | 8123 |
| 5 | **Airflow** | Orchestrating ETL DAGs | 8081 |
| 6 | **Grafana** | Visualizing music trends & system metrics | 3000 |

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop
- 16 GB RAM recommended for the full stack

### 1. Launch the Stack

```bash
docker-compose up -d
```

### 2. Verify Health

```bash
docker-compose ps
```

---

## 🌐 Access Points

| Interface | URL |
|-----------|-----|
| **Kafka UI** | http://localhost:8090 |
| **Grafana** | http://localhost:3000 |
| **Airflow** | http://localhost:8081 |
| **ClickHouse** | http://localhost:8123 |

---

## 📂 Project Structure

```text
music-pipeline/
├── dags/                # Airflow ETL workflows
├── notebooks/           # PySpark analytics notebooks
├── clickhouse/          # Gold layer schema
├── docker-compose.yml   # Infrastructure definition
└── scripts/             # Data ingestion helpers
```

---

## 📊 Analytics Goals

- 🎵 **Top Tracks:** Real-time ranking of the most popular songs.
- 👥 **User Insights:** Analyze user listening behavior and preferences.
- 🎼 **Genre Trends:** Discover rising and declining music genres over time.

---

## 💡 Performance Tuning & Roadmap

### Performance

- Optimized ClickHouse partitioning for high-speed analytical queries.
- Efficient Parquet storage for Bronze, Silver, and Gold layers.
- Spark distributed processing for scalable ETL pipelines.
- Build real-time dashboards powered by Grafana.

---

## 🛠️ Technology Stack

| Layer | Technology |
|--------|------------|
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Storage | MinIO |
| Analytics | ClickHouse |
| Orchestration | Apache Airflow |
| Monitoring | Grafana |
| Containerization | Docker Compose |

---

## 👨‍💻 Author

**Music Analytics Data Pipeline**  
Enterprise-grade Data Engineering project demonstrating a modern Lakehouse architecture using Kafka, Spark, MinIO, ClickHouse, and Airflow.
