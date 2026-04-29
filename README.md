# 📈 Nepse-Sight: NEPSE Real-Time Data Pipeline

> **Course:** Knowledge Discovery and Data Science (BSIT-375) — Week 8 Final Project  
> **Institution:** Presidential Graduate School  
> **Branch:** `saksham`

A containerized, event-driven data engineering pipeline that ingests live and historical stock market data from the Nepal Stock Exchange (NEPSE), streams it through Apache Kafka, processes it with Apache Spark, stores it in PostgreSQL, and visualizes it through a Grafana dashboard — all orchestrated via Docker Compose.

---

## 📁 Project Structure

```
nepse_data_pipeline/
├── app/                    # FastAPI application (main API + producer logic)
├── db/                     # Database init scripts (init.sql)
├── kafka_layer/            # Kafka producer/consumer modules
├── nepse_scrapper/         # NEPSE data scraping logic (uses nepse-scraper package)
├── services/               # Supplementary service modules
├── spark_jobs/             # PySpark streaming job (stream_processor.py)
├── grafana/
│   ├── provisioning/       # Grafana datasource & dashboard provisioning
│   └── dashboards/         # Pre-built dashboard JSON files
├── Dockerfile              # Main app image (Python 3.11-slim)
├── Dockerfile.ml           # ML service image
├── docker-compose.yml      # Full stack orchestration
├── requirements.txt        # Python dependencies
└── .env                    # Environment configuration (not committed)
```

---

## 🏗️ System Architecture

The pipeline follows a layered, event-driven design. All inter-service communication is asynchronous via Kafka.

```
┌─────────────────────────────────────────────────────┐
│              Data Ingestion Layer                    │
│   FastAPI (/store/prev_data) + NEPSE Scraper        │
│         ↓ publishes to Kafka topic                  │
│                  nepse-topic                        │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│           Stream Processing Layer                    │
│     Apache Spark (spark_jobs/stream_processor.py)   │
│         Consumes from nepse-topic                   │
│         Writes processed data → PostgreSQL          │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│            Storage & ML Layer                        │
│   PostgreSQL 16 (stock data + model outputs)        │
│   ML Service (scikit-learn based predictions)       │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│           Visualization Layer                        │
│     Grafana (port 3001) with ECharts panel          │
│     Connected to PostgreSQL datasource              │
└─────────────────────────────────────────────────────┘
```

### Services Overview

| Service | Image / Build | Port | Role |
|---|---|---|---|
| `api` | Custom (Dockerfile) | `8000` | FastAPI — data ingestion endpoint & system interface |
| `producer` | Custom (Dockerfile) | — | Publishes batched NEPSE data to Kafka |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | `2181` | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | `9092` | Message broker |
| `kafka-init` | `confluentinc/cp-kafka:7.5.0` | — | Creates `nepse-topic` on startup |
| `postgres` | `postgres:16` | `5432` | Persistent storage for stock data |
| `spark` | `apache/spark:3.5.1` | — | Structured Streaming from Kafka to PostgreSQL |
| `grafana` | `grafana/grafana-enterprise` | `3001` | Dashboard & visualization |
| `ml_service` | Custom (Dockerfile.ml) | — | ML predictions written to PostgreSQL |
| `prev_data_loader` | `curlimages/curl` | — | Bootstrap job: triggers historical data load via API |

---

## ⚙️ How the Pipeline Works

### 1. Bootstrap: Historical Data Load
On startup, `prev_data_loader` waits 20 seconds for services to stabilize, then sends a `curl` request to `http://api:8000/store/prev_data`. The FastAPI service uses the `nepse-scraper` Python package to fetch historical OHLCV data and writes it to PostgreSQL.

### 2. Real-Time Ingestion
The `producer` service runs continuously (`python -m app.run_producer`). It scrapes live NEPSE price data and batches data points into **1-minute windows** before publishing JSON messages to the `nepse-topic` Kafka topic.

### 3. Stream Processing
The `spark` service runs a `spark-submit` job (`spark_jobs/stream_processor.py`) that consumes from `nepse-topic` in real time using Spark Structured Streaming. It processes and enriches the data, then writes results to PostgreSQL.

### 4. ML Predictions
The `ml_service` reads processed data from PostgreSQL, runs prediction models (built with `scikit-learn`), and writes forecasts back to PostgreSQL for the dashboard to consume.

### 5. Visualization
Grafana (port `3001`) connects to PostgreSQL as its primary datasource. Dashboards and datasources are provisioned automatically from `grafana/provisioning/`. The **volkovlabs-echarts-panel** plugin enables rich candlestick and technical indicator charts.

---

## 🛠️ Technology Stack

| Component | Technology | Notes |
|---|---|---|
| Language | Python 3.11 | All services |
| API Framework | FastAPI + Uvicorn | Port 8000 |
| Data Scraping | `nepse-scraper` (PyPI) | NEPSE data source |
| Message Broker | Apache Kafka + Zookeeper | Confluent Docker images |
| Stream Processing | Apache Spark 3.5.1 | Structured Streaming |
| ML Models | scikit-learn, pandas, joblib | Regression/prediction |
| Database | PostgreSQL 16 | Primary data store |
| Visualization | Grafana Enterprise | Port 3001, ECharts plugin |
| Containerization | Docker + Docker Compose | Single `docker-compose.yml` |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- At least **8 GB of RAM** allocated to Docker (Spark and Kafka are memory-intensive)
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/caster-k/nepse_data_pipeline.git
cd nepse_data_pipeline
git checkout saksham
```

### 2. Configure Environment Variables

Create a `.env` file in the project root. The following variables are required:

```env
# PostgreSQL
POSTGRES_USER=your_pg_user
POSTGRES_PASSWORD=your_pg_password
POSTGRES_DB=nepse_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# App
DATABASE_URL=postgresql://your_pg_user:your_pg_password@postgres:5432/nepse_db
```

> **Note:** The `.env` file is listed in `.gitignore` and should never be committed.

### 3. Start the Full Stack

```bash
docker-compose up --build
```

Docker Compose will start all services in the correct dependency order:

1. `zookeeper` and `postgres` start first
2. `kafka` starts and connects to Zookeeper
3. `kafka-init` creates the `nepse-topic` (waits 15 seconds)
4. `api` and `spark` start
5. `prev_data_loader` triggers historical data ingestion (waits 20 seconds)
6. `producer` begins live data streaming (depends on `prev_data_loader` completing)
7. `ml_service` starts running predictions
8. `grafana` becomes available

### 4. Access the Services

| Service | URL | Credentials |
|---|---|---|
| FastAPI Docs | http://localhost:8000/docs | — |
| Grafana Dashboard | http://localhost:3001 | `admin` / `admin` |
| PostgreSQL | `localhost:5432` | As set in `.env` |
| Kafka Broker | `localhost:9092` | — |

### 5. Stopping the Stack

```bash
# Stop all containers
docker-compose down

# Stop and remove all volumes (full reset)
docker-compose down -v
```

---

## 📊 Dashboard

Grafana is pre-provisioned with dashboards from `grafana/dashboards/`. After startup:

1. Open http://localhost:3001
2. Log in with `admin` / `admin`
3. Navigate to **Dashboards** to find the NEPSE charts

The dashboard uses the **volkovlabs-echarts-panel** plugin for advanced charting and the **yesoreyeram-infinity-datasource** plugin for additional data flexibility.

---

## 📡 API Endpoints

The FastAPI service exposes the following key endpoints (full docs at `/docs`):

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/store/prev_data` | Triggers historical NEPSE data fetch and storage |
| `GET` | `/nepse_data` | Returns latest processed NEPSE data |

---

## 🔄 Startup Sequence & Dependencies

```
zookeeper ──────────────────────────────────────────────→ kafka
                                                              ↓
postgres ─────────────────────────────────────────→ kafka-init (creates topic)
    ↓                                                         ↓
   api ──────────────────────────────────────→ spark ─────→ grafana
    ↓
prev_data_loader (curl /store/prev_data, waits 20s)
    ↓ (completes successfully)
producer (starts live streaming)

ml_service (depends on postgres, restarts always)
```

---

## ⚠️ Known Limitations & Current Status

- **Consumer service is disabled** — the `consumer` container is commented out in `docker-compose.yml`. The Spark job acts as the primary consumer from Kafka.
- **ML models are in progress** — the `ml_service` runs but model training and prediction endpoints are still being developed. Full ARIMA, Linear Regression, and Bi-LSTM models (as outlined in the project brief) are planned.
- **Single Kafka topic** — the pipeline currently uses one unified `nepse-topic` rather than the three-topic architecture (`raw-stock-data` → `processed-stock-data` → `model-predictions`) described in the project specification. This is a deliberate simplification for the current stage.
- **No Jupyter/EDA notebooks** — formal EDA reports and correlation heatmaps have not yet been added to the repository.

---

## 📦 Dependencies

All Python dependencies are listed in `requirements.txt`:

```
nepse-scraper==1.0.0      # NEPSE data scraping
fastapi==0.136.0           # API framework
uvicorn==0.45.0            # ASGI server
kafka-python               # Kafka producer/consumer
psycopg2-binary            # PostgreSQL driver
scikit-learn               # ML models
pandas                     # Data manipulation
joblib                     # Model serialization
python-dotenv              # Environment variable management
requests==2.33.1           # HTTP client
```

---

## 🙏 Acknowledgements

This project was built as part of the BSIT-375 Knowledge Discovery and Data Science course at Presidential Graduate School. Data is sourced via the `nepse-scraper` package which interfaces with publicly available NEPSE market data.

---

*For questions about setup or architecture, refer to `INSTRUCTIONS.docx` included in the repository.*