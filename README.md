## NEPSE REAL-TIME DATA PIPELINE

This project is a containerized, distributed data engineering pipeline designed to transform raw stock market data from the Nepal Stock Exchange (NEPSE) into a continuous, structured stream. 

---

## HOW IT WORKS

The system has moved beyond a "run-once" script into a decoupled, streaming architecture. It functions through the following stages:

### 1. Data Ingestion & Batching
* **Scraper Service**: Continuously fetches live market data.
* **Batching Logic**: Instead of sending data point-by-point, the system groups data into **1-minute batches**. This ensures the stream is structured and time-consistent.

### 2. Streaming Layer (Kafka)
The project uses **Apache Kafka** to decouple the data source from the data consumer:
* **Producer**: Receives batched data from the scraper and pushes it to the `nepse-topic`.
* **Broker**: Managed by **Zookeeper**, it handles the storage and transmission of messages.
* **Consumer**: An independent service that "listens" to the Kafka topic and processes the data as it arrives.

### 3. API Layer
A **FastAPI** service acts as the gateway. It exposes a `/nepse_data` endpoint, allowing external systems or frontends to interact with the pipeline and view the processed data.

### 4. Containerization & Orchestration
The entire stack is managed by **Docker Compose**, ensuring all services (Kafka, Zookeeper, Producer, Consumer, API) run in an isolated environment with proper networking.

---

## WHAT WE HAVE SO FAR

The foundation of the streaming system is structurally complete and includes:

* **Distributed Architecture**: Separate containers for the Producer and Consumer to simulate a real-world distributed system.
* **Resilient Networking**: Services communicate via Docker service names (e.g., `kafka:9092`). The system includes **retry logic** to handle Kafka startup delays and prevent `NoBrokersAvailable` errors.
* **Standardized Execution**: Uses modular Python execution (`python -m app.run_producer`) to ensure consistent file pathing inside containers.
* **Pipeline Infrastructure**:
    * **Zookeeper/Kafka**: Core messaging backbone.
    * **Producer**: Batch-aware data pusher.
    * **Consumer**: Real-time message receiver.
    * **FastAPI**: System interface.

---

## TECH STACK

| Category | Technology |
| :--- | :--- |
| **Language** | Python |
| **Streaming** | Apache Kafka, Zookeeper |
| **API** | FastAPI |
| **Infrastructure** | Docker, Docker Compose |

## GETTING STARTED

To spin up the entire pipeline, run:
```bash
docker-compose up