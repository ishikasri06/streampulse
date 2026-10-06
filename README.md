# StreamPulse — Real-Time Distributed Log Analytics & Monitoring Platform

StreamPulse is a real-time distributed log analytics and monitoring platform that collects application logs, processes them as a continuous stream, stores aggregated service metrics, and displays them through a live monitoring dashboard.

The project demonstrates a complete event-driven data pipeline using **Apache Kafka, Apache Spark Structured Streaming, DataStax Astra DB, FastAPI, React, WebSockets, and Docker**.

---

## 🚀 Project Overview

Modern distributed applications generate a large volume of logs across multiple services. Processing these logs manually or in batches makes it difficult to identify errors, monitor response times, and understand the current health of services.

StreamPulse addresses this by continuously:

1. Generating application logs
2. Publishing logs to Apache Kafka
3. Processing the stream using Spark Structured Streaming
4. Aggregating service-level metrics
5. Persisting metrics in **DataStax Astra DB**
6. Exposing metrics through a FastAPI backend
7. Streaming live updates to a React dashboard using WebSockets

### High-Level Data Flow

```text
Application Logs
       │
       ▼
Python Log Generator
       │
       ▼
Aiven Kafka
       │
       ▼
Spark Structured Streaming
       │
       ▼
DataStax Astra DB
       │
       ▼
FastAPI
       │
       ▼
WebSocket
       │
       ▼
React Dashboard
```

---

## 🏗️ System Architecture

```text
┌──────────────────────────┐
│    Python Log Generator  │
│                          │
│   INFO / WARN / ERROR    │
└────────────┬─────────────┘
             │
             │ JSON Events
             ▼
┌──────────────────────────┐
│       Aiven Kafka        │
│                          │
│  Topic: application-logs │
└────────────┬─────────────┘
             │
             │ Streaming Events
             ▼
┌───────────────────────────────┐
│   Spark Structured Streaming  │
│                               │
│  • Read Kafka stream          │
│  • Parse JSON logs            │
│  • Aggregate service metrics  │
│  • Calculate error statistics │
│  • Calculate response times   │
└────────────┬──────────────────┘
             │
             │ Aggregated Metrics
             ▼
┌──────────────────────────────┐
│      DataStax Astra DB       │
│                              │
│   Cassandra-compatible DB    │
│                              │
│      service_metrics         │
└────────────┬─────────────────┘
             │
             │ Query Metrics
             ▼
┌──────────────────────────┐
│         FastAPI          │
│                          │
│  • REST APIs             │
│  • Health Check          │
│  • WebSocket             │
└────────────┬─────────────┘
             │
             │ WebSocket
             ▼
┌──────────────────────────┐
│     React Dashboard      │
│                          │
│  • Live Metrics          │
│  • Error Counts          │
│  • Warning Counts        │
│  • Response Times        │
│  • Service Status        │
└──────────────────────────┘
```

### Cloud vs Local Components

```text
                    StreamPulse Architecture

        LOCAL / DOCKER                  CLOUD
        ──────────────                  ─────

┌───────────────────────┐
│ Python Producer       │
└──────────┬────────────┘
           │
           ▼
                              ┌──────────────────────┐
                              │   Aiven Kafka ☁️     │
                              └──────────┬───────────┘
                                         │
┌───────────────────────┐                │
│ Spark Streaming       │◄───────────────┘
└──────────┬────────────┘
           │
           ▼
                              ┌──────────────────────┐
                              │   Astra DB ☁️        │
                              │ Cassandra-compatible  │
                              └──────────┬───────────┘
                                         │
┌───────────────────────┐                │
│ FastAPI               │◄───────────────┘
└──────────┬────────────┘
           │
           │ WebSocket
           ▼
┌───────────────────────┐
│ React Dashboard       │
└───────────────────────┘
```

---

## 🛠️ Tech Stack

| Technology | Purpose |
| ------------------------------ | ------------------------------------------------ |
| **Python** | Log generation and streaming development |
| **Apache Kafka** | Real-time event streaming |
| **Aiven Kafka** | Managed cloud Kafka cluster |
| **Apache Spark** | Distributed stream processing |
| **Spark Structured Streaming** | Continuous Kafka stream processing |
| **DataStax Astra DB** | Cloud Cassandra-compatible storage |
| **Apache Cassandra** | Database technology used by Astra DB |
| **FastAPI** | REST API and WebSocket backend |
| **React** | Real-time monitoring dashboard |
| **WebSocket** | Live metric updates |
| **Docker** | Containerization |
| **Docker Compose** | Local multi-container orchestration |
| **Git / GitHub** | Version control and project hosting |

---

## ✨ Features

### 1. Real-Time Log Generation

The Python producer continuously generates application logs for multiple services:

- `auth-service`
- `payment-service`
- `user-service`
- `order-service`
- `inventory-service`

Each generated log contains:

```json
{
  "timestamp": "2026-10-03T08:00:00+00:00",
  "service": "payment-service",
  "level": "ERROR",
  "message": "Payment processing failed",
  "status_code": 500,
  "response_time_ms": 842
}
```

Supported log levels:

- `INFO`
- `WARN`
- `ERROR`

---

## 📡 Kafka Streaming

Apache Kafka acts as the event streaming layer between the log generator and Spark.

The project uses **Aiven Kafka**, a managed cloud Kafka service.

### Kafka Topic

```text
application-logs
```

The topic uses multiple partitions to allow parallel processing of incoming events.

Kafka decouples the log producer from the downstream stream-processing system.

```text
Producer
   │
   ▼
Aiven Kafka
   │
   ├── Partition 0
   └── Partition 1
```

The producer and Spark Streaming authenticate with Aiven Kafka using secure SASL/SSL connections.

---

## ⚡ Spark Structured Streaming

Spark Structured Streaming continuously consumes log events from Kafka.

The streaming pipeline:

1. Reads events from Kafka
2. Parses the JSON log structure
3. Converts fields into appropriate data types
4. Groups logs by service
5. Calculates service-level metrics
6. Writes aggregated metrics to Astra DB

### Service Metrics

StreamPulse calculates:

- Total logs
- Error count
- Warning count
- Average response time
- Maximum response time
- Event timestamp

### Example

```text
Service: payment-service

Total Logs:        325
Errors:             29
Warnings:           74
Avg Response:     545.20 ms
Max Response:     1000 ms
```

Spark Structured Streaming uses checkpointing to maintain streaming state and processing progress.

---

## ☁️ DataStax Astra DB

StreamPulse uses **DataStax Astra DB**, a managed cloud database based on Apache Cassandra, to persist aggregated service metrics.

This replaces the earlier local Cassandra container while retaining Cassandra's data model and Spark Cassandra Connector integration.

### Keyspace

```text
streampulse
```

### Table

```text
service_metrics
```

### Table Structure

| Column | Type | Description |
|---|---|---|
| `service` | text | Service name |
| `event_time` | timestamp | Metric event timestamp |
| `total_logs` | int | Total logs processed |
| `error_count` | int | Number of ERROR logs |
| `warn_count` | int | Number of WARN logs |
| `avg_response_time` | double | Average response time |
| `max_response_time` | int | Maximum response time |

The primary key is:

```text
PRIMARY KEY (service, event_time)
```

The backend retrieves the latest available metrics for each service.

---

## 🔌 FastAPI Backend

FastAPI provides the backend API layer between Astra DB and the React frontend.

The backend connects to Astra DB using the Cassandra Python driver and Astra Secure Connect Bundle.

### API Endpoints

#### Health Check

```text
GET /health
```

Returns the health status of the backend and database connection.

Example:

```json
{
  "status": "healthy",
  "database": "connected"
}
```

#### All Service Metrics

```text
GET /metrics
```

Returns the latest metrics for all monitored services.

Example:

```json
[
  {
    "service": "payment-service",
    "event_time": "2026-10-06T14:06:13.579000",
    "total_logs": 325,
    "error_count": 29,
    "warn_count": 74,
    "avg_response_time": 545.20,
    "max_response_time": 1000,
    "status": "CRITICAL"
  }
]
```

#### Service-Specific Metrics

```text
GET /metrics/{service}
```

Example:

```text
GET /metrics/payment-service
```

Returns the latest metrics for the requested service.

### WebSocket

```text
/ws
```

The React dashboard connects to this endpoint to receive live metric updates.

Instead of repeatedly polling the API, the dashboard maintains a persistent WebSocket connection with the backend.

---

## 📊 React Dashboard

The React frontend provides a real-time monitoring interface.

The dashboard displays:

- Total Logs
- Total Errors
- Total Warnings
- Average Response Time
- Warning Services
- Critical Services
- Per-service metrics
- Service health status
- Live update status

### Real-Time Communication

```text
FastAPI
   │
   │ WebSocket
   ▼
React Dashboard
   │
   └── Live metric updates
```

The dashboard receives continuously updated metrics without requiring manual page refreshes.

---

## 🟢 Service Health Status

StreamPulse derives a service status from its error and warning metrics.

### Current Classification Logic

```text
Error Count >= 10
       │
       ▼
   CRITICAL


Warnings or Errors
       │
       ▼
   WARNING


No Warnings or Errors
       │
       ▼
   HEALTHY
```

This provides a simple way to identify services that require immediate attention.

---

## 🐳 Docker Architecture

Docker is used to containerize the local compute components of StreamPulse.

### Docker Containers

The current Docker Compose stack contains:

- `streampulse-spark`
- `streampulse-spark-streaming`
- `streampulse-producer`
- `streampulse-backend`
- `streampulse-frontend`

Kafka and Cassandra are **not run as local Docker containers** in the current architecture.

Instead:

- Kafka → **Aiven Kafka**
- Database → **DataStax Astra DB**

Docker Compose manages:

- Container creation
- Local networking
- Port mappings
- Environment variables
- Service dependencies
- Health checks
- Restart policies

---

## 📁 Project Structure

```text
streampulse/
│
├── backend/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── frontend/
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│
├── infrastructure/
│   └── docker-compose.yml
│
├── producer/
│   ├── Dockerfile
│   ├── log_generator.py
│   ├── requirements.txt
│   └── ca.pem
│
├── streaming/
│   ├── spark_streaming.py
│   ├── certs/
│   │   └── secure-connect-streampulse.zip
│   └── checkpoint/
│
├── tests/
│
├── docs/
│   ├── dashboard.png
│   ├── service-metrics.png
│   ├── avg-response-time.png
│   └── error-count.png
│
├── .env
├── .gitignore
└── README.md
```

> **Security:** `.env` contains credentials for cloud services and is excluded from Git using `.gitignore`. Credentials and access tokens should never be committed to the repository.

---

## ▶️ Running the Project Locally

### Prerequisites

Install:

- **Docker Desktop**
- **Git**

Ensure Docker Desktop is running before starting the application.

The project also requires active credentials for:

- Aiven Kafka
- DataStax Astra DB

### Environment Variables

Create a `.env` file in the project root:

```env
KAFKA_SERVER=<AIVEN_KAFKA_SERVER>
KAFKA_USERNAME=<AIVEN_KAFKA_USERNAME>
KAFKA_PASSWORD=<AIVEN_KAFKA_PASSWORD>
KAFKA_CA_FILE=producer/ca.pem
ASTRA_TOKEN=<ASTRA_APPLICATION_TOKEN>
```

Do not commit this file to GitHub.

### Start the Complete Stack

From the project root:

```bash
docker compose --env-file .env -f infrastructure/docker-compose.yml up -d --build
```

This starts the local Docker components of the StreamPulse platform.

### Check Containers

```bash
docker ps
```

The following containers should be active and running:

```text
streampulse-spark
streampulse-spark-streaming
streampulse-producer
streampulse-backend
streampulse-frontend
```

Kafka and Astra DB are managed externally through their cloud services.

### Open the Dashboard

Open your browser and navigate to:

```text
http://localhost:5173
```

The dashboard automatically connects to the backend WebSocket and streams live metrics.

---

## 🔍 Useful Docker Commands

### View All Running Containers

```bash
docker ps
```

### View Producer Logs

```bash
docker logs streampulse-producer
```

### View Spark Streaming Logs

```bash
docker logs streampulse-spark-streaming
```

### View Backend Logs

```bash
docker logs streampulse-backend
```

### View Frontend Logs

```bash
docker logs streampulse-frontend
```

### Check Backend Health

```bash
curl http://localhost:8001/health
```

### Get Current Metrics

```bash
curl http://localhost:8001/metrics
```

### Stop the Complete Stack

```bash
docker compose --env-file .env -f infrastructure/docker-compose.yml down
```

### Start the Stack Again

```bash
docker compose --env-file .env -f infrastructure/docker-compose.yml up -d
```

---

## 🔄 End-to-End Data Flow

A typical application log follows this path:

```text
1. Python Log Generator
           │
           │ JSON Event
           ▼
2. Aiven Kafka
           │
           │ Streaming Event
           ▼
3. Spark Structured Streaming
           │
           │ Aggregated Metrics
           ▼
4. DataStax Astra DB
           │
           │ Latest Metrics
           ▼
5. FastAPI
           │
           │ WebSocket
           ▼
6. React Dashboard
```

This transforms raw application logs into real-time service-level monitoring metrics.

---

## 🩺 Health Checks

Docker health checks are configured for critical local services.

### Backend

FastAPI exposes:

```text
GET /health
```

Docker uses this endpoint to determine backend readiness.

The frontend depends on the backend health check before starting.

### Cloud Services

Aiven Kafka and Astra DB are externally managed services.

Their availability and connectivity are validated by the producer, Spark Streaming, and backend connections respectively.

---

## 🧩 Why These Technologies?

### Apache Kafka

Kafka provides a distributed event-streaming layer and decouples log generation from stream processing.

### Aiven Kafka

Aiven provides managed Kafka infrastructure, allowing StreamPulse to use a cloud-hosted Kafka cluster without maintaining a Kafka broker locally.

### Apache Spark

Spark Structured Streaming provides distributed, fault-tolerant processing for continuously arriving log events.

### DataStax Astra DB

Astra DB provides managed cloud storage using the Cassandra data model, making it suitable for high-volume metric writes and distributed applications.

### FastAPI

FastAPI provides a high-performance backend API and WebSocket server.

### React

React provides an interactive, responsive user interface for visualizing real-time metrics.

### Docker & Docker Compose

Docker isolates dependencies and creates reproducible environments, while Docker Compose orchestrates the local application components.

---

## 🔐 Reliability

StreamPulse includes several reliability mechanisms:

- Kafka topic partitioning
- Spark Structured Streaming checkpointing
- Cloud persistence through Astra DB
- Docker health checks
- Docker restart policies
- Service dependency management
- Backend health validation endpoint
- WebSocket-based live communication
- Secure SASL/SSL connection to Aiven Kafka
- Secure Astra DB authentication using an application token and Secure Connect Bundle

---

## 🚧 Future Improvements

Planned improvements include:

- Real-time anomaly detection
- Configurable alert thresholds
- Email and Slack alerts
- Historical metric analysis
- Time-series visualizations
- Service-level filtering and custom search
- Authentication and authorization using JWT/OAuth
- Automated unit and integration testing pipelines
- Production Kubernetes (K8s) deployment
- Public cloud deployment
- CI/CD pipeline
- Centralized application observability

---

## 📸 Dashboard

StreamPulse provides a real-time monitoring dashboard for observing application services and their performance metrics.

### Live Monitoring Dashboard

![StreamPulse Dashboard](docs/dashboard.png)

### Service Metrics

![Service Metrics](docs/service-metrics.png)

### Average Response Time

![Average Response Time by Service](docs/avg-response-time.png)

### Error Count

![Error Count by Service](docs/error-count.png)

---

## 🎯 Project Goals

StreamPulse was built to demonstrate practical experience with:

- Distributed systems design
- Event-driven architecture
- Real-time streaming data pipelines
- Spark Structured Streaming and Kafka integration
- NoSQL data modeling using Cassandra-compatible storage
- Cloud-managed Kafka and database services
- REST APIs and WebSockets
- Full-stack dashboard integration
- Containerized application development
- Multi-container orchestration with Docker
- Real-time system monitoring

---

## 📌 Current Project Status

**Status: Local Deployment — Fully Functional**

The complete StreamPulse pipeline is currently running as a Dockerized application with cloud-managed Kafka and database services.

The current implementation includes:

- Real-time log generation
- Aiven Kafka event streaming
- Spark Structured Streaming
- Astra DB persistence
- FastAPI REST APIs
- WebSocket communication
- React real-time dashboard
- Docker Compose orchestration
- Container health checks
- Secure cloud connectivity

### Current Architecture Summary

```text
Python Producer
      │
      ▼
Aiven Kafka ☁️
      │
      ▼
Spark Structured Streaming
      │
      ▼
DataStax Astra DB ☁️
      │
      ▼
FastAPI
      │
      ▼
WebSocket
      │
      ▼
React Dashboard
```

---

## 👩‍💻 Author

**Ishika Srivastava**

---

## ⭐ Future Deployment

The project is designed to be deployed as a production-style distributed application.

Planned deployment architecture:

```text
React Dashboard
      ↓
Frontend Hosting
      ↓
FastAPI Backend
      ↓
Astra DB ☁️

Python Producer
      ↓
Aiven Kafka ☁️
      ↓
Spark Structured Streaming
```

### Deployment Goals

- Public live dashboard
- Public API endpoint
- Cloud-hosted compute
- Managed Kafka
- Managed Cassandra-compatible database
- HTTPS
- Production environment variables and secrets
- CI/CD

**Live Demo:** Coming Soon

---

## 📄 License

This project is currently intended as a personal portfolio and learning project.