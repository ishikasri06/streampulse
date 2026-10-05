# StreamPulse — Real-Time Distributed Log Analytics & Monitoring Platform

StreamPulse is a real-time distributed log analytics and monitoring platform that collects application logs, processes them as a continuous stream, stores aggregated service metrics, and displays them through a live monitoring dashboard.

The project demonstrates a complete event-driven data pipeline using **Apache Kafka, Apache Spark Structured Streaming, Apache Cassandra, FastAPI, React, WebSockets, and Docker**.

---

## 🚀 Project Overview

Modern distributed applications generate a large volume of logs across multiple services. Processing these logs manually or in batches makes it difficult to identify errors, monitor response times, and understand the current health of services.

StreamPulse addresses this by continuously:

1. Generating application logs
2. Publishing logs to Apache Kafka
3. Processing the stream using Spark Structured Streaming
4. Aggregating service-level metrics
5. Persisting metrics in Apache Cassandra
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
Apache Kafka
       │
       ▼
Spark Structured Streaming
       │
       ▼
Apache Cassandra
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
│       Apache Kafka       │
│                          │
│ Topic: application-logs  │
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
┌──────────────────────────┐
│     Apache Cassandra     │
│                          │
│     service_metrics      │
└────────────┬─────────────┘
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

---

## 🛠️ Tech Stack


| Technology                     | Purpose                                          |
| ------------------------------ | ------------------------------------------------ |
| **Python**                     | Log generation and backend/streaming development |
| **Apache Kafka**               | Real-time event streaming                        |
| **Apache Spark**               | Distributed stream processing                    |
| **Spark Structured Streaming** | Continuous Kafka stream processing               |
| **Apache Cassandra**           | Distributed storage for service metrics          |
| **FastAPI**                    | REST API and WebSocket backend                   |
| **React**                      | Real-time monitoring dashboard                   |
| **WebSocket**                  | Live metric updates                              |
| **Docker**                     | Containerization                                 |
| **Docker Compose**             | Multi-container orchestration                    |
| **Git / GitHub**               | Version control and project hosting              |


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

### Kafka Topic

`application-logs`

The topic is configured with multiple partitions to allow parallel processing of incoming events.

Kafka decouples the log producer from the downstream stream-processing system.

```text
Producer
   │
   ▼
Kafka Topic
   │
   ├── Partition 0
   ├── Partition 1
   └── Partition 2
```

---

## ⚡ Spark Structured Streaming

Spark Structured Streaming continuously consumes log events from Kafka.

The streaming pipeline:

1. Reads events from Kafka
2. Parses the JSON log structure
3. Converts fields into appropriate data types
4. Groups logs by service
5. Calculates service-level metrics
6. Writes aggregated metrics to Cassandra

### Service Metrics

StreamPulse calculates:

- Total logs
- Error count
- Warning count
- Average response time
- Maximum response time
- Event timestamp

**Example:**

```text
Service: payment-service

Total Logs:        258
Errors:             24
Warnings:           42
Avg Response:   532.70 ms
Max Response:      998 ms
```

---

## 🗄️️ Cassandra

Apache Cassandra is used to persist the aggregated service metrics.

### Keyspace

`streampulse`

### Table

`service_metrics`

### Table Structure

- `service`
- `event_time`
- `total_logs`
- `error_count`
- `warn_count`
- `avg_response_time`
- `max_response_time`

The backend retrieves the latest available metrics for each service.

---

## 🔌 FastAPI Backend

FastAPI provides the backend API layer between Cassandra and the React frontend.

### API Endpoints

#### Health Check

`GET /health`

Returns the health of the backend and its Cassandra connection.

**Example Response:**

```json
{
  "status": "healthy",
  "database": "connected"
}
```

#### All Service Metrics

`GET /metrics`

Returns the latest metrics for all monitored services.

**Example Response:**

```json
[
  {
    "service": "payment-service",
    "event_time": "2026-10-03T08:36:16.079000",
    "total_logs": 258,
    "error_count": 24,
    "warn_count": 42,
    "avg_response_time": 532.70,
    "max_response_time": 998,
    "status": "CRITICAL"
  }
]
```

#### Service-Specific Metrics

`GET /metrics/{service}`

**Example:**
`GET /metrics/payment-service`

Returns the latest metrics for the requested service.

### WebSocket

`/ws`

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

All major components run as Docker containers.

### Containers

- `streampulse-kafka`
- `streampulse-spark`
- `streampulse-spark-streaming`
- `streampulse-cassandra`
- `streampulse-producer`
- `streampulse-backend`
- `streampulse-frontend`

Docker Compose manages:

- Container creation
- Networking
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
│   └── requirements.txt
│
├── streaming/
│   ├── spark_streaming.py
│   └── checkpoint/
│
├── tests/
│
├── docs/
│
└── README.md
```

---

## ▶️ Running the Project Locally

### Prerequisites

Install:

- **Docker Desktop**
- **Git**

Ensure Docker Desktop is running before starting the application.

### Start the Complete Stack

From the project root:

```bash
docker compose -f infrastructure/docker-compose.yml up -d --build
```

This starts the complete StreamPulse platform.

### Check Containers

```bash
docker ps
```

The following containers should be active and running:

- `streampulse-kafka`
- `streampulse-spark`
- `streampulse-spark-streaming`
- `streampulse-cassandra`
- `streampulse-producer`
- `streampulse-backend`
- `streampulse-frontend`

### Open the Dashboard

Open your browser and navigate to:

[http://localhost:5173](http://localhost:5173)

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

### Stop the Complete Stack

```bash
docker compose -f infrastructure/docker-compose.yml down
```

### Start the Stack Again

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

---

## 🔄 End-to-End Data Flow

A typical application log follows this path:

```text
1. Python Log Generator
           │
           │ JSON Event
           ▼
2. Apache Kafka
           │
           │ Streaming Event
           ▼
3. Spark Structured Streaming
           │
           │ Aggregated Metrics
           ▼
4. Apache Cassandra
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

Docker health checks are configured for critical services:

### Cassandra

Cassandra is validated via a CQL query to verify the database is actively accepting connections.

### Kafka

Kafka is checked using a broker API request to verify that the broker is available.

### Backend

FastAPI exposes `/health`. Docker uses this endpoint to determine backend readiness. The frontend waits for this check before starting.

---

## 🧩 Why These Technologies?

### Apache Kafka

Kafka provides a distributed event-streaming layer and decouples log generation from stream processing.

### Apache Spark

Spark Structured Streaming provides distributed, fault-tolerant processing for continuously arriving log events.

### Apache Cassandra

Cassandra provides distributed NoSQL storage optimized for high-write-volume metric data.

### FastAPI

FastAPI provides a high-performance backend API and native asynchronous WebSocket server.

### React

React provides an interactive, responsive user interface for visualizing real-time metrics.

### Docker & Docker Compose

Docker isolates dependencies and creates reproducible environments, while Docker Compose orchestrates multi-container operations.

---

## 🔐 Reliability

StreamPulse includes several reliability mechanisms:

- Kafka topic partitioning
- Spark Structured Streaming checkpointing
- Cassandra data persistence through Docker volumes
- Docker health checks & restart policies
- Service dependency management
- Backend health validation endpoint
- WebSocket-based live communication with auto-reconnection

---

## 🚧 Future Improvements

Planned improvements include:

- Real-time anomaly detection
- Configurable alert thresholds (Email and Slack alerts)
- Historical metric analysis & time-series visualizations
- Service-level filtering & custom search
- Authentication and authorization (JWT/OAuth)
- Automated unit and integration testing pipelines
- Production Kubernetes (K8s) deployment manifests

---

## 📸 Dashboard

### Live Monitoring Dashboard

Add the dashboard screenshot here after uploading it to the repository:

```markdown
![StreamPulse Dashboard](docs/assets/dashboard-preview.png)
```

**Frontend URL:** [http://localhost:5173](http://localhost:5173)

---

## 🎯 Project Goals

StreamPulse was built to demonstrate practical experience with:

- Distributed systems design
- Event-driven architecture
- Real-time streaming data pipelines
- Spark Structured Streaming & Kafka integration
- NoSQL data modeling in Cassandra
- Async REST APIs & WebSockets
- Full-stack dashboard integration
- Multi-container orchestration with Docker

---

## 📌 Current Project Status

**Status:** Local Deployment — Fully Functional

The complete StreamPulse pipeline is running as a Dockerized multi-container application.

The current implementation includes:

- Real-time log generation
- Kafka event streaming
- Spark Structured Streaming
- Cassandra persistence
- FastAPI REST APIs
- WebSocket communication
- React real-time dashboard
- Docker Compose orchestration
- Container health checks

### Current Architecture Summary

```text
Python Producer ──> Apache Kafka ──> Spark Structured Streaming ──> Apache Cassandra ──> FastAPI ──> WebSocket ──> React Dashboard
```

---

## 👩‍💻 Author

**Ishika Srivastava**  
Data Engineer | Software Development | Distributed Systems

---

## ⭐ Future Deployment

The project is designed to be deployed as a production-style distributed application.

- **Live Demo:** Coming Soon

---

## 📄 License

This project is currently intended as a personal portfolio and learning project.

```

```

