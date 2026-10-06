import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import os
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.io.asyncioreactor import AsyncioConnection

app = FastAPI(title="StreamPulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")


ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
ASTRA_SECURE_CONNECT_BUNDLE = os.getenv(
    "ASTRA_SECURE_CONNECT_BUNDLE",
    "/app/certs/secure-connect-streampulse.zip"
)

cloud_config = {
    "secure_connect_bundle": ASTRA_SECURE_CONNECT_BUNDLE
}

auth_provider = PlainTextAuthProvider(
    username="token",
    password=ASTRA_TOKEN
)

cluster = Cluster(
    cloud=cloud_config,
    auth_provider=auth_provider
)

session = cluster.connect("streampulse")


@app.get("/")
def root():
    return {
        "message": "StreamPulse API is running"
    }


@app.get("/metrics")
def get_metrics():

    rows = session.execute("""
        SELECT service,
               event_time,
               total_logs,
               error_count,
               warn_count,
               avg_response_time,
               max_response_time
        FROM service_metrics
    """)

    # Keep only the latest snapshot for each service
    latest = {}

    for row in rows:
        if (
            row.service not in latest
            or row.event_time > latest[row.service].event_time
        ):
            latest[row.service] = row

    rows = latest.values()

    metrics = []

    for row in rows:

        if row.error_count >= 10:
            status = "CRITICAL"
        elif row.error_count > 0 or row.warn_count > 0:
            status = "WARNING"
        else:
            status = "HEALTHY"

        metrics.append({
            "service": row.service,
            "event_time": row.event_time.isoformat(),
            "total_logs": row.total_logs,
            "error_count": row.error_count,
            "warn_count": row.warn_count,
            "avg_response_time": row.avg_response_time,
            "max_response_time": row.max_response_time,
            "status": status
        })

    return metrics


@app.get("/metrics/{service}")
def get_service_metrics(service: str):

    rows = session.execute(
        """
        SELECT service,
               event_time,
               total_logs,
               error_count,
               warn_count,
               avg_response_time,
               max_response_time
        FROM service_metrics
        WHERE service = %s
        """,
        (service,)
    )

    metrics = []

    for row in rows:

        if row.error_count >= 10:
            status = "CRITICAL"
        elif row.error_count > 0 or row.warn_count > 0:
            status = "WARNING"
        else:
            status = "HEALTHY"

        metrics.append({
            "service": row.service,
            "event_time": row.event_time.isoformat(),
            "total_logs": row.total_logs,
            "error_count": row.error_count,
            "warn_count": row.warn_count,
            "avg_response_time": row.avg_response_time,
            "max_response_time": row.max_response_time,
            "status": status
        })

    return metrics


@app.get("/health")
def health_check():

    try:
        session.execute("SELECT release_version FROM system.local")

        return {
            "status": "healthy",
            "database": "connected"
        }

    except Exception:

        return {
            "status": "unhealthy",
            "database": "disconnected"
        }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:

            rows = session.execute("""
                SELECT service,
                       event_time,
                       total_logs,
                       error_count,
                       warn_count,
                       avg_response_time,
                       max_response_time
                FROM service_metrics
            """)

            latest = {}

            for row in rows:
                if (
                    row.service not in latest
                    or row.event_time > latest[row.service].event_time
                ):
                    latest[row.service] = row

            metrics = []

            for row in latest.values():

                if row.error_count >= 10:
                    status = "CRITICAL"
                elif row.error_count > 0 or row.warn_count > 0:
                    status = "WARNING"
                else:
                    status = "HEALTHY"

                metrics.append({
                    "service": row.service,
                    "event_time": row.event_time.isoformat(),
                    "total_logs": row.total_logs,
                    "error_count": row.error_count,
                    "warn_count": row.warn_count,
                    "avg_response_time": row.avg_response_time,
                    "max_response_time": row.max_response_time,
                    "status": status
                })

            await websocket.send_json(metrics)

            await asyncio.sleep(5)

    except WebSocketDisconnect:
        print("WebSocket client disconnected")