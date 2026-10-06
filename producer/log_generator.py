import json
import random
import time
import os
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_FILE = os.getenv("KAFKA_CA_FILE", "ca.pem")

KAFKA_TOPIC = "application-logs"


services = [
    "auth-service",
    "payment-service",
    "user-service",
    "order-service",
    "inventory-service",
]


log_templates = {
    "INFO": [
        "Request processed successfully",
        "User logged in successfully",
        "Order created successfully",
        "Product inventory fetched",
    ],
    "WARN": [
        "Response time is higher than expected",
        "Inventory level is low",
        "Repeated login attempt detected",
    ],
    "ERROR": [
        "Payment processing failed",
        "Database connection failed",
        "Request processing failed",
        "Unable to fetch inventory",
    ],
}


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    ssl_cafile=KAFKA_CA_FILE,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_log():
    level = random.choices(
        ["INFO", "WARN", "ERROR"],
        weights=[70, 20, 10],
    )[0]

    service = random.choice(services)

    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "level": level,
        "message": random.choice(log_templates[level]),
        "status_code": (
            200 if level == "INFO"
            else 400 if level == "WARN"
            else 500
        ),
        "response_time_ms": random.randint(50, 1000),
    }

    return log


while True:
    log = generate_log()

    producer.send(
        KAFKA_TOPIC,
        value=log,
    )

    print(f"Sent: {json.dumps(log)}")

    time.sleep(1)