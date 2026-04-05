"""
kafka_producer.py
------------------
Step 2 – Streaming Layer

Wraps kafka-python's KafkaProducer into a simple class that:
  1. Connects to the broker with retry logic
  2. Serialises event dicts to JSON bytes
  3. Sends each event to the 'cctv-hand-events' topic
  4. Uses a delivery callback to confirm or log failures
  5. Exposes a clean send() and close() API

This module is intentionally kept separate from gesture detection
so it can also be imported by test scripts or a replay tool.
"""

import json
import time
import logging
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

from kafka_config import KAFKA_BROKER, TOPIC_RAW_EVENTS, PRODUCER_CONFIG

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("KafkaEventProducer")


# ── Serialiser helpers ────────────────────────────────────────
def _json_serialiser(value: dict) -> bytes:
    """Convert a Python dict → UTF-8 encoded JSON bytes."""
    return json.dumps(value, default=str).encode("utf-8")


def _str_serialiser(value: str) -> bytes:
    """Convert a string key → UTF-8 bytes."""
    return value.encode("utf-8") if value else b""


# ── Delivery callbacks ────────────────────────────────────────
def _on_send_success(record_metadata):
    log.debug(
        "Delivered → topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def _on_send_error(excp):
    log.error("Delivery FAILED: %s", excp)


# ── Producer class ────────────────────────────────────────────
class GestureEventProducer:
    """
    Kafka producer for CCTV gesture events.

    Usage
    -----
        producer = GestureEventProducer()
        producer.send(event_dict)   # called for every confirmed gesture
        producer.close()            # flush + shut down on exit

    The event dict schema (from Step 1 build_event()):
        {
            "camera_id":    int,
            "gesture":      str,   e.g. "high_sign"
            "danger_level": str,   e.g. "HIGH"
            "priority":     int,   0-3
            "timestamp":    str,   ISO-8601
        }

    Kafka message layout:
        key   → "cam-<camera_id>"  (enables per-camera partitioning)
        value → full event JSON
    """

    def __init__(self,
                 topic: str = TOPIC_RAW_EVENTS,
                 max_retries: int = 5,
                 retry_delay: float = 3.0):
        self.topic       = topic
        self._producer   = None
        self._sent       = 0
        self._failed     = 0
        self._connect(max_retries, retry_delay)

    # ── connection ────────────────────────────────────────────

    def _connect(self, max_retries: int, retry_delay: float):
        """
        Attempt to connect to Kafka, retrying if the broker is not
        yet ready (common during Docker startup).
        """
        cfg = dict(PRODUCER_CONFIG)
        cfg["value_serializer"] = _json_serialiser
        cfg["key_serializer"]   = _str_serialiser

        for attempt in range(1, max_retries + 1):
            try:
                self._producer = KafkaProducer(**cfg)
                log.info(
                    "Connected to Kafka broker at %s  (topic: %s)",
                    KAFKA_BROKER, self.topic,
                )
                return
            except NoBrokersAvailable:
                log.warning(
                    "Kafka broker not available – attempt %d/%d. "
                    "Retrying in %.0fs…",
                    attempt, max_retries, retry_delay,
                )
                time.sleep(retry_delay)

        raise RuntimeError(
            f"Cannot reach Kafka broker at {KAFKA_BROKER} "
            f"after {max_retries} attempts.\n"
            "  → Is Kafka running?  Run:  docker-compose up -d"
        )

    # ── public API ────────────────────────────────────────────

    def send(self, event: dict):
        """
        Asynchronously send one gesture event to Kafka.

        The message key is 'cam-<camera_id>' so events from the same
        camera always land in the same partition (preserving order).
        """
        if self._producer is None:
            log.error("Producer not connected – event dropped: %s", event)
            return

        key = f"cam-{event.get('camera_id', 0)}"

        (self._producer
             .send(self.topic, key=key, value=event)
             .add_callback(_on_send_success)
             .add_errback(_on_send_error))

        self._sent += 1

        # Print to console so we can see the stream live
        icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(
            event.get("danger_level", ""), "⚪"
        )
        log.info(
            "%s  SENT → key=%-8s  gesture=%-14s  level=%-7s  priority=%d  ts=%s",
            icon,
            key,
            event.get("gesture", "?"),
            event.get("danger_level", "?"),
            event.get("priority", 0),
            event.get("timestamp", "")[-8:],   # HH:MM:SS
        )

    def flush(self):
        """Block until all buffered messages are delivered."""
        if self._producer:
            self._producer.flush()
            log.info("Producer flushed – all messages delivered.")

    def close(self):
        """Flush remaining messages and shut down the producer."""
        if self._producer:
            log.info(
                "Closing producer.  Total sent: %d  |  Failed: %d",
                self._sent, self._failed,
            )
            self._producer.flush(timeout=10)
            self._producer.close()
            self._producer = None

    def stats(self) -> dict:
        return {"sent": self._sent, "failed": self._failed}

    # ── context manager support ───────────────────────────────
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
