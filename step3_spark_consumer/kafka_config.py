"""
kafka_config.py
----------------
Step 2 – Shared Kafka Configuration

All Kafka settings live here so both the producer (Step 2)
and the Spark consumer (Step 3) import from one place.
Change KAFKA_BROKER if your Kafka runs on a different host/port.
"""

# ── Broker ────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"      # default Docker / local Kafka address

# ── Topics ────────────────────────────────────────────────────
TOPIC_RAW_EVENTS  = "cctv-hand-events"       # raw gesture events  (producer → Spark)
TOPIC_ALERTS      = "cctv-alerts"            # filtered HIGH/CRITICAL (Spark → consumer)

# ── Producer tuning ──────────────────────────────────────────
PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BROKER,

    # Serialise Python dicts → UTF-8 JSON bytes automatically
    # (set in producer code, not here, but documented for reference)

    # Reliability: wait for leader + 1 replica to acknowledge
    "acks": "all",

    # Retry up to 3 times on transient network errors
    "retries": 3,

    # Batch small messages together for efficiency (bytes)
    "batch_size": 16384,

    # Wait up to 10 ms to fill a batch before sending
    "linger_ms": 10,

    # Compress batches with snappy for lower bandwidth
    "compression_type": "gzip",
}

# ── Consumer group (used by Step 3 non-Spark consumer) ───────
CONSUMER_GROUP = "cctv-alert-group"
