"""
test_connections.py
--------------------
Run this BEFORE submit.bat/ps1 to verify all services are reachable.
Checks Kafka, MongoDB, and HDFS independently so you know exactly
which one is failing before Spark starts.

Usage:
    python test_connections.py
"""

import sys

print("=" * 58)
print("  CCTV Stack – Connection Diagnostics")
print("=" * 58)

all_ok = True

# ── 1. Kafka ─────────────────────────────────────────────────
print("\n[1/3] Kafka  (localhost:9092)...")
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    p = KafkaProducer(bootstrap_servers="localhost:9092",
                      request_timeout_ms=5000,
                      api_version_auto_timeout_ms=5000)
    p.close()
    print("      ✅  Kafka reachable")
except NoBrokersAvailable:
    print("      ❌  Kafka NOT reachable")
    print("          → Run: docker-compose up -d")
    print("          → Wait 30s then retry")
    all_ok = False
except Exception as e:
    print(f"      ❌  Kafka error: {e}")
    all_ok = False

# ── 2. MongoDB ───────────────────────────────────────────────
print("\n[2/3] MongoDB  (localhost:27017)...")
try:
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017",
                         serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.close()
    print("      ✅  MongoDB reachable")
except Exception as e:
    print(f"      ❌  MongoDB NOT reachable: {e}")
    print("          → Run: docker-compose up -d")
    all_ok = False

# ── 3. HDFS ──────────────────────────────────────────────────
print("\n[3/3] HDFS NameNode  (localhost:9870)...")
try:
    import urllib.request
    urllib.request.urlopen("http://localhost:9870", timeout=5)
    print("      ✅  HDFS NameNode web UI reachable")
except Exception as e:
    print(f"      ❌  HDFS NameNode NOT reachable: {e}")
    print("          → Run: docker-compose up -d")
    print("          → Wait 30s for HDFS to initialise")
    all_ok = False

# ── 4. HDFS DataNode block port ──────────────────────────────
print("\n[3b]  HDFS DataNode block port  (localhost:9866)...")
try:
    import socket
    s = socket.create_connection(("localhost", 9866), timeout=5)
    s.close()
    print("      ✅  DataNode port 9866 reachable")
    print("          (Spark can write data blocks)")
except Exception as e:
    print(f"      ❌  DataNode port 9866 NOT reachable: {e}")
    print("          → This means HDFS writes will fail")
    print("          → Fix: docker-compose down -v && docker-compose up -d")
    all_ok = False

# ── Summary ───────────────────────────────────────────────────
print("\n" + "=" * 58)
if all_ok:
    print("  ✅  All services OK – safe to run submit.bat")
else:
    print("  ❌  Some services failed – fix above before running Spark")
print("=" * 58 + "\n")

sys.exit(0 if all_ok else 1)
