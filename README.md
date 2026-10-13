# CCTV Gesture Alert System

This repository contains a prototype CCTV gesture alert system that:
- Detects hand gestures from webcam(s) using MediaPipe + OpenCV
- Confirms gestures (hold to confirm) and assigns a danger priority
- Streams confirmed gesture events to Kafka
- Processes events with PySpark Structured Streaming, writes raw events to HDFS, and persists alerts and all events to MongoDB

This project is useful as a learning / demo pipeline combining real-time computer vision and big-data streaming components.

## Structure
Key files (under `step3_spark_consumer/`):
- `main.py` — Entry point for Step 1 + Step 2 (gesture detection + Kafka producer)
- `gesture_detector.py` — Camera workers, MediaPipe classifier, overlay UI
- `kafka_producer.py` — Kafka producer wrapper (retries, serializers, logging)
- `kafka_config.py` — Broker address, topics, producer tuning
- `priority_processor.py` — Local priority queue and console alerting
- `spark_consumer.py` — Step 3: PySpark Structured Streaming consumer (Kafka → HDFS/MongoDB)
- `spark_priority.py` — Batch-level processing logic used by Spark
- `mongo_writer.py` — Helper for writing documents to MongoDB
- `docker-compose.yml` — Bring up Kafka, Zookeeper, HDFS, MongoDB (used in demo)
- `requirements.txt` — Python packages required

## Gestures & Priority
The system recognizes four states:
- `high_sign`  → Danger: HIGH (priority 3) — ILY / Thai love (index + pinky up, thumb horizontal)
- `medium_sign`→ Danger: MEDIUM (priority 2) — three fingers (index + middle + ring) pointing down (M)
- `low_sign`   → Danger: LOW (priority 1) — left hand only: index up + thumb horizontal (L)
- `unknown`    → Danger: UNKNOWN (priority 0)

Confirmation: a gesture must be held for 3 seconds to be confirmed, then a 3-second cooldown prevents immediate re-triggering. These values are configurable in `gesture_detector.py`.

## Quickstart (development)
1. From repository root:
   ```
   cd step3_spark_consumer
   pip install -r requirements.txt
   ```
2. Start required services (Kafka, HDFS, MongoDB):
   ```
   docker-compose up -d
   ```
3. Run the camera + producer:
   ```
   python main.py
   ```
   - Show your hand to a camera window, hold the gesture for ~3s to confirm.
4. Run the Spark consumer in another terminal:
   ```
   python spark_consumer.py
   ```
   - Spark will read the Kafka topic `cctv-hand-events`, process batches, write raw JSON to HDFS, and write documents to MongoDB.

## Configuration
- Kafka: edit `kafka_config.py` (KAFKA_BROKER, topics)
- Spark: edit constants in `spark_consumer.py`:
  - `KAFKA_BROKER`, `KAFKA_TOPIC`, `HDFS_OUTPUT_PATH`, `TRIGGER_INTERVAL`, `CHECKPOINT_DIR`
- Gesture timings: `CONFIRM_SECONDS` and `COOLDOWN_SECONDS` in `gesture_detector.py`
- If running on Windows, `submit.bat` contains convenience steps; set `PYSPARK_PYTHON` to your Python executable if needed.

## Development notes & troubleshooting
- If the producer can't connect to Kafka, ensure Docker services are up: `docker-compose ps`
- If Spark HDFS write fails, confirm Hadoop/HDFS in docker-compose is running and accessible at the configured `HDFS_OUTPUT_PATH`
- For MediaPipe/OpenCV camera errors: check device indexes and whether another app is using the webcam. Use `where python` (Windows) to confirm Python path and set `PYSPARK_PYTHON` before running Spark on Windows.
- Logs printed to console are intended to help debugging (producer logs, Spark INFO/WARN).

## Tests & utilities
- `test_connections.py` includes helper checks for external services and can be used to validate connectivity.


## Contact
For questions or contributions, open an issue or contact the repository owner.
