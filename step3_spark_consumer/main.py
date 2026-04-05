"""
main.py
--------
Step 1 + Step 2 Combined Entry Point

Data flow:
  Webcam
    ↓  (MediaPipe gesture detection)
  GestureDetector  [Step 1]
    ↓  (confirmed event dict)
  GestureEventProducer  [Step 2]
    ↓  (JSON → Kafka)
  Topic: cctv-hand-events
    ↓  (Step 3 will consume from here)
  PySpark Structured Streaming

Run order:
  1. Start Kafka:        docker-compose up -d
  2. Run this file:      python main.py
  3. (optional) Watch:   python kafka_consumer_test.py
"""

import sys
from gesture_detector  import MultiCameraSystem
from priority_processor import AlertProcessor
from kafka_producer    import GestureEventProducer

# ── Camera setup ─────────────────────────────────────────────
CAMERA_SETUP = [
    (1, 0),    # (camera_id, device_index)
    (2, 1),    # remove this line if you only have one webcam
]


def main():
    print("=" * 65)
    print("  CCTV GESTURE ALERT SYSTEM  –  Step 1 + 2")
    print("  Gesture Detection  →  Kafka Producer")
    print("=" * 65)
    print(f"\n  Cameras   : {len(CAMERA_SETUP)}")
    print(f"  Kafka     : cctv-hand-events topic")
    print(f"\n  Gesture → Level:")
    print("    LOW  (1) : L-hand – index up + thumb horizontal")
    print("    MED  (2) : Either – 3 fingers pointing down (M)")
    print("    HIGH (3) : Either – Thai love / ILY sign")
    print("\n  Hold 3s to confirm.  3s cooldown after each event.")
    print("  [Q] in any camera window to quit.\n")
    print("=" * 65 + "\n")

    # ── Initialise Kafka producer ─────────────────────────────
    try:
        producer = GestureEventProducer()
    except RuntimeError as e:
        print(f"\n[ERROR] {e}")
        print("\nPlease start Kafka first:")
        print("  docker-compose up -d\n")
        sys.exit(1)

    # ── Initialise alert processor (local priority queue) ─────
    processor = AlertProcessor(dedup_seconds=0)

    # ── Event callback: runs for every confirmed gesture ─────
    def handle_event(event: dict):
        # 1. Send to Kafka
        producer.send(event)

        # 2. Local alert processing (priority queue + console alert)
        processor.process(event)

    # ── Start cameras ─────────────────────────────────────────
    system = MultiCameraSystem(cameras=CAMERA_SETUP, on_event=handle_event)

    try:
        system.start()
        system.wait()
    except KeyboardInterrupt:
        print("\n[main] Interrupted.")
        system.stop_all()
    finally:
        # Flush any buffered Kafka messages before exit
        producer.close()
        processor.print_stats()

        stats = producer.stats()
        print(f"\n  Kafka messages sent : {stats['sent']}")
        print("[main] Done.\n")


if __name__ == "__main__":
    main()
