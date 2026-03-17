"""
priority_processor.py  –  Step 1 Priority Queue + Alert Processor
(updated gesture names for v3)
"""

import heapq
from datetime import datetime
from collections import defaultdict

ALERT_THRESHOLD = 2   # priority >= 2 triggers a visible alert

LEVEL_ICONS = {
    "UNKNOWN": "⚪",
    "LOW":     "🟢",
    "MEDIUM":  "🟡",
    "HIGH":    "🔴",
}

GESTURE_TO_LEVEL = {
    "low_sign":    "LOW",
    "medium_sign": "MEDIUM",
    "high_sign":   "HIGH",
    "unknown":     "UNKNOWN",
}


class EventPriorityQueue:
    """
    Max-priority queue backed by heapq.
    Stores (-priority, seq, event) so highest priority floats to top.
    """

    def __init__(self, max_size: int = 100):
        self._heap    = []
        self._counter = 0
        self.max_size = max_size
        self._stats   = defaultdict(int)

    def push(self, event: dict):
        if len(self._heap) >= self.max_size:
            self._heap.sort()
            self._heap.pop()
            heapq.heapify(self._heap)
        heapq.heappush(self._heap, (-event.get("priority", 0),
                                     self._counter, event))
        self._counter += 1
        self._stats[event.get("gesture", "unknown")] += 1

    def pop(self) -> dict | None:
        if self._heap:
            return heapq.heappop(self._heap)[2]
        return None

    def peek(self) -> dict | None:
        return self._heap[0][2] if self._heap else None

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def size(self) -> int:
        return len(self._heap)

    def get_stats(self) -> dict:
        return dict(self._stats)


class AlertProcessor:
    def __init__(self, dedup_seconds: int = 2):
        self.queue          = EventPriorityQueue()
        self.dedup_seconds  = dedup_seconds
        self._last_alert    = {}
        self.total_received = 0
        self.total_alerted  = 0

    def process(self, event: dict):
        self.total_received += 1
        self.queue.push(event)
        if event["priority"] >= ALERT_THRESHOLD:
            self._maybe_alert(event)

    def _maybe_alert(self, event: dict):
        gesture = event["gesture"]
        now     = datetime.now()
        last    = self._last_alert.get(gesture)
        if last is None or (now - last).total_seconds() >= self.dedup_seconds:
            self._last_alert[gesture] = now
            self.total_alerted += 1
            self._print_alert(event)

    @staticmethod
    def _print_alert(event: dict):
        icon  = LEVEL_ICONS.get(event["danger_level"], "⚪")
        sep   = "=" * 58
        print(sep)
        print(f"  {icon}  ALERT TRIGGERED")
        print(f"  Camera   : {event['camera_id']}")
        print(f"  Gesture  : {event['gesture'].upper()}")
        print(f"  Level    : {event['danger_level']}")
        print(f"  Priority : {event['priority']}")
        print(f"  Time     : {event['timestamp']}")
        print(sep)

    def print_stats(self):
        print("\n" + "─" * 48)
        print("  📊  SESSION STATISTICS")
        print("─" * 48)
        print(f"  Total events received : {self.total_received}")
        print(f"  Alerts triggered      : {self.total_alerted}")
        print(f"  Queue remaining       : {self.queue.size()}")
        print("\n  Gesture breakdown:")
        for gesture, count in sorted(self.queue.get_stats().items(),
                                     key=lambda x: -x[1]):
            level = GESTURE_TO_LEVEL.get(gesture, "UNKNOWN")
            icon  = LEVEL_ICONS.get(level, "⚪")
            print(f"    {icon}  {gesture:<16}  {count:>4} events")
        print("─" * 48 + "\n")
