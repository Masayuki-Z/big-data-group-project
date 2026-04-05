# -*- coding: utf-8 -*-
"""
spark_priority.py  -  Step 3  (Windows cp1252 safe)
All Unicode chars replaced with plain ASCII.
"""

import sys
import heapq
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ALERT_PRIORITY_THRESHOLD = 2

LEVEL_LABELS = {
    "UNKNOWN": "[?]",
    "LOW":     "[L]",
    "MEDIUM":  "[M]",
    "HIGH":    "[H]",
}


class BatchPriorityQueue:
    """
    Max-heap for one Spark micro-batch.
    Stores (-priority, sequence, event) so heapq gives highest first.
    """

    def __init__(self):
        self._heap    = []
        self._counter = 0

    def push(self, event: dict):
        priority = event.get("priority", 0)
        heapq.heappush(self._heap, (-priority, self._counter, event))
        self._counter += 1

    def pop(self) -> dict:
        _, _, event = heapq.heappop(self._heap)
        return event

    def drain(self) -> list:
        result = []
        while self._heap:
            result.append(self.pop())
        return result

    def __len__(self):
        return len(self._heap)


def process_batch(spark_df, batch_id: int):
    """
    Push all rows into the priority heap, drain in priority order,
    then split into (all_events, alerts).

    Returns
    -------
    all_sorted : list[dict]  - every event, highest priority first -> HDFS
    alerts     : list[dict]  - only MEDIUM/HIGH events             -> MongoDB
    """
    if spark_df.rdd.isEmpty():
        return [], []

    rows = [row.asDict() for row in spark_df.collect()]

    pq = BatchPriorityQueue()
    for row in rows:
        row["processed_at"] = datetime.now().isoformat()
        pq.push(row)

    all_sorted = pq.drain()
    alerts     = [e for e in all_sorted
                  if e.get("priority", 0) >= ALERT_PRIORITY_THRESHOLD]

    _print_batch_summary(batch_id, all_sorted, alerts)
    return all_sorted, alerts


def _print_batch_summary(batch_id: int, all_events: list, alerts: list):
    if not all_events:
        return

    sep = "-" * 60
    print(f"\n{sep}")
    print(f"  BATCH #{batch_id}   total={len(all_events)}   alerts={len(alerts)}")
    print(sep)

    for event in all_events:
        label = LEVEL_LABELS.get(event.get("danger_level", ""), "[?]")
        flag  = " <-- ALERT" if event.get("priority", 0) >= ALERT_PRIORITY_THRESHOLD else ""
        print(
            f"  {label}  cam={event.get('camera_id')}  "
            f"gesture={event.get('gesture', '?'):<14}  "
            f"level={event.get('danger_level', '?'):<8}  "
            f"p={event.get('priority')}{flag}"
        )
    print(sep)
