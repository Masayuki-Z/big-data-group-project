# -*- coding: utf-8 -*-
"""
mongo_writer.py  -  Step 3  (Windows cp1252 safe)
All Unicode chars replaced with plain ASCII.
"""

import sys
import logging
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure

log = logging.getLogger("MongoWriter")

MONGO_URI            = "mongodb://localhost:27017"
DB_NAME              = "cctv_alerts"
COLLECTION_ALERTS    = "alerts"
COLLECTION_ALL       = "all_events"


class MongoWriter:

    def __init__(self, uri: str = MONGO_URI, db_name: str = DB_NAME):
        self._client  = None
        self._db      = None
        self._uri     = uri
        self._db_name = db_name
        self._connect()

    def _connect(self):
        try:
            self._client = MongoClient(self._uri, serverSelectionTimeoutMS=5000)
            self._client.admin.command("ping")
            self._db = self._client[self._db_name]
            self._db[COLLECTION_ALERTS].create_index(
                [("priority", DESCENDING), ("timestamp", DESCENDING)]
            )
            self._db[COLLECTION_ALL].create_index(
                [("timestamp", DESCENDING)]
            )
            log.info("MongoDB connected  (db: %s)", self._db_name)
        except ConnectionFailure as e:
            raise RuntimeError(
                f"Cannot connect to MongoDB at {self._uri}.\n"
                f"  -> Is MongoDB running?  docker-compose ps\n"
                f"  Error: {e}"
            )

    def write_alerts(self, events: list):
        if not events:
            return
        docs   = [self._prepare_doc(e) for e in events]
        result = self._db[COLLECTION_ALERTS].insert_many(docs)
        log.info("MongoDB alerts: inserted %d doc(s)", len(result.inserted_ids))
        for doc in docs:
            self._print_alert(doc)

    def write_all(self, events: list):
        if not events:
            return
        docs   = [self._prepare_doc(e) for e in events]
        result = self._db[COLLECTION_ALL].insert_many(docs)
        log.info("MongoDB all_events: inserted %d doc(s)", len(result.inserted_ids))

    def get_stats(self) -> dict:
        return {
            "alerts_count":     self._db[COLLECTION_ALERTS].count_documents({}),
            "all_events_count": self._db[COLLECTION_ALL].count_documents({}),
        }

    @staticmethod
    def _prepare_doc(event: dict) -> dict:
        doc = dict(event)
        doc["mongo_written_at"] = datetime.now().isoformat()
        for k, v in doc.items():
            if hasattr(v, "isoformat"):
                doc[k] = v.isoformat()
        return doc

    @staticmethod
    def _print_alert(doc: dict):
        labels = {"LOW": "[LOW]", "MEDIUM": "[MEDIUM]", "HIGH": "[HIGH]"}
        label  = labels.get(doc.get("danger_level", ""), "[?]")
        sep    = "=" * 55
        print(sep)
        print(f"  {label} MONGODB ALERT SAVED")
        print(f"  Camera   : {doc.get('camera_id')}")
        print(f"  Gesture  : {str(doc.get('gesture', '?')).upper()}")
        print(f"  Level    : {doc.get('danger_level')}")
        print(f"  Priority : {doc.get('priority')}")
        print(f"  Time     : {doc.get('timestamp', '')}")
        print(sep)

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
