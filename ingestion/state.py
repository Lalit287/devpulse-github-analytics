"""Durable per-hour and per-destination state using Python's standard SQLite."""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS archives (
                    filename TEXT PRIMARY KEY, status TEXT NOT NULL,
                    details TEXT NOT NULL, updated_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS publications (
                    filename TEXT NOT NULL, backend TEXT NOT NULL, destination TEXT NOT NULL,
                    details TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY(filename, backend, destination)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, status TEXT NOT NULL, details TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def archive(self, filename):
        with self.connect() as db:
            row = db.execute("SELECT status, details FROM archives WHERE filename=?", (filename,)).fetchone()
        return {"status": row["status"], **json.loads(row["details"])} if row else None

    def save_archive(self, filename, status, details):
        if status not in {"downloading", "validated", "failed"}:
            raise ValueError("invalid archive status")
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO archives VALUES (?, ?, ?, ?)",
                       (filename, status, json.dumps(details, allow_nan=False), utc_now()))

    def save_publication(self, filename, backend, destination, details):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO publications VALUES (?, ?, ?, ?, ?)",
                       (filename, backend, destination, json.dumps(details, allow_nan=False), utc_now()))

    def save_run(self, run_id, status, details):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?)",
                       (run_id, status, json.dumps(details, allow_nan=False), utc_now()))
