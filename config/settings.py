"""Portable defaults resolved relative to this project, not the current directory."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data/raw"
SAMPLE_PATH = ROOT / "data/samples/github_events_sample.jsonl"
PROCESSED_DIR = ROOT / "data/processed"
REPORTS_DIR = ROOT / "reports"
CHART_DIR = ROOT / "visualizations"
BASE_URL = "https://data.gharchive.org"
DEFAULT_DATE = "2025-01-01"
DEFAULT_HOUR = 12
SAMPLE_SIZE = int(os.environ.get("DEVPULSE_SAMPLE_SIZE", "10000"))
MAX_DISK_MB = float(os.environ.get("DEVPULSE_MAX_DISK_MB", "500"))
TIMEOUT = float(os.environ.get("DEVPULSE_TIMEOUT", "60"))
IMPORTANT_FIELDS = (
    "id", "type", "actor.id", "actor.login", "repo.id", "repo.name",
    "created_at", "public", "payload",
)
COMMON_EVENTS = ("PushEvent", "WatchEvent", "ForkEvent", "PullRequestEvent", "IssuesEvent")
