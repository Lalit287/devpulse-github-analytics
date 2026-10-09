"""Budgeted, identity-checked public metadata requests with a checksummed cache."""
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urljoin, urlsplit

import requests

from config.settings import ROOT
from exploration.io import write_json
from ingestion.locking import directory_lock
from ingestion.state import utc_now

API_VERSION = "2022-11-28"
CACHE_VERSION = 1


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def repo_path(name):
    parts = name.split("/")
    if len(parts) != 2 or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", p) or p in {".", ".."} for p in parts):
        raise ValueError("Invalid repository owner/name")
    return "/repos/" + "/".join(quote(p, safe="") for p in parts)


def allowed_api_url(url):
    parsed = urlsplit(url)
    return (parsed.scheme == "https" and parsed.hostname == "api.github.com" and parsed.port in {None, 443}
            and not parsed.username and not parsed.password and
            (parsed.path.startswith("/repos/") or parsed.path.startswith("/repositories/")))


class BudgetStopped(RuntimeError):
    pass


class GitHubClient:
    def __init__(self, session=None, *, max_requests=50, reserve=5, retries=1):
        if type(max_requests) is not int or max_requests < 0 or reserve < 0 or retries < 0:
            raise ValueError("Request budget and retry/reserve limits must be nonnegative")
        self.session = session or requests.Session()
        self.requests = 0
        self.max_requests, self.reserve, self.retries = max_requests, reserve, retries
        self.stopped_reason = None
        self.remaining = None
        self.reset_at = None
        self.session.headers.update({"Accept": "application/vnd.github+json", "User-Agent": "DevPulse-Week4",
                                     "X-GitHub-Api-Version": API_VERSION})
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def get_json(self, path, previous=None):
        url = "https://api.github.com" + path
        headers = {"If-None-Match": previous["etag"]} if previous and previous.get("etag") else {}
        redirects, transient_attempts = 0, 0
        while True:
            if self.stopped_reason or self.requests >= self.max_requests:
                self.stopped_reason = self.stopped_reason or "request_budget_exhausted"
                raise BudgetStopped(self.stopped_reason)
            if not allowed_api_url(url):
                raise ValueError("Refusing redirect outside the public GitHub API")
            self.requests += 1
            try:
                response = self.session.get(url, headers=headers, timeout=(5, 20), stream=True, allow_redirects=False)
            except requests.RequestException:
                if transient_attempts < self.retries:
                    transient_attempts += 1
                    time.sleep(0.5)
                    continue
                return {"status": "network_error", "url": url, "checked_at_utc": utc_now()}
            with response:
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining and remaining.isdigit():
                    self.remaining = int(remaining)
                self.reset_at = response.headers.get("X-RateLimit-Reset", self.reset_at)
                now = utc_now()
                if response.status_code in {301, 302, 307, 308}:
                    redirects += 1
                    if redirects > 4:
                        return {"status": "redirect_limit", "url": url, "checked_at_utc": now}
                    url = urljoin(url, response.headers.get("Location", ""))
                    if self.remaining is not None and self.remaining <= self.reserve:
                        self.stopped_reason = "rate_limit_reserve"
                    continue
                if response.status_code == 429 or (response.status_code == 403 and self.remaining == 0):
                    self.stopped_reason = "rate_limited"
                    return {"status": "rate_limited", "url": url, "checked_at_utc": now,
                            "http_status": response.status_code, "retry_after": response.headers.get("Retry-After")}
                if self.remaining is not None and self.remaining <= self.reserve:
                    self.stopped_reason = "rate_limit_reserve"
                if response.status_code == 304:
                    if not previous or previous.get("status") != "ok":
                        raise ValueError("304 response has no verified prior body")
                    return dict(previous, checked_at_utc=now, revalidated=True)
                if response.status_code >= 500 and transient_attempts < self.retries:
                    transient_attempts += 1
                    time.sleep(0.5)
                    continue
                if response.status_code != 200:
                    label = {404: "not_found", 410: "gone", 403: "forbidden"}.get(response.status_code, "http_error")
                    if response.status_code == 403 and response.headers.get("Retry-After"):
                        self.stopped_reason = "secondary_rate_limit"
                    return {"status": label, "url": url, "http_status": response.status_code, "checked_at_utc": now}
                parts, length = [], 0
                for part in response.iter_content(64 * 1024):
                    length += len(part)
                    if length > 2 * 1024**2:
                        raise ValueError("GitHub JSON response exceeds the 2 MiB bound")
                    parts.append(part)
                try:
                    body = json.loads(b"".join(parts))
                except (ValueError, UnicodeDecodeError):
                    return {"status": "invalid_json", "url": url, "checked_at_utc": now}
                return {"status": "ok", "url": url, "body": body, "body_sha256": checksum(body),
                        "etag": response.headers.get("ETag"), "fetched_at_utc": now, "checked_at_utc": now}


def load_cache(path, expected_id):
    if not path.exists():
        return None
    entry = json.loads(path.read_text())
    digest = entry.get("record_sha256")
    if (entry.get("repo_id") != expected_id or entry.get("cache_version") != CACHE_VERSION or
            checksum({k: v for k, v in entry.items() if k != "record_sha256"}) != digest):
        raise ValueError(f"Corrupted or incompatible metadata cache: {path}")
    return entry


def fresh(entry, ttl_days, failure_ttl_hours=24):
    if entry is None:
        return False
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(entry["checked_at_utc"])).total_seconds()
    if entry["status"] == "request_budget_exhausted":
        return False  # A new invocation has a new caller-imposed request budget.
    if entry["status"] in {"rate_limited", "rate_limit_reserve", "secondary_rate_limit"}:
        reset = entry.get("retry_after_epoch")
        return time.time() < reset if reset is not None else 0 <= age < 60
    if entry["status"] in {"network_error", "http_error", "invalid_json"}:
        return 0 <= age < 300
    ttl = ttl_days * 86400 if entry["status"] == "ok" else failure_ttl_hours * 3600
    return 0 <= age < ttl


def retry_hint(client):
    return {"retry_after_epoch": int(client.reset_at)} if client.reset_at and str(client.reset_at).isdigit() else {}


def fetch_repository(client, candidate, previous=None):
    identifier, name = candidate["repo_id"], candidate["repo_name"]
    entry = {"cache_version": CACHE_VERSION, "api_version": API_VERSION, "repo_id": identifier,
             "archived_repo_name": name, "checked_at_utc": utc_now(), "status": "unavailable"}
    try:
        metadata = client.get_json(repo_path(name), previous.get("metadata") if previous else None)
    except BudgetStopped as exc:
        return dict(entry, status=str(exc), **retry_hint(client))
    entry["metadata"] = metadata
    if metadata["status"] != "ok":
        return dict(entry, status=metadata["status"], **retry_hint(client))
    body = metadata["body"]
    if not isinstance(body, dict) or type(body.get("id")) is not int:
        return dict(entry, status="invalid_metadata")
    if body.get("private") is not False:
        entry.pop("metadata")  # Public archive scope does not retain private metadata.
        return dict(entry, status="not_public")
    if body["id"] != identifier:
        return dict(entry, status="identity_mismatch", observed_repo_id=body["id"])
    if not isinstance(body.get("full_name"), str) or (body.get("language") is not None and not isinstance(body["language"], str)):
        return dict(entry, status="invalid_metadata")
    try:
        path = repo_path(body["full_name"]) + "/languages"
    except ValueError:
        return dict(entry, status="invalid_metadata")
    try:
        languages = client.get_json(path, previous.get("languages") if previous else None)
    except BudgetStopped as exc:
        return dict(entry, status=str(exc), **retry_hint(client))
    entry["languages"] = languages
    if languages["status"] != "ok":
        return dict(entry, status=languages["status"], **retry_hint(client))
    values = languages["body"]
    if not isinstance(values, dict) or any(not isinstance(k, str) or not k or type(v) is not int or v < 0 for k, v in values.items()):
        return dict(entry, status="invalid_languages")
    topics = body.get("topics", [])
    if not isinstance(topics, list) or any(not isinstance(topic, str) for topic in topics):
        return dict(entry, status="invalid_metadata")
    return dict(entry, status="ok", current_full_name=body["full_name"], primary_language=body.get("language"),
                topics=topics, description=body.get("description"), repository_created_at=body.get("created_at"),
                api_stargazers_count=body.get("stargazers_count"), api_forks_count=body.get("forks_count"),
                metadata_fetched_at_utc=metadata["fetched_at_utc"], languages_fetched_at_utc=languages["fetched_at_utc"],
                language_bytes=values)


def enrich(candidates, *, cache_dir=ROOT / "data/metadata/github_repositories", client=None, ttl_days=30):
    if not 0 < ttl_days <= 365 or not 1 <= len(candidates) <= 25:
        raise ValueError("Use a positive cache TTL up to 365 days and 1–25 repositories per run")
    identifiers = [c["repo_id"] for c in candidates]
    if len(set(identifiers)) != len(identifiers) or any(type(i) is not int or i <= 0 for i in identifiers):
        raise ValueError("Selected repository IDs must be unique positive integers")
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    client = client or GitHubClient()
    entries, hits = [], 0
    with directory_lock(cache_dir / ".enrichment.lock"):
        for candidate in candidates:
            path = cache_dir / f"{candidate['repo_id']}.json"
            previous = load_cache(path, candidate["repo_id"])
            if fresh(previous, ttl_days):
                entries.append(previous)
                hits += 1
                continue
            entry = fetch_repository(client, candidate, previous)
            entry["record_sha256"] = checksum(entry)
            write_json(path, entry)
            entries.append(entry)
    statuses = {status: sum(e["status"] == status for e in entries) for status in sorted({e["status"] for e in entries})}
    return entries, {"selected_repositories": len(candidates), "cache_hits": hits, "http_requests": client.requests,
                     "request_budget": client.max_requests, "rate_limit_remaining": client.remaining,
                     "rate_limit_reset": client.reset_at, "stopped_reason": client.stopped_reason,
                     "status_counts": statuses, "completed_at_utc": utc_now(), "cache_ttl_days": ttl_days}
