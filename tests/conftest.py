"""Synthetic, tiny, local-only test fixtures; never used for the real analysis."""
import gzip
import json

import pytest


@pytest.fixture
def events():
    return [
        {"id": "100", "type": "PushEvent", "actor": {"id": 1, "login": "fixture-alice"},
         "repo": {"id": 10, "name": "fixture/repo-a"}, "created_at": "2025-01-01T12:00:00Z",
         "public": True, "payload": {"commits": [{"sha": "a"}, {"sha": "b"}], "size": 2}},
        {"id": "101", "type": "WatchEvent", "actor": {"id": 2, "login": "fixture-bob"},
         "repo": {"id": 10, "name": "fixture/repo-a"}, "created_at": "2025-01-01T12:01:00Z",
         "public": True, "payload": {"action": "started"}},
        {"id": "101", "type": "WatchEvent", "actor": {"id": 2, "login": "fixture-bob"},
         "repo": {"id": 20, "name": "fixture/repo-b"}, "created_at": "bad-timestamp",
         "public": True, "payload": {"action": "started"}},
        {"id": None, "type": "ForkEvent", "actor": None, "repo": {"name": None},
         "created_at": None, "public": True, "payload": {}},
    ]


@pytest.fixture
def archive(tmp_path, events):
    path = tmp_path / "fixture.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for record in events:
            handle.write(json.dumps(record) + "\n")
    return path
