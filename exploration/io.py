"""Shared streaming readers and atomic output helpers."""
import gzip
import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

MISSING = object()


@dataclass
class ReadStats:
    lines: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    blank_lines: int = 0

    def to_dict(self):
        return asdict(self)


def get_field(record, path, default=MISSING):
    value = record
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def iter_events(path, *, stats=None, on_invalid="raise", max_line_bytes=8 * 1024 * 1024):
    """Yield JSON objects; malformed input is counted or raises with line context.

    An overlong line always raises to keep parsing bounded. Gzip CRC/truncation
    errors propagate even in skip mode; they are file corruption, not bad JSON.
    """
    if on_invalid not in {"raise", "skip"}:
        raise ValueError("on_invalid must be raise or skip")
    path = Path(path)
    stats = stats if stats is not None else ReadStats()
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as handle:
        while line := handle.readline(max_line_bytes + 1):
            stats.lines += 1
            if len(line) > max_line_bytes:
                raise ValueError(f"{path}: line {stats.lines} exceeds the size limit")
            if not line.strip():
                stats.blank_lines += 1
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("event must be a JSON object")
            except (ValueError, UnicodeDecodeError) as exc:
                stats.invalid_records += 1
                if on_invalid == "raise":
                    raise ValueError(f"{path}: invalid JSON event at line {stats.lines}: {exc}") from exc
                continue
            stats.valid_records += 1
            yield record


def atomic_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def write_json(path, value):
    atomic_text(path, json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
