"""Streaming full-file quality validation for raw GH Archive inputs."""
import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config.settings import IMPORTANT_FIELDS
from exploration.io import MISSING, ReadStats, get_field, iter_events, sha256_file, write_json

VALIDATOR_VERSION = 1


def validate_archive(path):
    path = Path(path)
    before = path.stat()
    stats, types, missing = ReadStats(), Counter(), Counter()
    invalid_timestamps, first, last = 0, None, None
    for event in iter_events(path, stats=stats, on_invalid="skip"):
        kind = event.get("type")
        types[kind if isinstance(kind, str) else "<missing or invalid>"] += 1
        for field in IMPORTANT_FIELDS:
            value = get_field(event, field)
            if value is MISSING or value is None or (isinstance(value, str) and not value.strip()):
                missing[field] += 1
        try:
            raw_time = event.get("created_at")
            parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone missing")
            parsed = parsed.astimezone(timezone.utc)
            first = min(first, parsed) if first else parsed
            last = max(last, parsed) if last else parsed
        except (TypeError, ValueError, AttributeError):
            invalid_timestamps += 1
    checksum = sha256_file(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise ValueError("archive changed during validation")
    if not stats.valid_records:
        raise ValueError("archive contains no JSON event objects")
    if stats.invalid_records or stats.blank_lines:
        raise ValueError(f"archive has {stats.invalid_records} invalid JSON records and {stats.blank_lines} blank lines")
    return {"validator_version": VALIDATOR_VERSION, "filename": path.name,
            "sha256": checksum, "compressed_bytes": after.st_size, "record_count": stats.valid_records,
            "read_stats": stats.to_dict(), "event_type_counts": dict(sorted(types.items())),
            "missing_fields": {field: missing[field] for field in IMPORTANT_FIELDS},
            "invalid_timestamps": invalid_timestamps,
            "timestamp_min_utc": first.isoformat() if first else None,
            "timestamp_max_utc": last.isoformat() if last else None,
            "validated_at_utc": datetime.now(timezone.utc).isoformat(), "status": "validated",
            "policy": "Gzip and JSON-object integrity required; missing fields/timestamps recorded, not fabricated. No deduplication or cleaning."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_archive(args.path)
    if args.output:
        write_json(args.output, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
