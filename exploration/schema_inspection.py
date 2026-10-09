"""Streaming scan and seeded reservoir sampling, preserving nested event JSON."""
import argparse
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config.settings import IMPORTANT_FIELDS, PROCESSED_DIR, ROOT, SAMPLE_PATH, SAMPLE_SIZE
from exploration.io import MISSING, ReadStats, atomic_text, get_field, iter_events, sha256_file, write_json


def json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str):
        return "string"
    return "integer" if isinstance(value, int) else "number"


def field_types(record):
    """Record-wise sets prevent repeated array members inflating field counts."""
    result = defaultdict(set)

    def visit(value, path):
        if path:
            result[path].add(json_type(value))
        if isinstance(value, dict):
            for key, child in value.items():
                visit(child, f"{path}.{key}" if path else key)
        elif isinstance(value, list):
            for child in value:
                visit(child, path + "[]")

    visit(record, "")
    return result


def extract_schema(records):
    presence, types = Counter(), defaultdict(Counter)
    important = {key: {"missing": 0, "null": 0} for key in IMPORTANT_FIELDS}
    event_types = Counter()
    payload_fields = defaultdict(Counter)
    for record in records:
        kind = record.get("type")
        label = kind if isinstance(kind, str) else "<missing or invalid type>"
        event_types[label] += 1
        fields = field_types(record)
        for path, names in fields.items():
            presence[path] += 1
            types[path].update(names)
            if path.startswith("payload."):
                payload_fields[label][path] += 1
        for key in IMPORTANT_FIELDS:
            value = get_field(record, key)
            important[key]["missing"] += int(value is MISSING)
            important[key]["null"] += int(value is None)
    total = len(records)
    return {
        "records": total, "event_types": dict(sorted(event_types.items())),
        "important_fields": important,
        "fields": {path: {"present_records": count, "absent_records": total - count,
                          "types_per_record": dict(sorted(types[path].items()))}
                   for path, count in sorted(presence.items())},
        "payload_fields_by_event_type": {kind: dict(sorted(paths.items()))
                                         for kind, paths in sorted(payload_fields.items())},
        "counting_note": "Array member paths use []; presence and types count records, not elements. Payload absence across different event types is usually expected.",
    }


def inspect_and_sample(paths, *, sample_size=SAMPLE_SIZE, seed=42, sample_path=SAMPLE_PATH,
                       summary_path=PROCESSED_DIR / "schema_summary.json", on_invalid="skip"):
    if sample_size < 1:
        raise ValueError("sample_size must be at least 1")
    paths = [Path(path) for path in paths]
    if not paths:
        raise ValueError("provide at least one input file")
    if Path(sample_path).resolve() in [p.resolve() for p in paths]:
        raise ValueError("sample output must differ from inputs")
    rng = random.Random(seed)
    reservoir, sources = [], []
    seen = 0
    for path in paths:
        stats = ReadStats()
        source_hash = sha256_file(path)
        for record in iter_events(path, stats=stats, on_invalid=on_invalid):
            seen += 1
            entry = (seen, record)
            if len(reservoir) < sample_size:
                reservoir.append(entry)
            else:
                index = rng.randrange(seen)
                if index < sample_size:
                    reservoir[index] = entry
        manifest_path = path.parent / "download_manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
        download = manifest.get("files", {}).get(path.name, {})
        verified = download.get("sha256") == source_hash and download.get("provenance") == "HTTPS download from GH Archive"
        try:
            stored_path = str(path.resolve().relative_to(ROOT))
        except ValueError:
            stored_path = str(path.resolve())
        sources.append({"path": stored_path, "bytes": path.stat().st_size, "sha256": source_hash,
                        "read_stats": stats.to_dict(), "verified_gharchive_download": verified,
                        "url": download.get("url") if verified else None})
    records = [record for _, record in sorted(reservoir, key=lambda pair: pair[0])]
    atomic_text(sample_path, "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records))
    metadata = {
        "scope": "sample" if seen > len(records) else "all valid input records",
        "data_origin": "real GH Archive" if all(s["verified_gharchive_download"] for s in sources) else "user-provided local input; origin not independently verified",
        "sampling_method": "uniform reservoir without replacement, retained in source order",
        "seed": seed, "requested_sample_size": sample_size, "sample_records": len(records),
        "valid_source_records": seen, "sources": sources,
        "sample_sha256": sha256_file(sample_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_json(Path(sample_path).with_suffix(".metadata.json"), metadata)
    schema = extract_schema(records)
    schema["provenance"] = metadata
    schema["examples"] = records[:2]
    write_json(summary_path, schema)
    return schema


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sample-path", type=Path, default=SAMPLE_PATH)
    parser.add_argument("--summary-path", type=Path, default=PROCESSED_DIR / "schema_summary.json")
    parser.add_argument("--strict", action="store_true", help="fail on invalid JSON instead of counting/skipping")
    args = parser.parse_args()
    try:
        result = inspect_and_sample(args.paths, sample_size=args.sample_size, seed=args.seed,
                                    sample_path=args.sample_path, summary_path=args.summary_path,
                                    on_invalid="raise" if args.strict else "skip")
    except (OSError, EOFError, ValueError) as exc:
        parser.exit(1, f"Inspection failed: {exc}\n")
    print(json.dumps({"records": result["records"], "event_types": result["event_types"],
                      "important_fields": result["important_fields"], "example": result["examples"][:1]}, indent=2))


if __name__ == "__main__":
    main()
