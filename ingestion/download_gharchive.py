"""Stream hourly archives with retries, integrity checks, and a raw-data quota."""
import argparse
import gzip
import json
import logging
import math
import time
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from config.settings import BASE_URL, DEFAULT_DATE, DEFAULT_HOUR, MAX_DISK_MB, RAW_DIR, TIMEOUT
from exploration.io import sha256_file, write_json
from ingestion.locking import directory_lock

LOG = logging.getLogger(__name__)


def validate_hour(day, hour):
    day = date.fromisoformat(day) if isinstance(day, str) else day
    if not isinstance(day, date) or isinstance(day, datetime):
        raise ValueError("date must be YYYY-MM-DD")
    if isinstance(hour, bool) or not isinstance(hour, int) or not 0 <= hour <= 23:
        raise ValueError("hour must be an integer from 0 to 23 (UTC)")
    if day < date(2011, 2, 12):
        raise ValueError("GH Archive starts on 2011-02-12")
    start = datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc)
    if start + timedelta(hours=1) > datetime.now(timezone.utc):
        raise ValueError("choose a completed historical UTC hour")
    return day


def archive_filename(day, hour):
    return f"{validate_hour(day, hour).isoformat()}-{hour}.json.gz"


def validate_gzip(path):
    """Read through EOF so gzip CRC and trailer checks really execute."""
    total = 0
    with gzip.open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            total += len(chunk)
    if total == 0:
        raise ValueError("archive decompresses to zero bytes")
    return total


def _download_archive_unlocked(day, hour, *, output_dir=RAW_DIR, max_disk_mb=MAX_DISK_MB,
                               timeout=TIMEOUT, retries=3, session=None, backoff=1.0,
                               force_download=False):
    """Download at most one file. Safe for sequential invocations; no parallel writers.

    The quota includes existing files in output_dir and temporary bytes. Existing
    targets remain intact until a replacement has passed validation.
    """
    filename = archive_filename(day, hour)
    if not math.isfinite(max_disk_mb) or max_disk_mb <= 0:
        raise ValueError("max_disk_mb must be positive and finite")
    if (not math.isfinite(timeout) or timeout <= 0 or isinstance(retries, bool)
            or not isinstance(retries, int) or retries < 0 or not math.isfinite(backoff) or backoff < 0):
        raise ValueError("timeout must be positive and retries nonnegative")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / filename
    partial = target.with_suffix(target.suffix + ".part")
    url = f"{BASE_URL}/{filename}"
    manifest_path = output_dir / "download_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
        raise ValueError("download manifest must contain a files object")
    old = manifest["files"].get(filename, {})
    reused = False
    uncompressed = None
    if target.exists() and not force_download:
        try:
            uncompressed = validate_gzip(target)
            current_hash = sha256_file(target)
            reused = not old.get("sha256") or old["sha256"] == current_hash
            if reused:
                LOG.info("Reusing validated %s", filename)
            else:
                LOG.warning("Checksum differs from recorded download; replacing %s", filename)
        except (OSError, EOFError, ValueError, zlib.error) as exc:
            LOG.warning("Existing file failed validation: %s", exc)
    owned_session = session is None
    session = session or requests.Session()
    try:
        if not reused:
            for attempt in range(retries + 1):
                try:
                    partial.unlink(missing_ok=True)
                    used = sum(p.stat().st_size for p in output_dir.iterdir() if p.is_file())
                    remaining = int(max_disk_mb * 1024 * 1024) - used
                    if remaining <= 0:
                        raise ValueError("raw-data disk limit reached; increase --max-disk-mb")
                    with session.get(url, stream=True, timeout=(min(timeout, 15), timeout),
                                     headers={"User-Agent": "DevPulse/2.0", "Accept-Encoding": "identity"}) as response:
                        response.raise_for_status()
                        length = int(response.headers.get("Content-Length", "0"))
                        if length > remaining:
                            raise ValueError(f"download of {length} bytes exceeds remaining quota {remaining}")
                        downloaded = 0
                        last_log = 0
                        with partial.open("wb") as handle:
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                if not chunk:
                                    continue
                                downloaded += len(chunk)
                                if downloaded > remaining:
                                    raise ValueError("stream exceeded the raw-data disk limit")
                                handle.write(chunk)
                                if downloaded - last_log >= 8 * 1024 * 1024:
                                    LOG.info("%s: %.1f MiB received", filename, downloaded / 1024**2)
                                    last_log = downloaded
                        if length and downloaded != length:
                            raise EOFError(f"incomplete download: expected {length}, received {downloaded}")
                    uncompressed = validate_gzip(partial)
                    partial.replace(target)
                    break
                except (requests.RequestException, OSError, EOFError, zlib.error) as exc:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    permanent = status is not None and status < 500 and status not in {408, 429}
                    if attempt == retries or permanent:
                        raise
                    LOG.warning("Attempt %d failed (%s); retrying", attempt + 1, exc)
                    time.sleep(min(backoff * 2**attempt, 60))
                finally:
                    partial.unlink(missing_ok=True)
        entry = {
            "filename": filename, "url": url, "compressed_bytes": target.stat().st_size,
            "uncompressed_bytes": uncompressed, "sha256": sha256_file(target),
            "gzip_validated": True, "reused": reused,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        entry["downloaded_at_utc"] = old.get("downloaded_at_utc") if reused else entry["checked_at_utc"]
        entry["provenance"] = (
            old.get("provenance", "existing local file; original download not verified")
            if reused and old.get("sha256") == entry["sha256"]
            else "existing local file; original download not verified" if reused
            else "HTTPS download from GH Archive")
        manifest["files"][filename] = entry
        write_json(manifest_path, manifest)
        LOG.info("Validated %s (%d bytes)", filename, entry["compressed_bytes"])
        return entry
    finally:
        if owned_session:
            session.close()


def download_archive(day, hour, *, output_dir=RAW_DIR, max_disk_mb=MAX_DISK_MB,
                     timeout=TIMEOUT, retries=3, session=None, backoff=1.0, force_download=False):
    """Serialize all writers sharing a raw directory, including Week 1 commands."""
    with directory_lock(Path(output_dir) / ".download.lock"):
        return _download_archive_unlocked(day, hour, output_dir=output_dir, max_disk_mb=max_disk_mb,
                                          timeout=timeout, retries=retries, session=session,
                                          backoff=backoff, force_download=force_download)


def build_hours(day, hour=DEFAULT_HOUR, hours=1, all_day=False, end_date=None):
    day = date.fromisoformat(day)
    if all_day or end_date:
        end = date.fromisoformat(end_date) if end_date else day
        if end < day:
            raise ValueError("end date must not precede start date")
        hours = ((end - day).days + 1) * 24
        hour = 0
    if not 1 <= hours <= 168:
        raise ValueError("Week 1 collection is bounded to 1–168 hours per command")
    start = datetime(day.year, day.month, day.day) + timedelta(hours=hour)
    validate_hour(day, hour)
    selections = [(start + timedelta(hours=i)) for i in range(hours)]
    for item in selections:
        validate_hour(item.date(), item.hour)
    return [(item.date(), item.hour) for item in selections]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--hour", type=int, default=DEFAULT_HOUR, help="UTC, 0–23")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--hours", type=int, default=1, help="consecutive hours; crosses dates")
    group.add_argument("--all-day", action="store_true", help="24 archives for --date")
    group.add_argument("--end-date", help="inclusive range of complete UTC days")
    parser.add_argument("--output-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--max-disk-mb", type=float, default=MAX_DISK_MB)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        selections = build_hours(args.date, args.hour, args.hours, args.all_day, args.end_date)
        for day, hour in selections:
            download_archive(day, hour, output_dir=args.output_dir, max_disk_mb=args.max_disk_mb,
                             timeout=args.timeout, retries=args.retries)
    except (ValueError, OSError, EOFError, zlib.error, requests.RequestException) as exc:
        parser.exit(1, f"Download failed: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
