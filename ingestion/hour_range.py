"""Validated UTC collection windows, independent of the Week 1 CLI cap."""
from datetime import date, datetime, timedelta, timezone

from ingestion.download_gharchive import archive_filename, validate_hour


def plan_hours(day, hour=0, hours=24, end_date=None):
    if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
        raise ValueError("date must use YYYY-MM-DD")
    parsed = date.fromisoformat(day)
    if end_date is not None:
        end = date.fromisoformat(end_date)
        if end.isoformat() != end_date or end < parsed:
            raise ValueError("end-date must be YYYY-MM-DD and not precede date")
        hour, hours = 0, ((end - parsed).days + 1) * 24
    validate_hour(parsed, hour)
    if isinstance(hours, bool) or not isinstance(hours, int) or not 1 <= hours <= 744:
        raise ValueError("hours must be an integer from 1 to 744 (31 days per run)")
    start = datetime(parsed.year, parsed.month, parsed.day, hour, tzinfo=timezone.utc)
    result = []
    for offset in range(hours):
        timestamp = start + timedelta(hours=offset)
        filename = archive_filename(timestamp.date(), timestamp.hour)
        result.append({"date": timestamp.date().isoformat(), "hour": timestamp.hour,
                       "filename": filename, "hour_start_utc": timestamp.isoformat()})
    return result


def delayed_completed_hour(now=None, lag_hours=6):
    """Choose a fully completed UTC hour with an explicit publication buffer."""
    if isinstance(lag_hours, bool) or not isinstance(lag_hours, int) or not 0 <= lag_hours <= 168:
        raise ValueError("lag-hours must be an integer from 0 to 168")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1 + lag_hours)
