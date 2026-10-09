"""Pandas EDA for a bounded sample, with machine-readable results and PNG charts."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from config.settings import CHART_DIR, COMMON_EVENTS, IMPORTANT_FIELDS, PROCESSED_DIR, SAMPLE_PATH
from exploration.io import MISSING, ReadStats, get_field, iter_events, sha256_file, write_json


def events_to_frame(records):
    rows = []
    for record in records:
        row = {}
        for field in IMPORTANT_FIELDS:
            value = get_field(record, field)
            # Scalar columns only; keep nested payloads in the source JSON sample.
            if value is MISSING or value is None or isinstance(value, (list, dict)):
                row[field] = None if field != "payload" else value
            elif field in {"id", "actor.id", "repo.id"}:
                row[field] = str(value) if not isinstance(value, bool) else None
            else:
                row[field] = value
        rows.append(row)
    frame = pd.DataFrame(rows, columns=IMPORTANT_FIELDS)
    for field in IMPORTANT_FIELDS:
        if field not in {"public", "payload"}:
            frame[field] = frame[field].astype("string").str.strip().replace("", pd.NA)
    frame["timestamp"] = pd.to_datetime(frame["created_at"], errors="coerce", utc=True, format="ISO8601")
    return frame


def ranking(frame, id_field, name_field):
    valid = frame[frame[id_field].notna()]
    counts = valid.groupby(id_field, dropna=True).size().rename("events")
    names = valid.groupby(id_field)[name_field].last().rename("name")
    table = counts.to_frame().join(names).reset_index().rename(columns={id_field: "id"})
    table["name"] = table["name"].fillna(table["id"])
    return table.sort_values(["events", "id"], ascending=[False, True]).reset_index(drop=True)


def analyze_records(records):
    frame = events_to_frame(records)
    counts = frame["type"].fillna("<missing>").value_counts().sort_index()
    repositories = ranking(frame, "repo.id", "repo.name")
    accounts = ranking(frame[frame["public"].eq(True)], "actor.id", "actor.login")
    ids = frame["id"].dropna()
    duplicate_ids = ids[ids.duplicated(keep=False)].value_counts().sort_index()
    missing = []
    for field in IMPORTANT_FIELDS:
        absent = sum(get_field(record, field) is MISSING for record in records)
        nulls = sum(get_field(record, field) is None for record in records)
        blanks = sum(isinstance(get_field(record, field), str) and not get_field(record, field).strip()
                     for record in records)
        missing.append({"field": field, "absent": absent, "null": nulls, "blank": blanks,
                        "missing_total": absent + nulls + blanks,
                        "missing_percent": (absent + nulls + blanks) / len(records) * 100 if records else 0.0})
    quality = pd.DataFrame(missing)
    timestamps = frame["timestamp"].dropna()
    minute = timestamps.dt.floor("min").value_counts().sort_index().rename_axis("utc_minute").rename("events")
    if len(minute):
        # Fill internal gaps only; do not invent coverage outside the observed interval.
        minute = minute.reindex(pd.date_range(minute.index.min(), minute.index.max(), freq="min"), fill_value=0)
        minute.index.name = "utc_minute"
    hourly = timestamps.dt.floor("h").value_counts().sort_index().rename_axis("utc_hour_start").rename("events")
    hour_distribution = timestamps.dt.hour.value_counts().sort_index().rename_axis("utc_hour").rename("events")
    summary = {
        "records_analyzed": len(frame), "unique_repositories_by_id": int(frame["repo.id"].nunique()),
        "unique_accounts_by_id": int(frame["actor.id"].nunique()),
        "unique_public_accounts_by_id": int(frame.loc[frame["public"].eq(True), "actor.id"].nunique()),
        "event_type_counts": {str(key): int(value) for key, value in counts.items()},
        "common_event_counts": {kind: int(counts.get(kind, 0)) for kind in COMMON_EVENTS},
        "duplicate_event_ids": int(len(duplicate_ids)),
        "duplicate_excess_records": int(ids.duplicated().sum()),
        "records_with_duplicated_ids": int(ids.duplicated(keep=False).sum()),
        "invalid_or_missing_timestamps": int(frame["timestamp"].isna().sum()),
        "timestamp_min_utc": timestamps.min().isoformat() if len(timestamps) else None,
        "timestamp_max_utc": timestamps.max().isoformat() if len(timestamps) else None,
        "observed_utc_hours": [int(hour) for hour in hour_distribution.index],
        "missing_fields": quality.to_dict(orient="records"),
        "ranking_definition": "Raw event counts, without deduplication; grouped by stable IDs. Public account ranking includes bots and only public=true records.",
    }
    tables = {"event_types": counts.rename_axis("event_type").rename("events").reset_index(),
              "repositories": repositories, "public_accounts": accounts, "missing_values": quality,
              "events_per_minute": minute.reset_index(), "events_per_hour": hourly.reset_index(),
              "utc_hour_distribution": hour_distribution.reset_index(),
              "duplicate_ids": duplicate_ids.rename_axis("event_id").rename("occurrences").reset_index()}
    return frame, summary, tables


def create_charts(tables, summary, output_dir=CHART_DIR):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "figure.facecolor": "#fafbfe"})
    note = f"{summary.get('scope', 'sample')} • {summary['records_analyzed']:,} records • UTC"
    saved = []

    def finish(fig, ax, title, xlabel, ylabel, filename):
        ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
        ax.grid(axis="x" if filename == "top_repositories.png" else "y", alpha=.2)
        fig.text(.02, .015, note, fontsize=9, color="#546279")
        fig.tight_layout(rect=(0, .045, 1, 1))
        fig.savefig(output_dir / filename, dpi=160)
        plt.close(fig)
        saved.append(filename)

    fig, ax = plt.subplots(figsize=(11, 6))
    events = tables["event_types"].sort_values("events", ascending=False)
    ax.bar(events["event_type"], events["events"], color="#446ce3")
    ax.tick_params(axis="x", labelrotation=55)
    for tick in ax.get_xticklabels():
        tick.set_horizontalalignment("right")
    finish(fig, ax, "GitHub Event Type Distribution", "Event type", "Observed events", "event_type_distribution.png")

    fig, ax = plt.subplots(figsize=(11, 6))
    repos = tables["repositories"].head(10).iloc[::-1]
    ax.barh(repos["name"], repos["events"], color="#20a39b")
    finish(fig, ax, "Top 10 Most Active Repositories", "Observed events", "Repository", "top_repositories.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    bins = pd.cut(tables["public_accounts"]["events"], [0, 1, 5, 10, 50, float("inf")],
                  labels=["1", "2–5", "6–10", "11–50", "51+"])
    activity = bins.value_counts(sort=False)
    ax.bar([str(value) for value in activity.index], activity.values, color="#446ce3")
    finish(fig, ax, "Developer Activity Distribution (Public Accounts)", "Events per account (includes bots)", "Accounts", "developer_activity_distribution.png")

    fig, ax = plt.subplots(figsize=(11, 5))
    minute = tables["events_per_minute"]
    if len(minute) >= 2:
        ax.plot(minute["utc_minute"], minute["events"], color="#20a39b", marker=".")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=__import__("datetime").timezone.utc))
    else:
        ax.text(.5, .5, "Insufficient timestamp coverage for a line chart", ha="center", transform=ax.transAxes)
    finish(fig, ax, "GitHub Events Over Time (Observed Interval)", "Time of day (UTC; date in analysis summary)", "Events per minute", "events_over_time.png")

    fig, ax = plt.subplots(figsize=(10, 5))
    quality = tables["missing_values"]
    ax.bar(quality["field"], quality["missing_percent"], color="#e89b43")
    ax.set_ylim(0, max(5, quality["missing_percent"].max() * 1.15))
    ax.tick_params(axis="x", labelrotation=40)
    finish(fig, ax, "Missing Data Analysis (Absent, Null, or Blank)", "Field", "Records missing field (%)", "missing_data.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    values = summary["common_event_counts"]
    ax.bar([kind.removesuffix("Event") for kind in values], values.values(), color="#7966cf")
    finish(fig, ax, "Event Type Comparison", "Common event type", "Observed events", "event_type_comparison.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    hours = tables["utc_hour_distribution"]
    ax.bar(hours["utc_hour"].astype(str), hours["events"], color="#446ce3")
    finish(fig, ax, "Activity by UTC Hour (Observed Hours Only)", "UTC hour of day", "Observed events", "utc_hour_distribution.png")
    return saved


def run_analysis(input_path=SAMPLE_PATH, *, output_dir=PROCESSED_DIR, chart_dir=CHART_DIR,
                 max_records=100000, scope="sample"):
    if max_records < 1:
        raise ValueError("max_records must be positive")
    records, stats = [], ReadStats()
    for record in iter_events(input_path, stats=stats, on_invalid="skip"):
        records.append(record)
        if len(records) > max_records:
            raise ValueError("Pandas input exceeds --max-records. Generate a smaller sample or explicitly raise the cap.")
    frame, summary, tables = analyze_records(records)
    metadata_path = Path(input_path).with_suffix(".metadata.json")
    metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else None
    input_hash = sha256_file(input_path)
    if metadata and metadata.get("sample_sha256") != input_hash:
        raise ValueError("sample hash differs from its metadata; regenerate the sample")
    summary.update({"scope": metadata["scope"] if metadata else scope,
                    "data_origin": metadata["data_origin"] if metadata else "user-provided input; origin unverified",
                    "input_filename": Path(input_path).name, "input_sha256": input_hash,
                    "read_stats": stats.to_dict(), "sample_metadata": metadata})
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(output_dir / f"{name}.csv", index=False)
    summary["charts"] = create_charts(tables, summary, chart_dir)
    write_json(output_dir / "analysis_summary.json", summary)
    return frame, summary, tables


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=SAMPLE_PATH)
    parser.add_argument("--output-dir", type=Path, default=PROCESSED_DIR)
    parser.add_argument("--chart-dir", type=Path, default=CHART_DIR)
    parser.add_argument("--max-records", type=int, default=100000)
    parser.add_argument("--scope", choices=["sample", "complete input file"], default="sample")
    args = parser.parse_args()
    try:
        _, summary, _ = run_analysis(args.input, output_dir=args.output_dir, chart_dir=args.chart_dir,
                                     max_records=args.max_records, scope=args.scope)
    except (ValueError, OSError, EOFError) as exc:
        parser.exit(1, f"Analysis failed: {exc}\n")
    print(json.dumps({key: value for key, value in summary.items() if key not in {"sample_metadata", "missing_fields"}}, indent=2))


if __name__ == "__main__":
    main()
