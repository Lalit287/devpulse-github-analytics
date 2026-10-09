import gzip
import json

import pytest

from exploration.explore_dataset import analyze_records, run_analysis
from exploration.io import ReadStats, iter_events
from exploration.schema_inspection import extract_schema, inspect_and_sample


def test_gzip_streaming(archive, events):
    stats = ReadStats()
    assert list(iter_events(archive, stats=stats)) == events
    assert stats.valid_records == len(events)


def test_invalid_json_and_nonobjects(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text('{}\nnot-json\n[]\nnull\n\n{"id":"ok"}\n')
    with pytest.raises(ValueError, match="line 2"):
        list(iter_events(path))
    stats = ReadStats()
    assert len(list(iter_events(path, stats=stats, on_invalid="skip"))) == 2
    assert stats.invalid_records == 3
    assert stats.blank_lines == 1


def test_line_size_limit(tmp_path):
    path = tmp_path / "large.jsonl"
    path.write_text('{"payload":"' + "x" * 100 + '"}\n')
    with pytest.raises(ValueError, match="size limit"):
        list(iter_events(path, max_line_bytes=20))


def test_truncated_gzip_is_not_silently_skipped(archive):
    archive.write_bytes(archive.read_bytes()[:-8])
    with pytest.raises((EOFError, OSError)):
        list(iter_events(archive, on_invalid="skip"))


def test_schema_nested_types_and_expected_missing_fields(events):
    schema = extract_schema(events)
    assert schema["fields"]["actor.id"]["present_records"] == 3
    assert schema["important_fields"]["actor.id"]["missing"] == 1
    assert schema["fields"]["actor"]["types_per_record"] == {"object": 3, "null": 1}
    assert schema["fields"]["payload.commits[].sha"]["present_records"] == 1
    assert schema["fields"]["payload.commits[].sha"]["types_per_record"] == {"string": 1}
    assert schema["event_types"]["WatchEvent"] == 2
    assert "payload.action" in schema["payload_fields_by_event_type"]["WatchEvent"]
    assert schema["fields"]["id"]["types_per_record"] == {"string": 3, "null": 1}


def test_boolean_is_not_classified_as_integer(events):
    assert extract_schema(events)["fields"]["public"]["types_per_record"] == {"boolean": 4}


def test_duplicate_ids_missing_fields_and_aggregation(events):
    _, summary, tables = analyze_records(events)
    assert summary["records_analyzed"] == 4
    assert summary["unique_repositories_by_id"] == 2
    assert summary["unique_accounts_by_id"] == 2
    assert summary["duplicate_event_ids"] == 1
    assert summary["duplicate_excess_records"] == 1
    assert summary["records_with_duplicated_ids"] == 2
    assert summary["common_event_counts"] == {"PushEvent": 1, "WatchEvent": 2, "ForkEvent": 1,
                                                "PullRequestEvent": 0, "IssuesEvent": 0}
    assert summary["invalid_or_missing_timestamps"] == 2
    quality = tables["missing_values"].set_index("field")
    assert quality.loc["repo.id", "missing_total"] == 1
    assert quality.loc["id", "null"] == 1
    assert tables["repositories"].iloc[0]["events"] == 2


def test_empty_dataset():
    _, summary, tables = analyze_records([])
    assert summary["records_analyzed"] == 0
    assert summary["duplicate_event_ids"] == 0
    assert not summary["timestamp_min_utc"]
    assert tables["repositories"].empty
    assert extract_schema([])["records"] == 0


def test_stable_ids_across_renames_and_public_filter():
    events = [{"id": "1", "type": "PushEvent", "repo": {"id": 9, "name": "old/name"},
               "actor": {"id": 8, "login": "old-login"}, "public": True},
              {"id": "2", "type": "PushEvent", "repo": {"id": 9, "name": "new/name"},
               "actor": {"id": 8, "login": "new-login"}, "public": True},
              {"id": "3", "actor": {"id": 7, "login": "private"}, "public": False}]
    _, summary, tables = analyze_records(events)
    assert summary["unique_repositories_by_id"] == 1
    assert summary["unique_accounts_by_id"] == 2
    assert summary["unique_public_accounts_by_id"] == 1
    assert tables["repositories"].iloc[0]["name"] == "new/name"
    assert tables["public_accounts"].iloc[0]["events"] == 2


def test_big_ids_preserve_precision():
    big_id = 2**63 - 1
    frame, summary, _ = analyze_records([{"id": big_id, "actor": {"id": big_id}}, {"id": None}])
    assert frame.iloc[0]["id"] == str(big_id)
    assert summary["duplicate_event_ids"] == 0


def test_whitespace_ids_are_missing_not_duplicates():
    _, summary, tables = analyze_records([{"id": "   ", "actor": {"id": " "}}, {"id": " "}])
    assert summary["duplicate_event_ids"] == 0
    assert summary["unique_accounts_by_id"] == 0
    quality = tables["missing_values"].set_index("field")
    assert quality.loc["id", "blank"] == 2


def test_utc_time_normalization():
    _, summary, tables = analyze_records([{"created_at": "2025-01-01T13:00:00+01:00"}])
    assert summary["observed_utc_hours"] == [12]
    assert tables["utc_hour_distribution"].iloc[0]["events"] == 1


def test_seeded_reservoir_preserves_nested_json(archive, tmp_path):
    sample = tmp_path / "sample.jsonl"
    summary = tmp_path / "schema.json"
    result = inspect_and_sample([archive], sample_size=2, sample_path=sample, summary_path=summary)
    first = sample.read_bytes()
    assert result["records"] == 2
    assert result["provenance"]["valid_source_records"] == 4
    assert result["provenance"]["scope"] == "sample"
    assert result["provenance"]["data_origin"].startswith("user-provided")
    inspect_and_sample([archive], sample_size=2, sample_path=sample, summary_path=summary)
    assert sample.read_bytes() == first
    assert all(isinstance(record["payload"], dict) for record in iter_events(sample))


def test_eda_outputs_and_sample_hash_guard(archive, tmp_path):
    sample = tmp_path / "sample.jsonl"
    inspect_and_sample([archive], sample_path=sample, summary_path=tmp_path / "schema.json")
    _, summary, _ = run_analysis(sample, output_dir=tmp_path / "processed", chart_dir=tmp_path / "charts")
    assert len(summary["charts"]) == 7
    assert (tmp_path / "processed/analysis_summary.json").exists()
    assert (tmp_path / "charts/events_over_time.png").stat().st_size > 1000
    sample.write_text(sample.read_text() + '{}\n')
    with pytest.raises(ValueError, match="hash differs"):
        run_analysis(sample, output_dir=tmp_path / "processed", chart_dir=tmp_path / "charts")


def test_pandas_record_cap(archive, tmp_path):
    with pytest.raises(ValueError, match="exceeds"):
        run_analysis(archive, max_records=2, output_dir=tmp_path, chart_dir=tmp_path / "charts")


def test_empty_input_can_generate_honest_charts(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    _, summary, _ = run_analysis(path, output_dir=tmp_path / "processed", chart_dir=tmp_path / "charts")
    assert summary["records_analyzed"] == 0
    assert len(summary["charts"]) == 7
