"""Verify the saved real-data artifacts without silently assuming success."""
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import nbformat
from PIL import Image

from config.settings import CHART_DIR, PROCESSED_DIR, RAW_DIR, REPORTS_DIR, ROOT, SAMPLE_PATH
from exploration.io import iter_events, sha256_file, write_json
from ingestion.download_gharchive import validate_gzip


def verify_outputs():
    checks = {}

    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise RuntimeError(f"Artifact verification failed: {name}")

    summary = json.loads((PROCESSED_DIR / "analysis_summary.json").read_text())
    metadata = json.loads(SAMPLE_PATH.with_suffix(".metadata.json").read_text())
    check("real_data_origin", metadata["data_origin"] == "real GH Archive")
    manifest = json.loads((RAW_DIR / "download_manifest.json").read_text())
    for source in metadata["sources"]:
        archive = ROOT / source["path"]
        check(f"archive_checksum:{archive.name}", sha256_file(archive) == source["sha256"])
        check(f"archive_gzip:{archive.name}", validate_gzip(archive) > 0)
        check(f"download_provenance:{archive.name}",
              manifest["files"][archive.name]["provenance"] == "HTTPS download from GH Archive")
    check("sample_checksum", sha256_file(SAMPLE_PATH) == metadata["sample_sha256"] == summary["input_sha256"])
    check("sample_record_count", sum(1 for _ in iter_events(SAMPLE_PATH)) == summary["records_analyzed"] == metadata["sample_records"])
    check("event_count_total", sum(summary["event_type_counts"].values()) == summary["records_analyzed"])
    schema = json.loads((PROCESSED_DIR / "schema_summary.json").read_text())
    check("schema_record_count", schema["records"] == summary["records_analyzed"])
    spark = json.loads((PROCESSED_DIR / "spark_verification.json").read_text())
    check("spark_local_mode", spark["status"] == "passed" and spark["master"] == "local[2]" and spark["smoke_sum"] == 6)
    check("spark_pandas_record_parity", spark["records"] == summary["records_analyzed"])
    check("spark_pandas_event_parity", spark["event_type_counts"] == summary["event_type_counts"])
    check("spark_pandas_repository_parity", spark["unique_repositories"] == summary["unique_repositories_by_id"])
    check("spark_pandas_account_parity", spark["unique_accounts"] == summary["unique_accounts_by_id"])
    check("seven_charts", len(summary["charts"]) == 7)
    for name in summary["charts"]:
        with Image.open(CHART_DIR / name) as chart:
            check(f"chart_dimensions:{name}", chart.width >= 1000 and chart.height >= 600)
            chart.verify()
    notebook = nbformat.read(ROOT / "notebooks/01_gharchive_exploration.ipynb", as_version=4)
    nbformat.validate(notebook)
    cells = [cell for cell in notebook.cells if cell.cell_type == "code"]
    check("notebook_cells_executed", len(cells) > 0 and all(cell.execution_count is not None for cell in cells))
    check("notebook_no_error_outputs", all(out.output_type != "error" for cell in cells for out in cell.outputs))
    status = json.loads((REPORTS_DIR / "notebook_execution.json").read_text())
    check("notebook_success_evidence", status["status"] == "passed" and status["executed_code_cells"] == len(cells))
    suites = ET.parse(REPORTS_DIR / "tests.xml").getroot()
    testcases = suites.findall(".//testcase")
    check("automated_tests_passed", len(testcases) > 0 and not suites.findall(".//failure")
          and not suites.findall(".//error") and not suites.findall(".//skipped"))
    write_json(REPORTS_DIR / "artifact_verification.json", {"status": "passed", "checks": checks,
                                                          "tests_passed": len(testcases)})
    print(f"Verified {len(checks)} artifact checks and {len(testcases)} passing tests.")
    return checks


if __name__ == "__main__":
    verify_outputs()
