"""Completion must reject absent native checks and stale implementation evidence."""
import json
from scripts import generate_week5_report as report


def test_report_rejects_missing_native_tests_and_stale_code(tmp_path, monkeypatch):
    evidence = tmp_path / 'reports/week5'
    evidence.mkdir(parents=True)
    monkeypatch.setattr(report, 'ROOT', tmp_path)
    monkeypatch.setattr(report, 'EVIDENCE', evidence)
    monkeypatch.setattr(report, 'code_hashes', lambda: {'current': 'hash'})
    native = {'status': 'not_run', 'tests': 0}
    monkeypatch.setattr(report, 'test_result', lambda name: native if name == 'database_tests' else {'status': 'passed', 'tests': 1})
    (evidence / 'verification.json').write_text(json.dumps({'status': 'complete', 'code_hashes': {'current': 'hash'}}))
    assert report.generate() is False
    assert 'VERIFICATION PENDING' in (tmp_path / 'reports/week5_report.md').read_text()
    tests = {name: {'status': 'passed', 'tests': 1} for name in report.TEST_GROUPS}
    monkeypatch.setattr(report, 'test_result', lambda name: tests[name])
    (evidence / 'verification.json').write_text(json.dumps({'status': 'complete', 'code_hashes': {'old': 'hash'}, 'tests': tests}))
    assert report.generate() is False
    assert json.loads((evidence / 'completion_status.json').read_text())['status'] == 'pending_database_verification'
