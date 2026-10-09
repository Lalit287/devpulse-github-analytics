"""Shared evidence checks; reports never infer database success from fixture tests."""
import json
import subprocess
import sys
import xml.etree.ElementTree as ET

from config.settings import ROOT
from exploration.io import sha256_file, write_json

EVIDENCE = ROOT / 'reports/week5'
TEST_GROUPS = {
    'offline_tests': (13, ['tests/test_week5_contracts.py', 'tests/test_week5_dashboard_offline.py', 'tests/test_week5_evidence.py']),
    'database_tests': (11, ['tests/test_week5_database.py', 'tests/test_week5_dashboard.py']),
    'previous_week_regression': (131, [
        'tests/test_download.py', 'tests/test_exploration.py',
        'tests/test_week2_ingestion.py', 'tests/test_week2_storage.py',
        'tests/test_week3_etl.py', 'tests/test_week4_analytics.py',
        'tests/test_week4_enrichment.py', 'tests/test_week4_publication.py']),
}


def code_hashes():
    paths = [ROOT / 'requirements.txt', ROOT / '.streamlit/config.toml']
    for directory in ['database', 'dashboard', 'scripts']:
        paths.extend((ROOT / directory).rglob('*.py'))
    paths.extend((ROOT / 'database').glob('*.sql'))
    paths.extend((ROOT / 'tests').glob('test_week5*.py'))
    return {str(p.relative_to(ROOT)): sha256_file(p) for p in sorted(paths)}


def test_result(name):
    path = EVIDENCE / f'{name}.xml'
    if not path.exists():
        return {'status': 'not_run', 'tests': 0}
    try:
        suites = list(ET.parse(path).getroot().iter('testsuite'))
        counts = {k: sum(int(s.get(k, 0)) for s in suites)
                  for k in ['tests', 'failures', 'errors', 'skipped']}
    except (ET.ParseError, ValueError):
        return {'status': 'invalid', 'tests': 0}
    passed = (counts['tests'] == TEST_GROUPS[name][0]
              and not any(counts[k] for k in ['failures', 'errors', 'skipped']))
    return {'status': 'passed' if passed else 'failed', **counts,
            'sha256': sha256_file(path)}


def preservation():
    expected = json.loads((EVIDENCE / 'previous_weeks_preservation.json').read_text())
    changed = [name for name, digest in expected.items()
               if not (ROOT / name).is_file() or sha256_file(ROOT / name) != digest]
    return {'status': 'passed' if not changed else 'failed',
            'checked_files': len(expected), 'changed_files': changed}


def run_tests(name):
    """Record the tested implementation and XML hash only after a successful run."""
    receipt = EVIDENCE / f'{name}_receipt.json'
    receipt.unlink(missing_ok=True)
    before = code_hashes()
    with (EVIDENCE / f'{name}.log').open('w') as log:
        subprocess.run([sys.executable, '-m', 'pytest', *TEST_GROUPS[name][1], '-q',
                        f'--junitxml={EVIDENCE / (name + ".xml")}'],
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    result = test_result(name)
    if result['status'] != 'passed' or before != code_hashes():
        raise ValueError('Tests failed, skipped, or implementation changed while testing')
    write_json(receipt, {'code_hashes': before, 'result': result})
    return result


def test_receipts_current():
    current = code_hashes()
    for name in TEST_GROUPS:
        path = EVIDENCE / f'{name}_receipt.json'
        if not path.exists():
            return False
        receipt = json.loads(path.read_text())
        if receipt.get('code_hashes') != current or receipt.get('result') != test_result(name):
            return False
    return True
