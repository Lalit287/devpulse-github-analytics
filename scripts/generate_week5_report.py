"""Generate an evidence-based Week 5 report, including explicitly pending checks."""
import json
from datetime import datetime, timezone

from config.settings import ROOT
from exploration.io import write_json
from scripts.week5_evidence import EVIDENCE, TEST_GROUPS, code_hashes, test_result


def generate():
    tests = {name: test_result(name) for name in TEST_GROUPS}
    path = EVIDENCE / 'verification.json'
    evidence = json.loads(path.read_text()) if path.exists() else {}
    complete = (evidence.get('status') == 'complete'
                and evidence.get('code_hashes') == code_hashes()
                and evidence.get('tests') == tests
                and all(t['status'] == 'passed' for t in tests.values()))
    state = 'COMPLETE' if complete else 'IMPLEMENTED — LIVE DATABASE VERIFICATION PENDING'
    lines = [f'# Week 5 — PostgreSQL and interactive dashboard', '', f'**Status: {state}.**', '',
             f'Report generated: {datetime.now(timezone.utc).isoformat()}.', '',
             '## Implemented deliverables', '',
             '- Project-owned native PostgreSQL cluster on loopback port 15432 with private credentials and separate owner/writer/reader roles.',
             '- Bounded binary COPY from all 15 verified Gold Parquet tables into typed snapshot schemas, indexes, full row-count and aggregate checks, and sampled field parity.',
             '- Atomic activation, unchanged-load reuse, advisory loader lock, durable run ledger, and transaction rollback drill.',
             '- Six Streamlit pages: overview, repositories and comparisons, languages/technology, public accounts and participation, prediction status, and pipeline monitoring.',
             '- Parameterized read-only queries, stable-ID grouping, bounded search/rankings, CSV exports, snapshot-aware caching, and explicit coverage/window labels.', '',
             'The prediction page reserves Week 6 model output; it does not invent probabilities.', '',
             '## Validation evidence', '',
             '| Group | Status | Tests |', '|---|---|---:|']
    for name, result in tests.items():
        lines.append(f"| {name} | {result['status']} | {result['tests']} |")
    lines += ['', 'Offline page tests use explicitly synthetic fixtures only inside Pytest. They establish UI behavior and do not establish a successful PostgreSQL load.', '']
    if complete:
        lines += [f"Native server: PostgreSQL {evidence['database']['server_version']}.", '',
                  f"All 15 loaded tables match the current Gold snapshot `{evidence['gold_snapshot_id']}`; clean-event total: **{evidence['input_events']:,}**.", '',
                  f"Active load: `{evidence['load_id']}`. Verification checks read-only grants, zero-copy rerun, failed-load rollback, dashboard HTTP health, and all {evidence['preservation']['checked_files']} protected source/report hashes.", '',
                  'Query measurements are single local requests including connection time, not distributed benchmarks.', '',
                  '| Ranking | Rows | Seconds |', '|---|---:|---:|']
        for name, result in evidence['query_measurements'].items():
            lines.append(f"| {name} | {result['rows']} | {result['seconds']} |")
    else:
        lines += ['The initial native PostgreSQL initialization was blocked inside the Codex execution sandbox:', '',
                  '```text', 'FATAL: could not create shared memory segment: Operation not permitted',
                  'DETAIL: Failed system call was shmget(...).', '```', '',
                  'The real database load, database-backed page tests, idempotency, and rollback checks have not all passed with current completion evidence. A running Streamlit server by itself does not make Week 5 complete.', '',
                  'Run the completion helper in a normal Mac Terminal. It starts only this project cluster, loads the existing real Gold data, executes all validation groups, and regenerates this report:', '',
                  '```bash', 'cd ~/Desktop/DevPulse', '.venv/bin/python -m scripts.complete_week5', '```']
    lines += ['', '## Scope and interpretation', '',
              'The available dataset is one complete UTC day (2025-01-01), with 3,909,986 unique clean events. Intraday comparisons use equal 12-hour windows; they do not establish multi-day trends. Metadata is current and targeted, covering 1.63% of event activity; named primary language covers 0.24%. Accounts include bots. Star events are observed actions, not current cumulative repository stars.', '',
              'Earlier-week reports, immutable snapshots, metadata cache, and raw archives are retained. Credentials and native database files are Git-ignored in `.runtime/`.', '',
              'Operations: [Week 5 guide](../docs/week5_database_dashboard.md). Schema: [PostgreSQL contract](../docs/contracts/postgresql.md).', '']
    (ROOT / 'reports/week5_report.md').write_text('\n'.join(lines))
    write_json(EVIDENCE / 'completion_status.json', {'status': 'complete' if complete else 'pending_database_verification',
                                                  'tests': tests})
    return complete


if __name__ == '__main__':
    print('complete' if generate() else 'pending_database_verification')
