"""Verify native PostgreSQL, full Gold parity, recovery tests, and source preservation."""
import json
import time
from datetime import datetime, timezone

import pyarrow.compute as pc
from psycopg import errors

from config.settings import ROOT
from database.cluster import connect, status
from database.contracts import KEYS, dataset
from database.load_analytics import verify_table
from dashboard import service
from dashboard.run import status as dashboard_status
from exploration.io import sha256_file, write_json
from spark.snapshots import current_manifest
from scripts.week5_evidence import EVIDENCE, TEST_GROUPS, code_hashes, preservation, test_result, test_receipts_current


def verify():
    # Remove stale success before any operation that can fail.
    write_json(EVIDENCE / 'verification.json', {'status': 'running'})
    started = time.monotonic()
    preserved = preservation()
    if preserved['status'] != 'passed':
        raise ValueError('Earlier-week files changed: ' + ', '.join(preserved['changed_files']))
    tests = {name: test_result(name) for name in TEST_GROUPS}
    if any(result['status'] != 'passed' for result in tests.values()):
        raise ValueError('All three test groups must pass with no skipped tests')
    if not test_receipts_current():
        raise ValueError('Test receipts are missing or stale; run the completion helper')
    database = status()
    if database['status'] != 'healthy':
        raise RuntimeError('Native PostgreSQL is not healthy')
    snapshot = service.current_dataset()
    gold = current_manifest(ROOT / 'data/gold')
    source = ROOT / 'data/gold/snapshots' / gold['snapshot_id']
    if (snapshot['gold_snapshot_id'] != gold['snapshot_id'] or
            snapshot['source_sha256'] != sha256_file(source / 'manifest.json')):
        raise ValueError('Database does not represent the current verified Gold snapshot')
    checks = {}
    with connect('reader') as connection:
        if connection.execute('SELECT current_user').fetchone()[0] != 'devpulse_reader':
            raise ValueError('Dashboard role mismatch')
        for name in KEYS:
            arrow = dataset(source / name)
            totals = {}
            samples = []
            for batch in arrow.scanner(batch_size=4096, use_threads=False).to_batches():
                for field in batch.schema:
                    if ((field.name.endswith('_events') or field.name.startswith('attributed_'))
                            and (str(field.type).startswith('int') or str(field.type) == 'double')):
                        value = pc.sum(batch.column(field.name)).as_py()
                        if value is not None:
                            totals[field.name] = totals.get(field.name, 0) + value
                if len(samples) < 5:
                    samples.extend(batch.slice(0, 5 - len(samples)).to_pylist())
            samples = [{k: (v.replace(tzinfo=timezone.utc)
                        if isinstance(v, datetime) and v.tzinfo is None else v)
                        for k,v in sample.items()} for sample in samples]
            checks[name] = verify_table(connection, snapshot['schema_name'], name,
                                        gold['tables'][name]['rows'], totals, samples)
        exact = connection.execute('SELECT sum(total_events) FROM devpulse.repository_metrics').fetchone()[0]
        if int(exact) != gold['input_events']:
            raise ValueError('Clean-event parity failed')
    with connect('reader') as connection:
        connection.execute('SET TRANSACTION READ WRITE')
        try:
            connection.execute('UPDATE devpulse_control.current_dataset SET load_id=load_id WHERE false')
        except errors.InsufficientPrivilege:
            pass
        else:
            raise ValueError('Reader can write control data')
    with connect('writer') as connection:
        runs = connection.execute("SELECT status,verification_run,copied_rows FROM devpulse_control.pipeline_runs WHERE load_id=%s", (snapshot['load_id'],)).fetchall()
        rollback_count = connection.execute("SELECT count(*) FROM devpulse_control.pipeline_runs WHERE verification_run AND status='failed' AND error_class='InjectedLoadFailure'").fetchone()[0]
    if not any(s == 'complete' and not v and n > 0 for s,v,n in runs):
        raise ValueError('No successful full real-data load')
    if not any(s == 'reused' and n == 0 for s,v,n in runs) or not rollback_count:
        raise ValueError('Idempotency and rollback drill evidence missing')
    dashboard = dashboard_status()
    if dashboard['status'] != 'healthy':
        raise RuntimeError('Dashboard HTTP health check failed')
    measurements = {}
    for sort in service.SORTS:
        left = time.perf_counter()
        rows = service.repository_rankings(snapshot, sort=sort, limit=20)
        measurements[sort] = {'rows': len(rows), 'seconds': round(time.perf_counter()-left, 4)}
    result = {'status': 'complete', 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
              'duration_seconds': round(time.monotonic()-started, 3),
              'load_id': snapshot['load_id'], 'gold_snapshot_id': gold['snapshot_id'],
              'input_events': gold['input_events'], 'tables': checks,
              'database': database, 'dashboard': dashboard, 'tests': tests,
              'preservation': preserved, 'query_measurements': measurements,
              'code_hashes': code_hashes()}
    write_json(EVIDENCE / 'verification.json', result)
    return result


def main():
    try:
        result = verify()
    except Exception as exc:
        write_json(EVIDENCE / 'verification.json', {'status': 'failed', 'error_class': type(exc).__name__})
        raise
    print(json.dumps({k: v for k, v in result.items() if k not in {'code_hashes', 'tables'}}, indent=2))


if __name__ == '__main__':
    main()
