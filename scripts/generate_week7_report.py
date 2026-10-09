"""Write Week 7 completion status from fresh measured evidence."""
import json
from config.settings import ROOT
from exploration.io import atomic_text

def generate(result=None):
    result=result or json.loads((ROOT/'reports/week7/verification.json').read_text())
    if result.get('status')!='complete':
        status=result.get('status','incomplete')
        atomic_text(ROOT/'reports/week7_report.md',f'# Week 7 — {status}\n\nCurrent verification is {status}; completion is not confirmed. Inspect reports/week7/verification.json and the latest completion log.\n')
        return False
    p=result['parity'];r=result['recovery'];tests=result['tests'];total=sum(v['tests'] for v in tests.values())
    lines=['# Week 7 — completed: native Airflow and Kafka streaming','',
      f"Verified at {result['verified_at_utc']}. All {total} tests passed, with no failures or skips.",'',
      '## Delivered and measured','',
      '- Isolated Airflow 3.3.2 with PostgreSQL metadata, authenticated loopback UI, scheduler, DAG processor and LocalExecutor.',
      '- A genuine daily scheduled DAG plus a manually retried run: collection → Spark Silver → Gold → native PostgreSQL → parity checks. All final tasks succeeded.',
      '- Native Kafka 4.2.2 KRaft broker, controlled acknowledged replay, Spark Kafka Structured Streaming and persistent checkpoints.',
      f"- Real source: January 1, 2025, 12:00–13:00 UTC; {p['unique_events']:,} unique events, {p['minutes']} exact minute windows and {p['repository_minutes']:,} exact repository-minute windows.",
      '- Pipeline monitor with historical minute charts, recent repositories/accounts, spikes, duplicate/late metrics, offsets and Airflow run history. Queries use the read-only database role.',
      '', '## Recovery verification','',
      f"- Independent recovery stream: {r['messages']:,} messages = {r['source_events']:,} unique events + {r['duplicate_records']:,} logical duplicates.",
      f"- {r['late_events']:,} late unique event(s) corrected historical windows; {r['out_of_order_events']:,} out-of-order unique events retained.",
      '- Real broker restart preserved partition offsets.',
      '- An injected crash after SQL commit and before Spark checkpoint commit recovered without double counting.',
      '- Replacement checkpoint replay preserved all event/message/duplicate/late counters.',
      '- Continuous microbatch mode, invalid JSON, out-of-hour timestamps, conflicting IDs and transaction rollback passed.',
      '', '## Tests','', '| Group | Passed | Failures/errors/skips |','|---|---:|---:|']
    for name,t in tests.items():lines.append(f"| {name.replace('_',' ')} | {t['tests']} | {t['failures']}/{t['errors']}/{t['skipped']} |")
    lines+=['', '## Preservation and operating state','',
      f"{result['preservation']['files']} original source/evidence files verified unchanged. Original Silver, Gold and model pointers are preserved, and the original PostgreSQL dashboard dataset is restored.",
      'Both demonstration DAGs are paused after measured verification; the daily UTC schedule remains configured. Kafka and Airflow serve only localhost. This is a single-machine development deployment, and the replay is historical.',
      '','## Evidence and usage','',
      '- [Native verification](week7/verification.json)',
      '- [Airflow task/run proof](week7/airflow_verification.json)',
      '- [Recovery drills](week7/recovery_verification.json)',
      '- [Broker offset recovery](week7/broker_recovery.json)',
      '- [Fresh test/code receipt](week7/test_receipt.json)',
      '- [Operations and semantics guide](../docs/week7_orchestration_streaming.md)',
      '', 'Week 8 remains: performance optimization/benchmarking, packaging/deployment and final handover.']
    atomic_text(ROOT/'reports/week7_report.md','\n'.join(lines)+'\n');return True

if __name__=='__main__':generate()
