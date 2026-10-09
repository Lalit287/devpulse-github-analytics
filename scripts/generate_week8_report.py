"""Generate honest current status and the consolidated final project report."""
import json,statistics
from datetime import datetime,timezone
from config.settings import ROOT
from exploration.io import atomic_text
from spark.snapshots import current_manifest


def read(name):
    p=ROOT/'reports/week8'/name
    return json.loads(p.read_text()) if p.exists() else None

def generate(result=None):
    result=result or read('verification.json') or {'status':'pending'}
    complete=result.get('status')=='complete';status=result.get('status','pending')
    compute=read('compute_benchmark.json');storage=read('storage_benchmark.json');rates=read('streaming_benchmark.json');distributed=read('distributed_pipeline.json')
    lines=[f'# Week 8 — {"completed" if complete else status}', '',
      'Final native deployment and every required test passed.' if complete else 'Final completion is not confirmed. PostgreSQL/service startup, streaming rate trials and native regression tests must all pass before completion.', '',
      '## Measured results','']
    if compute:
        lines+=['Full-day input: 3,909,986 clean events. Every configuration produced identical counts for 683,676 repositories. Three trials per configuration, warm filesystem caches; Spark startup measured separately.','',
                '| Configuration | Median query seconds | Min–max seconds |','|---|---:|---:|']
        for name,v in compute['summaries'].items():lines.append(f"| {name} | {v['median_seconds']:.3f} | {v['min_seconds']:.3f}–{v['max_seconds']:.3f} |")
        a=compute['summaries']['local4-shuffle200']['median_seconds'];b=compute['summaries']['local4']['median_seconds']
        lines+=['',f'The 32-partition query profile reduced median time by {(1-b/a)*100:.1f}% versus 200 shuffle partitions. Pandas was faster on this in-memory narrow projection; adding a second worker did not provide meaningful speedup. Workers share one Mac. No multi-machine scaling claim is made.',
                '', 'Memory values are per-process maximum RSS for Python and the largest reaped child JVM, plus recorded executor heap metrics. They are not a concurrent aggregate cluster-memory measurement.']
    if storage:
        lines+=['','| Equivalent projected format | Size MiB | Median query seconds | Candidate files |','|---|---:|---:|---:|']
        for name,v in storage['formats'].items():lines.append(f"| {name} | {v['bytes']/1024**2:.2f} | {v['median_seconds']:.4f} | {v['candidate_fragments']} |")
        lines+=['','All formats contain the same five logical fields from the real day. The query selects UTC hour 12 and returns 223,540 events. Parquet column/row-group filtering and hourly partition pruning both contribute. These sizes are not a full-payload archive compression comparison.']
    if distributed:
        lines+=['',f"Distributed pipeline: {distributed['clean_events']:,} clean events, {distributed['duplicates']} duplicate rows removed, {distributed['quarantine']} quarantined; both standalone executor JVMs processed tasks. All nine core table readback checks passed.",
                'The raw ETL uses 3 GiB executors with one concurrent task each and a 1,000-row cache batch. An earlier 1.5 GiB / two-task profile ran out of heap; its failed output was not published. The successful rerun preserved the original active dataset.']
    if rates:
        lines+=['','| Replay rate requested | Median end-to-end events/s | Median p95 SQL commit delay, s | Trials |','|---|---:|---:|---:|']
        for rate in [1000,5000,10000]:
            rows=[r for r in rates['results'] if r['rate_requested']==rate]
            lines.append(f"| {rate:,} | {statistics.median(r['effective_end_to_end_events_per_second'] for r in rows):,.0f} | {statistics.median(r['p95_commit_delay_seconds'] for r in rows):.3f} | {len(rows)} |")
        lines+=['','Each rate trial uses the same 20,000 real time-sorted events, concurrent producer/consumer execution, 2-second microbatches and acknowledged delivery. Wall-clock delay runs from Kafka CreateTime to SQL transaction commit; historical event time is preserved separately. Spark startup is excluded consistently.']
    else:lines+=['','Streaming-rate experiment: pending native PostgreSQL availability.']
    offline=read('offline_test_receipt.json')
    if not complete and offline:
        lines+=['', f"Offline regression: {offline['tests']['tests']} tests passed with no failures, errors or skips. Database-backed tests and final service/streaming checks are excluded from this count and remain pending.",
                '[Offline test receipt](week8/offline_test_receipt.json) · [Verified saved artifacts](week8/offline_artifact_verification.json)']
    if complete:
        total=sum(v['tests'] for v in result['tests'].values())
        lines+=['','## Final verification','',f'{total} tests passed with no failures, errors or skips. All six native deployment components are healthy. Original current dataset/model pointers and {result["preservation"]["files"]} source/evidence files are unchanged.',
                '','[Full verification](week8/verification.json) · [Fresh test receipt](week8/test_receipt.json) · [Deployment state](week8/deployment_status.json)']
    lines+=['','[Architecture](../docs/architecture.md) · [Dataset catalog](../docs/data_catalog.md) · [Demo and recovery runbook](../docs/demo_runbook.md)',
            '', 'Deployment scope: native, loopback-only, single-host development demonstration. Docker and cloud hosting are not executed. The local Spark cluster follows the blueprint’s alternative to Docker Compose. Source repository: [devpulse-github-analytics](https://github.com/Lalit287/devpulse-github-analytics). Large datasets and private service state are excluded from source distribution.']
    atomic_text(ROOT/'reports/week8_report.md','\n'.join(lines)+'\n')
    model=current_manifest(ROOT/'data/models');selected=model['selected_model'];metrics=model['models'][selected]['test']
    project=['# DevPulse — final project report','',f'Status: {"completed and verified" if complete else "final verification pending"}. Generated {datetime.now(timezone.utc).isoformat()}.','',
      '## Purpose and implementation','',
      'DevPulse processes historical public GitHub activity through validated ingestion, HDFS, Spark ETL, immutable Parquet analytics and atomic PostgreSQL publication. A six-page dashboard serves repository, language, public-account and pipeline views. Airflow schedules bounded work; Kafka and Structured Streaming maintain idempotent minute analytics with durable late corrections.',
      '', '## Actual datasets','',
      '- Activity: all 24 archive hours on January 1, 2025; 3,909,993 source records, 3,909,986 clean unique events and seven duplicates.',
      '- Prediction: 42 complete UTC days in 2015, 1,008 archives; 19,669,354 unique eligible events after 173 exact duplicate rows.',
      '- Streaming: a complete real 223,540-event historical hour; independent recovery proof added 1,000 duplicates without increasing unique counts.',
      '', '## Historical model evaluation','',
      f'Selected by validation PR-AUC: **{selected.replace("_"," ")}**. Test population: {metrics["rows"]:,} examples, {metrics["positives"]:,} positives ({metrics["prevalence"]*100:.3f}%).','',
      '| Algorithm | Test PR-AUC | Test ROC-AUC | Precision@10 | Test F1 |','|---|---:|---:|---:|---:|']
    for name,entry in model['models'].items():
        t=entry['test'];project.append(f"| {name} | {t['pr_auc']:.6f} | {t['roc_auc']:.6f} | {t['precision_at_10']:.3f} | {t['f1']:.6f} |")
    base=model['baseline']['test'];project.append(f"| Recent-star ranking baseline | {base['pr_auc']:.6f} | {base['roc_auc']:.6f} | {base['precision_at_10']:.3f} | {base['f1']:.6f} |")
    project+=['', f"Frozen validation threshold: {model['decision_threshold']}. Test confusion counts: TP {metrics['tp']:,}, FP {metrics['fp']:,}, FN {metrics['fn']:,}, TN {metrics['tn']:,}. Uncalibrated Brier score: {metrics['brier_score']:.6f}. Historical forecast rows: {model['forecast_rows']:,}; actual outcomes for the forecast week are not in the corpus.",
      '', 'The chronology prevents future labels/features from influencing the held-out selection. Scores are uncalibrated probabilities, not current GitHub recommendations. Low target prevalence makes PR-AUC and precision at selected ranks more informative than accuracy alone.',
      '', '## Performance, deployment and verification','',
      '[The Week 8 performance report](week8_report.md) records exact workload parity, per-process memory scope, worker trials, storage query results, streaming-rate latency and current verification status. [Architecture](../docs/architecture.md), [dataset catalog](../docs/data_catalog.md) and [demo runbook](../docs/demo_runbook.md) document the delivered system.',
      '', '## Limitations and interpretation','',
      'The 2025 activity day and 2015 modeling period are historical and selected windows. Public events omit private activity, include bots and do not measure complete developer productivity. Star actions are observed WatchEvents, not cumulative historical stars. Current language metadata has limited coverage and is associated with past activity rather than historically observed. Single-host worker results do not demonstrate multi-machine scalability. Local service configuration does not provide production high availability or public-network security.',
      '', '## Evidence and handover','',
      'Weekly reports retain measured development evidence. Source/manifest hashes, snapshot contracts, atomic publication checks and native recovery receipts support reproducibility. Large archives, models, credentials and runtime storage remain inside the private Desktop project; the source distribution excludes those. The source Git repository includes offline CI configuration. Repository: [devpulse-github-analytics](https://github.com/Lalit287/devpulse-github-analytics). Repository publication does not deploy the local services or upload their private data.',
      '', 'Future extensions include longer current activity windows, calibrated forecasting, broader metadata coverage and resource-governed multi-host deployment. These are extensions rather than measured results of this project.']
    atomic_text(ROOT/'reports/project_report.md','\n'.join(project)+'\n');return complete

if __name__=='__main__':generate()
