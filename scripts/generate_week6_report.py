"""Measured model report; completion requires fresh native verification evidence."""
import json
import os
from datetime import datetime,timezone

from config.settings import ROOT
from exploration.io import sha256_file,write_json
from spark.snapshots import current_manifest
from scripts.complete_week6 import implementation_hashes

EVIDENCE=ROOT/'reports/week6'


def plots(m):
    os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'.runtime/matplotlib'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    labels=list(m['models'])+['recent_star_baseline']
    values=[m['models'][n]['test']['pr_auc'] for n in m['models']]+[m['baseline']['test']['pr_auc']]
    fig,axis=plt.subplots(figsize=(9,4.5));axis.barh([n.replace('_',' ') for n in labels],values,color=['#14a38b','#5364d6','#eda847','#94a3b8'])
    axis.set_xlim(0,1);axis.set_xlabel('Held-out test PR-AUC (Spark trapezoidal)');axis.set_title('Later-period model comparison')
    for i,value in enumerate(values):axis.text(value+.01,i,f'{value:.3f}',va='center')
    fig.tight_layout();fig.savefig(EVIDENCE/'test_pr_auc.png',dpi=150);plt.close(fig)
    chosen=m['selected_model'];test=m['models'][chosen]['test'];bins=m['models'][chosen]['test_diagnostics']['reliability_bins']
    fig,axes=plt.subplots(1,2,figsize=(10,4.5))
    image=axes[0].imshow([[test['tn'],test['fp']],[test['fn'],test['tp']]],cmap='Blues')
    for y,row in enumerate([[test['tn'],test['fp']],[test['fn'],test['tp']]]):
        for x,value in enumerate(row):axes[0].text(x,y,f'{value:,}',ha='center',va='center',color='white' if value>test['tn']/2 else 'black')
    axes[0].set_xticks([0,1],['Predicted negative','Predicted positive']);axes[0].set_yticks([0,1],['Actual negative','Actual positive']);axes[0].set_title('Test confusion matrix')
    axes[1].plot([0,1],[0,1],'--',color='gray');axes[1].scatter([b['mean_score'] for b in bins],[b['observed_positive_fraction'] for b in bins],color='#14a38b')
    axes[1].set(xlim=(0,1),ylim=(0,1),xlabel='Mean model probability',ylabel='Observed positive fraction',title='Uncalibrated score reliability')
    fig.tight_layout();fig.savefig(EVIDENCE/'test_diagnostics.png',dpi=150);plt.close(fig)


def generate():
    path=EVIDENCE/'verification.json'
    evidence=json.loads(path.read_text()) if path.exists() else {}
    m=current_manifest(ROOT/'data/models')
    complete=bool(m and evidence.get('status')=='complete' and evidence.get('model_id')==m['snapshot_id']
                  and evidence.get('implementation_hashes')==implementation_hashes()
                  and all(sha256_file(EVIDENCE/f'{n}.xml')==h for n,h in evidence.get('test_xml_hashes',{}).items()))
    lines=['# Week 6 — Chronological repository-popularity prediction','',
           '**Status: '+('COMPLETE — evaluated historical experiment.' if complete else 'IN PROGRESS — native evaluation/verification pending.')+'**','',
           f'Report generated: {datetime.now(timezone.utc).isoformat()}.','',
           '## Dataset and scope','',
           'The original 2025-01-01 activity dataset and earlier reports are preserved. Week 6 uses a separate complete-hour GH Archive corpus for 2015-01-01 through 2015-02-11: 42 days / 1,008 hourly files. This is a bounded historical experiment; it does not establish current GitHub forecasting performance.','']
    if m:
        c=m['corpus'];chosen=m['selected_model'];threshold=m['target_growth_threshold']
        lines += [f"Raw events: **{c['raw_rows']:,}**. Projected rows: {c['projected_rows']:,}; rejected rows: {c['rejected_rows']:,}. Unique events: **{m['clean_events']:,}**. Duplicate rows: {m['duplicate_rows']:,}; conflicting duplicate IDs: {m['conflicting_duplicate_ids']:,}. Compressed archive bytes: {c['compressed_bytes']:,}.",'',
                  f"Events whose creation timestamps fall outside their source week: {m['outside_source_week_events']:,}; these are excluded from feature/outcome windows rather than retroactively inserted using a later archive.",'',
                  '## Temporal design and target','',
                  f'Target = 1 when the next seven days contain at least five observed star actions and a gain of at least **{threshold}** against the previous seven days. The gain cutoff is the exact 90th percentile of positive training-period gains. Validation/test outcomes do not choose the target.','',
                  '| Split | First cutoff (UTC) | Last cutoff (UTC) | Latest outcome end (UTC) | Eligible rows | Positives |',
                  '|---|---|---|---|---:|---:|']
        for name in ['train','validation','test','forecast']:
            s=m['splits'][name]
            lines.append(f"| {name} | {s['first_as_of']} | {s['last_as_of']} | {s['last_label_end']} | {s['rows']:,} | {int(s['positives']) if s['positives'] is not None else 'unknown'} |")
        lines += ['', 'Candidates require at least five past-week events. Features include log-transformed star/fork/push/PR/issue/total counts, distinct public and code-participating accounts, active days and within-week star-rate change. Repository IDs/names, later metadata, future counts and target columns never enter the feature vector. Full future windows stay inside their assigned split. Repository IDs may recur across periods; this evaluates future activity for observed repositories.', '',
                  'Training includes three weekly cutoffs. Validation chooses the algorithm by PR-AUC and its decision threshold by F1 on a preregistered grid. The later test period is used only for the final comparison. The saved selected model is reloaded and prediction parity checked.', '',
                  '## Measured comparison','',
                  '| Algorithm | Split | Precision | Recall | F1 | ROC-AUC | PR-AUC | P@10 | P@100 | Brier |',
                  '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
        for name,value in list(m['models'].items())+[('recent_star_ranking_baseline',m['baseline'])]:
            for split in ['validation','test']:
                v=value[split]
                vals=' | '.join(f'{v[k]:.4f}' for k in ['precision','recall','f1','roc_auc','pr_auc','precision_at_10','precision_at_100'])
                brier=f"{v['brier_score']:.4f}" if 'brier_score' in v else 'ranking only'
                lines.append(f'| {name} | {split} | {vals} | {brier} |')
        test=m['models'][chosen]['test'];baseline=m['baseline']['test']
        lines += ['',f"Selected algorithm: **{chosen}**. Frozen decision threshold: {m['decision_threshold']}. Test prevalence: {test['prevalence']:.4%}. Test PR-AUC versus recent-star ranking: {test['pr_auc']:.4f} vs {baseline['pr_auc']:.4f}.",'',
                  ('The selected model beats the simple recent-star ranking on this test PR-AUC.' if test['pr_auc']>baseline['pr_auc'] else 'The selected model does not beat the simple recent-star ranking on test PR-AUC; predictive improvement is not established.'),'',
                  'PR-AUC uses Spark’s exact unbinned trapezoidal curve and is not average precision. Scores are uncalibrated probabilities; reliability bins and Brier score expose calibration limitations. Baseline scores are only monotone ranking scores, not probability estimates.','',
                  f"Artifact: `data/models/snapshots/{m['snapshot_id']}`. Model/runtime duration: {m['duration_seconds']:.3f} seconds. Forecast population: {m['forecast_rows']:,}; held-out population: {m['heldout_rows']:,}.",'']
        if complete:
            plots(m)
            lines += ['![Test PR-AUC comparison](week6/test_pr_auc.png)','', '![Test confusion matrix and reliability](week6/test_diagnostics.png)','',
                      '## Verification','',f"Protected earlier-week files: **{evidence['protected_files']}**, all unchanged. Native reader role: `{evidence['reader_role']}`. Atomic publication rollback and zero-copy database reuse pass. Prediction counts, sums, UTC cutoff and bounds match Parquet. The live page provides rankings, literal search, CSV, measured model comparisons, actual-versus-predicted test results and reliability diagnostics.",'',
                      '| Test group | Passed |','|---|---:|']
            for name,counts in evidence['tests'].items():lines.append(f"| {name} | {counts['tests']} |")
    lines += ['', '## Limitations','',
              'Only six consecutive weeks, one validation period and one test period are evaluated. No statistical confidence or broad generalization is claimed. Historical archive hours approximate when public activity was observed; actual archive publication latency cannot be reconstructed. Unknown future activity is zero only for completed labelled windows; forecast outcomes remain NULL. Current language metadata is excluded. Repository age and 30-day activity features are omitted because the required lookbacks are incomplete. Event activity includes bots and does not measure developer productivity. The optional seven-day star-count regression enhancement remains outside the required binary-classification milestone.', '',
              'Operations: [Week 6 guide](../docs/week6_prediction.md). Sources: [GH Archive schema/history](https://www.gharchive.org/) and [Spark 4.0.1 classification](https://spark.apache.org/docs/4.0.1/ml-classification-regression.html).','']
    (ROOT/'reports/week6_report.md').write_text('\n'.join(lines))
    write_json(EVIDENCE/'completion_status.json',{'status':'complete' if complete else 'in_progress','model_id':m['snapshot_id'] if m else None})
    return complete


if __name__=='__main__':print('complete' if generate() else 'in_progress')
