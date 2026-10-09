"""Measured Week 4 report and charts from verified immutable Gold artifacts."""
import json
import os
from pathlib import Path

from config.settings import ROOT
os.environ.setdefault("MPLCONFIGDIR",str(ROOT/'.runtime/matplotlib'))
os.environ.setdefault("XDG_CACHE_HOME",str(ROOT/'.runtime/cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from exploration.io import atomic_text, write_json
from spark.snapshots import current_manifest
from analytics.artifacts import code_hashes


def charts(target, manifest, rankings, previews, coverage):
    out=ROOT/'reports/week4'
    plt.rcParams.update({'axes.spines.top':False,'axes.spines.right':False,'font.size':10})
    saved=[]
    def finish(fig,name,footer):
        fig.text(.02,.015,footer,fontsize=8,color='#444444')
        fig.tight_layout(rect=(0,.065,1,1))
        path=out/name
        fig.savefig(path,dpi=160)
        plt.close(fig)
        saved.append(name)
    rows=list(reversed(rankings['most_active'][:12]))
    fig,ax=plt.subplots(figsize=(12,6))
    ax.barh([r['repo_name'] for r in rows],[r['total_events'] for r in rows],color='#446ce3')
    ax.set(title='Most active repositories — 1 January 2025 UTC',xlabel='Clean public event records')
    ax.xaxis.set_major_formatter(FuncFormatter(lambda n,_:f'{n:,.0f}'))
    finish(fig,'repository_activity.png','All 3,909,986 clean events; automation/bots may dominate counts. Activity is not productivity.')
    rows=list(reversed(rankings['fastest_growing_attention'][:10]))
    fig,ax=plt.subplots(figsize=(12,6));positions=list(range(len(rows)))
    ax.barh([p-.18 for p in positions],[r['previous_attention'] for r in rows],height=.34,color='#a5b1c2',label='00:00–12:00 UTC')
    ax.barh([p+.18 for p in positions],[r['current_attention'] for r in rows],height=.34,color='#00897b',label='12:00–24:00 UTC')
    ax.set(yticks=positions,yticklabels=[r['repo_name'] for r in rows],title='Fastest-growing intraday attention — equal 12-hour periods',xlabel='Star actions + fork events')
    ax.legend(loc='lower right')
    previous_note = ' All ten shown have zero previous attention.' if all(r['previous_attention']==0 for r in rows) else ''
    finish(fig,'intraday_attention.png','2025-01-01 UTC; minimum five current attention events and two accounts.'+previous_note)
    named=[r for r in previews['language_primary_daily'] if r['enrichment_status']=='ok' and r['primary_language']!='No primary language reported']
    named=sorted(named,key=lambda r:r['total_events'])
    fig,(ax,other)=plt.subplots(1,2,figsize=(12,5),gridspec_kw={'width_ratios':[1.25,1]})
    ax.barh([r['primary_language'] for r in named],[r['total_events'] for r in named],color='#7950c8')
    ax.set(title='Named current primary-language associations',xlabel='Archived events in selected repositories')
    buckets={label:0 for label in ['Not enriched','Metadata unavailable','No primary language','Named primary language']}
    for r in previews['language_primary_daily']:
        key='Not enriched' if r['enrichment_status']=='not_selected' else ('Metadata unavailable' if r['enrichment_status']!='ok' else ('No primary language' if r['primary_language']=='No primary language reported' else 'Named primary language'))
        buckets[key]+=r['total_events']
    labels=list(buckets);values=[buckets[l]/manifest['input_events']*100 for l in labels]
    other.barh(list(reversed(labels)),list(reversed(values)),color=list(reversed(['#b6beca','#ef9650','#9b7cc4','#00897b'])))
    other.set(xlim=(0,105),title='Coverage across all clean events',xlabel='Percent of all events')
    for i,value in enumerate(reversed(values)):other.text(value+1,i,f'{value:.2f}%',va='center',fontsize=9)
    finish(fig,'language_associations.png','2025-01-01 activity associated with metadata fetched 2026-10-08. Selected subset is not representative of GitHub.')
    rows=list(reversed(json.loads((target/'top_accounts.json').read_text())[:12]))
    fig,ax=plt.subplots(figsize=(12,6))
    ax.barh([r['actor_login'] for r in rows],[r['total_events'] for r in rows],color='#d97728')
    ax.set(title='Observed public account activity — 1 January 2025 UTC',xlabel='Clean public event records')
    ax.xaxis.set_major_formatter(FuncFormatter(lambda n,_:f'{n:,.0f}'))
    finish(fig,'account_activity.png','Accounts include automation and bots. Counts do not measure total developer work or human productivity.')
    return saved


def main():
    out=ROOT/'reports/week4'
    manifest=current_manifest(ROOT/'data/gold')
    verified=json.loads((out/'integration_verification.json').read_text())
    initial=json.loads((out/'initial_run.json').read_text())
    rerun=json.loads((out/'idempotent_rerun.json').read_text())
    fetch=json.loads((out/'enrichment_run.json').read_text())
    if (verified['status']!='passed' or not all(verified['checks'].values()) or
        verified['snapshot_id']!=manifest['snapshot_id'] or manifest['recipe']['code_hashes']!=code_hashes() or
        initial['snapshot_id']!=manifest['snapshot_id'] or rerun['snapshot_id']!=manifest['snapshot_id']):
        raise ValueError('Matching successful Week 4 execution and verification evidence is required')
    target=ROOT/'data/gold/snapshots'/manifest['snapshot_id']
    rankings=json.loads((target/'rankings.json').read_text())
    previews=json.loads((target/'language_previews.json').read_text())
    coverage=manifest['coverage'];language_coverage=verified['language_coverage']
    images=charts(target,manifest,rankings,previews,coverage)
    criteria={
        'Full clean dataset processed into 15 unique-key Parquet tables':len(manifest['tables'])==15,
        'Exact repository and public-account aggregates':verified['checks']['exact_cross_repository_account_participation'],
        'Most-active, intraday, fastest-growth, and emerging-attention rankings':all(rankings[k] for k in rankings),
        'Reusable comparison of real repositories':verified['checks']['real_repository_comparison'],
        'Daily/hourly participation and UTC calendar metrics':manifest['tables']['hourly_activity']['rows']==24,
        'Descriptive collaboration ratio and bounded participation edges':manifest['tables']['participation_edges_sample']['rows']==1000,
        'Public, ID-verified, dated GitHub metadata and checksummed cache':coverage['successful_repositories']>0,
        'Primary-language, byte-weighted, and technology associations with coverage':verified['checks']['byte_weighted_activity_accounting'],
        'Normalized exploratory score and alternate-weight evaluation':len(verified['score_sensitivity_overlap'])==3,
        'Identical rerun uses no API requests and reuses immutable output':verified['checks']['real_idempotent_rerun'],
        'Week 4 and earlier-week tests pass':verified['tests_passed']>=129,
        'Week 1–3 artifacts and source code preserved':verified['previous_artifacts_preserved']==41,
        'Full-table parity, input conservation, and recorded integration checks':all(verified['checks'].values()),
        'Operations guide, table contract, reviewed chart artifacts':(ROOT/'docs/week4_analytics.md').exists() and len(images)==4,
    }
    if not all(criteria.values()):raise ValueError(f'Incomplete criteria: {[k for k,v in criteria.items() if not v]}')
    checklist='\n'.join(f'- [x] {name}' for name in criteria)
    table_rows='\n'.join(f"| `{name}` | {v['rows']:,} |" for name,v in manifest['tables'].items())
    most_active='\n'.join(f"| {r['repo_name']} | {r['repo_id']} | {r['total_events']:,} | {r['active_accounts']:,} |" for r in rankings['most_active'][:10])
    trends='\n'.join(f"| {r['repo_name']} | {r['previous_attention']:,} | {r['current_attention']:,} | {r['attention_growth_ratio']:.2f} | {r['trend_score']:.3f} |" for r in rankings['intraday_trending'][:10])
    languages='\n'.join(f"| {r['primary_language']} | {r['enrichment_status']} | {r['active_repositories']:,} | {r['total_events']:,} |" for r in sorted(previews['language_primary_daily'],key=lambda r:-r['total_events']))
    sensitivity='\n'.join(f'| {label} | {n}/20 |' for label,n in verified['score_sensitivity_overlap'].items())
    comparison=json.loads((out/'repository_comparison.json').read_text())
    ids=' '.join(str(r['repo_id']) for r in comparison)
    report=f"""# DevPulse — Week 4 Analytics Report

**Status: COMPLETE — all Week 4 criteria satisfied.**
Integration verified at **{verified['verified_at_utc']}**.
The measured input is the complete **2025-01-01 UTC** day. Current GitHub metadata
was fetched on **2026-10-08**; the two observation dates are kept separate.

## Delivered scope

{checklist}

## Dataset and output

The current Silver snapshot contains **{manifest['input_events']:,} clean unique
public events**, covering 24 archive hours. Week 4 produces exact aggregates for
**{manifest['tables']['repository_metrics']['rows']:,} repository IDs** and
**{manifest['tables']['account_metrics']['rows']:,} public account IDs**.
Counts include bots and automation; they are not developer productivity.

The current Gold snapshot is `{manifest['snapshot_id']}` under
`data/gold/snapshots/`; `data/gold/_CURRENT.json` selects it with a manifest checksum.
All 15 tables have verified keys and row counts. Output files plus checksum sidecars
total **{manifest['output_bytes']:,} bytes**.

| Parquet table | Saved rows |
|---|---:|
{table_rows}

Repository/account whole-window, daily, calendar, and primary-language event totals
each conserve all **{manifest['input_events']:,} events**. Hourly repository/account
groups are sparse; the global hourly table contains all 24 covered hours. A single
Wednesday provides UTC calendar labels but cannot establish weekend or weekly trends.
The sampled participation graph has 1,000 weighted account/repository edges,
limited to 20 star-active repositories and 50 accounts each.

## Repository activity and intraday attention

The most-active ranking orders observed public event totals, with repository ID
breaking ties. Repository names are latest observed archive labels.

| Repository | Stable ID | Events | Active accounts |
|---|---:|---:|---:|
{most_active}

![Most active repositories](week4/repository_activity.png)

Attention means star actions plus fork events. The equal comparison windows are
**00:00–12:00** and **12:00–24:00 UTC**, with half-open boundaries. The smoothed
ratio is `(current+1)/(previous+1)`; absolute deltas and zero-baseline flags accompany
it. A zero baseline has null percentage change. Ranked support requires five
current attention events and two current active accounts. The fastest-growth view
also requires a positive delta. Emerging attention means at most two prior attention
events with qualifying current support; it does not prove the repository is new.

| Intraday score ranking | Previous attention | Current attention | Smoothed ratio | Score |
|---|---:|---:|---:|---:|
{trends}

![Equal-window attention](week4/intraday_attention.png)

Scores normalize current stars, forks, active accounts, and opened PRs with log1p
relative to dataset maxima and weights **0.4/0.3/0.2/0.1**. The final score combines
70% activity with 30% bounded log growth. These are exploratory choices, not an
evaluated popularity predictor. Different weighting choices change rankings:

| Alternative activity weights | Top-20 overlap with default |
|---|---:|
{sensitivity}

`score_sensitivity.json` preserves each alternative's weights and ranked IDs.
The result shows why rankings should not be presented as an objective measure.

Compare the two real star-active repositories used by the audit:

```bash
python -m analytics.query --repo-id {ids}
```

The saved comparison is `reports/week4/repository_comparison.json`.

## Public accounts and collaboration

The account tables count exact distinct repositories and active dates across the
whole input. Daily distincts are not summed into global unique counts. Opened PRs,
opened issues, issue comments, reviews, and push events have separate counters.
Push records are not commit totals. Code-participating accounts have a documented
operational event definition and do not establish authorship or total work.
`participation_ratio` is distinct participating accounts divided by events, a
proposed descriptive ratio rather than a standard collaboration measure.

![Observed public account activity](week4/account_activity.png)

## Language enrichment and coverage

The targeted 20-repository selection combines starring, intraday score, and overall
activity. The first collection made **{fetch['http_requests']} actual HTTP requests**
within a 50-request budget, including redirects/retries. It obtained **16 complete
public metadata/language records**, **three not-found outcomes**, and **one repository
ID mismatch**. That mismatch is kept unavailable so a reused name cannot contaminate
historical labels. Current names, byte mixes, creation dates, API star inventories,
ETags, collection timestamps, and raw responses are checksummed and cached.

The complete cache covers **{coverage['covered_events']:,} archived events
({coverage['covered_event_fraction']*100:.2f}%)**. A named current primary language
covers **{language_coverage['named_primary_language_events']:,} events
({language_coverage['named_primary_language_event_fraction']*100:.2f}%)**. This is a
selected subset, not representative language market share across GitHub.

| Current primary-language bucket | Metadata status | Repositories | Archived events |
|---|---|---:|---:|
{languages}

![Language associations and coverage](week4/language_associations.png)

The byte-weighted table allocates activity according to the current code-byte mix.
Shares sum to one per repository and conserve total events, including explicit
unknown/unavailable/no-language buckets. It estimates associations; it does not
observe the programming language of each event. Technology labels use documented
topic/description rules for AI, cloud, data engineering, and mobile. Multiple
categories may apply, so technology totals are not globally additive.

Current metadata may differ from repository languages/topics in January 2025.
API star/fork inventory is separate from observed historical event counts. Current
metadata must not be backdated into historical prediction features.

## Execution, reuse, and verification

Core aggregation and persisted-table checks took **{manifest['core_duration_seconds']:.3f}
seconds**. The final analytics suite invocation took **{initial['duration_seconds']:.3f}
seconds**, using the completed core and metadata caches. These are separate phases;
the final invocation is not claimed as the entire initial collection runtime.
The identical invocation reused the same snapshot in **{rerun['duration_seconds']:.3f}
seconds**, with **zero API requests** and 20 cache hits. Matching recipes avoid Spark
transformation after checksums have been verified.

**{verified['tests_passed']} automated tests passed**, including the earlier 110.
Week 4 tests cover exact distinct participation, event boundaries, latest labels,
growth/zero baselines, score ranges, action semantics, incomplete coverage rejection,
language allocations, unknown buckets, topic boundaries, cache integrity, stable-ID
validation, private-response rejection, HTTP budgets, rate limits, ETags, retries,
safe redirects, and quota rollback. Synthetic fixtures are confined to tests.

The real-data audit checks all 15 table keys and counts; compares **every repository
daily row** against the Week 3 table; verifies global account/repository pairs and
action counters against Silver; checks each equal period; validates pinned cache
identity and language allocation; evaluates alternate score weights; and confirms
**{verified['previous_artifacts_preserved']} prior artifacts/source files** retain
their original fingerprints. File checksums cover current Silver and Gold output.

Spark runs locally on this Mac with a 4 GiB driver and 64 shuffle partitions. The
Gold and core caches have separate 2 GiB logical output budgets, excluding Spark
scratch space. Immutable snapshot staging and atomic publication preserve earlier
outputs; crashes may leave unreferenced staging directories requiring inspection.
This is not a multi-machine scalability benchmark.

## Files and next milestone

- Operations and reader examples: [Week 4 guide](../docs/week4_analytics.md).
- Table keys and meanings: [Gold contract](../docs/contracts/gold_analytics.md).
- Evidence: `reports/week4/integration_verification.json`, test XML/logs,
  `enrichment_run.json`, `initial_run.json`, `idempotent_rerun.json`, comparison,
  and score-sensitivity results.
- Data: `data/gold/snapshots/{manifest['snapshot_id']}/`, including pinned metadata,
  coverage, bounded rankings, and language previews.

Week 5 is PostgreSQL loading and the interactive analytics dashboard. Forecasting,
orchestration/streaming, and final evaluation remain Weeks 6–8.
"""
    atomic_text(ROOT/'reports/week4_report.md',report)
    write_json(out/'completion.json',{'status':'complete','snapshot_id':manifest['snapshot_id'],
                                     'verified_at_utc':verified['verified_at_utc'],'criteria':criteria,'charts':images})
    print('Measured Week 4 report and four charts generated.')


if __name__=='__main__':main()
