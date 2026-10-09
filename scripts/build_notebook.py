"""Create the Week 1 teaching notebook; execution is a separate verified step."""
from textwrap import dedent

import nbformat as nbf

from config.settings import ROOT


def build_notebook():
    cells = []

    def markdown(text):
        cells.append(nbf.v4.new_markdown_cell(dedent(text).strip()))

    def code(text):
        cells.append(nbf.v4.new_code_cell(dedent(text).strip()))

    markdown("""
    # DevPulse — Week 1: GH Archive Exploration
    ## 1. Project Introduction
    Explore historical public GitHub activity using a bounded real-data sample.
    This notebook establishes the data contract for later distributed engineering.
    All numbers below describe the selected input and sample, not all GitHub activity.
    """)
    markdown("""
    ## 2. GH Archive Dataset Overview
    [GH Archive](https://www.gharchive.org/) stores hourly compressed newline-delimited
    JSON events from GitHub's public event stream. Archive hours are UTC and filenames
    use an unpadded hour. Nested `payload` properties depend on the event type.
    The sample metadata records the source URL, checksum, seed, and scanned record count.
    """)
    markdown("""
    ## 3. Environment Setup and Imports
    Run with this project's virtual-environment kernel. Paths work from the repository
    root or its `notebooks/` directory. Java is configured locally when Spark starts.
    """)
    code("""
    from pathlib import Path
    import json
    import sys
    from IPython.display import display, Markdown, Image
    import pandas as pd

    ROOT = next((p for p in [Path.cwd(), *Path.cwd().parents]
                 if (p / 'config/settings.py').exists() and (p / 'notebooks').is_dir()), None)
    if ROOT is None:
        raise RuntimeError('Launch from the DevPulse project or its notebooks directory')
    sys.path.insert(0, str(ROOT))
    from config.settings import SAMPLE_PATH, PROCESSED_DIR, CHART_DIR
    from exploration.io import iter_events
    from exploration.explore_dataset import analyze_records, run_analysis
    from exploration.schema_inspection import extract_schema
    from exploration.spark_demo import run_spark_demo
    from scripts.environment_info import collect_environment
    environment = collect_environment()
    display(pd.Series({k: environment[k] for k in ['os', 'architecture', 'python', 'virtual_environment', 'java_version']}))
    display(pd.Series(environment['packages'], name='Installed version'))
    """)
    markdown("""
    ## 4. Loading the Dataset
    First run the downloader and schema-inspection commands in the README. The default
    10,000-event reservoir sample is selected uniformly from the entire downloaded hour
    using seed 42, rather than taking only the beginning of the file. Nested JSON is preserved.
    """)
    code("""
    if not SAMPLE_PATH.exists():
        raise FileNotFoundError('Generate data/samples/github_events_sample.jsonl using schema_inspection first')
    metadata = json.loads(SAMPLE_PATH.with_suffix('.metadata.json').read_text())
    records = []
    for event in iter_events(SAMPLE_PATH):
        records.append(event)
        if len(records) > 100000:
            raise ValueError('Notebook sample exceeds its 100,000-record memory cap')
    frame, summary, tables = analyze_records(records)
    display(pd.Series({k: metadata[k] for k in ['scope', 'data_origin', 'sampling_method', 'seed', 'sample_records', 'valid_source_records']}))
    display(pd.DataFrame(metadata['sources']))
    """)
    markdown("""
    ## 5. Exploring the JSON Structure
    Common properties identify the event, account, repository, time, visibility, and
    payload. Optional `org` and payload-specific paths vary. A field absent for unrelated
    event types is not automatically a quality defect. Array paths use `[]`.
    """)
    code("""
    schema = extract_schema(records)
    print(json.dumps(records[0], indent=2)[:8000])
    display(pd.DataFrame(schema['fields']).T.head(30))
    display(pd.DataFrame(schema['important_fields']).T)
    """)
    markdown("""
    ## 6. Understanding GitHub Event Types
    | Event | Meaning and useful payload examples |
    |---|---|
    | PushEvent | A push; `ref`, `head`, `before`, and historically `commits`/`size` may appear |
    | WatchEvent | Starring action, commonly `action=started` |
    | ForkEvent | A fork; `forkee` may contain repository metadata |
    | IssuesEvent | Issue lifecycle; `action`, `issue` |
    | PullRequestEvent | Pull request lifecycle; `action`, `number`, `pull_request` |
    | CreateEvent | Repository/branch/tag creation; `ref_type`, `ref` |
    | IssueCommentEvent | Comment lifecycle; `action`, `issue`, `comment` |
    | PullRequestReviewEvent | Review lifecycle; `action`, `review`, `pull_request` |

    Payload examples are optional, historical, and schema-dependent. Consult the actual
    schema output and [GitHub's event documentation](https://docs.github.com/en/rest/using-the-rest-api/github-event-types).
    Counts measure observed events, not commits, currently accumulated stars, or unique actions after deduplication.
    """)
    code("""
    display(tables['event_types'].sort_values('events', ascending=False))
    for event_type in ['PushEvent', 'WatchEvent', 'ForkEvent', 'IssuesEvent',
                       'PullRequestEvent', 'CreateEvent', 'IssueCommentEvent', 'PullRequestReviewEvent']:
        example = next((r for r in records if r.get('type') == event_type), None)
        print(event_type, ':', list(example.get('payload', {})) if example else 'Not observed in this sample')
    """)
    markdown("""
    ## 7. Data Quality Analysis
    Missing values are reported separately as absent, null, and blank. Duplicate metrics
    distinguish duplicated identifiers from excess records. Rankings retain raw event
    counts; this Week 1 step diagnoses duplicates rather than silently deleting records.
    Invalid timestamps are excluded from time-based aggregates and counted explicitly.
    """)
    code("""
    display(tables['missing_values'])
    display(pd.Series({k: summary[k] for k in ['duplicate_event_ids', 'duplicate_excess_records',
                    'records_with_duplicated_ids', 'invalid_or_missing_timestamps']}))
    display(tables['duplicate_ids'].head(10))
    """)
    markdown("""
    ## 8. Repository Activity Analysis
    Repositories are grouped by ID, so an observed rename does not split the count.
    The latest observed nonnull name labels each group. High event volume measures activity
    in this sample and does not establish that a repository is trending.
    """)
    code("""
    display(pd.Series({'Unique repository IDs': summary['unique_repositories_by_id']}))
    display(tables['repositories'].head(10))
    """)
    markdown("""
    ## 9. Developer Activity Analysis
    Public account activity includes automation and bot accounts. Event counts cannot
    measure a person's productivity. Stable actor IDs define distinct accounts; login
    names can change. The table includes only events with `public=true`.
    """)
    code("""
    display(pd.Series({'Unique account IDs': summary['unique_accounts_by_id'],
                      'Unique public account IDs': summary['unique_public_accounts_by_id']}))
    display(tables['public_accounts'].head(10))
    display(tables['utc_hour_distribution'])
    """)
    markdown("""
    ## 10. Data Visualizations
    Recompute the CSV/JSON outputs and save seven PNGs. The time chart uses one-minute
    bins only within the observed sample interval. Hour-of-day bars cover only observed
    UTC hours. A single archive provides no evidence for a 24-hour activity trend.
    """)
    code("""
    frame, summary, tables = run_analysis(SAMPLE_PATH)
    for filename in summary['charts']:
        display(Image(filename=str(CHART_DIR / filename), width=800))
    """)
    markdown("""
    ## 11. Introduction to PySpark DataFrames
    Pandas is convenient for this small sample in one process. Spark builds an execution
    plan and partitions work; local mode demonstrates its API and has startup overhead.
    These runs do not establish a speedup or demonstrate a multi-machine cluster.
    The helper starts `local[2]`, checks a Python-created DataFrame, reads nested JSON,
    prints the inferred schema, selects fields, aggregates, and always stops the session.
    """)
    code("""
    spark_result = run_spark_demo(SAMPLE_PATH)
    assert spark_result['records'] == summary['records_analyzed']
    assert spark_result['event_type_counts'] == summary['event_type_counts']
    assert spark_result['unique_repositories'] == summary['unique_repositories_by_id']
    assert spark_result['unique_accounts'] == summary['unique_accounts_by_id']
    print('Pandas / Spark parity checks passed.')
    print((PROCESSED_DIR / 'spark_schema.txt').read_text())
    display(pd.Series({k: v for k, v in spark_result.items() if k not in ['transformed_examples', 'event_type_counts']}))
    """)
    markdown("""
    ## 12. Initial Findings and Observations
    These observations are generated from the measured sample. The archive's physical
    record count is recorded separately from the sample's analyzed record count.
    """)
    code("""
    most_common = tables['event_types'].sort_values('events', ascending=False).iloc[0]
    display(Markdown(f"Analyzed **{summary['records_analyzed']:,} sampled records** from "
        f"**{metadata['valid_source_records']:,} valid scanned source records**. "
        f"Observed **{summary['unique_repositories_by_id']:,} repository IDs** and "
        f"**{summary['unique_accounts_by_id']:,} account IDs**. "
        f"The most frequent sampled type is **{most_common['event_type']}** "
        f"with **{int(most_common['events']):,} events**. "
        f"There are **{summary['duplicate_excess_records']} excess duplicated-ID records**. "
        f"UTC timestamp range: {summary['timestamp_min_utc']} to {summary['timestamp_max_utc']}."))
    """)
    markdown("""
    ## 13. Dataset Limitations
    - Historical public events exclude private activity and are not a complete productivity record.
    - WatchEvent measures observed starring actions, not current cumulative star counts.
    - PushEvent is a push event, not a commit count. Payload details can change over time.
    - Language metadata is not reliably present across events. Later enrichment must be cached
      and timestamped; no language popularity estimates are invented here.
    - Sampling introduces uncertainty, and one holiday hour is not representative of other times.
    - API latency and collection timing can make event timestamps differ from archive-hour boundaries.
    - Accounts include bots; counts include duplicate records if present. Rare types may be missed.
    """)
    markdown("""
    ## 14. Conclusion and Next Steps
    Week 1 establishes reproducible bounded collection, schema discovery, quality checks,
    Pandas EDA, charts, and local Spark verification. Next, expand controlled ingestion
    and design storage/HDFS in Week 2. Distributed ETL, language enrichment, modeling,
    streaming, and dashboards follow in later roadmap stages.
    """)
    notebook = nbf.v4.new_notebook(cells=cells, metadata={
        "kernelspec": {"display_name": "DevPulse (.venv)", "language": "python", "name": "devpulse"},
        "language_info": {"name": "python", "version": "3.12"}})
    nbf.write(notebook, ROOT / "notebooks/01_gharchive_exploration.ipynb")


if __name__ == "__main__":
    build_notebook()
