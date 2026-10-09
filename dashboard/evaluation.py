"""Measured final evaluation, kept separate from current activity scope."""
import json
from pathlib import Path
import pandas as pd
import streamlit as st
from config.settings import ROOT


def render():
    evidence=ROOT/'reports/week8';path=evidence/'compute_benchmark.json'
    if not path.exists():return
    compute=json.loads(path.read_text())
    if compute.get('status')!='passed':return
    st.divider();st.subheader('Project evaluation')
    status_path=evidence/'verification.json'
    status=json.loads(status_path.read_text()) if status_path.exists() else {}
    if status.get('status')=='complete':st.success('Final project verification passed.')
    else:st.info('Measured benchmarks are available. Final deployment verification is pending.')
    st.caption(f"Fixed historical benchmark: January 1, 2025 · {compute['exact_result']['input_events']:,} clean events · one Mac · three trials per configuration · filesystem caches warmed by source verification.")
    names={'pandas':'Pandas','local4':'Spark local: 4 cores','1-workers':'Spark: 1 worker, 2 cores','2-workers':'Spark: 2 workers, 4 cores','local4-shuffle200':'Spark local: 200 shuffle partitions','local4-shuffle64':'Spark local: 64 shuffle partitions'}
    table=pd.DataFrame([{'Configuration':names.get(k,k),'Median query seconds':round(v['median_seconds'],3),'Minimum':round(v['min_seconds'],3),'Maximum':round(v['max_seconds'],3)} for k,v in compute['summaries'].items()])
    st.dataframe(table,hide_index=True,width='stretch')
    st.caption('Each query reads the same narrow Parquet columns and produces the same exact repository counts. Spark startup is recorded separately. Worker processes share this Mac; these are not measurements across multiple machines.')
    storage_path=evidence/'storage_benchmark.json'
    if storage_path.exists():
        storage=json.loads(storage_path.read_text());formats={'json':'JSON lines','gzip_json':'Gzip JSON lines','flat':'Flat Parquet','partitioned':'Hourly partitioned Parquet'}
        st.subheader('Equivalent storage projection')
        st.dataframe(pd.DataFrame([{'Format':formats[k],'Size MiB':round(v['bytes']/1024**2,2),'Median read/query seconds':round(v['median_seconds'],4),'Candidate files':v['candidate_fragments']} for k,v in storage['formats'].items()]),hide_index=True,width='stretch')
        st.caption('All formats contain the same five benchmark fields. The query selects UTC hour 12. These sizes do not represent full nested raw events; Parquet statistics and partition pruning both help this cached query.')
    report=ROOT/'reports/project_report.md'
    if report.exists():st.download_button('Download project report',report.read_text(),file_name='DevPulse_project_report.md',mime='text/markdown',key='final_project_report')
