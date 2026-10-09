"""Standalone measured performance charts; no invented or projected points."""
import json,os
from config.settings import ROOT
cache=ROOT/".runtime/week8-plot-cache"
cache.mkdir(parents=True,exist_ok=True)
os.environ["MPLCONFIGDIR"]=str(cache)
os.environ["XDG_CACHE_HOME"]=str(cache)
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from config.settings import ROOT

def run():
    evidence=ROOT/'reports/week8';compute=json.loads((evidence/'compute_benchmark.json').read_text());storage=json.loads((evidence/'storage_benchmark.json').read_text())
    names={'pandas':'Pandas','local4':'Spark local 4 cores','1-workers':'1 worker / 2 cores','2-workers':'2 workers / 4 cores','local4-shuffle200':'Spark local / shuffle 200'}
    order=['pandas','local4','1-workers','2-workers','local4-shuffle200'];summaries=compute['summaries'];med=[summaries[k]['median_seconds'] for k in order]
    fig,ax=plt.subplots(figsize=(10,5));ax.barh([names[k] for k in order],med,color=['#14b8a6','#2563eb','#6366f1','#818cf8','#94a3b8'])
    for i,x in enumerate(med):ax.text(x+.04,i,f'{x:.3f} s',va='center',fontsize=10)
    ax.set_xlim(0,max(med)*1.2);ax.invert_yaxis();ax.set_xlabel('Median read + aggregation + materialization time (seconds)')
    ax.set_title('Same 3,909,986-event repository-count query\nThree trials · warm filesystem caches · one Mac · startup excluded')
    ax.spines[['top','right']].set_visible(False);fig.tight_layout();fig.savefig(evidence/'compute_performance.png',dpi=160);plt.close(fig)
    formats=list(storage['formats']);size=[storage['formats'][k]['bytes']/1024**2 for k in formats];seconds=[storage['formats'][k]['median_seconds'] for k in formats]
    fig,axes=plt.subplots(1,2,figsize=(12,5));labels=['JSON','Gzip JSON','Flat Parquet','Hourly Parquet']
    axes[0].bar(labels,size,color='#2563eb');axes[0].set_ylabel('Equivalent projection size (MiB)')
    axes[1].bar(labels,seconds,color='#14b8a6');axes[1].set_ylabel('Median hour-filter query time (seconds)');axes[1].set_yscale('log')
    for ax in axes:ax.tick_params(axis='x',rotation=20);ax.spines[['top','right']].set_visible(False)
    fig.suptitle('Equal five-column projection of the real day\nPyArrow query: UTC hour 12 · 223,540 events · warm caches · three trials');fig.tight_layout();fig.savefig(evidence/'storage_performance.png',dpi=160);plt.close(fig)

if __name__=='__main__':run()
