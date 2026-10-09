"""Real-history Spark ML comparison, chronological evaluation, immutable model artifacts."""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import time
import uuid

from pyspark import StorageLevel
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier,GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window,functions as F

from config.settings import ROOT
from exploration.io import sha256_file,write_json
from ingestion.locking import directory_lock
from spark.snapshots import verify_snapshot,switch_current
from analytics.artifacts import finish_snapshot,identity,write_table
from modeling.corpus import BASE,DAYS,START
from modeling.features import FEATURES,weekly,examples,target_threshold,label_examples,audit_splits
from modeling.evaluation import choose_threshold,metrics,diagnostics
from modeling.session import create_spark

OUTPUT=ROOT/'data/models'
PARAMETERS={
    'logistic_regression':{'regParam':.1,'maxIter':50},
    'random_forest':{'numTrees':40,'maxDepth':6,'minInstancesPerNode':20,'seed':42},
    'gradient_boosted_trees':{'maxIter':30,'maxDepth':4,'minInstancesPerNode':20,'stepSize':.1,'seed':42}}


def code_hashes():
    paths=list((ROOT/'modeling').glob('*.py'))+list((ROOT/'modeling').glob('*.sql'))
    return {str(p.relative_to(ROOT)):sha256_file(p) for p in sorted(paths)}


def verify_corpus():
    corpus=json.loads((BASE/'manifest.json').read_text())
    if corpus['status']!='complete' or corpus['hours']!=DAYS*24 or len(corpus['sources'])!=DAYS*24:
        raise ValueError('Complete historical archive coverage required')
    expected={f'{START+timedelta(days=i)}-{hour}.json.gz' for i in range(DAYS) for hour in range(24)}
    if {s['filename'] for s in corpus['sources']}!=expected:
        raise ValueError('Historical archive coverage is not the exact consecutive 42-day plan')
    for source in corpus['sources']:
        raw=BASE/'raw'/source['filename'].rsplit('-',1)[0]/source['filename']
        if sha256_file(raw)!=source['source_sha256'] or sha256_file(BASE/source['projected_path'])!=source['parquet_sha256']:
            raise ValueError('Historical source or projected checksum mismatch')
        if source['code_sha256']!=sha256_file(Path(__file__).with_name('corpus.py')):
            raise ValueError('Historical projection recipe is stale')
    return corpus


def score(model,frame):
    return model.transform(frame).withColumn('score',vector_to_array('probability')[1])


def run():
    OUTPUT.mkdir(parents=True,exist_ok=True)
    (OUTPUT/'snapshots').mkdir(parents=True,exist_ok=True)
    with directory_lock(OUTPUT/'.model.lock'):
        corpus=verify_corpus()
        recipe={'version':1,'corpus_id':corpus['corpus_id'],'corpus_source_inventory_id':identity({'sources':corpus['sources']}),
                'code_hashes':code_hashes(),'parameters':PARAMETERS,'features':FEATURES,
                'target':'future_stars >= 5 AND future_stars - past_stars >= train-only positive-growth 90th percentile',
                'split_weeks':{'train':[0,1,2],'validation':[3],'test':[4],'forecast':[5]},
                'minimum_past_events':5,'horizon_days':7,'seed':42,'spark_version':'4.0.1'}
        identifier=identity(recipe);existing=OUTPUT/'snapshots'/identifier
        if existing.exists():
            manifest=verify_snapshot(existing);switch_current(OUTPUT,identifier)
            print(f'Reused verified model snapshot {identifier}',flush=True)
            return manifest
        stage=OUTPUT/'.staging'/uuid.uuid4().hex;stage.mkdir(parents=True)
        started=time.monotonic();spark=create_spark()
        try:
            raw=spark.read.parquet(*[str(BASE/s['projected_path']) for s in corpus['sources']])
            if raw.count()!=corpus['projected_rows']:raise ValueError('Historical projected row mismatch')
            order=Window.partitionBy('event_id').orderBy('source_archive_start','source_filename','source_row','raw_sha256')
            clean=raw.withColumn('_rank',F.row_number().over(order)).filter('_rank=1').drop('_rank').persist(StorageLevel.DISK_ONLY)
            clean_rows=clean.count();print(f'Deduplicated history: {clean_rows:,} unique events',flush=True)
            duplicate_conflicts=raw.groupBy('event_id').agg(F.countDistinct('raw_sha256').alias('variants')).filter('variants>1').count()
            weeks=weekly(clean).persist(StorageLevel.MEMORY_AND_DISK)
            weeks_count=weeks.count();accounted_events=int(weeks.agg(F.sum('total_events_7d')).first()[0])
            print(f'Weekly historical aggregates: {weeks_count:,} repository-period rows',flush=True)
            unlabelled=examples(weeks).persist(StorageLevel.MEMORY_AND_DISK)
            threshold=target_threshold(unlabelled)
            frame=label_examples(unlabelled,threshold).persist(StorageLevel.MEMORY_AND_DISK)
            splits=audit_splits(frame)
            print(f'Train-only target growth threshold: {threshold}; splits: {json.dumps(splits)}',flush=True)
            write_table(weeks,stage/'weekly')
            write_table(frame,stage/'examples')
            train=frame.filter("split='train'");validation=frame.filter("split='validation'");test=frame.filter("split='test'");forecast=frame.filter("split='forecast'")
            classifiers={'logistic_regression':LogisticRegression(**PARAMETERS['logistic_regression']),
                         'random_forest':RandomForestClassifier(**PARAMETERS['random_forest']),
                         'gradient_boosted_trees':GBTClassifier(**PARAMETERS['gradient_boosted_trees'])}
            results={};fitted={};decision_thresholds={}
            for name,classifier in classifiers.items():
                print(f'Fitting {name} on chronological training only...',flush=True)
                left=time.monotonic()
                model=Pipeline(stages=[VectorAssembler(inputCols=FEATURES,outputCol='features',handleInvalid='error'),classifier]).fit(train)
                fitting=round(time.monotonic()-left,3)
                val=score(model,validation).persist(StorageLevel.MEMORY_AND_DISK)
                decision=choose_threshold(val);decision_thresholds[name]=decision
                results[name]={'validation':metrics(val,decision),'fit_seconds':fitting}
                last=model.stages[-1]
                values=last.coefficients.toArray() if name=='logistic_regression' else last.featureImportances.toArray()
                results[name]['feature_effects']={feature:float(value) for feature,value in zip(FEATURES,values)}
                model.write().save(str(stage/'models'/name));fitted[name]=model;val.unpersist()
                print(f'{name}: validation PR-AUC {results[name]["validation"]["pr_auc"]:.4f}',flush=True)
            chosen=max(results,key=lambda n:(results[n]['validation']['pr_auc'],results[n]['validation']['precision_at_10'],n))
            print(f'Frozen validation selection: {chosen}. Evaluating later test period...',flush=True)
            for name,model in fitted.items():
                scored=score(model,test).persist(StorageLevel.MEMORY_AND_DISK)
                results[name]['test']=metrics(scored,decision_thresholds[name])
                if name==chosen:
                    heldout=scored
                    results[name]['test_diagnostics']=diagnostics(scored)
                else:scored.unpersist()
            baseline={}
            for split,data in [('validation',validation),('test',test)]:
                ranked=data.withColumn('score',F.col('star_events_7d')/(F.col('star_events_7d')+1))
                baseline[split]=metrics(ranked,.5,probability=False)
            restored=PipelineModel.load(str(stage/'models'/chosen))
            sample=test.orderBy('repo_id').limit(100)
            original={int(r['repo_id']):r['score'] for r in score(fitted[chosen],sample).select('repo_id','score').collect()}
            reloaded={int(r['repo_id']):r['score'] for r in score(restored,sample).select('repo_id','score').collect()}
            if any(abs(original[k]-reloaded[k])>1e-12 for k in original):raise ValueError('Saved model reload changes predictions')
            predictions=score(restored,forecast).persist(StorageLevel.MEMORY_AND_DISK)
            forecast_rows=predictions.count()
            if predictions.filter(F.isnan('score') | (F.col('score')<0) | (F.col('score')>1)).limit(1).count():
                raise ValueError('Invalid model scores')
            columns=['repo_id','repo_name','as_of_utc','label_end_utc','star_events_7d','fork_events_7d',
                     'push_events_7d','total_events_7d','active_accounts_7d','future_star_events_7d','star_delta_7d','label','score']
            write_table(heldout.select(*columns),stage/'heldout_predictions')
            write_table(predictions.select(*columns),stage/'forecast_predictions')
            manifest={'recipe':recipe,'completed_at_utc':datetime.now(timezone.utc).isoformat(),
                      'duration_seconds':round(time.monotonic()-started,3),'corpus':{k:v for k,v in corpus.items() if k!='sources'},
                      'clean_events':clean_rows,'duplicate_rows':corpus['projected_rows']-clean_rows,
                      'conflicting_duplicate_ids':duplicate_conflicts,'window_accounted_events':accounted_events,
                      'outside_source_week_events':clean_rows-accounted_events,'weekly_rows':weeks_count,
                      'target_growth_threshold':threshold,'splits':splits,'models':results,
                      'selected_model':chosen,'decision_threshold':decision_thresholds[chosen],
                      'baseline':baseline,'forecast_rows':forecast_rows,'heldout_rows':splits['test']['rows'],
                      'checks':{'source_coverage':'passed','source_hashes':'passed','chronological_splits':'passed',
                                'feature_cutoffs':'passed','unknown_forecast_labels':'passed','model_reload_parity':'passed'}}
            target=finish_snapshot(stage,OUTPUT,identifier,manifest,max_bytes=3*1024**3)
            print(f'Completed historical model snapshot {identifier}: {target}',flush=True)
            return verify_snapshot(target)
        finally:spark.stop()


if __name__=='__main__':run()
