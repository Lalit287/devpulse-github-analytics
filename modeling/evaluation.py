"""Chronological metrics and validation-only model/decision-threshold selection."""
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

THRESHOLDS=[.005,.01,.025,.05,.1,.2,.3,.5,.7]


def confusion(frame,threshold):
    row=frame.agg(
        F.sum(F.when((F.col('score')>=threshold)&(F.col('label')==1),1).otherwise(0)).alias('tp'),
        F.sum(F.when((F.col('score')>=threshold)&(F.col('label')==0),1).otherwise(0)).alias('fp'),
        F.sum(F.when((F.col('score')<threshold)&(F.col('label')==1),1).otherwise(0)).alias('fn'),
        F.sum(F.when((F.col('score')<threshold)&(F.col('label')==0),1).otherwise(0)).alias('tn')).first().asDict()
    precision=row['tp']/(row['tp']+row['fp']) if row['tp']+row['fp'] else 0.
    recall=row['tp']/(row['tp']+row['fn']) if row['tp']+row['fn'] else 0.
    return {**row,'precision':precision,'recall':recall,'f1':2*precision*recall/(precision+recall) if precision+recall else 0.,'decision_threshold':threshold}


def choose_threshold(validation):
    return max((confusion(validation,t) for t in THRESHOLDS),key=lambda m:(m['f1'],m['precision'],m['decision_threshold']))['decision_threshold']


def metrics(frame,threshold,probability=True):
    count=frame.count();positive=int(frame.agg(F.sum('label')).first()[0])
    if not 0<positive<count:raise ValueError('Ranking metrics require both classes')
    result={'rows':count,'positives':positive,'prevalence':positive/count,**confusion(frame,threshold)}
    for name in ['areaUnderROC','areaUnderPR']:
        result['roc_auc' if name=='areaUnderROC' else 'pr_auc']=BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='score',metricName=name,numBins=0).evaluate(frame)
    for k in [10,100]:
        top=frame.orderBy(F.col('score').desc(),'repo_id','as_of_utc').limit(k)
        n=top.count();result[f'precision_at_{k}']=float(top.agg(F.avg('label')).first()[0]) if n else 0.
    if probability:result['brier_score']=float(frame.agg(F.avg(F.pow(F.col('score')-F.col('label'),2))).first()[0])
    return result


def diagnostics(frame):
    buckets=(frame.withColumn('bin',F.least(F.floor(F.col('score')*10).cast('int'),F.lit(9)))
             .groupBy('bin').agg(F.count('*').alias('rows'),F.avg('score').alias('mean_score'),F.avg('label').alias('observed_positive_fraction'))
             .orderBy('bin').collect())
    # Bounded threshold curves support plots without collecting all predictions.
    curve=[confusion(frame,t) for t in THRESHOLDS]
    return {'reliability_bins':[r.asDict() for r in buckets],'threshold_curve':curve}
