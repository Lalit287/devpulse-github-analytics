import pytest
from modeling.evaluation import confusion,choose_threshold,metrics
from modeling.session import create_spark


@pytest.fixture(scope='module')
def spark():
    session=create_spark();yield session;session.stop()


def test_hand_calculated_metrics_and_threshold_selection(spark):
    frame=spark.createDataFrame([(1,.9,1.),(2,.7,0.),(3,.4,1.),(4,.1,0.)],['repo_id','score','label']).withColumn('as_of_utc',__import__('pyspark').sql.functions.lit('2015-02-05'))
    c=confusion(frame,.5)
    assert (c['tp'],c['fp'],c['fn'],c['tn'])==(1,1,1,1)
    assert c['precision']==c['recall']==c['f1']==.5
    assert choose_threshold(frame)==.3
    m=metrics(frame,.5)
    assert m['roc_auc']==pytest.approx(.75)
    assert m['brier_score']==pytest.approx((.01+.49+.36+.01)/4)
    assert m['precision_at_10']==.5


def test_evaluation_refuses_one_class_data(spark):
    frame=spark.createDataFrame([(1,.9,1.)],['repo_id','score','label'])
    with pytest.raises(ValueError,match='both classes'):metrics(frame,.5)
