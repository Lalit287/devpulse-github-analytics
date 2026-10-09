"""Operate the complete project-owned native local deployment."""
import argparse,json,time
from config.settings import ROOT
from database.cluster import connect
from exploration.io import write_json
from deployment.spark_cluster import Cluster


def database_health():
    try:
        with connect('reader') as c:
            version=c.execute('SHOW server_version').fetchone()[0]
            count=c.execute('SELECT sum(total_events) FROM devpulse.repository_metrics').fetchone()[0]
            return {'status':'healthy','server_version':version,'clean_events':int(count),'port':15432}
    except Exception as e:return {'status':'stopped','error_class':type(e).__name__}

def status():
    from streaming.broker import status as kafka
    from orchestration.runtime import status as airflow
    from dashboard.run import status as dashboard
    from storage.hdfs_cluster import health
    try:
        h=health();hdfs={'status':'healthy' if h['live_datanodes']==1 else 'degraded',**h}
    except Exception as e:hdfs={'status':'stopped','error_class':type(e).__name__}
    af=airflow()
    healthy=af.get('metadatabase',{}).get('status')=='healthy' and af.get('scheduler',{}).get('status')=='healthy' and af.get('dag_processor',{}).get('status')=='healthy'
    return {'database':database_health(),'hdfs':hdfs,'kafka':kafka(),'airflow':{'status':'healthy' if healthy else 'stopped','components':af},'dashboard':dashboard(),'spark':Cluster().status()}

def start():
    from database.cluster import start as db
    from storage.hdfs_cluster import start_cluster
    from streaming.broker import start as kafka
    from orchestration.runtime import start as airflow
    from dashboard.run import start as dashboard
    # Normal Terminal is needed for PostgreSQL's macOS shared memory and process ownership checks.
    if database_health()['status']!='healthy':db()
    try:
        from storage.hdfs_cluster import health
        health()
    except Exception:start_cluster(evidence_path=ROOT/'reports/week8/hdfs_environment.json')
    kafka();airflow();dashboard();Cluster().start(2)
    result=status()
    if any(v['status']!='healthy' for v in result.values()):raise RuntimeError('One or more local deployment services are not healthy')
    write_json(ROOT/'reports/week8/deployment_status.json',{'status':'passed','services':result})
    return result

def stop():
    from dashboard.run import stop as dashboard
    from orchestration.runtime import stop as airflow
    from streaming.broker import stop as kafka
    from storage.hdfs_cluster import stop_cluster
    from database.cluster import stop as db
    Cluster().stop();dashboard();airflow();kafka();stop_cluster();db()
    return {'status':'stop_requested','note':'Only project-owned services are managed; data is retained'}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['start','status','stop']);a=p.parse_args();print(json.dumps(globals()[a.action](),indent=2,default=str))
