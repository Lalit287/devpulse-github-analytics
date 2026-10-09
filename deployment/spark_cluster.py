"""Project-owned Spark standalone master and bounded worker JVMs on loopback."""
import argparse,json,os,signal,socket,subprocess,time
from pathlib import Path
import pyspark,requests
from config.settings import ROOT
from exploration.spark_demo import configure_java
from exploration.io import write_json

class Cluster:
    def __init__(self,profile='deployment',worker_memory=None):
        if profile not in ['deployment','benchmark']:raise ValueError('Unknown Spark cluster profile')
        self.worker_memory=worker_memory or ('4g' if profile=='deployment' else '2g')
        if self.worker_memory not in ['2g','4g']:raise ValueError('Worker admission must be 2 or 4 GiB')
        self.profile=profile;self.root=ROOT/f'.runtime/spark-{profile}'
        self.port=17077 if profile=='deployment' else 17087
        self.ui=18081 if profile=='deployment' else 18087
        self.url=f'spark://127.0.0.1:{self.port}';self.children={}
    def environment(self):
        configure_java();self.root.mkdir(parents=True,exist_ok=True);self.root.chmod(0o700)
        conf=self.root/'conf';conf.mkdir(exist_ok=True)
        (conf/'spark-defaults.conf').write_text('spark.master.rest.enabled false\nspark.master.ui.decommission.allow.mode DENY\nspark.worker.cleanup.enabled true\nspark.worker.cleanup.interval 600\nspark.worker.cleanup.appDataTtl 86400\nspark.deploy.defaultCores 4\n')
        return {**os.environ,'SPARK_HOME':str(Path(pyspark.__file__).parent),'SPARK_CONF_DIR':str(conf),
            'SPARK_LOCAL_IP':'127.0.0.1','SPARK_PUBLIC_DNS':'127.0.0.1','SPARK_DAEMON_MEMORY':'256m',
            'SPARK_LOCAL_DIRS':str(self.root/'scratch'),'PYSPARK_PYTHON':str(ROOT/'.venv/bin/python')}
    def status(self):
        try:
            r=requests.get(f'http://127.0.0.1:{self.ui}/json/',timeout=3);r.raise_for_status();j=r.json()
            marker=json.loads((self.root/'identity.json').read_text())
            if marker['root']!=str(self.root) or j['url']!=self.url:raise ValueError('Spark service identity mismatch')
            workers=[w for w in j['workers'] if w['state']=='ALIVE']
            if any(w['host']!='127.0.0.1' for w in workers):raise ValueError('Unexpected remote worker')
            return {'status':'healthy','master':self.url,'ui':f'http://127.0.0.1:{self.ui}',
                    'workers':workers,'active_applications':j.get('activeapps',[])}
        except Exception as e:return {'status':'stopped','error_class':type(e).__name__}
    def launch(self,name,args):
        env=self.environment();home=Path(env['SPARK_HOME']);log=self.root/f'{name}.log'
        with log.open('ab') as out:
            p=subprocess.Popen([str(home/'bin/spark-class'),*args],cwd=ROOT,env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
        self.children[name]=p
        processes={name:child.pid for name,child in self.children.items()}
        # Preserve existing managed PIDs when adding a worker to a running deployment.
        marker=self.root/'processes.json'
        if marker.exists():processes={**json.loads(marker.read_text()),**processes}
        write_json(marker,processes)
    def start(self,workers=2):
        if type(workers)!=int or workers not in [1,2]:raise ValueError('Choose one or two bounded workers')
        state=self.status()
        if state['status']=='healthy':
            if len(state['workers'])!=workers:raise RuntimeError('Existing Spark cluster has another worker count; stop it explicitly first')
            return state
        for port in [self.port,self.ui,self.port+1,self.port+2,self.ui+1,self.ui+2]:
            with socket.socket() as s:
                if s.connect_ex(('127.0.0.1',port))==0:raise RuntimeError(f'Occupied Spark port {port}; no service was changed')
        self.environment();write_json(self.root/'identity.json',{'root':str(self.root),'master':self.url,'profile':self.profile})
        try:
            self.launch('master',['org.apache.spark.deploy.master.Master','--host','127.0.0.1','--port',str(self.port),'--webui-port',str(self.ui)])
            for n in range(1,workers+1):
                self.launch(f'worker{n}',['org.apache.spark.deploy.worker.Worker','--host','127.0.0.1','--port',str(self.port+n),
                    '--webui-port',str(self.ui+n),'--cores','2','--memory',self.worker_memory,'--work-dir',str(self.root/f'worker{n}'),self.url])
            for _ in range(80):
                state=self.status()
                if state['status']=='healthy' and len(state['workers'])==workers:return state
                if any(p.poll() is not None for p in self.children.values()):raise RuntimeError('Spark daemon exited; inspect project cluster logs')
                time.sleep(.5)
            raise RuntimeError('Spark cluster registration timeout')
        except BaseException:
            self.stop_children();raise
    def stop_children(self):
        for name,p in reversed(list(self.children.items())):
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=20)
                except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
        self.children.clear()
    def stop(self):
        if self.children:self.stop_children();return {'status':'stopped'}
        # Normal Terminal management verifies the full command before signalling.
        marker=self.root/'processes.json'
        if not marker.exists():return {'status':'stopped'}
        for name,pid in reversed(list(json.loads(marker.read_text()).items())):
            command=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True,check=True).stdout
            expected='org.apache.spark.deploy.master.Master' if name=='master' else 'org.apache.spark.deploy.worker.Worker'
            owned=str(self.port) in command if name=='master' else str(self.root/name) in command
            if expected not in command or not owned:continue
            try:os.kill(pid,signal.SIGTERM)
            except ProcessLookupError:pass
        return {'status':'stop_requested'}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['start','status','stop']);p.add_argument('--workers',type=int,default=2);a=p.parse_args();c=Cluster();print(json.dumps(c.start(a.workers) if a.action=='start' else getattr(c,a.action)(),indent=2))
