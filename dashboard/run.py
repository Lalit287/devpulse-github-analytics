"""Start and inspect the project dashboard on loopback port 18501."""
import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time

import requests
from config.settings import ROOT
from exploration.io import write_json

RUNTIME=ROOT/'.runtime/dashboard'
PORT=18501
URL=f'http://127.0.0.1:{PORT}'


def healthy():
    try:return requests.get(URL+'/_stcore/health',timeout=2).status_code==200
    except requests.RequestException:return False


def status():
    marker=RUNTIME/'server.json'
    own=marker.exists() and json.loads(marker.read_text()).get('app')==str(ROOT/'dashboard/app.py')
    return {'status':'healthy' if own and healthy() else 'stopped','url':URL}


def start():
    RUNTIME.mkdir(parents=True,exist_ok=True)
    marker=RUNTIME/'server.json'
    if healthy():
        if not marker.exists() or json.loads(marker.read_text()).get('app')!=str(ROOT/'dashboard/app.py'):
            raise RuntimeError('Dashboard port is occupied by an unrecognized service')
        return {'status':'healthy','url':URL}
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1',PORT))==0:raise RuntimeError('Dashboard port is already occupied')
    with (RUNTIME/'streamlit.log').open('ab') as output:
        process=subprocess.Popen([sys.executable,'-m','streamlit','run',str(ROOT/'dashboard/app.py'),
                                  '--server.address','127.0.0.1','--server.port',str(PORT),'--server.headless','true'],
                                 cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
    write_json(marker,{'pid':process.pid,'app':str(ROOT/'dashboard/app.py'),'port':PORT})
    for _ in range(40):
        if healthy():return {'status':'healthy','url':URL}
        if process.poll() is not None:raise RuntimeError('Dashboard exited; see .runtime/dashboard/streamlit.log')
        time.sleep(.25)
    raise RuntimeError('Dashboard did not become healthy within 10 seconds')


def stop():
    marker=RUNTIME/'server.json'
    if marker.exists():
        details=json.loads(marker.read_text())
        if details.get('app')!=str(ROOT/'dashboard/app.py'):raise RuntimeError('Unrecognized dashboard process')
        # A stale PID file must never stop a different process after PID reuse.
        command=subprocess.run(['ps','-p',str(details['pid']),'-o','command='],capture_output=True,text=True).stdout
        if str(ROOT/'dashboard/app.py') not in command:
            raise RuntimeError('Dashboard PID is absent or belongs to another process; no process was stopped')
        try:os.kill(details['pid'],signal.SIGTERM)
        except ProcessLookupError:pass
    return {'status':'stopped','url':URL}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['start','status','stop'])
    args=parser.parse_args()
    result=start() if args.action=='start' else stop() if args.action=='stop' else status()
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
