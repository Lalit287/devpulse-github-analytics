"""Create a verified source bundle without datasets, credentials or runtime binaries."""
import hashlib,json,tarfile
from pathlib import Path
from config.settings import ROOT
from exploration.io import sha256_file,write_json
FOLDERS=['analytics','benchmarks','config','dashboard','database','deployment','docs','enrichment','exploration','ingestion','modeling','notebooks','orchestration','scripts','spark','storage','streaming','tests','visualizations','.github','.streamlit','reports']
ROOT_FILES=['README.md','.gitignore','.env.example','pytest.ini','pyproject.toml']
FORBIDDEN={'.git','.runtime','.tools','.venv','.venv-airflow','__pycache__','.pytest_cache','.ipynb_checkpoints'}


def allowed(relative):
    p=Path(relative)
    if not p.parts or p.parts[0] not in FOLDERS+ROOT_FILES:return False
    if p.is_absolute() or '..' in p.parts or any(part in FORBIDDEN for part in p.parts):return False
    if len(p.parts)>1 and (p.parts[0],p.parts[1]) in [('config','hadoop'),('config','hadoop-verification'),('docs','references')]:return False
    if p.parts[0]=='reports' and (p.name in ['source_package.json','dashboard_pending.jpg'] or ' 2.' in p.name):return False
    if p.name.startswith('requirements') and p.suffix=='.txt':return False
    if p.name.startswith('.') and p.name not in ['.gitignore','.env.example']:return False
    if p.suffix in ['.pyc','.log'] or (p.suffix=='.xml' and p.parts[0]=='reports') or p.name in ['.env','credentials.json','passwords.json','secrets.json']:return False
    return True

def files(root=ROOT):
    root=Path(root);paths=[root/n for n in ROOT_FILES if (root/n).is_file()]
    for folder in FOLDERS:
        paths+=[p for p in (root/folder).rglob('*') if p.is_file() and not p.is_symlink() and allowed(p.relative_to(root))]
    return sorted(set(paths))

def create():
    dest=ROOT/'artifacts/DevPulse-source.tar.gz';dest.parent.mkdir(exist_ok=True);paths=files()
    entries={str(p.relative_to(ROOT)):{'sha256':sha256_file(p),'bytes':p.stat().st_size} for p in paths}
    # Never store a self-referencing package receipt inside its own source bundle.
    entries.pop('reports/week8/source_package.json',None)
    with tarfile.open(dest,'w:gz') as archive:
        for relative in entries:archive.add(ROOT/relative,arcname='DevPulse/'+relative,recursive=False)
    with tarfile.open(dest) as archive:
        for member in archive.getmembers():
            relative=str(Path(member.name).relative_to('DevPulse'))
            if not member.isfile() or not allowed(relative) or relative not in entries:raise ValueError('Unexpected source package member')
            if hashlib.sha256(archive.extractfile(member).read()).hexdigest()!=entries[relative]['sha256']:raise ValueError('Source package checksum mismatch')
    receipt={'status':'passed','path':str(dest),'sha256':sha256_file(dest),'bytes':dest.stat().st_size,'files':entries,'scope':'source, documentation and measured public-data evidence; large datasets and private runtime state excluded'}
    write_json(ROOT/'reports/week8/source_package.json',receipt);return receipt

if __name__=='__main__':print(json.dumps({k:v for k,v in create().items() if k!='files'},indent=2))
