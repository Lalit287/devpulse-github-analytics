# macOS setup and reproducibility

## 1. Inspect the machine

```bash
uname -sm
sw_vers
python3 --version
command -v python3
/usr/libexec/java_home -V
java -version
sysctl -n hw.memsize
df -h .
```

The measured environment is Apple Silicon (`arm64`), macOS 27.0.1, 24 GiB physical
memory, and about 247 GiB free at initial inspection. System Python is 3.13.5;
this project uses an available Python 3.12.14 interpreter in an isolated venv.
The sandbox denied direct `sysctl` memory inspection (`Operation not permitted`);
the installed Jupyter dependency `psutil` successfully measured 25,769,803,776 bytes.
Measurements are machine-specific, not minimum requirements.

## 2. Select compatible Python and Java

Python 3.11 or 3.12 plus Java 17 is the conservative setup used here. The project
pins PySpark 4.0.1, whose [official installation guide](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html)
documents Python 3.9+ and Java 17+. The local verification exercises the Python
worker as well as Spark SQL; installed versions alone do not establish compatibility.

Java 17 was already installed at `/opt/homebrew/opt/openjdk@17`, so no Java
installation or global shell changes were needed. `/usr/libexec/java_home -V`
also listed Java 24; that is not the JDK used in this project. Auto-detection
prefers Homebrew Java 17 or a registered Java 17. An explicit `JAVA_HOME` is
respected, so unset an incompatible setting or override it in this shell.

If your machine lacks these runtimes, install them through your normal software
manager. For Homebrew users, optional commands are:

```bash
brew install python@3.12 openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
```

Intel Macs commonly use `/usr/local/opt/` instead of `/opt/homebrew/opt/`.
Do not use `sudo pip`, overwrite system Python, or register global Java settings.

## 3. Create and install the isolated environment

From the DevPulse root:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install --editable .
source scripts/activate.sh
python -m pip check
python -m scripts.environment_info
```

The exact interpreter used for the initial setup in this Codex desktop run was:

```bash
/Users/lalitaditya/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m venv .venv
.venv/bin/python -m pip install --editable .
```

That bundled path is specific to this machine. Recreate `.venv` with your own
Python 3.12 if moving the project. Initial package installation needs internet
access and downloads the approximately 434 MB PySpark distribution. The raw-data
quota does not cover dependency installation. `pyproject.toml` pins the direct
dependencies; machine-specific dependency inventories remain local.

## 4. Run the bounded real-data pipeline

```bash
source scripts/activate.sh
python -m ingestion.download_gharchive --date 2025-01-01 --hour 12 --max-disk-mb 500
python -m exploration.schema_inspection data/raw/2025-01-01-12.json.gz --sample-size 10000 --seed 42
python -m exploration.explore_dataset
python -m exploration.spark_demo
```

Use module execution (`python -m ...`) from the root for reliable imports. The
downloader uses no credentials and leaves incomplete bytes out of final files.
Reexecution validates and reuses the existing archive. Source checksums and the
sample checksum make the reported provenance auditable. Raise quotas only after
checking storage capacity; the default task downloads exactly one hourly file.

## 5. Execute or open the notebook

```bash
source scripts/activate.sh
python -m scripts.execute_notebook
# To open interactively after configuring the local kernel:
python -m scripts.execute_notebook --register-only
jupyter lab notebooks/01_gharchive_exploration.ipynb
```

The local kernel specification invokes `.venv/bin/python`. Configuration,
IPython state, Matplotlib cache, and Jupyter runtime files live in `.runtime/`.
Choose `DevPulse (.venv)` in JupyterLab. Both root and notebook-directory launch
paths work. Notebook execution writes a success/failure JSON file and saves the
executed notebook only after every cell has run successfully.

## 6. Tests, result verification, and report

```bash
source scripts/activate.sh
python -m pytest --junitxml=reports/tests.xml
python -m scripts.verify_outputs
python -m scripts.generate_report
```

Tests use synthetic local fixtures only and need no GH Archive download or Java.
The separate real-data verifier checks manifests, output totals, PNG decoding,
notebook execution, and Pandas/Spark parity. The report consumes saved evidence;
unavailable criteria are marked BLOCKED rather than assumed complete.

## Troubleshooting

- HTTP failures print their actual exception. Check connectivity and archive
  availability; use a completed historical hour. The downloader retries 429/5xx
  and transient errors and does not repeatedly retry 404s.
- `JAVA_GATEWAY_EXITED`: inspect `JAVA_HOME/bin/java -version`, use Java 17, and
  inspect the saved notebook/Spark output. Auto-detection does not override an
  explicit JAVA_HOME.
- Spark and Jupyter require local loopback sockets. A restricted execution host
  must permit local networking. No external Spark service is required.
- Pandas record-cap error: generate a smaller reservoir sample rather than
  loading a large archive directly.
- Gzip corruption: rerun the downloader; replacement is promoted only after
  validation. CRC errors are not silently skipped as malformed JSON.
- Sample hash mismatch: regenerate the sample and rerun EDA; do not reuse stale
  provenance metadata with edited data.
- A raw disk-limit error preserves finished archives. Adjust the explicit quota
  only if the intended data window and available capacity justify it.
- In this Codex sandbox, Jupyter's shutdown cleanup logged a nonfatal psutil
  process-enumeration `Operation not permitted` error after all cells completed.
  The executed notebook and Spark/Pandas assertions succeeded. This host restriction
  is retained in the report; it is distinct from a notebook cell failure.
