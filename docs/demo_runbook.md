# Final local demonstration

## Start and verify

In your Mac Terminal:

```sh
cd ~/Desktop/DevPulse
.venv/bin/python -m deployment.runtime start
.venv/bin/python -m scripts.complete_week8 --reuse-benchmarks
```

The start command verifies/reuses the project PostgreSQL and HDFS storage, starts
Kafka, Airflow, the dashboard and two bounded standalone Spark workers, and reports
health. PostgreSQL's macOS shared-memory operation requires normal Terminal execution
if the desktop app sandbox prevents startup. It never formats an existing owned
cluster. Airflow migration uses a bounded two-connection pool for its database
lock; running services retain their one-connection pools. Completion checks benchmarks, source/model parity, service health and every
required test. It clears stale completion status before attempting a new verification.

Useful endpoints:

- Dashboard: `http://127.0.0.1:18501`
- Airflow: `http://127.0.0.1:18080` (user `devpulse`; local private password file
  `.runtime/airflow/passwords.json`)
- Spark master: `http://127.0.0.1:18081` (two separate worker JVMs)
- HDFS NameNode: `http://127.0.0.1:19870`

Daily/replay DAGs stay paused for the controlled demonstration. Trigger a replay or
unpause a schedule deliberately in Airflow; the default historical date is January 1,
2025. Starting services alone does not download a new day's archives.

## Ten-minute demonstration

1. Activity overview: show 3,909,986 clean events, repository/account totals and full
   archive coverage. Explain that the activity date is historical.
2. Repositories: compare observed attention and participation; use date, language
   and name filters. Explain the equal intraday windows and minimum growth criteria.
3. Languages: show association coverage and the difference between primary-language
   and byte-share attribution; do not imply complete language coverage.
4. Public accounts: explain that bots are included and public activity is not total
   developer productivity.
5. Prediction: identify the separate 2015 corpus, held-out PR-AUC, precision@10 and
   confusion matrix. Explain validation-only selection and uncalibrated probabilities.
6. Pipeline monitor: show completed Airflow runs, real Kafka hour, offset batches,
   late/duplicate counters and the independent recovery proof.
7. Expand performance evaluation: compare identical repository results, Pandas/Spark
   times, worker scaling and storage projections. Explain single-host warm-cache scope.
8. Open the Spark master: identify both registered worker JVMs and the recorded
   applications/executor task proof in the final report.

To rerun measured benchmarks, omit `--reuse-benchmarks`; expensive processing is
bounded and sequential. Source checksums and exact output parity are required.

## Stop and recover

```sh
.venv/bin/python -m deployment.runtime stop
```

Run service management from normal Terminal so process ownership can be inspected.
Stopping retains data and checkpoints. Restart with the start command; existing
snapshots and database publications are reused. If a port is occupied by an
unrecognized process, the manager refuses to replace it. Inspect project runtime
logs rather than deleting data or changing ports blindly.

## Backup and portability

Keep the entire project folder for the local demonstration, including `.runtime`
credentials/storage and `data` artifacts. A source bundle intentionally excludes
those. To reconstruct elsewhere, install the pinned Python/Java/PostgreSQL/Hadoop
requirements from the setup guide, collect the documented archive windows, then run
the existing weekly completion helpers. Native paths and installation details must
be adapted explicitly; copied credentials and database files should remain private.
The source repository is [devpulse-github-analytics](https://github.com/Lalit287/devpulse-github-analytics).
It contains source, guides and measured evidence. Local datasets, credentials,
service storage and generated Hadoop configuration are excluded; configuration
is regenerated for the destination machine by the HDFS lifecycle commands.
