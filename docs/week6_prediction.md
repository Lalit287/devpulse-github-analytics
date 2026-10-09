# Week 6 — Historical popularity prediction

The measured completion status and results are in `reports/week6_report.md`.
All files remain in `~/Desktop/DevPulse`. Prediction data is independent of the
2025 activity snapshots; those snapshots and reports are preserved.

## Run or resume

Use the existing Python 3.12 environment and Java 17. Spark MLlib 4.0.1 is already
installed; no new Python package is needed.

```bash
cd ~/Desktop/DevPulse
.venv/bin/python -m scripts.complete_week6
```

The helper collects/resumes all 1,008 hourly archives for 2015-01-01 through
2015-02-11, creates checksummed compact projections, trains/evaluates the three
Spark classifiers, publishes model output transactionally, executes tests,
checks earlier-week preservation and generates the report/plots.
PostgreSQL must be available at `127.0.0.1:15432`; start it in normal Mac Terminal
with `.venv/bin/python -m database.cluster start` if needed. Services are local.

If the full corpus already exists, skip the download/gzip-validation pass while
still verifying every source and projection hash:

```bash
.venv/bin/python -m scripts.complete_week6 --skip-collection
```

Component commands:

```bash
.venv/bin/python -m modeling.corpus
.venv/bin/python -m modeling.pipeline
.venv/bin/python -m modeling.publish
.venv/bin/python -m scripts.verify_week6
.venv/bin/python -m scripts.generate_week6_report
```

Full completion uses the helper, which additionally checks rollback and records
fresh test receipts. Training recipe identity excludes wall-clock collection
timestamps, so unchanged source/code reuse the existing checksummed model.
Changing the model contract/source creates a new immutable snapshot; previous
models and database records remain available. A failed publication rolls back all
model data and retains the previously active serving pointer.

Collection runs four workers with separate per-day downloader manifests/locks.
Each day has a 1 GiB compressed raw quota; the fixed 42-day plan bounds raw storage
to 42 GiB, plus compact projections and models. A 20 GiB free-space guard applies
before collection. Actual measured size appears in the report. All hourly gzip
files are retained. Requests retry transient failures and gzip CRC/length errors;
completed hours resume. JSON projection batches hold at most 8,192 rows, with
lossless BIGINT IDs, UTC timestamps and retained raw-file provenance. Invalid
records are quarantined and counted; empty hours or rejection above 1% fail.

## Feature and target contract

Each example represents a repository observed during a complete seven-day archive
week, with at least five past events. Stable repository IDs join later outcomes;
IDs and names are excluded from the feature vector. All daily/hourly source files
must exist before an absent future count can become zero.

Past-week features:

- Observed star, fork, push, opened PR, opened issue and total event counts.
- Exact distinct public accounts and code-participating accounts (PushEvent or
  opened PR), plus observed active days.
- Star counts in the first four and last three source days, and smoothed
  per-day star-rate change. Count features are log1p transformed.

The target is at least five future star actions and a future-minus-past star gain
at or above the exact 90th percentile of positive gains in training only.
The five-star minimum is fixed before evaluation. Current GitHub metadata,
future counts, labels, repository IDs and names never enter the feature vector.
Repository age and 30-day history are omitted because those full historical
lookbacks are not available for all training cutoffs.

Archive membership bounds historical availability: a later file cannot insert an
old event retroactively into an earlier example. Event creation time must also
fall inside that source week. Events outside the source week are counted and
excluded. Archive publication latency is unknown, so these are logical batch
cutoffs rather than evidence of minute-exact production availability.

| Split | Forecast cutoff(s), UTC | Outcome completion |
|---|---|---|
| Train | Jan 8, Jan 15, Jan 22, 2015 | Through Jan 29 |
| Validation | Jan 29, 2015 | Through Feb 5 |
| Test | Feb 5, 2015 | Through Feb 12 |
| Historical forecast | Feb 12, 2015 | Feb 19 outcome unavailable in corpus |

Features end before their cutoff; future labels span the next seven days.
Each training/validation outcome ends by the next split's first cutoff.
The forecast's future counts and labels stay NULL. Repositories can recur
across time; the experiment evaluates later activity, not unseen-repository
generalization.

## Algorithms and evaluation

Compare Logistic Regression, Random Forest and Gradient-Boosted Trees with fixed
hyperparameters and seed 42. Model choice uses validation PR-AUC, then validation
Precision@10 as a tie-break. Its classification threshold maximizes validation
F1 on a fixed grid. Test outcomes never choose the target, algorithm, features or
decision threshold. Model artifacts are reloaded and checked for score parity.

Report precision, recall, F1, ROC-AUC, PR-AUC, Precision@10, Precision@100 and Brier
score. PR-AUC is Spark's exact unbinned trapezoidal area, not average precision.
A monotone recent-star-count ranking is a simple comparison baseline. Its score
is not a probability. Candidate models are uncalibrated; the report and dashboard
show reliability bins and state whether the selected model beats the baseline.
An evaluated weak result does not establish a useful current forecast.

## Artifacts and serving

```text
data/prediction_history/raw/YYYY-MM-DD/       all real gzip archives
data/prediction_history/projected/YYYY-MM-DD/ compact Parquet, receipts, quarantine
data/prediction_history/manifest.json         complete-hour source inventory
data/models/snapshots/<id>/weekly/            all repository-week aggregates
data/models/snapshots/<id>/examples/          past features and known/unknown labels
data/models/snapshots/<id>/models/            saved Spark ML pipelines
data/models/snapshots/<id>/heldout_predictions/ later test results
data/models/snapshots/<id>/forecast_predictions/ historical forward scores
data/models/_CURRENT.json                    checksummed artifact pointer
reports/week6/                               test XML/logs, verification, plots
```

`devpulse_ml.models` records the model and evaluation once. `current_model`
selects the active verified model; `forecast_context` stores past-only labels and
counts; `heldout_predictions` stores actual-versus-predicted test results.
The existing reserved `devpulse_control.repository_predictions` table receives
seven-day forecasts with compact evaluation references. These inserts, counts,
reader grants and pointer activation share one transaction and an advisory lock.
The reader cannot write. Source/SQL row counts, probability sums and UTC cutoffs
are checked; publication reruns verify identity and reuse rows.

Open the dashboard's **Popularity prediction** page. It shows the 2015 scope,
forecast rankings/search/CSV, the top 100 held-out scores, full-population metrics,
model comparison, confusion matrix and reliability plot. The historical forecast
is distinct from the actual outcomes already known in the earlier test period.

## Validation

Eight edge-case tests check IDs, temporal perturbations, late archives, missing
future activity, training-only target choice, cutoff/NULL guards and hand-computed
metrics. Five native tests check complete real artifacts, SQL parity and UTC,
read-only grants, reuse/rollback, parameterized bounded searches and dashboard
controls. The earlier 155-test suite is rerun; its prediction assertions accept
evaluated Week 6 output while retaining the empty-state fixture tests.
The preservation fingerprint protects 90 earlier source/data/report files.

References: [GH Archive](https://www.gharchive.org/),
[Spark 4.0.1 MLlib](https://spark.apache.org/docs/4.0.1/ml-classification-regression.html).
