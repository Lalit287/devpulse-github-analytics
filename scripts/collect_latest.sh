#!/usr/bin/env bash
# Optional scheduler entry point; no scheduler is installed by this project.
set -euo pipefail
DEVPULSE_JOB_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$DEVPULSE_JOB_ROOT"
source scripts/activate.sh
exec python -m ingestion.collect --latest "$@"
