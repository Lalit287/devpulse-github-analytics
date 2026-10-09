# Source this script from the DevPulse root: source scripts/activate.sh
if [ ! -f config/settings.py ] || [ ! -f .venv/bin/activate ]; then
  echo 'Run from the DevPulse root after creating .venv.' >&2
  return 1
fi
source .venv/bin/activate
export DEVPULSE_PROJECT_DIR="$PWD"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export JUPYTER_PATH="$PWD/.runtime/share/jupyter"
export JUPYTER_CONFIG_DIR="$PWD/.runtime/config"
export JUPYTER_DATA_DIR="$PWD/.runtime/data"
export JUPYTER_RUNTIME_DIR="$PWD/.runtime/runtime"
export IPYTHONDIR="$PWD/.runtime/ipython"
export MPLCONFIGDIR="$PWD/.runtime/matplotlib"
export XDG_CACHE_HOME="$PWD/.runtime/cache"
mkdir -p "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR" "$IPYTHONDIR" "$MPLCONFIGDIR" "$XDG_CACHE_HOME"
# Auto-detection affects this shell only; explicit JAVA_HOME is respected.
if [ -z "${JAVA_HOME:-}" ]; then
  DEVPULSE_JAVA_DIR=$(python -c 'from exploration.spark_demo import configure_java; print(configure_java() or "")')
  if [ -n "$DEVPULSE_JAVA_DIR" ]; then export JAVA_HOME="$DEVPULSE_JAVA_DIR"; fi
fi
