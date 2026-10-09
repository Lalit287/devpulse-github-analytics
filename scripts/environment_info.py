"""Save measured runtime details, retaining actual detection errors."""
import importlib.metadata
import os
import platform
import subprocess
import sys

from config.settings import REPORTS_DIR
from exploration.io import write_json
from exploration.spark_demo import configure_java


def collect_environment():
    configure_java()
    java = os.path.join(os.environ["JAVA_HOME"], "bin/java") if os.environ.get("JAVA_HOME") else "java"
    try:
        probe = subprocess.run([java, "-version"], capture_output=True, text=True)
        java_version = probe.stderr.strip() or probe.stdout.strip()
    except OSError as exc:
        java_version = str(exc)
    memory, memory_note = None, None
    try:
        import psutil  # Supplied by Jupyter's ipykernel dependency.
        memory = psutil.virtual_memory().total
    except Exception as exc:
        memory_note = f"Memory detection unavailable: {type(exc).__name__}: {exc}"
    packages = {}
    for package in ("pyspark", "pandas", "numpy", "matplotlib", "jupyterlab", "requests", "pytest", "nbclient"):
        try:
            packages[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            packages[package] = "NOT INSTALLED"
    return {"os": platform.platform(), "architecture": platform.machine(),
            "python": sys.version, "executable": sys.executable,
            "virtual_environment": sys.prefix != sys.base_prefix,
            "java_home": os.environ.get("JAVA_HOME"), "java_version": java_version,
            "total_memory_bytes": memory, "memory_detection_note": memory_note, "packages": packages}


if __name__ == "__main__":
    info = collect_environment()
    write_json(REPORTS_DIR / "environment.json", info)
    print(info)
