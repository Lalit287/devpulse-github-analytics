"""Run every notebook cell using a project-local kernel, without global registration."""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

from config.settings import REPORTS_DIR, ROOT
from exploration.io import write_json


def configure_jupyter():
    runtime = ROOT / ".runtime"
    for env, subdirectory in {"JUPYTER_CONFIG_DIR": "config", "JUPYTER_DATA_DIR": "data",
                              "JUPYTER_RUNTIME_DIR": "runtime", "IPYTHONDIR": "ipython",
                              "MPLCONFIGDIR": "matplotlib"}.items():
        folder = runtime / subdirectory
        folder.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault(env, str(folder))
    kernel = runtime / "share/jupyter/kernels/devpulse"
    write_json(kernel / "kernel.json", {
        "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": "DevPulse (.venv)", "language": "python"})
    os.environ["JUPYTER_PATH"] = str(runtime / "share/jupyter") + os.pathsep + os.environ.get("JUPYTER_PATH", "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register-only", action="store_true", help="create the local kernel without executing")
    args = parser.parse_args()
    configure_jupyter()
    if args.register_only:
        print("DevPulse kernel created in .runtime/share/jupyter; source scripts/activate.sh before JupyterLab")
        return
    import nbformat
    from nbclient import NotebookClient
    notebook_path = ROOT / "notebooks/01_gharchive_exploration.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)
    started = time.monotonic()
    status = {"notebook": str(notebook_path.relative_to(ROOT)),
              "started_at_utc": datetime.now(timezone.utc).isoformat()}
    try:
        NotebookClient(notebook, timeout=300, kernel_name="devpulse",
                       resources={"metadata": {"path": str(ROOT)}}, allow_errors=False).execute()
        code_cells = [cell for cell in notebook.cells if cell.cell_type == "code"]
        if not all(cell.execution_count is not None for cell in code_cells):
            raise RuntimeError("some code cells did not execute")
        nbformat.write(notebook, notebook_path)
        status.update({"status": "passed", "executed_code_cells": len(code_cells), "error_outputs": 0})
    except Exception as exc:
        status.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        status["duration_seconds"] = round(time.monotonic() - started, 2)
        write_json(REPORTS_DIR / "notebook_execution.json", status)
    print(status)


if __name__ == "__main__":
    main()
