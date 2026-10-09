"""Fetch a bounded selection from the completed core analytics snapshot."""
import json
from config.settings import ROOT
from exploration.io import write_json
from enrichment.github import enrich
from spark.snapshots import current_manifest


def main():
    core_root = ROOT / "work/week4/core"
    manifest = current_manifest(core_root)
    if manifest is None:
        raise ValueError("Build core analytics before enrichment")
    selection = core_root / "snapshots" / manifest["snapshot_id"] / "enrichment_candidates.json"
    entries, run = enrich(json.loads(selection.read_text()))
    write_json(ROOT / "reports/week4/metadata_selection_snapshot.json", entries)
    write_json(ROOT / "reports/week4/enrichment_run.json", run)
    print(json.dumps(run, indent=2))


if __name__ == "__main__":
    main()
