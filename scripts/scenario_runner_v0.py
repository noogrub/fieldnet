#!/usr/bin/env python3
"""
Scenario Runner v0 — minimal loader / validator

Loads:
  - run.yaml
  - scenario.faults.yaml
  - scenario.logging.yaml

Computes a stable scenario_hash and prints a concise summary.
No simulation side effects.
"""

import hashlib
import json
import sys
from pathlib import Path

import yaml


# ---- helpers ---------------------------------------------------------------

def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def canonical_hash(obj):
    """
    Stable hash across runs:
    - JSON
    - sorted keys
    - dates normalized to ISO strings
    """
    def normalize(x):
        if isinstance(x, dict):
            return {k: normalize(v) for k, v in x.items()}
        if isinstance(x, list):
            return [normalize(v) for v in x]
        if hasattr(x, "isoformat"):   # date / datetime
            return x.isoformat()
        return x

    norm = normalize(obj)
    blob = json.dumps(norm, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()

def require_keys(doc: dict, keys: list[str], label: str):
    missing = [k for k in keys if k not in doc]
    if missing:
        raise ValueError(f"{label}: missing required keys: {missing}")


# ---- main ------------------------------------------------------------------

def main(run_dir: Path):
    if not run_dir.is_dir():
        raise SystemExit(f"Not a directory: {run_dir}")

    run_yaml     = run_dir / "run.yaml"
    faults_yaml  = run_dir / "scenario.faults.yaml"
    logging_yaml = run_dir / "scenario.logging.yaml"

    for p in (run_yaml, faults_yaml, logging_yaml):
        if not p.exists():
            raise SystemExit(f"Missing required file: {p}")

    run_cfg     = load_yaml(run_yaml)
    faults_cfg  = load_yaml(faults_yaml)
    logging_cfg = load_yaml(logging_yaml)

    # ---- light validation (v0) ---------------------------------------------

    require_keys(run_cfg, ["run_label", "intent"], "run.yaml")
    require_keys(faults_cfg, ["schema", "enabled", "segments"], "scenario.faults.yaml")
    require_keys(logging_cfg, ["logging"], "scenario.logging.yaml")
    require_keys(logging_cfg["logging"], ["enabled", "schema", "records"], "scenario.logging.yaml:logging")

    # ---- scenario hash -----------------------------------------------------

    scenario_obj = {
        "run": run_cfg,
        "faults": faults_cfg,
        "logging": logging_cfg,
    }

    scenario_hash = canonical_hash(scenario_obj)
    short_hash = scenario_hash[:8]

    # ---- concise summary ---------------------------------------------------

    print("=== Scenario Runner v0 ===")
    print(f"run_dir        : {run_dir}")
    print(f"run_label      : {run_cfg.get('run_label')}")
    print(f"intent         : {run_cfg.get('intent')}")
    print(f"fault_schema   : {faults_cfg.get('schema')}")
    log_schema = logging_cfg["logging"]["schema"]
    print(f"logging_schema : {log_schema.get('version')}")
    print(f"scenario_hash  : {scenario_hash}")
    print(f"hash_short     : {short_hash}")

    print("\nFault summary:")
    for name, f in faults_cfg.get("faults", {}).items():
        ftype = f.get("type", "unknown")
        print(f"  - {name}: {ftype}")

    print("\nLogging summary:")
    records = logging_cfg["logging"].get("records", {})
    for name, cfg in records.items():
        rate = cfg.get("rate", "n/a")
        print(f"  - {name}: rate={rate}")

    print("\nStatus: scenario loaded OK")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: scenario_runner_v0.py <run_directory>")
    main(Path(sys.argv[1]))
