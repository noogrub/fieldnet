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

def emit_mark(label: str, scenario_hash: str, note: str | None = None):
    rec = {
        "record": {
            "type": "mark",
            "schema_version": "node_state_schema_v0",
        },
        "time": {
            "sim_s": 0.0,
            "wall_ts": None,
        },
        "experiment": {
            "scenario_hash": scenario_hash,
        },
        "mark": {
            "label": label,
            "note": note,
        },
    }
    print(json.dumps(rec, sort_keys=True))
    return rec

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, obj: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")


def write_json(path: Path, obj: dict):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, sort_keys=True, indent=2)


def initial_beam_mark(faults_cfg: dict) -> str | None:
    """
    Return 'beam_on' or 'beam_off' based on the segment covering t=0.
    If ambiguous or unspecified, return None.
    """
    segments = faults_cfg.get("segments", [])
    for seg in segments:
        t0 = seg.get("t0")
        t1 = seg.get("t1")
        if t0 is None or t1 is None:
            continue
        if t0 <= 0 < t1:
            marks = set(seg.get("marks", []))
            if "beam_on" in marks:
                return "beam_on"
            if "beam_off" in marks:
                return "beam_off"
            return None
    return None

def segment_at_time(faults_cfg: dict, t: float) -> dict | None:
    for seg in faults_cfg.get("segments", []):
        t0 = seg.get("t0")
        t1 = seg.get("t1")
        if t0 is None or t1 is None:
            continue
        if t0 <= t < t1:
            return seg
    return None


def compile_fault_bundle_at_time(faults_cfg: dict, t: float) -> dict:
    """
    Deterministic-only compilation for v0:
      - level flux
      - apply events active at t
    """
    bundle: dict[str, float] = {}
    active_segments: list[str] = []
    marks: list[str] = []

    seg = segment_at_time(faults_cfg, t)
    if not seg:
        return {
            "t": t,
            "active_segments": active_segments,
            "marks": marks,
            "fault_bundle": bundle,
        }

    active_segments.append(seg.get("name"))
    marks.extend(seg.get("marks", []))

    # Level flux contributions
    for fx in seg.get("flux", []):
        if fx.get("process") != "level":
            continue
        effect = fx.get("effect", {})
        if effect.get("kind") != "continuous":
            continue
        for k, v in effect.get("bundle", {}).items():
            bundle[k] = bundle.get(k, 0.0) + float(v)

    # Apply events active at t
    for ev in seg.get("events", []):
        if ev.get("action") != "apply":
            continue
        at = ev.get("at")
        dur = ev.get("duration_s", 0)
        if at is None:
            continue
        if at <= t < (at + dur):
            for k, v in ev.get("bundle", {}).items():
                bundle[k] = bundle.get(k, 0.0) + float(v)

    return {
        "t": t,
        "active_segments": active_segments,
        "marks": marks,
        "fault_bundle": bundle,
    }

def require_keys(doc: dict, keys: list[str], label: str):
    missing = [k for k in keys if k not in doc]
    if missing:
        raise ValueError(f"{label}: missing required keys: {missing}")


# ---- main ------------------------------------------------------------------
def main(run_dir: Path, t: float = 0.0):
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
    
    # Output paths (use logging config if present)
    out_cfg = logging_cfg["logging"].get("output", {})
    base_dir = Path(out_cfg.get("base_dir", run_dir / "logs"))
    if not base_dir.is_absolute():
        base_dir = (run_dir / base_dir).resolve()
    ensure_dir(base_dir)

    run_label = out_cfg.get("run_label", run_cfg.get("run_label"))
    marks_path = base_dir / f"{run_label}.marks.jsonl"
    bundle_path = base_dir / f"{run_label}.fault_bundle.t{t}.json"

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

    # Emit scenario_loaded mark (stdout only, v0)
    rec = emit_mark(
        label="scenario_loaded",
        scenario_hash=scenario_hash,
        note="Scenario loaded and validated",
    )
    write_jsonl(marks_path, rec)

    # Emit faults enabled/disabled mark (stdout only, v0)
    if faults_cfg.get("enabled", False):
        rec = emit_mark(
            label="faults_enabled",
            scenario_hash=scenario_hash,
            note="Fault injection enabled by scenario",
        )
        write_jsonl(marks_path, rec)

    else:
        rec = emit_mark(
            label="faults_disabled",
            scenario_hash=scenario_hash,
            note="Fault injection disabled by scenario",
        )
        write_jsonl(marks_path, rec)

    # Emit initial beam state mark (stdout only, v0)
    beam_mark = initial_beam_mark(faults_cfg)
    if beam_mark:
        rec = emit_mark(
            label=beam_mark,
            scenario_hash=scenario_hash,
            note="Initial beam state at t=0",
        )
        write_jsonl(marks_path, rec)

    # Compile and emit initial fault bundle (t=0), stdout only
    initial_bundle = compile_fault_bundle_at_time(faults_cfg, t=t)
    print(f"\nInitial fault bundle @ t={t}:")
    print(json.dumps(initial_bundle, sort_keys=True))
    write_json(bundle_path, initial_bundle)

    print("\nStatus: scenario loaded OK")

if __name__ == "__main__":
    if len(sys.argv) not in (2, 4):
        raise SystemExit(
            "Usage: scenario_runner_v0.py <run_directory> [--t <seconds>]"
        )

    run_dir = Path(sys.argv[1])
    t = 0.0
    if len(sys.argv) == 4:
        if sys.argv[2] != "--t":
            raise SystemExit(
                "Usage: scenario_runner_v0.py <run_directory> [--t <seconds>]"
            )
        t = float(sys.argv[3])

    main(run_dir, t)
