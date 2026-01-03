# FieldNet – Edge Node

This repository contains the FieldNet / NopeNet edge-node implementation,
designed to run on small Linux devices (e.g. Raspberry Pi).

## Node identity

Each deployment defines its identity in:

    config/node.yaml

Example node IDs:
- field.pi1.mod4
- field.pi2.mod4
- field.pi3.nano

## Structure

- `transport/` – network transport (WebSockets, later MQTT, etc.)
- `field/`     – FieldNet / NopeNet logic
- `demos/`     – runnable demo entry points
- `viz/`       – visualization adapters (e.g. SpiroGrinder)
- `scripts/`   – helper scripts
- `logs/`      – local runtime logs (not versioned)

## Status

Early scaffolding. Transport and demo clients will be added incrementally.
