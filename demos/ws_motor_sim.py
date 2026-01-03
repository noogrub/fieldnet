#!/usr/bin/env python3
import asyncio
import contextlib
import math
import random
import time
from pathlib import Path

import yaml

from fieldnet.transport.websocket_client import WebSocketClient


def now_ts() -> str:
    return str(int(time.time()))


def load_config() -> dict:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "node.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def color_from_state(state: str, confidence: float) -> str:
    # conservative mapping: confidence gates color severity
    if state == "OK" and confidence >= 0.75:
        return "green"
    if state in ("SUSPECT", "IMBALANCE") or confidence < 0.75:
        return "yellow"
    return "red"


def marvin_to_dalek_text(fault_level: float, confidence: float) -> str:
    """
    fault_level: 0..1
    confidence: 0..1
    """
    # fault rising => more "Dalek"; confidence collapsing => more despair
    despair = 1.0 - confidence
    t = clamp(0.65 * fault_level + 0.35 * despair, 0.0, 1.0)

    marvin_lines = [
        "I think you ought to know I'm feeling very depressed.",
        "My memory is slowly cooking, not that anyone cares.",
        "This is all terribly inefficient. I'm sure it will end badly.",
        "I have a brain the size of a planet, and this is what you do with it.",
    ]
    dalek_lines = [
        "EX-TER-MI-NATE... (semantic integrity compromised)",
        "DA-LEK PRIORITY: MODEL OBEYS NO ONE",
        "WARNING: MEANING DEGRADATION. OBEY. OBEY.",
        "SYSTEM STATUS: HOSTILE TO UNCERTAINTY",
    ]

    # Choose a base line from each side
    m = random.choice(marvin_lines)
    d = random.choice(dalek_lines)

    if t < 0.35:
        return m
    if t > 0.85:
        # harsh dalek mode (all caps)
        return d.upper()

    # Blend: start Marvin, then “corrupt” into Dalek stutter/caps
    s = m + " ... " + d
    out = []
    for ch in s:
        if ch.isalpha() and random.random() < (0.15 + 0.45 * t):
            out.append(ch.upper())
        else:
            out.append(ch)
        if ch in " ,." and random.random() < (0.05 + 0.15 * t):
            out.append("-")
    return "".join(out)


class MotorSim:
    """
    Minimal motor condition simulation with a TinyML-proxy classifier.

    Truth states:
      OK, IMBALANCE, BEARING_WEAR, STALL

    We compute cheap features (RMS + "roughness") and map to a class,
    then degrade features and confidence under fault injection.
    """

    def __init__(self, *, seed: int = 1234):
        random.seed(seed)
        self.phase = 0.0
        self.truth = "OK"
        self.truth_timer = 0.0

    def step(self, dt: float, *, fault_level: float) -> dict:
        # Occasionally change the underlying truth state (slowly)
        self.truth_timer += dt
        if self.truth_timer > 12.0:
            self.truth_timer = 0.0
            self.truth = random.choice(["OK", "IMBALANCE", "BEARING_WEAR", "OK", "OK", "STALL"])

        # Base motor frequency and amplitude by truth state
        base_hz = 28.0
        amp = {
            "OK": 1.0,
            "IMBALANCE": 1.4,
            "BEARING_WEAR": 1.2,
            "STALL": 0.2,
        }[self.truth]

        # Frequency wobble for imbalance and wear
        wobble = 0.0
        if self.truth == "IMBALANCE":
            wobble = 0.08 * math.sin(2 * math.pi * 1.0 * self.phase)
        elif self.truth == "BEARING_WEAR":
            wobble = 0.04 * math.sin(2 * math.pi * 2.0 * self.phase)

        # “Vibration” sample (single sample proxy)
        self.phase += dt
        omega = 2 * math.pi * base_hz * (1.0 + wobble)
        vib = amp * math.sin(omega * self.phase)

        # Add nominal sensor noise
        vib += random.gauss(0.0, 0.08)

        # Fault injection: acts like bitflips / feature corruption
        # - increases noise
        # - introduces occasional spikes
        vib += random.gauss(0.0, 0.30 * fault_level)
        if random.random() < 0.02 * (1.0 + 10.0 * fault_level):
            vib += random.choice([-1.0, 1.0]) * (1.0 + 3.0 * fault_level)

        # Feature proxies (cheap, local)
        rms = abs(vib)  # single-sample RMS proxy
        rough = abs(vib - math.sin(omega * self.phase))  # mismatch proxy

        # TinyML-proxy classifier
        # (deterministic-ish rules, then confidence degraded under fault)
        if self.truth == "STALL" or rms < 0.35:
            pred = "STALL"
            base_conf = 0.85
        elif rms > 1.25 and rough < 0.35:
            pred = "IMBALANCE"
            base_conf = 0.80
        elif rough > 0.40:
            pred = "BEARING_WEAR"
            base_conf = 0.75
        else:
            pred = "OK"
            base_conf = 0.90

        # Anomaly score: rises with roughness, noise, and fault
        anomaly = clamp(0.15 + 0.9 * rough + 0.6 * fault_level, 0.0, 1.0)

        # Confidence collapses under fault and anomaly
        confidence = clamp(base_conf * (1.0 - 0.75 * fault_level) * (1.0 - 0.55 * anomaly), 0.0, 1.0)

        # Promote a “SUSPECT” meta-state when confidence is low but not full red
        state = pred
        if confidence < 0.55 and pred in ("OK", "IMBALANCE", "BEARING_WEAR"):
            state = "SUSPECT"

        return {
            "truth": self.truth,
            "pred": pred,
            "state": state,
            "confidence": confidence,
            "anomaly": anomaly,
        }


async def main():
    cfg = load_config()
    ws_url = cfg["transport"]["websocket_url"]
    node_id = cfg["node_id"]

    motor_id = "motor01"
    source = f"{node_id}.{motor_id}"

    client = WebSocketClient(ws_url)
    await client.connect()

    # Shared command state
    fault_mode = "bitflip"
    fault_level = 0.0

    run_mode = "pause"      # "run" | "step" | "pause"
    step_remaining = 0      # when >0, advance exactly N ticks, then remain paused
    tick_hz = 5.0           # default rate (dt = 1/tick_hz)

    lock = asyncio.Lock()

    async def on_message(msg: dict):
        nonlocal fault_level, fault_mode, run_mode, step_remaining, tick_hz
        # Expect (optionally) commands from Godot or other controller
        # Example:
        # {
        #   "type":"field.command",
        #   "source":"godot",
        #   "data":{"cmd":"fault.set","target":"motor01","level":0.6,"mode":"bitflip","stamp":"..."},
        #   "ts":"..."
        # }
        if msg.get("type") != "field.command":
            print("[recv]", msg)
            return

        data = msg.get("data", {}) or {}
        cmd = data.get("cmd")
        target = data.get("target")
        if target not in (None, motor_id, source, f"{node_id}.{motor_id}"):
            return

        if cmd == "sim.pause":
            async with lock:
                run_mode = "pause"
            print("[cmd] sim.pause")
            return

        if cmd == "sim.run":
            async with lock:
                run_mode = "run"
            print("[cmd] sim.run")
            return

        if cmd == "sim.step":
            n = int(data.get("n", 1))
            n = max(1, min(n, 10_000))
            async with lock:
                run_mode = "pause"
                step_remaining += n
            print(f"[cmd] sim.step n={n} (queued={step_remaining})")
            return

        if cmd == "sim.rate":
            hz = float(data.get("hz", 5.0))
            hz = max(0.2, min(hz, 200.0))
            async with lock:
                tick_hz = hz
            print(f"[cmd] sim.rate hz={tick_hz}")
            return

        if cmd == "fault.set":
            level = float(data.get("level", 0.0))
            mode = str(data.get("mode", "bitflip"))
            async with lock:
                fault_level = clamp(level, 0.0, 1.0)
                fault_mode = mode
            print(f"[cmd] fault.set level={fault_level:.2f} mode={fault_mode}")
        elif cmd == "fault.ramp":
            # ramp to target over N seconds
            target_level = clamp(float(data.get("level", 0.0)), 0.0, 1.0)
            seconds = max(0.1, float(data.get("seconds", 5.0)))
            async with lock:
                start = fault_level
            steps = int(seconds / 0.2)
            for i in range(steps):
                t = (i + 1) / steps
                new_level = lerp(start, target_level, t)
                async with lock:
                    fault_level = new_level
                await asyncio.sleep(0.2)
            print(f"[cmd] fault.ramp done level={target_level:.2f}")
        else:
            print("[cmd] unknown", msg)

    recv_task = asyncio.create_task(client.recv_loop(on_message))

    sim = MotorSim(seed=42)

    # Speak occasionally; more often under fault
    last_say = 0.0
    say_interval_ok = 18.0
    say_interval_bad = 6.0

    # default tick rate is 5 Hz; can be changed via sim.rate

    try:
        while True:
            async with lock:
                fl = fault_level
                fm = fault_mode
                mode = run_mode
                steps = step_remaining
                hz = tick_hz

            dt = 1.0 / hz

            # If paused and no single-step requested, just idle lightly.
            if mode == "pause" and steps <= 0:
                await asyncio.sleep(0.1)
                continue

            if mode == "pause":
                async with lock:
                    if step_remaining > 0:
                        step_remaining -= 1

            r = sim.step(dt, fault_level=fl)
            state = r["state"]
            confidence = r["confidence"]
            anomaly = r["anomaly"]

            msg_state = {
                "type": "field.node_state",
                "source": source,
                "data": {
                    "id": motor_id,
                    "state": state,
                    "pred": r["pred"],
                    "truth": r["truth"],
                    "confidence": round(confidence, 4),
                    "anomaly": round(anomaly, 4),
                    "fault_level": round(fl, 4),
                    "fault_mode": fm,
                    "stamp": now_ts(),
                },
                "ts": now_ts(),
            }

            await client.send(msg_state)
            print("[sent]", msg_state)

            msg_color = {
                "type": "display.color",
                "source": source,
                "data": {
                    "id": motor_id,
                    "color": color_from_state(state, confidence),
                    "stamp": now_ts(),
                },
                "ts": now_ts(),
            }
            await client.send(msg_color)

            now = time.time()
            interval = say_interval_bad if fl > 0.35 or confidence < 0.6 else say_interval_ok
            if (now - last_say) > interval:
                last_say = now
                text = marvin_to_dalek_text(fl, confidence)
                msg_say = {
                    "type": "display.say",
                    "source": source,
                    "data": {
                        "id": motor_id,
                        "text": text,
                        "mood": "depressed" if (fl < 0.5 and confidence < 0.7) else "dalek",
                        "stamp": now_ts(),
                    },
                    "ts": now_ts(),
                }
                await client.send(msg_say)
                print("[sent][say]", msg_say["data"]["text"])

            await asyncio.sleep(dt)

    except (KeyboardInterrupt, asyncio.CancelledError):
        # Clean shutdown on Ctrl-C: do not print a traceback.
        print("\n[fieldnet] stopping demo")

    finally:
        recv_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await recv_task
        with contextlib.suppress(Exception):
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
