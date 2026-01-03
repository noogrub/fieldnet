#!/usr/bin/env python3
import asyncio
import time
import yaml
from pathlib import Path
from fieldnet.transport.websocket_client import WebSocketClient

def now_ts() -> str:
    return str(int(time.time()))

def load_config() -> dict:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "node.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def on_message(msg: dict):
    print("[recv]", msg)

async def main():
    cfg = load_config()
    ws_url = cfg["transport"]["websocket_url"]
    node_id = cfg["node_id"]

    source = f"{node_id}.cam01"
    client = WebSocketClient(ws_url)

    await client.connect()
    asyncio.create_task(client.recv_loop(on_message))

    colors = ["green", "yellow", "red"]
    i = 0

    try:
        while True:
            msg = {
                "type": "display.color",
                "source": source,
                "data": {
                    "id": "cam01",
                    "color": colors[i % len(colors)],
                    "stamp": now_ts(),
                },
                "ts": now_ts(),
            }

            await client.send(msg)
            print("[sent]", msg)

            i += 1
            await asyncio.sleep(1)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
