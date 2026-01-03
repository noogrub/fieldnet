import asyncio
import json
import logging
from typing import Optional

import websockets


class WebSocketClient:
    """
    Minimal, boring, reliable WebSocket client.

    Responsibilities:
    - connect
    - send JSON messages
    - reconnect on failure

    No FieldNet logic belongs here.
    """

    def __init__(self, url: str, *, reconnect_delay: float = 2.0):
        self.url = url
        self.reconnect_delay = reconnect_delay
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._log = logging.getLogger("fieldnet.transport.websocket")

    async def connect(self):
        while True:
            try:
                self._log.info(f"connecting to {self.url}")
                self._ws = await websockets.connect(self.url)
                self._log.info("connected")
                return
            except Exception as e:
                self._log.warning(f"connect failed: {e}; retrying in {self.reconnect_delay}s")
                await asyncio.sleep(self.reconnect_delay)

    async def send(self, message: dict):
        if self._ws is None:
            raise RuntimeError("WebSocket not connected")

        payload = json.dumps(message)
        await self._ws.send(payload)

    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
