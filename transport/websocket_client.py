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

    async def recv_loop(self, handler):
        """
        Receive messages forever and pass decoded JSON to handler(message).

        Exits quietly on normal close or task cancellation.
        """
        if self._ws is None:
            raise RuntimeError("WebSocket not connected")

        try:
            async for raw in self._ws:
                try:
                    message = json.loads(raw)
                except Exception:
                    self._log.warning(f"received non-JSON: {raw}")
                    continue

                await handler(message)

        except asyncio.CancelledError:
            # Normal during shutdown (Ctrl-C, stop command, etc.)
            self._log.debug("recv_loop cancelled")
            raise

        except Exception as e:
            # Websockets may raise ConnectionClosed* variants here.
            # Treat close as informational, not a crash.
            self._log.info(f"recv_loop ended: {e}")
            return


    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
