"""
Async TCP client for a local Rithmic bridge process.

The Python strategy talks to a thin local bridge over JSON lines. That bridge
owns the official Rithmic API session and translates requests into actual
Rithmic order calls.
"""

import asyncio
import json


class RithmicClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6500,
        instrument: str = "MNQM6",
        account_id: str = "",
        exchange: str = "CME",
        gateway: str = "",
        timeout: float = 5.0,
    ):
        self.name = "rithmic"
        self.host = host
        self.port = port
        self.instrument = instrument
        self.account_id = account_id
        self.exchange = exchange
        self.gateway = gateway
        self.timeout = timeout

    async def _send(self, payload: dict) -> dict:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout,
            )
            writer.write((json.dumps(payload) + "\n").encode("utf-8"))
            await writer.drain()
            raw = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            writer.close()
            await writer.wait_closed()

            if not raw:
                return {
                    "success": False,
                    "message": "Bridge closed connection without a response",
                }

            msg = json.loads(raw.decode("utf-8").strip())
            if not isinstance(msg, dict):
                return {
                    "success": False,
                    "message": f"Unexpected bridge response: {msg!r}",
                }
            return msg
        except Exception as e:
            return {"success": False, "message": f"Rithmic bridge error: {e}"}

    def _payload(
        self,
        action: str,
        direction: str | None = None,
        qty: int | None = None,
    ) -> dict:
        payload = {
            "action": action,
            "instrument": self.instrument,
            "account_id": self.account_id,
            "exchange": self.exchange,
            "gateway": self.gateway,
        }
        if direction is not None:
            payload["direction"] = direction.upper()
        if qty is not None:
            payload["qty"] = int(qty)
        return payload

    async def place_entry(self, direction: str, qty: int) -> dict:
        return await self._send(self._payload("ENTRY", direction=direction, qty=qty))

    async def place_exit(self, direction: str, qty: int) -> dict:
        return await self._send(self._payload("EXIT", direction=direction, qty=qty))

    async def flatten(self) -> dict:
        return await self._send(self._payload("FLATTEN"))
