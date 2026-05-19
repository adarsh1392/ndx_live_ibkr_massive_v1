"""
ninja_trader_client.py
======================
Async TCP client for the PythonOrderServer NinjaScript AddOn.
Sends single pipe-delimited commands over a raw TCP socket on localhost.

Commands:
  ENTRY|LONG|1    — BUY  1 contract at market
  ENTRY|SHORT|1   — SELL SHORT 1 contract at market
  EXIT|LONG|1     — SELL 1 contract at market  (close long)
  EXIT|SHORT|1    — BUY TO COVER 1 contract    (close short)
  FLATTEN         — flatten entire position for the instrument

Response from NinjaTrader AddOn:
  "OK: ..."       — order submitted successfully
  "ERROR: ..."    — something went wrong
"""

import asyncio


class NinjaTraderClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5555,
        instrument: str = "MNQ 06-26",
        timeout: float = 5.0,
    ):
        self.name       = "ninjatrader"
        self.host       = host
        self.port       = port
        self.instrument = instrument
        self.timeout    = timeout

    @staticmethod
    def _parse_message_data(msg: str) -> dict:
        data: dict = {}
        prefix = "OK:"
        if not msg.upper().startswith(prefix):
            return data
        payload = msg[len(prefix):].strip()
        if "|" not in payload:
            return data

        parts = payload.split("|")
        data["type"] = parts[0].strip().upper()
        for token in parts[1:]:
            if "=" not in token:
                continue
            k, v = token.split("=", 1)
            data[k.strip()] = v.strip()
        return data

    async def _send(self, command: str) -> dict:
        """Open a short-lived TCP connection, send one command, read one reply."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout,
            )
            writer.write((command + "\n").encode("utf-8"))
            await writer.drain()
            raw = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            writer.close()
            await writer.wait_closed()
            msg = raw.decode("utf-8").strip()
            return {
                "success": msg.upper().startswith("OK"),
                "message": msg,
                "data": self._parse_message_data(msg),
            }
        except Exception as e:
            return {"success": False, "message": f"TCP error: {e}"}

    async def place_entry(self, direction: str, qty: int) -> dict:
        """Send a market ENTRY order (BUY or SELL SHORT)."""
        return await self._send(f"ENTRY|{direction.upper()}|{qty}")

    async def place_exit(self, direction: str, qty: int) -> dict:
        """Send a market EXIT order (SELL or BUY TO COVER)."""
        return await self._send(f"EXIT|{direction.upper()}|{qty}")

    async def flatten(self) -> dict:
        """Flatten the entire position for the instrument."""
        return await self._send("FLATTEN")

    async def ping(self) -> dict:
        """Check connectivity without placing or flattening orders."""
        return await self._send("PING")

    async def get_market_snapshot(self) -> dict:
        """Request current MNQ market snapshot from Ninja strategy context."""
        return await self._send("PRICE")

    async def get_last_execution(self) -> dict:
        """Request last captured execution report for this instrument."""
        return await self._send("LASTEXEC")
