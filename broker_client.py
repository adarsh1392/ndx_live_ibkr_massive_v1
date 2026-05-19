import os
from typing import Protocol

from ninja_trader_client import NinjaTraderClient
from rithmic_client import RithmicClient


class BrokerClient(Protocol):
    name: str
    host: str
    port: int
    instrument: str

    async def place_entry(self, direction: str, qty: int) -> dict: ...

    async def place_exit(self, direction: str, qty: int) -> dict: ...

    async def flatten(self) -> dict: ...


def create_broker_client() -> BrokerClient:
    broker = os.getenv("BROKER", "ninjatrader").strip().lower()

    if broker == "ninjatrader":
        instrument = os.getenv("NT_INSTRUMENT", "MNQ 06-26")
        return NinjaTraderClient(
            host=os.getenv("NT_HOST", "127.0.0.1"),
            port=int(os.getenv("NT_PORT", "5555")),
            instrument=instrument,
        )

    if broker == "rithmic":
        instrument = (
            os.getenv("RITHMIC_INSTRUMENT")
            or os.getenv("BROKER_INSTRUMENT")
            or os.getenv("NT_INSTRUMENT")
            or "MNQM6"
        )
        return RithmicClient(
            host=os.getenv("RITHMIC_HOST", "127.0.0.1"),
            port=int(os.getenv("RITHMIC_PORT", "6500")),
            instrument=instrument,
            account_id=os.getenv("RITHMIC_ACCOUNT_ID", ""),
            exchange=os.getenv("RITHMIC_EXCHANGE", "CME"),
            gateway=os.getenv("RITHMIC_GATEWAY", ""),
        )

    raise ValueError(
        f"Unsupported BROKER='{broker}'. Expected 'ninjatrader' or 'rithmic'."
    )
