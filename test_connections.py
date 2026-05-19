import asyncio
import os

from dotenv import load_dotenv
from ib_insync import IB, Index

from broker_client import create_broker_client

load_dotenv()

IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.getenv("IBKR_PORT", "7497"))
IBKR_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "1"))
IBKR_TIMEOUT_S = float(os.getenv("IBKR_CONNECT_TIMEOUT_SECONDS", "6"))


async def test_order_broker() -> None:
    client = create_broker_client()
    print(
        f"Broker config: name={client.name} host={client.host} "
        f"port={client.port} instrument={client.instrument}"
    )

    if hasattr(client, "ping"):
        resp = await client.ping()  # type: ignore[attr-defined]
        print(f"Broker ping: {resp}")
        if not resp.get("success", False):
            raise RuntimeError(f"Broker ping failed: {resp}")
        return

    # Generic socket probe for clients without a non-trading ping endpoint.
    reader, writer = await asyncio.open_connection(client.host, client.port)
    writer.close()
    await writer.wait_closed()
    print("Broker socket reachable")


async def connect_ibkr_with_fallback(ib: IB) -> int:
    port_candidates = [IBKR_PORT, 7497, 7496, 4001, 4002]
    unique_ports: list[int] = []
    for p in port_candidates:
        if p not in unique_ports:
            unique_ports.append(p)

    last_error: Exception | None = None
    for port in unique_ports:
        try:
            print(
                f"Trying IBKR {IBKR_HOST}:{port} "
                f"clientId={IBKR_CLIENT_ID} timeout={IBKR_TIMEOUT_S:.1f}s"
            )
            await ib.connectAsync(
                IBKR_HOST,
                port,
                clientId=IBKR_CLIENT_ID,
                timeout=IBKR_TIMEOUT_S,
            )
            return port
        except Exception as e:
            last_error = e
            print(f"  Failed on {IBKR_HOST}:{port} -> {e}")

    raise RuntimeError(
        "IBKR connection failed on all candidate ports "
        f"{unique_ports} at host {IBKR_HOST}."
    ) from last_error


async def test_ibkr() -> None:
    ib = IB()
    try:
        port = await connect_ibkr_with_fallback(ib)
        print(f"IBKR connected on {IBKR_HOST}:{port}")

        ndx = Index("NDX", "NASDAQ", "USD")
        await ib.qualifyContractsAsync(ndx)

        ticker = ib.reqMktData(ndx, "", False, False)
        deadline = asyncio.get_event_loop().time() + 6
        price = None
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.2)
            v = ticker.marketPrice()
            if v is not None and str(v) != "nan":
                price = float(v)
                break

        if price is None and getattr(ticker, "close", None) is not None:
            price = float(ticker.close)

        if price is None:
            print("IBKR connected, but no NDX price yet (subscription may be delayed).")
        else:
            print(f"IBKR NDX price: {price:.2f}")
    finally:
        if ib.isConnected():
            ib.disconnect()


async def main() -> None:
    await test_order_broker()
    await test_ibkr()
    print("All connections OK")


if __name__ == "__main__":
    asyncio.run(main())
