from massive import RESTClient
from datetime import datetime, timezone, timedelta

API_KEY = "uNioMAkg6bkQVWJGVw4HDDoVEp_GwMlF"
TICKER  = "I:NDX"
UTC     = timezone.utc

client  = RESTClient(api_key=API_KEY)
now_utc = datetime.now(UTC)
from_dt = (now_utc - timedelta(days=2)).strftime("%Y-%m-%d")
to_dt   = now_utc.strftime("%Y-%m-%d")

bars = list(client.list_aggs(TICKER, 5, "minute", from_=from_dt, to=to_dt, adjusted=False, sort="desc", limit=20))

print(f"Fetched {len(bars)} bars (desc order, showing last 10)")
print(f"{'Time (UTC)':<22} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>12}")
print("-" * 78)
for a in bars[:10]:
    ts  = datetime.fromtimestamp(a.timestamp / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M")
    vol = str(a.volume) if a.volume is not None else "N/A"
    print(f"{ts:<22} {a.open:>10.2f} {a.high:>10.2f} {a.low:>10.2f} {a.close:>10.2f} {vol:>12}")
