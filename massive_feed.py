"""
massive_feed.py — massive.com (Polygon.io) live NDX feed

Architecture:
  • WebSocket thread  — streams real-time index ticks; updates live_price only.
  • REST poll thread  — at each 5-min boundary (+3 s buffer) fetches the just-
                        completed official bar from Polygon REST and appends it
                        to self.bars.  Uses official OHLCV, not tick aggregation.
  • Warmup            — same REST endpoint, called once at startup.

Public interface (thread-safe for asyncio main loop reads):
    feed.bars        : list[BarData]  — completed 5-min bars, grows over time
    feed.live_price  : float | None   — latest NDX index value (updated on every tick)
"""

import asyncio
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
UTC   = timezone.utc


# ---------------------------------------------------------------------------
# BarData — same attribute names as ib_insync BarData so bars_to_df() and the
# dedup logic in ndx_live_trader.py work identically for both feed types.
# ---------------------------------------------------------------------------

@dataclass
class BarData:
    date:   datetime   # UTC-aware datetime (timezone.utc)
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float


def _floor_5min_utc(dt: datetime) -> datetime:
    """Truncate a UTC datetime to the start of its 5-minute bar."""
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def _bar_from_agg(a) -> "BarData | None":
    """Convert a massive Agg object to BarData; returns None if data is bad."""
    if a.timestamp is None:
        return None
    o, h, lo, c = a.open, a.high, a.low, a.close
    if any(v is None or float(v) <= 0 for v in (o, h, lo, c)):
        return None
    return BarData(
        date   = datetime.fromtimestamp(a.timestamp / 1000, tz=UTC),
        open   = float(o),
        high   = float(h),
        low    = float(lo),
        close  = float(c),
        volume = float(a.volume) if a.volume is not None else 0.0,
    )


# ---------------------------------------------------------------------------
# REST warmup helper — synchronous, called via asyncio.to_thread()
# ---------------------------------------------------------------------------

def _fetch_warmup_bars_sync(api_key: str, ticker: str, days: int) -> list:
    """
    Fetch historical 5-min bars from the Polygon/massive REST API.
    Returns only fully-completed bars, sorted and deduplicated.
    """
    from massive import RESTClient

    client  = RESTClient(api_key=api_key)
    to_dt   = datetime.now(NY_TZ)
    from_dt = to_dt - timedelta(days=days + 3)   # +3 days buffer for weekends / holidays

    raw = []
    for a in client.list_aggs(
        ticker=ticker,
        multiplier=5,
        timespan="minute",
        from_=from_dt.strftime("%Y-%m-%d"),
        to=to_dt.strftime("%Y-%m-%d"),
        limit=50000,
    ):
        b = _bar_from_agg(a)
        if b is not None:
            raw.append(b)

    # Sort chronologically and deduplicate
    raw.sort(key=lambda b: b.date)
    seen: set = set()
    bars = []
    for b in raw:
        if b.date not in seen:
            seen.add(b.date)
            bars.append(b)

    # Drop any bar whose 5-min period is still in progress
    current_bar_start = _floor_5min_utc(datetime.now(UTC))
    bars = [b for b in bars if b.date < current_bar_start]

    return bars


# ---------------------------------------------------------------------------
# MassiveFeed
# ---------------------------------------------------------------------------

class MassiveFeed:
    """
    Drop-in IBKR data feed replacement using massive.com WebSocket + REST API.

    Two background threads:
      _ws_thread   — WebSocket, updates live_price only (no bar aggregation)
      _poll_thread — REST poll at each 5-min boundary, appends official bars

    Usage:
        feed = MassiveFeed(api_key="...", ticker="I:NDX")
        warmup_bars = await feed.fetch_warmup_bars_async(days=2)
        feed.start()
        bars   = feed.bars    # same list object — grows as bars complete
        ticker = feed         # pass to get_ndx_price() which checks .live_price
    """

    def __init__(self, api_key: str, ticker: str = "I:NDX") -> None:
        self.api_key    = api_key
        self.ticker     = ticker

        # Public, read from asyncio loop (GIL-safe for simple reads)
        self.bars: list[BarData] = []
        self.live_price: float | None = None

        self._lock        = threading.Lock()
        self._stop        = threading.Event()
        self._ws_thread:   threading.Thread | None = None
        self._poll_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_warmup_bars_async(self, days: int = 2) -> list:
        """Async wrapper around the synchronous REST warmup fetch."""
        return await asyncio.to_thread(_fetch_warmup_bars_sync, self.api_key, self.ticker, days)

    def start(self) -> None:
        """Start both background threads (WebSocket + REST poll)."""
        self._stop.clear()
        self._ws_thread = threading.Thread(
            target=self._run_ws, daemon=True, name="MassiveFeed-WS"
        )
        self._poll_thread = threading.Thread(
            target=self._run_poll, daemon=True, name="MassiveFeed-Poll"
        )
        self._ws_thread.start()
        self._poll_thread.start()

    def stop(self) -> None:
        """Signal both background threads to exit."""
        self._stop.set()

    # ------------------------------------------------------------------
    # WebSocket thread — live price only
    # ------------------------------------------------------------------

    def _run_ws(self) -> None:
        """WebSocket loop — updates live_price only, reconnects on failure."""
        from massive import WebSocketClient
        from massive.websocket.models import Feed, Market

        while not self._stop.is_set():
            try:
                client = WebSocketClient(
                    api_key=self.api_key,
                    feed=Feed.RealTime,
                    market=Market.Indices,
                )
                client.subscribe(f"V.{self.ticker}")
                client.run(self._on_msg)
            except Exception:
                if not self._stop.is_set():
                    time.sleep(5)

    def _on_msg(self, msgs) -> None:
        """Update live_price from incoming WebSocket tick. No bar aggregation."""
        for m in msgs:
            ev = getattr(m, "event_type", None) or getattr(m, "ev", None)
            if ev != "V":
                continue
            val = getattr(m, "val", None) or getattr(m, "value", None)
            if val is None:
                continue
            with self._lock:
                self.live_price = float(val)

    # ------------------------------------------------------------------
    # REST poll thread — official completed bars at each 5-min boundary
    # ------------------------------------------------------------------

    def _run_poll(self) -> None:
        """
        At each 5-min boundary (+3 s buffer for Polygon to finalise the bar),
        fetch the just-completed official bar via REST and append to self.bars.
        Retries up to 3 times if the bar isn't available yet.
        """
        from massive import RESTClient

        client = RESTClient(api_key=self.api_key)

        while not self._stop.is_set():
            # Sleep until 3 s past the next 5-min boundary
            now          = datetime.now(UTC)
            bar_floor    = _floor_5min_utc(now)
            next_boundary = bar_floor + timedelta(minutes=5)
            wait_secs    = (next_boundary - now).total_seconds() + 3.0

            # Interruptible sleep in 0.5 s chunks
            deadline = time.monotonic() + wait_secs
            while time.monotonic() < deadline and not self._stop.is_set():
                time.sleep(0.5)

            if self._stop.is_set():
                break

            completed_bar_start = next_boundary - timedelta(minutes=5)

            # Retry loop — Polygon may take a moment to publish the bar
            for attempt in range(3):
                try:
                    bar = self._fetch_one_bar(client, completed_bar_start)
                    if bar is not None:
                        with self._lock:
                            existing = {b.date for b in self.bars}
                            if bar.date not in existing:
                                self.bars.append(bar)
                        break
                except Exception:
                    pass
                if attempt < 2:
                    time.sleep(5)   # wait 5 s before retry

    def _fetch_one_bar(self, client, bar_start_utc: datetime) -> "BarData | None":
        """
        Fetch a single 5-min bar whose UTC start matches bar_start_utc.
        Uses a narrow ±10-min window to avoid pulling excessive data.
        """
        from_dt = (bar_start_utc - timedelta(minutes=5)).strftime("%Y-%m-%d")
        to_dt   = (bar_start_utc + timedelta(minutes=10)).strftime("%Y-%m-%d")

        for a in client.list_aggs(
            ticker=self.ticker,
            multiplier=5,
            timespan="minute",
            from_=from_dt,
            to=to_dt,
            limit=20,
        ):
            b = _bar_from_agg(a)
            if b is not None and b.date == bar_start_utc:
                return b
        return None
