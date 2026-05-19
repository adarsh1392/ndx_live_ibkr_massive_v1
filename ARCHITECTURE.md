# NDX Live Trading Engine — Architecture & Developer Reference

## Table of Contents

1. [Purpose & Overview](#1-purpose--overview)
2. [Repository Layout](#2-repository-layout)
3. [System Architecture Diagram](#3-system-architecture-diagram)
4. [Component Responsibilities](#4-component-responsibilities)
5. [Configuration System](#5-configuration-system)
6. [Startup Sequence](#6-startup-sequence)
7. [Data Pipeline — IBKR Historical & Live Bars](#7-data-pipeline--ibkr-historical--live-bars)
8. [Strategy Core — Signal Detection Logic](#8-strategy-core--signal-detection-logic)
9. [Main Event Loop](#9-main-event-loop)
10. [Entry Trigger](#10-entry-trigger)
11. [Exit Monitoring — SL / TSL / TP](#11-exit-monitoring--sl--tsl--tp)
12. [Broker Abstraction Layer](#12-broker-abstraction-layer)
13. [NinjaTrader TCP Order Server (C#)](#13-ninjatrader-tcp-order-server-c)
14. [Rithmic Bridge Client](#14-rithmic-bridge-client)
15. [Telegram Alerting](#15-telegram-alerting)
16. [Keyboard Control System](#16-keyboard-control-system)
17. [HUD — Real-Time Console Display](#17-hud--real-time-console-display)
18. [Crash Recovery & Position State](#18-crash-recovery--position-state)
19. [Trade Logging](#19-trade-logging)
20. [Threading & Concurrency Model](#20-threading--concurrency-model)
21. [All Config Parameters Reference](#21-all-config-parameters-reference)
22. [Running the Engine](#22-running-the-engine)
23. [Known Behaviours & Gotchas](#23-known-behaviours--gotchas)

---

## 1. Purpose & Overview

The **NDX Live Trading Engine** is a Python asyncio application that:

- Reads 5-minute bar data and a real-time tick price for the **NDX cash index** from Interactive Brokers (IBKR) via `ib_insync`.
- Applies the **EMA-14 bounce strategy** to each completed bar using `strategy_core.py`.
- When a signal fires, it executes a **market order** for **MNQ** (Micro Nasdaq-100 futures) via either NinjaTrader 8 (TCP socket, default) or a Rithmic bridge process.
- Monitors the open position every tick and applies a **stop-loss → trailing stop-loss → take-profit** ladder entirely in Python code (not via native broker stops).
- Sends **Telegram alerts** to multiple bots/chat IDs on signal detection.
- Writes **daily log files** and **CSV trade records**.
- Provides a **non-blocking keyboard interface** (S / E / Q) and a live **HUD** line that overwrites itself on the console.

The instrument split is intentional:
| Feed | Instrument | Why |
|------|-----------|-----|
| NDX cash index (IBKR) | NDX | Cleaner price, no futures roll distortion |
| Execution (NinjaTrader / Rithmic) | MNQ | Micro futures, cheaper commissions |

All trades are executed as **market orders at signal-determined price levels** — there are no native broker stop/limit orders being managed; the Python code evaluates SL/TP on every tick.

---

## 2. Repository Layout

```
NDX_liveengine_final-1May26/
│
├── ndx_live_trader.py       ← MAIN ENGINE (entry point)
├── strategy_core.py         ← EMA bounce signal detection + indicators
├── config_loader.py         ← YAML config loader with deep-merge + lru_cache
├── config.yaml              ← All user-tunable parameters (no .env needed)
│
├── broker_client.py         ← Broker abstraction (Protocol + factory)
├── ninja_trader_client.py   ← NinjaTrader 8 async TCP client
├── rithmic_client.py        ← Rithmic local-bridge async TCP client
│
├── PythonOrderServer.cs     ← NinjaTrader 8 NinjaScript strategy (C#, server side)
│
├── test_connections.py      ← Pre-flight connectivity tester (run before live)
├── requirements.txt         ← pip dependencies
│
├── logs/                    ← Daily log files: log_mnq_YYYY-MM-DD.log
├── trades/                  ← Daily trade CSVs: trades_mnq_YYYY-MM-DD.csv
└── position_state.json      ← Written on every trade open/update; deleted on close
                                (used for crash recovery; absent when flat)
```

---

## 3. System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ndx_live_trader.py                                 │
│                                                                              │
│  ┌──────────────────┐    Stage 1 (warmup)    ┌───────────────────────────┐  │
│  │                  │ ──reqHistoricalData──▶  │  IBKR TWS / IB Gateway   │  │
│  │   ib_insync IB   │    keepUpToDate=False   │  (paper or live)         │  │
│  │   object         │ ──reqHistoricalData──▶  │  NDX Index contract      │  │
│  │                  │    keepUpToDate=True    │  5-min bars + tick feed  │  │
│  │   (asyncio       │ ◀──bars[] updated────   └───────────────────────────┘  │
│  │    event-loop)   │ ◀──ticker.marketPrice() (real-time tick)               │
│  └────────┬─────────┘                                                        │
│           │ new bar                                                          │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │  strategy_core   │  compute_indicators()  →  EMA14, ATR14, slope_atr     │
│  │  .py             │  get_latest_signal()   →  EMA bounce pattern check    │
│  └────────┬─────────┘                                                        │
│           │ signal dict  {direction, entry, sl, target, risk, qty}          │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │  Entry trigger   │  live_price crosses entry_level                       │
│  │  (per-tick check)│                                                        │
│  └────────┬─────────┘                                                        │
│           │ ORDER                                                            │
│           ▼                                                                  │
│  ┌──────────────────┐   ENTRY|DIR|QTY  ┌──────────────────────────────────┐ │
│  │  broker_client   │ ───────────────▶ │  NinjaTrader 8 (TCP :5555)       │ │
│  │  .py  (factory)  │                  │  PythonOrderServer.cs strategy   │ │
│  │                  │   EXIT|DIR|QTY   │  — MNQ market orders             │ │
│  │  NinjaTrader     │ ───────────────▶ │  — FLATTEN                       │ │
│  │  or Rithmic      │ ◀── OK / ERROR   │  — LASTEXEC (fill data)          │ │
│  └──────────────────┘                  └──────────────────────────────────┘ │
│                                                                              │
│  ┌──────────────────┐  exit condition met                                    │
│  │  Exit monitor    │ ─────────────────▶  safe_place_exit() / safe_flatten()│
│  │  (per-tick)      │                                                        │
│  └──────────────────┘                                                        │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Telegram  (httpx async)  ─▶  Bot 1 → chat_id_A, chat_id_B           │  │
│  │                           ─▶  Bot 2 → chat_id_C                       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────┐    ┌────────────────────────────────────────┐  │
│  │  Keyboard daemon thread  │    │  HUD (overwrites single console line)  │  │
│  │  msvcrt.getch() Windows  │    │  [NDX]/ 21045.25 | 10:15:30 | FLAT    │  │
│  │  S / E / Q               │    └────────────────────────────────────────┘  │
│  └──────────────────────────┘                                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Responsibilities

### `ndx_live_trader.py` — Main Engine
- Entry point (`asyncio.run(run())`).
- Loads config, sets up all state, starts keyboard thread.
- Manages IBKR connection with port fallback.
- Drives the two-stage historical bar fetch.
- Runs the main `while True` event loop at 10ms poll (`asyncio.sleep(0.01)`).
- All order placement, position tracking, SL/TSL/TP evaluation, and EOD handling live here.

### `strategy_core.py` — Signal Detection
- Stateless functions: `compute_indicators()` and `get_latest_signal()`.
- No I/O except log file writes for rejected signals.
- Takes a raw OHLCV DataFrame, returns a signal dict or `None`.

### `config_loader.py` — Config
- Reads `config.yaml` once per process (cached via `@lru_cache(maxsize=1)`).
- Deep-merges user YAML over `DEFAULT_CONFIG` so missing keys always have sane fallbacks.

### `broker_client.py` — Factory + Protocol
- Defines the `BrokerClient` Protocol (structural typing).
- `create_broker_client()` reads `os.environ["BROKER"]` and returns the correct client.

### `ninja_trader_client.py` — NinjaTrader TCP
- Opens a new TCP connection per command (short-lived).
- Pipe-delimited text protocol: `ENTRY|LONG|1\n` → `OK: ENTRY LONG 1 MNQ 06-26\n`.

### `rithmic_client.py` — Rithmic JSON bridge
- JSON lines over TCP to a local Rithmic bridge process.
- Same `place_entry` / `place_exit` / `flatten` interface as NinjaTrader client.

### `PythonOrderServer.cs` — NinjaTrader 8 AddOn
- NinjaScript **Strategy** (not an indicator) so it has account/order access.
- Runs a `TcpListener` on a background thread inside NinjaTrader 8.
- Dispatches one command per TCP connection; spawns a thread per client.
- Captures fill data via `OnExecutionUpdate` callback and serves it via `LASTEXEC`.

### `test_connections.py` — Pre-flight Tester
- Run this before starting the live engine to verify both IBKR and broker TCP are reachable.
- Does not place live orders; sends PING to NinjaTrader.

---

## 5. Configuration System

**File**: `config.yaml` (same directory as the engine)

**Loading chain**:
```
config.yaml  ──yaml.safe_load──▶  user_cfg dict
                                       │
DEFAULT_CONFIG (hardcoded in           │
config_loader.py)  ──_deep_merge──▶  final merged dict  ──@lru_cache──▶  get_config()
```

`_deep_merge` recurses into nested dicts; scalar values in user YAML override defaults.

**`get_config()` is called at module-level** in `ndx_live_trader.py` and `strategy_core.py`. Both get the same cached dict. If you need to reload config mid-process, call `get_config.cache_clear()` then `get_config()` again.

The engine sets `os.environ.setdefault()` keys from config so that `broker_client.py` (which reads env vars) stays decoupled from the config system:

```python
os.environ.setdefault("BROKER",         _broker_name)
os.environ.setdefault("NT_HOST",        _ntcfg["host"])
os.environ.setdefault("NT_PORT",        str(_ntcfg["port"]))
os.environ.setdefault("NT_INSTRUMENT",  _ntcfg["instrument"])
```

---

## 6. Startup Sequence

```
ndx_live_trader.py
│
├── _check_dependencies()          ← import-time: abort with install instructions if missing packages
├── colorama_init()
├── _ensure_dirs()                 ← create logs/ and trades/ if absent
├── print_banner()                 ← 6-line blue box
│
├── load config (get_config())
├── start keyboard daemon thread   ← non-blocking S/E/Q input
│
├── create_broker_client()         ← NinjaTrader or Rithmic from config
├── ping_ninja()                   ← TCP PING to NinjaTrader (warning only if fails)
│
├── load_position_state()          ← crash recovery: warn if position_state.json exists
├── STARTUP_FLATTEN? → safe_flatten()
├── clear_position_state()
│
├── connect_ibkr()                 ← tries ports [config_port, 7497, 7496, 4001, 4002]
│    └─ ConnectionError → print red error box → return (clean exit)
│
├── qualify NDX contract           ← ib.qualifyContractsAsync(Index("NDX","NASDAQ","USD"))
│
├── Stage 1: reqHistoricalDataAsync(keepUpToDate=False, "2 D")   ← warmup bars
│    └─ retry up to 3x with 5s delay on failure
│    └─ success: logs "✔ Warmup bars loaded: N bars — EMA/ATR ready"
│
├── asyncio.sleep(5)               ← IBKR pacing: mandatory gap between two historical requests
│
├── Stage 2: reqHistoricalDataAsync(keepUpToDate=True, "14400 S")  ← live bar stream
│    └─ Error 162 here is NORMAL (IBKR switching from historical backfill to live mode)
│    └─ logs "Live bar stream active: N recent bars"
│
├── reqMktData(ndx)                ← real-time tick subscription
│
├── print session info (mode, windows, EOD time)
├── print_kb_help()
└── enter main event loop
```

---

## 7. Data Pipeline — IBKR Historical & Live Bars

### Why Two Stages?

EMA-14 and ATR-14 need at least ~14–28 bars of history to produce valid values. Without a warmup fetch, the indicators would be `NaN` for the first 14 bars of the session.

**Stage 1 — Warmup (`keepUpToDate=False`, `"2 D"` duration)**

- Fetches ~2 trading days of completed 5-min bars (approx. 96–160 bars).
- This request is simple and reliable; it returns a static list and terminates.
- Stored in `warmup_bars` list. The date of the last bar is saved as `_warmup_cutoff`.

**Stage 2 — Live Stream (`keepUpToDate=True`, `"14400 S"` = 4 hours)**

- `keepUpToDate=True` causes IBKR to:
  1. First backfill the requested duration (4 hours of 5-min bars).
  2. Then switch the subscription to **live update mode** — new completed bars are automatically appended to `bars[]` as they close.
- **Error 162** fires during this transition. It is informational, not an error; the subscription remains active.
- The `bars` list is a live `BarDataList` object managed by `ib_insync`. Its length increases by 1 every 5 minutes as each bar completes.
- The engine detects new bars by comparing `len(bars)` to `prev_bar_count` on each loop tick.

**5-Second Pacing Gap**

IBKR enforces a rate limit on historical data requests. Two back-to-back requests will cause the second one to be throttled (resulting in timeout / empty bars). The `await asyncio.sleep(5)` between Stage 1 and Stage 2 satisfies this constraint.

### Bar Merge on Each Completed Bar

Every time a new bar is detected, the engine merges warmup and live bars to maximise indicator lookback depth:

```python
live_list  = list(bars[:-1])        # all completed live bars (exclude in-progress bar)
live_dates = {b.date for b in live_list}
extra      = [b for b in warmup_bars if b.date not in live_dates]  # non-overlapping warmup
combined   = sorted(extra + live_list, key=lambda b: b.date)
```

`bars_to_df(combined)` converts to a pandas DataFrame with columns:
`datetime_ny, Open, High, Low, Close, Volume`

The `datetime_ny` column is timezone-converted to America/New_York and stripped of tzinfo (naive NY time) for consistency with strategy time-window checks.

### Real-Time Price Tick

`ib.reqMktData(ndx, "", False, False)` returns a `Ticker` object. The engine reads:
1. `ticker.marketPrice()` — last trade price (preferred).
2. `ticker.close` — fallback if market price is NaN (e.g., pre-market).

Price is polled on every `asyncio.sleep(0.01)` iteration (100 Hz nominal, throttled by IB tick rate). If `None` is returned, the loop iteration is skipped (`continue`).

---

## 8. Strategy Core — Signal Detection Logic

### Indicators (`compute_indicators`)

| Indicator | Formula | Purpose |
|-----------|---------|---------|
| `ema` | EWM span=`ema_length` (default 14) on Close | Dynamic support/resistance level |
| `atr14` | 14-bar Wilder ATR (rolling mean of True Range) | Volatility normalisation |
| `slope_atr` | `(ema[i] - ema[i - ema_bar_difference]) / atr14` | EMA angle in ATR-normalised units |
| `body` | `abs(Close - Open)` | Candle body size filter |

### Signal Pattern (`get_latest_signal`)

The strategy detects an **EMA bounce** — price reaches through the EMA and then closes back on the correct side, suggesting the EMA acted as support/resistance.

**SHORT Signal Conditions** (all must be true):
```
slope_atr < short_slope             ← EMA trending downward (bearish regime)
Close > Open                        ← Candle is bullish (counter-trend bounce)
Low < ema                           ← Candle wicked below EMA
(ema - High) < bounce_distance      ← High didn't close far above EMA
Close < ema                         ← But closed back below EMA (rejection)
body >= min_body_points             ← Meaningful candle body
risk (sl - entry) within risk_cap   ← Position sizing guard
```

**LONG Signal Conditions** (mirror image):
```
slope_atr > long_slope              ← EMA trending upward (bullish regime)
Close < Open                        ← Candle is bearish (counter-trend bounce)
High > ema                          ← Wicked above EMA
(Low - ema) < bounce_distance       ← Didn't close far below EMA
Close > ema                         ← Closed back above EMA (rejection)
body >= min_body_points
risk (entry - sl) within risk_cap
```

### Signal Levels Calculation

**SHORT**:
- `entry = Low - entry_offset_points` (sell stop below the candle low)
- `sl    = max(ema, entry + sl_atr_mult × ATR14)` (stop above the EMA or ATR-based)
- `risk  = sl - entry`
- `target = entry - rr_multiple × risk`

**LONG**:
- `entry = High + entry_offset_points` (buy stop above the candle high)
- `sl    = min(ema, entry - sl_atr_mult × ATR14)` (stop below the EMA or ATR-based)
- `risk  = entry - sl`
- `target = entry + rr_multiple × risk`

### Steep-Angle Conditions (Disabled)

Two optional enhancements exist in the code but are **deliberately disabled**:
- `_bounce_distance_points()` — was going to expand bounce tolerance 1.8× in a steep trend; now returns `atr14 × normal_mult` unconditionally.
- `_allowed_risk_cap_points()` — was going to allow 80-pt risk cap in steep trends; now returns `(normal_cap, False)` unconditionally.

To re-enable, restore the conditional logic using `strong_trend_threshold`, `strong_trend_mult`, `steep_risk_atr_mult`, and `steep_risk_hard_cap` parameters.

### Signal Deduplication

The engine tracks `last_signal_bar_dt`. A signal is ignored if:
- `sig["signal_dt"]` date is not today (stale historical signal from warmup bars).
- `sig["signal_dt"]` equals `last_signal_bar_dt` (same bar, already processed).

### Risk Rejection Logging

Signals rejected due to risk > cap are logged once per (datetime, direction) pair to avoid log spam. The dedup set `_risk_reject_seen` is an in-memory set (resets on process restart).

---

## 9. Main Event Loop

```python
while True:
    await asyncio.sleep(0.01)            # yield control, ~100 Hz

    now_ny = datetime.now(NY_TZ)

    # 1. Daily reset (trade count, EOD arm)
    # 2. Keyboard command processing (queue drained synchronously)
    # 3. EOD flatten check (15:30 NY)
    # 4. Get live NDX price (skip iteration if None)
    # 5. Tick spinner
    # 6. Periodic heartbeat log (every STATUS_UPDATE_SECONDS)
    # 7. Bar detection → indicator computation → signal detection
    # 8. Entry trigger (pending signal vs live price)
    # 9. Exit monitoring (SL / TSL / TP vs live price)
    # 10. HUD render (overwriting \r line, suppressed during E/Q prompt)
```

The 10ms sleep is the minimum yield interval. Actual loop rate is bounded by how fast asyncio can schedule the coroutine, the IB tick rate (~few hundred ms between ticks), and bar close events (every 5 minutes).

---

## 10. Entry Trigger

After a signal is detected and stored in `pending_signal`, the engine checks on every tick:

```python
triggered = (
    (direction == "SHORT" and live_price <= entry_level) or
    (direction == "LONG"  and live_price >= entry_level)
)
```

Entry only fires if:
- `pending_signal is not None` (a signal is queued)
- `algo_position is None` (not already in a trade)
- `in_trade_window` (time is within `trade_start` → `session_end`)

On trigger:
1. Logs entry details with timestamp.
2. Calls `safe_place_entry(broker_client, direction, qty)`.
3. If NinjaTrader: polls `LASTEXEC` for fill data up to 2.5s to compute entry latency.
4. Stores position state dict in memory and persists to `position_state.json`.
5. Increments `trades_today_ny`.
6. Clears `pending_signal`.

**Position state dict**:
```python
{
    "direction":  "LONG" | "SHORT",
    "entry_ndx":  float,     # actual NDX tick price at trigger moment
    "sl":         float,     # current stop-loss (moves up with TSL)
    "target":     float,
    "risk":       float,     # original risk in points (used for TSL R-multiple calc)
    "qty":        int,
    "trail_step": int,       # 0=initial SL, 1=breakeven+, 2=2R, 3=3R
    "entry_time": str,       # ISO timestamp
}
```

---

## 11. Exit Monitoring — SL / TSL / TP

On every tick with an open position:

### Trailing Stop Update

The TSL uses an R-multiple staircase:

| Condition (LONG) | New SL | `trail_step` |
|-----------------|--------|-------------|
| `fr >= 2.0R` | `entry + 0.5R` (breakeven+) | 1 |
| `fr >= 3.0R` | `entry + 2.0R` | 2 |
| `fr >= 4.0R` | `entry + 3.0R` | 3 |

where `fr = (live_price - entry_ndx) / risk` for LONG, mirror for SHORT.

SL only ever **moves in the favourable direction** (`max(sl, new_sl)` for LONG, `min(sl, new_sl)` for SHORT). Updated state is immediately persisted to `position_state.json`.

### Exit Check

```python
# LONG
if live_price <= sl:     exit_reason = "TSL" if trail_step > 0 else "SL"
if live_price >= target: exit_reason = "TP"

# SHORT
if live_price >= sl:     exit_reason = "TSL" if trail_step > 0 else "SL"
if live_price <= target: exit_reason = "TP"
```

On exit:
1. Calls `safe_place_exit(broker_client, direction, qty)`.
2. Polls fill data from NinjaTrader for latency tracking.
3. Appends trade record to CSV.
4. Clears `algo_position` and deletes `position_state.json`.

---

## 12. Broker Abstraction Layer

`broker_client.py` defines a **structural Protocol** (`typing.Protocol`):

```python
class BrokerClient(Protocol):
    name: str
    host: str
    port: int
    instrument: str

    async def place_entry(self, direction: str, qty: int) -> dict: ...
    async def place_exit(self,  direction: str, qty: int) -> dict: ...
    async def flatten(self) -> dict: ...
```

Both `NinjaTraderClient` and `RithmicClient` satisfy this protocol without explicit inheritance.

`create_broker_client()` is a factory that reads `os.environ["BROKER"]` and instantiates the correct client. The environment variables are set from config by `ndx_live_trader.py` before calling the factory.

All order methods are wrapped in `safe_place_entry()` / `safe_place_exit()` / `safe_flatten()` which catch every exception and return `{"success": False, "message": str(exc)}` instead of crashing the engine.

---

## 13. NinjaTrader TCP Order Server (C#)

**File**: `PythonOrderServer.cs` — install to NinjaTrader 8 Strategies folder.

### Setup
1. Copy to `Documents\NinjaTrader 8\bin\Custom\Strategies\PythonOrderServer.cs`.
2. Compile in NinjaScript Editor (F5).
3. Add strategy to a chart of MNQ (any timeframe).
4. Set **Account** and **Port** in the strategy properties (default 5557, config uses 5555).
5. Verify Output tab shows: `[PythonOrderServer] Listening on 127.0.0.1:5555`.

### Protocol (text, pipe-delimited)

| Command | Action | Response |
|---------|--------|---------|
| `PING` | No-op connectivity test | `OK: PONG MNQ 06-26` |
| `ENTRY\|LONG\|1` | BUY 1 MNQ at market | `OK: ENTRY LONG 1 MNQ 06-26` |
| `ENTRY\|SHORT\|1` | SELL SHORT 1 MNQ | `OK: ENTRY SHORT 1 MNQ 06-26` |
| `EXIT\|LONG\|1` | SELL 1 MNQ (close long) | `OK: EXIT LONG 1 MNQ 06-26` |
| `EXIT\|SHORT\|1` | BUY TO COVER 1 MNQ | `OK: EXIT SHORT 1 MNQ 06-26` |
| `FLATTEN` | Flatten entire position | `OK: FLATTEN MNQ 06-26` |
| `PRICE` | Market snapshot (bid/ask/last) | `OK: PRICE\|epoch_ms=...\|last=...\|bid=...\|ask=...` |
| `LASTEXEC` | Most recent fill details | `OK: LASTEXEC\|epoch_ms=...\|price=...\|qty=...\|action=...` |

Any command returns `ERROR: ...` on failure.

### Server Architecture (C#)
- `TcpListener` on loopback, port = `TcpPort` property.
- Background `Thread` running `AcceptLoop` (one TCP connection = one thread).
- Orders submitted via `Account.CreateOrder()` + `Account.Submit()` (unmanaged mode).
- `OnExecutionUpdate()` captures fill details under `_execLock` for `LASTEXEC` queries.

---

## 14. Rithmic Bridge Client

`rithmic_client.py` communicates with a **separate local bridge process** (not included in this repository) that wraps the Rithmic API. The bridge must be started separately; the engine connects to it via JSON lines on TCP.

Payload format:
```json
{
  "action": "ENTRY",
  "direction": "LONG",
  "qty": 1,
  "instrument": "MNQM6",
  "account_id": "...",
  "exchange": "CME",
  "gateway": ""
}
```

Expected response: JSON dict with at minimum `{"success": true/false, "message": "..."}`.

To switch to Rithmic: set `broker.name: rithmic` in `config.yaml` and fill in `rithmic` section fields.

---

## 15. Telegram Alerting

### Multi-Bot Support

`config.yaml` supports a `bots[]` list, each with a token and list of chat_ids:

```yaml
telegram:
  enabled: true
  bots:
    - token: "BOT1_TOKEN"
      chat_ids: ["chat_id_A", "group_id_B"]
    - token: "BOT2_TOKEN"
      chat_ids: ["chat_id_C"]
```

Legacy single-bot format (`bot_token` + top-level `chat_ids`) is also supported via fallback in `_load_telegram_bots()`.

### Sending

`send_signal_telegram(sig, qty)` is called once per new signal (not on entry/exit, only detection). It:
1. Iterates all bots with non-empty `chat_ids`.
2. Opens a single `httpx.AsyncClient` (timeout 10s, no proxy trust).
3. POSTs to `https://api.telegram.org/bot{TOKEN}/sendMessage` for each (bot × chat_id) pair.
4. Logs success/failure per chat_id.
5. Never raises — all errors are caught and logged.

Message format:
```
📡 NDX signal candle detected

Time     : 2026-05-02 10:15:00 NY
Direction: LONG
Entry    : 21050.00
Stop     : 20990.00
Target   : 21350.00
Risk     : 60.00 pts
Qty      : 1
```

---

## 16. Keyboard Control System

### Design Constraints

On Windows with asyncio `ProactorEventLoop`, `input()` blocks the thread and cannot be interrupted by the event loop. The solution is a **daemon thread** that reads keyboard input independently and communicates with the async loop via `asyncio.Queue`.

### Keyboard Thread (`_kb_thread_fn`)

**Windows** (uses `msvcrt`):
- Polls `msvcrt.kbhit()` every 50ms.
- Reads one byte with `msvcrt.getch()` — no Enter needed for S.
- For E and Q, calls `_kb_confirm_win(key)` before queuing.

**Linux/Mac** (uses `termios` + `select`):
- Sets terminal to `cbreak` mode (no line buffering).
- Polls `select.select()` for stdin with 50ms timeout.
- For E and Q, restores canonical mode, reads a full `readline()`, then re-applies `cbreak`.

### E/Q Confirmation (`_kb_confirm_win`)

To prevent accidental exits:
1. Sets `_confirm_active = True` (freezes HUD overwrite — see §17).
2. Clears the HUD line with `\r{80 spaces}\r\n`.
3. Prompts: `Confirm: type 'exit' and press Enter (or press Enter to cancel)`.
4. Reads chars via `msvcrt.getch()`, echoing each, supporting backspace.
5. On Enter: compares typed string to the required word. Returns `True` only on exact match.
6. In a `try/finally` block: always resets `_confirm_active = False`.

### Command Handling (async loop side)

```python
# Drain queue synchronously each loop iteration
while not _kb_queue.empty():
    cmd = _kb_queue.get_nowait()
    if cmd == "s": print_position_status(...)
    elif cmd == "e": manual_exit_position()
    elif cmd == "q": return   # exits run() cleanly
```

---

## 17. HUD — Real-Time Console Display

The HUD overwrites itself on a single terminal line using `\r` (carriage return without newline):

```
[NDX]/ 21045.25 | 10:15:30 | trades=0/2 | FLAT | pend=—
```

Format: `\r[NDX]{spinner} {price} | [{time}] | trades={n}/{max} | {pos_str} | pend={pend_str}`

**Spinner**: 4-frame (`| / - \`) animated at max 1 frame per 250ms using a monotonic clock gate + threading lock. Advances only when a real NDX tick is received.

**Padding**: `last_hud_len` tracks the character length of the previous HUD string. If the new string is shorter, trailing spaces are appended to prevent ghost characters.

**Guard**: HUD print is suppressed when `_confirm_active = True` (during E/Q confirmation input) to prevent the `\r` from overwriting the confirmation prompt.

**Position string** when in a trade:
```
LONG entry=21050.0  sl=20990.0  tgt=21350.0  R=0.75
```

---

## 18. Crash Recovery & Position State

`position_state.json` is written to disk on:
- Trade entry (initial write)
- Trailing stop update (SL price updated)

Deleted on:
- Trade exit (SL / TSL / TP / manual E key)
- EOD flatten

At startup, if `position_state.json` exists, the engine:
1. Logs a `CRASH RECOVERY` warning showing the saved state.
2. If `startup_flatten: true` in config, sends a FLATTEN command to clear any open futures position.
3. Deletes the file (`clear_position_state()`).

**Note**: The engine does **not** re-enter the saved trade. Recovery means alerting the developer and optionally flattening the broker position. The Python-side `algo_position` starts as `None`.

---

## 19. Trade Logging

### Console + File Log (`log()`)

Every `log(msg)` call:
1. Appends `msg + "\n"` to `logs/log_mnq_YYYY-MM-DD.log` (NY date).
2. Calls `print(msg, flush=True)`.

Important log events:
- `BAR CLOSED` — every 5-min bar close with OHLC + EMA/ATR/slope/bounce dist/risk cap.
- `SIGNAL` — detected signal levels.
- `ENTRY TRIGGERED` — entry level crossed, order sent.
- `ENTRY LATENCY` — trigger→fill and submit→fill ms (when NinjaTrader fill captured).
- `TSL MOVED` — trailing stop updated.
- `EXIT` — exit reason (SL/TSL/TP), direction, PnL.
- `HEARTBEAT` — periodic status every `status_update_seconds`.

### Trade CSV (`append_trade_log()`)

Appended to `trades/trades_mnq_YYYY-MM-DD.csv` (date = entry date).

Columns: `entry_time, exit_time, side, entry_ndx, exit_ndx, exit_reason, pnl_pts`

All exits write here: SL, TSL, TP, and manual E key.

---

## 20. Threading & Concurrency Model

```
Main thread
  └─ asyncio event loop (ProactorEventLoop on Windows, default SelectorEventLoop on Linux)
       ├─ run() coroutine           ← main trading logic
       ├─ IBKR ib_insync callbacks  ← bar updates, tick updates (injected into loop)
       ├─ send_signal_telegram()    ← awaited in-loop (httpx async)
       └─ safe_place_entry/exit()   ← awaited in-loop (asyncio TCP)

Daemon thread (keyboard)
  └─ _kb_thread_fn()
       ├─ msvcrt.kbhit() poll (Windows)
       ├─ _kb_confirm_win() when E or Q pressed
       └─ asyncio.run_coroutine_threadsafe(_kb_queue.put(k), loop)
              ↑ thread-safe bridge back to the event loop queue
```

**No shared mutable state** is accessed from the keyboard thread except:
- `_confirm_active` (bool, set/cleared only by keyboard thread; read by main loop for HUD guard — race is benign, worst case is one HUD frame overlapping prompt).
- `_spinner_idx` / `_spinner_last_tick` (protected by `_spinner_lock`).

All business state (`algo_position`, `pending_signal`, `trades_today_ny`) is only accessed from the asyncio event loop coroutine.

---

## 21. All Config Parameters Reference

### `broker`
| Key | Values | Description |
|-----|--------|-------------|
| `name` | `ninjatrader` \| `rithmic` | Which order execution backend to use |

### `ninjatrader`
| Key | Default | Description |
|-----|---------|-------------|
| `host` | `"127.0.0.1"` | NinjaTrader machine IP |
| `port` | `5555` | TCP port of PythonOrderServer strategy |
| `instrument` | `"MNQ 06-26"` | Instrument name as shown in NinjaTrader |

### `rithmic`
| Key | Default | Description |
|-----|---------|-------------|
| `host` | `"127.0.0.1"` | Rithmic bridge process IP |
| `port` | `6500` | Rithmic bridge TCP port |
| `instrument` | `"MNQM6"` | Rithmic instrument code |
| `account_id` | `""` | Rithmic account ID |
| `exchange` | `"CME"` | Exchange code |
| `gateway` | `""` | Rithmic gateway (if required) |

### `ibkr`
| Key | Default | Description |
|-----|---------|-------------|
| `host` | `"127.0.0.1"` | TWS / IB Gateway IP |
| `port` | `7497` | Paper TWS=7497, Live TWS=7496, Gateway live=4001 |
| `client_id` | `99` | Must be unique per simultaneous IBKR API connection |
| `connect_timeout_seconds` | `6` | Per-port connection attempt timeout |

### `execution`
| Key | Default | Description |
|-----|---------|-------------|
| `enable_orders` | `true` | `false` = no TCP commands sent to broker |
| `dry_run` | `false` | `true` = log orders only, no TCP commands |
| `startup_flatten` | `false` | `true` = FLATTEN command at startup |

> Orders are only sent when `enable_orders=true` AND `dry_run=false`.

### `strategy`
| Key | Default | Description |
|-----|---------|-------------|
| `ema_length` | `14` | EMA period (bars) |
| `ema_bar_difference` | `6` | Bars over which EMA slope is measured |
| `short_slope` | `-0.4` | slope_atr threshold for bearish regime |
| `long_slope` | `0.2` | slope_atr threshold for bullish regime |
| `bounce_distance_atr_mult` | `1.2` | Max allowed candle proximity to EMA (× ATR) |
| `sl_atr_mult` | `0.8` | SL distance = `sl_atr_mult × ATR14` |
| `entry_offset_points` | `5.0` | Entry level offset beyond candle extremes (pts) |
| `risk_cap_points` | `60.0` | Maximum allowed risk per trade (pts); signal rejected if exceeded |
| `rr_multiple` | `5.0` | Target = `entry ± rr_multiple × risk` |
| `signal_start` | `"09:30"` | Earliest time to detect a signal (NY) |
| `trade_start` | `"10:01"` | Earliest time to trigger an entry (NY) |
| `session_end` | `"15:00"` | Latest time for signals and entries (NY) |
| `max_trades_per_day` | `2` | New signals suppressed after this count |
| `min_body_points` | `5.0` | Minimum candle body size for signal validity (pts) |

### `hud`
| Key | Default | Description |
|-----|---------|-------------|
| `status_update_seconds` | `300` | Heartbeat log frequency (seconds) |

### `orders`
| Key | Default | Description |
|-----|---------|-------------|
| `order_qty` | `1` | Default lot size when `qty_from_risk()` returns 1+ |
| `flatten_position_at_end` | `true` | EOD flatten at 15:30 NY (read but logic is hardcoded to 15:30) |

### `telegram`
| Key | Description |
|-----|-------------|
| `enabled` | `true` to send Telegram messages |
| `bots[]` | List of bot objects, each with `token` and `chat_ids[]` |
| `bots[].token` | Telegram Bot API token |
| `bots[].chat_ids` | List of chat IDs (strings; negative for groups) |

---

## 22. Running the Engine

### Prerequisites

1. **Interactive Brokers TWS or IB Gateway** running and logged in.
   - Enable: *Edit → Global Configuration → API → Settings → Enable ActiveX and Socket Clients*.
   - Disable: *Read-Only API*.
   - Confirm port matches `ibkr.port` in config.

2. **NinjaTrader 8** with `PythonOrderServer` strategy active on an MNQ chart.
   - Port must match `ninjatrader.port` in config.

3. **Python environment** with all requirements installed:
   ```powershell
   & "C:\Users\adars\algos\.venv\Scripts\python.exe" -m pip install -r requirements.txt
   ```

### Pre-flight Test

```powershell
cd "C:\Users\adars\algos\NDX_live_IBKR_rithmic\NDX_liveengine_final-1May26"
& "C:\Users\adars\algos\.venv\Scripts\python.exe" test_connections.py
```

Expected output:
```
Broker config: name=ninjatrader host=127.0.0.1 port=5555 instrument=MNQ 06-26
Broker ping: {'success': True, 'message': 'OK: PONG MNQ 06-26'}
IBKR NDX price: 21045.50
All connections OK
```

### Start Engine

```powershell
cd "C:\Users\adars\algos\NDX_live_IBKR_rithmic\NDX_liveengine_final-1May26"
& "C:\Users\adars\algos\.venv\Scripts\python.exe" ndx_live_trader.py
```

### Keyboard Controls at Runtime

| Key | Action |
|-----|--------|
| `S` | Print current position status box |
| `E` | Manually exit open position (type `exit` to confirm) |
| `Q` | Quit the engine without squaring off (type `quit` to confirm) |
| `Ctrl+C` | Emergency stop — auto-flattens position then exits |

---

## 23. Known Behaviours & Gotchas

### IBKR Error 162
Normal and expected when using `keepUpToDate=True`. It fires once, when IBKR finishes the historical backfill portion and activates live streaming. The `bars` list continues to update normally. **Not an error**.

### IBKR Pacing Violation
Two `reqHistoricalDataAsync` calls in quick succession will cause the second to fail or return empty. The `asyncio.sleep(5)` between Stage 1 and Stage 2 is mandatory. If increased reliability is needed, raise this to 10–15s.

### NinjaTrader PING Warning
If NinjaTrader is not running or the strategy is not active, the startup `ping_ninja()` will log a warning but **the engine continues running**. Orders will fail silently (caught by `safe_place_entry` try/except) and log `ORDER ERROR`.

### `whatToShow="TRADES"` for NDX
NDX is a cash index; it does not actually trade. IBKR may return bars for `whatToShow="TRADES"` by using the composite last price. If bars are empty with TRADES, try `whatToShow="MIDPOINT"`.

### Position State vs Broker State
The engine tracks position entirely in Python (`algo_position` dict + `position_state.json`). If the broker position and Python state drift out of sync (e.g., manual order in NinjaTrader), the engine will continue managing based on its own state. Use `STARTUP_FLATTEN=true` + restart to resync.

### No Native Broker Stops
All SL/TSL/TP management is in Python, evaluated on every NDX tick. If the engine crashes while in a trade, the MNQ position remains open at the broker with **no protective stops**. Set `startup_flatten: true` in config and restart immediately to flatten.

### Windows `msvcrt` vs `input()`
`input()` on Windows asyncio `ProactorEventLoop` blocks the thread and causes E/Q to appear to hang. The engine uses `msvcrt.getch()` char-by-char to avoid this. Do not replace with `input()`.

### `_confirm_active` Race Condition
The HUD guard `if not _confirm_active` is a best-effort suppression. There is a small window (one event loop tick) where a HUD write could race with the confirmation prompt. This is cosmetic only — the confirmation logic is not affected.

### Log File Interleaving
The HUD line uses `print(..., end="", flush=True)` without a newline. Log file writes use `\n`. If a log message fires mid-HUD, the console may show log text on the same line as the HUD for one frame. The next HUD write with `\r` will clear it. Log files are unaffected (newlines are always written to file).
