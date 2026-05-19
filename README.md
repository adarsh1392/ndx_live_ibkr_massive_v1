# NDX Signal → MNQ Execution Bot

Generates trade signals on the **NDX (Nasdaq-100 Cash Index)** using IBKR live data, then executes **MNQ (Micro Nasdaq futures) market orders** via **NinjaTrader 8**.

---

## Architecture

```
IBKR TWS / IB Gateway
  └── NDX 5-min bars (keepUpToDate)
  └── NDX real-time tick price
          │
          ▼
  strategy_core.py  ←  compute_indicators + get_latest_signal
          │
          ▼
  Entry/Exit price level monitoring (live NDX tick)
          │
          ▼
  NinjaTrader 8 (TCP socket)  →  MNQ market orders (1 contract)
```

No orders are ever sent to IBKR. IBKR is used for **data only** (NDX price).

---

## Strategy Summary

| Parameter | Value |
|---|---|
| EMA length | 14 bars |
| Regime filter | slope_atr (SHORT < −0.4, LONG > 0.2) |
| Bounce distance | ATR × 1.2 |
| Stop loss | ATR × 0.8 from entry |
| Risk/reward | 1 : 5 |
| Max risk/trade | 60 pts |
| Entry offset | 5 pts |
| Session | 10:01 – 15:00 NY |
| EOD flatten | 15:30 NY |
| Max trades/day | 2 |
| Trailing stop | 2R → 0.5R lock, 3R → 2R lock, 4R → 3R lock |

---

## Requirements

- **Python 3.11+**
- **IBKR TWS** (paper/demo port `7497`, live port `7496`) — for NDX market data
- **NinjaTrader 8** — for MNQ order execution via TCP socket
- **MNQ front-month contract** — update `NT_INSTRUMENT` in `.env` before each quarterly roll

### Python dependencies

```bash
pip install ib_insync httpx python-dotenv colorama pandas numpy pyyaml
```

---

## Setup

### 1. Configure .env

```bash
cp .env.example .env
```

Edit `.env`:

```
NT_HOST=127.0.0.1
NT_PORT=5557
NT_INSTRUMENT=MNQ 06-26      # update each quarterly expiry

ENABLE_ORDERS=false           # set true when ready to go live
DRY_RUN=true                  # set false when ready to go live
STARTUP_FLATTEN=false

IBKR_HOST=127.0.0.1
IBKR_PORT=7497                # 7497=TWS paper/demo, 7496=TWS live
IBKR_CLIENT_ID=99
```

### 2. Set up NinjaTrader TCP server

The bot sends orders to NinjaTrader via a custom NinjaScript Strategy (`PythonOrderServer.cs`).

**Install steps:**
1. Copy `PythonOrderServer.cs` to:
   ```
   Documents\NinjaTrader 8\bin\Custom\Strategies\PythonOrderServer.cs
   ```
2. Open NinjaTrader → **New → NinjaScript Editor** → press **F5** to compile
3. Open an MNQ chart → right-click → **Strategies → Add Strategy → PythonOrderServer**
4. Set **Account** = your Sim/live account, **TCP Port** = `5557` → click OK and enable
5. Confirm in the Output tab:
   ```
   [PythonOrderServer] Listening on 127.0.0.1:5555
   ```

### 3. Set up IBKR TWS

1. Open TWS and log in (paper/demo account for testing)
2. Go to **Edit → Global Configuration → API → Settings**
   - Check **"Enable ActiveX and Socket Clients"**
   - Set port to `7497` (paper) or `7496` (live)
   - Uncheck **"Read-Only API"**
3. Click OK — TWS will restart the API connection

### 4. Update MNQ contract (quarterly roll)

Update `NT_INSTRUMENT` in `.env` before each expiry Thursday:

| Quarter | Instrument name in NT |
|---|---|
| March | `MNQ 03-26` |
| June | `MNQ 06-26` |
| September | `MNQ 09-26` |
| December | `MNQ 12-26` |

---

## Running the bot

### Test connections first

```bash
python test_connections.py
```

Expected output:
```
NinjaTrader: {'success': True, 'message': 'OK: FLATTEN MNQ 06-26'}
IBKR NDX price: 19845.23
All connections OK
```

### Dry run (recommended first)

In `.env` set `ENABLE_ORDERS=false` and `DRY_RUN=true`, then:

```bash
python live_trader_v2.py
```

Orders will print to screen but nothing is sent to NinjaTrader.

### Go live

In `.env` set `ENABLE_ORDERS=true` and `DRY_RUN=false`, then:

```bash
python live_trader_v2.py
```

### Email alerts on signal candles

The bot can email signal alerts as soon as a completed 5-minute candle qualifies.

Add these `.env` values:

```bash
EMAIL_ALERTS_ENABLED=true
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=your_sender@gmail.com
EMAIL_SMTP_PASSWORD=your_app_password
EMAIL_FROM=your_sender@gmail.com
EMAIL_TO=nayak.nithin99@gmail.com,brindasharma18@gmail.com
```

For Gmail, use an app password from the sending account. The email includes the signal candle time, direction, entry, SL, target, risk, and qty.

### Telegram alerts on signal candles

The bot can also send Telegram alerts as soon as a completed 5-minute candle qualifies.

Add these `.env` values:

```bash
TELEGRAM_ALERTS_ENABLED=true
TELEGRAM_BOT_TOKEN=123456789:your_bot_token
TELEGRAM_CHAT_IDS=123456789,-1009876543210
```

Alert message includes signal candle time, direction, entry, exit (SL), target, risk, and qty.

---

## File Overview

| File | Purpose |
|---|---|
| `live_trader_v2.py` | Main bot — IBKR data + NinjaTrader execution |
| `strategy_core.py` | Signal generation — indicators + entry/exit levels |
| `ninja_trader_client.py` | Async TCP client that sends orders to NinjaTrader |
| `PythonOrderServer.cs` | NinjaScript Strategy — TCP server inside NinjaTrader |
| `config.yaml` | Strategy parameters |
| `config_loader.py` | Loads config.yaml with defaults fallback |
| `test_connections.py` | One-shot connection test (NT + IBKR) |
| `.env.example` | Template — copy to `.env` and fill in settings |
| `.gitignore` | Excludes `.env`, logs, trades, state files |

---

## Crash Recovery

On startup, the bot:
1. Loads any saved position state
2. Flattens only if `STARTUP_FLATTEN=true`
3. Clears `position_state.json` before resuming

If the bot is started after `3:30 PM` New York time, it will not immediately trigger the same-day EOD flatten on startup.

---

## Notes

- The bot trades **1 MNQ contract** by default (`order_qty: 1` in `config.yaml`)
- All P&L is tracked in `trades/trades_mnq_YYYY-MM-DD.csv`
- Logs are written to `logs/log_mnq_YYYY-MM-DD.log`
- IBKR NDX market data subscription is required (US Index data bundle)

---

## Rithmic Support

This repo now includes a broker abstraction plus a `rithmic_client.py` path for
order routing through a local Rithmic bridge process.

See `RITHMIC_SETUP.md` for the required `.env` settings and bridge protocol.

For local end-to-end testing, run:

```bash
python rithmic_bridge_server.py
python rithmic_bridge_smoketest.py
```
