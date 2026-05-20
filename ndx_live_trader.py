"""
NDX Signal → MNQ Execution  (live_trader_v3.py)
================================================
Data source : IBKR TWS  — NDX cash index, 5-min bars + real-time tick price
Order exec  : NinjaTrader 8 — MNQ market orders via TCP socket (PythonOrderServer AddOn)
SL / TSL / TP : monitored in this code against live NDX price

All configuration lives in config.yaml — no .env required.

Keyboard controls (non-blocking daemon thread):
  S  — print current position status
  E  — exit open position (keeps console running)
  Q  — quit without squaring off
"""

import importlib
import subprocess
import sys

# ---------------------------------------------------------------------------
# Startup dependency check — runs before any third-party imports
# ---------------------------------------------------------------------------

_REQUIRED_PACKAGES = {
    "ib_insync":    "ib_insync",
    "httpx":        "httpx",
    "numpy":        "numpy",
    "pandas":       "pandas",
    "colorama":     "colorama",
    "yaml":         "pyyaml",
    "dotenv":       "python-dotenv",
}

def _check_dependencies():
    missing = []
    for import_name, pip_name in _REQUIRED_PACKAGES.items():
        if importlib.util.find_spec(import_name) is None:
            missing.append(pip_name)
    if missing:
        pip_exe = sys.executable.replace("python.exe", "pip.exe") if sys.platform == "win32" else sys.executable.replace("python", "pip")
        print("\n" + "═" * 60, flush=True)
        print("  MISSING PYTHON PACKAGES", flush=True)
        print("═" * 60, flush=True)
        for pkg in missing:
            print(f"  ✖  {pkg}", flush=True)
        print("\n  Install them by running:", flush=True)
        print(f'\n    "{sys.executable}" -m pip install ' + " ".join(missing), flush=True)
        print("\n  Or install all requirements at once:", flush=True)
        print(f'\n    "{sys.executable}" -m pip install -r requirements.txt', flush=True)
        print("\n" + "═" * 60 + "\n", flush=True)
        sys.exit(1)

_check_dependencies()

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------

import asyncio
import csv
import json
import os
import threading
import time
from datetime import datetime, UTC, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo

import httpx
import numpy as np
import pandas as pd
from colorama import Fore, Back, Style, init as colorama_init
from ib_insync import IB, Index, util

from broker_client import BrokerClient, create_broker_client
from config_loader import get_config
from strategy_core import compute_indicators, get_latest_signal

# ---------------------------------------------------------------------------
# Globals / constants
# ---------------------------------------------------------------------------

NY_TZ      = ZoneInfo("America/New_York")
TICK_SIZE  = 0.25
STATE_FILE = "position_state.json"

# ---------------------------------------------------------------------------
# Load config once at module level
# ---------------------------------------------------------------------------

_CFG = get_config()

def _scfg(section: str) -> dict:
    return _CFG.get(section, {})

# IBKR
IBKR_HOST             = str(_scfg("ibkr").get("host", "127.0.0.1"))
IBKR_PORT             = int(_scfg("ibkr").get("port", 7497))
IBKR_CLIENT_ID        = int(_scfg("ibkr").get("client_id", 99))
IBKR_TIMEOUT_S        = float(_scfg("ibkr").get("connect_timeout_seconds", 6))

# Data feed
_dfc              = _scfg("data_feed")
DATA_FEED_SOURCE  = str(_dfc.get("source",           "ibkr")).lower()
MASSIVE_API_KEY   = str(_dfc.get("massive_api_key",  ""))
MASSIVE_TICKER    = str(_dfc.get("massive_ticker",   "I:NDX"))

# Execution switches
ENABLE_ORDERS         = str(_scfg("execution").get("enable_orders", False)).lower() == "true"
DRY_RUN               = str(_scfg("execution").get("dry_run", True)).lower() == "true"
STARTUP_FLATTEN       = str(_scfg("execution").get("startup_flatten", False)).lower() == "true"

# HUD
STATUS_UPDATE_SECONDS = int(_scfg("hud").get("status_update_seconds", 300))

# Telegram
_tg                   = _scfg("telegram")
TELEGRAM_ENABLED      = str(_tg.get("enabled", False)).lower() == "true"
def _load_telegram_bots(tg_cfg: dict) -> list:
    """Load bots list from config; supports multi-bot (bots[]) and legacy (bot_token+chat_ids)."""
    bots = tg_cfg.get("bots")
    if bots:
        return [
            {"token": str(b.get("token", "")).strip(),
             "chat_ids": [str(c).strip() for c in (b.get("chat_ids") or []) if str(c).strip()]}
            for b in bots if str(b.get("token", "")).strip()
        ]
    # legacy single-bot fallback
    token    = str(tg_cfg.get("bot_token", "")).strip()
    chat_ids = [str(c).strip() for c in (tg_cfg.get("chat_ids") or []) if str(c).strip()]
    return [{"token": token, "chat_ids": chat_ids}] if token else []

TELEGRAM_BOTS = _load_telegram_bots(_tg)

# Override broker client factory env vars from config so broker_client.py still works
_bcfg = _scfg("broker")
_broker_name = str(_bcfg.get("name", "ninjatrader")).lower()
os.environ.setdefault("BROKER", _broker_name)
if _broker_name == "ninjatrader":
    _ntcfg = _scfg("ninjatrader")
    os.environ.setdefault("NT_HOST",       str(_ntcfg.get("host",       "127.0.0.1")))
    os.environ.setdefault("NT_PORT",       str(_ntcfg.get("port",       5557)))
    os.environ.setdefault("NT_INSTRUMENT", str(_ntcfg.get("instrument", "MNQ 06-26")))
elif _broker_name == "rithmic":
    _rcfg = _scfg("rithmic")
    os.environ.setdefault("RITHMIC_HOST",       str(_rcfg.get("host",       "127.0.0.1")))
    os.environ.setdefault("RITHMIC_PORT",       str(_rcfg.get("port",       6500)))
    os.environ.setdefault("RITHMIC_INSTRUMENT", str(_rcfg.get("instrument", "MNQM6")))
    os.environ.setdefault("RITHMIC_ACCOUNT_ID", str(_rcfg.get("account_id", "")))
    os.environ.setdefault("RITHMIC_EXCHANGE",   str(_rcfg.get("exchange",   "CME")))
    os.environ.setdefault("RITHMIC_GATEWAY",    str(_rcfg.get("gateway",    "")))

# ---------------------------------------------------------------------------
# Spinner state (updated by tick handler, read by HUD)
# ---------------------------------------------------------------------------

_spinner_frames    = ["|", "/", "-", "\\"]
_spinner_idx       = 0
_spinner_lock      = threading.Lock()
_spinner_last_tick = 0.0          # monotonic time of last frame advance
_SPINNER_INTERVAL  = 0.25         # seconds between spinner frame changes
_confirm_active    = False        # True while E/Q confirmation prompt is active

# ---------------------------------------------------------------------------
# Signal history — in-memory log of every signal detected today
# ---------------------------------------------------------------------------

_signal_history: list[dict] = []
_signal_history_day = None   # date of current history; reset on new day


def _sh_reset_if_new_day(today):
    """Clear history when the trading date rolls over."""
    global _signal_history, _signal_history_day
    if _signal_history_day != today:
        _signal_history      = []
        _signal_history_day  = today


def _sh_add(sig: dict, status: str, now_ny) -> int:
    """Append a new signal record. Returns its index."""
    _sh_reset_if_new_day(now_ny.date())
    entry = {
        "id":          len(_signal_history),
        "detected_at": now_ny,
        "candle_time": sig.get("signal_dt", ""),
        "direction":   sig.get("direction", "?"),
        "entry":       sig.get("entry",     float("nan")),
        "sl":          sig.get("sl",        float("nan")),
        "target":      sig.get("target",    float("nan")),
        "risk":        sig.get("risk",      float("nan")),
        "qty":         sig.get("qty",       0),
        "status":      status,
        "status_at":   now_ny,
        "fill_price":  None,
    }
    _signal_history.append(entry)
    return entry["id"]


def _sh_update(signal_id: int, status: str, now_ny, fill_price=None):
    """Update status (and optionally fill price) of an existing history entry."""
    if signal_id is None:
        return
    for e in _signal_history:
        if e["id"] == signal_id:
            e["status"]    = status
            e["status_at"] = now_ny
            if fill_price is not None:
                e["fill_price"] = fill_price
            return


def print_signal_history():
    """Print the full signal history for today to console (not log file)."""
    from colorama import Fore, Style
    C = Fore.CYAN;  R = Style.RESET_ALL;  B = Style.BRIGHT
    if not _signal_history:
        print(f"\n{C}No signals detected today.{R}\n", flush=True)
        return

    STATUS_COL = {
        "PENDING":    Fore.YELLOW  + B,
        "TRIGGERED":  Fore.GREEN   + B,
        "EXPIRED":    Fore.WHITE   + Style.DIM,
        "OVERRIDDEN": Fore.MAGENTA + Style.DIM,
        "SUPPRESSED": Fore.WHITE   + Style.DIM,
    }
    DIR_COL = {"LONG": Fore.GREEN + B, "SHORT": Fore.RED + B}

    print(f"\n{C}╔{'═' * 74}╗{R}", flush=True)
    print(f"{C}║{'  SIGNAL HISTORY':^74}║{R}", flush=True)
    print(f"{C}╠{'═' * 4}╦{'═' * 6}╦{'═' * 6}╦{'═' * 10}╦{'═' * 8}╦{'═' * 8}╦{'═' * 8}╦{'═' * 14}╣{R}", flush=True)
    print(f"{C}║ # ║ Time ║  Dir ║   Entry  ║   SL   ║ Target ║  Risk  ║    Status    ║{R}", flush=True)
    print(f"{C}╠{'═' * 4}╬{'═' * 6}╬{'═' * 6}╬{'═' * 10}╬{'═' * 8}╬{'═' * 8}╬{'═' * 8}╬{'═' * 14}╣{R}", flush=True)

    for e in _signal_history:
        t = pd.Timestamp(e["candle_time"]).strftime("%H:%M") if e["candle_time"] else "--:--"
        d = e["direction"]
        dc = DIR_COL.get(d, "")
        sc = STATUS_COL.get(e["status"], "")
        fill_note = f" @{e['fill_price']:.2f}" if e["fill_price"] is not None else ""
        status_txt = e["status"] + fill_note
        print(
            f"{C}║{R}{e['id']:>2} {C}║{R} {t} {C}║{R} {dc}{d:5s}{R} {C}║{R} {e['entry']:>8.2f} "
            f"{C}║{R} {e['sl']:>6.2f} {C}║{R} {e['target']:>6.2f} {C}║{R} {e['risk']:>6.2f} "
            f"{C}║{R} {sc}{status_txt:<13}{R}{C}║{R}",
            flush=True,
        )

    print(f"{C}╚{'═' * 4}╩{'═' * 6}╩{'═' * 6}╩{'═' * 10}╩{'═' * 8}╩{'═' * 8}╩{'═' * 8}╩{'═' * 14}╝{R}\n", flush=True)


def _tick_spinner() -> str:
    """Advance the spinner frame at most once per _SPINNER_INTERVAL seconds."""
    global _spinner_idx, _spinner_last_tick
    now = time.monotonic()
    with _spinner_lock:
        if now - _spinner_last_tick >= _SPINNER_INTERVAL:
            _spinner_idx += 1
            _spinner_last_tick = now
        ch = _spinner_frames[_spinner_idx % 4]
    return ch

# ---------------------------------------------------------------------------
# Keyboard control (daemon thread)
# ---------------------------------------------------------------------------

IS_WINDOWS = sys.platform == "win32"

# Shared command queue: main async loop reads from this
_kb_queue: "asyncio.Queue[str]" = None  # set in run()

_CONFIRM_WORDS = {"e": "exit", "q": "quit"}

def _kb_confirm_win(key: str) -> bool:
    """
    Windows: prompt printed to stdout; user must type the confirmation word
    char-by-char using msvcrt.getch(), then press Enter.
    Avoids input() which conflicts with asyncio ProactorEventLoop on Windows.
    """
    global _confirm_active
    import msvcrt
    word = _CONFIRM_WORDS.get(key, "")
    _confirm_active = True
    try:
        # \r clears the current HUD line; \n then moves to a fresh line
        sys.stdout.write(
            f"\r{' ' * 80}\r\n{Fore.YELLOW}  Confirm: type '{word}' and press Enter "
            f"(or press Enter to cancel): {Style.RESET_ALL}"
        )
        sys.stdout.flush()
        typed = []
        while True:
            ch = msvcrt.getch()
            if ch in (b"\r", b"\n"):          # Enter — done
                sys.stdout.write("\n")
                sys.stdout.flush()
                break
            if ch == b"\x08" or ch == b"\x7f":  # Backspace
                if typed:
                    typed.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
                continue
            if ch in (b"\x00", b"\xe0"):      # special key prefix — skip next byte
                msvcrt.getch()
                continue
            if ch == b"\x03":                  # Ctrl-C — treat as cancel
                sys.stdout.write("\n")
                sys.stdout.flush()
                return False
            try:
                c = ch.decode("utf-8", errors="ignore")
            except Exception:
                continue
            typed.append(c.lower())
            sys.stdout.write(c)               # echo the character
            sys.stdout.flush()
        result = "".join(typed)
        if result == word:
            return True
        sys.stdout.write(f"{Fore.CYAN}  Cancelled.{Style.RESET_ALL}\n")
        sys.stdout.flush()
        return False
    finally:
        _confirm_active = False


def _kb_thread_fn(loop: asyncio.AbstractEventLoop):
    """
    Non-blocking keyboard reader. Runs in a daemon thread.
    Puts single-char commands ('s','e','q') into _kb_queue.
    E and Q require typed confirmation before being queued.
    """
    allowed = {"s", "e", "q", "h"}
    if IS_WINDOWS:
        import msvcrt
        while True:
            time.sleep(0.05)
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                if ch in (b"\x00", b"\xe0"):
                    msvcrt.getch()
                    continue
                try:
                    k = ch.decode("utf-8", errors="ignore").lower()
                except Exception:
                    continue
                if k in allowed:
                    if k in _CONFIRM_WORDS:
                        if not _kb_confirm_win(k):
                            continue
                    asyncio.run_coroutine_threadsafe(_kb_queue.put(k), loop)
    else:
        import select, termios, tty
        fd = sys.stdin.fileno()
        try:
            old = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        except Exception:
            return
        try:
            while True:
                r, _, _ = select.select([sys.stdin], [], [], 0.05)
                if r:
                    ch = sys.stdin.read(1)
                    if not ch:
                        continue
                    if ch == "\x1b":
                        try:
                            while select.select([sys.stdin], [], [], 0)[0]:
                                sys.stdin.read(1)
                        except Exception:
                            pass
                        continue
                    if ord(ch) < 32 or ord(ch) == 127:
                        continue
                    k = ch.lower()
                    if k in allowed:
                        if k in _CONFIRM_WORDS:
                            word = _CONFIRM_WORDS[k]
                            # restore canonical mode for char-by-char read, then re-apply cbreak
                            try:
                                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                            except Exception:
                                pass
                            sys.stdout.write(
                                f"\n{Fore.YELLOW}  Confirm: type '{word}' and press Enter "
                                f"(or press Enter to cancel): {Style.RESET_ALL}"
                            )
                            sys.stdout.flush()
                            try:
                                typed = sys.stdin.readline().strip().lower()
                            except Exception:
                                typed = ""
                            try:
                                tty.setcbreak(fd)
                            except Exception:
                                pass
                            if typed != word:
                                sys.stdout.write(f"{Fore.CYAN}  Cancelled.{Style.RESET_ALL}\n")
                                sys.stdout.flush()
                                continue
                        asyncio.run_coroutine_threadsafe(_kb_queue.put(k), loop)
        finally:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Logging / console helpers
# ---------------------------------------------------------------------------

def _ensure_dirs():
    os.makedirs("logs",   exist_ok=True)
    os.makedirs("trades", exist_ok=True)

def log(msg: str):
    """Print + append to today's log file."""
    _ensure_dirs()
    today = datetime.now(NY_TZ).date()
    path  = os.path.join("logs", f"log_mnq_{today.isoformat()}.log")
    with open(path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg, flush=True)

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

_BANNER_LINES = [
    "╔══════════════════════════════════════════════════════════╗",
    "║                                                          ║",
    "║          N D X   L I V E   T R A D E R                   ║",
    "║       NDX Signal  →  MNQ Execution via NinjaTrader       ║",
    "║                                                          ║",
    "╚══════════════════════════════════════════════════════════╝",
]

def print_banner():
    colorama_init(autoreset=True)
    color = Back.BLUE + Fore.WHITE + Style.BRIGHT
    print("", flush=True)
    for line in _BANNER_LINES:
        print(f"{color}{line}{Style.RESET_ALL}", flush=True)
    print("", flush=True)

# ---------------------------------------------------------------------------
# Keyboard control print helpers
# ---------------------------------------------------------------------------

def print_kb_help():
    print(
        f"\n{Fore.CYAN}{'─' * 56}{Style.RESET_ALL}\n"
        f"  {Fore.MAGENTA}[S]{Style.RESET_ALL} Status   "
        f"{Fore.MAGENTA}[E]{Style.RESET_ALL} Exit position   "
        f"{Fore.MAGENTA}[Q]{Style.RESET_ALL} Quit (no square-off)   "
        f"{Fore.MAGENTA}[H]{Style.RESET_ALL} Signal history\n"
        f"{Fore.CYAN}{'─' * 56}{Style.RESET_ALL}\n",
        flush=True,
    )

def print_position_status(algo_position, trades_today, max_trades, live_price):
    print(f"\n{Fore.GREEN}{Style.BRIGHT}╔══ POSITION STATUS ══════════════════════╗{Style.RESET_ALL}", flush=True)
    if algo_position:
        d         = algo_position["direction"]
        col       = Fore.GREEN if d == "LONG" else Fore.RED
        entry_ndx = algo_position["entry_ndx"]
        sl        = algo_position["sl"]
        tgt       = algo_position["target"]
        risk      = algo_position["risk"]
        qty       = algo_position["qty"]
        step      = algo_position["trail_step"]
        if live_price and risk:
            fr  = (live_price - entry_ndx) / risk if d == "LONG" else (entry_ndx - live_price) / risk
            pnl = (live_price - entry_ndx) if d == "LONG" else (entry_ndx - live_price)
        else:
            fr, pnl = 0.0, 0.0
        pnl_col = Fore.GREEN if pnl >= 0 else Fore.RED
        print(f"  Direction : {col}{Style.BRIGHT}{d}{Style.RESET_ALL}", flush=True)
        print(f"  Entry NDX : {entry_ndx:.2f}   Live NDX: {f'{live_price:.2f}' if live_price else '—'}", flush=True)
        print(f"  SL        : {sl:.2f}   Target: {tgt:.2f}   Risk: {risk:.2f} pts", flush=True)
        print(f"  Qty       : {qty}   Trail step: {step}   R achieved: {fr:.2f}R", flush=True)
        print(f"  Unrealised: {pnl_col}{pnl:+.2f} pts{Style.RESET_ALL}", flush=True)
    else:
        print(f"  {Fore.YELLOW}FLAT — no open position{Style.RESET_ALL}", flush=True)
    print(f"  Trades today: {trades_today}/{max_trades}", flush=True)
    print(f"{Fore.GREEN}{Style.BRIGHT}╚══════════════════════════════════════════╝{Style.RESET_ALL}\n", flush=True)

def print_signal_box(sig: dict, qty: int):
    d     = sig["direction"]
    color = Back.GREEN + Fore.BLACK + Style.BRIGHT if d == "LONG" else Back.RED + Fore.WHITE + Style.BRIGHT
    reset = Style.RESET_ALL
    arrow = "▲ LONG  (BUY)" if d == "LONG" else "▼ SHORT (SELL)"
    lines = [
        f"╔{'═'*54}╗",
        f"║{'':^54}║",
        f"║  {'NEW SIGNAL DETECTED':^50}  ║",
        f"║  {arrow:^50}  ║",
        f"║{'':^54}║",
        f"║  Entry  : {sig['entry']:>10.2f}{'':>33}║",
        f"║  Stop   : {sig['sl']:>10.2f}{'':>33}║",
        f"║  Target : {sig['target']:>10.2f}{'':>33}║",
        f"║  Risk   : {sig['risk']:>10.2f} pts{'':>29}║",
        f"║  Qty    : {qty:>10}{'':>33}║",
        f"║{'':^54}║",
        f"╚{'═'*54}╝",
    ]
    print("", flush=True)
    for line in lines:
        print(f"{color}{line}{reset}", flush=True)
    print("", flush=True)

# ---------------------------------------------------------------------------
# Trade CSV
# ---------------------------------------------------------------------------

def append_trade_log(entry_time, exit_time, side, entry_ndx, exit_ndx, reason, pnl):
    _ensure_dirs()
    date_str   = entry_time.date().isoformat() if hasattr(entry_time, "date") else str(entry_time)[:10]
    path       = os.path.join("trades", f"trades_mnq_{date_str}.csv")
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["entry_time", "exit_time", "side", "entry_ndx", "exit_ndx", "exit_reason", "pnl_pts"])
        w.writerow([str(entry_time), str(exit_time), side,
                    f"{entry_ndx:.2f}", f"{exit_ndx:.2f}", reason, f"{pnl:.2f}"])

# ---------------------------------------------------------------------------
# Position state  (crash recovery)
# ---------------------------------------------------------------------------

def save_position_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, default=str)

def load_position_state() -> dict | None:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def clear_position_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------

def qty_from_risk(risk_points: float) -> int:
    if risk_points >= 30.0: return 1
    if risk_points >= 20.0: return 2
    if risk_points >= 15.0: return 3
    return 4

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

async def _tg_send_raw(msg: str) -> None:
    """Send a plain text message to all configured Telegram bots."""
    if not TELEGRAM_ENABLED:
        return
    active_bots = [b for b in TELEGRAM_BOTS if b["token"] and b["chat_ids"]]
    if not active_bots:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            for bot in active_bots:
                url = f"https://api.telegram.org/bot{bot['token']}/sendMessage"
                for chat_id in bot["chat_ids"]:
                    try:
                        resp = await client.post(url, json={"chat_id": chat_id, "text": msg})
                        if resp.status_code >= 400:
                            log(f"  TELEGRAM: failed chat_id={chat_id}: {resp.status_code} {resp.text}")
                        else:
                            log(f"  TELEGRAM: sent to chat_id={chat_id}")
                    except Exception as exc:
                        log(f"  TELEGRAM: error for chat_id={chat_id}: {exc}")
    except Exception as exc:
        log(f"  TELEGRAM: failed: {exc}")


async def send_entry_telegram(direction: str, fill_price: float, sl: float,
                               target: float, risk: float, qty: int, now_ny) -> None:
    emoji = "🟢" if direction == "LONG" else "🔴"
    msg = (
        f"{emoji} MNQ ORDER ENTERED\n\n"
        f"Time     : {now_ny.strftime('%Y-%m-%d %H:%M:%S')} NY\n"
        f"Direction: {direction}\n"
        f"Fill     : {fill_price:.2f}\n"
        f"Stop     : {sl:.2f}\n"
        f"Target   : {target:.2f}\n"
        f"Risk     : {risk:.2f} pts\n"
        f"Qty      : {qty}"
    )
    await _tg_send_raw(msg)


async def send_exit_telegram(direction: str, reason: str, entry_ndx: float,
                              exit_ndx: float, pnl: float, qty: int, now_ny) -> None:
    emoji = "✅" if pnl >= 0 else "❌"
    reason_label = {"SL": "Stop Loss", "TSL": "Trailing Stop", "TP": "Take Profit",
                    "MANUAL": "Manual Exit", "EOD": "EOD Flatten"}.get(reason, reason)
    msg = (
        f"{emoji} MNQ POSITION CLOSED — {reason_label}\n\n"
        f"Time     : {now_ny.strftime('%Y-%m-%d %H:%M:%S')} NY\n"
        f"Direction: {direction}\n"
        f"Entry    : {entry_ndx:.2f}\n"
        f"Exit     : {exit_ndx:.2f}\n"
        f"PnL      : {pnl:+.2f} pts\n"
        f"Qty      : {qty}"
    )
    await _tg_send_raw(msg)


async def send_signal_telegram(sig: dict, signal_qty: int) -> None:
    if not TELEGRAM_ENABLED:
        return
    active_bots = [b for b in TELEGRAM_BOTS if b["token"] and b["chat_ids"]]
    if not active_bots:
        log("  TELEGRAM: skipped — no bots with chat_ids configured in config.yaml")
        return

    signal_dt    = pd.Timestamp(sig["signal_dt"])
    signal_dt_ny = (
        signal_dt.tz_localize(None)
        if signal_dt.tzinfo is None
        else signal_dt.tz_convert(NY_TZ).tz_localize(None)
    )
    msg = (
        "📡 NDX signal candle detected\n\n"
        f"Time     : {signal_dt_ny.strftime('%Y-%m-%d %H:%M:%S')} NY\n"
        f"Direction: {sig['direction']}\n"
        f"Entry    : {float(sig['entry']):.2f}\n"
        f"Stop     : {float(sig['sl']):.2f}\n"
        f"Target   : {float(sig['target']):.2f}\n"
        f"Risk     : {float(sig['risk']):.2f} pts\n"
        f"Qty      : {signal_qty}"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            for bot in active_bots:
                url = f"https://api.telegram.org/bot{bot['token']}/sendMessage"
                for chat_id in bot["chat_ids"]:
                    try:
                        resp = await client.post(url, json={"chat_id": chat_id, "text": msg})
                        if resp.status_code >= 400:
                            log(f"  TELEGRAM: failed chat_id={chat_id}: {resp.status_code} {resp.text}")
                        else:
                            log(f"  TELEGRAM: sent to chat_id={chat_id}")
                    except Exception as exc:
                        log(f"  TELEGRAM: error for chat_id={chat_id}: {exc}")
    except Exception as exc:
        log(f"  TELEGRAM: failed: {exc}")

# ---------------------------------------------------------------------------
# Broker helpers
# ---------------------------------------------------------------------------

async def safe_place_entry(broker_client: BrokerClient, direction: str, qty: int) -> dict:
    try:
        return await broker_client.place_entry(direction, qty)
    except Exception as exc:
        log(f"  ORDER ERROR (place_entry): {exc}")
        return {"success": False, "message": str(exc)}

async def safe_place_exit(broker_client: BrokerClient, direction: str, qty: int) -> dict:
    try:
        return await broker_client.place_exit(direction, qty)
    except Exception as exc:
        log(f"  ORDER ERROR (place_exit): {exc}")
        return {"success": False, "message": str(exc)}

async def safe_flatten(broker_client: BrokerClient, enable_orders: bool, dry_run: bool):
    if not enable_orders or dry_run:
        log("  DRY RUN FLATTEN")
        return
    try:
        resp = await broker_client.flatten()
        log(f"  FLATTEN response: {resp}")
    except Exception as exc:
        log(f"  FLATTEN ERROR: {exc}")

# ---------------------------------------------------------------------------
# IBKR price helper
# ---------------------------------------------------------------------------

def get_ndx_price(ticker) -> float | None:
    # MassiveFeed duck-type: exposes .live_price directly
    if hasattr(ticker, "live_price"):
        lp = ticker.live_price
        return float(lp) if lp is not None else None
    # ib_insync ticker
    v = ticker.marketPrice()
    if v is not None and not (isinstance(v, float) and np.isnan(v)):
        return float(v)
    c = getattr(ticker, "close", None)
    if c is not None and not (isinstance(c, float) and np.isnan(c)):
        return float(c)
    return None

# ---------------------------------------------------------------------------
# Bar DataFrame builder
# ---------------------------------------------------------------------------

def bars_to_df(bar_list) -> pd.DataFrame:
    # Polymorphic: supports both ib_insync BarData (naive datetime) and
    # massive_feed.BarData (UTC-aware datetime).
    first = bar_list[0]
    if getattr(first.date, "tzinfo", None) is not None:
        # massive_feed.BarData — build DataFrame directly from dataclass fields
        rows = [
            {"datetime_ny": b.date, "Open": b.open, "High": b.high,
             "Low": b.low, "Close": b.close, "Volume": getattr(b, "volume", 0)}
            for b in bar_list
        ]
        df = pd.DataFrame(rows)
        df["datetime_ny"] = (
            pd.to_datetime(df["datetime_ny"], utc=True)
            .dt.tz_convert("America/New_York")
            .dt.tz_localize(None)
        )
    else:
        # ib_insync BarData — use util.df() then rename
        df = util.df(list(bar_list)).rename(columns={
            "date": "datetime_ny", "open": "Open", "high": "High",
            "low":  "Low",         "close": "Close", "volume": "Volume",
        })
        df["datetime_ny"] = (
            pd.to_datetime(df["datetime_ny"])
            .dt.tz_convert("America/New_York")
            .dt.tz_localize(None)
        )
    if "Volume" not in df.columns:
        df["Volume"] = 0
    return df[["datetime_ny", "Open", "High", "Low", "Close", "Volume"]].copy()

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _parse_hhmm(s: str, default: str) -> dt_time:
    try:
        h, m = [int(x) for x in str(s).split(":")]
        return dt_time(h, m)
    except Exception:
        h, m = [int(x) for x in default.split(":")]
        return dt_time(h, m)

def _in_window(t: dt_time, start: dt_time, end: dt_time) -> bool:
    if start <= end:
        return start <= t <= end
    return (t >= start) or (t <= end)

# ---------------------------------------------------------------------------
# Fill capture / slippage helpers
# ---------------------------------------------------------------------------

def _safe_float(v) -> float | None:
    try:
        return float(v) if v is not None and str(v).strip() != "" else None
    except Exception:
        return None

def _safe_int(v) -> int | None:
    try:
        return int(v) if v is not None and str(v).strip() != "" else None
    except Exception:
        return None

async def _fetch_ninja_fill_after(broker_client, min_epoch_ms: int, max_wait_s: float = 2.5) -> dict | None:
    if not hasattr(broker_client, "get_last_execution"):
        return None
    attempts = max(1, int(max_wait_s / 0.1))
    for _ in range(attempts):
        try:
            resp = await broker_client.get_last_execution()
            if resp.get("success", False):
                data = resp.get("data") or {}
                epoch_ms = _safe_int(data.get("epoch_ms"))
                if epoch_ms is not None and epoch_ms >= min_epoch_ms:
                    return data
        except Exception:
            pass
        await asyncio.sleep(0.1)
    return None

def _calc_adverse_slippage(direction: str, leg: str, ref_px: float, fill_px: float) -> float:
    d, l = direction.upper(), leg.upper()
    if l == "ENTRY":
        return (fill_px - ref_px) if d == "LONG" else (ref_px - fill_px)
    return (ref_px - fill_px) if d == "LONG" else (fill_px - ref_px)

# ---------------------------------------------------------------------------
# IBKR connection with port fallback
# ---------------------------------------------------------------------------

async def connect_ibkr(ib: IB) -> tuple[str, int]:
    port_candidates = list(dict.fromkeys([IBKR_PORT, 7497, 7496, 4001, 4002]))
    last_error = None
    for port in port_candidates:
        try:
            log(f"  Connecting to IBKR | {IBKR_HOST}:{port}  clientId={IBKR_CLIENT_ID} ...")
            await ib.connectAsync(IBKR_HOST, port, clientId=IBKR_CLIENT_ID, timeout=IBKR_TIMEOUT_S)
            log(f"{Fore.GREEN}  ✔ IBKR connected | {IBKR_HOST}:{port}{Style.RESET_ALL}")
            return IBKR_HOST, port
        except Exception as e:
            last_error = e
            log(f"  ✖ IBKR connect failed on {IBKR_HOST}:{port} → {e}")
    raise ConnectionError(
        f"IBKR connection failed on all ports {port_candidates}. "
        "Check TWS/IB Gateway: API → Enable ActiveX and Socket Clients, Read-Only API disabled."
    ) from last_error

# ---------------------------------------------------------------------------
# NinjaTrader ping
# ---------------------------------------------------------------------------

async def ping_ninja(broker_client) -> bool:
    if not hasattr(broker_client, "ping"):
        return True
    try:
        resp = await broker_client.ping()
        if resp.get("success", False):
            log(f"{Fore.GREEN}  ✔ NinjaTrader connected | {broker_client.host}:{broker_client.port} "
                f"instrument={broker_client.instrument}{Style.RESET_ALL}")
            return True
        else:
            log(f"{Fore.RED}{Style.BRIGHT}  ✖ NinjaTrader ping failed: {resp.get('message')}{Style.RESET_ALL}")
            return False
    except Exception as exc:
        log(f"{Fore.RED}{Style.BRIGHT}  ✖ NinjaTrader ping error: {exc}{Style.RESET_ALL}")
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run():
    global _kb_queue

    colorama_init(autoreset=True)
    _ensure_dirs()

    print_banner()

    cfg          = get_config()
    strategy_cfg = cfg.get("strategy", {})
    orders_cfg   = cfg.get("orders",   {})

    default_order_qty  = max(1, int(orders_cfg.get("order_qty", 1)))
    qty_multiplier     = max(1, int(orders_cfg.get("qty_multiplier", 1)))
    max_trades_per_day = int(strategy_cfg.get("max_trades_per_day", 2))

    signal_start_t = _parse_hhmm(strategy_cfg.get("signal_start", "09:30"), "09:30")
    trade_start_t  = _parse_hhmm(strategy_cfg.get("trade_start",  "10:01"), "10:01")
    session_end_t  = _parse_hhmm(strategy_cfg.get("session_end",  "15:00"), "15:00")
    eod_exit_t     = dt_time(15, 30)

    # Keyboard queue
    loop      = asyncio.get_running_loop()
    _kb_queue = asyncio.Queue()
    kb_thread = threading.Thread(target=_kb_thread_fn, args=(loop,), daemon=True)
    kb_thread.start()

    # --- Broker client ---
    broker_client = create_broker_client()
    log(
        f"\n{Fore.CYAN}Broker: {broker_client.name.upper()}  |  "
        f"{broker_client.host}:{broker_client.port}  |  {broker_client.instrument}{Style.RESET_ALL}"
    )
    log(f"  Orders enabled: {ENABLE_ORDERS}   Dry run: {DRY_RUN}")

    # Ping NinjaTrader — mandatory: abort if unreachable
    _nt_ok = await ping_ninja(broker_client)
    if not _nt_ok:
        _ntcfg_live = get_config().get("ninjatrader", {})
        log(
            f"\n{Fore.RED}{Style.BRIGHT}"
            f"╔══ NINJATRADER CONNECTION FAILED ═══════════════════════╗\n"
            f"║                                                        ║\n"
            f"║  Cannot reach PythonOrderServer on TCP socket.         ║\n"
            f"║                                                        ║\n"
            f"║  To fix:                                               ║\n"
            f"║  1. Open NinjaTrader 8                                 ║\n"
            f"║  2. Enable the PythonOrderServer AddOn                 ║\n"
            f"║  3. Confirm it is listening on                         ║\n"
            f"║     {_ntcfg_live.get('host','127.0.0.1')}:{_ntcfg_live.get('port',5555)}  (config.yaml → ninjatrader.port)     ║\n"
            f"║  4. Restart this script                                ║\n"
            f"║                                                        ║\n"
            f"╚════════════════════════════════════════════════════════╝"
            f"{Style.RESET_ALL}"
        )
        log("  Aborting: NinjaTrader connection is required for order execution.")
        return

    # --- Startup position recovery ---
    saved = load_position_state()
    if saved:
        log(f"\n{Fore.YELLOW}CRASH RECOVERY: saved state found → {saved}{Style.RESET_ALL}")
    if STARTUP_FLATTEN:
        log("Startup: flattening any open MNQ position...")
        await safe_flatten(broker_client, ENABLE_ORDERS, DRY_RUN)
    else:
        log("Startup: STARTUP_FLATTEN=false — skipping auto-flatten")
    clear_position_state()

    # --- Data feed setup (ibkr | massive) ---
    ib                    = None
    feed                  = None
    warmup_bars: list     = []
    _warmup_cutoff        = None
    _ib_needs_resubscribe = False
    _live_bars_offset     = 1   # IBKR: last bar is in-progress; massive: all bars are completed

    if DATA_FEED_SOURCE == "ibkr":
        log(f"\n{Fore.CYAN}Connecting to Interactive Brokers TWS...{Style.RESET_ALL}")
        ib = IB()
        try:
            connected_host, connected_port = await connect_ibkr(ib)
        except ConnectionError as exc:
            log(
                f"\n{Fore.RED}{Style.BRIGHT}"
                f"╔══ IBKR CONNECTION FAILED ══════════════════════════════╗\n"
                f"║                                                        ║\n"
                f"║  Could not connect to TWS / IB Gateway on any port.    ║\n"
                f"║                                                        ║\n"
                f"║  To fix:                                               ║\n"
                f"║  1. Open TWS or IB Gateway and log in                  ║\n"
                f"║  2. Go to:  Edit → Global Configuration → API          ║\n"
                f"║             → Settings                                 ║\n"
                f"║  3. Check:  ✔ Enable ActiveX and Socket Clients        ║\n"
                f"║  4. Uncheck: Read-Only API                             ║\n"
                f"║  5. Make sure the port matches config.yaml (ibkr.port) ║\n"
                f"║     Paper/demo TWS = 7497  Live TWS = 7496             ║\n"
                f"║     IB Gateway live = 4001                             ║\n"
                f"║                                                        ║\n"
                f"╚════════════════════════════════════════════════════════╝"
                f"{Style.RESET_ALL}"
            )
            log(f"\n  Detail: {exc}\n")
            return

        ndx = Index("NDX", "NASDAQ", "USD")
        await ib.qualifyContractsAsync(ndx)
        log(f"  NDX contract qualified: {ndx.symbol} {ndx.exchange} {ndx.currency}")

        # ---------------------------------------------------------------
        # Stage 1: reliable historical warmup (no keepUpToDate)
        # Needed for EMA-14 / ATR-14 / slope_atr to have valid values.
        # This fetch is simple and will not timeout like keepUpToDate.
        # ---------------------------------------------------------------
        log("  Fetching EMA/ATR warmup bars (2 days)...")
        for attempt in range(1, 4):
            try:
                warmup_bars = await ib.reqHistoricalDataAsync(
                    ndx, endDateTime="", durationStr="2 D",
                    barSizeSetting="5 mins", whatToShow="TRADES",
                    useRTH=False, keepUpToDate=False,
                )
            except Exception as _wu_exc:
                log(f"{Fore.YELLOW}  ⚠ Warmup attempt {attempt}/3 failed: {_wu_exc}{Style.RESET_ALL}")
            if warmup_bars:
                break
            if attempt < 3:
                log(f"{Fore.YELLOW}  ⚠ No warmup bars on attempt {attempt} — retrying in 5s...{Style.RESET_ALL}")
                await asyncio.sleep(5)
        if not warmup_bars:
            log(f"{Fore.RED}  ✖ Could not load warmup bars after 3 attempts. EMA/ATR will be NaN until enough live bars accumulate.{Style.RESET_ALL}")
        else:
            log(f"{Fore.GREEN}  ✔ Warmup bars loaded: {len(warmup_bars)} bars — EMA/ATR ready{Style.RESET_ALL}")

        # IBKR pacing: wait before making a second historical data request
        log("  Pausing 5s before live stream subscription (IBKR pacing)...")
        await asyncio.sleep(5)

        # ---------------------------------------------------------------
        # Stage 2: live-streaming bar subscription (keepUpToDate=True)
        # Short duration (4 hours) so the initial fetch is fast and
        # unlikely to timeout. Bar detection watches len(bars) change.
        # Error 162 on this request is NORMAL — it fires when IBKR
        # finishes the historical backfill and switches to live mode.
        # ---------------------------------------------------------------
        log("  Starting live 5-min bar stream (Error 162 below is normal)...")
        bars = await ib.reqHistoricalDataAsync(
            ndx, endDateTime="", durationStr="14400 S",
            barSizeSetting="5 mins", whatToShow="TRADES",
            useRTH=False, keepUpToDate=True,
        )
        log(f"  Live bar stream active: {len(bars)} recent bars")

        # -----------------------------------------------------------
        # IBKR disconnect / reconnect handler
        # Error 1100 = disconnected  (bars subscription freezes)
        # Error 1101 = reconnected, data LOST  → must resubscribe
        # Error 1102 = reconnected, data maintained → bar stream
        #              still freezes in ib_insync; resubscribe anyway
        # -----------------------------------------------------------
        def _on_ib_error(reqId, errorCode, errorString, contract):
            nonlocal _ib_needs_resubscribe
            if errorCode == 1100:
                log(
                    f"\n{Fore.YELLOW}{Style.BRIGHT}"
                    f"[IBKR] Connectivity lost (1100) — bar stream paused until reconnect."
                    f"{Style.RESET_ALL}"
                )
            elif errorCode in (1101, 1102):
                log(
                    f"\n{Fore.YELLOW}{Style.BRIGHT}"
                    f"[IBKR] Reconnected (code {errorCode}) — scheduling bar stream resubscribe."
                    f"{Style.RESET_ALL}"
                )
                _ib_needs_resubscribe = True

        ib.errorEvent += _on_ib_error
        _warmup_cutoff = warmup_bars[-1].date if warmup_bars else None
        ticker = ib.reqMktData(ndx, "", False, False)
        log(f"{Fore.GREEN}  ✔ Real-time NDX tick subscription active{Style.RESET_ALL}\n")

    elif DATA_FEED_SOURCE == "massive":
        from massive_feed import MassiveFeed
        _live_bars_offset = 0   # MassiveFeed.bars contains only completed bars
        log(f"\n{Fore.CYAN}Connecting to massive.com (Polygon.io) data feed...{Style.RESET_ALL}")
        feed = MassiveFeed(api_key=MASSIVE_API_KEY, ticker=MASSIVE_TICKER)

        # Wait until the current 5-min bar completes so the warmup fetch
        # returns only fully-closed bars (no in-progress partial candle).
        _now_utc       = datetime.now(UTC)
        _bar_start_utc = _now_utc.replace(
            minute=(_now_utc.minute // 5) * 5, second=0, microsecond=0
        )
        _next_bar_utc  = _bar_start_utc + timedelta(minutes=5)
        _wait_secs     = (_next_bar_utc - datetime.now(UTC)).total_seconds()
        if _wait_secs > 0:
            _next_ny = _next_bar_utc.astimezone(NY_TZ).strftime("%H:%M:%S")
            log(
                f"  Waiting {_wait_secs:.0f}s for current 5-min bar to complete "
                f"(next bar at {_next_ny} NY)..."
            )
            await asyncio.sleep(_wait_secs)
            log("  Bar complete — fetching warmup bars now.")

        log("  Fetching EMA/ATR warmup bars via Polygon REST API (last 2 RTH sessions)...")
        try:
            warmup_bars = await feed.fetch_warmup_bars_async(days=2)
            log(f"{Fore.GREEN}  ✔ Warmup bars loaded: {len(warmup_bars)} bars — EMA/ATR ready{Style.RESET_ALL}")
            if warmup_bars:
                _wb_first = warmup_bars[0].date.astimezone(NY_TZ).strftime("%Y-%m-%d %H:%M")
                _wb_last  = warmup_bars[-1].date.astimezone(NY_TZ).strftime("%Y-%m-%d %H:%M")
                _wb_c     = warmup_bars[-1].close
                log("  Warmup normalization: RTH-only, capped to last 156 completed 5-min bars")
                log(f"  Warmup range: {_wb_first} → {_wb_last} NY  |  last close={_wb_c:.2f}")
        except Exception as _wu_exc:
            log(f"{Fore.RED}  ✖ Warmup fetch failed: {_wu_exc} — EMA/ATR will be NaN until enough live bars accumulate.{Style.RESET_ALL}")
        _warmup_cutoff = warmup_bars[-1].date if warmup_bars else None
        feed.start()
        bars   = feed.bars   # live reference — grows as REST poll adds completed bars
        ticker = feed        # duck-typed: get_ndx_price() reads ticker.live_price
        # Seed feed.bars with the last warmup bar so the main loop fires diagnostics
        # immediately on startup (mirrors IBKR behaviour where bars are non-empty at start).
        # The poll thread deduplicates, so no double-counting risk.
        if warmup_bars:
            with feed._lock:
                feed.bars.append(warmup_bars[-1])
        log(f"{Fore.GREEN}  ✔ Massive WebSocket feed started — streaming {MASSIVE_TICKER}{Style.RESET_ALL}\n")

    else:
        log(f"{Fore.RED}Unknown data_feed.source '{DATA_FEED_SOURCE}' in config.yaml — expected 'ibkr' or 'massive'. Aborting.{Style.RESET_ALL}")
        return

    # --- Session info ---
    mode_str = (
        f"{Fore.YELLOW}{Style.BRIGHT}DRY RUN{Style.RESET_ALL}"
        if DRY_RUN else
        f"{Fore.RED}{Style.BRIGHT}LIVE ORDERS{Style.RESET_ALL}"
    )
    log(f"{Fore.CYAN}{'═' * 56}{Style.RESET_ALL}")
    log(f"  Mode         : {mode_str}")
    log(f"  Signal window: {signal_start_t.strftime('%H:%M')} – {session_end_t.strftime('%H:%M')} NY")
    log(f"  Trade window : {trade_start_t.strftime('%H:%M')} – {session_end_t.strftime('%H:%M')} NY")
    log(f"  EOD flatten  : {eod_exit_t.strftime('%H:%M')} NY")
    log(f"  Max trades   : {max_trades_per_day}/day")
    log(f"{Fore.CYAN}{'═' * 56}{Style.RESET_ALL}\n")
    print_kb_help()

    # --- State ---
    pending_signal:    dict | None = None
    algo_position:     dict | None = None
    last_signal_bar_dt             = None
    _pending_signal_id: int | None = None   # index into _signal_history for current pending
    prev_bar_count:    int         = 0
    trades_today_ny:   int         = 0
    current_day_ny                 = None
    eod_processed_day              = None
    eod_armed_day                  = None
    last_status_ts                 = None
    last_hud_len:      int         = 0
    live_price_cache:  float | None = None  # shared between HUD and keyboard handler
    prev_day_close:    float | None = None  # last close of the previous calendar day

    startup_now_ny = datetime.now(NY_TZ)
    if startup_now_ny.time() < eod_exit_t:
        eod_armed_day = startup_now_ny.date()

    try:
        while True:
            await asyncio.sleep(0.01)

            now_ny = datetime.now(NY_TZ)
            tnow   = now_ny.time()

            # -----------------------------------------------------------
            # IBKR reconnect: resubscribe frozen bar stream
            # -----------------------------------------------------------
            if DATA_FEED_SOURCE == "ibkr" and _ib_needs_resubscribe:
                _ib_needs_resubscribe = False
                log(f"\n{Fore.YELLOW}  ↻ Resubscribing to live 5-min bar stream...{Style.RESET_ALL}")
                try:
                    ib.cancelHistoricalData(bars)
                except Exception:
                    pass
                await asyncio.sleep(3)
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        ndx, endDateTime="", durationStr="14400 S",
                        barSizeSetting="5 mins", whatToShow="TRADES",
                        useRTH=False, keepUpToDate=True,
                    )
                    prev_bar_count = len(bars)
                    log(f"{Fore.GREEN}  ✔ Bar stream resubscribed: {len(bars)} bars{Style.RESET_ALL}")
                except Exception as _resub_exc:
                    log(
                        f"{Fore.RED}{Style.BRIGHT}"
                        f"  ✖ Bar stream resubscribe failed: {_resub_exc} — will retry on next reconnect."
                        f"{Style.RESET_ALL}"
                    )
                    _ib_needs_resubscribe = True  # retry on next loop tick

            # -----------------------------------------------------------
            # Daily reset
            # -----------------------------------------------------------
            if current_day_ny != now_ny.date():
                current_day_ny  = now_ny.date()
                trades_today_ny = 0
                eod_armed_day   = current_day_ny if tnow < eod_exit_t else None
                _sh_reset_if_new_day(current_day_ny)  # clear signal history for new day
                log(f"\n{Fore.CYAN}New trading day: {current_day_ny} | trade count reset{Style.RESET_ALL}")

            in_signal_window = _in_window(tnow, signal_start_t, session_end_t)
            in_trade_window  = _in_window(tnow, trade_start_t,  session_end_t)

            # -----------------------------------------------------------
            # Keyboard command processing
            # -----------------------------------------------------------
            try:
                while not _kb_queue.empty():
                    cmd = _kb_queue.get_nowait()
                    if cmd == "s":
                        print_position_status(algo_position, trades_today_ny, max_trades_per_day, live_price_cache)
                        print_kb_help()
                    elif cmd == "h":
                        _sh_reset_if_new_day(datetime.now(NY_TZ).date())
                        print_signal_history()
                        print_kb_help()
                    elif cmd == "e":
                        if algo_position is not None:
                            direction    = algo_position["direction"]
                            position_qty = int(algo_position.get("qty", default_order_qty))
                            entry_ndx    = algo_position["entry_ndx"]
                            px           = live_price_cache or 0.0
                            pnl          = (px - entry_ndx) if direction == "LONG" else (entry_ndx - px)
                            log(
                                f"\n{Fore.YELLOW}{Style.BRIGHT}[MANUAL EXIT] "
                                f"{direction} qty={position_qty} NDX={px:.2f} "
                                f"PnL={pnl:+.2f} pts{Style.RESET_ALL}"
                            )
                            if ENABLE_ORDERS and not DRY_RUN:
                                resp = await safe_place_exit(broker_client, direction, position_qty)
                                log(f"  MANUAL EXIT order response: {resp}")
                            else:
                                log("  DRY RUN MANUAL EXIT")
                            append_trade_log(
                                datetime.fromisoformat(algo_position["entry_time"]),
                                now_ny, direction, entry_ndx, px, "MANUAL", pnl,
                            )
                            await send_exit_telegram(direction, "MANUAL", entry_ndx, px, pnl, position_qty, now_ny)
                            algo_position  = None
                            if _pending_signal_id is not None:
                                _sh_update(_pending_signal_id, "EXPIRED", now_ny)
                                _pending_signal_id = None
                            pending_signal = None
                            clear_position_state()
                        else:
                            log(f"\n{Fore.YELLOW}No open position to exit{Style.RESET_ALL}")
                        print_kb_help()
                    elif cmd == "q":
                        log(
                            f"\n{Fore.YELLOW}{Style.BRIGHT}[Q] Quit requested — "
                            "exiting WITHOUT squaring off position.{Style.RESET_ALL}"
                        )
                        return
            except asyncio.QueueEmpty:
                pass

            # -----------------------------------------------------------
            # EOD at 15:30
            # -----------------------------------------------------------
            if (
                eod_armed_day == now_ny.date()
                and tnow >= eod_exit_t
                and eod_processed_day != now_ny.date()
            ):
                eod_processed_day = now_ny.date()
                log(f"\n{Fore.YELLOW}{Style.BRIGHT}EOD 15:30 — flattening all...{Style.RESET_ALL}")
                await safe_flatten(broker_client, ENABLE_ORDERS, DRY_RUN)
                # mark any still-pending signal as expired
                if _pending_signal_id is not None:
                    _sh_update(_pending_signal_id, "EXPIRED", now_ny)
                    _pending_signal_id = None
                algo_position  = None
                pending_signal = None
                clear_position_state()
                log(f"{Fore.YELLOW}EOD complete.{Style.RESET_ALL}")
                continue

            # -----------------------------------------------------------
            # Live NDX price
            # -----------------------------------------------------------
            live_price = get_ndx_price(ticker)
            if live_price is None:
                continue
            live_price_cache = live_price

            # Advance spinner on each tick we actually receive a price
            spinner = _tick_spinner()

            # -----------------------------------------------------------
            # Periodic status heartbeat
            # -----------------------------------------------------------
            if (
                last_status_ts is None
                or (now_ny - last_status_ts).total_seconds() >= STATUS_UPDATE_SECONDS
            ):
                last_status_ts = now_ny
                pos_s = (
                    f"{algo_position['direction']} entry={algo_position['entry_ndx']:.2f} "
                    f"sl={algo_position['sl']:.2f} target={algo_position['target']:.2f} "
                    f"step={algo_position['trail_step']} qty={algo_position['qty']}"
                    if algo_position else "FLAT"
                )
                pend_s = (
                    f"{pending_signal['direction']} entry={pending_signal['entry']:.2f}"
                    if pending_signal else "NONE"
                )
                log(
                    f"\n[HEARTBEAT {now_ny.strftime('%H:%M:%S')}] "
                    f"NDX={live_price:.2f}  trades={trades_today_ny}/{max_trades_per_day}  "
                    f"position={pos_s}  pending={pend_s}"
                )

            # -----------------------------------------------------------
            # New completed 5-min bar → signal detection
            # -----------------------------------------------------------
            current_bar_count = len(bars)
            if current_bar_count != prev_bar_count and current_bar_count >= 1:
                prev_bar_count = current_bar_count

                # Merge warmup + live bars for indicator computation.
                # Live bars may overlap with warmup; deduplicate by date.
                live_list = list(bars[:-1]) if _live_bars_offset else list(bars)  # IBKR: skip in-progress bar at [-1]; massive: all bars completed
                if warmup_bars and _warmup_cutoff is not None:
                    live_dates = {b.date for b in live_list}
                    extra = [b for b in warmup_bars if b.date not in live_dates]
                    combined = sorted(extra + live_list, key=lambda b: b.date)
                else:
                    combined = live_list
                df_raw = bars_to_df(combined)
                df_ind = compute_indicators(df_raw)
                sig    = get_latest_signal(df_ind)

                # Track prev-day close for gap-trail detection
                today_date = now_ny.date()
                prev_rows = df_raw[df_raw["datetime_ny"].dt.date < today_date]
                if not prev_rows.empty:
                    prev_day_close = float(prev_rows.iloc[-1]["Close"])

                last_bar  = df_raw.iloc[-1]
                ind_row   = df_ind.iloc[-1]
                log(
                    f"\n[{now_ny.strftime('%H:%M:%S')}] BAR CLOSED "
                    f"{last_bar['datetime_ny']} | "
                    f"O={last_bar['Open']:.2f}  H={last_bar['High']:.2f}  "
                    f"L={last_bar['Low']:.2f}  C={last_bar['Close']:.2f} | "
                    f"NDX tick={live_price:.2f}"
                )

                # --- Per-bar candle diagnostics box ---
                _ema_val   = ind_row["ema"]       if not pd.isna(ind_row["ema"])       else float("nan")
                _atr_val   = ind_row["atr14"]     if not pd.isna(ind_row["atr14"])     else float("nan")
                _slope_val = ind_row["slope_atr"] if not pd.isna(ind_row["slope_atr"]) else float("nan")
                _bd_mult   = float(strategy_cfg.get("bounce_distance_atr_mult", 1.2))
                _sl_mult   = float(strategy_cfg.get("sl_atr_mult",              0.8))
                _rcap      = float(strategy_cfg.get("risk_cap_points",          60.0))
                _long_sl   = float(strategy_cfg.get("long_slope",               0.2))
                _short_sl  = float(strategy_cfg.get("short_slope",             -0.4))
                _min_body  = float(strategy_cfg.get("min_body_points",          5.0))
                _ema_len   = int(strategy_cfg.get("ema_length", 14))

                _bounce    = _atr_val * _bd_mult if not pd.isna(_atr_val) else float("nan")
                _sl_dist   = _atr_val * _sl_mult  if not pd.isna(_atr_val) else float("nan")
                _body_len  = abs(last_bar["Close"] - last_bar["Open"])
                _h_ema     = last_bar["High"] - _ema_val
                _ema_l     = _ema_val - last_bar["Low"]

                if   not pd.isna(_slope_val) and _slope_val > _long_sl:
                    _regime_str = "BULLISH"; _regime_col = Fore.GREEN + Style.BRIGHT
                elif not pd.isna(_slope_val) and _slope_val < _short_sl:
                    _regime_str = "BEARISH"; _regime_col = Fore.RED   + Style.BRIGHT
                else:
                    _regime_str = "NEUTRAL"; _regime_col = Fore.WHITE + Style.BRIGHT

                _C = Fore.CYAN
                _R = Style.RESET_ALL
                _D = Style.DIM + Fore.CYAN
                log(
                    f"  {_C}\u250c\u2500 CANDLE DIAGNOSTICS {chr(10004) if sig else '':{'' if sig else ''}}{'\u2500' * 42}\u2510{_R}\n"
                    f"  {_C}\u2502  EMA-{_ema_len:<2}   : {_ema_val:>10.2f}{_R}\n"
                    f"  {_C}\u2502  ATR-14   : {_atr_val:>10.2f} pts{_R}\n"
                    f"  {_C}\u2502  Slope    : {_slope_val:>+10.3f}   "
                    f"{_D}[bullish >{_long_sl:+.3f} | bearish <{_short_sl:+.3f}]{_R}  \u2192  "
                    f"{_regime_col}{_regime_str}{_R}\n"
                    f"  {_C}\u2502  Body     : {_body_len:>10.2f} pts   {_D}(min {_min_body:.2f} required){_R}\n"
                    f"  {_C}\u2502  Bounce   : {_bounce:>10.2f} pts   {_D}= {_bd_mult:.1f} \u00d7 {_atr_val:.2f} ATR{_R}\n"
                    f"  {_C}\u2502  H\u2212EMA  : {_h_ema:>+10.2f} pts   {_D}(candle high vs EMA; negative = high below EMA){_R}\n"
                    f"  {_C}\u2502  EMA\u2212L  : {_ema_l:>+10.2f} pts   {_D}(EMA vs candle low;  negative = low above EMA){_R}\n"
                    f"  {_C}\u2502  SL base  : {_sl_dist:>10.2f} pts   {_D}= {_sl_mult:.1f} \u00d7 {_atr_val:.2f} ATR{_R}\n"
                    f"  {_C}\u2502  Risk cap : {_rcap:>10.2f} pts{_R}\n"
                    f"  {_C}\u2514{'\u2500' * 64}\u2518{_R}"
                )

                if sig:
                    sig_dt  = sig["signal_dt"]
                    sig_day = pd.Timestamp(sig_dt).date()

                    if sig_day != now_ny.date():
                        pass  # stale historical signal — ignore silently

                    elif str(sig_dt) == str(last_signal_bar_dt):
                        pass  # already processed

                    else:
                        last_signal_bar_dt = sig_dt

                        if not in_signal_window:
                            log(f"  Signal outside signal window — ignored")
                        elif algo_position is not None:
                            log(f"{Fore.YELLOW}  Signal suppressed — position already open{Style.RESET_ALL}")
                            _sh_add(sig, "SUPPRESSED", now_ny)
                        elif trades_today_ny >= max_trades_per_day:
                            log(f"{Fore.YELLOW}  Signal suppressed — max trades ({max_trades_per_day}) reached{Style.RESET_ALL}")
                            _sh_add(sig, "SUPPRESSED", now_ny)
                        else:
                            forced_qty = sig.get("force_qty")
                            signal_qty = (
                                int(forced_qty)
                                if forced_qty is not None
                                else qty_from_risk(float(sig["risk"]))
                            ) * qty_multiplier
                            # expire previous pending signal if still alive
                            if _pending_signal_id is not None:
                                _sh_update(_pending_signal_id, "EXPIRED", now_ny)
                            sig["qty"] = signal_qty
                            _pending_signal_id = _sh_add(sig, "PENDING", now_ny)
                            pending_signal       = sig
                            pending_signal["qty"] = signal_qty

                            # Prominent signal box
                            print_signal_box(sig, signal_qty)
                            log(
                                f"  SIGNAL {sig['direction']} | "
                                f"entry={sig['entry']:.2f}  sl={sig['sl']:.2f}  "
                                f"target={sig['target']:.2f}  risk={sig['risk']:.2f} pts  qty={signal_qty}"
                            )
                            if not in_trade_window:
                                log(f"  Queued — entry monitoring starts at {trade_start_t.strftime('%H:%M')} NY")

                            await send_signal_telegram(sig, signal_qty)

            # -----------------------------------------------------------
            # Entry trigger
            # -----------------------------------------------------------
            if pending_signal is not None and algo_position is None and in_trade_window:
                direction   = pending_signal["direction"]
                entry_level = pending_signal["entry"]
                entry_qty   = int(pending_signal.get("qty", default_order_qty))

                triggered = (
                    (direction == "SHORT" and live_price <= entry_level) or
                    (direction == "LONG"  and live_price >= entry_level)
                )

                # --- Over-chase guard ---
                # If price has run more than max_entry_deviation_pts past the
                # entry level the risk/reward is degraded; cancel the signal.
                _max_dev = float(strategy_cfg.get("max_entry_deviation_pts", 15.0))
                if triggered:
                    _deviation = (
                        live_price - entry_level if direction == "LONG"
                        else entry_level - live_price
                    )
                    if _deviation > _max_dev:
                        log(
                            f"\n{Fore.YELLOW}{Style.BRIGHT}"
                            f"[{now_ny.strftime('%H:%M:%S')}] SIGNAL CANCELLED — price ran too far past entry\n"
                            f"  {direction} entry={entry_level:.2f}  NDX={live_price:.2f}  "
                            f"deviation={_deviation:+.2f} pts  max={_max_dev:.1f} pts"
                            f"{Style.RESET_ALL}"
                        )
                        _sh_update(_pending_signal_id, "EXPIRED", now_ny)
                        _pending_signal_id = None
                        pending_signal = None
                        triggered = False

                if triggered:
                    trigger_ndx      = float(live_price)
                    trigger_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
                    submit_epoch_ms  = None

                    log(
                        f"\n{Fore.YELLOW}{Style.BRIGHT}"
                        f"[{now_ny.strftime('%H:%M:%S')}] ENTRY TRIGGERED | "
                        f"{direction} | NDX={trigger_ndx:.2f} | level={entry_level:.2f}"
                        f"{Style.RESET_ALL}"
                    )

                    filled = False
                    if not ENABLE_ORDERS or DRY_RUN:
                        log(f"  DRY RUN ENTRY: {direction} {entry_qty}")
                        filled = True
                    else:
                        if broker_client.name == "ninjatrader":
                            log(
                                f"  ORDER → NinjaTrader | ENTRY|{direction}|{entry_qty} | "
                                f"{broker_client.instrument} | {broker_client.host}:{broker_client.port}"
                            )
                        submit_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
                        resp = await safe_place_entry(broker_client, direction, entry_qty)
                        log(f"  ENTRY ORDER response: {resp}")
                        filled = resp.get("success", False)

                        if filled and broker_client.name == "ninjatrader":
                            fill_data = await _fetch_ninja_fill_after(broker_client, trigger_epoch_ms)
                            if fill_data is not None:
                                fill_px = _safe_float(fill_data.get("price"))
                                log(f"  NINJA FILL (ENTRY): {fill_data}")
                                if fill_px is not None and submit_epoch_ms:
                                    fill_epoch_ms = _safe_int(fill_data.get("epoch_ms"))
                                    if fill_epoch_ms:
                                        log(
                                            f"  ENTRY LATENCY: "
                                            f"trigger→fill {fill_epoch_ms - trigger_epoch_ms} ms  "
                                            f"submit→fill {fill_epoch_ms - submit_epoch_ms} ms"
                                        )
                            else:
                                log("  NINJA FILL (ENTRY): not captured within window")

                    if filled:
                        algo_position = {
                            "direction":  direction,
                            "entry_ndx":  trigger_ndx,
                            "sl":         float(pending_signal["sl"]),
                            "target":     float(pending_signal["target"]),
                            "risk":       float(pending_signal["risk"]),
                            "qty":        entry_qty,
                            "trail_step": 0,
                            "entry_time": now_ny.isoformat(),
                            "gap_trail": (
                                prev_day_close is not None
                                and abs(trigger_ndx - prev_day_close) / prev_day_close > 0.015
                                and float(pending_signal["risk"]) > 60.0
                            ),
                        }
                        trades_today_ny += 1
                        save_position_state(algo_position)
                        _sh_update(_pending_signal_id, "TRIGGERED", now_ny, fill_price=trigger_ndx)
                        _pending_signal_id = None
                        pending_signal = None
                        await send_entry_telegram(
                            direction, trigger_ndx,
                            algo_position["sl"], algo_position["target"],
                            algo_position["risk"], entry_qty, now_ny,
                        )

            # -----------------------------------------------------------
            # Exit monitoring — SL / TSL / TP
            # -----------------------------------------------------------
            if algo_position is not None:
                direction    = algo_position["direction"]
                position_qty = int(algo_position.get("qty", default_order_qty))
                entry_ndx    = algo_position["entry_ndx"]
                sl           = algo_position["sl"]
                target       = algo_position["target"]
                risk         = algo_position["risk"]
                trail_step   = algo_position["trail_step"]

                # Update trailing stop
                new_sl, new_step = sl, trail_step
                if algo_position.get("gap_trail"):
                    # Fixed-point gap-trail schedule (activated when entry > 1.5% from prev-day close AND risk > 60 pts)
                    # Steps: move (pts in favour) → lock SL at (pts from entry)
                    # 100 → 40,  150 → 80,  200 → 100,  260 → 130,  320 → 200
                    _GAP_SCHEDULE = [(320, 200), (260, 130), (200, 100), (150, 80), (100, 40)]
                    if direction == "LONG":
                        move = live_price - entry_ndx
                        for _trigger, _lock in _GAP_SCHEDULE:
                            if move >= _trigger:
                                new_sl   = max(sl, entry_ndx + _lock)
                                new_step = max(trail_step, _GAP_SCHEDULE.index((_trigger, _lock)) + 1)
                                break
                    else:
                        move = entry_ndx - live_price
                        for _trigger, _lock in _GAP_SCHEDULE:
                            if move >= _trigger:
                                new_sl   = min(sl, entry_ndx - _lock)
                                new_step = max(trail_step, _GAP_SCHEDULE.index((_trigger, _lock)) + 1)
                                break
                else:
                    if direction == "LONG":
                        fr = (live_price - entry_ndx) / risk
                        if   fr >= 4.0: new_sl = max(sl, entry_ndx + 3.0 * risk); new_step = max(trail_step, 3)
                        elif fr >= 3.0: new_sl = max(sl, entry_ndx + 2.0 * risk); new_step = max(trail_step, 2)
                        elif fr >= 2.0: new_sl = max(sl, entry_ndx + 0.5 * risk); new_step = max(trail_step, 1)
                    else:
                        fr = (entry_ndx - live_price) / risk
                        if   fr >= 4.0: new_sl = min(sl, entry_ndx - 3.0 * risk); new_step = max(trail_step, 3)
                        elif fr >= 3.0: new_sl = min(sl, entry_ndx - 2.0 * risk); new_step = max(trail_step, 2)
                        elif fr >= 2.0: new_sl = min(sl, entry_ndx - 0.5 * risk); new_step = max(trail_step, 1)

                if new_step > trail_step:
                    algo_position["sl"]         = new_sl
                    algo_position["trail_step"] = new_step
                    save_position_state(algo_position)
                    if algo_position.get("gap_trail"):
                        _move = live_price - entry_ndx if direction == "LONG" else entry_ndx - live_price
                        log(
                            f"\n{Fore.YELLOW}[{now_ny.strftime('%H:%M:%S')}] TSL MOVED (GAP) | "
                            f"{direction} | move={_move:.1f} pts | sl: {sl:.2f} → {new_sl:.2f} | "
                            f"step {trail_step}→{new_step}{Style.RESET_ALL}"
                        )
                    else:
                        _fr = (live_price - entry_ndx) / risk if direction == "LONG" else (entry_ndx - live_price) / risk
                        log(
                            f"\n{Fore.YELLOW}[{now_ny.strftime('%H:%M:%S')}] TSL MOVED | "
                            f"{direction} | {_fr:.2f}R | sl: {sl:.2f} → {new_sl:.2f} | "
                            f"step {trail_step}→{new_step}{Style.RESET_ALL}"
                        )
                    sl, trail_step = new_sl, new_step

                # Check exit
                exit_reason: str | None = None
                exit_ndx:    float | None = None
                if direction == "LONG":
                    if   live_price <= sl:     exit_reason = "TSL" if trail_step > 0 else "SL"; exit_ndx = sl
                    elif live_price >= target: exit_reason = "TP";                               exit_ndx = target
                else:
                    if   live_price >= sl:     exit_reason = "TSL" if trail_step > 0 else "SL"; exit_ndx = sl
                    elif live_price <= target: exit_reason = "TP";                               exit_ndx = target

                if exit_reason:
                    pnl   = (exit_ndx - entry_ndx) if direction == "LONG" else (entry_ndx - exit_ndx)
                    col   = Fore.GREEN if pnl >= 0 else Fore.RED
                    trigger_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
                    submit_epoch_ms  = None

                    log(
                        f"\n{col}{Style.BRIGHT}"
                        f"[{now_ny.strftime('%H:%M:%S')}] EXIT | {exit_reason} | "
                        f"{direction} | NDX={live_price:.2f} | level={exit_ndx:.2f} | "
                        f"PnL={pnl:+.2f} pts"
                        f"{Style.RESET_ALL}"
                    )

                    if not ENABLE_ORDERS or DRY_RUN:
                        log(f"  DRY RUN EXIT: {direction} {position_qty}")
                    else:
                        if broker_client.name == "ninjatrader":
                            log(
                                f"  ORDER → NinjaTrader | EXIT|{direction}|{position_qty} | "
                                f"{broker_client.instrument} | {broker_client.host}:{broker_client.port}"
                            )
                        submit_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
                        resp = await safe_place_exit(broker_client, direction, position_qty)
                        log(f"  EXIT ORDER response: {resp}")

                        if resp.get("success", False) and broker_client.name == "ninjatrader":
                            fill_data = await _fetch_ninja_fill_after(broker_client, trigger_epoch_ms)
                            if fill_data is not None:
                                fill_px       = _safe_float(fill_data.get("price"))
                                fill_epoch_ms = _safe_int(fill_data.get("epoch_ms"))
                                log(f"  NINJA FILL (EXIT): {fill_data}")
                                if fill_epoch_ms and submit_epoch_ms:
                                    log(
                                        f"  EXIT LATENCY: "
                                        f"trigger→fill {fill_epoch_ms - trigger_epoch_ms} ms  "
                                        f"submit→fill {fill_epoch_ms - submit_epoch_ms} ms"
                                    )
                            else:
                                log("  NINJA FILL (EXIT): not captured within window")

                    append_trade_log(
                        datetime.fromisoformat(algo_position["entry_time"]),
                        now_ny, direction, entry_ndx, exit_ndx, exit_reason, pnl,
                    )
                    await send_exit_telegram(direction, exit_reason, entry_ndx, exit_ndx, pnl, position_qty, now_ny)
                    algo_position = None
                    clear_position_state()

            # -----------------------------------------------------------
            # HUD — single overwriting line with spinner
            # -----------------------------------------------------------
            pos_str  = (
                f"{'LONG' if algo_position['direction'] == 'LONG' else 'SHORT'} "
                f"entry={algo_position['entry_ndx']:.1f}  "
                f"sl={algo_position['sl']:.1f}  "
                f"tgt={algo_position['target']:.1f}  "
                f"R={((live_price - algo_position['entry_ndx']) / algo_position['risk'] if algo_position['direction'] == 'LONG' else (algo_position['entry_ndx'] - live_price) / algo_position['risk']):.2f}"
                if algo_position else "FLAT"
            )
            pend_str = (
                f"{pending_signal['direction']}@{pending_signal['entry']:.1f}"
                if pending_signal else "—"
            )
            pos_col  = (
                (Fore.GREEN if algo_position["direction"] == "LONG" else Fore.RED)
                if algo_position else Fore.WHITE
            )
            hud = (
                f"\r{Fore.CYAN}[NDX]{Style.RESET_ALL}{spinner} "
                f"{Fore.LIGHTYELLOW_EX}{Style.BRIGHT}{live_price:,.2f}{Style.RESET_ALL} | "
                f"[{now_ny.strftime('%H:%M:%S')}] | "
                f"trades={trades_today_ny}/{max_trades_per_day} | "
                f"{pos_col}{pos_str}{Style.RESET_ALL} | "
                f"pend={pend_str}"
            )
            padding = max(0, last_hud_len - len(hud))
            if not _confirm_active:
                print(hud + " " * padding, end="", flush=True)
            last_hud_len = len(hud)

    except KeyboardInterrupt:
        log(f"\n[{datetime.now(NY_TZ).strftime('%H:%M:%S')}] KeyboardInterrupt — flattening all...")
        await safe_flatten(broker_client, ENABLE_ORDERS, DRY_RUN)
        clear_position_state()
        log("Stopped cleanly.")

    except Exception as e:
        log(f"\n[{datetime.now(NY_TZ).strftime('%H:%M:%S')}] FATAL ERROR: {e}")
        log("Emergency flatten attempt...")
        await safe_flatten(broker_client, ENABLE_ORDERS, DRY_RUN)
        clear_position_state()
        raise

    finally:
        try:
            if DATA_FEED_SOURCE == "ibkr" and ib is not None:
                ib.disconnect()
                log("IBKR disconnected.")
            elif DATA_FEED_SOURCE == "massive" and feed is not None:
                feed.stop()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(run())
