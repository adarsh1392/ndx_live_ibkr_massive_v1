import os
import numpy as np
import pandas as pd
from datetime import time
from colorama import Fore, Style, init as colorama_init
from config_loader import get_config

TICK_SIZE = 0.25


def points_to_ticks(points: float) -> int:
    return max(1, round(points / TICK_SIZE))


def _bounce_distance_points(
    atr14: float,
    slope_atr: float,
    direction: str,
    normal_mult: float,
    steep_slope_threshold: float = 1.0,
    steep_mult: float = 1.6,
) -> float:
    steep_enabled = steep_slope_threshold > 0 and steep_mult > 0
    steep_short = direction == "SHORT" and slope_atr <= -steep_slope_threshold
    steep_long  = direction == "LONG"  and slope_atr >=  steep_slope_threshold
    if steep_enabled and (steep_short or steep_long):
        return atr14 * steep_mult
    return atr14 * normal_mult


def _allowed_risk_cap_points(
    atr14: float,
    slope_atr: float,
    direction: str,
    normal_cap: float,
    steep_slope_threshold: float = 1.0,
    steep_risk_atr_mult: float = 2.0,
    steep_risk_hard_cap: float = 90.0,
) -> tuple[float, bool]:
    steep_enabled = (
        steep_slope_threshold > 0
        and steep_risk_atr_mult > 0
        and steep_risk_hard_cap > normal_cap
    )
    steep_short = direction == "SHORT" and slope_atr <= -steep_slope_threshold
    steep_long  = direction == "LONG"  and slope_atr >=  steep_slope_threshold
    if not (steep_enabled and (steep_short or steep_long)):
        return normal_cap, False
    steep_cap   = min(steep_risk_atr_mult * atr14, steep_risk_hard_cap)
    allowed_cap = max(normal_cap, steep_cap)
    return allowed_cap, allowed_cap > normal_cap


def _parse_time_window(
    cfg: dict,
    start_key: str,
    default_start: str,
    end_key: str = "session_end",
    default_end: str = "15:00",
) -> tuple[time, time]:
    start_raw = str(cfg.get(start_key, cfg.get("session_start", default_start)))
    end_raw = str(cfg.get(end_key, default_end))
    try:
        sh, sm = [int(x) for x in start_raw.split(":")]
        eh, em = [int(x) for x in end_raw.split(":")]
    except Exception:
        fallback_sh, fallback_sm = [int(x) for x in default_start.split(":")]
        fallback_eh, fallback_em = [int(x) for x in default_end.split(":")]
        return time(fallback_sh, fallback_sm), time(fallback_eh, fallback_em)
    return time(sh, sm), time(eh, em)


def _ensure_log_dir():
    os.makedirs("logs", exist_ok=True)


_risk_reject_seen: set[tuple[str, str]] = set()


def _risk_log(message: str, dt, direction: str):
    key = (str(dt), direction)
    if key in _risk_reject_seen:
        return
    _risk_reject_seen.add(key)
    _ensure_log_dir()
    date_str = dt.date().isoformat()
    filename = f"log_mnq_{date_str}.log"
    path = os.path.join("logs", filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    try:
        colorama_init(autoreset=True)
    except Exception:
        pass
    print("\n" + Style.BRIGHT + Fore.YELLOW + message + Style.RESET_ALL)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    required = ["datetime_ny", "Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df["datetime_ny"] = pd.to_datetime(df["datetime_ny"])
    df = df.sort_values("datetime_ny").reset_index(drop=True)

    cfg = get_config().get("strategy", {})
    ema_length = int(cfg.get("ema_length", 14))
    ema_bar_difference = int(cfg.get("ema_bar_difference", 6))

    df["ema"] = df["Close"].ewm(span=ema_length, adjust=False).mean()

    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [
            (df["High"] - df["Low"]).abs(),
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()  # SMA smoothing

    df["slope_atr"] = (df["ema"] - df["ema"].shift(ema_bar_difference)) / df["atr14"]

    df["body"] = (df["Close"] - df["Open"]).abs()
    return df


def get_latest_signal(df: pd.DataFrame):
    if len(df) < 20:
        return None

    row = df.iloc[-1]
    t = row["datetime_ny"].time()

    cfg = get_config().get("strategy", {})
    start_t, end_t = _parse_time_window(
        cfg,
        start_key="signal_start",
        default_start="18:00",
    )
    if start_t <= end_t:
        in_window = (start_t <= t <= end_t)
    else:
        in_window = (t >= start_t) or (t <= end_t)
    if not in_window:
        return None

    slope_atr = row["slope_atr"]
    atr14 = row["atr14"]
    ema = row["ema"]
    o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]

    if pd.isna(slope_atr) or pd.isna(atr14) or pd.isna(ema):
        return None

    short_slope = float(cfg.get("short_slope", -0.4))
    long_slope = float(cfg.get("long_slope", 0.2))
    bounce_distance_atr_mult = float(cfg.get("bounce_distance_atr_mult", 1.2))
    sl_atr_mult = float(cfg.get("sl_atr_mult", 0.8))
    entry_off = float(cfg.get("entry_offset_points", 5.0))
    risk_cap = float(cfg.get("risk_cap_points", 60.0))
    rr_mult = float(cfg.get("rr_multiple", 5.0))
    min_body = float(cfg.get("min_body_points", 5.0))
    steep_slope_threshold          = float(cfg.get("steep_slope_threshold",          1.0))
    steep_risk_atr_mult            = float(cfg.get("steep_risk_atr_mult",            2.0))
    steep_risk_hard_cap            = float(cfg.get("steep_risk_hard_cap",            90.0))
    steep_bounce_distance_atr_mult = float(cfg.get("steep_bounce_distance_atr_mult", 1.6))
    body = float(row["body"])

    bearish_regime = slope_atr < short_slope
    bullish_regime = slope_atr > long_slope
    short_bounce_distance = _bounce_distance_points(
        atr14=float(atr14),
        slope_atr=float(slope_atr),
        direction="SHORT",
        normal_mult=bounce_distance_atr_mult,
        steep_slope_threshold=steep_slope_threshold,
        steep_mult=steep_bounce_distance_atr_mult,
    )
    long_bounce_distance = _bounce_distance_points(
        atr14=float(atr14),
        slope_atr=float(slope_atr),
        direction="LONG",
        normal_mult=bounce_distance_atr_mult,
        steep_slope_threshold=steep_slope_threshold,
        steep_mult=steep_bounce_distance_atr_mult,
    )

    if (
        bearish_regime
        and c > o
        and l < ema
        and (ema - h) < short_bounce_distance
        and (c - ema) < 0
        and body >= min_body
    ):
        entry = l - entry_off
        sl = max(ema, entry + sl_atr_mult * atr14)
        risk = sl - entry
        allowed_risk_cap, used_steep_risk_cap = _allowed_risk_cap_points(
            atr14=float(atr14),
            slope_atr=float(slope_atr),
            direction="SHORT",
            normal_cap=risk_cap,
            steep_slope_threshold=steep_slope_threshold,
            steep_risk_atr_mult=steep_risk_atr_mult,
            steep_risk_hard_cap=steep_risk_hard_cap,
        )
        if 0 < risk <= allowed_risk_cap:
            target = entry - (rr_mult * risk)
            return {
                "direction": "SHORT",
                "signal_dt": row["datetime_ny"],
                "entry": float(entry),
                "sl": float(sl),
                "target": float(target),
                "sl_ticks": points_to_ticks(risk),
                "tp_ticks": points_to_ticks(rr_mult * risk),
                "risk": float(risk),
                "force_qty": 1 if used_steep_risk_cap and risk > risk_cap else None,
                "allowed_risk_cap": float(allowed_risk_cap),
                "steep_risk_cap_used": bool(used_steep_risk_cap),
            }
        else:
            dt = row["datetime_ny"]
            msg = (
                f"[{dt}] SHORT signal rejected: risk {risk:.2f} pts "
                f"outside allowed range (0, {allowed_risk_cap:.2f}]"
            )
            _risk_log(msg, dt, "SHORT")

    if (
        bullish_regime
        and c < o
        and h > ema
        and (l - ema) < long_bounce_distance
        and (c - ema) > 0
        and body >= min_body
    ):
        entry = h + entry_off
        sl = min(ema, entry - sl_atr_mult * atr14)
        risk = entry - sl
        allowed_risk_cap, used_steep_risk_cap = _allowed_risk_cap_points(
            atr14=float(atr14),
            slope_atr=float(slope_atr),
            direction="LONG",
            normal_cap=risk_cap,
            steep_slope_threshold=steep_slope_threshold,
            steep_risk_atr_mult=steep_risk_atr_mult,
            steep_risk_hard_cap=steep_risk_hard_cap,
        )
        if 0 < risk <= allowed_risk_cap:
            target = entry + (rr_mult * risk)
            return {
                "direction": "LONG",
                "signal_dt": row["datetime_ny"],
                "entry": float(entry),
                "sl": float(sl),
                "target": float(target),
                "sl_ticks": points_to_ticks(risk),
                "tp_ticks": points_to_ticks(rr_mult * risk),
                "risk": float(risk),
                "force_qty": 1 if used_steep_risk_cap and risk > risk_cap else None,
                "allowed_risk_cap": float(allowed_risk_cap),
                "steep_risk_cap_used": bool(used_steep_risk_cap),
            }
        else:
            dt = row["datetime_ny"]
            msg = (
                f"[{dt}] LONG signal rejected: risk {risk:.2f} pts "
                f"outside allowed range (0, {allowed_risk_cap:.2f}]"
            )
            _risk_log(msg, dt, "LONG")

    return None
