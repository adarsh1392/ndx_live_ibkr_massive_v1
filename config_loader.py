import os
from functools import lru_cache
from typing import Any, Dict

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # graceful fallback if PyYAML is unavailable

DEFAULT_CONFIG: Dict[str, Any] = {
    "strategy": {
        "ema_length": 10,
        "ema_angle_lookback": 3,
        "angle_scale_per_bar": 5.0,
        "short_min_angle": -25.0,
        "long_min_angle": 40.0,
        "bounce_max_points": 25.0,
        "entry_offset_points": 5.0,
        "risk_cap_points": 50.0,
        "rr_multiple": 5.0,
        "session_start": "18:00",
        "session_end": "15:00",
        "max_trades_per_day": 2,
    },
    "hud": {
        "poll_seconds": 15,
    },
    "orders": {
        "order_expiry_minutes": 30,
        "order_qty": 1,
        "flatten_position_at_end": True,
        "cancel_orders_at_end": True,
        "end_action_grace_seconds": 0,
    },
}


def _read_yaml_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    if yaml is None:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


@lru_cache(maxsize=1)
def get_config() -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    user_cfg = _read_yaml_config(cfg_path)
    return _deep_merge(DEFAULT_CONFIG, user_cfg)
