"""Formatting helpers and misc utilities for the SARC GUI."""
from __future__ import annotations

from datetime import datetime


def fmt_datetime(dt: datetime | None) -> str:
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_elapsed(seconds: float | None) -> str:
    if seconds is None or seconds != seconds:  # NaN check
        return "N/A"
    try:
        secs = int(seconds)
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        return f"{h}h {m}m {s}s"
    except (TypeError, ValueError):
        return "N/A"


def fmt_memory(mem_mb: int | None) -> str:
    if mem_mb is None:
        return "N/A"
    if mem_mb >= 1024:
        return f"{mem_mb / 1024:.1f} GB"
    return f"{mem_mb} MB"


def fmt_float(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        if v != v:  # NaN check
            return "N/A"
        return f"{v:.2f}"
    except (TypeError, ValueError):
        return "N/A"


def get_member_type(user_data) -> str:
    """Safely extract current member type from a ValidField."""
    try:
        mt = user_data.member_type
        if mt is None:
            return "N/A"
        val = mt.get_value()
        if val is None:
            return "N/A"
        if hasattr(val, "value"):
            return val.value
        return str(val)
    except Exception:
        try:
            if user_data.member_type.values:
                last = user_data.member_type.values[-1]
                v = last.value
                if hasattr(v, "value"):
                    return v.value
                return str(v)
        except Exception:
            pass
        return "N/A"
