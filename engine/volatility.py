"""
railrisk/engine/volatility.py
-----------------------------
Calculates delay drift acceleration and variance across chronological snapshots.
"""

from datetime import datetime, timezone
from typing import List, Optional
from railrisk.models.train import LiveTrainStatus


def _extract_timestamp(snapshot: LiveTrainStatus) -> datetime:
    """
    Extracts a timezone-aware observation timestamp from a status snapshot.
    Falls back to scheduled_arrival if an explicit captured_at is not present.
    """
    dt = getattr(snapshot, "captured_at", None) or snapshot.scheduled_arrival
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def calculate_delay_drift(snapshots: List[LiveTrainStatus]) -> float:
    """
    Measures delay variance acceleration/deceleration over time ($\Delta \\text{delay} / \\Delta t$).

    Returns:
        float: Drift rate in minutes per hour.
               - Positive (> 0): Train is accumulating further delays.
               - Zero (0.0): Delay is stable.
               - Negative (< 0): Train is making up time rapidly (earlier arrival).
    """
    if len(snapshots) < 2:
        return 0.0

    # Ensure snapshots are strictly ordered chronologically by timestamp
    sorted_snapshots = sorted(snapshots, key=_extract_timestamp)

    first = sorted_snapshots[0]
    last = sorted_snapshots[-1]

    t_first = _extract_timestamp(first)
    t_last = _extract_timestamp(last)

    # Datetime subtraction across midnight boundaries automatically yields
    # elapsed time via Python's timedelta
    elapsed_seconds = (t_last - t_first).total_seconds()

    # Prevent negative durations (clock skew) or division by zero
    if elapsed_seconds <= 0:
        return 0.0

    elapsed_hours = elapsed_seconds / 3600.0
    delay_change_mins = float(last.delay_minutes - first.delay_minutes)

    drift_rate = delay_change_mins / elapsed_hours
    return round(drift_rate, 2)