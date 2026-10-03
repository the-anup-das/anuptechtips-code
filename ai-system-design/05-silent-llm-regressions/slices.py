"""Per-slice regression check: is a slice's pass rate below the baseline, beyond chance?"""
import math
from typing import NamedTuple

import psycopg

DIMENSIONS = ("pool", "hardware", "batch_bucket", "model_version", "prompt_version",
              "client_version")
METRICS = ("passed", "script_ok")


def wilson_bounds(passed: int, total: int, z: float = 3.0) -> tuple[float, float]:
    """Wilson score interval for a pass rate. Unlike p ± z·se it stays sane at small n and
    near 0% or 100%. z = 3 is roughly a 1-in-740 false alarm per check, on each side."""
    if total == 0:
        return 0.0, 1.0
    p = passed / total
    denom = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denom
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denom
    return centre - half, centre + half


class SliceAlert(NamedTuple):
    key: tuple[str, ...]    # the slice, e.g. ("long-context",); () is all traffic
    passed: int
    total: int
    upper: float            # the top of the slice's confidence interval


def degraded_slices(conn: psycopg.Connection, suite: str, baseline: float, *,
                    by: tuple[str, ...] = ("pool",), ticks: tuple[int, int] = (0, 2**31 - 1),
                    metric: str = "passed", tolerance: float = 0.01, min_n: int = 30,
                    z: float = 3.0) -> list[SliceAlert]:
    """Slices that sit more than `tolerance` below `baseline` even at the top of their
    confidence interval. `by=()` checks all traffic as one slice: the global alert."""
    if not set(by) <= set(DIMENSIONS) or metric not in METRICS:
        raise ValueError("unknown slice dimension or metric")
    cols = ", ".join(by)  # safe to interpolate: checked against the lists above
    rows = conn.execute(
        f"SELECT {cols + ', ' if by else ''}sum({metric}), sum(total) FROM slice_stats "
        f"WHERE suite = %s AND tick BETWEEN %s AND %s {'GROUP BY ' + cols if by else ''}",
        (suite, *ticks),
    ).fetchall()
    alerts = []
    for *key, passed, total in rows:
        if not total or total < min_n:
            continue                    # too few replies to page anyone
        upper = wilson_bounds(passed, total, z)[1]
        if upper < baseline - tolerance:
            alerts.append(SliceAlert(tuple(key), passed, total, upper))
    return sorted(alerts, key=lambda a: a.upper)
