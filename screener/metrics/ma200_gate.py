"""
screener/metrics/ma200_gate.py — MA200 gate: binary multiplier applied to composite score.

    price >= MA200  →  multiplier = GATE_ABOVE (1.0)
    price <  MA200  →  multiplier = GATE_BELOW (0.5)

Input comes from compute_score() output — no yfinance fetch or I/O needed.
"""

from __future__ import annotations

GATE_ABOVE = 1.0
GATE_BELOW = 0.5


def apply_gate(price: float, ma200: float) -> dict:
    """
    Compute the MA200 gate for a single ticker.

    Returns dict:
        {
            "above_ma200": bool,
            "multiplier":  float,   # GATE_ABOVE or GATE_BELOW
        }
    """
    above = price >= ma200
    return {
        "above_ma200": above,
        "multiplier": GATE_ABOVE if above else GATE_BELOW,
    }
