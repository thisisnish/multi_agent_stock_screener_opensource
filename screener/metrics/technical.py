"""
screener/metrics/technical.py — Technical signal: RSI, MA50/200, volume, momentum.

Pure pandas — no I/O. All functions are stateless and testable in isolation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

RSI_PERIOD = 14
MA_SHORT = 50
MA_LONG = 200
VOL_SHORT = 10
VOL_LONG = 30
MOM_DAYS = 20

WEIGHTS = {
    "rsi": 0.30,
    "ma50": 0.25,
    "ma200": 0.20,
    "volume": 0.15,
    "momentum": 0.10,
}

MIN_ROWS = MA_LONG + 5


def _rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    span = 2 * period - 1
    avg_gain = gain.ewm(span=span, adjust=False).mean()
    avg_loss = loss.ewm(span=span, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _rsi_score(rsi_val: float) -> float:
    if rsi_val < 30:
        return 100.0
    elif rsi_val < 40:
        return 100.0 - (rsi_val - 30) * 2.0
    elif rsi_val < 60:
        return 80.0 - (rsi_val - 40) * 1.0
    elif rsi_val < 70:
        return 60.0 - (rsi_val - 60) * 3.0
    else:
        return 0.0


def _ma_score(price: float, ma: float) -> float:
    if ma <= 0:
        return 50.0
    raw = (price / ma - 1.0) * 250.0 + 50.0
    return float(np.clip(raw, 0.0, 100.0))


def _volume_score(vol_short: float, vol_long: float) -> float:
    if vol_long <= 0:
        return 50.0
    raw = (vol_short / vol_long - 1.0) * 200.0 + 50.0
    return float(np.clip(raw, 0.0, 100.0))


def _momentum_score(pct_change: float) -> float:
    raw = pct_change * 200.0 + 50.0
    return float(np.clip(raw, 0.0, 100.0))


def compute_score(symbol: str, df: pd.DataFrame) -> dict:
    """
    Compute composite technical score for a single ticker.

    Returns dict with keys: score, rsi, ma50, ma200, price, signals, skipped.
    Returns {"skipped": True, "reason": str} on insufficient data.
    """
    if df is None or len(df) < MIN_ROWS:
        return {
            "skipped": True,
            "reason": f"insufficient data: {len(df) if df is not None else 0} rows, need {MIN_ROWS}",
        }

    close = df["Close"].dropna()
    volume = df["Volume"].dropna()

    if len(close) < MIN_ROWS:
        return {
            "skipped": True,
            "reason": f"insufficient non-null close prices: {len(close)}",
        }

    rsi_series = _rsi(close)
    ma50_series = close.rolling(MA_SHORT).mean()
    ma200_series = close.rolling(MA_LONG).mean()
    vol_short_series = volume.rolling(VOL_SHORT).mean()
    vol_long_series = volume.rolling(VOL_LONG).mean()

    price = float(close.iloc[-1])
    rsi_val = float(rsi_series.iloc[-1])
    ma50_val = float(ma50_series.iloc[-1])
    ma200_val = float(ma200_series.iloc[-1])
    vol_s = float(vol_short_series.iloc[-1])
    vol_l = float(vol_long_series.iloc[-1])

    if len(close) > MOM_DAYS:
        price_then = float(close.iloc[-(MOM_DAYS + 1)])
        pct_change = (price - price_then) / price_then if price_then > 0 else 0.0
    else:
        pct_change = 0.0

    if any(np.isnan(v) for v in [rsi_val, ma50_val, ma200_val, vol_s, vol_l]):
        return {
            "skipped": True,
            "reason": "NaN in core indicators (insufficient history)",
        }

    sub_scores = {
        "rsi": _rsi_score(rsi_val),
        "ma50": _ma_score(price, ma50_val),
        "ma200": _ma_score(price, ma200_val),
        "volume": _volume_score(vol_s, vol_l),
        "momentum": _momentum_score(pct_change),
    }

    composite = sum(sub_scores[k] * WEIGHTS[k] for k in WEIGHTS)

    return {
        "score": round(composite, 2),
        "rsi": round(rsi_val, 2),
        "ma50": round(ma50_val, 2),
        "ma200": round(ma200_val, 2),
        "price": round(price, 2),
        "signals": {
            k: {"score": round(sub_scores[k], 2), "weight": WEIGHTS[k]} for k in WEIGHTS
        },
        "skipped": False,
    }
