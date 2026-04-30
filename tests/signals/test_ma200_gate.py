"""
tests/signals/test_ma200_gate.py — Unit tests for screener/metrics/ma200_gate.py.
"""

from __future__ import annotations

from screener.metrics.ma200_gate import GATE_ABOVE, GATE_BELOW, apply_gate


class TestApplyGate:
    def test_price_above_ma200_returns_above_true_and_multiplier_1(self):
        result = apply_gate(price=110.0, ma200=100.0)
        assert result["above_ma200"] is True
        assert result["multiplier"] == GATE_ABOVE

    def test_price_below_ma200_returns_above_false_and_multiplier_half(self):
        result = apply_gate(price=90.0, ma200=100.0)
        assert result["above_ma200"] is False
        assert result["multiplier"] == GATE_BELOW

    def test_price_equal_to_ma200_treated_as_above(self):
        result = apply_gate(price=100.0, ma200=100.0)
        assert result["above_ma200"] is True
        assert result["multiplier"] == GATE_ABOVE

    def test_returns_correct_keys(self):
        result = apply_gate(price=100.0, ma200=100.0)
        assert set(result.keys()) == {"above_ma200", "multiplier"}

    def test_multiplier_values_are_correct_constants(self):
        assert GATE_ABOVE == 1.0
        assert GATE_BELOW == 0.5

    def test_large_price_above_small_ma200(self):
        result = apply_gate(price=9999.0, ma200=0.01)
        assert result["above_ma200"] is True
        assert result["multiplier"] == 1.0

    def test_tiny_price_below_large_ma200(self):
        result = apply_gate(price=0.01, ma200=9999.0)
        assert result["above_ma200"] is False
        assert result["multiplier"] == 0.5
