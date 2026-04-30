"""
tests/eval/test_rubric.py — Unit tests for screener/eval/rubric.py.

Covers: rubric factory functions, weight sum invariants, validate_rubric().
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from screener.eval.rubric import (
    get_aggressive_rubric,
    get_conservative_rubric,
    get_default_rubric,
    validate_rubric,
)
from screener.lib.models import RubricDefinition


def test_default_rubric_weights_sum_to_100():
    rubric = get_default_rubric()
    total = (
        rubric.accuracy_weight
        + rubric.confidence_alignment_weight
        + rubric.timing_quality_weight
        + rubric.risk_management_weight
    )
    assert total == 100


def test_aggressive_rubric_has_higher_timing():
    rubric = get_aggressive_rubric()
    assert rubric.timing_quality_weight > 20


def test_conservative_rubric_has_higher_risk():
    rubric = get_conservative_rubric()
    assert rubric.risk_management_weight > 20


def test_validate_rubric_valid():
    rubric = get_default_rubric()
    is_valid, err = validate_rubric(rubric)
    assert is_valid is True
    assert err is None


def test_validate_rubric_invalid_threshold():
    rubric = get_default_rubric()
    # Bypass the model to inject an out-of-range value via model_copy
    rubric_bad = rubric.model_copy(update={"overconfidence_threshold": 150})
    is_valid, err = validate_rubric(rubric_bad)
    assert is_valid is False
    assert err is not None
    assert "overconfidence_threshold" in err


def test_rubric_definition_rejects_bad_weights():
    with pytest.raises(ValidationError):
        RubricDefinition(
            accuracy_weight=41,  # 41+30+15+15 = 101
            confidence_alignment_weight=30,
            timing_quality_weight=15,
            risk_management_weight=15,
        )
