"""
screener/eval/rubric.py — Rubric factory functions and validation helpers.

Provides named rubric variants (default, aggressive, conservative) and a
validate_rubric() helper for runtime sanity-checking user-supplied rubrics.
"""

from __future__ import annotations

from screener.lib.models import RubricDefinition


def get_default_rubric() -> RubricDefinition:
    """Return the default balanced rubric (accuracy-heavy, v1).

    Weights: accuracy=40, confidence_alignment=30, timing=15, risk=15.
    """
    return RubricDefinition(
        name="default_v1",
        accuracy_weight=40,
        confidence_alignment_weight=30,
        timing_quality_weight=15,
        risk_management_weight=15,
        overconfidence_threshold=20,
        poor_timing_threshold=40,
    )


def get_aggressive_rubric() -> RubricDefinition:
    """Return a timing-focused rubric for short-horizon traders.

    Shifts weight from accuracy toward timing quality to penalise correct
    direction but bad entry/exit. Weights: accuracy=30, confidence=25,
    timing=30, risk=15.
    """
    return RubricDefinition(
        name="aggressive_v1",
        accuracy_weight=30,
        confidence_alignment_weight=25,
        timing_quality_weight=30,
        risk_management_weight=15,
        overconfidence_threshold=15,
        poor_timing_threshold=30,
    )


def get_conservative_rubric() -> RubricDefinition:
    """Return a risk-management-focused rubric for capital-preservation mandates.

    Shifts weight from accuracy toward risk management to penalise picks
    with no stop-loss or position-sizing mention. Weights: accuracy=35,
    confidence=25, timing=15, risk=25.
    """
    return RubricDefinition(
        name="conservative_v1",
        accuracy_weight=35,
        confidence_alignment_weight=25,
        timing_quality_weight=15,
        risk_management_weight=25,
        overconfidence_threshold=25,
        poor_timing_threshold=50,
    )


def validate_rubric(rubric: RubricDefinition) -> tuple[bool, str | None]:
    """Validate a RubricDefinition for runtime sanity.

    Checks:
    - Weights already sum to 100 (enforced by model_validator, but we surface
      a clear message here if somehow bypassed).
    - overconfidence_threshold is in 0–100 range.
    - poor_timing_threshold is in 0–100 range.

    Returns:
        (True, None) if the rubric is valid.
        (False, error_message) if any check fails.
    """
    total = (
        rubric.accuracy_weight
        + rubric.confidence_alignment_weight
        + rubric.timing_quality_weight
        + rubric.risk_management_weight
    )
    if total != 100:
        return False, f"Rubric weights must sum to 100, got {total}"

    if not (0 <= rubric.overconfidence_threshold <= 100):
        return False, (
            f"overconfidence_threshold must be 0–100, got {rubric.overconfidence_threshold}"
        )

    if not (0 <= rubric.poor_timing_threshold <= 100):
        return False, (
            f"poor_timing_threshold must be 0–100, got {rubric.poor_timing_threshold}"
        )

    return True, None
