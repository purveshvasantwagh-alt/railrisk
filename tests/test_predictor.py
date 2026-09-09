"""
tests/test_predictor.py
------------------------
Production-level unit test suite enforcing strict exception granularity,
midnight/timezone boundary arithmetic, and total network isolation.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

from railrisk.db.repository import get_recent_snapshots, save_snapshot
from railrisk.engine.dynamic_eval import (
    FormulaEvaluationError,
    SecurityError,
    evaluate_formula,
)
from railrisk.engine.risk_calculator import assess_order_risk
from railrisk.engine.volatility import calculate_delay_drift
from railrisk.models.risk import Recommendation, RiskLevel
from railrisk.models.train import LiveTrainStatus
from railrisk.providers.ntes_provider import (
    NTESProvider,
    ProviderDataError,
    StationFetchError,
)

# Standard IST timezone offset (+05:30)
IST = timezone(timedelta(hours=5, minutes=30))


# =====================================================================
# 1. TIMEZONE COERCION & MIDNIGHT DATE BOUNDARY TESTS
# =====================================================================
def test_timezone_coercion_from_naive_iso():
    """
    Validates that naive ISO datetime strings are automatically coerced to IST (+05:30)
    and perform arithmetic against timezone-aware timestamps without TypeError.
    """
    status = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T14:30:00",  # Offset-naive string
        delay_minutes=30,
        halt_duration_mins=10,
    )

    # Assert timezone offset was automatically assigned to IST
    assert status.scheduled_arrival.tzinfo is not None
    assert status.scheduled_arrival.utcoffset() == timedelta(hours=5, minutes=30)

    # Perform arithmetic with explicit UTC datetime
    now_utc = datetime.now(timezone.utc)
    time_delta = status.estimated_arrival - now_utc

    assert isinstance(time_delta, timedelta)


def test_midnight_boundary_drift_calculation():
    """
    Verifies that elapsed time arithmetic spanning across midnight date boundaries
    (e.g., 23:45 IST on Day 1 to 00:30 IST on Day 2) correctly computes positive elapsed
    duration and drift velocity without negative duration artifacts.
    """
    # Snapshot 1 at 23:45 IST on Sept 9
    t1 = datetime(2026, 9, 9, 23, 45, 0, tzinfo=IST)
    s1 = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T06:00:00+05:30",
        delay_minutes=10,
        halt_duration_mins=10,
    )
    s1.captured_at = t1

    # Snapshot 2 at 00:30 IST on Sept 10 (45 mins elapsed = 0.75 hours)
    # Delay increased from 10m to 25m (+15 mins) -> Drift = +15 / 0.75 = +20.0 mins/hr
    t2 = datetime(2026, 9, 10, 0, 30, 0, tzinfo=IST)
    s2 = LiveTrainStatus(
        train_number="12301",
        current_station="ALJN",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T06:00:00+05:30",
        delay_minutes=25,
        halt_duration_mins=10,
    )
    s2.captured_at = t2

    drift = calculate_delay_drift([s1, s2])
    assert drift == 20.0


# =====================================================================
# 2. VOLATILITY ENGINE TESTS
# =====================================================================
def test_delay_drift_deceleration_and_acceleration():
    """
    Verifies delay variance drift rates:
    - Positive drift: Delay increases (+15.0 mins/hr).
    - Negative drift: Train recovers lost time (-30.0 mins/hr).
    """
    base_time = datetime(2026, 9, 10, 10, 0, 0, tzinfo=IST)

    s1 = LiveTrainStatus(
        train_number="12301",
        current_station="PRYJ",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T18:00:00+05:30",
        delay_minutes=10,
        halt_duration_mins=5,
    )
    s1.captured_at = base_time

    s2 = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T18:00:00+05:30",
        delay_minutes=40,
        halt_duration_mins=5,
    )
    s2.captured_at = base_time + timedelta(hours=2)

    assert calculate_delay_drift([s1, s2]) == 15.0

    # Train regaining time: Delay drops from 60m to 0m over 2 hours
    s1.delay_minutes = 60
    s3 = LiveTrainStatus(
        train_number="12301",
        current_station="ALJN",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T18:00:00+05:30",
        delay_minutes=0,
        halt_duration_mins=5,
    )
    s3.captured_at = base_time + timedelta(hours=2)

    assert calculate_delay_drift([s1, s3]) == -30.0


# =====================================================================
# 3. RISK MATRIX EVALUATION TESTS
# =====================================================================
def test_risk_matrix_evaluations():
    """Asserts correct RiskLevel and Recommendation mappings based on delivery buffers."""
    ref_time = datetime(2026, 9, 10, 12, 0, 0, tzinfo=IST)

    # LOW Risk / PROCEED
    status_safe = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T14:00:00+05:30",
        delay_minutes=0,
        halt_duration_mins=10,
    )
    assessment_safe = assess_order_risk(
        status=status_safe,
        drift_rate=0.0,
        order_prep_mins=45,
        delivery_lead_mins=15,
        current_time=ref_time,
    )
    assert assessment_safe.risk_level == RiskLevel.LOW
    assert assessment_safe.recommendation == Recommendation.PROCEED

    # MODERATE Risk / WARN_VENDOR
    status_tight = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T13:15:00+05:30",
        delay_minutes=0,
        halt_duration_mins=10,
    )
    assessment_mod = assess_order_risk(
        status=status_tight,
        drift_rate=0.0,
        order_prep_mins=45,
        delivery_lead_mins=15,
        current_time=ref_time,
    )
    assert assessment_mod.risk_level == RiskLevel.MODERATE
    assert assessment_mod.recommendation == Recommendation.WARN_VENDOR

    # CRITICAL Risk / CANCEL_ORDER
    status_critical = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T12:30:00+05:30",
        delay_minutes=0,
        halt_duration_mins=5,
    )
    assessment_crit = assess_order_risk(
        status=status_critical,
        drift_rate=0.0,
        order_prep_mins=45,
        delivery_lead_mins=15,
        current_time=ref_time,
    )
    assert assessment_crit.risk_level == RiskLevel.CRITICAL
    assert assessment_crit.recommendation == Recommendation.CANCEL_ORDER


# =====================================================================
# 4. AST SANDBOX SECURITY & EXCEPTION GRANULARITY TESTS
# =====================================================================
def test_ast_sandbox_valid_formulas():
    """Validates safe dynamic mathematical evaluation."""
    context = {
        "effective_buffer_mins": 20.0,
        "drift_rate_mph": -10.0,
        "delay_minutes": 15.0,
    }
    formula = "(100 - effective_buffer_mins * 2) + (-drift_rate_mph * 1.5)"
    assert evaluate_formula(formula, context) == 75.0


def test_ast_sandbox_security_rejections_explicit():
    """Explicitly asserts SecurityError for unsafe AST constructs."""
    context = {"effective_buffer_mins": 20.0}

    # Function call attempt (ast.Call)
    with pytest.raises(SecurityError, match="Forbidden or unsafe expression construct"):
        evaluate_formula("abs(effective_buffer_mins)", context)

    # Attribute access attempt (ast.Attribute)
    with pytest.raises(SecurityError, match="Forbidden or unsafe expression construct"):
        evaluate_formula("effective_buffer_mins.__class__", context)

    # Import attempt
    with pytest.raises(SecurityError, match="Forbidden or unsafe expression construct"):
        evaluate_formula("__import__('os').system('echo hacked')", context)


def test_ast_sandbox_evaluation_errors_explicit():
    """Explicitly asserts FormulaEvaluationError for math/parsing violations."""
    context = {"effective_buffer_mins": 20.0}

    # Division by zero
    with pytest.raises(FormulaEvaluationError, match="Division by zero"):
        evaluate_formula("effective_buffer_mins / 0", context)

    # Undefined variable
    with pytest.raises(FormulaEvaluationError, match="Undefined context variable"):
        evaluate_formula("missing_variable + 10", context)


def test_pydantic_validation_error_granularity():
    """Explicitly asserts Pydantic ValidationError for malformed inputs."""
    with pytest.raises(ValidationError):
        LiveTrainStatus(
            train_number="INVALID_TRAIN",  # Fails 5-digit regex pattern
            current_station="CNB",
            target_station="NDLS",
            scheduled_arrival="2026-09-10T14:30:00+05:30",
            delay_minutes=30,
            halt_duration_mins=10,
        )


# =====================================================================
# 5. PROVIDER PERSISTENCE & NETWORK ISOLATION TESTS
# =====================================================================
@patch("requests.get")
def test_provider_complete_network_isolation_and_fallback(mock_get: MagicMock, tmp_path: Path):
    """
    Verifies that requests.get is completely intercepted by unittest.mock.patch
    to guarantee zero external network traffic, and validates local SQLite fallback.
    """
    db_file = tmp_path / "test_railrisk.db"

    # Seed SQLite with cached snapshot
    cached_status = LiveTrainStatus(
        train_number="12301",
        current_station="PRYJ",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T20:00:00+05:30",
        delay_minutes=25,
        halt_duration_mins=10,
    )
    save_snapshot(cached_status, db_path=db_file)

    # Force mock_get to raise a network exception
    mock_get.side_effect = requests.ConnectionError("Simulated network outage")

    provider = NTESProvider(
        base_url="https://api.railway.example.com/v1",
        db_path=db_file,
    )

    status = provider.get_live_status("12301", "NDLS")

    # Assert mock intercept occurred (no real HTTP calls made)
    assert mock_get.called
    assert mock_get.call_count == 3  # Verifies Tenacity retried 3 times before fallback

    # Assert fallback returned valid SQLite data
    assert status.train_number == "12301"
    assert status.current_station == "PRYJ"
    assert status.delay_minutes == 25


@patch("requests.get")
def test_provider_malformed_json_triggers_data_error_and_fallback(mock_get: MagicMock, tmp_path: Path):
    """
    Verifies that corrupted JSON responses trigger ProviderDataError and
    degrade gracefully to local SQLite fallback without unhandled model crashes.
    """
    db_file = tmp_path / "test_railrisk.db"

    # Seed SQLite
    save_snapshot(
        LiveTrainStatus(
            train_number="12301",
            current_station="CNB",
            target_station="NDLS",
            scheduled_arrival="2026-09-10T20:00:00+05:30",
            delay_minutes=10,
            halt_duration_mins=10,
        ),
        db_path=db_file,
    )

    # Mock HTTP 200 OK returning corrupted schema
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"corrupted_field": "invalid"}
    mock_get.return_value = mock_response

    provider = NTESProvider(base_url="https://api.railway.example.com/v1", db_path=db_file)

    status = provider.get_live_status("12301", "NDLS")

    assert mock_get.called
    assert status.current_station == "CNB"