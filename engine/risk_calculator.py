"""
railrisk/engine/risk_calculator.py
-----------------------------------
Core scoring engine merging station ETA, vendor lead time, and delay drift velocity.
"""

from datetime import datetime, timezone
from typing import Optional
from railrisk.models.risk import Recommendation, RiskAssessment, RiskLevel
from railrisk.models.train import LiveTrainStatus


def assess_order_risk(
    status: LiveTrainStatus,
    drift_rate: float,
    order_prep_mins: int,
    delivery_lead_mins: int,
    current_time: Optional[datetime] = None,
) -> RiskAssessment:
    """
    Computes effective order feasibility buffer and outputs a structured RiskAssessment.

    Args:
        status: Parsed LiveTrainStatus model.
        drift_rate: Calculated delay velocity in minutes per hour (from volatility engine).
        order_prep_mins: Kitchen preparation time required by vendor.
        delivery_lead_mins: Time required for vendor staff to deliver to platform.
        current_time: Reference timestamp (defaults to current UTC time).
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Align timezone awareness
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    target_eta = status.estimated_arrival
    if target_eta.tzinfo is None:
        target_eta = target_eta.replace(tzinfo=timezone.utc)

    required_window_mins = order_prep_mins + delivery_lead_mins
    time_to_arrival_mins = (target_eta - current_time).total_seconds() / 60.0

    # Effective buffer = Available time until arrival - Minimum vendor prep/delivery window
    effective_buffer_mins = time_to_arrival_mins - required_window_mins

    # Estimate drift effect over remaining hours
    remaining_hours = max(time_to_arrival_mins / 60.0, 0.0)
    projected_delay_change = drift_rate * remaining_hours

    # Negative drift means train arrives EARLIER than currently estimated,
    # directly collapsing the delivery buffer.
    projected_buffer_mins = effective_buffer_mins + projected_delay_change

    # --- Risk Scoring Algorithm (0.0 to 100.0) ---
    base_risk = 0.0
    details = []

    if effective_buffer_mins <= 0:
        base_risk = 100.0
        details.append(
            f"Train arrives in {time_to_arrival_mins:.1f}m, which is less than "
            f"required vendor window ({required_window_mins}m)."
        )
    else:
        # Scale risk based on buffer size (30+ mins buffer is baseline safe)
        if effective_buffer_mins < 15:
            base_risk = 75.0 - (effective_buffer_mins * 2)
            details.append(f"Tight buffer: only {effective_buffer_mins:.1f}m margin remaining.")
        elif effective_buffer_mins < 30:
            base_risk = 45.0 - ((effective_buffer_mins - 15) * 1.5)
            details.append(f"Moderate buffer: {effective_buffer_mins:.1f}m margin remaining.")
        else:
            base_risk = max(10.0 - (effective_buffer_mins - 30) * 0.2, 0.0)
            details.append(f"Healthy buffer: {effective_buffer_mins:.1f}m margin available.")

    # Apply negative drift penalty (train making up time rapidly)
    if drift_rate < -5.0:
        drift_penalty = min(abs(drift_rate) * 1.5, 30.0)
        base_risk += drift_penalty
        details.append(
            f"ELEVATED DRIFT RISK: Train regaining lost time at {abs(drift_rate):.1f} mins/hr. "
            f"Projected buffer shrinks to {projected_buffer_mins:.1f}m."
        )

    # Halt duration check
    if status.halt_duration_mins < 3:
        base_risk += 10.0
        details.append(f"Short station halt duration ({status.halt_duration_mins}m) increases delivery friction.")

    final_score = round(min(max(base_risk, 0.0), 100.0), 1)

    # Categorize Risk Level & Recommendation
    if final_score >= 70.0 or projected_buffer_mins < 0:
        level = RiskLevel.CRITICAL
        rec = Recommendation.CANCEL_ORDER
    elif final_score >= 35.0:
        level = RiskLevel.MODERATE
        rec = Recommendation.WARN_VENDOR
    else:
        level = RiskLevel.LOW
        rec = Recommendation.PROCEED

    return RiskAssessment(
        risk_score=final_score,
        risk_level=level,
        recommendation=rec,
        effective_buffer_mins=round(effective_buffer_mins, 1),
        projected_buffer_mins=round(projected_buffer_mins, 1),
        drift_rate_mph=drift_rate,
        details=" ".join(details),
    )