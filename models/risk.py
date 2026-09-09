"""
railrisk/models/risk.py
-----------------------
Pydantic v2 models and enums for order feasibility and risk scoring.
"""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class RiskLevel(str, Enum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    CRITICAL = "CRITICAL"


class Recommendation(str, Enum):
    PROCEED = "PROCEED"
    WARN_VENDOR = "WARN_VENDOR"
    CANCEL_ORDER = "CANCEL_ORDER"


class RiskAssessment(BaseModel):
    """
    Evaluates food order delivery viability at downstream station.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    risk_score: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Calculated risk score from 0.0 (safe) to 100.0 (critical/impossible)",
    )
    risk_level: RiskLevel
    recommendation: Recommendation
    effective_buffer_mins: float = Field(
        ...,
        description="Current margin in minutes before vendor cutoff deadline",
    )
    projected_buffer_mins: float = Field(
        ...,
        description="Delivery margin adjusted for delay acceleration/deceleration drift",
    )
    drift_rate_mph: float = Field(
        ...,
        description="Delay drift velocity in minutes per hour (negative means train is regaining lost time)",
    )
    details: str = Field(..., description="Human-readable breakdown of the assessment verdict")