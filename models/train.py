"""
railrisk/models/train.py
------------------------
Pydantic v2 models for validating live train running status
and station metadata payloads from API responses.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

# Standard IST timezone offset (+05:30)
IST = timezone(timedelta(hours=5, minutes=30))


class LiveTrainStatus(BaseModel):
    """
    Validates live train status and station details for e-catering risk evaluation.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="ignore",
    )

    train_number: str = Field(
        ...,
        pattern=r"^\d{5}$",
        description="5-digit Indian Railways train number",
        examples=["12301"],
    )
    current_station: str = Field(..., min_length=1)
    target_station: str = Field(..., min_length=1)
    scheduled_arrival: datetime = Field(
        ...,
        description="Scheduled arrival datetime (ISO 8601)",
    )
    delay_minutes: int = Field(...)
    halt_duration_mins: int = Field(..., ge=0)
    vendor_cutoff_mins: int = Field(default=60, ge=0)
    captured_at: Optional[datetime] = Field(
        default=None,
        description="Observation timestamp when status payload was captured",
    )

    @field_validator("scheduled_arrival", "captured_at", mode="after")
    @classmethod
    def enforce_ist_timezone(cls, dt: Optional[datetime]) -> Optional[datetime]:
        """
        Ensures datetimes are timezone-aware (Asia/Kolkata / IST).
        """
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST)

    @computed_field
    @property
    def estimated_arrival(self) -> datetime:
        """
        Dynamically computes estimated arrival time at target station.
        """
        return self.scheduled_arrival + timedelta(minutes=self.delay_minutes)


class StationMetadata(BaseModel):
    """
    Validates standalone station details for caching or route lookups.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    station_code: str = Field(..., min_length=2, max_length=10)
    station_name: str = Field(..., min_length=1)
    platform_count: int = Field(default=1, ge=1)