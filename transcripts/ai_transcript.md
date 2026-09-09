I want to build a lightweight Python utility named 'RailRisk' to solve a real issue on Indian Railways: train delay volatility causing missed e-catering food orders at downstream stations



Give me a high-level architecture. however, keep it lean,I do not want heavy streaming or queue systems like Kafka, Redis, or Celery. I want a pure Python modular project using Pydantic for validation, Tenacity for retry logic, local SQLite for stale data caching, and a CLI entry point. suggest the core module structure for this.

RailRisk uses a synchronous, pull-based architecture centered around a time-series delay variance heuristic. Instead of active stream processing, it periodically snapshots live train status into local SQLite, computing volatility and vendor delivery risk on demand.

High-Level Architectural Flow
[CLI Entry Point]
       │
       ▼
[Risk Engine Orchestrator]
       │
       ├─► [Live Data Provider] ──(Tenacity Retry)──► [Railway / NTES API]
       │            │
       │            ▼
       ├─► [SQLite Cache Store] ◄── Snapshots delay history & static schedules
       │            │
       │            ▼
       └─► [Volatility Engine] ──► Evaluates delay drift variance & vendor lead times
                    │
                    ▼
           [Risk Score Output] (SAFE / RISKY / ABORT)
Ingestion & Resilience: Fetch live running status via an API wrapper. Use Tenacity retry strategies to handle flaky public rail endpoints (exponential backoff, retry on network errors).

Data Contracts & Validation: Parse raw API JSON into strict Pydantic models (validating timestamps, delay minutes, speed, and platform changes).

Local State & Volatility Caching: Store running-status snapshots in a single-file SQLite database (WAL mode for fast local reads/writes). Consecutive snapshots allow calculating delay variance (Δdelay/Δtime) to detect if a train is rapidly making up time or accumulating unpredictable lag.

Risk Scoring Engine: Calculate order feasibility by comparing vendor preparation time + platform delivery lead time against the train’s estimated arrival window and variance score.

Core Module Structure
Plaintext
railrisk/
│
├── railrisk/
│   ├── __init__.py
│   ├── cli.py                    # Entry point (e.g., Click or Typer interface)
│   ├── config.py                 # Pydantic BaseSettings (DB path, threshold parameters)
│   │
│   ├── models/                   # Pure Pydantic data schemas
│   │   ├── __init__.py
│   │   ├── train.py              # Station, TrainStatus, DelaySnapshot schemas
│   │   ├── vendor.py             # VendorOrder, PrepTimeWindow schemas
│   │   └── risk.py               # RiskAssessment, RiskLevel enum, Recommendation
│   │
│   ├── providers/                # External API wrappers
│   │   ├── __init__.py
│   │   ├── base.py               # Abstract base class for railway status providers
│   │   └── ntes_provider.py      # Live status client wrapped with Tenacity retry logic
│   │
│   ├── db/                       # Local storage & caching
│   │   ├── __init__.py
│   │   ├── schema.py             # SQLite DDL and table initializations
│   │   └── repository.py         # SQLite CRUD: fetch past snapshots, clean stale cache
│   │
│   └── engine/                   # Core business logic (no side-effects)
│       ├── __init__.py
│       ├── volatility.py         # Calculates delay acceleration and standard deviation
│       └── risk_calculator.py    # Merges volatility, vendor lead times, and ETA windows
│
├── pyproject.toml                # Project dependencies & CLI entrypoints
└── README.md
Core Components & Responsibilities
1. Models (railrisk/models/)
Defines strict Pydantic models to guarantee field types from unstructured railway APIs:

train.py: Validates station_code, scheduled_arrival, actual_arrival, delay_minutes, and distance_from_source.

risk.py: Defines the RiskAssessment object outputting a RiskScore (0–100), RiskLevel (LOW, MODERATE, CRITICAL), and recommended action (PROCEED, WARN_VENDOR, CANCEL_ORDER).

2. Provider with Retry (railrisk/providers/ntes_provider.py)
Uses tenacity decorators to wrap HTTP requests against network drops and HTTP 5xx errors commonly seen on railway endpoints:

Python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(NetworkError)
)
def fetch_live_status(train_no: str) -> dict:
    ...
3. SQLite Storage (railrisk/db/repository.py)
Stores delay history snapshots using standard sqlite3 context managers:

snapshots table: Stores (train_no, station_code, timestamp, delay_minutes, distance_km).

Volatility Analysis: Querying the last N snapshots for a train determines if it is speeding up (risk of early arrival before food prep) or slowing down (risk of food spoiling during long delays).

4. Volatility & Risk Engine (railrisk/engine/)
Decouples computation from external calls to allow offline testing:

Input: Current DelaySnapshot, historical Snapshots[], and VendorOrder (prep lead time, minimum required delivery window).

Formula:

ETA Buffer = (Estimated Arrival−Current Time)−(Prep Time+Delivery Lead Time)

Drift Rate = (Delay 
current
​
 −Delay 
previous
​
 )/Δt

High negative drift rate indicates a train making up time rapidly, drastically shortening the delivery buffer.

5. CLI Entrypoint (railrisk/cli.py)
Provides quick operational terminal queries:

Bash
# Example usage
railrisk assess --train 12301 --station NDLS --order-prep-mins 45
Write the file 'railrisk/models/train.py' using Pydantic v2 to validate API payloads for live train status and station metadata.



The schema must capture:

- train_number: string (validated as 5 digits)

- current_station: string

- target_station: string

- scheduled_arrival: ISO datetime string

- delay_minutes: integer

- halt_duration_mins: integer

- vendor_cutoff_mins: integer (default 60 mins)



Make sure field types are strictly enforced and string whitespace is automatically stripped

Here is the complete implementation for railrisk/models/train.py using Pydantic v2.

Python
"""
railrisk/models/train.py
------------------------
Pydantic v2 models for validating live train running status
and station metadata payloads from API responses.
"""

from datetime import datetime, timedelta
from pydantic import BaseModel, ConfigDict, Field, computed_field


class LiveTrainStatus(BaseModel):
    """
    Validates live train status and station details for e-catering risk evaluation.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,  # Automatically trims leading/trailing whitespace from string fields
        extra="ignore",              # Ignores unexpected metadata fields in raw API payloads
    )

    train_number: str = Field(
        ...,
        pattern=r"^\d{5}$",
        description="5-digit Indian Railways train number (e.g., '12301')",
        examples=["12301", "22436"]
    )
    current_station: str = Field(
        ...,
        min_length=1,
        description="Current station code or name (e.g., 'CNB' or 'Kanpur Central')",
        examples=["CNB"]
    )
    target_station: str = Field(
        ...,
        min_length=1,
        description="Target delivery station code or name (e.g., 'NDLS')",
        examples=["NDLS"]
    )
    scheduled_arrival: datetime = Field(
        ...,
        description="ISO 8601 formatted scheduled arrival datetime at target station",
        examples=["2026-09-10T14:30:00+05:30"]
    )
    delay_minutes: int = Field(
        ...,
        description="Current delay in minutes (positive for late, negative for ahead of schedule)",
        examples=[45]
    )
    halt_duration_mins: int = Field(
        ...,
        ge=0,
        description="Scheduled halt duration in minutes at the target station",
        examples=[10]
    )
    vendor_cutoff_mins: int = Field(
        default=60,
        ge=0,
        description="Minimum preparation lead time required by e-catering vendor before arrival",
        examples=[60]
    )

    @computed_field
    @property
    def estimated_arrival(self) -> datetime:
        """
        Dynamically computes the estimated arrival time at the target station based on delay.
        """
        return self.scheduled_arrival + timedelta(minutes=self.delay_minutes)


class StationMetadata(BaseModel):
    """
    Validates standalone station details for caching or route lookups.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    station_code: str = Field(
        ...,
        min_length=2,
        max_length=10,
        description="Standard IR station code (e.g., 'NDLS', 'HWH')",
    )
    station_name: str = Field(..., min_length=1, description="Full name of the railway station")
    platform_count: int = Field(default=1, ge=1, description="Total platform count for delivery logistics")
Key Features Implemented
Pydantic v2 ConfigDict:

str_strip_whitespace=True: Strips any unnecessary whitespace around input string payloads automatically (e.g., " 12301 " becomes "12301").

extra="ignore": Silently drops extraneous fields from unstable public train API responses without breaking validation.

Regex Validation:

pattern=r"^\d{5}$" strictly enforces standard 5-digit Indian Railways train numbering.

ISO Datetime Parsing:

scheduled_arrival: datetime leverages Pydantic v2’s built-in fast Rust parser to automatically parse ISO 8601 strings into standard Python datetime objects.

Computed Field (@computed_field):

Automatically computes estimated_arrival on demand (scheduled_arrival + delay_minutes), making it seamlessly exportable when converting models to .model_dump() or .model_dump_json().

I put your railrisk/models/train.py code into my environment and ran python test.py, but it crashed with this error in my terminal- Traceback (most recent call last):

  File "C:\Users\purve\railrisk\test.py", line 15, in <module>

    print(status.estimated_arrival - now)

          ~~~~~~~~~~~~~~~~~~~~~~~~~^~~~~

TypeError: can't subtract offset-naive and offset-aware datetimes

This error occurs because Python cannot subtract a timezone-aware datetime (which contains timezone offset information like +05:30 or Z) from a timezone-naive datetime (like one created with standard datetime.now()).

When Pydantic parses an ISO datetime string with a timezone offset (e.g., "2026-09-10T14:30:00+05:30"), it creates a timezone-aware datetime object. If your test.py created now = datetime.now() without a timezone, the subtraction fails.

Fix 1: Update test.py (Quick Fix)
Make now timezone-aware so it matches the parsed ISO datetime from the model:

Python
from datetime import datetime, timezone
from railrisk.models.train import LiveTrainStatus

# Create a timezone-aware "now" timestamp in UTC or IST
now = datetime.now(timezone.utc)

# Ensure status payload uses ISO string with timezone, e.g., 'Z' or '+05:30'
status = LiveTrainStatus(
    train_number="12301",
    current_station="CNB",
    target_station="NDLS",
    scheduled_arrival="2026-09-10T14:30:00+05:30",
    delay_minutes=45,
    halt_duration_mins=10,
)

# Convert status.estimated_arrival to UTC before subtracting if needed
time_remaining = status.estimated_arrival - now
print(time_remaining)
Fix 2: Harden railrisk/models/train.py for Indian Standard Time (IST)
Update railrisk/models/train.py with a Pydantic v2 field validator to guarantee all parsed scheduled_arrival datetimes are timezone-aware and normalized to Indian Standard Time (+05:30), even if the input API string omits the timezone offset.

Python
"""
railrisk/models/train.py
------------------------
Pydantic v2 models for validating live train running status
and station metadata payloads from API responses.
"""

from datetime import datetime, timedelta, timezone
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

    @field_validator("scheduled_arrival", mode="after")
    @classmethod
    def enforce_ist_timezone(cls, dt: datetime) -> datetime:
        """
        Ensures scheduled_arrival is timezone-aware. If offset-naive,
        assumes Indian Standard Time (IST, UTC+05:30).
        """
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
Build the core risk and volatility engine across two files: `railrisk/engine/volatility.py` and `railrisk/engine/risk_calculator.py`



Requirements:



1. railrisk/engine/volatility.py:

   - Implement `calculate_delay_drift(snapshots: list[LiveTrainStatus]) -> float' to measure delay variance acceleration/deceleration over time ($\Delta \text{delay} / \Delta t$ in minutes per hour).

   - Ensure datetime arithmetic spanning midnight date boundaries calculates elapsed time accurately without generating negative duration metrics.



2. `railrisk/engine/risk_calculator.py`:

   - Implement `assess_order_risk(status: LiveTrainStatus, drift_rate: float, order_prep_mins: int, delivery_lead_mins: int) -> RiskAssessment`.

   - Compute effective delivery buffer: $(\text{Estimated Arrival} - \text{Current Time}) - (\text{Prep Time} + \text{Delivery Lead Time})$.

   - Output structured Pydantic model `RiskAssessment` containing:

     - risk_score: Float between 0.0 and 100.0.

     - risk_level: Enum (`LOW`, `MODERATE`, `CRITICAL`).

     - recommendation: Enum (`PROCEED`, `WARN_VENDOR`, `CANCEL_ORDER`).

   - Flag negative drift rates (train making up time rapidly) as elevated risk if it collapses the required vendor preparation window.

railrisk/models/risk.py
Python
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
railrisk/engine/volatility.py
Python
"""
railrisk/engine/volatility.py
-----------------------------
Calculates delay drift acceleration and variance across chronological snapshots.
"""

from datetime import datetime, timezone
from typing import List, Optional
from railrisk.models.train import LiveTrainStatus


def _extract_timestamp(snapshot: LiveTrainStatus) -> datetime:
    """
    Extracts a timezone-aware observation timestamp from a status snapshot.
    Falls back to scheduled_arrival if an explicit captured_at is not present.
    """
    dt = getattr(snapshot, "captured_at", None) or snapshot.scheduled_arrival
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def calculate_delay_drift(snapshots: List[LiveTrainStatus]) -> float:
    """
    Measures delay variance acceleration/deceleration over time ($\Delta \\text{delay} / \\Delta t$).

    Returns:
        float: Drift rate in minutes per hour.
               - Positive (> 0): Train is accumulating further delays.
               - Zero (0.0): Delay is stable.
               - Negative (< 0): Train is making up time rapidly (earlier arrival).
    """
    if len(snapshots) < 2:
        return 0.0

    # Ensure snapshots are strictly ordered chronologically by timestamp
    sorted_snapshots = sorted(snapshots, key=_extract_timestamp)

    first = sorted_snapshots[0]
    last = sorted_snapshots[-1]

    t_first = _extract_timestamp(first)
    t_last = _extract_timestamp(last)

    # Datetime subtraction across midnight boundaries automatically yields
    # elapsed time via Python's timedelta
    elapsed_seconds = (t_last - t_first).total_seconds()

    # Prevent negative durations (clock skew) or division by zero
    if elapsed_seconds <= 0:
        return 0.0

    elapsed_hours = elapsed_seconds / 3600.0
    delay_change_mins = float(last.delay_minutes - first.delay_minutes)

    drift_rate = delay_change_mins / elapsed_hours
    return round(drift_rate, 2)
railrisk/engine/risk_calculator.py
Python
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
Create `railrisk/engine/dynamic_eval.py` to evaluate dynamic, user-defined risk scoring formulas safely without using Python's unsafe `eval()` or `exec()`.



Requirements:



1. Function signature: `evaluate_formula(expression: str, context: dict[str, float]) -> float

2. Secure Parsing:

   - Parse mathematical expressions strictly using Python's native `ast` module.

   - Support standard binary operators (`+`, `-`, `*`, `/`, `**`), unary operators (`-`, `+`), and numeric literals (integers and floats).

   - Resolve named variables from the provided `context` dictionary (e.g., `effective_buffer_mins`, `drift_rate_mph`, `delay_minutes`).

3. Security Guards:

   - Explicitly raise a `ValueError` or custom `SecurityError` if the expression contains unsafe AST nodes such as function calls (`ast.Call`), attribute lookups (`ast.Attribute`), import statements, string/list literals, or assignments.

   - Handle division by zero gracefully by raising a structured exception.

Here is the implementation for railrisk/engine/dynamic_eval.py.

Python
"""
railrisk/engine/dynamic_eval.py
-------------------------------
Safe dynamic mathematical expression evaluator for user-defined risk scoring rules.
Uses Python's native AST module to execute formulas without using `eval()` or `exec()`.
"""

import ast
import operator
from typing import Dict, Union


class SecurityError(ValueError):
    """Raised when an expression contains forbidden or unsafe AST constructs."""
    pass


class FormulaEvaluationError(ValueError):
    """Raised when formula parsing or execution fails (e.g. syntax, missing variables, division by zero)."""
    pass


# Allowed binary operator mappings
_ALLOWED_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}

# Allowed unary operator mappings
_ALLOWED_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def _eval_ast_node(node: ast.AST, context: Dict[str, float]) -> float:
    """
    Recursively evaluates an AST node strictly enforcing allowed mathematical constructs.
    """
    # Top-level expression container
    if isinstance(node, ast.Expression):
        return _eval_ast_node(node.body, context)

    # Numeric constants (integers and floats)
    elif isinstance(node, ast.Constant):
        # Exclude string, boolean, and non-numeric constants
        if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            return float(node.value)
        raise SecurityError(
            f"Forbidden constant type '{type(node.value).__name__}'. Only numeric literals are allowed."
        )

    # Named variables (e.g. delay_minutes, drift_rate_mph)
    elif isinstance(node, ast.Name):
        if not isinstance(node.ctx, ast.Load):
            raise SecurityError(f"Forbidden variable access context: {type(node.ctx).__name__}")
        if node.id not in context:
            raise FormulaEvaluationError(f"Undefined context variable: '{node.id}'")
        try:
            return float(context[node.id])
        except (ValueError, TypeError) as err:
            raise FormulaEvaluationError(
                f"Variable '{node.id}' value '{context[node.id]}' cannot be converted to float."
            ) from err

    # Binary operations (+, -, *, /, **)
    elif isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type not in _ALLOWED_BIN_OPS:
            raise SecurityError(f"Forbidden binary operator: '{op_type.__name__}'")

        left_val = _eval_ast_node(node.left, context)
        right_val = _eval_ast_node(node.right, context)

        if op_type is ast.Div and right_val == 0.0:
            raise FormulaEvaluationError("Division by zero in risk formula evaluation.")

        try:
            return float(_ALLOWED_BIN_OPS[op_type](left_val, right_val))
        except OverflowError as err:
            raise FormulaEvaluationError("Numeric overflow occurred during formula evaluation.") from err

    # Unary operations (+x, -x)
    elif isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type not in _ALLOWED_UNARY_OPS:
            raise SecurityError(f"Forbidden unary operator: '{op_type.__name__}'")

        operand_val = _eval_ast_node(node.operand, context)
        return float(_ALLOWED_UNARY_OPS[op_type](operand_val))

    # Reject all unsafe constructs (ast.Call, ast.Attribute, ast.Subscript, imports, etc.)
    else:
        raise SecurityError(f"Forbidden or unsafe expression construct: '{type(node).__name__}'")


def evaluate_formula(expression: str, context: Dict[str, float]) -> float:
    """
    Safely evaluates a user-defined mathematical risk formula against a variable context.

    Args:
        expression: Math string (e.g., "(100 - effective_buffer_mins) + (drift_rate_mph ** 2)")
        context: Map of variable names to numeric float values.

    Returns:
        float: Evaluated formula result.

    Raises:
        SecurityError: If expression contains function calls, attributes, or forbidden AST nodes.
        FormulaEvaluationError: If parsing fails, variables are missing, or division by zero occurs.
    """
    if not expression or not expression.strip():
        raise FormulaEvaluationError("Formula expression cannot be empty.")

    try:
        # Parse expression into an AST in single-expression mode
        parsed_ast = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as err:
        raise FormulaEvaluationError(f"Invalid formula syntax: {err}") from err

    return _eval_ast_node(parsed_ast, context)
Key Security & Architectural Features
AST Node Whitelisting:

Only ast.Expression, ast.Constant, ast.Name, ast.BinOp, and ast.UnaryOp are allowed.

Any invocation of ast.Call (func()), ast.Attribute (obj.attr), ast.Subscript (arr[0]), or ast.Import immediately triggers a SecurityError.

Strict Constant Guarding:

Explicitly rejects boolean constants (e.g., True, False) because Python treats isinstance(True, int) as True.

Division by Zero Protection:

Intercepts division operations (/) where the denominator evaluates to 0.0 and converts them into a structured FormulaEvaluationError.

Example Usage:

Python
from railrisk.engine.dynamic_eval import evaluate_formula

ctx = {
    "effective_buffer_mins": 12.5,
    "drift_rate_mph": -8.0,
    "delay_minutes": 45.0,
}

# Safe dynamic custom scoring
formula = "(100 - effective_buffer_mins * 2) + (-drift_rate_mph * 1.5)"
score = evaluate_formula(formula, ctx)
print(score)  # Output: 87.0

# Security Violation Attempt (Rejection)
unsafe_formula = "__import__('os').system('echo hacked')"
evaluate_formula(unsafe_formula, ctx)  # Raises SecurityError
Build the persistence and external network layer across `railrisk/db/repository.py` and `railrisk/providers/ntes_provider.py`.



Requirements:



1. `railrisk/db/repository.py`:

   - SQLite database module handling `railrisk.db`.

   - Table `train_snapshots` storing `train_number`, `current_station`, `target_station`, `scheduled_arrival`, `delay_minutes`, `halt_duration_mins`, `captured_at`.

   - Functions `save_snapshot(status: LiveTrainStatus) -> None` and `get_recent_snapshots(train_number: str, limit: int = 5) -> list[LiveTrainStatus]`.



2. `railrisk/providers/ntes_provider.py`:

   - Class `NTESProvider` to fetch live train running status.

   - Wrap network calls with `tenacity` retry logic on HTTP 429/503 status codes.

   - Fallback mechanism: If all retries fail or an network exception occurs, fetch the latest snapshot from `railrisk/db/repository.py`.

railrisk/db/repository.py
Python
"""
railrisk/db/repository.py
-------------------------
Local SQLite persistence module for caching live train snapshots
and retrieving historical delay records for volatility computation.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from railrisk.models.train import LiveTrainStatus

DEFAULT_DB_PATH = Path("railrisk.db")


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Creates and configures a SQLite connection with WAL mode enabled
    for performance and concurrency.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """
    Initializes the SQLite schema if the table does not exist.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS train_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                train_number TEXT NOT NULL,
                current_station TEXT NOT NULL,
                target_station TEXT NOT NULL,
                scheduled_arrival TEXT NOT NULL,
                delay_minutes INTEGER NOT NULL,
                halt_duration_mins INTEGER NOT NULL,
                captured_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_train_captured 
            ON train_snapshots (train_number, captured_at DESC);
            """
        )
        conn.commit()


def save_snapshot(status: LiveTrainStatus, db_path: Path = DEFAULT_DB_PATH) -> None:
    """
    Persists a LiveTrainStatus snapshot into SQLite.
    """
    init_db(db_path)

    captured_at = getattr(
        status, "captured_at", None
    ) or datetime.now(timezone.utc)
    if isinstance(captured_at, datetime):
        captured_at_str = captured_at.isoformat()
    else:
        captured_at_str = str(captured_at)

    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO train_snapshots (
                train_number,
                current_station,
                target_station,
                scheduled_arrival,
                delay_minutes,
                halt_duration_mins,
                captured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                status.train_number,
                status.current_station,
                status.target_station,
                status.scheduled_arrival.isoformat(),
                status.delay_minutes,
                status.halt_duration_mins,
                captured_at_str,
            ),
        )
        conn.commit()


def get_recent_snapshots(
    train_number: str, limit: int = 5, db_path: Path = DEFAULT_DB_PATH
) -> List[LiveTrainStatus]:
    """
    Retrieves the most recent N delay snapshots for a train, ordered chronologically.
    """
    init_db(db_path)

    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT 
                train_number,
                current_station,
                target_station,
                scheduled_arrival,
                delay_minutes,
                halt_duration_mins,
                captured_at
            FROM train_snapshots
            WHERE train_number = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (train_number, limit),
        )
        rows = cursor.fetchall()

    snapshots: List[LiveTrainStatus] = []
    # Reverse to return snapshots in ascending chronological order
    for row in reversed(rows):
        status = LiveTrainStatus(
            train_number=row["train_number"],
            current_station=row["current_station"],
            target_station=row["target_station"],
            scheduled_arrival=row["scheduled_arrival"],
            delay_minutes=row["delay_minutes"],
            halt_duration_mins=row["halt_duration_mins"],
        )
        # Attach captured_at timestamp for engine volatility evaluation
        try:
            status.captured_at = datetime.fromisoformat(row["captured_at"])
        except (ValueError, TypeError):
            status.captured_at = status.scheduled_arrival

        snapshots.append(status)

    return snapshots
railrisk/providers/ntes_provider.py
Python
"""
railrisk/providers/ntes_provider.py
-----------------------------------
Live NTES / Indian Railways API status fetcher wrapped with Tenacity
retry logic and local SQLite fallback caching.
"""

from pathlib import Path
from typing import Optional
import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from railrisk.db.repository import get_recent_snapshots, save_snapshot
from railrisk.models.train import LiveTrainStatus


class TransientAPIError(Exception):
    """Raised when railway API returns retriable status codes like 429 or 503."""

    pass


class StationFetchError(Exception):
    """Raised when no live data or cached snapshot is available."""

    pass


def _is_transient_error(exception: BaseException) -> bool:
    """Helper filter for tenacity to identify retriable HTTP or connection errors."""
    if isinstance(exception, TransientAPIError):
        return True
    if isinstance(exception, (requests.ConnectionError, requests.Timeout)):
        return True
    return False


class NTESProvider:
    """
    Client for fetching live running status from NTES/Railway endpoints
    with automated retries and local DB snapshot fallback.
    """

    def __init__(
        self,
        base_url: str = "https://api.railway.example.com/v1",
        api_key: Optional[str] = None,
        db_path: Optional[Path] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.db_path = db_path

    @retry(
        retry=retry_if_exception(_is_transient_error),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _execute_request(self, train_number: str) -> dict:
        """
        Executes HTTP GET request against public API endpoint with retry logic.
        """
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        url = f"{self.base_url}/live/{train_number}"
        response = requests.get(url, headers=headers, timeout=5.0)

        # Retry specifically on 429 Rate Limit and 503 Service Unavailable
        if response.status_code in (429, 503):
            raise TransientAPIError(
                f"HTTP {response.status_code} transient error from NTES provider"
            )

        response.raise_for_status()
        return response.json()

    def get_live_status(
        self, train_number: str, target_station: str
    ) -> LiveTrainStatus:
        """
        Attempts to fetch live train status over network.
        Saves fresh snapshot to SQLite on success.
        Falls back to local SQLite cache if network/API fails.
        """
        try:
            payload = self._execute_request(train_number)

            # Ensure target station matches requested delivery station
            if "target_station" not in payload:
                payload["target_station"] = target_station

            status = LiveTrainStatus.model_validate(payload)

            # Save fresh snapshot to SQLite database
            save_snapshot(
                status,
                db_path=self.db_path
                if self.db_path
                else Path("railrisk.db"),
            )
            return status

        except Exception as err:
            # Network call failed after retries — fallback to stale data from local DB
            db_kwargs = {"db_path": self.db_path} if self.db_path else {}
            cached_snapshots = get_recent_snapshots(
                train_number, limit=1, **db_kwargs
            )

            if cached_snapshots:
                return cached_snapshots[-1]

            raise StationFetchError(
                f"Unable to fetch live status for train '{train_number}' "
                f"and no cached snapshot exists in local SQLite. Original error: {err}"
            ) from err
I reviewed your initial `repository.py` and `ntes_provider.py` code. Before integrating it, we need to address three production edge cases:



1. SQLite Resource Leaks & Concurrency: Raw connection handling will cause locks during rapid retry cycles. Refactor `railrisk/db/repository.py` to use Python context managers (`with sqlite3.connect(...)`) and enable Write-Ahead Logging (WAL) mode on startup.

2. Thundering Herd Prevention: Basic `tenacity` retry backoff lacks randomized jitter. Update `railrisk/providers/ntes_provider.py` to use exponential backoff with full jitter (`wait_random_exponential`).

3. Malformed Payload Resilience: If external API responses return corrupted JSON or missing fields, raise a explicit `ProviderDataError` before triggering the SQLite fallback path, rather than letting raw Pydantic or KeyError exceptions crash the process.



Please provide the updated, hardened implementations for both files.

railrisk/db/repository.py
Python
"""
railrisk/db/repository.py
-------------------------
Hardened local SQLite persistence module with connection pooling semantics,
automatic cleanup via context managers, and Write-Ahead Logging (WAL) mode.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List

from railrisk.models.train import LiveTrainStatus

DEFAULT_DB_PATH = Path("railrisk.db")


@contextmanager
def get_db_connection(
    db_path: Path = DEFAULT_DB_PATH, timeout: float = 10.0
) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager for managing SQLite connections.
    Guarantees Write-Ahead Logging (WAL) mode, manages transaction commits/rollbacks,
    and explicitly closes connections to prevent resource leaks and file locks.
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    conn.row_factory = sqlite3.Row
    try:
        # Enable Write-Ahead Logging for higher write concurrency during retry bursts
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """
    Initializes the database schema if non-existent.
    """
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS train_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                train_number TEXT NOT NULL,
                current_station TEXT NOT NULL,
                target_station TEXT NOT NULL,
                scheduled_arrival TEXT NOT NULL,
                delay_minutes INTEGER NOT NULL,
                halt_duration_mins INTEGER NOT NULL,
                captured_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_train_captured 
            ON train_snapshots (train_number, captured_at DESC);
            """
        )


def save_snapshot(status: LiveTrainStatus, db_path: Path = DEFAULT_DB_PATH) -> None:
    """
    Persists a LiveTrainStatus snapshot into SQLite using a managed transaction.
    """
    init_db(db_path)

    captured_at = getattr(status, "captured_at", None) or datetime.now(timezone.utc)
    if isinstance(captured_at, datetime):
        captured_at_str = captured_at.isoformat()
    else:
        captured_at_str = str(captured_at)

    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO train_snapshots (
                train_number,
                current_station,
                target_station,
                scheduled_arrival,
                delay_minutes,
                halt_duration_mins,
                captured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                status.train_number,
                status.current_station,
                status.target_station,
                status.scheduled_arrival.isoformat(),
                status.delay_minutes,
                status.halt_duration_mins,
                captured_at_str,
            ),
        )


def get_recent_snapshots(
    train_number: str, limit: int = 5, db_path: Path = DEFAULT_DB_PATH
) -> List[LiveTrainStatus]:
    """
    Retrieves recent historical delay snapshots ordered chronologically.
    """
    init_db(db_path)

    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT 
                train_number,
                current_station,
                target_station,
                scheduled_arrival,
                delay_minutes,
                halt_duration_mins,
                captured_at
            FROM train_snapshots
            WHERE train_number = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (train_number, limit),
        )
        rows = cursor.fetchall()

    snapshots: List[LiveTrainStatus] = []
    # Reverse to restore ascending chronological order
    for row in reversed(rows):
        status = LiveTrainStatus(
            train_number=row["train_number"],
            current_station=row["current_station"],
            target_station=row["target_station"],
            scheduled_arrival=row["scheduled_arrival"],
            delay_minutes=row["delay_minutes"],
            halt_duration_mins=row["halt_duration_mins"],
        )
        try:
            status.captured_at = datetime.fromisoformat(row["captured_at"])
        except (ValueError, TypeError):
            status.captured_at = status.scheduled_arrival

        snapshots.append(status)

    return snapshots
railrisk/providers/ntes_provider.py
Python
"""
railrisk/providers/ntes_provider.py
-----------------------------------
Resilient NTES train status fetcher featuring randomized exponential backoff jitter
and strict validation error handling for malformed upstream payloads.
"""

from pathlib import Path
from typing import Optional
import requests
from pydantic import ValidationError
from requests.exceptions import JSONDecodeError, RequestException
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from railrisk.db.repository import DEFAULT_DB_PATH, get_recent_snapshots, save_snapshot
from railrisk.models.train import LiveTrainStatus


class TransientAPIError(Exception):
    """Raised when railway API returns retriable status codes (e.g. HTTP 429, 503)."""

    pass


class ProviderDataError(Exception):
    """Raised when external API returns corrupt JSON, missing fields, or invalid schemas."""

    pass


class StationFetchError(Exception):
    """Raised when both live network fetch and local SQLite fallback fail."""

    pass


def _is_transient_error(exception: BaseException) -> bool:
    """Filter for tenacity to isolate retriable network or rate-limit exceptions."""
    if isinstance(exception, TransientAPIError):
        return True
    if isinstance(exception, (requests.ConnectionError, requests.Timeout)):
        return True
    return False


class NTESProvider:
    """
    Client for fetching live running status from NTES/Railway endpoints.
    """

    def __init__(
        self,
        base_url: str = "https://api.railway.example.com/v1",
        api_key: Optional[str] = None,
        db_path: Path = DEFAULT_DB_PATH,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.db_path = db_path

    @retry(
        retry=retry_if_exception(_is_transient_error),
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),  # Full jitter to prevent thundering herd
        reraise=True,
    )
    def _execute_request(self, train_number: str) -> dict:
        """
        Executes HTTP request with full jitter exponential backoff.
        """
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        url = f"{self.base_url}/live/{train_number}"
        try:
            response = requests.get(url, headers=headers, timeout=5.0)
        except RequestException as err:
            raise err

        if response.status_code in (429, 503):
            raise TransientAPIError(
                f"HTTP {response.status_code} transient response from endpoint."
            )

        response.raise_for_status()

        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ProviderDataError(f"Expected JSON object response, received {type(data).__name__}")
            return data
        except (JSONDecodeError, ValueError) as err:
            raise ProviderDataError(f"Corrupted or non-JSON response payload: {err}") from err

    def get_live_status(
        self, train_number: str, target_station: str
    ) -> LiveTrainStatus:
        """
        Attempts network fetch, validates schema safely, persists valid data,
        and gracefully degrades to local SQLite cache if network or data issues occur.
        """
        try:
            payload = self._execute_request(train_number)

            if "target_station" not in payload:
                payload["target_station"] = target_station

            # Validate raw JSON payload via Pydantic model
            try:
                status = LiveTrainStatus.model_validate(payload)
            except (ValidationError, KeyError, TypeError) as val_err:
                raise ProviderDataError(
                    f"Malformed schema for train '{train_number}': {val_err}"
                ) from val_err

            # Persist fresh snapshot to SQLite
            save_snapshot(status, db_path=self.db_path)
            return status

        except (RequestException, TransientAPIError, ProviderDataError) as err:
            # Trigger fallback path on network issues or corrupted payloads
            cached_snapshots = get_recent_snapshots(
                train_number, limit=1, db_path=self.db_path
            )

            if cached_snapshots:
                return cached_snapshots[-1]

            raise StationFetchError(
                f"Failed to retrieve live status for train '{train_number}' "
                f"and no cached snapshot exists in local database. Cause: {err}"
            ) from err
Create a complete offline unit test suite in `tests/test_predictor.py` using `pytest` and `unittest.mock`.



Requirements:

1. Ensure zero external network calls occur during test execution.

2. Test Timezone Coercion: Validate that naive ISO datetimes are coerced to `Asia/Kolkata` (IST) without throwing arithmetic type errors.

3. Test Volatility Calculations: Verify drift rates (delay change over time) for train acceleration and deceleration scenarios.

4. Test Risk Matrix: Assert correct `RiskLevel` and `Recommendation` mappings based on effective vendor time margins.

5. Test AST Sandbox Security: Validate that `evaluate_formula` successfully evaluates math strings while rejecting forbidden constructs (`ast.Call`, `ast.Import`, `ast.Attribute`).

6. Test Fallback Mechanism: Mock a network failure in `NTESProvider` and assert that cached SQLite data is successfully returned.

Here is the complete offline unit test suite for tests/test_predictor.py built using pytest and unittest.mock.

Python
"""
tests/test_predictor.py
------------------------
Offline unit test suite for RailRisk engine, validation models,
dynamic formula evaluator, volatility calculator, and local persistence fallback.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

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
from railrisk.providers.ntes_provider import NTESProvider

# Standard IST timezone offset (+05:30)
IST = timezone(timedelta(hours=5, minutes=30))


# =====================================================================
# 1. TIMEZONE COERCION TESTS
# =====================================================================
def test_timezone_coercion_from_naive_iso():
    """
    Validates that naive ISO datetime strings are automatically coerced to IST (+05:30)
    and can perform arithmetic against timezone-aware timestamps without TypeError.
    """
    # Naive ISO datetime string without offset
    status = LiveTrainStatus(
        train_number="12301",
        current_station="CNB",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T14:30:00",  # Naive
        delay_minutes=30,
        halt_duration_mins=10,
    )

    # 1. Assert timezone offset was assigned to IST
    assert status.scheduled_arrival.tzinfo is not None
    assert status.scheduled_arrival.utcoffset() == timedelta(hours=5, minutes=30)

    # 2. Perform arithmetic with explicit timezone-aware UTC datetime
    now_utc = datetime.now(timezone.utc)
    time_delta = status.estimated_arrival - now_utc

    # Must evaluate without throwing 'TypeError: can't subtract offset-naive and offset-aware datetimes'
    assert isinstance(time_delta, timedelta)


# =====================================================================
# 2. VOLATILITY ENGINE TESTS
# =====================================================================
def test_delay_drift_deceleration_and_acceleration():
    """
    Verifies delay variance drift rates (minutes per hour):
    - Positive drift: Train accumulates delay (slower/decelerating).
    - Negative drift: Train recovers lost time (faster/accelerating).
    """
    base_time = datetime(2026, 9, 10, 10, 0, 0, tzinfo=IST)

    # Scenario A: Deceleration (Delay increases from 10m to 40m over 2 hours = +15.0 mins/hr)
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

    decel_drift = calculate_delay_drift([s1, s2])
    assert decel_drift == 15.0

    # Scenario B: Acceleration (Delay decreases from 60m to 0m over 2 hours = -30.0 mins/hr)
    s3 = LiveTrainStatus(
        train_number="12301",
        current_station="ALJN",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T18:00:00+05:30",
        delay_minutes=0,
        halt_duration_mins=5,
    )
    s3.captured_at = base_time + timedelta(hours=2)

    # Re-use s1 with 60 min initial delay
    s1.delay_minutes = 60
    accel_drift = calculate_delay_drift([s1, s3])
    assert accel_drift == -30.0


# =====================================================================
# 3. RISK MATRIX EVALUATION TESTS
# =====================================================================
def test_risk_matrix_evaluations():
    """
    Asserts correct RiskLevel and Recommendation mappings based on time margins.
    """
    ref_time = datetime(2026, 9, 10, 12, 0, 0, tzinfo=IST)

    # Case 1: LOW Risk / PROCEED (Train arriving in 120 mins, vendor needs 45m prep + 15m lead = 60m)
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

    # Case 2: MODERATE Risk / WARN_VENDOR (Effective buffer is tight ~15 mins)
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

    # Case 3: CRITICAL Risk / CANCEL_ORDER (Negative buffer: train arrives in 30 mins, vendor needs 60 mins)
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
# 4. AST SANDBOX SECURITY TESTS
# =====================================================================
def test_ast_sandbox_valid_formulas():
    """Validates safe dynamic mathematical evaluation."""
    context = {
        "effective_buffer_mins": 20.0,
        "drift_rate_mph": -10.0,
        "delay_minutes": 15.0,
    }
    formula = "(100 - effective_buffer_mins * 2) + (-drift_rate_mph * 1.5)"
    result = evaluate_formula(formula, context)
    assert result == 75.0


def test_ast_sandbox_security_rejections():
    """Ensures forbidden constructs (ast.Call, ast.Import, ast.Attribute) raise SecurityError."""
    context = {"effective_buffer_mins": 20.0}

    # Reject Function Calls (ast.Call)
    with pytest.raises(SecurityError, match="Forbidden or unsafe expression construct"):
        evaluate_formula("abs(effective_buffer_mins)", context)

    # Reject Import / System Access
    with pytest.raises((SecurityError, FormulaEvaluationError)):
        evaluate_formula("__import__('os').system('echo hacked')", context)

    # Reject Attribute Access (ast.Attribute)
    with pytest.raises(SecurityError, match="Forbidden or unsafe expression construct"):
        evaluate_formula("effective_buffer_mins.__class__", context)


# =====================================================================
# 5. PROVIDER PERSISTENCE & NETWORK FALLBACK TESTS
# =====================================================================
def test_provider_network_fallback_to_sqlite(tmp_path: Path):
    """
    Mocks network failure in NTESProvider and asserts that cached snapshot data
    is successfully retrieved from local SQLite database.
    """
    db_file = tmp_path / "test_railrisk.db"

    # 1. Seed local SQLite database with a historical snapshot
    cached_status = LiveTrainStatus(
        train_number="12301",
        current_station="PRYJ",
        target_station="NDLS",
        scheduled_arrival="2026-09-10T20:00:00+05:30",
        delay_minutes=25,
        halt_duration_mins=10,
    )
    save_snapshot(cached_status, db_path=db_file)

    # Verify snapshot was stored
    stored = get_recent_snapshots("12301", limit=1, db_path=db_file)
    assert len(stored) == 1
    assert stored[0].current_station == "PRYJ"

    # 2. Instantiate NTESProvider targeting isolated database
    provider = NTESProvider(
        base_url="https://api.railway.example.com/v1",
        db_path=db_file,
    )

    # 3. Mock requests.get to simulate a persistent connection error
    with patch("requests.get", side_effect=requests.ConnectionError("Network connection down")):
        # Execute provider status call — should trigger SQLite fallback
        fallback_status = provider.get_live_status("12301", "NDLS")

        assert fallback_status is not None
        assert fallback_status.train_number == "12301"
        assert fallback_status.current_station == "PRYJ"
        assert fallback_status.delay_minutes == 25
Test Suite Highlights
Isolation & Safety: Uses pytest's native tmp_path fixture to spin up transient SQLite databases per test run, ensuring no state leaks between executions.

Strict Network Isolation: Mocks network interfaces (requests.get) to verify the fallback logic without making real HTTP requests.

Pydantic & Exception Coverage: Covers timezone parsing, arithmetic bounds, risk level categorizations, security boundaries for mathematical parsing, and persistence layer failure modes.

I reviewed the generated unit tests in `tests/test_predictor.py`. Let's refine the test suite to enforce production-level standards:



1. Exception Granularity: Ensure security tests explicitly assert custom exceptions (`SecurityError`, `FormulaEvaluationError`, and Pydantic `ValidationError`) rather than generic `Exception`.

2. Midnight & Timezone Boundaries: Add a specific test case where train schedule datetimes cross midnight (e.g., 23:45 to 00:30 IST) to verify elapsed time arithmetic does not yield negative duration artifacts.

3. Complete Network Isolation: Verify that `unittest.mock.patch` completely intercepts all network dependencies inside `NTESProvider` so zero HTTP traffic occurs during `pytest` execution.



Please provide the updated `tests/test_predictor.py` file.

Here is the updated, production-hardened test suite for tests/test_predictor.py.

Python
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
Build the terminal CLI interface across `railrisk/cli.py` and `main.py`.



Requirements:



1. `railrisk/cli.py`:

   - Parse command-line arguments using `argparse`:

     - `--train` (5-digit string)

     - `--target-station` (station code)

     - `--prep-time` (integer, default: 45)

     - `--lead-time` (integer, default: 15)

     - `--formula` (optional mathematical risk scoring string)

   - Fetch live train status via `NTESProvider`.

   - Calculate volatility drift across recent snapshots.

   - Run risk assessment (and evaluate `--formula` if supplied).

   - Render a formatted ASCII terminal report showing train status, calculated drift rate, effective margin buffer, risk level, and recommended action.



2. `main.py`:

   - Provide the root execution wrapper calling `railrisk.cli.main()`.

railrisk/cli.py
Python
"""
railrisk/cli.py
---------------
Terminal CLI interface for RailRisk. Interactively fetches train status,
calculates delay volatility, evaluates vendor delivery risk, and outputs
a formatted ASCII assessment report.
"""

import argparse
from pathlib import Path
import sys
from typing import Optional

from railrisk.db.repository import DEFAULT_DB_PATH, get_recent_snapshots
from railrisk.engine.dynamic_eval import (
    FormulaEvaluationError,
    SecurityError,
    evaluate_formula,
)
from railrisk.engine.risk_calculator import assess_order_risk
from railrisk.engine.volatility import calculate_delay_drift
from railrisk.models.risk import Recommendation, RiskLevel
from railrisk.providers.ntes_provider import (
    NTESProvider,
    ProviderDataError,
    StationFetchError,
)


def _build_parser() -> argparse.ArgumentParser:
    """Configures command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="railrisk",
        description="RailRisk: Train delay volatility & e-catering delivery risk evaluator.",
    )
    parser.add_argument(
        "-t",
        "--train",
        required=True,
        type=str,
        help="5-digit Indian Railways train number (e.g., '12301')",
    )
    parser.add_argument(
        "-s",
        "--target-station",
        required=True,
        type=str,
        help="Target station code for food delivery (e.g., 'NDLS')",
    )
    parser.add_argument(
        "--prep-time",
        type=int,
        default=45,
        help="Vendor kitchen food preparation time in minutes (default: 45)",
    )
    parser.add_argument(
        "--lead-time",
        type=int,
        default=15,
        help="Platform delivery lead time in minutes (default: 15)",
    )
    parser.add_argument(
        "--formula",
        type=str,
        default=None,
        help="Optional dynamic math formula (e.g., '(100 - effective_buffer_mins * 2) + (-drift_rate_mph * 1.5)')",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to local SQLite snapshot cache database",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Optional API key for railway status endpoint",
    )
    return parser


def _render_ascii_report(
    train_number: str,
    current_station: str,
    target_station: str,
    scheduled_arrival: str,
    estimated_arrival: str,
    delay_minutes: int,
    drift_rate: float,
    effective_buffer: float,
    projected_buffer: float,
    risk_score: float,
    risk_level: RiskLevel,
    recommendation: Recommendation,
    details: str,
    custom_formula_score: Optional[float] = None,
) -> None:
    """Renders a formatted ASCII box summary report in the terminal."""
    banner_symbol = "✓" if risk_level == RiskLevel.LOW else ("⚠" if risk_level == RiskLevel.MODERATE else "✖")
    
    border = "=" * 68
    subborder = "-" * 68

    print("\n" + border)
    print(f" RAILRISK ASSESSMENT REPORT | TRAIN #{train_number} -> {target_station}")
    print(border)
    print(f" Current Location      : {current_station}")
    print(f" Scheduled Arrival     : {scheduled_arrival}")
    print(f" Estimated Arrival     : {estimated_arrival} (Delay: {delay_minutes:+d} mins)")
    print(subborder)
    print(f" Delay Drift Velocity  : {drift_rate:+.2f} mins/hour")
    print(f" Effective Margin      : {effective_buffer:+.1f} mins")
    print(f" Projected Margin      : {projected_buffer:+.1f} mins (drift adjusted)")
    print(subborder)
    print(f" Risk Score            : {risk_score:.1f} / 100.0")
    if custom_formula_score is not None:
        print(f" Custom Formula Score  : {custom_formula_score:.1f} / 100.0")
    print(f" Risk Level            : [{banner_symbol}] {risk_level.value}")
    print(f" Recommendation        : >>> {recommendation.value} <<<")
    print(subborder)
    print(f" Assessment Details    : {details}")
    print(border + "\n")


def main() -> None:
    """CLI execution entrypoint."""
    parser = _build_parser()
    args = parser.parse_args()

    provider = NTESProvider(api_key=args.api_key, db_path=args.db_path)

    try:
        # 1. Fetch live train status (or fallback to SQLite cache)
        status = provider.get_live_status(
            train_number=args.train, target_station=args.target_station
        )

        # 2. Fetch recent snapshots to compute delay drift rate
        recent_snapshots = get_recent_snapshots(
            train_number=args.train, limit=5, db_path=args.db_path
        )
        drift_rate = calculate_delay_drift(recent_snapshots)

        # 3. Assess food delivery risk
        assessment = assess_order_risk(
            status=status,
            drift_rate=drift_rate,
            order_prep_mins=args.prep_time,
            delivery_lead_mins=args.lead_time,
        )

        # 4. Safely evaluate custom formula if supplied
        custom_score: Optional[float] = None
        if args.formula:
            context = {
                "effective_buffer_mins": assessment.effective_buffer_mins,
                "projected_buffer_mins": assessment.projected_buffer_mins,
                "drift_rate_mph": assessment.drift_rate_mph,
                "delay_minutes": float(status.delay_minutes),
                "halt_duration_mins": float(status.halt_duration_mins),
            }
            try:
                custom_score = evaluate_formula(args.formula, context)
            except (SecurityError, FormulaEvaluationError) as err:
                print(f"[Warning] Custom formula evaluation failed: {err}", file=sys.stderr)

        # 5. Render ASCII terminal report
        _render_ascii_report(
            train_number=status.train_number,
            current_station=status.current_station,
            target_station=status.target_station,
            scheduled_arrival=status.scheduled_arrival.strftime("%Y-%m-%d %H:%M %Z"),
            estimated_arrival=status.estimated_arrival.strftime("%Y-%m-%d %H:%M %Z"),
            delay_minutes=status.delay_minutes,
            drift_rate=assessment.drift_rate_mph,
            effective_buffer=assessment.effective_buffer_mins,
            projected_buffer=assessment.projected_buffer_mins,
            risk_score=assessment.risk_score,
            risk_level=assessment.risk_level,
            recommendation=assessment.recommendation,
            details=assessment.details,
            custom_formula_score=custom_score,
        )

    except (StationFetchError, ProviderDataError) as err:
        print(f"\n[RailRisk Error] {err}", file=sys.stderr)
        sys.exit(1)
    except Exception as err:
        print(f"\n[Unexpected Error] {err}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
main.py
Python
"""
main.py
-------
Root execution wrapper for running RailRisk directly from project directory.
Usage:
    python main.py --train 12301 --target-station NDLS --prep-time 45
"""

import sys
from railrisk.cli import main

if __name__ == "__main__":
    sys.exit(main())
Before finalizing `railrisk/cli.py`, let's address CLI production hardening:



1. Input Validation & Exit Codes: Validate that `--train` strictly matches a 5-digit regex pattern. Return explicit exit codes (`0` for success, `1` for invalid arguments, `2` for data/provider failures).

2. Cache Awareness Indicator: If the assessment relies on SQLite cached snapshots due to an API failure, display a clear header flag `[OFFLINE / CACHED DATA]` in the ASCII output.

3. Dynamic Formula Error Handling: If a custom `--formula` fails evaluation (syntax error or blocked AST node), catch `SecurityError` or `FormulaEvaluationError`, display a user warning, and cleanly fall back to the default rule engine.



Please provide the final updated code for `railrisk/cli.py` and `main.py`.

railrisk/cli.py
Python
"""
railrisk/cli.py
---------------
Terminal CLI interface for RailRisk. Interactively fetches train status,
calculates delay volatility, evaluates vendor delivery risk, and outputs
a formatted ASCII assessment report.
"""

import argparse
from pathlib import Path
import re
import sys
from typing import Optional

from railrisk.db.repository import DEFAULT_DB_PATH, get_recent_snapshots
from railrisk.engine.dynamic_eval import (
    FormulaEvaluationError,
    SecurityError,
    evaluate_formula,
)
from railrisk.engine.risk_calculator import assess_order_risk
from railrisk.engine.volatility import calculate_delay_drift
from railrisk.models.risk import Recommendation, RiskLevel
from railrisk.providers.ntes_provider import (
    NTESProvider,
    ProviderDataError,
    StationFetchError,
)


def _build_parser() -> argparse.ArgumentParser:
    """Configures command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="railrisk",
        description="RailRisk: Train delay volatility & e-catering delivery risk evaluator.",
    )
    parser.add_argument(
        "-t",
        "--train",
        required=True,
        type=str,
        help="5-digit Indian Railways train number (e.g., '12301')",
    )
    parser.add_argument(
        "-s",
        "--target-station",
        required=True,
        type=str,
        help="Target station code for food delivery (e.g., 'NDLS')",
    )
    parser.add_argument(
        "--prep-time",
        type=int,
        default=45,
        help="Vendor kitchen food preparation time in minutes (default: 45)",
    )
    parser.add_argument(
        "--lead-time",
        type=int,
        default=15,
        help="Platform delivery lead time in minutes (default: 15)",
    )
    parser.add_argument(
        "--formula",
        type=str,
        default=None,
        help="Optional dynamic math formula (e.g., '(100 - effective_buffer_mins * 2) + (-drift_rate_mph * 1.5)')",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to local SQLite snapshot cache database",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Optional API key for railway status endpoint",
    )
    return parser


def _render_ascii_report(
    train_number: str,
    current_station: str,
    target_station: str,
    scheduled_arrival: str,
    estimated_arrival: str,
    delay_minutes: int,
    drift_rate: float,
    effective_buffer: float,
    projected_buffer: float,
    risk_score: float,
    risk_level: RiskLevel,
    recommendation: Recommendation,
    details: str,
    is_cached: bool = False,
    custom_formula_score: Optional[float] = None,
) -> None:
    """Renders a formatted ASCII box summary report in the terminal."""
    banner_symbol = "✓" if risk_level == RiskLevel.LOW else ("⚠" if risk_level == RiskLevel.MODERATE else "✖")
    cache_tag = " [OFFLINE / CACHED DATA]" if is_cached else ""

    border = "=" * 68
    subborder = "-" * 68

    print("\n" + border)
    print(f" RAILRISK ASSESSMENT REPORT{cache_tag} | TRAIN #{train_number} -> {target_station}")
    print(border)
    print(f" Current Location      : {current_station}")
    print(f" Scheduled Arrival     : {scheduled_arrival}")
    print(f" Estimated Arrival     : {estimated_arrival} (Delay: {delay_minutes:+d} mins)")
    print(subborder)
    print(f" Delay Drift Velocity  : {drift_rate:+.2f} mins/hour")
    print(f" Effective Margin      : {effective_buffer:+.1f} mins")
    print(f" Projected Margin      : {projected_buffer:+.1f} mins (drift adjusted)")
    print(subborder)
    print(f" Risk Score            : {risk_score:.1f} / 100.0")
    if custom_formula_score is not None:
        print(f" Custom Formula Score  : {custom_formula_score:.1f} / 100.0")
    print(f" Risk Level            : [{banner_symbol}] {risk_level.value}")
    print(f" Recommendation        : >>> {recommendation.value} <<<")
    print(subborder)
    print(f" Assessment Details    : {details}")
    print(border + "\n")


def main() -> int:
    """
    CLI execution entrypoint.

    Exit Codes:
        0: Successful execution
        1: Input validation / argument error
        2: Data provider / network / database failure
    """
    parser = _build_parser()
    args = parser.parse_args()

    # 1. Strict Input Validation
    train_number = args.train.strip()
    if not re.match(r"^\d{5}$", train_number):
        print(
            f"[RailRisk Argument Error] Invalid train number '{args.train}'. Must be exactly 5 digits.",
            file=sys.stderr,
        )
        return 1

    if args.prep_time < 0 or args.lead_time < 0:
        print(
            "[RailRisk Argument Error] Prep time and lead time must be non-negative integers.",
            file=sys.stderr,
        )
        return 1

    provider = NTESProvider(api_key=args.api_key, db_path=args.db_path)

    try:
        # 2. Fetch live train status (or fallback to SQLite cache)
        status = provider.get_live_status(
            train_number=train_number, target_station=args.target_station
        )
        is_cached = getattr(status, "is_cached", False) or getattr(status, "_from_cache", False)

        # 3. Fetch recent snapshots to compute delay drift rate
        recent_snapshots = get_recent_snapshots(
            train_number=train_number, limit=5, db_path=args.db_path
        )
        drift_rate = calculate_delay_drift(recent_snapshots)

        # 4. Assess food delivery risk via core engine
        assessment = assess_order_risk(
            status=status,
            drift_rate=drift_rate,
            order_prep_mins=args.prep_time,
            delivery_lead_mins=args.lead_time,
        )

        # 5. Safely evaluate custom formula if supplied
        custom_score: Optional[float] = None
        if args.formula:
            context = {
                "effective_buffer_mins": assessment.effective_buffer_mins,
                "projected_buffer_mins": assessment.projected_buffer_mins,
                "drift_rate_mph": assessment.drift_rate_mph,
                "delay_minutes": float(status.delay_minutes),
                "halt_duration_mins": float(status.halt_duration_mins),
            }
            try:
                custom_score = evaluate_formula(args.formula, context)
            except (SecurityError, FormulaEvaluationError) as err:
                print(
                    f"[Warning] Custom formula evaluation failed ({err}). "
                    "Falling back to default risk engine score.",
                    file=sys.stderr,
                )

        # 6. Render ASCII terminal report
        _render_ascii_report(
            train_number=status.train_number,
            current_station=status.current_station,
            target_station=status.target_station,
            scheduled_arrival=status.scheduled_arrival.strftime("%Y-%m-%d %H:%M %Z"),
            estimated_arrival=status.estimated_arrival.strftime("%Y-%m-%d %H:%M %Z"),
            delay_minutes=status.delay_minutes,
            drift_rate=assessment.drift_rate_mph,
            effective_buffer=assessment.effective_buffer_mins,
            projected_buffer=assessment.projected_buffer_mins,
            risk_score=assessment.risk_score,
            risk_level=assessment.risk_level,
            recommendation=assessment.recommendation,
            details=assessment.details,
            is_cached=is_cached,
            custom_formula_score=custom_score,
        )
        return 0

    except (StationFetchError, ProviderDataError) as err:
        print(f"\n[RailRisk Data Error] {err}", file=sys.stderr)
        return 2
    except Exception as err:
        print(f"\n[Unexpected Error] {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
main.py
Python
"""
main.py
-------
Root execution wrapper for running RailRisk directly from the project directory.
Usage:
    python main.py --train 12301 --target-station NDLS --prep-time 45
"""

import sys
from railrisk.cli import main

if __name__ == "__main__":
    sys.exit(main())



Gemini is AI and can make mistakes.