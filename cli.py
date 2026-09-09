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