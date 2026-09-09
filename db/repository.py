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