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