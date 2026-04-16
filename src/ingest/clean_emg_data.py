"""Cleaning and validation helpers for raw EMG CSV data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)
TEXT_COLUMNS = ["subject_id", "session_id", "gesture_label"]
NUMERIC_COLUMNS = [f"channel_{index}" for index in range(1, 9)]
REQUIRED_COLUMNS = ["subject_id", "session_id", "timestamp", *NUMERIC_COLUMNS, "gesture_label"]


class EmgDataValidationError(ValueError):
    """Raised when an EMG dataset cannot be cleaned for loading."""


@dataclass(frozen=True, slots=True)
class CleaningResult:
    dataframe: pd.DataFrame
    rejected_rows: int


def read_emg_csv(csv_path: str | Path) -> pd.DataFrame:
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_file}")

    LOGGER.info("Reading EMG CSV from %s", csv_file)
    dataframe = pd.read_csv(csv_file)
    if dataframe.empty:
        raise EmgDataValidationError(f"CSV file is empty: {csv_file}")

    return dataframe


def validate_required_columns(dataframe: pd.DataFrame) -> None:
    normalized_columns = [str(column).strip() for column in dataframe.columns]
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in normalized_columns]

    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise EmgDataValidationError(f"CSV is missing required columns: {missing_list}")


def _normalize_timestamp(series: pd.Series) -> pd.Series:
    timestamp_series = pd.to_datetime(series, errors="coerce")
    if isinstance(timestamp_series.dtype, pd.DatetimeTZDtype):
        timestamp_series = timestamp_series.dt.tz_convert("UTC").dt.tz_localize(None)
    return timestamp_series


def clean_emg_dataframe(dataframe: pd.DataFrame) -> CleaningResult:
    validate_required_columns(dataframe)

    cleaned = dataframe.copy()
    cleaned.columns = [str(column).strip() for column in cleaned.columns]
    # Keep the downstream load contract explicit and stable.
    cleaned = cleaned.loc[:, REQUIRED_COLUMNS].copy()

    for column in TEXT_COLUMNS:
        cleaned[column] = cleaned[column].astype("string").str.strip()
        cleaned[column] = cleaned[column].replace("", pd.NA)

    cleaned["gesture_label"] = cleaned["gesture_label"].str.lower()
    cleaned["timestamp"] = _normalize_timestamp(cleaned["timestamp"])

    for column in NUMERIC_COLUMNS:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    invalid_rows = cleaned[REQUIRED_COLUMNS].isna().any(axis=1)
    rejected_rows = int(invalid_rows.sum())

    if rejected_rows:
        LOGGER.warning("Rejected %s rows with null or invalid values in required fields", rejected_rows)

    cleaned = cleaned.loc[~invalid_rows].copy()
    if cleaned.empty:
        raise EmgDataValidationError("No valid EMG rows remain after cleaning")

    for column in TEXT_COLUMNS:
        cleaned[column] = cleaned[column].astype(str)

    LOGGER.info("Prepared %s valid EMG rows for database load", len(cleaned))
    return CleaningResult(dataframe=cleaned, rejected_rows=rejected_rows)


def load_and_clean_emg_csv(csv_path: str | Path) -> CleaningResult:
    raw_dataframe = read_emg_csv(csv_path)
    return clean_emg_dataframe(raw_dataframe)
