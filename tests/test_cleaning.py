from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingest.clean_emg_data import EmgDataValidationError, clean_emg_dataframe


def build_emg_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "subject_id": "SUBJ_01",
        "session_id": "SUBJ_01_SES_01",
        "timestamp": "2026-01-12 08:00:00.000",
        "channel_1": 0.1123,
        "channel_2": 0.1187,
        "channel_3": 0.1215,
        "channel_4": 0.1281,
        "channel_5": 0.1314,
        "channel_6": 0.1359,
        "channel_7": 0.1402,
        "channel_8": 0.1446,
        "gesture_label": "grip",
    }
    row.update(overrides)
    return row


def test_clean_emg_dataframe_raises_for_missing_required_columns() -> None:
    dataframe = pd.DataFrame([build_emg_row()])
    dataframe = dataframe.drop(columns=["channel_8"])

    with pytest.raises(EmgDataValidationError, match="missing required columns"):
        clean_emg_dataframe(dataframe)


def test_clean_emg_dataframe_drops_rows_with_invalid_timestamps() -> None:
    dataframe = pd.DataFrame(
        [
            build_emg_row(subject_id="SUBJ_INVALID_TS", timestamp="not-a-timestamp"),
            build_emg_row(subject_id="SUBJ_VALID_TS", timestamp="2026-01-12 08:00:01.500"),
        ]
    )

    result = clean_emg_dataframe(dataframe)

    assert result.rejected_rows == 1
    assert len(result.dataframe) == 1
    assert result.dataframe.iloc[0]["subject_id"] == "SUBJ_VALID_TS"
    assert pd.api.types.is_datetime64_any_dtype(result.dataframe["timestamp"])


def test_clean_emg_dataframe_trims_whitespace_from_text_columns() -> None:
    dataframe = pd.DataFrame(
        [
            build_emg_row(
                subject_id="  SUBJ_02  ",
                session_id="  SUBJ_02_SES_01  ",
                gesture_label="  grip  ",
            )
        ]
    )

    result = clean_emg_dataframe(dataframe)
    row = result.dataframe.iloc[0]

    assert row["subject_id"] == "SUBJ_02"
    assert row["session_id"] == "SUBJ_02_SES_01"
    assert row["gesture_label"] == "grip"


def test_clean_emg_dataframe_lowercases_gesture_labels() -> None:
    dataframe = pd.DataFrame([build_emg_row(gesture_label="OpEn_HaNd")])

    result = clean_emg_dataframe(dataframe)

    assert result.dataframe.iloc[0]["gesture_label"] == "open_hand"


def test_clean_emg_dataframe_drops_rows_with_null_required_fields() -> None:
    dataframe = pd.DataFrame(
        [
            build_emg_row(subject_id="SUBJ_VALID"),
            build_emg_row(subject_id="   "),
            build_emg_row(subject_id="SUBJ_NULL_SIGNAL", channel_4=None),
        ]
    )

    result = clean_emg_dataframe(dataframe)

    assert result.rejected_rows == 2
    assert len(result.dataframe) == 1
    assert result.dataframe.iloc[0]["subject_id"] == "SUBJ_VALID"
