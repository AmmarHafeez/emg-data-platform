"""Command-line loader for raw EMG signals into PostgreSQL."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.ingest.clean_emg_data import REQUIRED_COLUMNS, EmgDataValidationError, load_and_clean_emg_csv
from src.ingest.db import get_engine, load_database_config
from src.utils.logging import configure_logging, get_logger

LOGGER = get_logger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "sample_emg.csv"
TARGET_SCHEMA = "raw"
TARGET_TABLE = "emg_signals"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw EMG signals from a CSV file into PostgreSQL.")
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="Path to the raw EMG CSV file.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of rows per insert batch.",
    )
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Truncate the target table before loading to keep local demo runs idempotent.",
    )
    return parser.parse_args()


def verify_target_table(engine: Engine) -> None:
    inspector = inspect(engine)
    if not inspector.has_table(TARGET_TABLE, schema=TARGET_SCHEMA):
        raise RuntimeError(
            f"Target table {TARGET_SCHEMA}.{TARGET_TABLE} does not exist. "
            "Run the database initialization first."
        )


def truncate_emg_signals_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE}"))


def insert_emg_signals(
    engine: Engine,
    chunk_size: int,
    csv_path: Path,
    *,
    truncate_first: bool = False,
) -> int:
    cleaning_result = load_and_clean_emg_csv(csv_path)
    rows_to_insert = cleaning_result.dataframe.loc[:, REQUIRED_COLUMNS].copy()

    verify_target_table(engine)
    if truncate_first:
        LOGGER.info("Truncating %s.%s before loading the latest sample file", TARGET_SCHEMA, TARGET_TABLE)
        truncate_emg_signals_table(engine)

    LOGGER.info(
        "Loading %s rows into %s.%s",
        len(rows_to_insert),
        TARGET_SCHEMA,
        TARGET_TABLE,
    )
    if cleaning_result.rejected_rows:
        LOGGER.info("Skipped %s invalid rows during cleaning", cleaning_result.rejected_rows)

    with engine.begin() as connection:
        rows_to_insert.to_sql(
            name=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            con=connection,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=chunk_size,
        )

    LOGGER.info("Inserted %s rows into %s.%s", len(rows_to_insert), TARGET_SCHEMA, TARGET_TABLE)
    return len(rows_to_insert)


def main() -> int:
    configure_logging()
    engine = None

    try:
        args = parse_args()
        if args.chunk_size <= 0:
            raise ValueError("--chunk-size must be a positive integer")

        LOGGER.info("Starting raw EMG load from %s", args.csv_path)

        config = load_database_config()
        LOGGER.info(
            "Connecting to PostgreSQL database '%s' at %s:%s",
            config.database,
            config.host,
            config.port,
        )
        engine = get_engine(config)
        rows_loaded = insert_emg_signals(
            engine=engine,
            chunk_size=args.chunk_size,
            csv_path=args.csv_path,
            truncate_first=args.truncate_first,
        )
    except (EmgDataValidationError, FileNotFoundError, SQLAlchemyError, RuntimeError, ValueError):
        LOGGER.exception("EMG CSV load failed")
        return 1
    finally:
        if engine is not None:
            engine.dispose()

    LOGGER.info("Raw EMG load completed successfully with %s inserted rows", rows_loaded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
