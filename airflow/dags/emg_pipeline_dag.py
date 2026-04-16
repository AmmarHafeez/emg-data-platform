"""Airflow DAG for the local EMG ingestion and transformation pipeline."""

from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)

DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(os.getenv("EMG_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
DOTENV_PATH = PROJECT_ROOT / ".env"
INGEST_SCRIPT_PATH = Path(
    os.getenv("EMG_INGEST_SCRIPT", str(PROJECT_ROOT / "src" / "ingest" / "load_raw_csv.py"))
)
RAW_CSV_PATH = Path(
    os.getenv("EMG_RAW_CSV_PATH", str(PROJECT_ROOT / "data" / "raw" / "sample_emg.csv"))
)
DBT_PROJECT_DIR = Path(os.getenv("EMG_DBT_PROJECT_DIR", str(PROJECT_ROOT / "dbt_emg")))
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR)))
DEFAULT_POSTGRES_HOST = os.getenv("AIRFLOW_POSTGRES_HOST", "postgres")

TASK_ENV = {
    "EMG_PROJECT_ROOT": str(PROJECT_ROOT),
    "EMG_INGEST_SCRIPT": str(INGEST_SCRIPT_PATH),
    "EMG_RAW_CSV_PATH": str(RAW_CSV_PATH),
    "EMG_DBT_PROJECT_DIR": str(DBT_PROJECT_DIR),
    "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR),
    "AIRFLOW_POSTGRES_HOST": DEFAULT_POSTGRES_HOST,
}

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def load_local_env_file() -> None:
    if not DOTENV_PATH.exists():
        LOGGER.info("No .env file found at %s; relying on container environment variables", DOTENV_PATH)
        return

    for raw_line in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def wait_for_postgres(max_wait_seconds: int = 90, check_interval_seconds: int = 5) -> None:
    load_local_env_file()

    host = os.getenv("AIRFLOW_POSTGRES_HOST") or os.getenv("POSTGRES_HOST") or "postgres"
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    deadline = time.monotonic() + max_wait_seconds
    attempt = 0

    LOGGER.info("Waiting for PostgreSQL readiness at %s:%s", host, port)

    while time.monotonic() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=5):
                LOGGER.info(
                    "PostgreSQL is reachable on %s:%s after %s attempt(s)",
                    host,
                    port,
                    attempt,
                )
                return
        except OSError as exc:
            remaining_seconds = max(0, int(deadline - time.monotonic()))
            LOGGER.info(
                "PostgreSQL not ready yet on attempt %s: %s. Retrying in %s seconds (%s seconds remaining).",
                attempt,
                exc,
                check_interval_seconds,
                remaining_seconds,
            )
            time.sleep(check_interval_seconds)

    raise RuntimeError(f"PostgreSQL was not reachable on {host}:{port} within {max_wait_seconds} seconds")


def build_bash_command(command: str) -> str:
    # All pipeline commands run inside the mounted project workspace.
    return f"""
set -euo pipefail
cd "{PROJECT_ROOT.as_posix()}"

if [ -f "{DOTENV_PATH.as_posix()}" ]; then
    set -a
    . "{DOTENV_PATH.as_posix()}"
    set +a
fi

export POSTGRES_HOST="${{AIRFLOW_POSTGRES_HOST:-${{POSTGRES_HOST:-postgres}}}}"

echo "[emg_pipeline] Running command: {command}"
{command}
""".strip()


with DAG(
    dag_id="emg_data_platform_pipeline",
    description="Run EMG raw ingestion and dbt transformations for the local-first data platform.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["emg", "portfolio", "local-first"],
) as dag:
    check_postgres_readiness = PythonOperator(
        task_id="check_postgres_readiness",
        python_callable=wait_for_postgres,
    )

    ingest_raw_signals = BashOperator(
        task_id="ingest_raw_signals",
        bash_command=build_bash_command(
            f'python "{INGEST_SCRIPT_PATH.as_posix()}" --csv-path "{RAW_CSV_PATH.as_posix()}" --truncate-first'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    install_dbt_packages = BashOperator(
        task_id="install_dbt_packages",
        bash_command=build_bash_command(
            f'dbt deps --project-dir "{DBT_PROJECT_DIR.as_posix()}" --profiles-dir "{DBT_PROFILES_DIR.as_posix()}"'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    seed_reference_data = BashOperator(
        task_id="seed_reference_data",
        bash_command=build_bash_command(
            f'dbt seed --project-dir "{DBT_PROJECT_DIR.as_posix()}" --profiles-dir "{DBT_PROFILES_DIR.as_posix()}"'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    build_analytics_models = BashOperator(
        task_id="build_analytics_models",
        bash_command=build_bash_command(
            f'dbt run --project-dir "{DBT_PROJECT_DIR.as_posix()}" --profiles-dir "{DBT_PROFILES_DIR.as_posix()}"'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=build_bash_command(
            f'dbt test --project-dir "{DBT_PROJECT_DIR.as_posix()}" --profiles-dir "{DBT_PROFILES_DIR.as_posix()}"'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    check_postgres_readiness >> ingest_raw_signals >> install_dbt_packages >> seed_reference_data >> build_analytics_models >> run_dbt_tests
