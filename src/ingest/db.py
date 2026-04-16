"""Database connection helpers for the EMG ingestion workflow."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOTENV_PATH = PROJECT_ROOT / ".env"


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> URL:
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


def load_environment(dotenv_path: Path | None = None) -> None:
    env_path = dotenv_path or DEFAULT_DOTENV_PATH
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        return

    load_dotenv(override=False)


def load_database_config() -> DatabaseConfig:
    load_environment()

    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "localhost").strip(),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "emg_platform").strip(),
        user=os.getenv("POSTGRES_USER", "emg_admin").strip(),
        password=os.getenv("POSTGRES_PASSWORD", "emg_admin"),
    )


def get_engine(
    config: DatabaseConfig | None = None,
    *,
    echo: bool | None = None,
) -> Engine:
    resolved_config = config or load_database_config()
    echo_enabled = (
        echo
        if echo is not None
        else os.getenv("SQLALCHEMY_ECHO", "false").strip().lower() == "true"
    )

    return create_engine(
        resolved_config.sqlalchemy_url,
        future=True,
        pool_pre_ping=True,
        echo=echo_enabled,
    )
