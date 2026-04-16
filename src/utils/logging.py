"""Shared logging configuration for local scripts and workflows."""

from __future__ import annotations

import logging
import os

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: int | str | None = None) -> None:
    resolved_level = level if level is not None else os.getenv("LOG_LEVEL", "INFO")
    numeric_level = (
        getattr(logging, resolved_level.upper(), logging.INFO)
        if isinstance(resolved_level, str)
        else resolved_level
    )

    root_logger = logging.getLogger()
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    if root_logger.handlers:
        root_logger.setLevel(numeric_level)
        for handler in root_logger.handlers:
            handler.setFormatter(formatter)
        return

    logging.basicConfig(
        level=numeric_level,
        format=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
