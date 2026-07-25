from __future__ import annotations

import logging
import sys
from typing import Dict, Optional


class StructuredLogger:
    def __init__(self, name: str, level: int = logging.INFO) -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _enrich(
        self, msg: str, extra: Optional[Dict[str, object]] = None
    ) -> str:
        if extra:
            extras = " ".join(f"{k}={v}" for k, v in extra.items())
            return f"{msg} | {extras}"
        return msg

    def info(
        self, msg: str, extra: Optional[Dict[str, object]] = None
    ) -> None:
        self._logger.info(self._enrich(msg, extra))

    def warning(
        self, msg: str, extra: Optional[Dict[str, object]] = None
    ) -> None:
        self._logger.warning(self._enrich(msg, extra))

    def error(
        self, msg: str, extra: Optional[Dict[str, object]] = None
    ) -> None:
        self._logger.error(self._enrich(msg, extra))

    def debug(
        self, msg: str, extra: Optional[Dict[str, object]] = None
    ) -> None:
        self._logger.debug(self._enrich(msg, extra))


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)
