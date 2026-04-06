'''ultra.utils.logging -- Structured logging setup.

Configures stdlib logging with a consistent format for all
ultra modules. Call ``setup_logging()`` once at application
startup.
'''
from __future__ import annotations

import logging
import logging.handlers
import sys


LOG_FORMAT = (
    '%(asctime)s %(levelname)-8s '
    '[%(name)s] %(message)s'
)
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(
        level: int = logging.INFO,
        log_file: str | None = None,
) -> None:
    '''Configure root logger for the ultra application.

    Args:
        level: Logging level (default INFO).
        log_file: Optional path to a rotating log file.
    '''
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
    ]

    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=handlers,
        force=True,
    )

    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
