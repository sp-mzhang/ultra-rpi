'''ultra.utils.logging -- Structured logging setup.

Configures stdlib logging with a consistent format for all
ultra modules. Call ``setup_logging()`` once at application
startup.
'''
from __future__ import annotations

import logging
import logging.handlers
import sys
from collections import deque
from typing import Callable


LOG_FORMAT = (
    '%(asctime)s %(levelname)-8s '
    '[%(name)s] %(message)s'
)
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

_BUFFER_SIZE = 500


class BufferedLogHandler(logging.Handler):
    '''Ring-buffer handler that keeps recent log lines.

    Optionally calls a callback on each new record so
    the GUI can broadcast log lines over WebSocket.
    '''

    def __init__(
            self,
            maxlen: int = _BUFFER_SIZE,
    ) -> None:
        super().__init__()
        self.buffer: deque[str] = deque(maxlen=maxlen)
        self._callback: Callable[[str], None] | None = (
            None
        )

    def set_callback(
            self, cb: Callable[[str], None],
    ) -> None:
        '''Register a callback for each new log line.'''
        self._callback = cb

    def emit(self, record: logging.LogRecord) -> None:
        line = self.format(record)
        self.buffer.append(line)
        if self._callback:
            try:
                self._callback(line)
            except Exception:
                pass

    def get_lines(self) -> list[str]:
        '''Return all buffered log lines.'''
        return list(self.buffer)


_log_handler: BufferedLogHandler | None = None


def get_log_handler() -> BufferedLogHandler | None:
    '''Return the shared BufferedLogHandler instance.'''
    return _log_handler


def setup_logging(
        level: int = logging.INFO,
        log_file: str | None = None,
) -> None:
    '''Configure root logger for the ultra application.

    Args:
        level: Logging level (default INFO).
        log_file: Optional path to a rotating log file.
    '''
    global _log_handler
    _log_handler = BufferedLogHandler(
        maxlen=_BUFFER_SIZE,
    )

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        _log_handler,
    ]

    if log_file:
        file_handler = (
            logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
            )
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=handlers,
        force=True,
    )

    logging.getLogger('uvicorn').setLevel(
        logging.WARNING,
    )
    logging.getLogger('uvicorn.access').setLevel(
        logging.WARNING,
    )
