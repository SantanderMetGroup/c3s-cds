import logging
import os
from pathlib import Path


DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | pid=%(process)d | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _resolve_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level

    raw_level = (level or os.getenv("C3S_LOG_LEVEL", DEFAULT_LOG_LEVEL)).upper()
    resolved = logging.getLevelName(raw_level)
    if isinstance(resolved, int):
        return resolved
    return logging.INFO


def setup_logging(
    level: str | int | None = None,
    log_file: str | Path | None = None,
    force: bool = False,
) -> None:
    """
    Configure process-wide logging once.

    By default this logs to stderr and supports optional file logging through the
    ``log_file`` argument or ``C3S_LOG_FILE`` environment variable.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers and not force:
        return

    resolved_level = _resolve_level(level)
    target_file = Path(log_file or os.getenv("C3S_LOG_FILE", "")).expanduser() if (log_file or os.getenv("C3S_LOG_FILE")) else None

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if target_file:
        target_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(target_file))

    logging.basicConfig(
        level=resolved_level,
        format=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT,
        handlers=handlers,
        force=force,
    )
    logging.captureWarnings(True)
