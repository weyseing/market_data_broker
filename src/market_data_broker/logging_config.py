import logging
import sys
from typing import IO

import structlog


def configure_logging(level: str = "INFO", *, stream: IO[str] | None = None) -> None:
    """Configure JSON logging.

    ``stream`` defaults to stdout. The MCP stdio transport owns stdout for
    JSON-RPC framing, so the MCP entrypoint passes ``sys.stderr`` to keep the
    protocol channel pristine.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    out = stream if stream is not None else sys.stdout

    logging.basicConfig(
        format="%(message)s",
        stream=out,
        level=log_level,
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=out),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
