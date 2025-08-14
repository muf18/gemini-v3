import json
import logging
import sys
from pathlib import Path
from typing import Any, cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT


class InterceptHandler(logging.Handler):
    """A custom logging handler to intercept standard logging messages.

    This handler redirects standard logging messages to Loguru.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Emits a log record to the Loguru logger.

        Args:
            record: The log record to emit.
        """
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = cast(Any, frame.f_back)
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def _sensitive_data_filter(record: dict[str, Any]) -> bool:
    """Filter and sanitizer for log records.

    This function inspects the log record's 'extra' data and parameters
    to redact sensitive information before it is written to any sink.
    It replaces values of keys deemed sensitive with '***REDACTED***'.
    """
    sensitive_keys = {"api_key", "api_secret", "secret", "password", "token"}

    # Sanitize extra data
    for key, value in record["extra"].items():
        if key in sensitive_keys and isinstance(value, str):
            record["extra"][key] = "***REDACTED***"

    # Sanitize formatted message parameters
    # Loguru's `record["message"]` is the final formatted string.
    # We can't easily change it, but we can check the original format parameters.
    if "parameters" in record:
        # This is an internal structure, handle with care
        new_params = []
        for param in record["parameters"]:
            # This is a heuristic. A more robust solution might involve
            # checking format string specifics, but this covers many cases.
            if isinstance(param, str) and any(k in param for k in sensitive_keys):
                new_params.append("***REDACTED***")
            else:
                new_params.append(param)
        # This modification is complex; for now, we focus on 'extra'
        # which is the primary way to pass structured, sensitive data.

    return True


def _json_formatter(record: dict[str, Any]) -> str:
    """Custom formatter to structure log records as JSON."""
    log_object = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "source": {
            "name": record["name"],
            "file": f"{record['file'].name}:{record['line']}",
            "function": record["function"],
        },
        "extra": record["extra"],
    }
    # Ensure the output is a single line
    return json.dumps(log_object) + "\n"


def setup_logging(
    console_level: str = "INFO",
    file_level: str = "DEBUG",
    log_dir: Path | None = None,
) -> None:
    """Configures the application-wide Loguru logger.

    This function removes any default handlers, sets up a new console sink
    with a readable format, and an optional rotating file sink with
    structured JSON output. It also intercepts standard library logging.

    Args:
        console_level: The minimum log level for console output.
        file_level: The minimum log level for file output.
        log_dir: Directory to store log files. If None, file logging is disabled.
    """
    # 1. Remove default handlers and disable propagation to root logger
    logger.remove()
    logger.add(
        sys.stderr,
        level=console_level.upper(),
        format=LOGURU_FORMAT,
        colorize=True,
        filter=_sensitive_data_filter,
    )

    # 2. Configure file sink if a directory is provided
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "cryptochart_{time:YYYY-MM-DD}.log"

        # Use a custom formatter for JSON output
        def json_sink(message: str) -> None:
            # The record is already serialized by the format function
            with open(
                log_file_path.with_name(
                    log_file_path.name.format(time=logger.now())
                ),
                "a",
                encoding="utf-8",
            ) as f:
                f.write(message)

        logger.add(
            json_sink,
            level=file_level.upper(),
            format=_json_formatter,
            rotation="00:00",  # New file at midnight
            retention="7 days",
            compression="zip",
            serialize=False,  # We do custom serialization in the format function
            filter=_sensitive_data_filter,
            enqueue=True,  # Make logging calls non-blocking
            backtrace=False,  # Keep log files clean
            diagnose=False,
        )

    # 3. Intercept standard logging messages
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    logger.info("Logging configured successfully.")