"""Logging configuration module

Provides logger configuration used throughout the application.
Use the functions in this module instead of configuring logging individually in each module.
"""
import logging
import sys
from pathlib import Path
from typing import Optional


def setup_file_and_console_logger(
    name: str,
    log_file: Path,
    level: int = logging.INFO,
    log_format: str = '[%(asctime)s] %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S'
) -> logging.Logger:
    """Configure a logger that outputs to both file and console.

    Args:
        name: Logger name
        log_file: Path to the log file
        level: Log level (default: INFO)
        log_format: Log format
        date_format: Date format

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def setup_console_logger(
    name: str,
    level: int = logging.INFO,
    stream: Optional[object] = None,
    log_format: str = '[%(asctime)s] %(levelname)s: %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S',
    propagate: bool = False
) -> logging.Logger:
    """Configure a logger that outputs to console (stderr) only.

    Args:
        name: Logger name
        level: Log level (default: INFO)
        stream: Output stream (default: sys.stderr)
        log_format: Log format
        date_format: Date format
        propagate: Propagate to parent logger (default: False)

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)

    # Skip if handlers are already configured
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = propagate

    # Formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Console handler
    output_stream = stream if stream is not None else sys.stderr
    console_handler = logging.StreamHandler(output_stream)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def setup_segment_logger(
    segment_idx: int,
    log_file: Path,
    level: int = logging.DEBUG
) -> logging.Logger:
    """Configure a logger for segment encoding.

    Each segment outputs to an independent log file.
    Propagation to other loggers is disabled.

    Args:
        segment_idx: Segment index
        log_file: Path to the log file
        level: Log level (default: DEBUG)

    Returns:
        Configured segment logger
    """
    logger = logging.getLogger(f"av1_encoder.segment_{segment_idx}")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    # Add file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def cleanup_logger(logger: logging.Logger) -> None:
    """Clean up logger handlers.

    Args:
        logger: Logger to clean up
    """
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
