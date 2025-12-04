"""Platform-specific utilities for cross-platform compatibility.

This module provides utilities for platform detection and handling
OS-specific differences, particularly for signal handling on Windows
vs Unix-like systems.
"""
import signal
import sys


def is_windows() -> bool:
    """Check if the current platform is Windows.

    Returns:
        bool: True if running on Windows, False otherwise.

    Examples:
        >>> import sys
        >>> old_platform = sys.platform
        >>> sys.platform = 'win32'
        >>> is_windows()
        True
        >>> sys.platform = 'linux'
        >>> is_windows()
        False
        >>> sys.platform = old_platform  # restore
    """
    return sys.platform == 'win32'


def get_available_signals() -> dict[str, int]:
    """Get platform-appropriate signals for graceful shutdown.

    On Unix-like systems (Linux, macOS), both SIGINT and SIGTERM are available.
    On Windows, only SIGINT is available (Ctrl+C). SIGTERM does not exist on Windows.

    Returns:
        dict: Dictionary mapping signal names to signal numbers.
              On Windows: {'SIGINT': signal.SIGINT}
              On Unix: {'SIGINT': signal.SIGINT, 'SIGTERM': signal.SIGTERM}

    Examples:
        >>> signals = get_available_signals()
        >>> 'SIGINT' in signals
        True
        >>> # On Windows: 'SIGTERM' not in signals
        >>> # On Unix: 'SIGTERM' in signals
    """
    signals: dict[str, int] = {'SIGINT': signal.SIGINT}

    if not is_windows():
        # SIGTERM only exists on Unix-like systems
        signals['SIGTERM'] = signal.SIGTERM

    return signals
