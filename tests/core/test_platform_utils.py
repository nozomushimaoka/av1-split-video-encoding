"""Tests for platform utilities."""
import signal
from unittest.mock import patch

import pytest

from av1_encoder.core.platform_utils import get_available_signals, is_windows


class TestPlatformDetection:
    """Test platform detection functions."""

    def test_is_windows_on_windows(self):
        """Test is_windows() returns True on Windows."""
        with patch('sys.platform', 'win32'):
            assert is_windows() is True

    def test_is_windows_on_linux(self):
        """Test is_windows() returns False on Linux."""
        with patch('sys.platform', 'linux'):
            assert is_windows() is False

    def test_is_windows_on_darwin(self):
        """Test is_windows() returns False on macOS."""
        with patch('sys.platform', 'darwin'):
            assert is_windows() is False

    def test_is_windows_on_cygwin(self):
        """Test is_windows() returns False on Cygwin (not native Windows)."""
        with patch('sys.platform', 'cygwin'):
            assert is_windows() is False


class TestSignalDetection:
    """Test signal availability detection."""

    def test_get_available_signals_on_windows(self):
        """Test get_available_signals() on Windows includes only SIGINT."""
        with patch('av1_encoder.core.platform_utils.is_windows', return_value=True):
            signals = get_available_signals()
            assert 'SIGINT' in signals
            assert 'SIGTERM' not in signals
            assert len(signals) == 1
            assert signals['SIGINT'] == signal.SIGINT

    def test_get_available_signals_on_unix(self):
        """Test get_available_signals() on Unix includes SIGINT and SIGTERM."""
        with patch('av1_encoder.core.platform_utils.is_windows', return_value=False):
            signals = get_available_signals()
            assert 'SIGINT' in signals
            assert 'SIGTERM' in signals
            assert len(signals) == 2
            assert signals['SIGINT'] == signal.SIGINT
            assert signals['SIGTERM'] == signal.SIGTERM

    def test_get_available_signals_returns_dict(self):
        """Test get_available_signals() returns a dictionary."""
        signals = get_available_signals()
        assert isinstance(signals, dict)

    def test_get_available_signals_values_are_integers(self):
        """Test all signal values in the returned dict are integers."""
        signals = get_available_signals()
        for name, sig_num in signals.items():
            assert isinstance(sig_num, int)
            assert hasattr(signal, 'signal')  # Verify signal module exists

    def test_get_available_signals_sigint_always_present(self):
        """Test SIGINT is always available regardless of platform."""
        with patch('av1_encoder.core.platform_utils.is_windows', return_value=True):
            signals_win = get_available_signals()
            assert 'SIGINT' in signals_win

        with patch('av1_encoder.core.platform_utils.is_windows', return_value=False):
            signals_unix = get_available_signals()
            assert 'SIGINT' in signals_unix
