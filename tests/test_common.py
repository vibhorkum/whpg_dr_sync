"""Tests for whpg_dr_sync.common module."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from whpg_dr_sync.common import (
    atomic_write_json,
    utc_now_iso,
    with_retry,
)


class TestUtcNowIso:
    """Tests for utc_now_iso function."""

    def test_returns_iso_format(self):
        """Should return timestamp in ISO 8601 format."""
        result = utc_now_iso()
        # Format: YYYY-MM-DDTHH:MM:SSZ
        assert len(result) == 20
        assert result[4] == "-"
        assert result[7] == "-"
        assert result[10] == "T"
        assert result[13] == ":"
        assert result[16] == ":"
        assert result.endswith("Z")

    def test_returns_current_time(self):
        """Should return approximately current UTC time."""
        before = time.time()
        result = utc_now_iso()
        after = time.time()

        # Parse year from result
        year = int(result[:4])
        assert 2024 <= year <= 2030  # Reasonable range


class TestAtomicWriteJson:
    """Tests for atomic_write_json function."""

    def test_creates_file(self, temp_dir):
        """Should create JSON file with correct content."""
        path = temp_dir / "test.json"
        data = {"key": "value", "number": 42}

        atomic_write_json(path, data)

        assert path.exists()
        content = json.loads(path.read_text())
        assert content == data

    def test_creates_parent_directories(self, temp_dir):
        """Should create parent directories if they don't exist."""
        path = temp_dir / "nested" / "dirs" / "test.json"
        data = {"nested": True}

        atomic_write_json(path, data)

        assert path.exists()
        content = json.loads(path.read_text())
        assert content == data

    def test_overwrites_existing_file(self, temp_dir):
        """Should overwrite existing file atomically."""
        path = temp_dir / "test.json"
        path.write_text('{"old": "data"}')

        new_data = {"new": "data"}
        atomic_write_json(path, new_data)

        content = json.loads(path.read_text())
        assert content == new_data

    def test_no_temp_file_remains_on_success(self, temp_dir):
        """Should not leave temporary file after success."""
        path = temp_dir / "test.json"
        atomic_write_json(path, {"test": True})

        tmp_path = Path(str(path) + ".tmp")
        assert not tmp_path.exists()

    def test_formats_with_indent(self, temp_dir):
        """Should format JSON with indentation."""
        path = temp_dir / "test.json"
        atomic_write_json(path, {"key": "value"})

        content = path.read_text()
        assert "  " in content  # Has indentation


class TestWithRetry:
    """Tests for with_retry function."""

    def test_returns_on_first_success(self):
        """Should return immediately on successful call."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = with_retry(func, max_retries=3)

        assert result == "success"
        assert call_count == 1

    def test_retries_on_failure(self):
        """Should retry on failure until success."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "success"

        result = with_retry(func, max_retries=3, backoff_base=0.01)

        assert result == "success"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        """Should raise last exception after max retries exhausted."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            raise ValueError(f"attempt {call_count}")

        with pytest.raises(ValueError) as exc_info:
            with_retry(func, max_retries=3, backoff_base=0.01)

        assert "attempt 3" in str(exc_info.value)
        assert call_count == 3

    def test_only_catches_specified_exceptions(self):
        """Should only retry on specified exception types."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            raise TypeError("wrong type")

        with pytest.raises(TypeError):
            with_retry(
                func,
                max_retries=3,
                backoff_base=0.01,
                exceptions=(ValueError,),
            )

        assert call_count == 1  # No retry for TypeError

    def test_calls_on_retry_callback(self):
        """Should call on_retry callback before each retry."""
        retry_calls = []

        def on_retry(exc, attempt):
            retry_calls.append((str(exc), attempt))

        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"fail {call_count}")
            return "ok"

        with_retry(
            func,
            max_retries=3,
            backoff_base=0.01,
            on_retry=on_retry,
        )

        assert len(retry_calls) == 2
        assert retry_calls[0] == ("fail 1", 0)
        assert retry_calls[1] == ("fail 2", 1)

    def test_exponential_backoff(self):
        """Should use exponential backoff between retries."""
        start_times = []

        def func():
            start_times.append(time.time())
            if len(start_times) < 3:
                raise ValueError("retry")
            return "ok"

        with_retry(func, max_retries=3, backoff_base=0.1)

        # Check delays increase exponentially
        delay1 = start_times[1] - start_times[0]
        delay2 = start_times[2] - start_times[1]

        assert delay1 >= 0.09  # ~0.1s (backoff_base * 2^0)
        assert delay2 >= 0.18  # ~0.2s (backoff_base * 2^1)
        assert delay2 > delay1
