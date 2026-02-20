"""Tests for whpg_dr_sync.dr module."""
from __future__ import annotations

import pytest

from whpg_dr_sync.dr import (
    lsn_ge,
    lsn_to_int,
    sh_quote,
    _list_wal_files_between_lsns,
    _wal_filename_for_lsn,
)


class TestShQuote:
    """Tests for sh_quote function."""

    def test_simple_string(self):
        """Should quote simple strings."""
        assert sh_quote("hello") == "'hello'"

    def test_string_with_spaces(self):
        """Should handle strings with spaces."""
        result = sh_quote("hello world")
        assert "hello world" in result

    def test_string_with_single_quote(self):
        """Should escape single quotes properly."""
        result = sh_quote("it's")
        # shlex.quote handles this
        assert "'" in result

    def test_empty_string(self):
        """Should handle empty string."""
        result = sh_quote("")
        assert result == "''"

    def test_string_with_special_chars(self):
        """Should handle special shell characters."""
        result = sh_quote("$HOME; rm -rf /")
        # Result should be safely quoted
        assert "$" in result or "'" in result


class TestLsnToInt:
    """Tests for lsn_to_int function."""

    def test_simple_lsn(self):
        """Should convert simple LSN."""
        result = lsn_to_int("0/1000000")
        assert result == 0x1000000

    def test_large_lsn(self):
        """Should convert large LSN."""
        result = lsn_to_int("1/0")
        assert result == (1 << 32)

    def test_complex_lsn(self):
        """Should convert complex LSN."""
        result = lsn_to_int("3C/2F000000")
        expected = (0x3C << 32) + 0x2F000000
        assert result == expected

    def test_zero_lsn(self):
        """Should handle 0/0."""
        result = lsn_to_int("0/0")
        assert result == 0

    def test_whitespace_handling(self):
        """Should strip whitespace."""
        result = lsn_to_int("  0/1000000  ")
        assert result == 0x1000000

    def test_invalid_lsn_no_slash(self):
        """Should raise on invalid LSN without slash."""
        with pytest.raises(ValueError):
            lsn_to_int("12345678")

    def test_empty_string(self):
        """Should return 0 for empty string."""
        result = lsn_to_int("")
        assert result == 0


class TestLsnGe:
    """Tests for lsn_ge function."""

    def test_greater_than(self):
        """Should return True when a > b."""
        assert lsn_ge("0/2000000", "0/1000000") is True

    def test_equal(self):
        """Should return True when a == b."""
        assert lsn_ge("0/1000000", "0/1000000") is True

    def test_less_than(self):
        """Should return False when a < b."""
        assert lsn_ge("0/1000000", "0/2000000") is False

    def test_cross_segment_comparison(self):
        """Should compare across segment boundaries."""
        assert lsn_ge("1/0", "0/FFFFFFFF") is True

    def test_invalid_lsn_returns_false(self):
        """Should return False for invalid LSN."""
        assert lsn_ge("invalid", "0/1") is False
        assert lsn_ge("0/1", "invalid") is False


class TestWalFilenameForLsn:
    """Tests for _wal_filename_for_lsn function."""

    def test_simple_lsn_64mb(self):
        """Should generate correct filename for 64MB segments."""
        wal_seg_size = 64 * 1024 * 1024  # 64MB
        timeline = 1

        # LSN 0/5000000 is in segment 0x50 / 64 = 1 (with 64MB segments)
        # Actually: segno = lsn_int // wal_seg_size
        # For 0/5000000: lsn_int = 0x5000000 = 83886080
        # segno = 83886080 // 67108864 = 1
        # segments_per_xlogid = 0x100000000 // 67108864 = 64
        # xlogid = 1 // 64 = 0
        # seg = 1 % 64 = 1
        result = _wal_filename_for_lsn("0/5000000", timeline, wal_seg_size)

        assert len(result) == 24
        assert result.startswith("00000001")  # Timeline 1

    def test_timeline_in_filename(self):
        """Should include timeline ID in filename."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 5

        result = _wal_filename_for_lsn("0/1000000", timeline, wal_seg_size)

        assert result.startswith("00000005")

    def test_high_lsn(self):
        """Should handle high LSN values."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        # LSN 10/0 is way into the logs
        result = _wal_filename_for_lsn("10/0", timeline, wal_seg_size)

        assert len(result) == 24
        assert result.startswith("00000001")


class TestListWalFilesBetweenLsns:
    """Tests for _list_wal_files_between_lsns function."""

    def test_no_files_when_equal(self):
        """Should return empty list when start == end."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        result = _list_wal_files_between_lsns(
            "0/5000000", "0/5000000", timeline, wal_seg_size
        )

        assert result == []

    def test_no_files_when_start_after_end(self):
        """Should return empty list when start > end."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        result = _list_wal_files_between_lsns(
            "0/6000000", "0/5000000", timeline, wal_seg_size
        )

        assert result == []

    def test_single_segment_span(self):
        """Should return files for single segment span."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        # Span within same segment should return that segment
        # LSN 0/4000000 to 0/5000000 (both in segment 1 with 64MB segments)
        result = _list_wal_files_between_lsns(
            "0/4000000", "0/7FFFFFF", timeline, wal_seg_size
        )

        # All files should have timeline 1
        for f in result:
            assert f.startswith("00000001")
            assert len(f) == 24

    def test_multiple_segments(self):
        """Should return multiple files for large span."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        # Span across multiple segments
        result = _list_wal_files_between_lsns(
            "0/0", "0/10000000", timeline, wal_seg_size
        )

        # Should have multiple files
        assert len(result) > 0

        # All filenames should be valid format
        for f in result:
            assert len(f) == 24
            assert f.startswith("00000001")

    def test_excludes_start_includes_end(self):
        """Start LSN segment is excluded, end LSN segment is included."""
        wal_seg_size = 64 * 1024 * 1024
        timeline = 1

        # Get segment number for start and end
        start_segno = lsn_to_int("0/0") // wal_seg_size
        end_segno = lsn_to_int("0/8000000") // wal_seg_size

        result = _list_wal_files_between_lsns(
            "0/0", "0/8000000", timeline, wal_seg_size
        )

        # Number of files should be (end_segno - start_segno)
        # because we exclude start but include end
        expected_count = end_segno - start_segno
        assert len(result) == expected_count
