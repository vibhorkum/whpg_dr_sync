"""Shared test fixtures for whpg_dr_sync tests."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def valid_config_dict() -> Dict[str, Any]:
    """Return a valid configuration dictionary."""
    return {
        "primary": {
            "host": "primary.example.com",
            "port": 5432,
            "user": "gpadmin",
            "db": "postgres",
        },
        "storage": {
            "manifest_dir": "/var/lib/whpg_dr_sync/manifests",
            "latest_path": "/var/lib/whpg_dr_sync/LATEST.json",
        },
        "archive": {
            "archive_dir": "/data/archive",
        },
        "dr": {
            "gp_home": "/usr/local/greenplum-db",
            "state_dir": "/var/lib/whpg_dr_sync/state",
            "receipts_dir": "/var/lib/whpg_dr_sync/receipts",
            "instances": [
                {
                    "gp_segment_id": -1,
                    "host": "dr-coordinator.example.com",
                    "port": 5432,
                    "data_dir": "/data/coordinator",
                    "is_local": True,
                },
                {
                    "gp_segment_id": 0,
                    "host": "dr-segment0.example.com",
                    "port": 6000,
                    "data_dir": "/data/segment0",
                    "is_local": False,
                },
                {
                    "gp_segment_id": 1,
                    "host": "dr-segment1.example.com",
                    "port": 6001,
                    "data_dir": "/data/segment1",
                    "is_local": False,
                },
            ],
        },
        "behavior": {
            "publisher_sleep_secs": 10,
            "archive_wait_max_secs": 30,
            "archive_poll_interval_secs": 2,
            "consumer_sleep_secs": 30,
            "consumer_reach_poll_secs": 5,
            "consumer_wait_reach_secs": 300,
            "wal_segment_size_mb": 64,
        },
    }


@pytest.fixture
def valid_config_file(temp_dir, valid_config_dict) -> Path:
    """Create a valid configuration file."""
    config_path = temp_dir / "config.json"
    config_path.write_text(json.dumps(valid_config_dict, indent=2))
    return config_path


@pytest.fixture
def sample_manifest() -> Dict[str, Any]:
    """Return a sample manifest dictionary."""
    return {
        "restore_point": "sync_point_20260220_120000",
        "created_at_utc": "2026-02-20T12:00:00Z",
        "ready": True,
        "segments": [
            {"gp_segment_id": -1, "restore_lsn": "0/5000000"},
            {"gp_segment_id": 0, "restore_lsn": "0/5000100"},
            {"gp_segment_id": 1, "restore_lsn": "0/5000200"},
        ],
        "evidence": {
            "targets": [],
            "archiver_check": {},
            "switch_wal": [],
            "archive_wait": {
                "max_wait_secs": 30,
                "poll_interval_secs": 2,
                "waited_secs": 5,
                "ready_at_utc": "2026-02-20T12:00:05Z",
            },
        },
    }
