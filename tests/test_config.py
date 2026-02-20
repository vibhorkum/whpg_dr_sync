"""Tests for whpg_dr_sync.config module."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from whpg_dr_sync.config import (
    Config,
    ConfigValidationError,
    load_config,
    validate_config,
)


class TestValidateConfig:
    """Tests for validate_config function."""

    def test_valid_config_returns_no_errors(self, valid_config_dict):
        """Should return empty list for valid config."""
        errors = validate_config(valid_config_dict)
        assert errors == []

    def test_missing_primary_section(self, valid_config_dict):
        """Should report missing primary section."""
        del valid_config_dict["primary"]
        errors = validate_config(valid_config_dict)
        assert any("primary" in e for e in errors)

    def test_missing_storage_section(self, valid_config_dict):
        """Should report missing storage section."""
        del valid_config_dict["storage"]
        errors = validate_config(valid_config_dict)
        assert any("storage" in e for e in errors)

    def test_missing_archive_section(self, valid_config_dict):
        """Should report missing archive section."""
        del valid_config_dict["archive"]
        errors = validate_config(valid_config_dict)
        assert any("archive" in e for e in errors)

    def test_missing_dr_section(self, valid_config_dict):
        """Should report missing dr section."""
        del valid_config_dict["dr"]
        errors = validate_config(valid_config_dict)
        assert any("dr" in e for e in errors)

    def test_missing_primary_host(self, valid_config_dict):
        """Should report missing primary.host."""
        del valid_config_dict["primary"]["host"]
        errors = validate_config(valid_config_dict)
        assert any("primary.host" in e for e in errors)

    def test_missing_primary_port(self, valid_config_dict):
        """Should report missing primary.port."""
        del valid_config_dict["primary"]["port"]
        errors = validate_config(valid_config_dict)
        assert any("primary.port" in e for e in errors)

    def test_invalid_primary_port_type(self, valid_config_dict):
        """Should report invalid primary.port type."""
        valid_config_dict["primary"]["port"] = "not-a-number"
        errors = validate_config(valid_config_dict)
        assert any("port" in e and "integer" in e for e in errors)

    def test_invalid_primary_port_range(self, valid_config_dict):
        """Should report port out of valid range."""
        valid_config_dict["primary"]["port"] = 70000
        errors = validate_config(valid_config_dict)
        assert any("1-65535" in e for e in errors)

    def test_missing_storage_manifest_dir(self, valid_config_dict):
        """Should report missing storage.manifest_dir."""
        del valid_config_dict["storage"]["manifest_dir"]
        errors = validate_config(valid_config_dict)
        assert any("manifest_dir" in e for e in errors)

    def test_missing_archive_dir(self, valid_config_dict):
        """Should report missing archive.archive_dir."""
        del valid_config_dict["archive"]["archive_dir"]
        errors = validate_config(valid_config_dict)
        assert any("archive_dir" in e for e in errors)

    def test_missing_dr_gp_home(self, valid_config_dict):
        """Should report missing dr.gp_home."""
        del valid_config_dict["dr"]["gp_home"]
        errors = validate_config(valid_config_dict)
        assert any("gp_home" in e for e in errors)

    def test_empty_instances_list(self, valid_config_dict):
        """Should report empty instances list."""
        valid_config_dict["dr"]["instances"] = []
        errors = validate_config(valid_config_dict)
        assert any("empty" in e for e in errors)

    def test_instance_missing_segment_id(self, valid_config_dict):
        """Should report missing gp_segment_id in instance."""
        del valid_config_dict["dr"]["instances"][0]["gp_segment_id"]
        errors = validate_config(valid_config_dict)
        assert any("gp_segment_id" in e for e in errors)

    def test_instance_missing_host(self, valid_config_dict):
        """Should report missing host in instance."""
        del valid_config_dict["dr"]["instances"][0]["host"]
        errors = validate_config(valid_config_dict)
        assert any("instances[0].host" in e for e in errors)

    def test_duplicate_segment_ids(self, valid_config_dict):
        """Should report duplicate gp_segment_id."""
        valid_config_dict["dr"]["instances"][1]["gp_segment_id"] = -1
        errors = validate_config(valid_config_dict)
        assert any("Duplicate" in e and "-1" in e for e in errors)

    def test_instance_invalid_port(self, valid_config_dict):
        """Should report invalid port in instance."""
        valid_config_dict["dr"]["instances"][0]["port"] = 99999
        errors = validate_config(valid_config_dict)
        assert any("instances[0].port" in e and "1-65535" in e for e in errors)


class TestLoadConfig:
    """Tests for load_config function."""

    def test_loads_valid_config(self, valid_config_file):
        """Should load valid configuration file."""
        cfg = load_config(str(valid_config_file))

        assert isinstance(cfg, Config)
        assert cfg.primary_host == "primary.example.com"
        assert cfg.primary_port == 5432
        assert cfg.primary_user == "gpadmin"
        assert cfg.primary_db == "postgres"
        assert len(cfg.instances) == 3

    def test_raises_on_missing_file(self, temp_dir):
        """Should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_config(str(temp_dir / "nonexistent.json"))
        assert "not found" in str(exc_info.value)

    def test_raises_on_invalid_json(self, temp_dir):
        """Should raise on invalid JSON."""
        bad_file = temp_dir / "bad.json"
        bad_file.write_text("not valid json {")

        with pytest.raises(json.JSONDecodeError):
            load_config(str(bad_file))

    def test_raises_on_validation_error(self, temp_dir):
        """Should raise ConfigValidationError for invalid config."""
        invalid_config = {"incomplete": True}
        config_file = temp_dir / "invalid.json"
        config_file.write_text(json.dumps(invalid_config))

        with pytest.raises(ConfigValidationError) as exc_info:
            load_config(str(config_file))

        assert len(exc_info.value.errors) > 0

    def test_skips_validation_when_disabled(self, temp_dir):
        """Should skip validation when validate=False."""
        # This config is missing required fields
        minimal_config = {
            "primary": {"host": "h", "port": 5432, "user": "u", "db": "d"},
            "storage": {"manifest_dir": "/m", "latest_path": "/l"},
            "archive": {"archive_dir": "/a"},
            "dr": {
                "gp_home": "/g",
                "state_dir": "/s",
                "receipts_dir": "/r",
                "instances": [
                    {"gp_segment_id": -1, "host": "h", "port": 5432, "data_dir": "/d"}
                ],
            },
        }
        config_file = temp_dir / "minimal.json"
        config_file.write_text(json.dumps(minimal_config))

        # Should not raise
        cfg = load_config(str(config_file), validate=True)
        assert cfg.primary_host == "h"

    def test_parses_behavior_defaults(self, temp_dir):
        """Should use default behavior values when not specified."""
        minimal_config = {
            "primary": {"host": "h", "port": 5432, "user": "u", "db": "d"},
            "storage": {"manifest_dir": "/m", "latest_path": "/l"},
            "archive": {"archive_dir": "/a"},
            "dr": {
                "gp_home": "/g",
                "state_dir": "/s",
                "receipts_dir": "/r",
                "instances": [
                    {"gp_segment_id": -1, "host": "h", "port": 5432, "data_dir": "/d"}
                ],
            },
        }
        config_file = temp_dir / "minimal.json"
        config_file.write_text(json.dumps(minimal_config))

        cfg = load_config(str(config_file))

        # Check defaults
        assert cfg.publisher_sleep_secs == 10
        assert cfg.archive_wait_max_secs == 30
        assert cfg.consumer_sleep_secs == 30
        assert cfg.wal_segment_size_mb == 64

    def test_parses_instances_correctly(self, valid_config_file):
        """Should correctly parse instance configurations."""
        cfg = load_config(str(valid_config_file))

        # Check coordinator (gp_segment_id = -1)
        coord = next(i for i in cfg.instances if i.gp_segment_id == -1)
        assert coord.host == "dr-coordinator.example.com"
        assert coord.port == 5432
        assert coord.is_local is True

        # Check segment
        seg0 = next(i for i in cfg.instances if i.gp_segment_id == 0)
        assert seg0.host == "dr-segment0.example.com"
        assert seg0.port == 6000
        assert seg0.is_local is False

    def test_parses_wal_check_commands(self, temp_dir, valid_config_dict):
        """Should parse per-segment WAL check commands."""
        valid_config_dict["behavior"]["wal_check_command"] = "test -f {wal_path}"
        valid_config_dict["behavior"]["wal_check_commands"] = {
            "-1": "ssh coord test -f {wal_path}",
            "0": "ssh seg0 test -f {wal_path}",
        }

        config_file = temp_dir / "config.json"
        config_file.write_text(json.dumps(valid_config_dict))

        cfg = load_config(str(config_file))

        assert cfg.wal_check_command == "test -f {wal_path}"
        assert cfg.wal_check_commands[-1] == "ssh coord test -f {wal_path}"
        assert cfg.wal_check_commands[0] == "ssh seg0 test -f {wal_path}"
