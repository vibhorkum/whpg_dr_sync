from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Instance:
    gp_segment_id: int
    host: str
    port: int
    data_dir: str
    is_local: bool


@dataclass(frozen=True)
class Config:
    raw: Dict[str, Any]
    config_path: str

    # primary connection
    primary_host: str
    primary_port: int
    primary_user: str
    primary_db: str

    # storage
    manifest_dir: str
    latest_path: str
    manifest_fetch_command: str  # Optional custom command to fetch manifest files remotely
    manifest_list_command: str  # Optional custom command to list manifest files remotely

    # archive (publisher uses this)
    archive_dir: str

    # dr
    gp_home: str
    state_dir: str
    receipts_dir: str
    instances: List[Instance]

    # behavior
    publisher_sleep_secs: int
    archive_wait_max_secs: int
    archive_poll_interval_secs: int

    consumer_sleep_secs: int
    consumer_reach_poll_secs: int
    consumer_wait_reach_secs: int

    # wal
    wal_segment_size_mb: int
    wal_enumerate_hard_limit: int
    wal_check_command: str  # Optional custom command to check WAL file existence (global fallback)
    wal_check_commands: Dict[int, str]  # Per-segment/coordinator custom commands (segment_id -> command)


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"Configuration validation failed: {'; '.join(errors)}")


def validate_config(raw: Dict[str, Any]) -> List[str]:
    """
    Validate configuration structure and return list of errors.

    Args:
        raw: Parsed JSON configuration dictionary

    Returns:
        List of validation error messages (empty if valid)
    """
    errors: List[str] = []

    # Check required top-level sections
    required_sections = ["primary", "storage", "archive", "dr"]
    for section in required_sections:
        if section not in raw:
            errors.append(f"Missing required section: '{section}'")

    # Validate primary connection settings
    if "primary" in raw:
        primary = raw["primary"]
        for field in ["host", "port", "user", "db"]:
            if field not in primary:
                errors.append(f"Missing primary.{field}")
            elif field == "port":
                try:
                    port = int(primary[field])
                    if port < 1 or port > 65535:
                        errors.append(f"primary.port must be 1-65535, got {port}")
                except (ValueError, TypeError):
                    errors.append(f"primary.port must be an integer")

    # Validate storage settings
    if "storage" in raw:
        storage = raw["storage"]
        for field in ["manifest_dir", "latest_path"]:
            if field not in storage:
                errors.append(f"Missing storage.{field}")

    # Validate archive settings
    if "archive" in raw:
        if "archive_dir" not in raw["archive"]:
            errors.append("Missing archive.archive_dir")

    # Validate DR settings
    if "dr" in raw:
        dr = raw["dr"]
        for field in ["gp_home", "state_dir", "receipts_dir", "instances"]:
            if field not in dr:
                errors.append(f"Missing dr.{field}")

        # Validate instances
        if "instances" in dr:
            instances = dr["instances"]
            if not isinstance(instances, list):
                errors.append("dr.instances must be a list")
            elif len(instances) == 0:
                errors.append("dr.instances cannot be empty")
            else:
                seen_seg_ids: set = set()
                for i, inst in enumerate(instances):
                    inst_prefix = f"dr.instances[{i}]"
                    for field in ["gp_segment_id", "host", "port", "data_dir"]:
                        if field not in inst:
                            errors.append(f"Missing {inst_prefix}.{field}")

                    if "gp_segment_id" in inst:
                        seg_id = inst["gp_segment_id"]
                        if seg_id in seen_seg_ids:
                            errors.append(f"Duplicate gp_segment_id: {seg_id}")
                        seen_seg_ids.add(seg_id)

                    if "port" in inst:
                        try:
                            port = int(inst["port"])
                            if port < 1 or port > 65535:
                                errors.append(f"{inst_prefix}.port must be 1-65535")
                        except (ValueError, TypeError):
                            errors.append(f"{inst_prefix}.port must be an integer")

    return errors


def load_config(path: str, validate: bool = True) -> Config:
    """
    Load and validate configuration from JSON file.

    Args:
        path: Path to configuration file
        validate: If True, validate config and raise on errors

    Returns:
        Config object

    Raises:
        ConfigValidationError: If validation fails and validate=True
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is not valid JSON
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in configuration file {path}: {e.msg}",
            e.doc,
            e.pos,
        )

    if validate:
        errors = validate_config(raw)
        if errors:
            for err in errors:
                logger.error("Config validation: %s", err)
            raise ConfigValidationError(errors)

    beh = raw.get("behavior", {})

    def geti(k: str, default: int) -> int:
        return int(beh.get(k, default))

    instances: List[Instance] = []
    for it in raw["dr"]["instances"]:
        instances.append(
            Instance(
                gp_segment_id=int(it["gp_segment_id"]),
                host=str(it["host"]).strip(),
                port=int(it["port"]),
                data_dir=str(it["data_dir"]).strip(),
                is_local=bool(it.get("is_local", False)),
            )
        )

    # Parse wal_check_commands (per-segment configuration)
    wal_check_commands_raw = beh.get("wal_check_commands", {})
    wal_check_commands: Dict[int, str] = {}
    if isinstance(wal_check_commands_raw, dict):
        for seg_id_str, cmd in wal_check_commands_raw.items():
            try:
                seg_id = int(seg_id_str)
                wal_check_commands[seg_id] = str(cmd)
            except (ValueError, TypeError):
                # Skip invalid entries
                pass

    return Config(
        raw=raw,
        config_path=str(p),

        primary_host=raw["primary"]["host"],
        primary_port=int(raw["primary"]["port"]),
        primary_user=raw["primary"]["user"],
        primary_db=raw["primary"]["db"],

        manifest_dir=raw["storage"]["manifest_dir"],
        latest_path=raw["storage"]["latest_path"],
        manifest_fetch_command=raw["storage"].get("manifest_fetch_command", ""),
        manifest_list_command=raw["storage"].get("manifest_list_command", ""),

        archive_dir=raw["archive"]["archive_dir"],

        gp_home=raw["dr"]["gp_home"],
        state_dir=raw["dr"]["state_dir"],
        receipts_dir=raw["dr"]["receipts_dir"],
        instances=instances,

        publisher_sleep_secs=geti("publisher_sleep_secs", 10),
        archive_wait_max_secs=geti("archive_wait_max_secs", 30),
        archive_poll_interval_secs=geti("archive_poll_interval_secs", 2),

        consumer_sleep_secs=geti("consumer_sleep_secs", 30),
        consumer_reach_poll_secs=geti("consumer_reach_poll_secs", 5),
        consumer_wait_reach_secs=geti("consumer_wait_reach_secs", 300),

        wal_segment_size_mb=geti("wal_segment_size_mb", 64),
        wal_enumerate_hard_limit=geti("wal_enumerate_hard_limit", 250000),
        wal_check_command=beh.get("wal_check_command", ""),
        wal_check_commands=wal_check_commands,
    )
