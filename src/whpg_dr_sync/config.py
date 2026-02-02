from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


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


def load_config(path: str) -> Config:
    p = Path(path)
    raw = json.loads(p.read_text())
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

    return Config(
        raw=raw,
        config_path=str(p),

        primary_host=raw["primary"]["host"],
        primary_port=int(raw["primary"]["port"]),
        primary_user=raw["primary"]["user"],
        primary_db=raw["primary"]["db"],

        manifest_dir=raw["storage"]["manifest_dir"],
        latest_path=raw["storage"]["latest_path"],

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
    )
