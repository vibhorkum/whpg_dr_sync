from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .common import atomic_write_json, psql, psql_util, ssh_test_file, utc_now_iso
from .config import Config


@dataclass(frozen=True)
class PrimaryConn:
    host: str
    port: int
    user: str
    db: str


def gp_switch_wal(primary: PrimaryConn) -> List[Dict[str, Any]]:
    sql = """
    SELECT gp_segment_id, pg_switch_wal, pg_walfile_name
      FROM gp_switch_wal()
     ORDER BY gp_segment_id;
    """
    out = psql(primary.host, primary.port, primary.user, primary.db, sql)
    rows: List[Dict[str, Any]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        seg_id_s, lsn, wal = line.split("|")
        rows.append(
            {
                "gp_segment_id": int(seg_id_s),
                "switch_lsn": lsn.strip(),
                "switch_wal_file": wal.strip(),
            }
        )
    return rows


def create_restore_point_with_hosts(primary: PrimaryConn, restore_name: str) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT rp.gp_segment_id,
           rp.restore_lsn,
           sc.dbid,
           sc.hostname,
           sc.port
      FROM gp_create_restore_point('{restore_name}') rp
      JOIN gp_segment_configuration sc
        ON rp.gp_segment_id = sc.content
     WHERE sc.role = 'p'
     ORDER BY rp.gp_segment_id, sc.dbid;
    """
    out = psql(primary.host, primary.port, primary.user, primary.db, sql)
    rows: List[Dict[str, Any]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        seg_id_s, restore_lsn, dbid_s, hostname, port_s = line.split("|")
        rows.append(
            {
                "gp_segment_id": int(seg_id_s),
                "restore_lsn": restore_lsn.strip(),
                "dbid": int(dbid_s),
                "primary_host": hostname.strip(),
                "primary_port": int(port_s),
            }
        )
    if not rows:
        raise RuntimeError("gp_create_restore_point join gp_segment_configuration returned no rows")
    return rows


def wal_file_for_lsn_on_instance(
    primary: PrimaryConn, seg_id: int, seg_host: str, seg_port: int, lsn: str
) -> str:
    sql = f"SELECT pg_walfile_name('{lsn}');"
    if seg_id == -1:
        return psql(primary.host, primary.port, primary.user, primary.db, sql).strip()
    return psql_util(seg_host, seg_port, primary.user, primary.db, sql).strip()


def archiver_stats_cluster(primary: PrimaryConn) -> Dict[str, Any]:
    sql = r"""
    SELECT COALESCE(
      json_agg(
        json_build_object(
          'role', role,
          'segment_id', content,
          'archived_count', archived_count,
          'last_archived_wal', last_archived_wal,
          'last_archived_time', last_archived_time,
          'failed_count', failed_count,
          'last_failed_wal', last_failed_wal,
          'last_failed_time', last_failed_time
        )
        ORDER BY content
      )::text,
      '[]'
    )
    FROM (
      SELECT
        'coordinator' AS role,
        -1            AS content,
        a.archived_count,
        a.last_archived_wal,
        a.last_archived_time,
        a.failed_count,
        a.last_failed_wal,
        a.last_failed_time
      FROM pg_stat_archiver AS a

      UNION ALL

      SELECT
        'segment'           AS role,
        s.gp_segment_id     AS content,
        sa.archived_count,
        sa.last_archived_wal,
        sa.last_archived_time,
        sa.failed_count,
        sa.last_failed_wal,
        sa.last_failed_time
      FROM gp_dist_random('pg_stat_archiver') AS sa
      JOIN gp_dist_random('gp_id') AS s ON true
    ) foo;
    """
    raw = psql(primary.host, primary.port, primary.user, primary.db, sql).strip()
    rows = json.loads(raw) if raw else []
    any_failed_time = any(bool(r.get("last_failed_time")) for r in rows)
    return {
        "method": "cluster_pg_stat_archiver",
        "no_recent_failures": (not any_failed_time),
        "rows": rows,
    }


def publish_one(cfg: Config, once_no_gp_switch_wal: bool = False) -> None:
    primary = PrimaryConn(cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db)

    manifest_dir = Path(cfg.manifest_dir)
    latest_path = Path(cfg.latest_path)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)

    ts = utc_now_iso()
    restore_name = "sync_point_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"---- {ts} ----")
    print(f"[PRIMARY] restore_point={restore_name}")

    print("[PRIMARY] gp_create_restore_point() + primary host mapping...")
    rp_rows = create_restore_point_with_hosts(primary, restore_name)

    targets: List[Dict[str, Any]] = []
    for r in rp_rows:
        seg_id = int(r["gp_segment_id"])
        lsn = r["restore_lsn"]
        src_host = r["primary_host"]
        src_port = int(r["primary_port"])

        wal_file = wal_file_for_lsn_on_instance(primary, seg_id, src_host, src_port, lsn)
        targets.append(
            {
                "gp_segment_id": seg_id,
                "restore_lsn": lsn,
                "archive_source_host": src_host,
                "archive_dir": cfg.archive_dir,
                "wal_file": wal_file,
                "wal_present": False,
                "primary_port": src_port,
            }
        )

    switch_rows: List[Dict[str, Any]] = []
    if not once_no_gp_switch_wal:
        print("[PRIMARY] gp_switch_wal() on coordinator + segments (after restore point)...")
        switch_rows = gp_switch_wal(primary)

    archiver = archiver_stats_cluster(primary)

    manifest = {
        "restore_point": restore_name,
        "created_at_utc": ts,
        "ready": False,
        "segments": [{"gp_segment_id": t["gp_segment_id"], "restore_lsn": t["restore_lsn"]} for t in targets],
        "evidence": {
            "targets": targets,
            "archiver_check": archiver,
            "switch_wal": switch_rows,
            "archive_wait": {
                "max_wait_secs": cfg.archive_wait_max_secs,
                "poll_interval_secs": cfg.archive_poll_interval_secs,
                "waited_secs": 0,
                "ready_at_utc": "",
            },
        },
    }

    out_path = manifest_dir / f"{restore_name}.json"
    atomic_write_json(out_path, manifest)
    atomic_write_json(latest_path, manifest)
    print(f"[PRIMARY] Published pending manifest {out_path} (ready=False)")

    print("[PRIMARY] Waiting for per-segment WAL files to appear in remote archive sources...")
    waited = 0
    ready = False

    while waited <= cfg.archive_wait_max_secs:
        all_present = True
        for t in targets:
            remote_path = f"{t['archive_dir']}/{t['wal_file']}"
            present = ssh_test_file(t["archive_source_host"], remote_path)
            t["wal_present"] = present
            if not present:
                all_present = False

        if all_present:
            ready = True
            break

        time.sleep(cfg.archive_poll_interval_secs)
        waited += cfg.archive_poll_interval_secs

    manifest["ready"] = ready
    manifest["evidence"]["archive_wait"]["waited_secs"] = waited
    if ready:
        manifest["evidence"]["archive_wait"]["ready_at_utc"] = utc_now_iso()

    atomic_write_json(out_path, manifest)
    atomic_write_json(latest_path, manifest)

    for t in targets:
        print(
            f"  seg={t['gp_segment_id']} lsn={t['restore_lsn']} "
            f"src={t['archive_source_host']}:{t.get('primary_port','?')} "
            f"wal={t['wal_file']} present={t['wal_present']}"
        )
    print(f"[PRIMARY] Updated {out_path} (ready={ready}) waited_secs={waited}")


def run_daemon(cfg: Config, once_no_gp_switch_wal: bool = False) -> int:
    try:
        while True:
            try:
                publish_one(cfg, once_no_gp_switch_wal=once_no_gp_switch_wal)
            except Exception as e:
                print(f"[PRIMARY] ERROR: {e}", file=sys.stderr)
            time.sleep(cfg.publisher_sleep_secs)
    except KeyboardInterrupt:
        print("\n[PRIMARY] stop requested (Ctrl+C). Exiting cleanly.")
        return 130
