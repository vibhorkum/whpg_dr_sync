from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .common import ShutdownRequested, atomic_write_json, check_stop, run, utc_now_iso
from .config import Config


# =============================
# Shell helpers
# =============================
def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def gpssh_bash(host: str, script: str, check: bool = True) -> str:
    cmd = f"bash --noprofile --norc -lc {sh_quote(script)}"
    return run(["gpssh", "-h", host, "-e", cmd], check=check)


def rewrite_conf_kv(conf_path: str, key: str, value_line: str) -> str:
    k = sh_quote(key)
    v = sh_quote(value_line)
    c = sh_quote(conf_path)
    tmp = sh_quote(conf_path + ".tmp")
    return (
        f"set -euo pipefail; "
        f"conf={c}; tmp={tmp}; "
        f"awk -v k={k} '!($0 ~ \"^\" k \"[[:space:]]*=\") {{print}}' \"$conf\" > \"$tmp\"; "
        f"printf '%s\\n' {v} >> \"$tmp\"; "
        f"mv -f \"$tmp\" \"$conf\""
    )


# =============================
# SQL helpers (utility mode)
# =============================
class PsqlConnError(RuntimeError):
    pass


def psql_util(host: str, port: int, user: str, db: str, sql: str) -> str:
    env = os.environ.copy()
    env["PGOPTIONS"] = "-c gp_session_role=utility"
    p = subprocess.run(
        ["psql", "-qtA", "-h", host, "-p", str(port), "-U", user, "-d", db, "-c", sql],
        text=True,
        capture_output=True,
        env=env,
    )
    if p.returncode != 0:
        stderr = (p.stderr or "").strip()
        if ("could not connect to server" in stderr) or ("Connection refused" in stderr) or ("timeout" in stderr):
            raise PsqlConnError(stderr)
        raise RuntimeError(
            "Command failed: psql -qtA -h {} -p {} -U {} -d {} -c {}\nSTDOUT:\n{}\nSTDERR:\n{}".format(
                host, port, user, db, sql, (p.stdout or "").strip(), stderr
            )
        )
    return (p.stdout or "").strip()


def try_sql(host: str, port: int, user: str, db: str, sql: str) -> Tuple[bool, Optional[str], Optional[str]]:
    try:
        return True, psql_util(host, port, user, db, sql).strip(), None
    except PsqlConnError as e:
        return False, None, str(e)


# =============================
# LSN compare
# =============================
def lsn_to_int(lsn: str) -> int:
    s = (lsn or "").strip()
    if not s or s == "0/0":
        return 0
    if "/" not in s:
        raise ValueError(f"Invalid pg_lsn: {lsn}")
    x, y = s.split("/", 1)
    return (int(x, 16) << 32) + int(y, 16)


def lsn_ge(a: str, b: str) -> bool:
    try:
        return lsn_to_int(a) >= lsn_to_int(b)
    except Exception:
        return False


# =============================
# Instance model
# =============================
@dataclass(frozen=True)
class DrInstance:
    gp_segment_id: int
    host: str
    port: int
    data_dir: str
    is_local: bool


def load_instances(cfg: Config) -> Dict[int, DrInstance]:
    m: Dict[int, DrInstance] = {}
    for it in cfg.instances:
        inst = DrInstance(
            gp_segment_id=int(it.gp_segment_id),
            host=str(it.host).strip(),
            port=int(it.port),
            data_dir=str(it.data_dir).strip(),
            is_local=bool(it.is_local),
        )
        m[inst.gp_segment_id] = inst
    return m


# =============================
# Config edits (NO sed)
# =============================
def ensure_standby_signal(inst: DrInstance) -> None:
    check_stop()
    sig = f"{inst.data_dir}/standby.signal"
    cmd = f"test -f {sh_quote(sig)} || touch {sh_quote(sig)}"
    if inst.is_local:
        run(["bash", "-lc", cmd], check=True)
    else:
        gpssh_bash(inst.host, cmd, check=True)


def set_recovery_target_action_shutdown(inst: DrInstance) -> None:
    check_stop()
    conf = f"{inst.data_dir}/postgresql.conf"
    script = rewrite_conf_kv(conf, "recovery_target_action", "recovery_target_action = 'shutdown'")
    if inst.is_local:
        run(["bash", "-lc", script], check=True)
    else:
        gpssh_bash(inst.host, script, check=True)


def set_recovery_target_lsn(inst: DrInstance, target_lsn: str) -> None:
    check_stop()
    conf = f"{inst.data_dir}/postgresql.conf"
    lsn = (target_lsn or "").strip().replace("\r", "")
    script = rewrite_conf_kv(conf, "recovery_target_lsn", f"recovery_target_lsn = '{lsn}'")
    if inst.is_local:
        run(["bash", "-lc", script], check=True)
    else:
        gpssh_bash(inst.host, script, check=True)


# =============================
# pg_ctl
# =============================
def _pg_ctl_stop(inst: DrInstance, gp_home: str) -> None:
    check_stop()
    if inst.gp_segment_id == -1:
        cmd = (
            f"source {gp_home}/greenplum_path.sh && "
            f"export COORDINATOR_DATA_DIRECTORY={inst.data_dir} && "
            f"pg_ctl -D {inst.data_dir} stop -m fast"
        )
        run(["bash", "-lc", cmd], check=False)
        return
    cmd = f"source {gp_home}/greenplum_path.sh && pg_ctl -D {inst.data_dir} stop -m fast"
    gpssh_bash(inst.host, cmd, check=False)


def _pg_ctl_start(inst: DrInstance, gp_home: str) -> None:
    check_stop()
    if inst.gp_segment_id == -1:
        cmd = (
            f"source {gp_home}/greenplum_path.sh && "
            f"export COORDINATOR_DATA_DIRECTORY={inst.data_dir} && "
            f"pg_ctl -D {inst.data_dir} -o \"-c gp_role=utility\" start"
        )
        run(["bash", "-lc", cmd], check=False)
        return
    cmd = (
        f"source {gp_home}/greenplum_path.sh && "
        f"pg_ctl -D {inst.data_dir} -o \"-c gp_role=utility -c port={inst.port}\" start -l start.log"
    )
    gpssh_bash(inst.host, cmd, check=False)


# =============================
# Manifest selection (keep it simple + stable)
# =============================
def _manifest_ready(m: dict) -> bool:
    return bool(m.get("ready", False)) and bool(m.get("restore_point")) and bool(m.get("segments"))


def _load_target_manifest(cfg: Config, target: str) -> Optional[dict]:
    manifest_dir = Path(cfg.manifest_dir)
    latest_path = Path(cfg.latest_path)

    if target != "LATEST":
        p = manifest_dir / f"{target}.json"
        if not p.exists():
            print(f"[DR] ERROR: manifest not found: {p}")
            return None
        m = json.loads(p.read_text())
        return m if _manifest_ready(m) else None

    if not latest_path.exists():
        print("[DR] No LATEST manifest exists.")
        return None

    m = json.loads(latest_path.read_text())
    if not _manifest_ready(m):
        print("[DR] LATEST manifest not ready/valid yet.")
        return None
    return m


# =============================
# State
# =============================
def _get_current_restore_point(cfg: Config) -> str:
    state_file = Path(cfg.state_dir) / "current_restore_point.txt"
    return state_file.read_text().strip() if state_file.exists() else ""


def _set_current_restore_point(cfg: Config, rp: str) -> None:
    state_file = Path(cfg.state_dir) / "current_restore_point.txt"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(rp + "\n")


# =============================
# Cycle (your proven “all DOWN = success” logic)
# =============================
def _cycle(cfg: Config, target: str = "LATEST") -> int:
    check_stop()

    user = cfg.primary_user
    db = cfg.primary_db

    state_dir = Path(cfg.state_dir)
    receipts_dir = Path(cfg.receipts_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)

    target_manifest = _load_target_manifest(cfg, target)
    if not target_manifest:
        return 0

    target_rp = str(target_manifest.get("restore_point") or "").strip()
    if not target_rp:
        print("[DR] ERROR: target manifest missing restore_point")
        return 0

    current_rp = _get_current_restore_point(cfg)
    if not current_rp:
        print("[DR] ERROR: current_restore_point.txt missing/empty.")
        return 0

    if current_rp == target_rp:
        print(f"[DR] Already at {target_rp}")
        return 0

    print(f"[DR] current={current_rp} -> target={target_rp}")

    instances = load_instances(cfg)
    target_lsns = {int(s["gp_segment_id"]): str(s["restore_lsn"]).strip() for s in target_manifest["segments"]}

    print("[DR] Applying recovery_target_lsn (per instance) and recovery_target_action='shutdown' + start...")
    for seg_id, inst in instances.items():
        check_stop()
        tgt_lsn = target_lsns.get(seg_id)
        if not tgt_lsn:
            raise RuntimeError(f"[DR][seg={seg_id}] target manifest missing restore_lsn")

        print(f"[DR][seg={seg_id}] apply target_lsn={tgt_lsn} and start")
        ensure_standby_signal(inst)
        set_recovery_target_action_shutdown(inst)
        set_recovery_target_lsn(inst, tgt_lsn)
        _pg_ctl_stop(inst, cfg.gp_home)
        time.sleep(1)
        _pg_ctl_start(inst, cfg.gp_home)

    print(
        f"[DR] Waiting for shutdown-at-target confirmation "
        f"(max_wait_secs={cfg.consumer_wait_reach_secs} poll_secs={cfg.consumer_reach_poll_secs})..."
    )

    waited = 0
    while waited <= cfg.consumer_wait_reach_secs:
        check_stop()
        all_down = True

        for _, inst in instances.items():
            ok, _, _ = try_sql(inst.host, inst.port, user, db, "SELECT 1;")
            if ok:
                all_down = False

        if all_down:
            print("[DR] ✅ All instances appear DOWN (expected after shutdown target). Advancing state.")
            _set_current_restore_point(cfg, target_rp)
            atomic_write_json(
                receipts_dir / f"{target_rp}.receipt.json",
                {
                    "current_restore_point": current_rp,
                    "target_restore_point": target_rp,
                    "checked_at_utc": utc_now_iso(),
                    "mode": "shutdown",
                    "status": "reached_then_shutdown_best_effort",
                    "waited_secs": waited,
                    "target_lsns": {str(k): v for k, v in target_lsns.items()},
                },
            )
            return 0

        time.sleep(cfg.consumer_reach_poll_secs)
        waited += cfg.consumer_reach_poll_secs

    print("[DR] Timeout. Will retry next cycle.")
    atomic_write_json(
        receipts_dir / f"{target_rp}.receipt.json",
        {
            "current_restore_point": current_rp,
            "target_restore_point": target_rp,
            "checked_at_utc": utc_now_iso(),
            "mode": "shutdown",
            "status": "timeout",
            "waited_secs": waited,
            "target_lsns": {str(k): v for k, v in target_lsns.items()},
        },
    )
    return 0


# =============================
# Public entrypoints (used by CLI)
# =============================
def run_once(cfg: Config, target: str = "LATEST") -> int:
    try:
        return _cycle(cfg, target=target)
    except ShutdownRequested as e:
        print(f"[stop] {e.reason}")
        return e.code
    except KeyboardInterrupt:
        print("[stop] keyboard_interrupt")
        return 0
    except Exception as e:
        print(f"[DR] ERROR: {e}", file=sys.stderr)
        return 2


def run_daemon(cfg: Config, target: str = "LATEST") -> int:
    try:
        while True:
            try:
                _cycle(cfg, target=target)
            except ShutdownRequested:
                raise
            except Exception as e:
                print(f"[DR] ERROR: {e}", file=sys.stderr)

            # sleep in small chunks so stop is responsive
            slept = 0
            while slept < cfg.consumer_sleep_secs:
                check_stop()
                time.sleep(1)
                slept += 1

    except ShutdownRequested as e:
        print(f"[stop] {e.reason}")
        return e.code
    except KeyboardInterrupt:
        print("[stop] keyboard_interrupt")
        return 0
