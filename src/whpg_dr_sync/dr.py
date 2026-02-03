#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .common import atomic_write_json, run, utc_now_iso
from .config import Config


# =============================
# Shell helpers
# =============================
def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def ssh_bash(host: str, script: str, check: bool = True) -> str:
    cmd = f"bash --noprofile --norc -lc {sh_quote(script)}"
    return run(["ssh", host, cmd], check=check)


def gpssh_bash(host: str, script: str, check: bool = True) -> str:
    # NOTE: no -q (some environments don’t support it)
    cmd = f"bash --noprofile --norc -lc {sh_quote(script)}"
    return run(["gpssh", "-h", host, "-e", cmd], check=check)


def rewrite_conf_kv(conf_path: str, key: str, value_line: str) -> str:
    """
    Atomic config rewrite (no sed portability issues):
      - remove any lines matching '^key[[:space:]]*='
      - append value_line
      - write to tmp then mv
    """
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
# Stop-signature detection
# =============================
_STOP_LSN_RE = re.compile(
    r'recovery stopping after WAL location \(LSN\)\s+""?([0-9A-Fa-f]+/[0-9A-Fa-f]+)""?'
)


def _extract_latest_stop_lsn(text: str) -> Optional[str]:
    """
    Given log text, return the last (most recent in the text stream) stop LSN found.
    """
    last: Optional[str] = None
    for m in _STOP_LSN_RE.finditer(text):
        last = m.group(1).strip()
    return last


def _tail_stop_signature(inst: DrInstance, target_lsn: str, scan_files: int = 6, tail_lines: int = 400) -> bool:
    """
    Look for evidence that recovery stopped at/after target LSN.

    IMPORTANT FIX:
      - we scan the last N newest csv files, not only the newest,
        because the “newest” file can be stale or not the file that captured the stop.
    """
    logdir = f"{inst.data_dir}/log"

    # Print a small filtered view from multiple files (newest first).
    # We echo file boundaries so the caller can debug quickly if needed.
    script = (
        f"set -euo pipefail; "
        f"ld={sh_quote(logdir)}; "
        f"files=$(ls -1t \"$ld\"/*.csv 2>/dev/null | head -n {int(scan_files)} || true); "
        f"if [ -z \"$files\" ]; then exit 1; fi; "
        f"for f in $files; do "
        f"  echo \"--- $f ---\"; "
        f"  tail -n {int(tail_lines)} \"$f\" | egrep -n "
        f"'recovery stopping after WAL location|database system is shut down|requested WAL|could not open file|restore_command|FATAL|PANIC|ERROR|archive' || true; "
        f"done"
    )

    out = (
        run(["bash", "-lc", script], check=False)
        if inst.is_local
        else ssh_bash(inst.host, script, check=False)
    )
    if not out:
        return False

    stopped_lsn = _extract_latest_stop_lsn(out)
    if stopped_lsn:
        return lsn_ge(stopped_lsn, target_lsn)

    # Fallback: if shutdown line exists alongside any recovery-stopping hint, accept.
    if ("database system is shut down" in out) and ("recovery stopping" in out):
        return True

    return False


_PGCD_MIN_REC_END_RE = re.compile(r"Minimum recovery ending location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)")

def _min_recovery_end_lsn_from_controldata(inst: DrInstance, gp_home: str) -> Optional[str]:
    """
    Read 'Minimum recovery ending location' from pg_controldata for a DOWN instance.
    This is the most reliable signal when logs are rotated/missing.
    """
    pgcd = f"{gp_home}/bin/pg_controldata"
    cmd = f"{pgcd} {sh_quote(inst.data_dir)}"

    # IMPORTANT: use gpssh for remote if you want; ssh is fine too.
    # Because you already use gpssh_bash elsewhere, stay consistent:
    out = run(["bash", "-lc", cmd], check=False) if inst.is_local else gpssh_bash(inst.host, cmd, check=False)
    if not out:
        return None

    m = _PGCD_MIN_REC_END_RE.search(out)
    return m.group(1).strip() if m else None


def _down_and_reached_target_via_controldata(inst: DrInstance, gp_home: str, target_lsn: str) -> Tuple[bool, str]:
    """
    Returns (ok, reason). ok=True if pg_controldata proves recovery progressed to >= target_lsn.
    """
    v = _min_recovery_end_lsn_from_controldata(inst, gp_home)
    if not v:
        return False, "pg_controldata missing/unreadable min_recovery_end_lsn"
    if lsn_ge(v, target_lsn):
        return True, f"min_recovery_end_lsn={v} >= target_lsn={target_lsn}"
    return False, f"min_recovery_end_lsn={v} < target_lsn={target_lsn}"


# =============================
# Config edits
# =============================
def ensure_standby_signal(inst: DrInstance) -> None:
    sig = f"{inst.data_dir}/standby.signal"
    cmd = f"test -f {sh_quote(sig)} || touch {sh_quote(sig)}"
    if inst.is_local:
        run(["bash", "-lc", cmd], check=True)
    else:
        gpssh_bash(inst.host, cmd, check=True)


def set_recovery_target_action_shutdown(inst: DrInstance) -> None:
    conf = f"{inst.data_dir}/postgresql.conf"
    script = rewrite_conf_kv(conf, "recovery_target_action", "recovery_target_action = 'shutdown'")
    if inst.is_local:
        run(["bash", "-lc", script], check=True)
    else:
        gpssh_bash(inst.host, script, check=True)


def set_recovery_target_lsn(inst: DrInstance, target_lsn: str) -> None:
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
# Recovery floor
# =============================
def get_floor_lsn_sql(inst: DrInstance, user: str, db: str) -> Optional[str]:
    ok, v, _ = try_sql(inst.host, inst.port, user, db, "SELECT min_recovery_end_lsn FROM pg_control_recovery();")
    return v.strip() if ok and v else None


def get_floor_lsn_controldata(inst: DrInstance, gp_home: str) -> Optional[str]:
    pgcd = f"{gp_home}/bin/pg_controldata"
    cmd = f"{pgcd} {sh_quote(inst.data_dir)}"
    out = run(["bash", "-lc", cmd], check=False) if inst.is_local else ssh_bash(inst.host, cmd, check=False)
    if not out:
        return None
    m = re.search(r"Minimum recovery ending location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)", out)
    return m.group(1).strip() if m else None


def get_recovery_floors(instances: Dict[int, DrInstance], gp_home: str, user: str, db: str) -> Dict[int, str]:
    floors: Dict[int, str] = {}
    for seg_id, inst in instances.items():
        v = get_floor_lsn_sql(inst, user, db) or get_floor_lsn_controldata(inst, gp_home)
        if v:
            floors[seg_id] = v
    return floors


# =============================
# Manifests
# =============================
def list_manifest_paths(manifest_dir: Path) -> List[Path]:
    return sorted(manifest_dir.glob("sync_point_*.json"))


def manifest_ready(m: dict) -> bool:
    return bool(m.get("ready", False)) and bool(m.get("restore_point")) and bool(m.get("segments"))


def manifest_lsn_map(m: dict) -> Dict[int, str]:
    return {int(s["gp_segment_id"]): str(s["restore_lsn"]).strip() for s in m.get("segments", [])}


def manifest_satisfies_floors(m: dict, floors: Dict[int, str]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    lsn_map = manifest_lsn_map(m)
    for seg_id, floor in floors.items():
        tgt = lsn_map.get(seg_id)
        if not tgt:
            reasons.append(f"[seg={seg_id}] manifest missing restore_lsn")
        elif not lsn_ge(tgt, floor):
            reasons.append(f"[seg={seg_id}] target_lsn={tgt} < floor_lsn={floor}")
    return (len(reasons) == 0, reasons)


def load_all_ready_manifests(manifest_dir: Path) -> List[dict]:
    out: List[dict] = []
    for p in list_manifest_paths(manifest_dir):
        try:
            m = json.loads(p.read_text())
            if manifest_ready(m):
                out.append(m)
        except Exception:
            continue
    return out


def pick_best_manifest(
    manifest_dir: Path, latest_path: Path, floors: Dict[int, str]
) -> Tuple[Optional[dict], str, List[str]]:
    diags: List[str] = []
    latest = None
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text())
        except Exception:
            latest = None

    if latest and manifest_ready(latest):
        ok, reasons = manifest_satisfies_floors(latest, floors)
        if ok:
            return latest, "LATEST satisfies recovery floors", diags
        diags.append("[DR] Target is behind recovery floor; auto-skipping forward:")
        for r in reasons:
            diags.append("  " + r)

    ready = load_all_ready_manifests(manifest_dir)
    if not ready:
        return None, "no ready manifests exist", diags

    for m in ready:
        ok, _ = manifest_satisfies_floors(m, floors)
        if ok:
            return m, "selected earliest READY manifest at/after floors", diags

    return None, "no ready manifest found at/after floors", diags


# =============================
# State
# =============================
def get_current_restore_point(state_file: Path) -> Optional[str]:
    if not state_file.exists():
        return None
    v = state_file.read_text().strip()
    return v or None


def set_current_restore_point(state_file: Path, rp: str) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(rp + "\n")


# =============================
# Cycle
# =============================
def _cycle(cfg: Config, target: str = "LATEST") -> int:
    user = cfg.primary_user
    db = cfg.primary_db

    manifest_dir = Path(cfg.manifest_dir)
    latest_path = Path(cfg.latest_path)

    state_dir = Path(cfg.state_dir)
    receipts_dir = Path(cfg.receipts_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)
    current_state_file = state_dir / "current_restore_point.txt"

    instances = load_instances(cfg)
    floors = get_recovery_floors(instances, cfg.gp_home, user, db)

    if floors:
        print("[DR] Recovery floors (min_recovery_end_lsn):")
        for seg_id in sorted(floors.keys()):
            print(f"  [seg={seg_id}] floor_lsn={floors[seg_id]}")
    else:
        print("[DR] WARNING: Could not compute recovery floors.")

    if target != "LATEST":
        p = manifest_dir / f"{target}.json"
        if not p.exists():
            print(f"[DR] ERROR: manifest not found: {p}")
            return 0
        target_manifest = json.loads(p.read_text())
    else:
        chosen, reason, diags = pick_best_manifest(manifest_dir, latest_path, floors)
        for line in diags:
            print(line)
        if not chosen:
            print("[DR] No suitable READY manifest at/after recovery floors.")
            return 0
        target_manifest = chosen
        print(f"[DR] Selected target restore_point={target_manifest['restore_point']} ({reason})")

    target_rp = str(target_manifest.get("restore_point") or "").strip()
    if not target_rp:
        print("[DR] ERROR: target manifest missing restore_point")
        return 0

    current_rp = get_current_restore_point(current_state_file)
    if not current_rp:
        print("[DR] ERROR: current_restore_point.txt missing/empty.")
        return 0

    if current_rp == target_rp:
        print(f"[DR] Already at {target_rp}")
        return 0

    print(f"[DR] current={current_rp} -> target={target_rp}")

    target_lsns = {int(s["gp_segment_id"]): str(s["restore_lsn"]).strip() for s in target_manifest["segments"]}

    print("[DR] Applying recovery_target_lsn (per instance) and recovery_target_action='shutdown' + start...")
    for seg_id, inst in instances.items():
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
        all_done = True

        for seg_id, inst in instances.items():
            tgt_lsn = target_lsns[seg_id]

            ok, inrec, err = try_sql(inst.host, inst.port, user, db, "SELECT pg_is_in_recovery();")
            if ok:
                ok2, replay, _ = try_sql(inst.host, inst.port, user, db, "SELECT pg_last_wal_replay_lsn();")
                replay_s = (replay or "").strip() if ok2 else "?"
                reached = lsn_ge(replay_s, tgt_lsn) if replay_s != "?" else False
                print(
                    f"[DR][seg={seg_id}] UP; replay_lsn={replay_s} target_lsn={tgt_lsn} "
                    f"reached={reached} in_recovery={inrec}"
                )
                all_done = False
                continue

            # DOWN/unreachable by SQL is expected final state. Prove stop via logs.

            # Instance is DOWN/unreachable by SQL (expected final state)
            ok_ctl, why = _down_and_reached_target_via_controldata(inst, cfg.gp_home, tgt_lsn)
            if ok_ctl:
                print(f"[DR][seg={seg_id}] DOWN; controldata confirms target reached. {why}")
            else:
                print(f"[DR][seg={seg_id}] DOWN; not yet confirmed via controldata. {why}. err={err or '-'}")
                all_done = False


        if all_done:
            print(f"[DR] ✅ All instances confirmed stopped at/after target. Advancing state to {target_rp}.")
            set_current_restore_point(current_state_file, target_rp)
            atomic_write_json(
                receipts_dir / f"{target_rp}.receipt.json",
                {
                    "current_restore_point": current_rp,
                    "target_restore_point": target_rp,
                    "checked_at_utc": utc_now_iso(),
                    "mode": "shutdown",
                    "status": "reached_then_shutdown",
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


def run_once(cfg: Config, target: str = "LATEST") -> int:
    try:
        return _cycle(cfg, target=target)
    except KeyboardInterrupt:
        print("\n[DR] stop requested (Ctrl+C). Exiting cleanly.")
        return 130
    except Exception as e:
        print(f"[DR] ERROR: {e}", file=sys.stderr)
        return 2


def run_daemon(cfg: Config, target: str = "LATEST") -> int:
    try:
        while True:
            try:
                _cycle(cfg, target=target)
            except Exception as e:
                print(f"[DR] ERROR: {e}", file=sys.stderr)
            time.sleep(cfg.consumer_sleep_secs)
    except KeyboardInterrupt:
        print("\n[DR] stop requested (Ctrl+C). Exiting cleanly.")
        return 130
