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
from .service import write_pid, remove_pid

# =============================
# Shell helpers
# =============================
def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"

def ssh_bash(host: str, script: str, check: bool = True) -> str:
    # Use a non-interactive, non-login shell to keep output stable
    cmd = f"bash --noprofile --norc -lc {sh_quote(script)}"
    return run(["ssh", host, cmd], check=check)

def gpssh_bash(host: str, script: str, check: bool = True) -> str:
    cmd = f"bash --noprofile --norc -lc {sh_quote(script)}"
    return run(["gpssh", "-h", host, "-e", cmd], check=check)

def _preflight(inst: DrInstance, gp_home: str) -> None:
    if inst.gp_segment_id == -1:
        return
    cmd = (
        f"set -euo pipefail; "
        f"test -f {sh_quote(gp_home)}/greenplum_path.sh; "
        f"source {sh_quote(gp_home)}/greenplum_path.sh; "
        f"which pg_ctl; "
        f"test -d {sh_quote(inst.data_dir)}; "
        f"echo OK host=$(hostname) datadir={sh_quote(inst.data_dir)}"
    )
    out = ssh_bash(inst.host, cmd, check=False)
    print(f"[DR][seg={inst.gp_segment_id}] preflight: {out}")

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

def _pg_controldata_min_recovery_end_lsn(inst: DrInstance, gp_home: str) -> Optional[str]:
    """
    Reads 'Minimum recovery ending location' from pg_controldata.
    Works even when the postmaster is down.
    """
    pgcd = f"{gp_home}/bin/pg_controldata"
    cmd = f"{pgcd} {sh_quote(inst.data_dir)}"
    out = run(["bash", "-lc", cmd], check=False) if inst.is_local else gpssh_bash(inst.host, cmd, check=False)
    if not out:
        return None
    m = re.search(r"Minimum recovery ending location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)", out)
    return m.group(1).strip() if m else None

def controldata_lsns(inst: DrInstance, gp_home: str) -> Dict[str, str]:
    pgcd = f"{gp_home}/bin/pg_controldata"
    cmd = f"{pgcd} {sh_quote(inst.data_dir)}"
    out = run(["bash", "-lc", cmd], check=False) if inst.is_local else gpssh_bash(inst.host, cmd, check=False)
    if not out:
        return {}

    fields = {
        "min_recovery_end_lsn": r"Minimum recovery ending location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)",
        "latest_checkpoint_lsn": r"Latest checkpoint location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)",
        "latest_redo_lsn": r"Latest redo location:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)",
    }
    res: Dict[str, str] = {}
    for k, pat in fields.items():
        m = re.search(pat, out)
        if m:
            res[k] = m.group(1).strip()
    return res

def controldata_reached_target(inst: DrInstance, gp_home: str, target_lsn: str) -> Tuple[bool, Dict[str, str]]:
    lsns = controldata_lsns(inst, gp_home)
    for _, v in lsns.items():
        if lsn_ge(v, target_lsn):
            return True, lsns
    return False, lsns


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
def clear_recovery_targets(inst: DrInstance) -> None:
    check_stop()
    conf = f"{inst.data_dir}/postgresql.conf"
    keys = [
        "recovery_target",
        "recovery_target_name",
        "recovery_target_lsn",
        "recovery_target_time",
        "recovery_target_xid",
    ]
    for k in keys:
        script = rewrite_conf_kv(conf, k, f"# {k} = ''")
        if inst.is_local:
            run(["bash", "-lc", script], check=True)
        else:
            run(["ssh", inst.host, "bash", "--noprofile", "--norc", "-lc", script], check=True)

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


def set_recovery_target_name(inst: DrInstance, target_rp: str) -> None:
    check_stop()
    conf = f"{inst.data_dir}/postgresql.conf"
    rp = (target_rp or "").strip().replace("\r", "")
    script = rewrite_conf_kv(conf, "recovery_target_name", f"recovery_target_name = '{rp}'")
    if inst.is_local:
        run(["bash", "-lc", script], check=True)
    else:
        run(["ssh", inst.host, "bash", "--noprofile", "--norc", "-lc", script], check=True)

def set_recovery_target_lsn(inst: DrInstance, target_lsn: str) -> None:
    check_stop()
    conf = f"{inst.data_dir}/postgresql.conf"
    lsn = (target_lsn or "").strip().replace("\r", "")
    script = rewrite_conf_kv(conf, "recovery_target_lsn", f"recovery_target_lsn = '{lsn}'")
    if inst.is_local:
        run(["bash", "-lc", script], check=True)
    else:
        gpssh_bash(inst.host, script, check=True)

def _extract_first_csv_path(text: str) -> Optional[str]:
    # gpssh output often includes: "[host] /path/to/gpdb-....csv"
    m = re.search(r"(/[^ \n\t]+\.csv)\b", text or "")
    return m.group(1) if m else None

def newest_log_csv(inst: DrInstance) -> Optional[str]:
    logdir = f"{inst.data_dir}/log"
    script = (
        "set -euo pipefail; "
        f"ls -1t {sh_quote(logdir)}/*.csv 2>/dev/null | head -n 1 || true"
    )

    out = run(["bash", "-lc", script], check=False) if inst.is_local else ssh_bash(inst.host, script, check=False)
    p = (out or "").strip()
    return p or None

#def newest_log_csv(inst: DrInstance) -> Optional[str]:
#    """
#    Return full path to newest gpdb CSV log file for an instance, or None.
#    """
#    logdir = f"{inst.data_dir}/log"
#    script = (
#        "set -euo pipefail; "
#        f"f=$(ls -1t {sh_quote(logdir)}/*.csv 2>/dev/null | head -n 1 || true); "
#        'if [ -z "$f" ]; then exit 0; fi; '
#        'echo "$f"'
#    )
#    out = run(["bash", "-lc", script], check=False) if inst.is_local else gpssh_bash(inst.host, script, check=False)
#    p = (out or "").strip()
#    return p or None



def tail_text_file(inst: DrInstance, path: str, n: int = 200) -> str:
    """
    Tail last N lines of a file (local or remote). Returns text (may be empty).
    """
    script = f"set -euo pipefail; test -f {sh_quote(path)} || exit 0; tail -n {int(n)} {sh_quote(path)}"
    return run(["bash", "-lc", script], check=False) if inst.is_local else ssh_bash(inst.host, script, check=False)


def parse_latest_recovery_stop_restore_point(log_text: str) -> Optional[str]:
    """
    Parse the most recent restore point name from lines like:
      recovery stopping at restore point "sync_point_20260201_183847", time ...

    Returns restore_point string or None.
    """
    if not log_text:
        return None

    # We want the *latest* occurrence in the tailed chunk.
    rp: Optional[str] = None

    for line in log_text.splitlines():
        if "recovery stopping at restore point" not in line:
            continue

        # Works for CSV log lines too (quoted fields)
        # Example snippet:
        # ...,"LOG","00000","recovery stopping at restore point ""sync_point_..."" ...
        m = re.search(r'recovery stopping at restore point\s+""?([^",]+)""?', line)
        if m:
            rp = m.group(1).strip()

    return rp

def recent_log_csv(inst: DrInstance, k: int = 5) -> List[str]:
    """
    Return full paths to the newest K gpdb CSV log files for an instance.
    Newest first. May be empty.
    """
    logdir = f"{inst.data_dir}/log"
    script = (
        "set -euo pipefail; "
        f"ls -1t {sh_quote(logdir)}/*.csv 2>/dev/null | head -n {int(k)} || true"
    )
    out = run(["bash", "-lc", script], check=False) if inst.is_local else ssh_bash(inst.host, script, check=False)
    return [ln.strip() for ln in (out or "").splitlines() if ln.strip()]


def last_stopped_restore_point_scan(
    inst: DrInstance,
    k_files: int = 5,
    tail_n: int = 1500,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Scan newest K CSV log files and return the most recent 'recovery stopping at restore point' seen.
    Returns (restore_point, logfile_path_where_found_or_newest_checked).
    """
    files = recent_log_csv(inst, k=k_files)
    if not files:
        return None, None

    for f in files:
        txt = tail_text_file(inst, f, n=tail_n)
        rp = parse_latest_recovery_stop_restore_point(txt)
        if rp:
            return rp, f

    return None, files[0]

def last_stopped_restore_point(inst: DrInstance, n: int = 300) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (restore_point, logfile_path).
    If not found, returns (None, logfile_path or None).
    """
    f = newest_log_csv(inst)
    if not f or not f.endswith(".csv") or "bash --noprofile" in f:
        print(f"[DR][seg={inst.gp_segment_id}] LOG invalid logfile path: {f!r}")
        return None, None
    #if not f:
    #    return None, None

    txt = tail_text_file(inst, f, n=n)
    rp = parse_latest_recovery_stop_restore_point(txt)
    return rp, f


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
    logfile = f"{inst.data_dir}/start.log"
    if inst.gp_segment_id == -1:
        cmd = (
            f"source {gp_home}/greenplum_path.sh && "
            f"export COORDINATOR_DATA_DIRECTORY={inst.data_dir} && "
            f"pg_ctl -D {inst.data_dir} -o \"-c gp_role=utility\" -l {sh_quote(logfile)} start"
        )
        run(["bash", "-lc", cmd], check=False)
        return
    cmd = (
        f"source {gp_home}/greenplum_path.sh && "
        f"pg_ctl -D {inst.data_dir} -o \"-c gp_role=utility -c port={inst.port}\" start -l {sh_quote(logfile)}"
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
        #set_recovery_target_lsn(inst, tgt_lsn)
        clear_recovery_targets(inst)
        set_recovery_target_name(inst, target_rp)
        _pg_ctl_stop(inst, cfg.gp_home)
        time.sleep(1)
        _preflight(inst, cfg.gp_home)
        _pg_ctl_start(inst, cfg.gp_home)

    print(
        f"[DR] Waiting for shutdown-at-target confirmation "
        f"(max_wait_secs={cfg.consumer_wait_reach_secs} poll_secs={cfg.consumer_reach_poll_secs})..."
    )

    waited = 0
    while waited <= cfg.consumer_wait_reach_secs:
        check_stop()
        all_down = True

        for seg_id, inst in instances.items():
            tgt_lsn = target_lsns[seg_id]

            rp, logfile = last_stopped_restore_point_scan(inst, k_files=5, tail_n=1500)
            if rp:
                print(f"[DR][seg={seg_id}] LOG stop_restore_point={rp} file={logfile}")
            else:
                print(f"[DR][seg={seg_id}] LOG no stop signature found (tail) file={logfile or '-'}")

            ok, replay, _ = try_sql(inst.host, inst.port, user, db, "SELECT pg_last_wal_replay_lsn();")
            if ok and replay:
                replay_s = replay.strip()
                reached = lsn_ge(replay_s, tgt_lsn)
                print(f"[DR][seg={seg_id}] UP replay_lsn={replay_s} target_lsn={tgt_lsn} reached={reached}")
                if not reached:
                    all_down = False
                continue

            # DOWN: confirm via pg_controldata
            floor = _pg_controldata_min_recovery_end_lsn(inst, cfg.gp_home)
            if floor and lsn_ge(floor, tgt_lsn):
                print(f"[DR][seg={seg_id}] DOWN controldata_ok min_recovery_end_lsn={floor} >= target_lsn={tgt_lsn}")
            else:
                ok_cd, lsns = controldata_reached_target(inst, cfg.gp_home, tgt_lsn)
                if ok_cd:
                    print(f"[DR][seg={seg_id}] DOWN controldata_ok {lsns}")
                else:
                    print(f"[DR][seg={seg_id}] DOWN not_confirmed {lsns or 'no_controldata'} < target_lsn={tgt_lsn}")
                    all_done = False


        if all_down:
            rp, logfile = last_stopped_restore_point_scan(inst, k_files=5, tail_n=1500)
            if rp:
                print(f"[DR][seg={seg_id}] LOG stop_restore_point={rp} file={logfile}")
            else:
                print(f"[DR][seg={seg_id}] LOG no stop signature found (tail) file={logfile or '-'}")

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
    pid = os.getpid()
    write_pid(cfg, "dr", pid)
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
    finally:
        remove_pid(cfg, "dr", pid)
