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
        f"awk -v k={k} '!($0 ~ \"^[[:space:]]*#?[[:space:]]*\" k \"[[:space:]]*=\") {{print}}' \"$conf\" > \"$tmp\"; "
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
    """
    Load target manifest. If target is "LATEST", always use LATEST.json when ready.
    Do not fall back to older manifests if LATEST exists but is not ready.
    """
    manifest_dir = Path(cfg.manifest_dir)
    latest_path = Path(cfg.latest_path)

    if target != "LATEST":
        # Specific target requested
        p = manifest_dir / f"{target}.json"
        if not p.exists():
            print(f"[DR] ERROR: manifest not found: {p}")
            return None
        m = json.loads(p.read_text())
        return m if _manifest_ready(m) else None

    # target == "LATEST": always use LATEST.json, do not fall back
    if not latest_path.exists():
        print("[DR] No LATEST manifest exists.")
        return None

    m = json.loads(latest_path.read_text())
    if not _manifest_ready(m):
        print("[DR] LATEST manifest not ready/valid yet. Will not use older manifests.")
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
# WAL file helpers
# =============================
def _get_wal_segment_info(inst: DrInstance, gp_home: str) -> Tuple[int, int]:
    """
    Get WAL segment size and current timeline ID from pg_controldata and timeline history files.
    Returns (wal_segment_size_bytes, timeline_id).
    """
    pgcd = f"{gp_home}/bin/pg_controldata"
    cmd = f"{pgcd} {sh_quote(inst.data_dir)}"
    out = run(["bash", "-lc", cmd], check=False) if inst.is_local else gpssh_bash(inst.host, cmd, check=False)
    
    wal_seg_size = 64 * 1024 * 1024  # default 64MB
    timeline_id = 1  # default
    
    if out:
        # Look for "Bytes per WAL segment:"
        m = re.search(r"Bytes per WAL segment:\s+(\d+)", out)
        if m:
            wal_seg_size = int(m.group(1))
        
        # Look for timeline from pg_controldata
        m = re.search(r"Latest checkpoint's TimeLineID:\s+(\d+)", out)
        if m:
            timeline_id = int(m.group(1))
    
    # Check for timeline history files to get the most accurate current timeline
    # Timeline history files are named like 00000002.history, 00000003.history, etc.
    # The highest numbered .history file + 1 is the current timeline (or the value itself if on that timeline)
    wal_dir = f"{inst.data_dir}/pg_wal"
    # Try pg_wal first (PG 10+), fall back to pg_xlog (PG 9.6 and earlier)
    history_cmd = (
        f"ls {sh_quote(wal_dir)}/*.history 2>/dev/null || "
        f"ls {sh_quote(inst.data_dir)}/pg_xlog/*.history 2>/dev/null || true"
    )
    
    history_out = run(["bash", "-lc", history_cmd], check=False) if inst.is_local else ssh_bash(inst.host, history_cmd, check=False)
    
    if history_out and history_out.strip():
        # Parse timeline from history filenames
        max_timeline = timeline_id  # Start with pg_controldata value
        for line in history_out.strip().splitlines():
            # Extract timeline number from filename like /path/00000003.history
            m = re.search(r"/([0-9A-Fa-f]{8})\.history", line)
            if m:
                tl = int(m.group(1), 16)
                if tl > max_timeline:
                    max_timeline = tl
        
        # If we found history files, the current timeline is likely the max + 1
        # or we're on the timeline of the latest history file
        if max_timeline > timeline_id:
            timeline_id = max_timeline
    
    return wal_seg_size, timeline_id


def _wal_filename_for_lsn(lsn: str, timeline_id: int, wal_seg_size: int) -> str:
    """
    Convert LSN to WAL filename given timeline and segment size.
    Returns filename like "000000010000003C0000002F".
    
    WAL filename format: TTTTTTTTXXXXXXXXYYYYYYYY where:
    - T = timeline ID (8 hex digits)
    - X = high 32 bits of LSN (log file ID) (8 hex digits)
    - Y = (low 32 bits of LSN) / segment_size (segment number) (8 hex digits)
    """
    lsn_int = lsn_to_int(lsn)
    
    # Extract high and low 32 bits from LSN
    xlogid = (lsn_int >> 32) & 0xFFFFFFFF  # High 32 bits (log file number)
    xrecoff = lsn_int & 0xFFFFFFFF  # Low 32 bits (offset)
    
    # Calculate segment number within the log file
    seg_no = xrecoff // wal_seg_size
    
    return f"{timeline_id:08X}{xlogid:08X}{seg_no:08X}"


def _list_wal_files_between_lsns(start_lsn: str, end_lsn: str, timeline_id: int, wal_seg_size: int) -> List[str]:
    """
    List all WAL filenames needed between start_lsn (exclusive) and end_lsn (inclusive).
    
    WAL filename format: TTTTTTTTXXXXXXXXYYYYYYYY where:
    - T = timeline ID (8 hex digits)
    - X = high 32 bits of LSN (log file ID) (8 hex digits)  
    - Y = (low 32 bits of LSN) / segment_size (segment number) (8 hex digits)
    """
    start_int = lsn_to_int(start_lsn)
    end_int = lsn_to_int(end_lsn)
    
    if start_int >= end_int:
        return []
    
    # Extract log file and segment for start LSN
    start_xlogid = (start_int >> 32) & 0xFFFFFFFF
    start_xrecoff = start_int & 0xFFFFFFFF
    start_seg = start_xrecoff // wal_seg_size
    
    # Extract log file and segment for end LSN
    end_xlogid = (end_int >> 32) & 0xFFFFFFFF
    end_xrecoff = end_int & 0xFFFFFFFF
    end_seg = end_xrecoff // wal_seg_size
    
    files = []
    
    # Calculate max segments per log file (256 for 64MB segments in 16GB log files)
    # In PostgreSQL, XLogSegmentsPerXLogId is typically 0x100 (256)
    segs_per_xlogid = 0x100000000 // wal_seg_size
    
    # Iterate through all log files and segments
    current_xlogid = start_xlogid
    current_seg = start_seg + 1  # Start from next segment after start_lsn
    
    while (current_xlogid < end_xlogid) or (current_xlogid == end_xlogid and current_seg <= end_seg):
        filename = f"{timeline_id:08X}{current_xlogid:08X}{current_seg:08X}"
        files.append(filename)
        
        # Move to next segment
        current_seg += 1
        
        # If we've reached the max segments per log file, move to next log file
        if current_seg >= segs_per_xlogid:
            current_seg = 0
            current_xlogid += 1
    
    return files


def _check_wal_file_exists(archive_dir: str, wal_filename: str, host: str, is_local: bool, custom_cmd: str = "") -> bool:
    """
    Check if a WAL file exists in the archive directory (local or remote).
    
    Supports custom check commands for flexibility (e.g., S3, API, remote SSH).
    Custom command template variables:
    - {archive_dir}: Archive directory path
    - {wal_filename}: WAL filename to check
    - {host}: Host where check should run
    
    Custom command should output 'EXISTS' if file is present.
    
    Examples:
    - SSH: "ssh {host} test -f {archive_dir}/{wal_filename} && echo EXISTS"
    - S3: "aws s3 ls s3://bucket/{archive_dir}/{wal_filename} && echo EXISTS"
    """
    wal_path = f"{archive_dir}/{wal_filename}"
    
    if custom_cmd:
        # Use custom command with template substitution
        script = custom_cmd.format(
            archive_dir=archive_dir,
            wal_filename=wal_filename,
            wal_path=wal_path,
            host=host,
        )
        out = run(["bash", "-lc", script], check=False)
    else:
        # Default: simple file existence check
        script = f"test -f {sh_quote(wal_path)} && echo 'EXISTS' || echo 'MISSING'"
        out = run(["bash", "-lc", script], check=False) if is_local else ssh_bash(host, script, check=False)
    
    return "EXISTS" in (out or "")


def _preflight_wal_check(
    cfg: Config,
    instances: Dict[int, DrInstance],
    current_rp: str,
    target_rp: str,
    target_lsns: Dict[int, str],
) -> Tuple[bool, Dict[int, List[str]]]:
    """
    Pre-flight check: verify all required WAL files exist before starting recovery.
    
    Returns (all_present, missing_by_segment) where:
    - all_present: True if all WAL files are present
    - missing_by_segment: dict of segment_id -> list of missing WAL filenames
    """
    print("[DR] Pre-flight: checking WAL availability...")
    
    # Get current state LSNs from pg_controldata
    current_lsns: Dict[int, str] = {}
    for seg_id, inst in instances.items():
        lsns = controldata_lsns(inst, cfg.gp_home)
        # Use the highest LSN as current position
        current_lsn = lsns.get("min_recovery_end_lsn") or lsns.get("latest_checkpoint_lsn") or "0/0"
        current_lsns[seg_id] = current_lsn
    
    missing_by_segment: Dict[int, List[str]] = {}
    all_present = True
    
    for seg_id, inst in instances.items():
        start_lsn = current_lsns.get(seg_id, "0/0")
        end_lsn = target_lsns.get(seg_id, "0/0")
        
        # Get WAL segment size and timeline
        wal_seg_size, timeline_id = _get_wal_segment_info(inst, cfg.gp_home)
        
        # List required WAL files
        required_wals = _list_wal_files_between_lsns(start_lsn, end_lsn, timeline_id, wal_seg_size)
        
        if not required_wals:
            print(f"[DR][seg={seg_id}] No WAL files needed (current={start_lsn}, target={end_lsn})")
            continue
        
        print(f"[DR][seg={seg_id}] Checking {len(required_wals)} WAL files (current={start_lsn}, target={end_lsn})")
        
        # Check each WAL file
        missing = []
        archive_dir = cfg.archive_dir
        custom_cmd = cfg.wal_check_command
        
        for wal_file in required_wals[:100]:  # Limit check to first 100 to avoid overwhelming
            if not _check_wal_file_exists(archive_dir, wal_file, inst.host, inst.is_local, custom_cmd):
                missing.append(wal_file)
        
        if missing:
            missing_by_segment[seg_id] = missing
            all_present = False
            print(f"[DR][seg={seg_id}] ❌ Missing {len(missing)} WAL file(s), first few: {missing[:5]}")
        else:
            print(f"[DR][seg={seg_id}] ✅ All required WAL files present")
    
    return all_present, missing_by_segment


def _validate_recovery_points(
    instances: Dict[int, DrInstance],
    target_rp: str,
) -> Tuple[bool, Dict[int, Optional[str]]]:
    """
    Validate that all segments stopped at the expected restore point.
    
    Returns (all_match, recovery_points) where:
    - all_match: True if all segments stopped at target_rp
    - recovery_points: dict of segment_id -> actual recovery_point found (or None)
    """
    recovery_points: Dict[int, Optional[str]] = {}
    all_match = True
    
    for seg_id, inst in instances.items():
        rp, logfile = last_stopped_restore_point_scan(inst, k_files=5, tail_n=1500)
        recovery_points[seg_id] = rp
        
        if rp != target_rp:
            all_match = False
            if rp:
                print(f"[DR][seg={seg_id}] ❌ Recovery point mismatch: expected={target_rp}, actual={rp}")
            else:
                print(f"[DR][seg={seg_id}] ❌ No recovery point found in logs (expected={target_rp})")
        else:
            print(f"[DR][seg={seg_id}] ✅ Recovery point matches: {rp}")
    
    return all_match, recovery_points


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

    # Pre-flight WAL availability check
    wal_check_ok, missing_wals = _preflight_wal_check(cfg, instances, current_rp, target_rp, target_lsns)
    
    if not wal_check_ok:
        print("[DR] ❌ Pre-flight FAILED: Missing WAL files detected. Will NOT start recovery.")
        print("[DR] Missing WAL summary:")
        for seg_id, missing in missing_wals.items():
            print(f"  seg={seg_id}: {len(missing)} missing, first 5: {missing[:5]}")
        
        # Write receipt for failed pre-flight
        atomic_write_json(
            receipts_dir / f"{target_rp}.preflight_failed.json",
            {
                "current_restore_point": current_rp,
                "target_restore_point": target_rp,
                "checked_at_utc": utc_now_iso(),
                "status": "preflight_failed_missing_wal",
                "missing_wals_by_segment": {str(k): v for k, v in missing_wals.items()},
            },
        )
        return 0
    
    print("[DR] ✅ Pre-flight passed: All required WAL files are present")

    print("[DR] Applying recovery_target_name and recovery_target_action='shutdown' + start...")
    for seg_id, inst in instances.items():
        check_stop()
        tgt_lsn = target_lsns.get(seg_id)
        if not tgt_lsn:
            raise RuntimeError(f"[DR][seg={seg_id}] target manifest missing restore_lsn")

        print(f"[DR][seg={seg_id}] apply target_name={target_rp} and start")
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
                    all_down = False


        if all_down:
            # Validate recovery points from logs
            print("[DR] All instances DOWN. Validating recovery points from logs...")
            rp_match, recovery_points = _validate_recovery_points(instances, target_rp)
            
            if rp_match:
                print(f"[DR] ✅ SUCCESS: All segments stopped at restore point '{target_rp}'. Advancing state.")
                _set_current_restore_point(cfg, target_rp)
                atomic_write_json(
                    receipts_dir / f"{target_rp}.receipt.json",
                    {
                        "current_restore_point": current_rp,
                        "target_restore_point": target_rp,
                        "checked_at_utc": utc_now_iso(),
                        "mode": "shutdown",
                        "status": "success_recovery_point_validated",
                        "waited_secs": waited,
                        "target_lsns": {str(k): v for k, v in target_lsns.items()},
                        "recovery_points": {str(k): v for k, v in recovery_points.items()},
                    },
                )
                return 0
            else:
                print(f"[DR] ⚠️  All instances DOWN but recovery points don't match. Will retry next cycle.")
                atomic_write_json(
                    receipts_dir / f"{target_rp}.recovery_point_mismatch.json",
                    {
                        "current_restore_point": current_rp,
                        "target_restore_point": target_rp,
                        "checked_at_utc": utc_now_iso(),
                        "mode": "shutdown",
                        "status": "recovery_point_mismatch",
                        "waited_secs": waited,
                        "target_lsns": {str(k): v for k, v in target_lsns.items()},
                        "actual_recovery_points": {str(k): v for k, v in recovery_points.items()},
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
