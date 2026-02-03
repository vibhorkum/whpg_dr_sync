from __future__ import annotations

import json
import os
import signal
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# =============================
# Graceful shutdown plumbing
# =============================
_STOP = False


def _request_stop(signum: int, frame: object) -> None:
    # Called on SIGINT/SIGTERM
    global _STOP
    _STOP = True


# Register handlers once at import time
signal.signal(signal.SIGINT, _request_stop)
signal.signal(signal.SIGTERM, _request_stop)


@dataclass(frozen=True)
class ShutdownRequested(RuntimeError):
    reason: str = "shutdown requested"
    code: int = 130  # 130 = standard exit code for Ctrl-C


def check_stop() -> None:
    """
    Call this in loops / between steps to exit cleanly on Ctrl-C / SIGTERM.
    """
    if _STOP:
        raise ShutdownRequested("shutdown requested")


# =============================
# Basic helpers
# =============================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n")
    os.replace(str(tmp), str(path))


def run(cmd: List[str], env: Optional[Dict[str, str]] = None, check: bool = True) -> str:
    """
    subprocess runner that converts Ctrl-C/SIGTERM into ShutdownRequested,
    instead of dumping a traceback.
    """
    check_stop()
    try:
        p = subprocess.run(cmd, text=True, capture_output=True, env=env)
    except KeyboardInterrupt:
        # If SIGINT arrived while we were waiting on a child
        raise ShutdownRequested("interrupted (Ctrl-C)")

    if _STOP:
        # SIGTERM/SIGINT could arrive just after subprocess returns
        raise ShutdownRequested("shutdown requested")

    if check and p.returncode != 0:
        raise RuntimeError(
            "Command failed: {}\nSTDOUT:\n{}\nSTDERR:\n{}".format(" ".join(cmd), p.stdout, p.stderr)
        )
    return (p.stdout or "").strip()


# =============================
# psql helpers
# =============================
def psql(
    host: str,
    port: int,
    user: str,
    db: str,
    sql: str,
    pgoptions: str = "",
) -> str:
    env = os.environ.copy()
    if pgoptions:
        env["PGOPTIONS"] = pgoptions
    cmd = ["psql", "-qtA", "-h", host, "-p", str(port), "-U", user, "-d", db, "-c", sql]
    return run(cmd, env=env, check=True).strip()


def psql_util(host: str, port: int, user: str, db: str, sql: str) -> str:
    """
    Utility-mode psql (Greenplum segments)
    """
    return psql(host, port, user, db, sql, pgoptions="-c gp_session_role=utility")


# =============================
# SSH helpers
# =============================
def ssh_test_file(host: str, path: str) -> bool:
    """
    Fast existence check used by publisher archive readiness logic.
    """
    try:
        run(["ssh", host, f"test -f {path}"], check=True)
        return True
    except ShutdownRequested:
        raise
    except Exception:
        return False
