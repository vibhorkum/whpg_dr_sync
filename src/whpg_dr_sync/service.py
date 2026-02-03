from __future__ import annotations

import os
import signal
from pathlib import Path
from typing import Optional

from .config import Config


def _pidfile(cfg: Config, role: str) -> Path:
    p = Path(cfg.state_dir) / f"{role}.pid"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def write_pid(cfg: Config, role: str, pid: int) -> None:
    """
    Write pidfile, but refuse to overwrite an active daemon pid.
    This prevents accidental double-start.
    """
    p = _pidfile(cfg, role)
    old = read_pid(cfg, role)
    if old and is_running(old):
        raise RuntimeError(f"[{role}] already running (pid={old}) pidfile={p}")
    p.write_text(str(pid) + "\n")


def remove_pid(cfg: Config, role: str, pid: Optional[int] = None) -> None:
    """
    Remove pidfile if it belongs to 'pid' (if provided), else remove unconditionally.
    """
    p = _pidfile(cfg, role)
    if not p.exists():
        return
    if pid is None:
        p.unlink(missing_ok=True)
        return
    cur = read_pid(cfg, role)
    if cur == pid:
        p.unlink(missing_ok=True)


def read_pid(cfg: Config, role: str) -> Optional[int]:
    p = _pidfile(cfg, role)
    if not p.exists():
        return None
    try:
        s = p.read_text().strip()
        return int(s) if s else None
    except Exception:
        return None


def is_running(pid: int) -> bool:
    """
    True if process exists (even if permission denied).
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def stop(cfg: Config, role: str) -> None:
    pid = read_pid(cfg, role)
    if not pid:
        print(f"[{role}] no pidfile")
        return
    if not is_running(pid):
        print(f"[{role}] pid {pid} not running; cleaning pidfile")
        remove_pid(cfg, role)
        return
    os.kill(pid, signal.SIGTERM)
    print(f"[{role}] sent SIGTERM to pid={pid}")


def status(cfg: Config, role: str) -> None:
    pid = read_pid(cfg, role)
    if not pid:
        print(f"[{role}] STOPPED (no pidfile)")
        return
    if is_running(pid):
        print(f"[{role}] RUNNING pid={pid}")
    else:
        print(f"[{role}] STOPPED (stale pidfile pid={pid})")
