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
    _pidfile(cfg, role).write_text(str(pid) + "\n")


def read_pid(cfg: Config, role: str) -> Optional[int]:
    p = _pidfile(cfg, role)
    if not p.exists():
        return None
    try:
        return int(p.read_text().strip())
    except Exception:
        return None


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
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
        _pidfile(cfg, role).unlink(missing_ok=True)
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
