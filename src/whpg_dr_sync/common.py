from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar

# Configure module logger
logger = logging.getLogger(__name__)

T = TypeVar('T')


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


def with_retry(
    func: Callable[[], T],
    max_retries: int = 3,
    backoff_base: float = 1.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> T:
    """
    Execute a function with exponential backoff retry.

    Args:
        func: Function to execute
        max_retries: Maximum number of attempts
        backoff_base: Base delay in seconds (doubles each retry)
        exceptions: Tuple of exception types to catch and retry
        on_retry: Optional callback(exception, attempt) called before each retry

    Returns:
        Result of func()

    Raises:
        The last exception if all retries fail
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            last_exc = e
            if attempt < max_retries - 1:
                sleep_time = backoff_base * (2 ** attempt)
                if on_retry:
                    on_retry(e, attempt)
                else:
                    logger.warning(
                        "Retry %d/%d after error: %s (sleeping %.1fs)",
                        attempt + 1, max_retries, e, sleep_time
                    )
                time.sleep(sleep_time)
    if last_exc:
        raise last_exc
    raise RuntimeError("with_retry: no attempts made")


# Default timeout for subprocess operations (5 minutes)
DEFAULT_TIMEOUT_SECS = 300


def run(
    cmd: List[str],
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    timeout: Optional[int] = DEFAULT_TIMEOUT_SECS,
) -> str:
    """
    Subprocess runner that converts Ctrl-C/SIGTERM into ShutdownRequested,
    instead of dumping a traceback.

    Args:
        cmd: Command and arguments to execute
        env: Optional environment variables
        check: If True, raise on non-zero exit code
        timeout: Timeout in seconds (default 300s, None for no timeout)

    Returns:
        stdout as string (stripped)

    Raises:
        ShutdownRequested: On SIGINT/SIGTERM
        RuntimeError: On command failure (if check=True) or timeout
    """
    check_stop()
    try:
        p = subprocess.run(cmd, text=True, capture_output=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        cmd_str = " ".join(cmd[:5]) + ("..." if len(cmd) > 5 else "")
        raise RuntimeError(f"Command timed out after {timeout}s: {cmd_str}")
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
