from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n")
    os.replace(str(tmp), str(path))


def run(cmd: List[str], env: Optional[Dict[str, str]] = None, check: bool = True) -> str:
    p = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if check and p.returncode != 0:
        raise RuntimeError(
            "Command failed: {}\nSTDOUT:\n{}\nSTDERR:\n{}".format(" ".join(cmd), p.stdout, p.stderr)
        )
    return (p.stdout or "").strip()

def atomic_write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n")
    os.replace(str(tmp), str(path))


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


def ssh_test_file(host: str, path: str) -> bool:
    try:
        run(["ssh", host, f"test -f {path}"], check=True)
        return True
    except Exception:
        return False
