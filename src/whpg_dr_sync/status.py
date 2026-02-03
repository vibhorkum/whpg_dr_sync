from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text())
    except Exception:
        return None


def _read_text(path: Path) -> Optional[str]:
    try:
        if not path.exists():
            return None
        s = path.read_text().strip()
        return s or None
    except Exception:
        return None


def _safe_str(x: Any, default: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return default


def _fmt_ts(ts: Any) -> str:
    s = _safe_str(ts, "")
    return s if s else "-"


def _glob_receipts(receipts_dir: Path) -> List[Path]:
    if not receipts_dir.exists():
        return []
    return sorted(receipts_dir.glob("*.receipt.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def _table(rows: List[List[str]]) -> str:
    if not rows:
        return ""
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    out: List[str] = []
    for idx, r in enumerate(rows):
        out.append("  ".join(r[i].ljust(widths[i]) for i in range(len(r))))
        if idx == 0:
            out.append("  ".join("-" * widths[i] for i in range(len(r))))
    return "\n".join(out)


@dataclass
class Snapshot:
    mode: str  # "dr" or "primary"
    latest_restore_point: str
    latest_ready: Optional[bool]
    current_restore_point: str
    target_restore_point: str
    last_receipt_file: str
    last_receipt_status: str
    last_receipt_checked_at_utc: str
    last_receipt_waited_secs: Optional[int]
    notes: List[str]


def _load_latest(cfg) -> Tuple[str, Optional[bool], List[str]]:
    notes: List[str] = []
    latest_path = Path(cfg.latest_path)
    latest = _read_json(latest_path)
    if not latest:
        return "-", None, ["LATEST manifest not readable/missing"]

    rp = _safe_str(latest.get("restore_point"), "").strip() or "-"
    ready = latest.get("ready", None)
    if rp == "-":
        notes.append("LATEST missing restore_point")
    return rp, (ready if isinstance(ready, bool) else None), notes


def _load_current_dr(cfg) -> Tuple[str, List[str]]:
    notes: List[str] = []
    state_file = Path(cfg.state_dir) / "current_restore_point.txt"
    cur = _read_text(state_file) or "-"
    if cur == "-":
        notes.append("current_restore_point.txt missing/empty")
    return cur, notes


def _load_last_receipt(cfg, target_rp: str, history_n: int) -> Tuple[str, str, str, Optional[int], List[dict], List[str]]:
    notes: List[str] = []
    receipts_dir = Path(cfg.receipts_dir)

    last: Optional[dict] = None
    last_file = "-"

    # Prefer target receipt if it exists
    if target_rp and target_rp != "-":
        p = receipts_dir / f"{target_rp}.receipt.json"
        r = _read_json(p)
        if r:
            last = r
            last_file = p.name

    # Fallback: newest receipt by mtime
    if last is None:
        receipts = _glob_receipts(receipts_dir)
        if receipts:
            last_file = receipts[0].name
            last = _read_json(receipts[0])

    status = _safe_str((last or {}).get("status"), "-")
    checked = _fmt_ts((last or {}).get("checked_at_utc"))
    waited = (last or {}).get("waited_secs", None)
    waited_i: Optional[int] = None
    try:
        if waited is not None:
            waited_i = int(waited)
    except Exception:
        waited_i = None

    # history
    hist: List[dict] = []
    for p in _glob_receipts(receipts_dir)[: max(1, history_n)]:
        r = _read_json(p)
        if r:
            r["_file"] = p.name
            hist.append(r)

    if not hist:
        notes.append("no receipts found")

    return last_file, status, checked, waited_i, hist, notes


def collect_dr(cfg, history_n: int = 10) -> Tuple[Snapshot, List[dict]]:
    notes: List[str] = []

    latest_rp, latest_ready, n1 = _load_latest(cfg)
    notes.extend(n1)

    current_rp, n2 = _load_current_dr(cfg)
    notes.extend(n2)

    target_rp = latest_rp  # DR wants to converge to LATEST by default

    last_file, last_status, last_checked, last_waited, hist, n3 = _load_last_receipt(cfg, target_rp, history_n)
    notes.extend(n3)

    snap = Snapshot(
        mode="dr",
        latest_restore_point=latest_rp,
        latest_ready=latest_ready,
        current_restore_point=current_rp,
        target_restore_point=target_rp,
        last_receipt_file=last_file,
        last_receipt_status=last_status,
        last_receipt_checked_at_utc=last_checked,
        last_receipt_waited_secs=last_waited,
        notes=notes,
    )
    return snap, hist


def collect_primary(cfg, history_n: int = 10) -> Tuple[Snapshot, List[dict]]:
    notes: List[str] = []

    latest_rp, latest_ready, n1 = _load_latest(cfg)
    notes.extend(n1)

    # On PRIMARY there’s no "current_restore_point.txt" concept; make it explicit.
    current_rp = "-"
    target_rp = latest_rp  # “what am I publishing most recently?”

    # Reuse receipts dir if you keep it shared; otherwise it will just say none.
    last_file, last_status, last_checked, last_waited, hist, n3 = _load_last_receipt(cfg, target_rp, history_n)
    # For primary it is normal to have no receipts; don’t treat as scary.
    # Still include note for visibility.
    notes.extend(n3)

    snap = Snapshot(
        mode="primary",
        latest_restore_point=latest_rp,
        latest_ready=latest_ready,
        current_restore_point=current_rp,
        target_restore_point=target_rp,
        last_receipt_file=last_file,
        last_receipt_status=last_status,
        last_receipt_checked_at_utc=last_checked,
        last_receipt_waited_secs=last_waited,
        notes=notes,
    )
    return snap, hist


def render_prometheus(s: Snapshot, history: List[dict], metric_name: str) -> str:
    name = (metric_name or "whpg_dr_sync").strip()

    ok_statuses = {
        "stopped_at_target_all",
        "reached_then_shutdown",
        "reached_then_shutdown_best_effort",
        "fast_forward_state_only",
    }
    bad_statuses = {"timeout", "no_candidate", "archive_gap", "error"}

    if s.last_receipt_status in ok_statuses:
        code = 1
    elif s.last_receipt_status in bad_statuses:
        code = -1
    else:
        code = 0

    latest_ready_val = -1
    if s.latest_ready is True:
        latest_ready_val = 1
    elif s.latest_ready is False:
        latest_ready_val = 0

    drift = 0
    if s.mode == "dr":
        if s.current_restore_point not in ("-", "") and s.target_restore_point not in ("-", ""):
            drift = 1 if s.current_restore_point != s.target_restore_point else 0

    lines: List[str] = []
    lines.append(f"# HELP {name}_status_code 1=ok,0=unknown,-1=bad")
    lines.append(f"# TYPE {name}_status_code gauge")
    lines.append(f'{name}_status_code{{mode="{s.mode}",status="{s.last_receipt_status}"}} {code}')

    lines.append(f"# HELP {name}_latest_ready Whether LATEST manifest is ready (1=true,0=false,-1=unknown)")
    lines.append(f"# TYPE {name}_latest_ready gauge")
    lines.append(f'{name}_latest_ready{{mode="{s.mode}"}} {latest_ready_val}')

    lines.append(f"# HELP {name}_drift Whether current restore point differs from target (1=yes,0=no) (dr only)")
    lines.append(f"# TYPE {name}_drift gauge")
    lines.append(f'{name}_drift{{mode="{s.mode}"}} {drift}')

    if s.last_receipt_waited_secs is not None:
        lines.append(f"# HELP {name}_last_waited_seconds waited_secs from last receipt (if present)")
        lines.append(f"# TYPE {name}_last_waited_seconds gauge")
        lines.append(f'{name}_last_waited_seconds{{mode="{s.mode}"}} {int(s.last_receipt_waited_secs)}')

    if history:
        ok = 0
        timeout = 0
        other = 0
        for r in history:
            st = _safe_str(r.get("status"), "")
            if st in ok_statuses:
                ok += 1
            elif st == "timeout":
                timeout += 1
            else:
                other += 1

        lines.append(f"# HELP {name}_receipts_recent_count Counts of recent receipt statuses")
        lines.append(f"# TYPE {name}_receipts_recent_count gauge")
        lines.append(f'{name}_receipts_recent_count{{mode="{s.mode}",kind="ok"}} {ok}')
        lines.append(f'{name}_receipts_recent_count{{mode="{s.mode}",kind="timeout"}} {timeout}')
        lines.append(f'{name}_receipts_recent_count{{mode="{s.mode}",kind="other"}} {other}')

    return "\n".join(lines) + "\n"


def render_table(s: Snapshot, history: List[dict], include_history: bool) -> str:
    rows = [
        ["field", "value"],
        ["mode", s.mode],
        ["latest.restore_point", s.latest_restore_point],
        ["latest.ready", "-" if s.latest_ready is None else ("true" if s.latest_ready else "false")],
        ["current.restore_point", s.current_restore_point],
        ["target.restore_point", s.target_restore_point],
        ["last.receipt.file", s.last_receipt_file],
        ["last.receipt.status", s.last_receipt_status],
        ["last.receipt.checked_at_utc", s.last_receipt_checked_at_utc],
        ["last.receipt.waited_secs", "-" if s.last_receipt_waited_secs is None else str(s.last_receipt_waited_secs)],
    ]
    out = _table(rows)

    if s.notes:
        out += "\n\nNOTES:\n" + "\n".join(f"- {n}" for n in s.notes)

    if include_history:
        out += "\n\nRECENT RECEIPTS:\n"
        if not history:
            out += "(none)\n"
        else:
            hrows = [["checked_at_utc", "status", "current", "target", "file"]]
            for r in history:
                hrows.append(
                    [
                        _fmt_ts(r.get("checked_at_utc")),
                        _safe_str(r.get("status"), "-"),
                        _safe_str(r.get("current_restore_point"), "-"),
                        _safe_str(r.get("target_restore_point"), "-"),
                        _safe_str(r.get("_file"), "-"),
                    ]
                )
            out += _table(hrows)

    return out + "\n"


def render_json(s: Snapshot, history: List[dict], include_history: bool) -> str:
    obj = {
        "mode": s.mode,
        "latest": {"restore_point": s.latest_restore_point, "ready": s.latest_ready},
        "state": {"current_restore_point": s.current_restore_point, "target_restore_point": s.target_restore_point},
        "last_receipt": {
            "file": s.last_receipt_file,
            "status": s.last_receipt_status,
            "checked_at_utc": s.last_receipt_checked_at_utc,
            "waited_secs": s.last_receipt_waited_secs,
        },
        "notes": s.notes,
    }
    if include_history:
        obj["recent_receipts"] = history
    return json.dumps(obj, indent=2) + "\n"


def render_status(
    cfg,
    fmt: str = "table",
    include_history: bool = False,
    history_n: int = 10,
    metric_name: str = "whpg_dr_sync",
    mode: str = "dr",
) -> str:
    mode = (mode or "dr").strip().lower()
    if mode == "primary":
        snap, hist = collect_primary(cfg, history_n=history_n)
    else:
        snap, hist = collect_dr(cfg, history_n=history_n)

    if fmt == "prometheus":
        return render_prometheus(snap, hist, metric_name=metric_name)

    if fmt == "json":
        return render_json(snap, hist, include_history=include_history)

    return render_table(snap, hist, include_history=include_history)
