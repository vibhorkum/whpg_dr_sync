"""
Recovery Point Objective (RPO) tracking for whpg_dr_sync.

Provides RPO metrics calculation, tracking, and alerting capabilities
to ensure DR meets compliance requirements.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .common import atomic_write_json, check_stop, utc_now_iso
from .config import Config

logger = logging.getLogger(__name__)


@dataclass
class RPOMetrics:
    """RPO metrics for the DR cluster."""
    timestamp: str

    # Time-based metrics
    current_lag_seconds: float  # Time since last successful sync
    rpo_target_seconds: int  # Configured RPO target
    rpo_target_met: bool  # Whether RPO target is met

    # WAL-based metrics
    wal_lag_bytes: Dict[int, int]  # Per-segment WAL lag in bytes
    total_wal_lag_bytes: int  # Total WAL lag across all segments
    max_wal_lag_bytes: int  # Maximum lag on any segment

    # Recovery metrics
    estimated_recovery_seconds: float  # Estimated time to catch up
    recovery_rate_bytes_per_sec: float  # Current recovery rate

    # Sync history
    last_successful_sync: Optional[str]  # Restore point name
    last_sync_time: Optional[str]  # UTC timestamp
    syncs_in_last_hour: int
    syncs_in_last_24h: int

    # Status
    status: str  # "ok", "warning", "violation"
    warnings: List[str] = field(default_factory=list)


@dataclass
class RPOConfig:
    """RPO-specific configuration."""
    target_seconds: int = 300  # 5 minute default RPO
    warning_threshold_seconds: int = 180  # Warn at 3 minutes
    violation_threshold_seconds: int = 600  # Violation at 10 minutes
    estimated_recovery_rate: int = 50 * 1024 * 1024  # 50 MB/s default


def _lsn_to_int(lsn: str) -> int:
    """Convert LSN string to integer."""
    if not lsn or lsn == "0/0":
        return 0
    if "/" not in lsn:
        return 0
    x, y = lsn.split("/", 1)
    return (int(x, 16) << 32) + int(y, 16)


def _psql_query(host: str, port: int, user: str, db: str, sql: str) -> Optional[str]:
    """Execute SQL query and return result."""
    try:
        p = subprocess.run(
            ["psql", "-qtA", "-h", host, "-p", str(port), "-U", user, "-d", db, "-c", sql],
            text=True,
            capture_output=True,
            timeout=30,
        )
        if p.returncode == 0:
            return (p.stdout or "").strip()
        return None
    except Exception:
        return None


def get_primary_lsn(cfg: Config) -> Optional[str]:
    """Get current WAL LSN from primary."""
    return _psql_query(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        "SELECT pg_current_wal_lsn();"
    )


def get_dr_replay_lsns(cfg: Config) -> Dict[int, str]:
    """Get replay LSNs from all DR instances."""
    lsns: Dict[int, str] = {}

    for inst in cfg.instances:
        env = os.environ.copy()
        if inst.gp_segment_id >= 0:
            env["PGOPTIONS"] = "-c gp_session_role=utility"

        try:
            p = subprocess.run(
                ["psql", "-qtA", "-h", inst.host, "-p", str(inst.port),
                 "-U", cfg.primary_user, "-d", cfg.primary_db,
                 "-c", "SELECT pg_last_wal_replay_lsn();"],
                text=True,
                capture_output=True,
                timeout=30,
                env=env,
            )
            if p.returncode == 0:
                lsn = (p.stdout or "").strip()
                if lsn:
                    lsns[inst.gp_segment_id] = lsn
        except Exception:
            pass

    return lsns


def calculate_wal_lag(primary_lsn: str, dr_lsns: Dict[int, str]) -> Dict[int, int]:
    """Calculate WAL lag in bytes for each segment."""
    primary_int = _lsn_to_int(primary_lsn)
    lag: Dict[int, int] = {}

    for seg_id, dr_lsn in dr_lsns.items():
        dr_int = _lsn_to_int(dr_lsn)
        lag[seg_id] = max(0, primary_int - dr_int)

    return lag


def get_sync_history(cfg: Config, hours: int = 24) -> List[Dict[str, Any]]:
    """Get sync history from receipts."""
    receipts_dir = Path(cfg.receipts_dir)
    if not receipts_dir.exists():
        return []

    cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
    history: List[Dict[str, Any]] = []

    for receipt_path in receipts_dir.glob("*.receipt.json"):
        try:
            if receipt_path.stat().st_mtime < cutoff:
                continue

            data = json.loads(receipt_path.read_text())
            if data.get("status") in ("success_recovery_point_validated", "stopped_at_target_all"):
                history.append({
                    "restore_point": data.get("target_restore_point"),
                    "checked_at": data.get("checked_at_utc"),
                    "status": data.get("status"),
                    "mtime": receipt_path.stat().st_mtime,
                })
        except (json.JSONDecodeError, OSError):
            continue

    return sorted(history, key=lambda x: x.get("mtime", 0), reverse=True)


def get_last_successful_sync(cfg: Config) -> Tuple[Optional[str], Optional[str], float]:
    """
    Get last successful sync information.

    Returns:
        (restore_point_name, timestamp, lag_seconds)
    """
    history = get_sync_history(cfg, hours=168)  # 1 week

    for entry in history:
        checked_at = entry.get("checked_at")
        if checked_at:
            try:
                sync_time = datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%SZ")
                sync_time = sync_time.replace(tzinfo=timezone.utc)
                lag = (datetime.now(timezone.utc) - sync_time).total_seconds()
                return entry.get("restore_point"), checked_at, lag
            except ValueError:
                continue

    return None, None, -1


def calculate_rpo_metrics(cfg: Config, rpo_config: Optional[RPOConfig] = None) -> RPOMetrics:
    """
    Calculate comprehensive RPO metrics.

    Args:
        cfg: Main configuration
        rpo_config: Optional RPO-specific configuration

    Returns:
        RPOMetrics with all calculated values
    """
    if rpo_config is None:
        # Load from config behavior section or use defaults
        beh = cfg.raw.get("behavior", {})
        rpo_config = RPOConfig(
            target_seconds=beh.get("rpo_target_seconds", 300),
            warning_threshold_seconds=beh.get("rpo_warning_seconds", 180),
            violation_threshold_seconds=beh.get("rpo_violation_seconds", 600),
            estimated_recovery_rate=beh.get("estimated_recovery_rate", 50 * 1024 * 1024),
        )

    timestamp = utc_now_iso()
    warnings: List[str] = []

    # Get sync history
    last_rp, last_time, current_lag = get_last_successful_sync(cfg)

    # Get WAL positions
    primary_lsn = get_primary_lsn(cfg)
    dr_lsns = get_dr_replay_lsns(cfg)

    # Calculate WAL lag
    wal_lag: Dict[int, int] = {}
    total_lag = 0
    max_lag = 0

    if primary_lsn and dr_lsns:
        wal_lag = calculate_wal_lag(primary_lsn, dr_lsns)
        if wal_lag:
            total_lag = sum(wal_lag.values())
            max_lag = max(wal_lag.values())

    # Estimate recovery time
    estimated_recovery = 0.0
    recovery_rate = float(rpo_config.estimated_recovery_rate)

    if max_lag > 0 and recovery_rate > 0:
        estimated_recovery = max_lag / recovery_rate

    # Count syncs in time windows
    history = get_sync_history(cfg, hours=24)
    now = datetime.now(timezone.utc).timestamp()

    syncs_1h = sum(1 for h in history if now - h.get("mtime", 0) <= 3600)
    syncs_24h = len(history)

    # Determine RPO status
    rpo_target_met = current_lag >= 0 and current_lag <= rpo_config.target_seconds

    if current_lag < 0:
        status = "unknown"
        warnings.append("No successful sync found - RPO status unknown")
    elif current_lag > rpo_config.violation_threshold_seconds:
        status = "violation"
        warnings.append(f"RPO VIOLATION: {current_lag:.0f}s > {rpo_config.violation_threshold_seconds}s threshold")
    elif current_lag > rpo_config.warning_threshold_seconds:
        status = "warning"
        warnings.append(f"RPO warning: {current_lag:.0f}s approaching target of {rpo_config.target_seconds}s")
    else:
        status = "ok"

    # Add warnings for WAL lag
    if max_lag > 1024 * 1024 * 1024:  # 1GB
        warnings.append(f"High WAL lag: {max_lag / 1024 / 1024:.1f}MB")

    if not primary_lsn:
        warnings.append("Could not get primary LSN - primary may be unreachable")

    if not dr_lsns:
        warnings.append("Could not get any DR replay LSNs - DR instances may be down")

    return RPOMetrics(
        timestamp=timestamp,
        current_lag_seconds=current_lag if current_lag >= 0 else -1,
        rpo_target_seconds=rpo_config.target_seconds,
        rpo_target_met=rpo_target_met,
        wal_lag_bytes=wal_lag,
        total_wal_lag_bytes=total_lag,
        max_wal_lag_bytes=max_lag,
        estimated_recovery_seconds=estimated_recovery,
        recovery_rate_bytes_per_sec=recovery_rate,
        last_successful_sync=last_rp,
        last_sync_time=last_time,
        syncs_in_last_hour=syncs_1h,
        syncs_in_last_24h=syncs_24h,
        status=status,
        warnings=warnings,
    )


def render_rpo_table(metrics: RPOMetrics) -> str:
    """Render RPO metrics as human-readable table."""
    lines = []

    lines.append(f"RPO Metrics: {metrics.timestamp}")
    lines.append(f"Status: {metrics.status.upper()}")
    lines.append("")

    # Time-based metrics
    lines.append("TIME-BASED METRICS:")
    if metrics.current_lag_seconds >= 0:
        lines.append(f"  Current Lag:    {metrics.current_lag_seconds:.0f}s ({metrics.current_lag_seconds/60:.1f}m)")
    else:
        lines.append(f"  Current Lag:    unknown")
    lines.append(f"  RPO Target:     {metrics.rpo_target_seconds}s ({metrics.rpo_target_seconds/60:.1f}m)")
    lines.append(f"  Target Met:     {metrics.rpo_target_met}")
    lines.append("")

    # WAL-based metrics
    lines.append("WAL-BASED METRICS:")
    lines.append(f"  Max WAL Lag:    {metrics.max_wal_lag_bytes / 1024 / 1024:.2f} MB")
    lines.append(f"  Total WAL Lag:  {metrics.total_wal_lag_bytes / 1024 / 1024:.2f} MB")
    lines.append("")

    if metrics.wal_lag_bytes:
        lines.append("  Per-Segment WAL Lag:")
        for seg_id in sorted(metrics.wal_lag_bytes.keys()):
            lag = metrics.wal_lag_bytes[seg_id]
            lines.append(f"    Segment {seg_id:>3}: {lag / 1024 / 1024:.2f} MB")
        lines.append("")

    # Recovery estimates
    lines.append("RECOVERY ESTIMATES:")
    lines.append(f"  Est. Recovery Time:  {metrics.estimated_recovery_seconds:.0f}s ({metrics.estimated_recovery_seconds/60:.1f}m)")
    lines.append(f"  Recovery Rate:       {metrics.recovery_rate_bytes_per_sec / 1024 / 1024:.1f} MB/s")
    lines.append("")

    # Sync history
    lines.append("SYNC HISTORY:")
    lines.append(f"  Last Sync:          {metrics.last_successful_sync or '-'}")
    lines.append(f"  Last Sync Time:     {metrics.last_sync_time or '-'}")
    lines.append(f"  Syncs (1h):         {metrics.syncs_in_last_hour}")
    lines.append(f"  Syncs (24h):        {metrics.syncs_in_last_24h}")
    lines.append("")

    # Warnings
    if metrics.warnings:
        lines.append("WARNINGS:")
        for w in metrics.warnings:
            lines.append(f"  - {w}")
        lines.append("")

    return "\n".join(lines)


def render_rpo_json(metrics: RPOMetrics) -> str:
    """Render RPO metrics as JSON."""
    def to_dict(obj):
        if hasattr(obj, '__dataclass_fields__'):
            return {k: to_dict(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {str(k): to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [to_dict(v) for v in obj]
        return obj

    return json.dumps(to_dict(metrics), indent=2)


def render_rpo_prometheus(metrics: RPOMetrics, metric_name: str = "whpg_dr_sync") -> str:
    """Render RPO metrics as Prometheus metrics."""
    lines = []
    name = metric_name

    # RPO status (1=ok, 0=warning, -1=violation, -2=unknown)
    status_code = {"ok": 1, "warning": 0, "violation": -1, "unknown": -2}.get(metrics.status, -2)
    lines.append(f"# HELP {name}_rpo_status RPO status (1=ok, 0=warning, -1=violation, -2=unknown)")
    lines.append(f"# TYPE {name}_rpo_status gauge")
    lines.append(f'{name}_rpo_status {status_code}')

    # Current lag
    lines.append(f"# HELP {name}_rpo_lag_seconds Current lag since last successful sync")
    lines.append(f"# TYPE {name}_rpo_lag_seconds gauge")
    lines.append(f'{name}_rpo_lag_seconds {metrics.current_lag_seconds}')

    # RPO target
    lines.append(f"# HELP {name}_rpo_target_seconds Configured RPO target")
    lines.append(f"# TYPE {name}_rpo_target_seconds gauge")
    lines.append(f'{name}_rpo_target_seconds {metrics.rpo_target_seconds}')

    # RPO target met
    lines.append(f"# HELP {name}_rpo_target_met Whether RPO target is met (1=yes, 0=no)")
    lines.append(f"# TYPE {name}_rpo_target_met gauge")
    lines.append(f'{name}_rpo_target_met {1 if metrics.rpo_target_met else 0}')

    # WAL lag
    lines.append(f"# HELP {name}_rpo_wal_lag_bytes WAL lag per segment in bytes")
    lines.append(f"# TYPE {name}_rpo_wal_lag_bytes gauge")
    for seg_id, lag in metrics.wal_lag_bytes.items():
        lines.append(f'{name}_rpo_wal_lag_bytes{{segment_id="{seg_id}"}} {lag}')

    lines.append(f"# HELP {name}_rpo_max_wal_lag_bytes Maximum WAL lag across all segments")
    lines.append(f"# TYPE {name}_rpo_max_wal_lag_bytes gauge")
    lines.append(f'{name}_rpo_max_wal_lag_bytes {metrics.max_wal_lag_bytes}')

    # Recovery estimates
    lines.append(f"# HELP {name}_rpo_estimated_recovery_seconds Estimated time to recover")
    lines.append(f"# TYPE {name}_rpo_estimated_recovery_seconds gauge")
    lines.append(f'{name}_rpo_estimated_recovery_seconds {metrics.estimated_recovery_seconds}')

    # Sync counts
    lines.append(f"# HELP {name}_rpo_syncs_1h Number of successful syncs in last hour")
    lines.append(f"# TYPE {name}_rpo_syncs_1h gauge")
    lines.append(f'{name}_rpo_syncs_1h {metrics.syncs_in_last_hour}')

    lines.append(f"# HELP {name}_rpo_syncs_24h Number of successful syncs in last 24 hours")
    lines.append(f"# TYPE {name}_rpo_syncs_24h gauge")
    lines.append(f'{name}_rpo_syncs_24h {metrics.syncs_in_last_24h}')

    return "\n".join(lines) + "\n"


def rpo_check(cfg: Config, fmt: str = "table") -> str:
    """
    Perform RPO check and return formatted output.

    Args:
        cfg: Configuration
        fmt: Output format ("table", "json", "prometheus")

    Returns:
        Formatted RPO metrics
    """
    metrics = calculate_rpo_metrics(cfg)

    if fmt == "json":
        return render_rpo_json(metrics)
    elif fmt == "prometheus":
        return render_rpo_prometheus(metrics)
    else:
        return render_rpo_table(metrics)
