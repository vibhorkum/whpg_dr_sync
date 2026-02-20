"""
Health monitoring for whpg_dr_sync.

Provides continuous health checks for both primary and DR clusters,
with support for one-shot checks and daemon mode monitoring.
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

from .common import atomic_write_json, check_stop, run, utc_now_iso, with_retry
from .config import Config

logger = logging.getLogger(__name__)


@dataclass
class InstanceHealth:
    """Health status for a single instance."""
    gp_segment_id: int
    host: str
    port: int
    reachable: bool
    is_recovering: bool
    replay_lsn: Optional[str]
    disk_free_bytes: Optional[int]
    disk_free_pct: Optional[float]
    error: Optional[str]
    checked_at: str


@dataclass
class PrimaryHealth:
    """Health status for primary cluster."""
    reachable: bool
    current_lsn: Optional[str]
    archive_command_ok: bool
    last_archived_wal: Optional[str]
    last_archived_time: Optional[str]
    archiver_failed_count: int
    error: Optional[str]
    checked_at: str


@dataclass
class ArchiveHealth:
    """Health status for WAL archive."""
    accessible: bool
    latest_wal_file: Optional[str]
    latest_wal_time: Optional[str]
    total_files: int
    total_size_bytes: int
    error: Optional[str]
    checked_at: str


@dataclass
class HealthStatus:
    """Overall health status."""
    timestamp: str
    overall_health: str  # "healthy", "degraded", "critical"
    primary: Optional[PrimaryHealth]
    dr_instances: Dict[int, InstanceHealth]
    archive: Optional[ArchiveHealth]
    wal_lag_bytes: Dict[int, int]
    sync_lag_seconds: float
    last_successful_sync: Optional[str]
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def _psql_check(host: str, port: int, user: str, db: str, sql: str, timeout: int = 10) -> Tuple[bool, Optional[str], Optional[str]]:
    """Execute SQL and return (success, result, error)."""
    env = os.environ.copy()
    try:
        p = subprocess.run(
            ["psql", "-qtA", "-h", host, "-p", str(port), "-U", user, "-d", db, "-c", sql],
            text=True,
            capture_output=True,
            env=env,
            timeout=timeout,
        )
        if p.returncode == 0:
            return True, (p.stdout or "").strip(), None
        return False, None, (p.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return False, None, f"Connection timeout after {timeout}s"
    except Exception as e:
        return False, None, str(e)


def _ssh_check(host: str, cmd: str, timeout: int = 10) -> Tuple[bool, Optional[str], Optional[str]]:
    """Execute SSH command and return (success, result, error)."""
    try:
        p = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes", host, cmd],
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if p.returncode == 0:
            return True, (p.stdout or "").strip(), None
        return False, None, (p.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return False, None, f"SSH timeout after {timeout}s"
    except Exception as e:
        return False, None, str(e)


def check_primary_health(cfg: Config) -> PrimaryHealth:
    """Check primary cluster health."""
    checked_at = utc_now_iso()

    # Check basic connectivity
    ok, result, error = _psql_check(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        "SELECT 1;"
    )

    if not ok:
        return PrimaryHealth(
            reachable=False,
            current_lsn=None,
            archive_command_ok=False,
            last_archived_wal=None,
            last_archived_time=None,
            archiver_failed_count=0,
            error=error,
            checked_at=checked_at,
        )

    # Get current LSN
    ok, current_lsn, _ = _psql_check(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        "SELECT pg_current_wal_lsn();"
    )

    # Get archiver status
    ok, archiver_json, _ = _psql_check(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        """
        SELECT json_build_object(
            'archived_count', archived_count,
            'last_archived_wal', last_archived_wal,
            'last_archived_time', last_archived_time::text,
            'failed_count', failed_count
        )::text FROM pg_stat_archiver;
        """
    )

    archiver_data = {}
    if archiver_json:
        try:
            archiver_data = json.loads(archiver_json)
        except json.JSONDecodeError:
            pass

    return PrimaryHealth(
        reachable=True,
        current_lsn=current_lsn,
        archive_command_ok=archiver_data.get("failed_count", 0) == 0,
        last_archived_wal=archiver_data.get("last_archived_wal"),
        last_archived_time=archiver_data.get("last_archived_time"),
        archiver_failed_count=archiver_data.get("failed_count", 0),
        error=None,
        checked_at=checked_at,
    )


def check_dr_instance_health(
    host: str,
    port: int,
    gp_segment_id: int,
    user: str,
    db: str,
    data_dir: str,
    is_local: bool,
) -> InstanceHealth:
    """Check health of a single DR instance."""
    checked_at = utc_now_iso()

    # Check SQL connectivity (utility mode for segments)
    env_opts = "-c gp_session_role=utility" if gp_segment_id >= 0 else ""

    ok, result, error = _psql_check(host, port, user, db, "SELECT 1;")

    if not ok:
        # Instance might be down (expected during recovery pause)
        return InstanceHealth(
            gp_segment_id=gp_segment_id,
            host=host,
            port=port,
            reachable=False,
            is_recovering=False,
            replay_lsn=None,
            disk_free_bytes=None,
            disk_free_pct=None,
            error=error,
            checked_at=checked_at,
        )

    # Check if in recovery
    ok, is_recovery, _ = _psql_check(host, port, user, db, "SELECT pg_is_in_recovery();")
    is_recovering = is_recovery == "t" if ok else False

    # Get replay LSN
    replay_lsn = None
    if is_recovering:
        ok, replay_lsn, _ = _psql_check(host, port, user, db, "SELECT pg_last_wal_replay_lsn();")

    # Check disk space
    disk_free_bytes = None
    disk_free_pct = None

    disk_cmd = f"df -B1 {data_dir} | tail -1 | awk '{{print $4, $5}}'"
    if is_local:
        ok, disk_out, _ = True, None, None
        try:
            p = subprocess.run(["bash", "-c", disk_cmd], capture_output=True, text=True, timeout=10)
            if p.returncode == 0:
                disk_out = p.stdout.strip()
        except Exception:
            pass
    else:
        ok, disk_out, _ = _ssh_check(host, disk_cmd)

    if disk_out:
        parts = disk_out.split()
        if len(parts) >= 2:
            try:
                disk_free_bytes = int(parts[0])
                disk_free_pct = 100.0 - float(parts[1].rstrip('%'))
            except (ValueError, IndexError):
                pass

    return InstanceHealth(
        gp_segment_id=gp_segment_id,
        host=host,
        port=port,
        reachable=True,
        is_recovering=is_recovering,
        replay_lsn=replay_lsn,
        disk_free_bytes=disk_free_bytes,
        disk_free_pct=disk_free_pct,
        error=None,
        checked_at=checked_at,
    )


def check_archive_health(cfg: Config) -> ArchiveHealth:
    """Check WAL archive health."""
    checked_at = utc_now_iso()
    archive_dir = Path(cfg.archive_dir)

    if not archive_dir.exists():
        return ArchiveHealth(
            accessible=False,
            latest_wal_file=None,
            latest_wal_time=None,
            total_files=0,
            total_size_bytes=0,
            error=f"Archive directory not found: {archive_dir}",
            checked_at=checked_at,
        )

    try:
        wal_files = sorted(archive_dir.glob("0000*"), key=lambda p: p.stat().st_mtime, reverse=True)

        total_files = len(wal_files)
        total_size = sum(f.stat().st_size for f in wal_files[:1000])  # Limit for performance

        latest_file = None
        latest_time = None
        if wal_files:
            latest_file = wal_files[0].name
            latest_time = datetime.fromtimestamp(
                wal_files[0].stat().st_mtime, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

        return ArchiveHealth(
            accessible=True,
            latest_wal_file=latest_file,
            latest_wal_time=latest_time,
            total_files=total_files,
            total_size_bytes=total_size,
            error=None,
            checked_at=checked_at,
        )
    except Exception as e:
        return ArchiveHealth(
            accessible=False,
            latest_wal_file=None,
            latest_wal_time=None,
            total_files=0,
            total_size_bytes=0,
            error=str(e),
            checked_at=checked_at,
        )


def _lsn_to_int(lsn: str) -> int:
    """Convert LSN string to integer."""
    if not lsn or lsn == "0/0":
        return 0
    if "/" not in lsn:
        return 0
    x, y = lsn.split("/", 1)
    return (int(x, 16) << 32) + int(y, 16)


def calculate_wal_lag(primary_lsn: Optional[str], dr_lsn: Optional[str]) -> int:
    """Calculate WAL lag in bytes between primary and DR."""
    if not primary_lsn or not dr_lsn:
        return -1  # Unknown

    primary_int = _lsn_to_int(primary_lsn)
    dr_int = _lsn_to_int(dr_lsn)

    return max(0, primary_int - dr_int)


def get_last_successful_sync(cfg: Config) -> Tuple[Optional[str], float]:
    """
    Get last successful sync time and calculate lag.

    Returns:
        (restore_point_name, lag_seconds)
    """
    receipts_dir = Path(cfg.receipts_dir)
    if not receipts_dir.exists():
        return None, -1

    receipts = sorted(
        receipts_dir.glob("*.receipt.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    for receipt_path in receipts[:10]:
        try:
            data = json.loads(receipt_path.read_text())
            if data.get("status") in ("success_recovery_point_validated", "stopped_at_target_all"):
                checked_at = data.get("checked_at_utc", "")
                if checked_at:
                    # Parse timestamp and calculate lag
                    try:
                        sync_time = datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%SZ")
                        sync_time = sync_time.replace(tzinfo=timezone.utc)
                        lag = (datetime.now(timezone.utc) - sync_time).total_seconds()
                        return data.get("target_restore_point"), lag
                    except ValueError:
                        pass
        except (json.JSONDecodeError, OSError):
            continue

    return None, -1


def perform_health_check(cfg: Config, check_primary: bool = True) -> HealthStatus:
    """
    Perform comprehensive health check.

    Args:
        cfg: Configuration object
        check_primary: Whether to check primary (may be False during failover)

    Returns:
        HealthStatus with all health information
    """
    timestamp = utc_now_iso()
    warnings: List[str] = []
    errors: List[str] = []

    # Check primary
    primary_health = None
    if check_primary:
        primary_health = check_primary_health(cfg)
        if not primary_health.reachable:
            errors.append(f"Primary unreachable: {primary_health.error}")
        elif primary_health.archiver_failed_count > 0:
            warnings.append(f"Primary archiver has {primary_health.archiver_failed_count} failed attempts")

    # Check DR instances
    dr_instances: Dict[int, InstanceHealth] = {}
    for inst in cfg.instances:
        health = check_dr_instance_health(
            host=inst.host,
            port=inst.port,
            gp_segment_id=inst.gp_segment_id,
            user=cfg.primary_user,
            db=cfg.primary_db,
            data_dir=inst.data_dir,
            is_local=inst.is_local,
        )
        dr_instances[inst.gp_segment_id] = health

        if health.error and "timeout" in health.error.lower():
            errors.append(f"DR instance {inst.gp_segment_id} timeout: {health.error}")

        if health.disk_free_pct is not None and health.disk_free_pct < 10:
            warnings.append(f"DR instance {inst.gp_segment_id} low disk: {health.disk_free_pct:.1f}% free")

    # Check archive
    archive_health = check_archive_health(cfg)
    if not archive_health.accessible:
        errors.append(f"Archive inaccessible: {archive_health.error}")

    # Calculate WAL lag per instance
    wal_lag: Dict[int, int] = {}
    if primary_health and primary_health.current_lsn:
        for seg_id, inst_health in dr_instances.items():
            if inst_health.replay_lsn:
                lag = calculate_wal_lag(primary_health.current_lsn, inst_health.replay_lsn)
                wal_lag[seg_id] = lag
                if lag > 1024 * 1024 * 1024:  # 1GB lag
                    warnings.append(f"Instance {seg_id} has {lag / 1024 / 1024:.1f}MB WAL lag")

    # Get sync lag
    last_sync, sync_lag = get_last_successful_sync(cfg)
    if sync_lag > 3600:  # 1 hour
        warnings.append(f"Last successful sync was {sync_lag / 3600:.1f} hours ago")
    elif sync_lag < 0:
        warnings.append("No successful sync found in receipts")

    # Determine overall health
    if errors:
        overall_health = "critical"
    elif warnings:
        overall_health = "degraded"
    else:
        overall_health = "healthy"

    return HealthStatus(
        timestamp=timestamp,
        overall_health=overall_health,
        primary=primary_health,
        dr_instances=dr_instances,
        archive=archive_health,
        wal_lag_bytes=wal_lag,
        sync_lag_seconds=sync_lag,
        last_successful_sync=last_sync,
        warnings=warnings,
        errors=errors,
    )


def render_health_table(status: HealthStatus) -> str:
    """Render health status as human-readable table."""
    lines = []

    # Header
    lines.append(f"Health Check: {status.timestamp}")
    lines.append(f"Overall Status: {status.overall_health.upper()}")
    lines.append("")

    # Primary
    if status.primary:
        lines.append("PRIMARY:")
        lines.append(f"  Reachable:     {status.primary.reachable}")
        lines.append(f"  Current LSN:   {status.primary.current_lsn or '-'}")
        lines.append(f"  Archiver OK:   {status.primary.archive_command_ok}")
        lines.append(f"  Last Archived: {status.primary.last_archived_wal or '-'}")
        if status.primary.error:
            lines.append(f"  Error:         {status.primary.error}")
        lines.append("")

    # DR Instances
    lines.append("DR INSTANCES:")
    lines.append(f"  {'Seg':<5} {'Host':<25} {'Reachable':<10} {'Recovery':<10} {'Replay LSN':<15} {'Disk Free':<10}")
    lines.append(f"  {'-'*5:<5} {'-'*25:<25} {'-'*10:<10} {'-'*10:<10} {'-'*15:<15} {'-'*10:<10}")

    for seg_id in sorted(status.dr_instances.keys()):
        inst = status.dr_instances[seg_id]
        disk_str = f"{inst.disk_free_pct:.1f}%" if inst.disk_free_pct else "-"
        lines.append(
            f"  {seg_id:<5} {inst.host:<25} {str(inst.reachable):<10} "
            f"{str(inst.is_recovering):<10} {inst.replay_lsn or '-':<15} {disk_str:<10}"
        )
    lines.append("")

    # Archive
    if status.archive:
        lines.append("ARCHIVE:")
        lines.append(f"  Accessible:    {status.archive.accessible}")
        lines.append(f"  Latest WAL:    {status.archive.latest_wal_file or '-'}")
        lines.append(f"  Total Files:   {status.archive.total_files}")
        if status.archive.error:
            lines.append(f"  Error:         {status.archive.error}")
        lines.append("")

    # Sync Status
    lines.append("SYNC STATUS:")
    lines.append(f"  Last Sync:     {status.last_successful_sync or '-'}")
    if status.sync_lag_seconds >= 0:
        lines.append(f"  Sync Lag:      {status.sync_lag_seconds:.0f}s ({status.sync_lag_seconds/60:.1f}m)")
    else:
        lines.append(f"  Sync Lag:      unknown")
    lines.append("")

    # Warnings and Errors
    if status.warnings:
        lines.append("WARNINGS:")
        for w in status.warnings:
            lines.append(f"  - {w}")
        lines.append("")

    if status.errors:
        lines.append("ERRORS:")
        for e in status.errors:
            lines.append(f"  - {e}")
        lines.append("")

    return "\n".join(lines)


def render_health_json(status: HealthStatus) -> str:
    """Render health status as JSON."""
    def to_dict(obj):
        if hasattr(obj, '__dataclass_fields__'):
            return {k: to_dict(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {str(k): to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [to_dict(v) for v in obj]
        return obj

    return json.dumps(to_dict(status), indent=2)


def render_health_prometheus(status: HealthStatus, metric_name: str = "whpg_dr_sync") -> str:
    """Render health status as Prometheus metrics."""
    lines = []
    name = metric_name

    # Overall health (1=healthy, 0=degraded, -1=critical)
    health_code = {"healthy": 1, "degraded": 0, "critical": -1}.get(status.overall_health, 0)
    lines.append(f"# HELP {name}_health_status Overall health status (1=healthy, 0=degraded, -1=critical)")
    lines.append(f"# TYPE {name}_health_status gauge")
    lines.append(f'{name}_health_status {health_code}')

    # Primary reachable
    if status.primary:
        lines.append(f"# HELP {name}_primary_reachable Whether primary is reachable (1=yes, 0=no)")
        lines.append(f"# TYPE {name}_primary_reachable gauge")
        lines.append(f'{name}_primary_reachable {1 if status.primary.reachable else 0}')

        lines.append(f"# HELP {name}_primary_archiver_failures Number of archiver failures")
        lines.append(f"# TYPE {name}_primary_archiver_failures gauge")
        lines.append(f'{name}_primary_archiver_failures {status.primary.archiver_failed_count}')

    # DR instances
    lines.append(f"# HELP {name}_dr_instance_reachable Whether DR instance is reachable")
    lines.append(f"# TYPE {name}_dr_instance_reachable gauge")
    for seg_id, inst in status.dr_instances.items():
        lines.append(f'{name}_dr_instance_reachable{{segment_id="{seg_id}"}} {1 if inst.reachable else 0}')

    lines.append(f"# HELP {name}_dr_instance_recovering Whether DR instance is in recovery")
    lines.append(f"# TYPE {name}_dr_instance_recovering gauge")
    for seg_id, inst in status.dr_instances.items():
        lines.append(f'{name}_dr_instance_recovering{{segment_id="{seg_id}"}} {1 if inst.is_recovering else 0}')

    # Disk free
    lines.append(f"# HELP {name}_dr_instance_disk_free_pct Disk free percentage")
    lines.append(f"# TYPE {name}_dr_instance_disk_free_pct gauge")
    for seg_id, inst in status.dr_instances.items():
        if inst.disk_free_pct is not None:
            lines.append(f'{name}_dr_instance_disk_free_pct{{segment_id="{seg_id}"}} {inst.disk_free_pct:.2f}')

    # WAL lag
    lines.append(f"# HELP {name}_wal_lag_bytes WAL lag in bytes per instance")
    lines.append(f"# TYPE {name}_wal_lag_bytes gauge")
    for seg_id, lag in status.wal_lag_bytes.items():
        lines.append(f'{name}_wal_lag_bytes{{segment_id="{seg_id}"}} {lag}')

    # Sync lag
    lines.append(f"# HELP {name}_sync_lag_seconds Time since last successful sync")
    lines.append(f"# TYPE {name}_sync_lag_seconds gauge")
    lines.append(f'{name}_sync_lag_seconds {status.sync_lag_seconds if status.sync_lag_seconds >= 0 else -1}')

    # Archive
    if status.archive:
        lines.append(f"# HELP {name}_archive_accessible Whether archive is accessible")
        lines.append(f"# TYPE {name}_archive_accessible gauge")
        lines.append(f'{name}_archive_accessible {1 if status.archive.accessible else 0}')

        lines.append(f"# HELP {name}_archive_files_total Total WAL files in archive")
        lines.append(f"# TYPE {name}_archive_files_total gauge")
        lines.append(f'{name}_archive_files_total {status.archive.total_files}')

    return "\n".join(lines) + "\n"


def health_daemon(cfg: Config, interval_secs: int = 60, output_file: Optional[str] = None) -> int:
    """
    Run health check daemon.

    Args:
        cfg: Configuration
        interval_secs: Check interval in seconds
        output_file: Optional file to write Prometheus metrics

    Returns:
        Exit code
    """
    print(f"[health] Starting health monitor (interval={interval_secs}s)")

    try:
        while True:
            check_stop()

            status = perform_health_check(cfg)

            # Print summary
            print(f"[health] {status.timestamp} status={status.overall_health} "
                  f"warnings={len(status.warnings)} errors={len(status.errors)}")

            # Write metrics file if specified
            if output_file:
                metrics = render_health_prometheus(status)
                Path(output_file).write_text(metrics)

            # Sleep with interrupt checks
            slept = 0
            while slept < interval_secs:
                check_stop()
                time.sleep(1)
                slept += 1

    except KeyboardInterrupt:
        print("[health] Stopped")
        return 0
    except Exception as e:
        print(f"[health] Error: {e}")
        return 1
