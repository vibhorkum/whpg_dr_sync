"""
Switchover and failover operations for whpg_dr_sync.

Provides controlled switchover (graceful) and failover (emergency)
capabilities for promoting DR cluster to primary.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .common import atomic_write_json, check_stop, run, utc_now_iso
from .config import Config
from .health import check_primary_health, check_dr_instance_health
from .rpo import get_dr_replay_lsns, get_primary_lsn, get_last_successful_sync

logger = logging.getLogger(__name__)


class SwitchoverType(Enum):
    """Type of switchover operation."""
    GRACEFUL = "graceful"  # Primary is up, controlled handoff
    FAILOVER = "failover"  # Primary is down, emergency promotion


class PreflightStatus(Enum):
    """Status of preflight check."""
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class PreflightCheck:
    """Result of a single preflight check."""
    name: str
    status: PreflightStatus
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class SwitchoverPlan:
    """Plan for switchover operation."""
    switchover_type: SwitchoverType
    created_at: str

    # Preflight results
    preflight_passed: bool
    preflight_checks: List[PreflightCheck]

    # Current state
    primary_reachable: bool
    primary_lsn: Optional[str]
    dr_restore_point: Optional[str]
    dr_lsns: Dict[int, str]

    # Validation
    all_instances_at_same_point: bool
    wal_gap_detected: bool
    estimated_data_loss_bytes: int

    # Recommendations
    recommended_action: str
    warnings: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)


def _psql_check(host: str, port: int, user: str, db: str, sql: str) -> Tuple[bool, Optional[str]]:
    """Execute SQL and return (success, result)."""
    try:
        p = subprocess.run(
            ["psql", "-qtA", "-h", host, "-p", str(port), "-U", user, "-d", db, "-c", sql],
            text=True,
            capture_output=True,
            timeout=30,
        )
        if p.returncode == 0:
            return True, (p.stdout or "").strip()
        return False, None
    except Exception:
        return False, None


def _lsn_to_int(lsn: str) -> int:
    """Convert LSN string to integer."""
    if not lsn or lsn == "0/0":
        return 0
    if "/" not in lsn:
        return 0
    x, y = lsn.split("/", 1)
    return (int(x, 16) << 32) + int(y, 16)


def check_primary_connectivity(cfg: Config) -> PreflightCheck:
    """Check if primary is reachable."""
    ok, result = _psql_check(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        "SELECT 1;"
    )

    if ok:
        return PreflightCheck(
            name="primary_connectivity",
            status=PreflightStatus.PASS,
            message="Primary is reachable",
            details={"host": cfg.primary_host, "port": cfg.primary_port}
        )
    else:
        return PreflightCheck(
            name="primary_connectivity",
            status=PreflightStatus.FAIL,
            message="Primary is NOT reachable",
            details={"host": cfg.primary_host, "port": cfg.primary_port}
        )


def check_dr_instances_status(cfg: Config) -> Tuple[PreflightCheck, Dict[int, str]]:
    """Check DR instances are accessible and get their replay LSNs."""
    lsns = get_dr_replay_lsns(cfg)
    total = len(cfg.instances)
    reachable = len(lsns)

    if reachable == total:
        return PreflightCheck(
            name="dr_instances_status",
            status=PreflightStatus.PASS,
            message=f"All {total} DR instances are reachable",
            details={"total": total, "reachable": reachable, "lsns": lsns}
        ), lsns
    elif reachable > 0:
        return PreflightCheck(
            name="dr_instances_status",
            status=PreflightStatus.WARN,
            message=f"Only {reachable}/{total} DR instances are reachable",
            details={"total": total, "reachable": reachable, "lsns": lsns}
        ), lsns
    else:
        return PreflightCheck(
            name="dr_instances_status",
            status=PreflightStatus.FAIL,
            message="No DR instances are reachable",
            details={"total": total, "reachable": 0}
        ), lsns


def check_dr_consistency(cfg: Config) -> PreflightCheck:
    """Check all DR instances are at the same restore point."""
    state_file = Path(cfg.state_dir) / "current_restore_point.txt"

    if not state_file.exists():
        return PreflightCheck(
            name="dr_consistency",
            status=PreflightStatus.FAIL,
            message="No current restore point recorded",
            details={"state_file": str(state_file)}
        )

    current_rp = state_file.read_text().strip()
    if not current_rp:
        return PreflightCheck(
            name="dr_consistency",
            status=PreflightStatus.FAIL,
            message="Current restore point is empty",
        )

    # Check receipt for this restore point
    receipt_file = Path(cfg.receipts_dir) / f"{current_rp}.receipt.json"
    if receipt_file.exists():
        try:
            receipt = json.loads(receipt_file.read_text())
            status = receipt.get("status", "")
            if status in ("success_recovery_point_validated", "stopped_at_target_all"):
                return PreflightCheck(
                    name="dr_consistency",
                    status=PreflightStatus.PASS,
                    message=f"All DR instances validated at {current_rp}",
                    details={"restore_point": current_rp, "receipt_status": status}
                )
        except json.JSONDecodeError:
            pass

    return PreflightCheck(
        name="dr_consistency",
        status=PreflightStatus.WARN,
        message=f"DR consistency at {current_rp} not fully validated",
        details={"restore_point": current_rp}
    )


def check_wal_continuity(cfg: Config, primary_lsn: Optional[str], dr_lsns: Dict[int, str]) -> PreflightCheck:
    """Check for WAL gaps between primary and DR."""
    if not primary_lsn:
        return PreflightCheck(
            name="wal_continuity",
            status=PreflightStatus.SKIP,
            message="Cannot check WAL continuity - primary LSN unknown",
        )

    if not dr_lsns:
        return PreflightCheck(
            name="wal_continuity",
            status=PreflightStatus.FAIL,
            message="Cannot check WAL continuity - no DR LSNs available",
        )

    primary_int = _lsn_to_int(primary_lsn)
    max_lag = 0
    max_lag_seg = None

    for seg_id, dr_lsn in dr_lsns.items():
        dr_int = _lsn_to_int(dr_lsn)
        lag = primary_int - dr_int
        if lag > max_lag:
            max_lag = lag
            max_lag_seg = seg_id

    # Check archive for WAL files
    # This is a simplified check - full implementation would verify each WAL file
    if max_lag > 1024 * 1024 * 1024:  # > 1GB
        return PreflightCheck(
            name="wal_continuity",
            status=PreflightStatus.WARN,
            message=f"Large WAL lag detected: {max_lag / 1024 / 1024:.1f}MB on segment {max_lag_seg}",
            details={"max_lag_bytes": max_lag, "segment": max_lag_seg}
        )
    elif max_lag > 0:
        return PreflightCheck(
            name="wal_continuity",
            status=PreflightStatus.PASS,
            message=f"WAL lag is acceptable: {max_lag / 1024:.1f}KB max",
            details={"max_lag_bytes": max_lag}
        )
    else:
        return PreflightCheck(
            name="wal_continuity",
            status=PreflightStatus.PASS,
            message="DR is fully caught up with primary",
        )


def check_network_connectivity(cfg: Config) -> PreflightCheck:
    """Check network connectivity between DR instances."""
    # Check SSH connectivity to all non-local instances
    failures = []

    for inst in cfg.instances:
        if inst.is_local:
            continue

        try:
            p = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
                 inst.host, "echo ok"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if p.returncode != 0:
                failures.append(inst.host)
        except Exception:
            failures.append(inst.host)

    if not failures:
        return PreflightCheck(
            name="network_connectivity",
            status=PreflightStatus.PASS,
            message="SSH connectivity to all DR hosts verified",
        )
    else:
        return PreflightCheck(
            name="network_connectivity",
            status=PreflightStatus.FAIL,
            message=f"SSH connectivity failed to: {', '.join(failures)}",
            details={"failed_hosts": failures}
        )


def check_disk_space(cfg: Config) -> PreflightCheck:
    """Check disk space on DR instances."""
    low_disk = []

    for inst in cfg.instances:
        try:
            if inst.is_local:
                cmd = f"df {inst.data_dir} | tail -1 | awk '{{print $5}}'"
                p = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=10)
            else:
                cmd = f"df {inst.data_dir} | tail -1 | awk '{{print $5}}'"
                p = subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", inst.host, cmd],
                    capture_output=True, text=True, timeout=10
                )

            if p.returncode == 0:
                usage = p.stdout.strip().rstrip('%')
                if int(usage) > 90:
                    low_disk.append(f"{inst.host}:{usage}%")
        except Exception:
            pass

    if not low_disk:
        return PreflightCheck(
            name="disk_space",
            status=PreflightStatus.PASS,
            message="Disk space adequate on all DR instances",
        )
    else:
        return PreflightCheck(
            name="disk_space",
            status=PreflightStatus.WARN,
            message=f"Low disk space on: {', '.join(low_disk)}",
            details={"low_disk_instances": low_disk}
        )


def check_no_active_connections(cfg: Config) -> PreflightCheck:
    """Check primary has no active user connections (for graceful switchover)."""
    ok, result = _psql_check(
        cfg.primary_host, cfg.primary_port, cfg.primary_user, cfg.primary_db,
        """
        SELECT count(*) FROM pg_stat_activity
        WHERE state = 'active'
        AND pid != pg_backend_pid()
        AND usename NOT IN ('replication', 'gpadmin');
        """
    )

    if not ok:
        return PreflightCheck(
            name="active_connections",
            status=PreflightStatus.SKIP,
            message="Could not check active connections",
        )

    try:
        active_count = int(result or "0")
        if active_count == 0:
            return PreflightCheck(
                name="active_connections",
                status=PreflightStatus.PASS,
                message="No active user connections on primary",
            )
        else:
            return PreflightCheck(
                name="active_connections",
                status=PreflightStatus.WARN,
                message=f"{active_count} active connections on primary",
                details={"active_connections": active_count}
            )
    except ValueError:
        return PreflightCheck(
            name="active_connections",
            status=PreflightStatus.SKIP,
            message="Could not parse connection count",
        )


def run_preflight_checks(
    cfg: Config,
    switchover_type: SwitchoverType,
    skip_primary_checks: bool = False,
) -> SwitchoverPlan:
    """
    Run all preflight checks and generate switchover plan.

    Args:
        cfg: Configuration
        switchover_type: Type of switchover (graceful or failover)
        skip_primary_checks: Skip checks requiring primary connectivity

    Returns:
        SwitchoverPlan with all check results
    """
    created_at = utc_now_iso()
    checks: List[PreflightCheck] = []
    warnings: List[str] = []
    blockers: List[str] = []

    # Check primary connectivity
    primary_reachable = False
    primary_lsn = None

    if not skip_primary_checks:
        check = check_primary_connectivity(cfg)
        checks.append(check)
        primary_reachable = check.status == PreflightStatus.PASS

        if primary_reachable:
            primary_lsn = get_primary_lsn(cfg)

            # For graceful switchover, check active connections
            if switchover_type == SwitchoverType.GRACEFUL:
                conn_check = check_no_active_connections(cfg)
                checks.append(conn_check)
                if conn_check.status == PreflightStatus.WARN:
                    warnings.append(conn_check.message)
        else:
            if switchover_type == SwitchoverType.GRACEFUL:
                blockers.append("Primary not reachable - cannot do graceful switchover")
    else:
        checks.append(PreflightCheck(
            name="primary_connectivity",
            status=PreflightStatus.SKIP,
            message="Primary checks skipped (failover mode)",
        ))

    # Check DR instances
    dr_check, dr_lsns = check_dr_instances_status(cfg)
    checks.append(dr_check)

    if dr_check.status == PreflightStatus.FAIL:
        blockers.append("No DR instances reachable")
    elif dr_check.status == PreflightStatus.WARN:
        warnings.append(dr_check.message)

    # Check DR consistency
    consistency_check = check_dr_consistency(cfg)
    checks.append(consistency_check)

    if consistency_check.status == PreflightStatus.FAIL:
        blockers.append("DR consistency not validated")
    elif consistency_check.status == PreflightStatus.WARN:
        warnings.append(consistency_check.message)

    # Check WAL continuity
    wal_check = check_wal_continuity(cfg, primary_lsn, dr_lsns)
    checks.append(wal_check)

    if wal_check.status == PreflightStatus.WARN:
        warnings.append(wal_check.message)

    # Check network
    net_check = check_network_connectivity(cfg)
    checks.append(net_check)

    if net_check.status == PreflightStatus.FAIL:
        blockers.append(net_check.message)

    # Check disk space
    disk_check = check_disk_space(cfg)
    checks.append(disk_check)

    if disk_check.status == PreflightStatus.WARN:
        warnings.append(disk_check.message)

    # Get current restore point
    last_rp, _, _ = get_last_successful_sync(cfg)

    # Check all instances at same point
    all_same = len(set(dr_lsns.values())) <= 1 if dr_lsns else False

    # Calculate estimated data loss
    estimated_loss = 0
    if primary_lsn and dr_lsns:
        primary_int = _lsn_to_int(primary_lsn)
        max_dr_int = max(_lsn_to_int(lsn) for lsn in dr_lsns.values())
        estimated_loss = max(0, primary_int - max_dr_int)

    # WAL gap detection
    wal_gap = wal_check.status == PreflightStatus.WARN and wal_check.details and wal_check.details.get("max_lag_bytes", 0) > 100 * 1024 * 1024

    # Determine recommendation
    preflight_passed = len(blockers) == 0

    if preflight_passed:
        if switchover_type == SwitchoverType.GRACEFUL:
            recommended_action = "PROCEED with graceful switchover"
        else:
            recommended_action = "PROCEED with failover (ensure primary is truly down)"
    else:
        recommended_action = "DO NOT PROCEED - resolve blockers first"

    return SwitchoverPlan(
        switchover_type=switchover_type,
        created_at=created_at,
        preflight_passed=preflight_passed,
        preflight_checks=checks,
        primary_reachable=primary_reachable,
        primary_lsn=primary_lsn,
        dr_restore_point=last_rp,
        dr_lsns=dr_lsns,
        all_instances_at_same_point=all_same,
        wal_gap_detected=wal_gap,
        estimated_data_loss_bytes=estimated_loss,
        recommended_action=recommended_action,
        warnings=warnings,
        blockers=blockers,
    )


def render_plan_table(plan: SwitchoverPlan) -> str:
    """Render switchover plan as human-readable table."""
    lines = []

    lines.append(f"Switchover Preflight Report: {plan.created_at}")
    lines.append(f"Type: {plan.switchover_type.value.upper()}")
    lines.append(f"Overall Status: {'READY' if plan.preflight_passed else 'NOT READY'}")
    lines.append("")

    # Summary
    lines.append("SUMMARY:")
    lines.append(f"  Primary Reachable:     {plan.primary_reachable}")
    lines.append(f"  Primary LSN:           {plan.primary_lsn or '-'}")
    lines.append(f"  DR Restore Point:      {plan.dr_restore_point or '-'}")
    lines.append(f"  All DR at Same Point:  {plan.all_instances_at_same_point}")
    lines.append(f"  WAL Gap Detected:      {plan.wal_gap_detected}")
    if plan.estimated_data_loss_bytes > 0:
        lines.append(f"  Est. Data Loss:        {plan.estimated_data_loss_bytes / 1024:.1f} KB")
    else:
        lines.append(f"  Est. Data Loss:        0 (fully synced)")
    lines.append("")

    # Preflight checks
    lines.append("PREFLIGHT CHECKS:")
    for check in plan.preflight_checks:
        status_icon = {
            PreflightStatus.PASS: "[PASS]",
            PreflightStatus.WARN: "[WARN]",
            PreflightStatus.FAIL: "[FAIL]",
            PreflightStatus.SKIP: "[SKIP]",
        }.get(check.status, "[????]")
        lines.append(f"  {status_icon} {check.name}: {check.message}")
    lines.append("")

    # DR LSNs
    if plan.dr_lsns:
        lines.append("DR INSTANCE LSNs:")
        for seg_id in sorted(plan.dr_lsns.keys()):
            lines.append(f"  Segment {seg_id:>3}: {plan.dr_lsns[seg_id]}")
        lines.append("")

    # Blockers
    if plan.blockers:
        lines.append("BLOCKERS (must resolve before proceeding):")
        for b in plan.blockers:
            lines.append(f"  [X] {b}")
        lines.append("")

    # Warnings
    if plan.warnings:
        lines.append("WARNINGS (review before proceeding):")
        for w in plan.warnings:
            lines.append(f"  [!] {w}")
        lines.append("")

    # Recommendation
    lines.append("RECOMMENDATION:")
    lines.append(f"  {plan.recommended_action}")
    lines.append("")

    if plan.preflight_passed:
        lines.append("Next steps:")
        if plan.switchover_type == SwitchoverType.GRACEFUL:
            lines.append("  1. Stop applications connecting to primary")
            lines.append("  2. Run final sync: whpg_dr_sync dr run --once")
            lines.append("  3. Execute switchover: whpg_dr_sync switchover execute")
            lines.append("  4. Update connection strings to point to DR")
        else:
            lines.append("  1. Confirm primary is truly down (avoid split-brain!)")
            lines.append("  2. Execute failover: whpg_dr_sync failover execute")
            lines.append("  3. Update connection strings to point to DR")
            lines.append("  4. Investigate and repair original primary")

    return "\n".join(lines)


def render_plan_json(plan: SwitchoverPlan) -> str:
    """Render switchover plan as JSON."""
    def to_dict(obj):
        if isinstance(obj, Enum):
            return obj.value
        elif hasattr(obj, '__dataclass_fields__'):
            return {k: to_dict(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {str(k): to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [to_dict(v) for v in obj]
        return obj

    return json.dumps(to_dict(plan), indent=2)


def preflight_switchover(cfg: Config, fmt: str = "table") -> str:
    """
    Run preflight checks for graceful switchover.

    Args:
        cfg: Configuration
        fmt: Output format ("table" or "json")

    Returns:
        Formatted preflight report
    """
    plan = run_preflight_checks(cfg, SwitchoverType.GRACEFUL)

    if fmt == "json":
        return render_plan_json(plan)
    return render_plan_table(plan)


def preflight_failover(cfg: Config, fmt: str = "table") -> str:
    """
    Run preflight checks for failover (emergency promotion).

    Args:
        cfg: Configuration
        fmt: Output format ("table" or "json")

    Returns:
        Formatted preflight report
    """
    plan = run_preflight_checks(
        cfg,
        SwitchoverType.FAILOVER,
        skip_primary_checks=True,  # Don't require primary for failover
    )

    if fmt == "json":
        return render_plan_json(plan)
    return render_plan_table(plan)
