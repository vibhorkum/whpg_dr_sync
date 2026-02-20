from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .config import load_config
from .service import status as pid_status, stop as pid_stop
from .common import ShutdownRequested

import signal

def install_signal_handlers() -> None:
    """
    Make SIGTERM behave like Ctrl+C so systemd stop/shutdown exits cleanly
    without tracebacks.
    """
    def _handler(signum, frame):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _tail_file(path: Path, n: int = 50) -> None:
    if not path.exists():
        print(f"[logs] not found: {path}")
        return
    lines = path.read_text().splitlines()[-n:]
    for ln in lines:
        print(ln)


def main() -> int:
    ap = argparse.ArgumentParser(prog="whpg_dr_sync", description="WHPG DR Sync tool (PRIMARY publisher + DR consumer).")
    ap.add_argument("--config", required=True, help="Path to dr_sync_config.json")

    sub = ap.add_subparsers(dest="mode", required=True)

    # =========================================================================
    # PRIMARY
    # =========================================================================
    p_primary = sub.add_parser("primary", help="Primary-side restore-point publisher")
    sp_primary = p_primary.add_subparsers(dest="cmd", required=True)

    p_run = sp_primary.add_parser("run", help="Run publisher")
    p_run.add_argument("--once", action="store_true")
    p_run.add_argument("--no-gp-switch-wal", action="store_true")
    # In PRIMARY subcommands, add:
    sp_primary.add_parser("stop", help="Stop daemon (pidfile mode)")
    sp_primary.add_parser("pid-status", help="Show pidfile status (pidfile mode)")

    p_status = sp_primary.add_parser("status", help="Show PRIMARY state (LATEST manifest etc.)")
    p_status.add_argument("--format", choices=["table", "prometheus", "json"], default="table")
    p_status.add_argument("--include-history", action="store_true")
    p_status.add_argument("--history-n", type=int, default=10)
    p_status.add_argument("--name", default="whpg_dr_sync")

    p_logs = sp_primary.add_parser("logs", help="Tail latest manifest/LATEST.json")
    p_logs.add_argument("--n", type=int, default=50)

    # =========================================================================
    # DR
    # =========================================================================
    p_dr = sub.add_parser("dr", help="DR-side manifest consumer")
    sp_dr = p_dr.add_subparsers(dest="cmd", required=True)

    d_run = sp_dr.add_parser("run", help="Run consumer")
    d_run.add_argument("--once", action="store_true")
    d_run.add_argument("--target", default="LATEST")

    sp_dr.add_parser("stop", help="Stop daemon (pidfile mode)")
    sp_dr.add_parser("pid-status", help="Show pidfile status (pidfile mode)")

    # Enhanced status (backward compatible)
    d_status = sp_dr.add_parser("status", help="Show DR state (current restore point + latest receipt)")
    d_status.add_argument("--format", choices=["table", "prometheus", "json"], default="table")
    d_status.add_argument("--include-history", action="store_true", help="Include recent receipts summary")
    d_status.add_argument("--history-n", type=int, default=10, help="How many receipts to scan (default: 10)")
    d_status.add_argument("--name", default="whpg_dr_sync", help="Metric prefix/name for prometheus output")

    d_logs = sp_dr.add_parser("logs", help="Tail receipts directory")
    d_logs.add_argument("--n", type=int, default=50)

    # =========================================================================
    # HEALTH - Cluster health monitoring
    # =========================================================================
    p_health = sub.add_parser("health", help="Cluster health monitoring")
    sp_health = p_health.add_subparsers(dest="cmd", required=True)

    h_status = sp_health.add_parser("status", help="One-shot health check")
    h_status.add_argument("--format", choices=["table", "prometheus", "json"], default="table")
    h_status.add_argument("--name", default="whpg_dr_sync", help="Metric prefix for prometheus")
    h_status.add_argument("--skip-primary", action="store_true", help="Skip primary checks")

    h_watch = sp_health.add_parser("watch", help="Continuous health monitoring daemon")
    h_watch.add_argument("--interval", type=int, default=60, help="Check interval in seconds")
    h_watch.add_argument("--output-file", help="Write prometheus metrics to file")

    # =========================================================================
    # RPO - Recovery Point Objective tracking
    # =========================================================================
    p_rpo = sub.add_parser("rpo", help="Recovery Point Objective tracking")
    sp_rpo = p_rpo.add_subparsers(dest="cmd", required=True)

    r_status = sp_rpo.add_parser("status", help="Show current RPO metrics")
    r_status.add_argument("--format", choices=["table", "prometheus", "json"], default="table")
    r_status.add_argument("--name", default="whpg_dr_sync", help="Metric prefix for prometheus")

    # =========================================================================
    # SWITCHOVER - Graceful switchover (primary is up)
    # =========================================================================
    p_switch = sub.add_parser("switchover", help="Graceful switchover operations")
    sp_switch = p_switch.add_subparsers(dest="cmd", required=True)

    sw_preflight = sp_switch.add_parser("preflight", help="Run preflight checks for switchover")
    sw_preflight.add_argument("--format", choices=["table", "json"], default="table")

    # =========================================================================
    # FAILOVER - Emergency failover (primary is down)
    # =========================================================================
    p_failover = sub.add_parser("failover", help="Emergency failover operations")
    sp_failover = p_failover.add_subparsers(dest="cmd", required=True)

    fo_preflight = sp_failover.add_parser("preflight", help="Run preflight checks for failover")
    fo_preflight.add_argument("--format", choices=["table", "json"], default="table")

    # =========================================================================
    # Parse and dispatch
    # =========================================================================
    args = ap.parse_args()
    install_signal_handlers()
    cfg = load_config(args.config)

    # -------------------------------------------------------------------------
    # PRIMARY mode
    # -------------------------------------------------------------------------
    if args.mode == "primary":
        from .primary import publish_one

        if args.cmd == "stop":
            pid_stop(cfg, "primary")
            return 0

        if args.cmd == "pid-status":
            pid_status(cfg, "primary")
            return 0

        if args.cmd == "status":
            from .status import render_status
            out = render_status(cfg, fmt=args.format, include_history=args.include_history,
                        history_n=args.history_n, metric_name=args.name, mode="primary")
            sys.stdout.write(out if out.endswith("\n") else out + "\n")
            return 0

        if args.cmd == "run":
                from .primary import publish_one, run_daemon
                if args.once:
                    publish_one(cfg, once_no_gp_switch_wal=args.no_gp_switch_wal)
                    return 0
                return run_daemon(cfg, once_no_gp_switch_wal=args.no_gp_switch_wal)
        if args.cmd == "logs":
            latest = Path(cfg.latest_path)
            manifest_dir = Path(cfg.manifest_dir)

            # 1) Show LATEST.json (most important)
            print(f"[PRIMARY] tailing LATEST: {latest}")
            _tail_file(latest, n=args.n)

            # 2) Also show newest manifest file (by mtime)
            manifests = sorted(
                manifest_dir.glob("sync_point_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not manifests:
                print(f"[PRIMARY] no manifests found in {manifest_dir}")
                print("[PRIMARY] NOTE: primary logs are manifests (JSON), not receipts.")
                return 0

            newest = manifests[0]
            # Avoid duplicating if newest == latest_path (in case you store LATEST inside manifest_dir)
            if newest.resolve() != latest.resolve():
                print(f"\n[PRIMARY] tailing newest manifest: {newest.name}")
                _tail_file(newest, n=args.n)

            print("\n[PRIMARY] TIP: if running under systemd, use:")
            print("  journalctl -u whpg_dr_sync-primary -n 200 --no-pager")
            return 0

        return 0

    # -------------------------------------------------------------------------
    # DR mode
    # -------------------------------------------------------------------------
    if args.mode == "dr":
        if args.cmd == "stop":
            pid_stop(cfg, "dr")
            return 0

        if args.cmd == "pid-status":
            pid_status(cfg, "dr")
            return 0

        if args.cmd == "logs":
            receipts = sorted(Path(cfg.receipts_dir).glob("*.receipt.json"))
            if not receipts:
                print("[DR] no receipts yet")
                return 0
            latest = receipts[-1]
            print(f"[DR] tailing latest receipt: {latest}")
            _tail_file(latest, n=args.n)
            return 0

        if args.cmd == "status":
            # New richer status; still "feels" like your old status by default
            from .status import render_status

            out = render_status(
                cfg=cfg,
                fmt=args.format,
                include_history=bool(args.include_history),
                history_n=int(args.history_n),
                metric_name=str(args.name),
            )
            sys.stdout.write(out)
            if not out.endswith("\n"):
                sys.stdout.write("\n")
            return 0

        if args.cmd == "run":
            from .dr import run_daemon, run_once

            if args.once:
                return run_once(cfg, target=args.target)
            return run_daemon(cfg, target=args.target)

    # -------------------------------------------------------------------------
    # HEALTH mode
    # -------------------------------------------------------------------------
    if args.mode == "health":
        from .health import (
            perform_health_check,
            render_health_table,
            render_health_json,
            render_health_prometheus,
            health_daemon,
        )

        if args.cmd == "status":
            status = perform_health_check(cfg, check_primary=not args.skip_primary)

            if args.format == "json":
                out = render_health_json(status)
            elif args.format == "prometheus":
                out = render_health_prometheus(status, metric_name=args.name)
            else:
                out = render_health_table(status)

            sys.stdout.write(out)
            if not out.endswith("\n"):
                sys.stdout.write("\n")

            # Return non-zero exit code if critical
            return 1 if status.overall_health == "critical" else 0

        if args.cmd == "watch":
            return health_daemon(
                cfg,
                interval_secs=args.interval,
                output_file=args.output_file,
            )

    # -------------------------------------------------------------------------
    # RPO mode
    # -------------------------------------------------------------------------
    if args.mode == "rpo":
        from .rpo import rpo_check, calculate_rpo_metrics

        if args.cmd == "status":
            out = rpo_check(cfg, fmt=args.format)
            sys.stdout.write(out)
            if not out.endswith("\n"):
                sys.stdout.write("\n")

            # Return non-zero if RPO violation
            metrics = calculate_rpo_metrics(cfg)
            return 1 if metrics.status == "violation" else 0

    # -------------------------------------------------------------------------
    # SWITCHOVER mode
    # -------------------------------------------------------------------------
    if args.mode == "switchover":
        from .switchover import preflight_switchover

        if args.cmd == "preflight":
            out = preflight_switchover(cfg, fmt=args.format)
            sys.stdout.write(out)
            if not out.endswith("\n"):
                sys.stdout.write("\n")
            return 0

    # -------------------------------------------------------------------------
    # FAILOVER mode
    # -------------------------------------------------------------------------
    if args.mode == "failover":
        from .switchover import preflight_failover

        if args.cmd == "preflight":
            out = preflight_failover(cfg, fmt=args.format)
            sys.stdout.write(out)
            if not out.endswith("\n"):
                sys.stdout.write("\n")
            return 0

    return 0
