from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .config import load_config
from .service import status as pid_status, stop as pid_stop


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

    # PRIMARY
    p_primary = sub.add_parser("primary", help="Primary-side restore-point publisher")
    sp_primary = p_primary.add_subparsers(dest="cmd", required=True)

    p_run = sp_primary.add_parser("run", help="Run publisher")
    p_run.add_argument("--once", action="store_true")
    p_run.add_argument("--no-gp-switch-wal", action="store_true")

    # DR
    p_dr = sub.add_parser("dr", help="DR-side manifest consumer")
    sp_dr = p_dr.add_subparsers(dest="cmd", required=True)

    d_run = sp_dr.add_parser("run", help="Run consumer")
    d_run.add_argument("--once", action="store_true")
    d_run.add_argument("--target", default="LATEST")

    sp_dr.add_parser("stop", help="Stop daemon (pidfile mode)")
    sp_dr.add_parser("pid-status", help="Show pidfile status (pidfile mode)")

    d_status = sp_dr.add_parser("status", help="Show DR state (current restore point + latest receipt)")
    d_logs = sp_dr.add_parser("logs", help="Tail receipts directory")
    d_logs.add_argument("--n", type=int, default=50)

    args = ap.parse_args()
    cfg = load_config(args.config)

    if args.mode == "primary":
        from .primary import publish_one

        if args.cmd == "run":
            if args.once:
                publish_one(cfg, once_no_gp_switch_wal=args.no_gp_switch_wal)
                return 0
            while True:
                try:
                    publish_one(cfg, once_no_gp_switch_wal=args.no_gp_switch_wal)
                except Exception as e:
                    print(f"[PRIMARY] ERROR: {e}", file=sys.stderr)
                time.sleep(cfg.publisher_sleep_secs)
        return 0

    if args.mode == "dr":
        if args.cmd == "stop":
            pid_stop(cfg, "dr")
            return 0

        if args.cmd == "pid-status":
            pid_status(cfg, "dr")
            return 0

        if args.cmd == "status":
            state_file = Path(cfg.state_dir) / "current_restore_point.txt"
            rp = state_file.read_text().strip() if state_file.exists() else ""
            print(f"[DR] current_restore_point={rp or '(missing)'}")

            receipts = sorted(Path(cfg.receipts_dir).glob("*.receipt.json"))
            if not receipts:
                print("[DR] no receipts yet")
                return 0

            latest = receipts[-1]
            print(f"[DR] latest_receipt={latest.name}")
            _tail_file(latest, n=80)
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

        if args.cmd == "run":
            from .dr import run_daemon, run_once

            if args.once:
                return run_once(cfg, target=args.target)
            return run_daemon(cfg, target=args.target)

    return 0
