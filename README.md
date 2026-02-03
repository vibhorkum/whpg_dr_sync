
# whpg_dr_sync

**Deterministic DR synchronization for Greenplum / WHPG using restore points and WAL archives.**

`whpg_dr_sync` is an operationally safe, production-oriented tool that coordinates  
**Primary → DR synchronization** using:

- PostgreSQL / Greenplum restore points  
- WAL archive verification  
- Deterministic, shutdown-based recovery semantics  
- Explicit state tracking and receipts  

It is designed for **real operators**, not demos.

---

## Why this exists

Most DR tooling fails in one of three ways:

1. It assumes continuous streaming replication  
2. It relies on fragile pause/resume semantics  
3. It can’t explain *what actually happened* after the fact

`whpg_dr_sync` takes a different approach:

> **DR is consistent only when *all* instances stop at the same point.**

No ambiguity. No “probably caught up.”  
Either the system is parked at a known restore point — or it isn’t.

---

## Architecture (high-level)

```
Primary cluster
├─ gp_create_restore_point()
├─ gp_switch_wal()
└─ WAL archived per segment
↓
Manifest (JSON)
↓
DR cluster
├─ Validate WAL availability
├─ Apply recovery_target_lsn (per instance)
├─ recovery_target_action = "shutdown"
└─ State advanced only after clean stop
```

Everything is explicit. Everything is auditable.

---

## Components

### Primary (publisher)

- Creates restore points  
- Computes WAL filenames on owning instances  
- Waits for archived WAL files  
- Publishes **READY / NOT READY** manifests  
- Updates `LATEST.json`

### DR (consumer)

- Computes recovery floors (SQL + pg_controldata)  
- Selects the safest applicable manifest  
- Applies `recovery_target_lsn` per instance  
- Forces `standby.signal`  
- Shuts down cleanly at target  
- Advances state **only when safe**

---

## Installation

Python 3.9+ required.

```bash
pip install whpg_dr_sync
```

For development:

```bash
git clone https://github.com/<your-org>/whpg_dr_sync.git
cd whpg_dr_sync
pip install -e .
```

---

## Configuration

Single configuration file for both roles:

```json
{
  "primary": {
    "host": "whpg-coordinator",
    "port": 5432,
    "db": "postgres",
    "user": "gpadmin"
  },

  "storage": {
    "manifest_dir": "/data/archive/dr_sync/manifests",
    "latest_path": "/data/archive/dr_sync/LATEST.json"
  },

  "archive": {
    "archive_dir": "/data/archive/wal"
  },

  "dr": {
    "gp_home": "/usr/local/greenplum-db",
    "state_dir": "/data/archive/dr_sync/dr_state",
    "receipts_dir": "/data/archive/dr_sync/receipts",
    "instances": [
      {
        "gp_segment_id": -1,
        "host": "dr-coordinator",
        "port": 5432,
        "data_dir": "/data/coordinator/gpseg-1",
        "is_local": true
      },
      {
        "gp_segment_id": 0,
        "host": "dr-segment-1",
        "port": 6000,
        "data_dir": "/data/primary/seg/gpseg0",
        "is_local": false
      }
    ]
  },

  "behavior": {
    "publisher_sleep_secs": 10,
    "consumer_sleep_secs": 30,
    "consumer_reach_poll_secs": 5,
    "consumer_wait_reach_secs": 300,
    "wal_segment_size_mb": 64
  }
}
```

---

## Usage

### Publish restore points (Primary)

```bash
whpg_dr_sync primary run --config dr_sync_config.json
```

Run once:

```bash
whpg_dr_sync primary run --config dr_sync_config.json --once
```

---

### Consume restore points (DR)

```bash
whpg_dr_sync dr run --config dr_sync_config.json
```

Run once:

```bash
whpg_dr_sync dr run --config dr_sync_config.json --once
```

---

### Inspect status

```bash
whpg_dr_sync dr status --config dr_sync_config.json
```

Shows:

- Current restore point  
- Latest available manifest  
- Recovery floors  
- Receipts history  

---

## State & Receipts

`whpg_dr_sync` maintains:

- current_restore_point.txt`  
- Per-target JSON receipts  

Example receipt:

```json
{
  "current_restore_point": "sync_point_20260201_181406",
  "target_restore_point": "sync_point_20260201_181640",
  "checked_at_utc": "2026-02-01T18:35:49Z",
  "mode": "shutdown",
  "status": "reached_then_shutdown_best_effort",
  "target_lsns": {
    "-1": "9/E40000C8",
    "0": "9/EC0000C8"
  }
}
```

This is intentional: post-mortems matter.

---

## Design guarantees

- No silent promotion  
- No partial advancement  
- No implicit success  
- No state drift without evidence  

If the tool advances state, it earned it.

---

## Non-goals

- Not a streaming replication manager  
- Not a failover automation tool  
- Not HA orchestration  

This is deterministic DR, not magic.

---

## License

Apache License 2.0 — See [LICENSE](LICENSE).

---

## Contributing

Contributions welcome — especially for:

- Multi-segment scaling  
- Better archive backends  
- Observability hooks  

Open an issue or PR with intent, not guesses.
