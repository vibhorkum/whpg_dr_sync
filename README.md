
# whpg_dr_sync

**Deterministic DR synchronization for Greenplum / WHPG using restore points and WAL archives.**

`whpg_dr_sync` is a production-oriented tool that coordinates **Primary → DR synchronization** using:

* Greenplum restore points (`gp_create_restore_point`)
* WAL archive verification (per segment)
* Deterministic shutdown-based recovery (`recovery_target_action='shutdown'`)
* Explicit state tracking and receipts (auditability)

It is designed for **real operators**, not demos.

---

## Why this exists

Most DR tooling fails in one of three ways:

1. It assumes continuous streaming replication.
2. It relies on fragile pause/resume semantics.
3. It can’t explain *what actually happened* after the fact.

`whpg_dr_sync` takes a different approach:

> **DR is consistent only when *all* instances stop at the same point.**

No ambiguity. No "probably caught up." Either the system is parked at a known restore point — or it isn’t.

---

## Key Features

### Production Reliability

* **Pre-flight WAL Availability Checks** - Verifies all required WAL files exist before starting recovery
* **Recovery Point Validation** - Confirms instances stopped at correct restore point before advancing state
* **Explicit State Tracking** - All actions recorded in receipts for post-mortem analysis
* **Deterministic Shutdown** - Uses `recovery_target_action='shutdown'` for safe, repeatable recovery

### Flexible Storage Integration

* **Remote Manifest Access** - Retrieve manifests from S3, HTTP servers, or SSH-accessible remote hosts
* **Per-Segment WAL Verification** - Different storage backends for coordinator and each segment
* **Custom Commands** - Template-based commands for any storage system with CLI tools
* **Multi-Region Support** - Works with centralized or replicated manifest storage

### Configuration Options

* **Template Variables** - Dynamic path substitution for manifest and WAL access
* **Global and Per-Segment Settings** - Fine-grained control over WAL verification
* **Backward Compatible** - All new features are optional, existing configs work unchanged

### Performance & Concurrency

* **Parallel WAL Checks** - Primary publisher checks all segment WAL files concurrently (up to 32 workers)
* **Parallel DR Operations** - Consumer configures, starts, and monitors all instances in parallel
* **Fail-Fast Execution** - First error stops all operations immediately, no wasted time
* **Bounded Resources** - Worker pools capped at 32 to prevent resource exhaustion on large clusters
* **Smart Barriers** - Global synchronization points ensure correctness while maximizing parallelism
* **Thread-Safe Design** - No race conditions, distinct per-instance resources

**Performance Impact:**
- **Primary**: WAL verification time = slowest segment (not sum of all)
- **DR Consumer**: Recovery time ~N× faster for N-segment clusters (typical: 5-10× speedup)
- **Large Clusters**: Batched execution for >32 segments, still much faster than sequential

---


## Architecture

The system operates via a decoupled Publisher/Consumer model to ensure safety.

```
+--------------------------------+          +--------------------------------+
|       PRIMARY CLUSTER          |          |           DR CLUSTER           |
+--------------------------------+          +--------------------------------+
|                                |          |                                |
|  [ Create Restore Point ]      |     |--->|  [ Read LATEST.json ]          |
|             |                  |     |    |             |                  |
|      [ Switch WAL ]            |     |    |  [ Compute Recovery Floors ]   |
|             |                  |     |    |             |                  |
|  [ Archive WAL per Seg ]       |     |    |  [ Pick Safest Manifest ]      |
|             |                  |     |    |             |                  |
|     { Verify Archives }        |     |    |  [ Apply recovery_target_lsn ] |
|             | (Ready)          |     |    |             |                  |
|             v                  |     |    |   [ Write standby.signal ]     |
| [ Publish Manifest + LATEST ]--+-----|    |             |                  |
|                                |   JSON   |  [ Wait for Shutdown Evidence ]|
|                                |          |                                |
+--------------------------------+          +--------------------------------+
```
### Workflow

**Primary Cluster**

1. Calls `gp_create_restore_point()`
2. Calls `gp_switch_wal()` (optional)
3. Waits for WALs to be archived per segment
4. Generates Manifest (JSON) + Updates `LATEST.json`

**DR Cluster**

1. Computes recovery floors (via SQL / `pg_controldata`)
2. Picks safest **READY** manifest (LATEST or safe-forward)
3. Applies `recovery_target_lsn` (per instance)
4. Enforces `standby.signal` + `recovery_target_action='shutdown'`
5. Advances state only after evidence is confirmed

Everything is explicit. Everything is auditable.

---

## Components

### Primary (Publisher)

* Creates restore points.
* Maps segment → owning host/port.
* Computes WAL filename for each restore LSN on the owning instance.
* Waits for archived WAL files to appear.
* Publishes **READY / NOT READY** manifests.
* Updates `LATEST.json`.
* **Supports custom WAL verification commands** (per-segment or global).
* **Parallel WAL checks** across all segments using ThreadPoolExecutor (up to 32 workers).

### DR (Consumer)

* Computes recovery floors (SQL + `pg_controldata`).
* Selects the safest applicable manifest (LATEST or safe-forward).
* **Supports remote manifest access** (S3, HTTP, SSH, or local filesystem).
* Applies `recovery_target_lsn` per instance.
* Ensures `standby.signal`.
* Enforces `recovery_target_action='shutdown'`.
* Confirms stop-at-target via evidence.
* Advances state only when safe; writes receipts.
* **Pre-flight WAL availability checks** with configurable verification.
* **Recovery point validation** from logs before advancing state.
* **Parallel operations** for configuration, startup, and progress monitoring (up to 32 workers).
* **Smart barrier-based coordination** ensures all instances complete each phase before advancing.

---

## Installation

**Requirement:** Python 3.9+

### From source (recommended)

```bash
git clone https://github.com/vibhorkum/whpg_dr_sync.git
cd whpg_dr_sync
pip install -e .

```

---

## Configuration

A single config file drives both roles.

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

### Configuration Fields

**Primary Connection:**
- `primary.host`, `primary.port`, `primary.db`, `primary.user` - Connection details for primary cluster

**Storage:**
- `storage.manifest_dir` - Directory for manifest files
- `storage.latest_path` - Path to LATEST.json file
- `storage.manifest_fetch_command` - (Optional) Custom command to fetch manifests from remote storage
- `storage.manifest_list_command` - (Optional) Custom command to list manifest files

**Archive:**
- `archive.archive_dir` - Directory where WAL files are archived

**DR:**
- `dr.gp_home` - Greenplum installation directory
- `dr.state_dir` - Directory for DR state files
- `dr.receipts_dir` - Directory for receipt files
- `dr.instances[]` - Array of DR instance configurations

**Behavior:**
- `behavior.publisher_sleep_secs` - Sleep interval for publisher daemon
- `behavior.consumer_sleep_secs` - Sleep interval for DR consumer daemon
- `behavior.consumer_reach_poll_secs` - Polling interval when waiting for target
- `behavior.consumer_wait_reach_secs` - Maximum wait time for target
- `behavior.wal_segment_size_mb` - WAL segment size (typically 64MB)
- `behavior.wal_check_command` - (Optional) Global command to check WAL file existence
- `behavior.wal_check_commands` - (Optional) Per-segment/coordinator WAL check commands

---

## Advanced Configuration

### Remote Manifest Access

Store and retrieve manifests from remote locations (S3, HTTP, SSH) instead of local filesystem.

**Use Cases:**
- Centralized manifest storage in S3 or cloud storage
- Cross-network DR with manifests shared via HTTP/HTTPS
- Multi-region DR with manifest replication

**Configuration:**

```json
{
  "storage": {
    "manifest_dir": "/manifests",
    "latest_path": "/manifests/LATEST.json",
    "manifest_fetch_command": "aws s3 cp s3://my-bucket{manifest_path} -",
    "manifest_list_command": "aws s3 ls s3://my-bucket{manifest_dir}/ | awk '{print $4}'"
  }
}
```

**Template Variables:**
- `{manifest_path}` - Full path to manifest file
- `{manifest_dir}` - Manifest directory path
- `{manifest_file}` - Manifest filename only

**Examples:**

**AWS S3:**
```json
{
  "storage": {
    "manifest_fetch_command": "aws s3 cp s3://bucket{manifest_path} -",
    "manifest_list_command": "aws s3 ls s3://bucket{manifest_dir}/ | awk '{print $4}'"
  }
}
```

**SSH/Remote Host:**
```json
{
  "storage": {
    "manifest_fetch_command": "ssh remote-host cat {manifest_path}",
    "manifest_list_command": "ssh remote-host ls {manifest_dir}/"
  }
}
```

**HTTP/HTTPS:**
```json
{
  "storage": {
    "manifest_fetch_command": "curl -f https://storage.example.com{manifest_path}"
  }
}
```

### Per-Segment WAL Check Commands

Configure different WAL verification methods for coordinator and individual segments.

**Use Cases:**
- Mixed storage backends (coordinator on local disk, segments on S3)
- Different access methods per segment (SSH, S3, HTTP)
- Segment-specific storage locations

**Configuration:**

```json
{
  "behavior": {
    "wal_check_command": "ssh backup-server test -f /wal/{wal_filename} && echo EXISTS",
    "wal_check_commands": {
      "-1": "ssh coordinator-backup test -f /coord/{wal_filename} && echo EXISTS",
      "0": "aws s3 ls s3://seg0-bucket/{wal_filename} && echo EXISTS",
      "1": "curl -f https://api.storage.com/check?file={wal_filename} && echo EXISTS"
    }
  }
}
```

**Template Variables:**
- `{archive_dir}` - Archive directory path
- `{wal_filename}` - WAL filename to check
- `{wal_path}` - Full path (archive_dir/wal_filename)
- `{host}` - Host where check should run

**Behavior:**
- Coordinator uses command for segment `-1`
- Segment 0 uses command for segment `0`
- Other segments fall back to global `wal_check_command`
- If no custom command, uses local file check

---

## Artifacts on disk

### Publisher (Primary)

* `${storage.manifest_dir}/sync_point_*.json`
* `${storage.latest_path}` (LATEST.json)

### DR

* `${dr.state_dir}/current_restore_point.txt`
* `${dr.receipts_dir}/*.receipt.json`
* `${dr.state_dir}/primary.pid`
* `${dr.state_dir}/dr.pid`

---

## CLI

All commands follow this structure:
`whpg_dr_sync --config dr_sync_config.json <mode> <command> [options]`

### Primary Commands

**Run continuously:**

```bash
whpg_dr_sync --config dr_sync_config.json primary run

```

**Run once:**

```bash
whpg_dr_sync --config dr_sync_config.json primary run --once

```

**Disable WAL switch:**

```bash
whpg_dr_sync --config dr_sync_config.json primary run --no-gp-switch-wal

```

**Daemon control:**

```bash
whpg_dr_sync --config dr_sync_config.json primary pid-status
whpg_dr_sync --config dr_sync_config.json primary stop

```

**Status & Logs:**

```bash
whpg_dr_sync --config dr_sync_config.json primary status --format table
whpg_dr_sync --config dr_sync_config.json primary status --format json
whpg_dr_sync --config dr_sync_config.json primary status --format prometheus
whpg_dr_sync --config dr_sync_config.json primary logs --n 100

```

### DR Commands

**Run continuously:**

```bash
whpg_dr_sync --config dr_sync_config.json dr run

```

**Run once:**

```bash
whpg_dr_sync --config dr_sync_config.json dr run --once

```

**Consume a specific restore point:**

```bash
whpg_dr_sync --config dr_sync_config.json dr run --once --target sync_point_YYYYMMDD_HHMMSS

```

**Daemon control:**

```bash
whpg_dr_sync --config dr_sync_config.json dr pid-status
whpg_dr_sync --config dr_sync_config.json dr stop

```

**Status & Logs:**

```bash
whpg_dr_sync --config dr_sync_config.json dr status --format table --include-history
whpg_dr_sync --config dr_sync_config.json dr status --format json --include-history
whpg_dr_sync --config dr_sync_config.json dr status --format prometheus
whpg_dr_sync --config dr_sync_config.json dr logs --n 100

```

---

## State & Receipts

`whpg_dr_sync` records explicit evidence of every DR action. Receipts exist so post-mortems are factual, not emotional.

**Example receipt:**

```json
{
  "current_restore_point": "sync_point_20260201_181406",
  "target_restore_point": "sync_point_20260201_181640",
  "checked_at_utc": "2026-02-01T18:35:49Z",
  "mode": "shutdown",
  "status": "reached_then_shutdown_best_effort",
  "waited_secs": 120,
  "target_lsns": {
    "-1": "9/E40000C8",
    "0": "9/EC0000C8"
  }
}

```

---

## Implementation Details

### Concurrency Model

Both the **Primary (Publisher)** and **DR (Consumer)** use Python's `concurrent.futures.ThreadPoolExecutor` for parallel operations while maintaining correctness through barrier-based synchronization.

#### Primary Publisher

**Parallelized Operations:**
- WAL file existence checks across all segments
- Each segment's archive source is checked concurrently via SSH

**Implementation:**
```python
with ThreadPoolExecutor(max_workers=min(32, max(1, len(targets)))) as executor:
    futures = {executor.submit(_check_target, t): t for t in targets}
    for future in as_completed(futures):
        present = future.result()
        # Collect results for manifest
```

**Benefits:**
- Verification time = slowest segment (not sum)
- Scales efficiently with number of segments
- Bounded resources prevent system overload

#### DR Consumer

**Parallelized Phases:**

1. **Configuration Phase** - All instances configure recovery targets concurrently
2. **Startup Phase** - All instances stop, preflight, and start in parallel
3. **Progress Monitoring** - All instances checked concurrently in each poll iteration

**Barrier Points (Sequential):**
- All instances must complete configuration before any start
- All instances must complete startup before entering polling loop
- All instances must be DOWN before validating restore points
- State advancement only after global validation succeeds

**Implementation Pattern:**
```python
with ThreadPoolExecutor(max_workers=min(len(instances), 32)) as executor:
    futures = {executor.submit(operation, inst): seg_id for seg_id, inst in instances.items()}
    for future in as_completed(futures):
        result = future.result()  # Fail-fast on exceptions
```

**Thread Safety:**
- Each instance operates on distinct resources (data dirs, config files, processes)
- No shared mutable state between workers
- Results aggregated via futures after all complete

**Fail-Fast Behavior:**
- First exception stops all operations
- Clear error messages with instance labels
- No wasted polling if recovery fails

### Special Case: Instances DOWN Before Target LSN

The DR consumer handles the case where instances stop before reaching the target LSN (due to missing WAL, earlier restore point, or errors).

**Detection:**
- Tracks two conditions separately: "instances reached target" vs "instances are DOWN"
- Uses `replay_lsn != None` to detect if instance is still running

**Behavior:**
- When all instances are DOWN (even if LSN < target), proceeds to restore point validation
- Validation checks if instances stopped at the correct restore point *name*
- Success if restore point names match (LSN mismatch might indicate manifest issue)
- Failure with clear error if restore points don't match or aren't found

**Rationale:**
- PostgreSQL `recovery_target_name` stops at restore point name, not specific LSN
- Restore point validation is the source of truth
- LSN check is a sanity check, not the primary success criterion

---

## Design Guarantees

* **No silent promotion:** The system will never promote a standby without explicit instruction.
* **No partial advancement:** All segments must meet criteria.
* **No implicit success:** Success is only reported after verification.
* **No state drift without evidence:** If the tool advances state, it earned it.

## Non-goals

* Not a streaming replication manager.
* Not a failover automation tool.
* Not HA orchestration.

This is **deterministic DR**, not magic.

---

## License

Apache License 2.0 — see [LICENSE](https://www.google.com/search?q=LICENSE).

## Author

**Vibhor Kumar**

Data Platform & PostgreSQL Leader  
Open Source • Enterprise Postgres • AI-aware Data Platforms

- GitHub: https://github.com/vibhorkum
- LinkedIn: https://www.linkedin.com/in/vibhork/
