# whpg_dr_sync — Design

## Purpose

`whpg_dr_sync` synchronizes a DR Greenplum/WHPG cluster to a deterministic recovery point derived from the Primary cluster.

It is built around one contract:

> DR is considered consistent only when all instances (coordinator + segments)
> have reached the target and stopped deterministically at that target.

This avoids ambiguity inherent in “caught up” logic when segments are independent and availability can vary.

---

## Key ideas

### 1) Manifests are the truth
Primary publishes restore-point manifests as JSON:
- restore_point name
- restore_lsn per instance (segment/coordinator)
- evidence including WAL filename and archive source

Consumer never “guesses.” It consumes declared intent.

### 2) Deterministic shutdown beats pause/resume
Instead of replay pause/resume, DR uses:
- `recovery_target_lsn = '<lsn>'` (per instance)
- `recovery_target_action = 'shutdown'`
- `standby.signal` enforced

So the “parked” state is physically unambiguous: the instance is stopped.

### 3) Recovery floors prevent unsafe rewinds
A DR instance may have a recovery floor (e.g., after timeline changes or base backup constraints).
Consumer computes floor as:
1) SQL when up: `pg_control_recovery().min_recovery_end_lsn`
2) Offline when down: `pg_controldata` → “Minimum recovery ending location”

Consumer must choose a target at/after floor, otherwise it skips forward.

### 4) State is advanced only with evidence
Consumer maintains:
- `current_restore_point.txt`
- receipts per target restore point

State advancement is explicit, auditable, and conservative.

---

## Flow

### Primary publisher
1. Create restore point: `gp_create_restore_point('<name>')`
2. Compute WAL file for each instance at restore_lsn on that instance
3. Encourage WAL archival: `gp_switch_wal()`
4. Publish manifest as `ready=false`
5. Poll archive source(s) for WAL presence
6. Update manifest: `ready=true` and update `LATEST.json`

### DR consumer
1. Compute recovery floor per instance
2. Choose target:
   - Use `LATEST` if ready and >= floors
   - Else scan READY manifests and pick earliest >= floors
3. Ensure WAL continuity between current and target (optional but recommended)
4. Apply:
   - enforce `standby.signal`
   - set `recovery_target_action='shutdown'`
   - set `recovery_target_lsn` per instance
   - stop/start instance
5. Confirm:
   - instances become DOWN after reaching target
   - (optional) validate “stopping after WAL location” signature in logs
6. Write receipt and advance `current_restore_point.txt`

---

## Failure modes and responses

### A) No READY manifest >= floor
- Consumer logs operator-friendly message
- In daemon mode: it sleeps and retries
- Does not crash with stack traces (unless configured to)

### B) Archive mismatch / missing WAL
- Consumer fails fast to protect correctness
- This is a *safety brake*, not an inconvenience

### C) Instance down, no stop signature
- Consumer treats as “unknown down”
- Does not advance state unless evidence indicates a valid stop at target

### D) Config mutation failures
- Consumer avoids `sed` portability issues
- Uses atomic rewrite logic for postgresql.conf keys

---

## Invariants

1. Consumer never advances `current_restore_point` without evidence.
2. Consumer never targets an LSN behind recovery floors.
3. DR instances never exit recovery and promote as part of this flow.
4. Manifests are append-only audit artifacts; receipts are the consumer audit trail.

---

## Observability

Two layers:
- Human: `whpg_dr_sync dr status --format table`
- Machine: Prometheus via node_exporter textfile collector

The “truth” lives in:
- `LATEST.json`
- manifest history
- receipts
- DR instance logs

---

## Security model (assumed)

- `gpadmin` has SSH trust to segment hosts
- `psql`, `pg_ctl`, `gpssh` available via PATH or gp_home
- Filesystem permissions allow updating recovery config on DR data dirs

---

## Non-goals

- This tool does not orchestrate failover.
- This tool does not replace streaming replication.
- This tool does not heal broken archiving.

It ensures deterministic DR positioning — nothing more, nothing less.
