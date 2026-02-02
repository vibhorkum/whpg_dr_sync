# Operations

## Run modes

### Primary publisher (daemon)
systemd:
- whpg_dr_sync-primary.service

Manual:
```bash
whpg_dr_sync primary run --config dr_sync_config.json
```

### DR consumer (daemon)
systemd:
- whpg_dr_sync-dr.service

Manual:
```bash
whpg_dr_sync dr run --config dr_sync_config.json
```


# Monitoring

## Human status

```bash
whpg_dr_sync dr status --config dr_sync_config.json --format table
```

## Prometheus metrics (node_exporter textfile collector)

- exporter writes: /var/lib/node_exporter/textfile_collector/whpg_dr_sync.prom
- timer runs every 10 seconds

# Logging

Recommended: journald

```bash
journalctl -u whpg_dr_sync-dr.service -f
```

- write to /var/log/whpg_dr_sync/*.log
- rotate daily, keep 14

# Common issues

## “archive_gap” but files exist

Usually:

- wrong host (manifest archive_source_host mismatch)
- ssh identity mismatch (tool runs as different user than your manual check)
- permissions/SELinux context differs in non-interactive ssh

Validate as gpadmin:

```bash
ssh whpg-coordinator "test -f /data/archive/wal/<WALFILE> && echo OK"
```

## Consumer not advancing state

If instances are DOWN but tool didn’t record evidence, enable:
	•	log signature scan for “recovery stopping after WAL location”
	•	or treat “all down” as best-effort success only when targeting recovery_target_lsn

⸻

## Runbook suggestion

1. Ensure publisher READY manifests flowing
2. Ensure DR floors computed
3. Ensure WAL continuity checks succeed
4. Observe receipts advancing
