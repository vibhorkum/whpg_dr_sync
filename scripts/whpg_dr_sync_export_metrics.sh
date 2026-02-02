#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-/data/archive/dr_sync/dr_sync_config.json}"
OUTDIR="${2:-/var/lib/node_exporter/textfile_collector}"
OUTFILE="${OUTDIR}/whpg_dr_sync.prom"
TMPFILE="${OUTFILE}.tmp"

mkdir -p "${OUTDIR}"

# Prefer pip CLI if available, else call python status module.
if command -v whpg_dr_sync >/dev/null 2>&1; then
  METRICS="$(whpg_dr_sync dr status --config "${CONFIG}" --format prometheus)"
else
  # fallback; adjust if you keep scripts outside a package
  METRICS="$(python3 -c 'print("ERROR: whpg_dr_sync not installed")')"
fi

# Atomic write
printf "%s\n" "${METRICS}" > "${TMPFILE}"
mv -f "${TMPFILE}" "${OUTFILE}"
