#!/usr/bin/env bash
# Muse hook: wake the worker when Stoat messages are waiting in the inbox.
#
# Register this script as a Muse hook (poll_interval_secs ~5,
# script_timeout_secs >= STOAT_HOOK_WINDOW + 30). Each run blocks for up to
# STOAT_HOOK_WINDOW seconds watching inbox.jsonl and calls `wake` the moment a
# message lands, so pickup latency is well under a second rather than one
# poll interval. All decisions (single-instance lock, worker lease, wake
# cooldown, bridge-down alerts, dry-run purity) live in `stoatctl.py hook-poll`.
#
# Multi-channel: with a routes.json, register one hook per route (each with
# its own queue and worker). `stoatctl routes hooks --write` generates
# per-route wrappers that set STOAT_ROUTE and source this script; without
# STOAT_ROUTE this script serves every channel.
#
# The Muse hook runtime ($HATCH_HOOK_RUNTIME) provides wake / silent / log.
# Each run must end in exactly one wake or silent; both exit the script.
set -uo pipefail
source "$HATCH_HOOK_RUNTIME"

# Edit STOAT_BRIDGE_DIR if this script is copied out of the repository.
BRIDGE_DIR=${STOAT_BRIDGE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
PY=${STOAT_PYTHON:-$BRIDGE_DIR/.venv/bin/python}
[ -x "$PY" ] || PY=python3

# stderr capture stays inside the install (data dir), never in /tmp.
DATA_DIR=${STOAT_DATA_DIR:-$BRIDGE_DIR/data}
mkdir -p "$DATA_DIR"
errf="$DATA_DIR/.hook-stderr.$$"
trap 'rm -f "$errf"' EXIT
out=$("$PY" "$BRIDGE_DIR/stoatctl.py" hook-poll \
  --window "${STOAT_HOOK_WINDOW:-25}" 2>"$errf")
rc=$?
err=$(head -c 300 "$errf"); rm -f "$errf"
if [ $rc -ne 0 ]; then
  log "stoatctl hook-poll failed" "{\"exit\":$rc}"
  silent "hook-poll failed (exit $rc): $err"
fi

IFS=$'\t' read -r decision reason payload <<<"$out"
if [ "$decision" = "wake" ]; then
  [ -n "$payload" ] || payload='{}'
  wake "$reason" "$payload"
fi
silent "${reason:-no decision}"
