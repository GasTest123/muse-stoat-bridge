#!/usr/bin/env bash
# Uninstall muse-stoat-bridge: everything listed in install-manifest.txt, then the
# project directory itself (normally ~/workspace/muse-stoat-bridge).
#
#   ./uninstall.sh          show what will be removed; removes nothing
#   ./uninstall.sh --yes    remove it all
#
# Order matters: remove the Muse hooks first (they are registrations inside
# Muse; this script cannot reach them), then run --yes. It then
#   1. stops the bridge and waits for every process running from the project
#      (hook polls) to end, so nothing writes files afterwards;
#   2. removes each 'outside' path in the install list (the skill symlink: the
#      link only, never what it points to);
#   3. deletes the project directory, including this script.
set -uo pipefail
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
MANIFEST=$DIR/install-manifest.txt
YES=0
[ "${1:-}" = "--yes" ] && YES=1

[ -f "$MANIFEST" ] || { echo "no install list at $MANIFEST: muse-stoat-bridge is not installed here (run install.sh first)" >&2; exit 2; }
listed=$(awk -F'\t' '$1=="project"{print $2}' "$MANIFEST")
[ "$listed" = "$DIR" ] || { echo "install list is for ${listed:-?}, not $DIR; refusing" >&2; exit 2; }
case "$DIR" in /|"$HOME"|"$(cd ~ && pwd -P)") echo "refusing to remove $DIR" >&2; exit 2 ;; esac
[ -f "$DIR/stoatctl.py" ] || { echo "$DIR does not look like muse-stoat-bridge; refusing" >&2; exit 2; }

PY=$DIR/.venv/bin/python
[ -x "$PY" ] || PY=python3

hook_ids() {
  # Every hook this installation can have generated; the reliable rule is the
  # script_path prefix, the ids are a convenience.
  {
    "$PY" "$DIR/stoatctl.py" routes hooks 2>/dev/null | "$PY" -c 'import json,sys
for l in sys.stdin:
    l = l.strip()
    if l: print(json.loads(l)["id"])' 2>/dev/null
    "$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$DIR/hook/hook.example.json" 2>/dev/null
  } | sort -u | paste -sd ' ' -
}

echo "muse-stoat-bridge uninstall plan"
echo "  Muse hooks (remove them in Muse first, with hooks.remove):"
echo "    every hook whose script_path is under $DIR/"
echo "    ids this installation uses: $(hook_ids)"
echo "  processes: the bridge and any hook poll running from $DIR"
awk -F'\t' '$1=="outside"{printf "  remove outside path: %s  (%s)\n", $2, $3}' "$MANIFEST"
echo "  remove project directory: $DIR"
if [ $YES -ne 1 ]; then
  echo
  echo "nothing removed. After the Muse hooks are gone, run: $DIR/uninstall.sh --yes"
  exit 0
fi
outside=$(awk -F'\t' '$1=="outside"{print $2}' "$MANIFEST")

# 1. stop everything that runs from the project
touch "$DIR/.uninstalling"            # hook polls must not restart the bridge now
"$PY" "$DIR/stoatctl.py" bridge stop >/dev/null 2>&1 || true
# The bridge and stoatctl (hook polls, workers' commands) are the processes
# that write into the project.
RE=$(printf '%s' "$DIR" | sed 's/[][\.*^$+?(){}|]/\\&/g')
others() { pgrep -f -- "$RE/(bridge|stoatctl)\.py" || true; }
for _ in $(seq 60); do [ -z "$(others)" ] && break; sleep 0.5; done
pids=$(others)
if [ -n "$pids" ]; then
  echo "stopping leftover processes: $(echo $pids)"
  kill $pids 2>/dev/null || true
  sleep 2
  pids=$(others); [ -n "$pids" ] && kill -9 $pids 2>/dev/null || true
fi

# 2. paths outside the project
fail=0
while IFS= read -r path; do
  [ -n "$path" ] || continue
  case "$path" in /|"$HOME"|"$(cd ~ && pwd -P)") echo "skip unsafe path '$path'"; fail=1; continue ;; esac
  if [ -L "$path" ]; then
    rm -f -- "$path" && echo "removed link $path" || { echo "could not remove $path"; fail=1; }
  elif [ -e "$path" ]; then
    rm -rf -- "$path" && echo "removed $path" || { echo "could not remove $path"; fail=1; }
  else
    echo "already gone: $path"
  fi
done <<<"$outside"

# 3. the project directory (bash keeps reading this script from its open file)
cd /
rm -rf -- "$DIR"
sleep 1
[ -e "$DIR" ] && rm -rf -- "$DIR"     # a straggler wrote something back
if [ -e "$DIR" ]; then echo "could not fully remove $DIR" >&2; exit 1; fi
echo "removed $DIR"

[ $fail -eq 0 ] || { echo "some paths need manual removal (see above)" >&2; exit 1; }
echo "muse-stoat-bridge is uninstalled."
