#!/usr/bin/env bash
# Install muse-stoat-bridge in place. In Muse the repository is cloned to
# ~/workspace/muse-stoat-bridge and this script is run from there; the project
# directory IS the installation, and uninstall.sh deletes all of it.
#
#   ./install.sh                        set up / repair the installation, including the
#                                       stoat-media skill: a symlink
#                                       ~/workspace/custom-skills/stoat-media -> skills/stoat-media
#                                       (Muse's directory for user-installed skills)
#   ./install.sh --skills-dir DIR       link the skill into another skills directory
#   ./install.sh --manifest             print the install list
#
# Created inside the project:
#   .venv/        Python environment              (uv sync)
#   .uv/          uv's Python, cache, and uv itself when it was not installed
#   .env          configuration incl. the bot token (from .env.example, mode 600)
#   data/         inbox, cursors, send state, hook state, logs, generated media
#   routes.json, hook/routes/   later, when routes are configured
# The only thing outside it is the skill symlink, recorded in
# install-manifest.txt for uninstall.sh. Running install.sh again is safe.
set -euo pipefail
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
MANIFEST=$DIR/install-manifest.txt
cd "$DIR"

case "$DIR" in
  /|"$HOME"|"$(cd ~ && pwd -P)") echo "refusing to install into $DIR: the project needs a directory of its own" >&2; exit 2 ;;
esac
[ -f "$DIR/stoatctl.py" ] && [ -f "$DIR/bridge.py" ] || { echo "install.sh must stay in the muse-stoat-bridge project directory" >&2; exit 2; }

# --- install list ---------------------------------------------------------------------
# One entry per line, tab separated: <kind> <path> <note>
#   project  the project directory; uninstall.sh deletes it as a whole
#   outside  a path outside the project that uninstall.sh removes (a symlink: the link only)
#   muse     a Muse-side registration uninstall.sh cannot remove itself

write_manifest() {
  local tmp=$MANIFEST.tmp
  local outside
  outside=$( [ -f "$MANIFEST" ] && awk -F'\t' '$1=="outside"' "$MANIFEST" || true)
  {
    echo "# muse-stoat-bridge install list, read by uninstall.sh. <kind> TAB <path> TAB <note>"
    printf 'project\t%s\t%s\n' "$DIR" "code, .venv, .uv, .env, data, routes.json, hook/routes: deleted as a whole"
    printf 'muse\t%s\t%s\n' "$DIR/hook" "Muse hooks whose script_path is under this directory (hooks.remove before uninstalling)"
    if [ -n "$outside" ]; then printf '%s\n' "$outside"; fi
  } >"$tmp"
  mv "$tmp" "$MANIFEST"
}

record_outside() {  # path note
  local path=$1 note=$2
  awk -F'\t' -v p="$path" '$1=="outside" && $2==p {f=1} END {exit !f}' "$MANIFEST" \
    || printf 'outside\t%s\t%s\n' "$path" "$note" >>"$MANIFEST"
}

# --- arguments ------------------------------------------------------------------------------
SKILL_DIR=$HOME/workspace/custom-skills   # where Muse loads user-installed skills
while [ $# -gt 0 ]; do
  case "$1" in
    --skills-dir) [ -n "${2:-}" ] || { echo "usage: install.sh --skills-dir DIR" >&2; exit 2; }
                  SKILL_DIR=$2; shift 2 ;;
    --manifest) cat "$MANIFEST"; exit 0 ;;
    *) sed -n '2,20p' "$0"; exit 2 ;;
  esac
done

# --- uv and the Python environment, all under $DIR/.uv ----------------------------------------
export UV_CACHE_DIR=$DIR/.uv/cache
export UV_PYTHON_INSTALL_DIR=$DIR/.uv/python
export UV_NO_CONFIG=1
UV=$(command -v uv || true)
if [ -z "$UV" ] && [ -x "$DIR/.uv/bin/uv" ]; then UV=$DIR/.uv/bin/uv; fi
if [ -z "$UV" ]; then
  echo "uv not found; installing a private copy into $DIR/.uv/bin (no PATH or profile changes)"
  mkdir -p "$DIR/.uv/bin"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$DIR/.uv/bin" sh >/dev/null
  UV=$DIR/.uv/bin/uv
fi
"$UV" sync --frozen --no-dev --quiet
"$UV" cache prune --ci --quiet 2>/dev/null || true

# --- configuration and data ------------------------------------------------------------------
mkdir -p "$DIR/data"
if [ ! -f "$DIR/.env" ]; then
  cp "$DIR/.env.example" "$DIR/.env"
  echo "created $DIR/.env (fill in STOAT_BOT_TOKEN)"
fi
chmod 600 "$DIR/.env"
# The Muse hook keeps the bridge alive: every hook run starts it if it is down.
grep -q '^STOAT_AUTOSTART=' "$DIR/.env" \
  || printf '\n# Set by install.sh: every hook run starts the bridge if it is down.\nSTOAT_AUTOSTART=1\n' >>"$DIR/.env"
write_manifest

# --- the stoat-media skill, as a symlink into this project ------------------------------------
"$DIR/skills/install.sh" --dest "$SKILL_DIR" >/dev/null
record_outside "$(cd "$SKILL_DIR" && pwd -P)/stoat-media" "stoat-media skill (symlink to $DIR/skills/stoat-media)"
echo "linked the stoat-media skill: $SKILL_DIR/stoat-media -> $DIR/skills/stoat-media"
"$DIR/skills/install.sh" --check || true

echo "muse-stoat-bridge installed in $DIR"
echo "install list: $MANIFEST"
if grep -q '^STOAT_BOT_TOKEN=$' "$DIR/.env"; then
  echo "next: put the bot token into $DIR/.env (STOAT_BOT_TOKEN=...), then:"
else
  echo "next:"
fi
echo "  $DIR/.venv/bin/python $DIR/stoatctl.py whoami"
echo "  $DIR/.venv/bin/python $DIR/stoatctl.py bridge start"
