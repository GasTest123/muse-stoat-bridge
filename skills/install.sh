#!/usr/bin/env bash
# Install the Stoat skills shipped with muse-stoat-bridge into an agent's skills
# directory (the Agent Skills layout: <dir>/<name>/SKILL.md).
#
#   skills/install.sh                         link stoat-media into $MUSE_SKILLS_DIR
#                                             (default ~/workspace/custom-skills, where
#                                             Muse loads user-installed skills): a symlink
#                                             to this project's skills/stoat-media
#   skills/install.sh --dest DIR              another skills directory
#   skills/install.sh --uninstall [--dest DIR]
#   skills/install.sh --check                 only report what the skill needs
#
# Installing changes nothing in the bridge: `stoatctl send --attach` works
# with or without the skill. The skill only teaches the agent the procedure.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NAME=stoat-media
DEST=${MUSE_SKILLS_DIR:-$HOME/workspace/custom-skills}
MODE=link
while [ $# -gt 0 ]; do
  case "$1" in
    --dest) DEST=$2; shift 2 ;;
    --link) MODE=link; shift ;;
    --uninstall) MODE=uninstall; shift ;;
    --check) MODE=check; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

check() {
  local ok=0
  if command -v python3 >/dev/null; then
    echo "python3      ok ($(python3 -c 'import sys; print(sys.version.split()[0])'))"
  else
    echo "python3      MISSING: the skill's prepare / demo script needs it"; ok=1
  fi
  for tool in ffmpeg ffprobe; do
    if command -v $tool >/dev/null; then echo "$(printf '%-12s' $tool) ok"
    else echo "$(printf '%-12s' $tool) missing: video conversion/compression is unavailable"; fi
  done
  if ! command -v ffmpeg >/dev/null; then
    if command -v sips >/dev/null; then echo "sips         ok (images only)"
    else echo "             images can only be sent as already-valid png/jpeg/gif/webp under the limit"; fi
  fi
  return $ok
}

TARGET=$DEST/$NAME
case "$MODE" in
  check) check; exit $? ;;
  uninstall)
    if [ -L "$TARGET" ] || [ -d "$TARGET" ]; then rm -rf "$TARGET"; echo "removed $TARGET"
    else echo "not installed at $TARGET"; fi
    exit 0 ;;
esac

mkdir -p "$DEST"
if [ -e "$TARGET" ] || [ -L "$TARGET" ]; then
  [ -f "$TARGET/SKILL.md" ] || [ -L "$TARGET" ] || {
    echo "$TARGET exists and is not a skill; refusing to replace it" >&2; exit 1; }
  rm -rf "$TARGET"
fi
ln -s "$HERE/$NAME" "$TARGET"
chmod +x "$HERE/$NAME/scripts/stoat_media.py"
echo "linked $TARGET -> $HERE/$NAME"
check || true
