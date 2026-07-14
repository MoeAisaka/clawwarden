#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if ! git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Initialize Git before installing hooks." >&2
  exit 2
fi

HOOK_DIR=$(git -C "$ROOT" rev-parse --git-path hooks)
mkdir -p "$HOOK_DIR"
for hook in pre-commit pre-push; do
  target="$HOOK_DIR/$hook"
  printf '%s\n' '#!/bin/sh' 'exec "$(git rev-parse --show-toplevel)/scripts/preflight.sh"' > "$target"
  chmod 0755 "$target"
done
echo "Installed fail-closed pre-commit and pre-push hooks."
