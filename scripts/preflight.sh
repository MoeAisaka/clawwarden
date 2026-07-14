#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

python3 -m py_compile scripts/*.py
python3 -m unittest discover -s tests -v
node --test plugin/openclaw/tests/*.test.mjs
python3 scripts/secret_scan.py --self-test

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  python3 scripts/secret_scan.py --tracked
else
  python3 scripts/secret_scan.py .
fi

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "preflight: gitleaks is required; install it before committing or pushing" >&2
  exit 3
fi

gitleaks dir --redact --no-banner .
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  gitleaks git --redact --no-banner .
fi

echo "preflight: all checks passed"
