#!/usr/bin/env python3
"""Out-of-process liveness watchdog for the Clawwarden control-plane worker."""

from __future__ import print_function

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "memory" / "clawwarden" / "control-plane"
HEARTBEAT = STATE_DIR / "worker-heartbeat.json"
WATCHDOG_STATE = STATE_DIR / "watchdog-state.json"
LABEL = "com.openclaw.clawwarden-control-plane"
MAX_AGE_SECONDS = int(os.environ.get("CLAWWARDEN_WORKER_MAX_AGE_SECONDS", "180"))
COOLDOWN_SECONDS = int(os.environ.get("CLAWWARDEN_WATCHDOG_COOLDOWN_SECONDS", "300"))


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(str(tmp), 0o600)
    os.replace(str(tmp), str(path))


def main():
    now = time.time()
    heartbeat = read_json(HEARTBEAT, {})
    age = now - float(heartbeat.get("epoch") or 0)
    state = read_json(WATCHDOG_STATE, {})
    last_restart = float(state.get("last_restart_epoch") or 0)
    startup_grace = False
    if not heartbeat:
        first_missing = float(state.get("first_missing_epoch") or now)
        state["first_missing_epoch"] = first_missing
        startup_grace = now - first_missing < MAX_AGE_SECONDS
        write_json(WATCHDOG_STATE, state)
    else:
        state.pop("first_missing_epoch", None)
        write_json(WATCHDOG_STATE, state)
    healthy = bool(heartbeat) and age <= MAX_AGE_SECONDS and heartbeat.get("state") == "running"
    result = {
        "checked_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "heartbeat_age_seconds": round(age, 1) if heartbeat else None,
        "healthy": healthy,
        "startup_grace": startup_grace,
        "restarted": False,
    }
    if not healthy and not startup_grace and now - last_restart >= COOLDOWN_SECONDS:
        domain = "gui/%s/%s" % (os.getuid(), LABEL)
        proc = subprocess.run(
            ["/bin/launchctl", "kickstart", "-k", domain],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        result.update(
            {
                "restarted": proc.returncode == 0,
                "restart_returncode": proc.returncode,
                "restart_output": (proc.stdout + proc.stderr)[-1000:],
            }
        )
        state["last_restart_epoch"] = now
        state["last_restart_result"] = result
        write_json(WATCHDOG_STATE, state)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if healthy or startup_grace or result.get("restarted") else 1


if __name__ == "__main__":
    raise SystemExit(main())
