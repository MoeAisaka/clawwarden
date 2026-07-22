#!/usr/bin/env python3
"""Durable control plane for unattended Clawwarden operation.

The control plane is deliberately split from the conversational agent:

* OpenClaw lifecycle hooks enqueue small, idempotent events.
* A launchd-supervised worker owns retries, leases and dead-letter handling.
* Low-risk housekeeping is automatic; protected actions remain policy-gated.
* Alerts are stateful and deduplicated before waking the OpenClaw session.

Only Python's standard library is used so the service can run under the macOS
system Python without a virtual environment.
"""

from __future__ import print_function

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "memory" / "clawwarden" / "control-plane"
DEFAULT_DB = STATE_DIR / "control-plane.sqlite3"
DEFAULT_CONFIG = STATE_DIR / "config.json"
STATUS_JSON = STATE_DIR / "status.json"
OUTBOX_LOG = STATE_DIR / "alert-outbox.jsonl"
HEARTBEAT_FILE = STATE_DIR / "worker-heartbeat.json"
DAEMON_LOCK = STATE_DIR / "worker.lock"
BACKUP_DIR = STATE_DIR / "backups"
CLAWWARDEN = ROOT / "scripts" / "clawwarden.py"
NMEM_ADAPTER = ROOT / "scripts" / "nmem_adapter.py"
ARTIFACT_HELPER = Path(os.environ.get("CLAWWARDEN_ARTIFACT_HELPER", ROOT / "scripts" / "artifact_helper.py"))
WORKFLOW_TASK_DIR = ROOT / "memory" / "workflow-ledger" / "tasks"
MEMORY_CANDIDATE_DIR = ROOT / "memory" / "nmem-adapter" / "candidates"
OPENCLAW_AGENTS_DIR = Path.home() / ".openclaw" / "agents"

LOW_INFORMATION_MEMORY_MARKERS = {
    "ack",
    "heartbeat_ok",
    "no_reply",
    "ok",
    "pong",
    "ready",
    "session_ready",
    "session_visible",
}

DEFAULT_CONFIG_VALUE = {
    "enabled": True,
    "kill_switch": False,
    "poll_seconds": 2,
    "lease_seconds": 180,
    "max_attempts": 5,
    "retry_base_seconds": 5,
    "retry_max_seconds": 300,
    "health_interval_seconds": 120,
    "memory_govern_interval_seconds": 300,
    "recovery_interval_seconds": 300,
    "patrol_interval_seconds": 1800,
    "artifact_cleanup_interval_seconds": 3600,
    "backup_interval_seconds": 21600,
    "stale_task_hours": 24,
    "runtime_reconcile_grace_seconds": 900,
    "runtime_reconcile_alert_seconds": 1800,
    "candidate_high_watermark": 200,
    "auto_commit_low_risk_memory": False,
    "auto_archive_low_information_memory": True,
    "auto_archive_exact_duplicates": True,
    "auto_archive_same_task_superseded": True,
    "auto_reconcile_completed_runs": True,
    "auto_resume_failed_runs": False,
    "max_auto_resume_attempts": 2,
    "auto_restart_gateway": False,
    "gateway_failure_threshold": 3,
    "gateway_restart_cooldown_seconds": 1800,
    "alert_cooldown_seconds": 7200,
    "alert_session_key": "agent:main:main",
    "backup_retention_days": 14,
    "backup_openclaw_config": False,
    "backup_nmem": False,
    "observation_started_at": None,
    "observation_days": 7,
    "observation_targets": {
        "managed_completion_rate_pct": 95,
        "runtime_reconciliation_unresolved": 0,
        "duplicate_side_effects_reported": 0,
        "low_information_pending": 0,
        "active_alerts": 0,
    },
    "protected_memory_patterns": [
        "trading", "交易", "strategy", "策略", "live", "实盘",
        "production", "生产", "credential", "password", "token", "cookie", "session",
        "route", "路由", "model", "模型", "openclaw", "clawwarden", "publish", "发布",
        "skill", "gateway", "nas", "network", "网络",
    ],
}

RETRYABLE_EVENT_TYPES = {
    "turn_start",
    "turn_finish",
    "tool_error",
    "health",
    "patrol",
    "memory_govern",
    "artifact_cleanup",
    "recover_stale",
    "backup",
    "resume_run",
}


def iso_now():
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def epoch_now():
    return time.time()


def atomic_write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, str(path))
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)


def append_jsonl(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_json(path, default=None):
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def merged_config(path=None):
    config = dict(DEFAULT_CONFIG_VALUE)
    loaded = read_json(path or DEFAULT_CONFIG, {})
    if isinstance(loaded, dict):
        config.update(loaded)
    return config


def compact_text(value, limit=6000):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def parse_datetime(value):
    try:
        text = str(value or "").strip().replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
        return parsed
    except Exception:
        return None


def normalized_memory_signal(value):
    text = compact_text(value, 500).casefold()
    text = re.sub(r"^[#>*`_\-\s]+", "", text)
    return text.strip("`*_ .,!?:;，。！？：；｜|")


def low_information_memory_reason(title="", content=""):
    normalized_values = [normalized_memory_signal(value) for value in (title, content)]
    for normalized in normalized_values:
        if normalized in LOW_INFORMATION_MEMORY_MARKERS:
            return "low_information_marker:%s" % normalized
        if (
            len(normalized) <= 200
            and normalized.startswith("ok")
            and any(marker in normalized for marker in ("gpt", "token", "nmem", "模型", "成本", "付费", "免费", "📊"))
        ):
            return "low_information_telemetry_ack"
    if (
        normalized_values[0]
        and normalized_values[0] == normalized_values[1]
        and re.fullmatch(
            r"[a-z0-9]+(?:[_-][a-z0-9]+)*_(?:ok|ready|pass|success)",
            normalized_values[0],
        )
    ):
        return "low_information_runtime_marker:%s" % normalized_values[0]
    return None


def title_from_prompt(prompt):
    text = compact_text(prompt, 1000)
    text = re.sub(r"^\[[^\]]+\]\s*", "", text)
    return (text[:80] or "OpenClaw runtime task").strip()


def stable_task_id(run_id, event_id):
    raw = str(run_id or event_id).encode("utf-8")
    return "runtime-" + hashlib.sha256(raw).hexdigest()[:20]


def subprocess_env():
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    return env


def run_command(argv, timeout=180, json_mode=False, check=True):
    proc = subprocess.run(
        [str(item) for item in argv],
        cwd=str(ROOT),
        env=subprocess_env(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            "command failed rc=%s: %s\nstderr=%s\nstdout=%s"
            % (proc.returncode, " ".join(map(str, argv)), proc.stderr[-2000:], proc.stdout[-2000:])
        )
    if json_mode:
        try:
            return json.loads(proc.stdout)
        except Exception as exc:
            raise RuntimeError("invalid JSON from %s: %s" % (argv[0], exc))
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


class ControlPlane(object):
    def __init__(self, db_path=None, config_path=None):
        self.db_path = Path(db_path or os.environ.get("CLAWWARDEN_CONTROL_DB") or DEFAULT_DB)
        self.config_path = Path(config_path or os.environ.get("CLAWWARDEN_CONTROL_CONFIG") or DEFAULT_CONFIG)
        self.config = merged_config(self.config_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        previous_umask = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        finally:
            os.umask(previous_umask)
        os.chmod(self.db_path, 0o600)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.init_schema()

    def close(self):
        self.conn.close()

    def init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 5,
                available_at REAL NOT NULL,
                lease_until REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                last_error TEXT,
                result_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_events_claim
              ON events(status, available_at, lease_until);

            CREATE TABLE IF NOT EXISTS runtime_runs (
                run_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                session_key TEXT,
                managed INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                resume_attempts INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts (
                fingerprint TEXT PRIMARY KEY,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                count INTEGER NOT NULL DEFAULT 1,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                last_notified REAL,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

    def set_meta(self, key, value):
        self.conn.execute(
            """INSERT INTO meta(key,value_json,updated_at) VALUES(?,?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at""",
            (key, json.dumps(value, ensure_ascii=False), iso_now()),
        )

    def get_meta(self, key, default=None):
        row = self.conn.execute("SELECT value_json FROM meta WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except Exception:
            return default

    def enqueue(self, event_id, event_type, payload, max_attempts=None, available_at=None):
        now = iso_now()
        attempts = int(max_attempts or self.config.get("max_attempts", 5))
        cursor = self.conn.execute(
            """INSERT OR IGNORE INTO events(
                 event_id,event_type,payload_json,status,attempts,max_attempts,
                 available_at,created_at,updated_at
               ) VALUES(?,?,?,'queued',0,?,?,?,?)""",
            (
                event_id,
                event_type,
                json.dumps(payload, ensure_ascii=False),
                attempts,
                float(available_at if available_at is not None else epoch_now()),
                now,
                now,
            ),
        )
        row = self.conn.execute("SELECT status,attempts FROM events WHERE event_id=?", (event_id,)).fetchone()
        return {
            "event_id": event_id,
            "event_type": event_type,
            "inserted": cursor.rowcount == 1,
            "status": row["status"],
            "attempts": row["attempts"],
        }

    def reclaim_expired(self):
        now_epoch = epoch_now()
        cursor = self.conn.execute(
            """UPDATE events SET status='retry', available_at=?, lease_until=NULL,
                 updated_at=?, last_error=COALESCE(last_error,'lease expired')
               WHERE status='running' AND lease_until IS NOT NULL AND lease_until < ?""",
            (now_epoch, iso_now(), now_epoch),
        )
        return cursor.rowcount

    def claim(self):
        now_epoch = epoch_now()
        lease = int(self.config.get("lease_seconds", 180))
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                """SELECT * FROM events
                   WHERE status IN ('queued','retry') AND available_at <= ?
                   ORDER BY available_at ASC, created_at ASC LIMIT 1""",
                (now_epoch,),
            ).fetchone()
            if not row:
                self.conn.execute("COMMIT")
                return None
            self.conn.execute(
                """UPDATE events SET status='running', attempts=attempts+1,
                     lease_until=?, updated_at=? WHERE event_id=?""",
                (now_epoch + lease, iso_now(), row["event_id"]),
            )
            claimed = self.conn.execute("SELECT * FROM events WHERE event_id=?", (row["event_id"],)).fetchone()
            self.conn.execute("COMMIT")
            return dict(claimed)
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def finish_event(self, event_id, result):
        event_row = self.conn.execute(
            "SELECT event_type,created_at FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        self.conn.execute(
            """UPDATE events SET status='done', lease_until=NULL, completed_at=?, updated_at=?,
                 result_json=?, last_error=NULL WHERE event_id=?""",
            (iso_now(), iso_now(), json.dumps(result, ensure_ascii=False), event_id),
        )
        if event_row:
            self.resolve_recovered_dead_event_alerts(
                event_row["event_type"], event_row["created_at"], event_id
            )

    def resolve_recovered_dead_event_alerts(self, event_type, success_created_at, success_event_id):
        """Resolve an older same-type dead-letter alert after a newer success."""
        rows = self.conn.execute(
            "SELECT fingerprint FROM alerts WHERE status='active' AND fingerprint LIKE 'dead-event:%'"
        ).fetchall()
        for row in rows:
            fingerprint = row["fingerprint"]
            dead_event_id = fingerprint[len("dead-event:") :]
            if dead_event_id == success_event_id:
                continue
            dead_event = self.conn.execute(
                "SELECT event_type,status,created_at FROM events WHERE event_id=?",
                (dead_event_id,),
            ).fetchone()
            if not dead_event:
                continue
            if dead_event["event_type"] != event_type or dead_event["status"] != "dead":
                continue
            if str(dead_event["created_at"]) >= str(success_created_at):
                continue
            self.resolve_alert(fingerprint)

    def fail_event(self, event, error):
        attempts = int(event["attempts"])
        max_attempts = int(event["max_attempts"])
        retryable = event["event_type"] in RETRYABLE_EVENT_TYPES
        if retryable and attempts < max_attempts:
            base = int(self.config.get("retry_base_seconds", 5))
            maximum = int(self.config.get("retry_max_seconds", 300))
            delay = min(maximum, base * (2 ** max(0, attempts - 1)))
            self.conn.execute(
                """UPDATE events SET status='retry', lease_until=NULL, available_at=?,
                     updated_at=?, last_error=? WHERE event_id=?""",
                (epoch_now() + delay, iso_now(), compact_text(error, 4000), event["event_id"]),
            )
            return "retry"
        self.conn.execute(
            """UPDATE events SET status='dead', lease_until=NULL, updated_at=?,
                 completed_at=?, last_error=? WHERE event_id=?""",
            (iso_now(), iso_now(), compact_text(error, 4000), event["event_id"]),
        )
        self.emit_alert(
            "dead-event:%s" % event["event_id"],
            "critical",
            "Clawwarden事件进入死信队列：%s (%s)" % (event["event_type"], compact_text(error, 800)),
        )
        return "dead"

    def process_one(self):
        event = self.claim()
        if not event:
            return None
        try:
            payload = json.loads(event["payload_json"])
            result = self.dispatch(event["event_type"], event["event_id"], payload)
            self.finish_event(event["event_id"], result)
            return {"event_id": event["event_id"], "status": "done", "result": result}
        except Exception as exc:
            detail = "%s\n%s" % (exc, traceback.format_exc(limit=8))
            state = self.fail_event(event, detail)
            return {"event_id": event["event_id"], "status": state, "error": str(exc)}

    def dispatch(self, event_type, event_id, payload):
        handlers = {
            "turn_start": self.handle_turn_start,
            "turn_finish": self.handle_turn_finish,
            "tool_error": self.handle_tool_error,
            "gateway_start": self.handle_gateway_start,
            "gateway_stop": self.handle_gateway_stop,
            "health": self.handle_health,
            "patrol": self.handle_patrol,
            "memory_govern": self.handle_memory_govern,
            "artifact_cleanup": self.handle_artifact_cleanup,
            "recover_stale": self.handle_recover_stale,
            "backup": self.handle_backup,
            "resume_run": self.handle_resume_run,
        }
        handler = handlers.get(event_type)
        if not handler:
            return {"action": "ignored_unknown_event", "event_type": event_type}
        return handler(event_id, payload)

    def upsert_runtime_run(self, run_id, task_id, session_key, managed, status, event_id):
        now = iso_now()
        self.conn.execute(
            """INSERT INTO runtime_runs(
                 run_id,task_id,session_key,managed,status,last_event_id,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 task_id=excluded.task_id, session_key=COALESCE(excluded.session_key,runtime_runs.session_key),
                 managed=excluded.managed, status=excluded.status,
                 last_event_id=excluded.last_event_id, updated_at=excluded.updated_at""",
            (run_id, task_id, session_key, 1 if managed else 0, status, event_id, now, now),
        )

    def runtime_run(self, run_id):
        row = self.conn.execute("SELECT * FROM runtime_runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def handle_turn_start(self, event_id, payload):
        run_id = str(payload.get("runId") or event_id)
        prompt = compact_text(payload.get("prompt"), 8000)
        recovery_of = str(payload.get("recoveryOfRunId") or "").strip()
        if recovery_of:
            parent = self.runtime_run(recovery_of)
            requested_task_id = str(payload.get("recoveryTaskId") or "").strip()
            if parent and parent.get("managed") and (
                not requested_task_id or requested_task_id == parent.get("task_id")
            ):
                task_id = parent["task_id"]
                result = run_command(
                    [
                        CLAWWARDEN,
                        "hook",
                        "checkpoint",
                        "--execute",
                        "--event-id",
                        event_id,
                        "--source",
                        "openclaw-runtime-plugin",
                        "--task-id",
                        task_id,
                        "--summary",
                        "Automatic recovery turn started",
                        "--status",
                        "in_progress",
                        "--next-step",
                        "Verify external state, continue from the last checkpoint, and avoid duplicate side effects",
                    ],
                    timeout=180,
                    json_mode=True,
                )
                self.upsert_runtime_run(
                    run_id,
                    task_id,
                    payload.get("sessionKey") or parent.get("session_key"),
                    True,
                    "running",
                    event_id,
                )
                self.conn.execute(
                    "UPDATE runtime_runs SET resume_attempts=? WHERE run_id=?",
                    (int(parent.get("resume_attempts", 0)), run_id),
                )
                self.conn.execute(
                    "UPDATE runtime_runs SET status='resumed', updated_at=? WHERE run_id=?",
                    (iso_now(), recovery_of),
                )
                return {
                    "action": "recovery_turn_start",
                    "managed": True,
                    "task_id": task_id,
                    "recovery_of": recovery_of,
                    "hook": result,
                }
        if payload.get("gatewayContinuation"):
            session_key = str(payload.get("sessionKey") or "").strip()
            parent = None
            if session_key:
                row = self.conn.execute(
                    """SELECT * FROM runtime_runs
                       WHERE session_key=? AND managed=1
                         AND status IN ('running','waiting','resuming') AND run_id<>?
                       ORDER BY updated_at DESC LIMIT 1""",
                    (session_key, run_id),
                ).fetchone()
                parent = dict(row) if row else None
            if not parent:
                task_id = stable_task_id(run_id, event_id)
                self.upsert_runtime_run(run_id, task_id, session_key, False, "unmanaged", event_id)
                return {
                    "action": "skip_orphan_gateway_continuation",
                    "managed": False,
                    "task_id": task_id,
                }
            task_id = parent["task_id"]
            result = run_command(
                [
                    CLAWWARDEN,
                    "hook",
                    "checkpoint",
                    "--execute",
                    "--event-id",
                    event_id,
                    "--source",
                    "openclaw-runtime-plugin",
                    "--task-id",
                    task_id,
                    "--summary",
                    "Gateway interruption continuation started",
                    "--status",
                    "in_progress",
                    "--next-step",
                    "Continue the interrupted turn from the latest verified checkpoint",
                ],
                timeout=180,
                json_mode=True,
            )
            self.upsert_runtime_run(run_id, task_id, session_key, True, "running", event_id)
            self.conn.execute(
                "UPDATE runtime_runs SET resume_attempts=? WHERE run_id=?",
                (int(parent.get("resume_attempts", 0)), run_id),
            )
            self.conn.execute(
                "UPDATE runtime_runs SET status='resumed', updated_at=? WHERE run_id=?",
                (iso_now(), parent["run_id"]),
            )
            return {
                "action": "gateway_continuation_start",
                "managed": True,
                "task_id": task_id,
                "recovery_of": parent["run_id"],
                "hook": result,
            }
        task_id = stable_task_id(run_id, event_id)
        title = title_from_prompt(prompt)
        result = run_command(
            [
                CLAWWARDEN,
                "hook",
                "turn-start",
                "--execute",
                "--event-id",
                event_id,
                "--source",
                "openclaw-runtime-plugin",
                "--task-id",
                task_id,
                "--title",
                title,
                "--goal",
                "Complete and verify the OpenClaw runtime turn",
                "--query",
                prompt or title,
                "--mode",
                "auto",
                "--current-step",
                "Runtime turn started",
                "--next-step",
                "Execute the requested work and close the runtime turn",
            ],
            timeout=240,
            json_mode=True,
        )
        original = result.get("original") if result.get("duplicate") else result
        managed = bool((original or {}).get("outputs", {}).get("start"))
        self.upsert_runtime_run(
            run_id,
            task_id,
            payload.get("sessionKey"),
            managed,
            "running" if managed else "unmanaged",
            event_id,
        )
        return {"action": "turn_start", "managed": managed, "task_id": task_id, "hook": result}

    def handle_turn_finish(self, event_id, payload):
        run_id = str(payload.get("runId") or "")
        record = self.runtime_run(run_id)
        if not record or not record.get("managed"):
            return {"action": "skip_unmanaged_finish", "run_id": run_id}
        task_id = record["task_id"]
        success = bool(payload.get("success", True))
        summary = compact_text(payload.get("summary") or payload.get("error") or "Runtime turn finished", 6000)
        memory_suppression_reason = low_information_memory_reason(summary, summary)
        if success:
            command = [
                CLAWWARDEN,
                "hook",
                "turn-finish",
                "--execute",
                "--event-id",
                event_id,
                "--source",
                "openclaw-runtime-plugin",
                "--task-id",
                task_id,
                "--title",
                title_from_prompt(summary),
                "--summary",
                summary,
                "--status",
                "completed",
                "--status-reason",
                "OpenClaw runtime turn completed successfully",
            ]
            if memory_suppression_reason:
                command.append("--suppress-memory-candidate")
            result = run_command(command, timeout=240, json_mode=True)
            status = "completed"
        else:
            result = run_command(
                [
                    CLAWWARDEN,
                    "hook",
                    "checkpoint",
                    "--execute",
                    "--event-id",
                    event_id,
                    "--source",
                    "openclaw-runtime-plugin",
                    "--task-id",
                    task_id,
                    "--summary",
                    "Runtime turn failed: " + summary,
                    "--status",
                    "waiting",
                    "--next-step",
                    "Resume from the last successful transcript checkpoint",
                ],
                timeout=180,
                json_mode=True,
            )
            status = "waiting"
            if self.config.get("auto_resume_failed_runs"):
                resume_id = "resume:%s:%s" % (run_id, int(record.get("resume_attempts", 0)) + 1)
                self.enqueue(
                    resume_id,
                    "resume_run",
                    {
                        "runId": run_id,
                        "taskId": task_id,
                        "sessionKey": record.get("session_key"),
                        "error": summary,
                    },
                    max_attempts=3,
                    available_at=epoch_now() + 30,
                )
        self.conn.execute(
            "UPDATE runtime_runs SET status=?, last_event_id=?, updated_at=? WHERE run_id=?",
            (status, event_id, iso_now(), run_id),
        )
        return {
            "action": "turn_finish",
            "status": status,
            "task_id": task_id,
            "memory_candidate_suppressed": bool(success and memory_suppression_reason),
            "memory_suppression_reason": memory_suppression_reason if success else None,
            "hook": result,
        }

    def handle_tool_error(self, event_id, payload):
        run_id = str(payload.get("runId") or "")
        record = self.runtime_run(run_id)
        if not record or not record.get("managed"):
            return {"action": "skip_unmanaged_tool_error"}
        summary = "Tool %s failed: %s" % (
            payload.get("toolName") or "unknown",
            compact_text(payload.get("error"), 1200),
        )
        result = run_command(
            [
                CLAWWARDEN,
                "checkpoint",
                record["task_id"],
                "--summary",
                summary,
                "--status",
                "in_progress",
                "--next-step",
                "Retry safely or choose a verified alternative",
            ],
            timeout=120,
            json_mode=True,
        )
        return {"action": "tool_error_checkpoint", "task_id": record["task_id"], "result": result}

    def handle_gateway_start(self, event_id, payload):
        self.set_meta("gateway", {"status": "running", "event_id": event_id, "payload": payload, "at": iso_now()})
        self.resolve_alert("gateway-unhealthy")
        return {"action": "gateway_start_recorded"}

    def handle_gateway_stop(self, event_id, payload):
        self.set_meta("gateway", {"status": "stopped", "event_id": event_id, "payload": payload, "at": iso_now()})
        return {"action": "gateway_stop_recorded"}

    def gateway_healthy(self):
        result = run_command(["openclaw", "gateway", "status"], timeout=20, check=False)
        text = (result.get("stdout") or "") + "\n" + (result.get("stderr") or "")
        return result["returncode"] == 0 and "Connectivity probe: ok" in text, compact_text(text, 1800)

    def maybe_restart_gateway(self):
        if not self.config.get("auto_restart_gateway"):
            return {"attempted": False, "reason": "disabled"}
        last_restart = float(self.get_meta("gateway_last_restart_epoch", 0) or 0)
        cooldown = int(self.config.get("gateway_restart_cooldown_seconds", 1800))
        if epoch_now() - last_restart < cooldown:
            return {"attempted": False, "reason": "cooldown"}
        self.set_meta("gateway_last_restart_epoch", epoch_now())
        result = run_command(["openclaw", "gateway", "restart"], timeout=60, check=False)
        return {"attempted": True, "returncode": result["returncode"], "output": compact_text(result.get("stdout"), 1000)}

    def handle_health(self, event_id, payload):
        gateway_ok, gateway_detail = self.gateway_healthy()
        nmem = run_command(["nmem", "status"], timeout=20, check=False)
        nmem_ok = nmem["returncode"] == 0 and "status   ok" in nmem.get("stdout", "")
        failures = int(self.get_meta("gateway_consecutive_failures", 0) or 0)
        restart = None
        if gateway_ok:
            failures = 0
            self.resolve_alert("gateway-unhealthy")
        else:
            failures += 1
            if failures >= int(self.config.get("gateway_failure_threshold", 3)):
                restart = self.maybe_restart_gateway()
                self.emit_alert("gateway-unhealthy", "critical", "OpenClaw Gateway连续探测失败：" + gateway_detail)
        self.set_meta("gateway_consecutive_failures", failures)
        if nmem_ok:
            self.resolve_alert("nmem-unhealthy")
        else:
            self.emit_alert("nmem-unhealthy", "critical", "nmem状态异常：" + compact_text(nmem.get("stdout") or nmem.get("stderr"), 1000))
        result = {
            "action": "health",
            "gateway_ok": gateway_ok,
            "gateway_failures": failures,
            "nmem_ok": nmem_ok,
            "gateway_restart": restart,
        }
        self.set_meta("last_health", {"at": iso_now(), **result})
        return result

    def handle_patrol(self, event_id, payload):
        result = run_command(
            [CLAWWARDEN, "patrol", "--limit", "200", "--stale-hours", str(self.config.get("stale_task_hours", 24))],
            timeout=240,
            json_mode=True,
        )
        active_fingerprints = set()
        for alert in result.get("alerts", []):
            severity = alert.get("severity", "info")
            message = str(alert.get("message") or "Clawwarden alert")
            if "workflow task" in message and "stale" in message:
                fingerprint = "stale-workflow-tasks"
            else:
                fingerprint = "patrol:" + hashlib.sha256(message.encode("utf-8")).hexdigest()[:16]
            active_fingerprints.add(fingerprint)
            if severity in ("warning", "critical"):
                self.emit_alert(fingerprint, severity, message)
        rows = self.conn.execute("SELECT fingerprint FROM alerts WHERE fingerprint LIKE 'patrol:%' AND status='active'").fetchall()
        for row in rows:
            if row[0] not in active_fingerprints:
                self.resolve_alert(row[0])
        return {"action": "patrol", "overall": result.get("overall"), "severity": result.get("severity"), "alerts": result.get("alerts", [])}

    def protected_candidate(self, candidate):
        labels = {str(item).lower() for item in candidate.get("labels") or []}
        if labels.intersection({"manual-only", "protected", "production", "trading", "credential"}):
            return True
        haystack = "\n".join(
            [
                str(candidate.get("title") or ""),
                str(candidate.get("content") or ""),
            ]
        ).lower()
        return any(str(pattern).lower() in haystack for pattern in self.config.get("protected_memory_patterns", []))

    def archive_candidate(self, path, reason, superseded_by=None):
        candidate = read_json(path, {})
        candidate["status"] = "archived"
        candidate["archived_at"] = iso_now()
        candidate["archive_reason"] = reason
        if superseded_by:
            candidate["superseded_by"] = superseded_by
        atomic_write_json(path, candidate)

    def handle_memory_govern(self, event_id, payload):
        paths = sorted(MEMORY_CANDIDATE_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime)
        pending = []
        for path in paths:
            candidate = read_json(path, {})
            if candidate.get("status") == "pending":
                pending.append((path, candidate))

        archived = []
        archived_low_information = []
        superseded = []
        committed = []
        protected = []
        exact = {}
        if self.config.get("auto_archive_low_information_memory"):
            for path, candidate in pending:
                reason = low_information_memory_reason(
                    candidate.get("title"), candidate.get("content")
                )
                if not reason:
                    continue
                self.archive_candidate(path, reason)
                archived_low_information.append(candidate.get("candidate_id") or path.stem)

        for path, candidate in pending:
            candidate = read_json(path, {})
            if candidate.get("status") != "pending":
                continue
            normalized = re.sub(r"\s+", " ", str(candidate.get("content") or "")).strip()
            digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            if normalized and digest in exact and self.config.get("auto_archive_exact_duplicates"):
                keeper_path, keeper = exact[digest]
                self.archive_candidate(path, "exact_duplicate", keeper.get("candidate_id") or keeper_path.stem)
                archived.append(candidate.get("candidate_id") or path.stem)
            else:
                exact[digest] = (path, candidate)

        if self.config.get("auto_archive_same_task_superseded"):
            groups = {}
            for path, candidate in pending:
                current = read_json(path, {})
                if current.get("status") != "pending":
                    continue
                task_id = str(current.get("task_id") or "").strip()
                title = re.sub(r"\s+", " ", str(current.get("title") or "")).strip()
                if not task_id or not title:
                    continue
                groups.setdefault((task_id, title), []).append((path, current))
            for _key, group in groups.items():
                if len(group) < 2:
                    continue
                group.sort(key=lambda item: str(item[1].get("created_at") or ""))
                keeper_path, keeper = group[-1]
                keeper_id = keeper.get("candidate_id") or keeper_path.stem
                for duplicate_path, duplicate in group[:-1]:
                    self.archive_candidate(duplicate_path, "same_task_superseded", keeper_id)
                    superseded.append(duplicate.get("candidate_id") or duplicate_path.stem)

        for path, candidate in pending:
            if candidate.get("candidate_id") in archived or read_json(path, {}).get("status") != "pending":
                continue
            if candidate.get("recommendation") != "auto_commit_allowed" or candidate.get("risk_level") != "low":
                continue
            if self.protected_candidate(candidate):
                protected.append(candidate.get("candidate_id") or path.stem)
                continue
            if not self.config.get("auto_commit_low_risk_memory"):
                continue
            result = run_command([NMEM_ADAPTER, "commit", str(path)], timeout=120, check=True)
            committed.append({"candidate_id": candidate.get("candidate_id") or path.stem, "result": compact_text(result.get("stdout"), 500)})

        remaining = sum(1 for path in MEMORY_CANDIDATE_DIR.glob("*.json") if read_json(path, {}).get("status") == "pending")
        if remaining >= int(self.config.get("candidate_high_watermark", 200)):
            self.emit_alert("memory-high-watermark", "warning", "Clawwarden待审记忆达到高水位：%s" % remaining)
        else:
            self.resolve_alert("memory-high-watermark")
        result = {
            "action": "memory_govern",
            "pending_before": len(pending),
            "archived_low_information": archived_low_information,
            "archived_exact_duplicates": archived,
            "archived_same_task_superseded": superseded,
            "auto_committed": committed,
            "protected_auto_commit_candidates": protected,
            "pending_after": remaining,
        }
        totals = self.get_meta(
            "memory_govern_totals",
            {},
        )
        if not totals:
            previous = self.get_meta("last_memory_govern", {})
            totals = {
                "archived_low_information": len(previous.get("archived_low_information") or []),
                "archived_duplicates": len(previous.get("archived_exact_duplicates") or [])
                + len(previous.get("archived_same_task_superseded") or []),
                "auto_committed": len(previous.get("auto_committed") or []),
            }
        totals["archived_low_information"] = int(totals.get("archived_low_information", 0)) + len(archived_low_information)
        totals["archived_duplicates"] = int(totals.get("archived_duplicates", 0)) + len(archived) + len(superseded)
        totals["auto_committed"] = int(totals.get("auto_committed", 0)) + len(committed)
        self.set_meta("memory_govern_totals", {"at": iso_now(), **totals})
        self.set_meta("last_memory_govern", {"at": iso_now(), **result})
        return result

    def handle_artifact_cleanup(self, event_id, payload):
        if not ARTIFACT_HELPER.is_file():
            return {"action": "artifact_cleanup", "status": "disabled"}
        result = run_command([ARTIFACT_HELPER, "cleanup-local", "--limit", "5000", "--report-limit", "100"], timeout=180, json_mode=True)
        return {"action": "artifact_cleanup", "result": result}

    def session_store_path(self, session_key):
        parts = str(session_key or "").split(":", 2)
        if len(parts) < 3 or parts[0] != "agent" or not parts[1]:
            return None
        agents_dir = Path(
            os.path.expanduser(str(self.config.get("openclaw_agents_dir") or OPENCLAW_AGENTS_DIR))
        )
        return agents_dir / parts[1] / "sessions" / "sessions.json"

    def reconcile_runtime_runs(self, event_id):
        if not self.config.get("auto_reconcile_completed_runs"):
            return {"enabled": False, "inspected": 0, "reconciled": [], "unresolved": []}

        grace_seconds = max(60, int(self.config.get("runtime_reconcile_grace_seconds", 900)))
        alert_seconds = max(
            grace_seconds,
            int(self.config.get("runtime_reconcile_alert_seconds", 1800)),
        )
        now_epoch = epoch_now()
        rows = self.conn.execute(
            """SELECT * FROM runtime_runs
               WHERE managed=1 AND status='running'
               ORDER BY created_at ASC"""
        ).fetchall()
        stores = {}
        reconciled = []
        unresolved = []
        inspected = 0

        for raw in rows:
            record = dict(raw)
            created = parse_datetime(record.get("created_at"))
            if not created:
                continue
            age_seconds = max(0, now_epoch - created.timestamp())
            if age_seconds < grace_seconds:
                continue
            inspected += 1
            session_key = str(record.get("session_key") or "")
            store_path = self.session_store_path(session_key)
            if not store_path:
                unresolved.append({"run_id": record["run_id"], "reason": "invalid_session_key", "age_seconds": int(age_seconds)})
                continue
            store_key = str(store_path)
            if store_key not in stores:
                stores[store_key] = read_json(store_path, None)
            store = stores[store_key]
            entry = store.get(session_key) if isinstance(store, dict) else None
            if not isinstance(entry, dict):
                unresolved.append({"run_id": record["run_id"], "reason": "session_entry_missing", "age_seconds": int(age_seconds)})
                continue

            session_status = str(entry.get("status") or "").casefold()
            session_updated_ms = float(entry.get("updatedAt") or 0)
            if session_updated_ms and session_updated_ms < created.timestamp() * 1000:
                unresolved.append({"run_id": record["run_id"], "reason": "session_index_older_than_run", "age_seconds": int(age_seconds)})
                continue
            if session_status == "running":
                continue
            if session_status != "done" or bool(entry.get("abortedLastRun")):
                unresolved.append({
                    "run_id": record["run_id"],
                    "reason": "session_not_cleanly_done:%s" % (session_status or "unknown"),
                    "age_seconds": int(age_seconds),
                })
                continue

            reconcile_event_id = "%s:runtime:%s" % (event_id, record["run_id"])
            summary = "Reconciled completed OpenClaw session after a missing terminal lifecycle event"
            result = run_command(
                [
                    CLAWWARDEN,
                    "hook",
                    "turn-finish",
                    "--execute",
                    "--event-id",
                    reconcile_event_id,
                    "--source",
                    "clawwarden-runtime-reconciler",
                    "--task-id",
                    record["task_id"],
                    "--title",
                    "Runtime reconciliation",
                    "--summary",
                    summary,
                    "--status",
                    "completed",
                    "--status-reason",
                    "OpenClaw session index is cleanly done after the reconciliation grace period",
                    "--suppress-memory-candidate",
                ],
                timeout=240,
                json_mode=True,
            )
            self.conn.execute(
                """UPDATE runtime_runs
                   SET status='completed_reconciled', last_event_id=?, updated_at=?
                   WHERE run_id=? AND status='running'""",
                (reconcile_event_id, iso_now(), record["run_id"]),
            )
            reconciled.append({
                "run_id": record["run_id"],
                "task_id": record["task_id"],
                "session_key": session_key,
                "age_seconds": int(age_seconds),
                "hook_action": result.get("action"),
            })

        alertable = [item for item in unresolved if item["age_seconds"] >= alert_seconds]
        if alertable:
            self.emit_alert(
                "runtime-reconciliation-blocked",
                "warning",
                "Clawwarden有%s个运行记录无法安全对账：%s"
                % (len(alertable), ", ".join(item["run_id"] for item in alertable[:8])),
            )
        else:
            self.resolve_alert("runtime-reconciliation-blocked")

        result = {
            "enabled": True,
            "grace_seconds": grace_seconds,
            "inspected": inspected,
            "reconciled": reconciled,
            "unresolved": unresolved,
        }
        totals = self.get_meta("runtime_reconciliation_totals", {})
        if not totals:
            previous = self.get_meta("last_runtime_reconciliation", {})
            totals = {"reconciled": len(previous.get("reconciled") or [])}
        totals["reconciled"] = int(totals.get("reconciled", 0)) + len(reconciled)
        self.set_meta("runtime_reconciliation_totals", {"at": iso_now(), **totals})
        self.set_meta("last_runtime_reconciliation", {"at": iso_now(), **result})
        return result

    def handle_recover_stale(self, event_id, payload):
        runtime_reconciliation = self.reconcile_runtime_runs(event_id)
        stale_hours = float(self.config.get("stale_task_hours", 24))
        cutoff = dt.datetime.now().astimezone() - dt.timedelta(hours=stale_hours)
        stale = []
        resume_scheduled = []
        for path in WORKFLOW_TASK_DIR.glob("*.json"):
            task = read_json(path, {})
            if task.get("status") not in ("planned", "in_progress", "waiting"):
                continue
            try:
                updated = dt.datetime.fromisoformat(str(task.get("updated_at")))
                if updated.tzinfo is None:
                    updated = updated.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
            except Exception:
                continue
            if updated > cutoff:
                continue
            item = {"task_id": task.get("task_id"), "title": task.get("title"), "status": task.get("status")}
            stale.append(item)
            if not self.config.get("auto_resume_failed_runs"):
                continue
            runtime = self.conn.execute(
                """SELECT * FROM runtime_runs
                   WHERE task_id=? AND managed=1 AND status IN ('running','waiting','resuming')
                   ORDER BY updated_at DESC LIMIT 1""",
                (task.get("task_id"),),
            ).fetchone()
            if not runtime or not runtime["session_key"]:
                continue
            attempts = int(runtime["resume_attempts"])
            if attempts >= int(self.config.get("max_auto_resume_attempts", 2)):
                continue
            resume_id = "resume:%s:%s" % (runtime["run_id"], attempts + 1)
            enqueue_result = self.enqueue(
                resume_id,
                "resume_run",
                {
                    "runId": runtime["run_id"],
                    "taskId": runtime["task_id"],
                    "sessionKey": runtime["session_key"],
                    "error": "Runtime task became stale without a terminal lifecycle event",
                },
                max_attempts=3,
                available_at=epoch_now() + 5,
            )
            if enqueue_result.get("inserted"):
                resume_scheduled.append(runtime["task_id"])
        if stale:
            self.emit_alert("stale-workflow-tasks", "warning", "Clawwarden发现%s个超时工作流任务：%s" % (len(stale), ", ".join(item["task_id"] for item in stale[:8])))
        else:
            self.resolve_alert("stale-workflow-tasks")
        return {
            "action": "recover_stale",
            "runtime_reconciliation": runtime_reconciliation,
            "stale": stale,
            "resume_scheduled": resume_scheduled,
        }

    def handle_resume_run(self, event_id, payload):
        run_id = str(payload.get("runId") or "")
        record = self.runtime_run(run_id)
        if not record:
            return {"action": "skip_missing_runtime_run"}
        if str(record.get("status") or "").startswith("completed"):
            return {"action": "skip_completed_runtime_run"}
        attempts = int(record.get("resume_attempts", 0))
        if attempts >= int(self.config.get("max_auto_resume_attempts", 2)):
            self.emit_alert("resume-exhausted:%s" % run_id, "warning", "Clawwarden自动恢复次数耗尽：%s" % record.get("task_id"))
            return {"action": "resume_exhausted", "attempts": attempts}
        session_key = payload.get("sessionKey") or record.get("session_key")
        if not session_key:
            raise RuntimeError("resume_run missing session key")
        message = (
            "[CLAWWARDEN_RECOVERY run=%s task=%s event=%s] 上一轮因可恢复错误中断。请从现有转录和最近检查点继续原任务；"
            "先核验外部状态，禁止重复已完成的副作用。任务ID：%s。错误摘要：%s"
            % (run_id, record.get("task_id"), event_id, record.get("task_id"), compact_text(payload.get("error"), 800))
        )
        result = run_command(
            [
                "openclaw",
                "system",
                "event",
                "--mode",
                "now",
                "--expect-final",
                "--session-key",
                session_key,
                "--text",
                message,
                "--timeout",
                "120000",
                "--json",
            ],
            timeout=140,
            check=True,
        )
        self.conn.execute(
            "UPDATE runtime_runs SET resume_attempts=resume_attempts+1, status='resuming', updated_at=? WHERE run_id=?",
            (iso_now(), run_id),
        )
        return {"action": "resume_enqueued", "session_key": session_key, "result": compact_text(result.get("stdout"), 1000)}

    def handle_backup(self, event_id, payload):
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        snapshot_dir = BACKUP_DIR / stamp
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        target = snapshot_dir / "control-plane.sqlite3"
        backup_conn = sqlite3.connect(str(target))
        try:
            self.conn.backup(backup_conn)
        finally:
            backup_conn.close()
        os.chmod(str(target), 0o600)

        config_source = Path.home() / ".openclaw" / "openclaw.json"
        if self.config.get("backup_openclaw_config") and config_source.exists():
            shutil.copy2(str(config_source), str(snapshot_dir / "openclaw.json"))
            os.chmod(str(snapshot_dir / "openclaw.json"), 0o600)

        state_archive = snapshot_dir / "clawwarden-state.tar.gz"
        with tarfile.open(str(state_archive), "w:gz") as archive:
            for source, arcname in (
                (ROOT / "memory" / "workflow-ledger", "workflow-ledger"),
                (ROOT / "memory" / "nmem-adapter" / "candidates", "nmem-candidates"),
                (ROOT / "memory" / "clawwarden" / "runtime-hooks.json", "runtime-hooks.json"),
                (ROOT / "memory" / "clawwarden" / "runtime-integration.json", "runtime-integration.json"),
            ):
                if source.exists():
                    archive.add(str(source), arcname=arcname, recursive=True)
        os.chmod(str(state_archive), 0o600)

        nmem_export = None
        if self.config.get("backup_nmem"):
            nmem_export = snapshot_dir / "nmem-export.zip"
            run_command(["nmem", "export", str(nmem_export), "--overwrite"], timeout=600, check=True)
            if nmem_export.exists():
                os.chmod(str(nmem_export), 0o600)

        cutoff = epoch_now() - int(self.config.get("backup_retention_days", 14)) * 86400
        removed = []
        for path in BACKUP_DIR.iterdir():
            if path.stat().st_mtime < cutoff:
                if path.is_dir():
                    shutil.rmtree(str(path))
                else:
                    path.unlink()
                removed.append(path.name)
        return {
            "action": "backup",
            "snapshot_dir": str(snapshot_dir),
            "database": str(target),
            "state_archive": str(state_archive),
            "nmem_export": str(nmem_export) if nmem_export else None,
            "removed": removed,
        }

    def emit_alert(self, fingerprint, severity, message):
        now_iso = iso_now()
        now_epoch = epoch_now()
        row = self.conn.execute("SELECT * FROM alerts WHERE fingerprint=?", (fingerprint,)).fetchone()
        cooldown = int(self.config.get("alert_cooldown_seconds", 7200))
        should_notify = row is None or row["status"] != "active" or row["message"] != message or not row["last_notified"] or now_epoch - float(row["last_notified"]) >= cooldown
        if row:
            self.conn.execute(
                """UPDATE alerts SET severity=?,message=?,status='active',count=count+1,
                     last_seen=?,resolved_at=NULL WHERE fingerprint=?""",
                (severity, message, now_iso, fingerprint),
            )
        else:
            self.conn.execute(
                """INSERT INTO alerts(fingerprint,severity,message,status,count,first_seen,last_seen)
                   VALUES(?,?,?,'active',1,?,?)""",
                (fingerprint, severity, message, now_iso, now_iso),
            )
        if should_notify:
            self.conn.execute("UPDATE alerts SET last_notified=? WHERE fingerprint=?", (now_epoch, fingerprint))
            self.notify(fingerprint, severity, message)
        return should_notify

    def resolve_alert(self, fingerprint):
        self.conn.execute(
            "UPDATE alerts SET status='resolved',resolved_at=?,last_seen=? WHERE fingerprint=? AND status='active'",
            (iso_now(), iso_now(), fingerprint),
        )

    def notify(self, fingerprint, severity, message):
        payload = {"at": iso_now(), "fingerprint": fingerprint, "severity": severity, "message": message}
        append_jsonl(OUTBOX_LOG, payload)
        if severity not in ("warning", "critical"):
            return
        session_key = str(self.config.get("alert_session_key") or "agent:main:main")
        text = "route:light\n[CLAWWARDEN_ALERT] %s｜%s\n告警指纹：%s。请原样简短通知用户，不执行修复。" % (severity.upper(), message, fingerprint)
        try:
            subprocess.Popen(
                [
                    "openclaw",
                    "system",
                    "event",
                    "--mode",
                    "now",
                    "--session-key",
                    session_key,
                    "--text",
                    text,
                ],
                cwd=str(ROOT),
                env=subprocess_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception:
            pass
        with contextlib.suppress(Exception):
            subprocess.Popen(
                ["osascript", "-e", 'display notification %s with title "Clawwarden"' % json.dumps(message[:180])],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )

    def enqueue_periodic(self, name, interval, event_type):
        bucket = int(epoch_now() // max(1, int(interval)))
        event_id = "periodic:%s:%s" % (name, bucket)
        return self.enqueue(event_id, event_type, {"scheduledAt": iso_now(), "bucket": bucket})

    def schedule_periodic(self):
        self.enqueue_periodic("health", self.config.get("health_interval_seconds", 120), "health")
        self.enqueue_periodic("memory-govern", self.config.get("memory_govern_interval_seconds", 300), "memory_govern")
        self.enqueue_periodic("recover-stale", self.config.get("recovery_interval_seconds", 300), "recover_stale")
        self.enqueue_periodic("patrol", self.config.get("patrol_interval_seconds", 1800), "patrol")
        self.enqueue_periodic("artifact-cleanup", self.config.get("artifact_cleanup_interval_seconds", 3600), "artifact_cleanup")
        self.enqueue_periodic("backup", self.config.get("backup_interval_seconds", 21600), "backup")

    def status(self):
        queues = {row["status"]: row["count"] for row in self.conn.execute("SELECT status,COUNT(*) AS count FROM events GROUP BY status")}
        active_alerts = [dict(row) for row in self.conn.execute("SELECT fingerprint,severity,message,count,last_seen FROM alerts WHERE status='active' ORDER BY last_seen DESC")]
        runs = {row["status"]: row["count"] for row in self.conn.execute("SELECT status,COUNT(*) AS count FROM runtime_runs GROUP BY status")}
        event_metrics = dict(
            self.conn.execute(
                """SELECT COUNT(*) AS total,
                          SUM(CASE WHEN attempts>1 THEN attempts-1 ELSE 0 END) AS retries,
                          SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END) AS dead
                   FROM events"""
            ).fetchone()
        )
        run_metrics = dict(
            self.conn.execute(
                """SELECT
                     SUM(CASE WHEN managed=1 THEN 1 ELSE 0 END) AS managed_total,
                     SUM(CASE WHEN managed=1 AND status IN ('completed','completed_reconciled') THEN 1 ELSE 0 END) AS completed_total,
                     SUM(CASE WHEN managed=1 AND status='completed_reconciled' THEN 1 ELSE 0 END) AS reconciled_total,
                     SUM(CASE WHEN managed=1 AND status IN ('running','waiting','resuming') THEN 1 ELSE 0 END) AS open_total
                   FROM runtime_runs"""
            ).fetchone()
        )
        managed_total = int(run_metrics.get("managed_total") or 0)
        completed_total = int(run_metrics.get("completed_total") or 0)
        memory_pending_total = 0
        low_information_pending = 0
        for path in MEMORY_CANDIDATE_DIR.glob("*.json"):
            candidate = read_json(path, {})
            if candidate.get("status") != "pending":
                continue
            memory_pending_total += 1
            if low_information_memory_reason(candidate.get("title"), candidate.get("content")):
                low_information_pending += 1
        reconciliation = self.get_meta("last_runtime_reconciliation", {})
        operational_metrics = {
            "events_total": int(event_metrics.get("total") or 0),
            "event_retries": int(event_metrics.get("retries") or 0),
            "dead_events": int(event_metrics.get("dead") or 0),
            "managed_runs_total": managed_total,
            "managed_completed_total": completed_total,
            "managed_completion_rate_pct": round(100.0 * completed_total / managed_total, 2) if managed_total else 100.0,
            "managed_reconciled_total": int(run_metrics.get("reconciled_total") or 0),
            "managed_open_total": int(run_metrics.get("open_total") or 0),
            "runtime_reconciliation_unresolved": len(reconciliation.get("unresolved") or []),
            "duplicate_side_effects_reported": int(self.get_meta("duplicate_side_effects_reported", 0) or 0),
            "memory_pending_total": memory_pending_total,
            "low_information_pending": low_information_pending,
            "active_alerts": len(active_alerts),
            "memory_govern_totals": self.get_meta("memory_govern_totals", {}),
            "runtime_reconciliation_totals": self.get_meta("runtime_reconciliation_totals", {}),
        }
        observation = None
        observation_started = parse_datetime(self.config.get("observation_started_at"))
        if observation_started:
            baseline = self.get_meta("observation_baseline")
            if not baseline:
                baseline = {"at": iso_now(), "metrics": operational_metrics}
                self.set_meta("observation_baseline", baseline)
            observation = {
                "started_at": observation_started.isoformat(timespec="seconds"),
                "ends_at": (observation_started + dt.timedelta(days=int(self.config.get("observation_days", 7)))).isoformat(timespec="seconds"),
                "targets": self.config.get("observation_targets", {}),
                "baseline": baseline,
                "current": operational_metrics,
            }
        payload = {
            "checked_at": iso_now(),
            "enabled": bool(self.config.get("enabled")),
            "kill_switch": bool(self.config.get("kill_switch")),
            "database": str(self.db_path),
            "event_queues": queues,
            "runtime_runs": runs,
            "operational_metrics": operational_metrics,
            "observation": observation,
            "active_alerts": active_alerts,
            "last_health": self.get_meta("last_health"),
            "last_memory_govern": self.get_meta("last_memory_govern"),
            "last_runtime_reconciliation": self.get_meta("last_runtime_reconciliation"),
            "worker_heartbeat": read_json(HEARTBEAT_FILE, None),
        }
        atomic_write_json(STATUS_JSON, payload)
        return payload

    def heartbeat(self, state="running"):
        atomic_write_json(
            HEARTBEAT_FILE,
            {"at": iso_now(), "epoch": epoch_now(), "pid": os.getpid(), "state": state},
        )


def daemon(control):
    stopping = {"value": False}

    def stop_handler(_signum, _frame):
        stopping["value"] = True

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    poll = max(0.25, float(control.config.get("poll_seconds", 2)))
    last_status = 0.0
    while not stopping["value"]:
        control.config = merged_config(control.config_path)
        control.heartbeat("running")
        if not control.config.get("enabled") or control.config.get("kill_switch"):
            if epoch_now() - last_status > 30:
                control.status()
                last_status = epoch_now()
            time.sleep(poll)
            continue
        control.reclaim_expired()
        control.schedule_periodic()
        result = control.process_one()
        if epoch_now() - last_status > 30:
            control.status()
            last_status = epoch_now()
        if result is None:
            time.sleep(poll)
    control.heartbeat("stopped")
    control.status()


def payload_from_args(args):
    if args.payload_file:
        return read_json(args.payload_file, {})
    if args.payload:
        return json.loads(args.payload)
    if not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        return json.loads(raw) if raw else {}
    return {}


def build_parser():
    parser = argparse.ArgumentParser(description="Clawwarden durable unattended control plane")
    parser.add_argument("--db", default=None)
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init")
    enqueue = sub.add_parser("enqueue")
    enqueue.add_argument("--event-id", required=True)
    enqueue.add_argument("--event-type", required=True)
    enqueue.add_argument("--payload")
    enqueue.add_argument("--payload-file")
    enqueue.add_argument("--max-attempts", type=int)
    enqueue.add_argument("--delay-seconds", type=float, default=0)

    sub.add_parser("once")
    sub.add_parser("daemon")
    sub.add_parser("status")
    sub.add_parser("reclaim")
    sub.add_parser("schedule")
    return parser


def main():
    args = build_parser().parse_args()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not DEFAULT_CONFIG.exists():
        atomic_write_json(DEFAULT_CONFIG, DEFAULT_CONFIG_VALUE)
    control = ControlPlane(args.db, args.config)
    try:
        if args.command == "init":
            result = {"initialized_at": iso_now(), "database": str(control.db_path), "config": str(control.config_path)}
        elif args.command == "enqueue":
            result = control.enqueue(
                args.event_id,
                args.event_type,
                payload_from_args(args),
                max_attempts=args.max_attempts,
                available_at=epoch_now() + max(0, args.delay_seconds),
            )
        elif args.command == "once":
            control.reclaim_expired()
            result = control.process_one() or {"status": "idle"}
        elif args.command == "daemon":
            DAEMON_LOCK.parent.mkdir(parents=True, exist_ok=True)
            with DAEMON_LOCK.open("a+") as lock_handle:
                try:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    print(json.dumps({"status": "already_running", "lock": str(DAEMON_LOCK)}))
                    return 3
                daemon(control)
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            return 0
        elif args.command == "status":
            result = control.status()
        elif args.command == "reclaim":
            result = {"reclaimed": control.reclaim_expired()}
        elif args.command == "schedule":
            control.schedule_periodic()
            result = control.status()
        else:
            raise RuntimeError("unsupported command")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        control.close()


if __name__ == "__main__":
    raise SystemExit(main())
