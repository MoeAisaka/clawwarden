import importlib.util
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "control_plane.py"
SPEC = importlib.util.spec_from_file_location("clawwarden_control_plane", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class QuietControlPlane(MODULE.ControlPlane):
    def __init__(self, *args, **kwargs):
        self.notifications = []
        super().__init__(*args, **kwargs)

    def notify(self, fingerprint, severity, message):
        self.notifications.append((fingerprint, severity, message))


class ClawwardenControlPlaneTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / "config.json"
        self.config.write_text(
            json.dumps(
                {
                    **MODULE.DEFAULT_CONFIG_VALUE,
                    "auto_commit_low_risk_memory": False,
                    "auto_resume_failed_runs": True,
                    "alert_cooldown_seconds": 3600,
                }
            ),
            encoding="utf-8",
        )
        self.control = QuietControlPlane(self.root / "control.sqlite3", self.config)

    def tearDown(self):
        self.control.close()
        self.temp.cleanup()

    def test_secure_defaults_disable_side_effectful_automation(self):
        self.assertFalse(MODULE.DEFAULT_CONFIG_VALUE["auto_commit_low_risk_memory"])
        self.assertFalse(MODULE.DEFAULT_CONFIG_VALUE["auto_resume_failed_runs"])
        self.assertFalse(MODULE.DEFAULT_CONFIG_VALUE["auto_restart_gateway"])
        self.assertFalse(MODULE.DEFAULT_CONFIG_VALUE["backup_openclaw_config"])
        self.assertFalse(MODULE.DEFAULT_CONFIG_VALUE["backup_nmem"])

    def test_state_database_is_owner_only(self):
        mode = (self.root / "control.sqlite3").stat().st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_enqueue_is_idempotent(self):
        first = self.control.enqueue("event-1", "unknown", {"a": 1})
        second = self.control.enqueue("event-1", "unknown", {"a": 2})
        self.assertTrue(first["inserted"])
        self.assertFalse(second["inserted"])
        row = self.control.conn.execute(
            "SELECT payload_json FROM events WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual(json.loads(row[0]), {"a": 1})

    def test_claim_and_finish_exactly_once(self):
        self.control.enqueue("event-2", "unknown", {})
        result = self.control.process_one()
        self.assertEqual(result["status"], "done")
        self.assertIsNone(self.control.process_one())
        row = self.control.conn.execute(
            "SELECT status,attempts FROM events WHERE event_id='event-2'"
        ).fetchone()
        self.assertEqual(tuple(row), ("done", 1))

    def test_retry_then_dead_letter(self):
        self.control.enqueue("event-3", "health", {}, max_attempts=1)
        event = dict(self.control.claim())
        state = self.control.fail_event(event, "boom")
        self.assertEqual(state, "dead")
        self.assertEqual(len(self.control.notifications), 1)

    def test_newer_same_type_success_resolves_dead_letter_alert(self):
        self.control.enqueue("event-dead", "unknown", {}, max_attempts=1)
        dead = dict(self.control.claim())
        self.control.conn.execute(
            "UPDATE events SET created_at='2026-01-01T00:00:00+00:00' WHERE event_id='event-dead'"
        )
        self.assertEqual(self.control.fail_event(dead, "boom"), "dead")
        self.control.enqueue("event-recovered", "unknown", {})
        self.control.conn.execute(
            "UPDATE events SET created_at='2026-01-02T00:00:00+00:00' WHERE event_id='event-recovered'"
        )
        self.control.finish_event("event-recovered", {"ok": True})
        alert = self.control.conn.execute(
            "SELECT status FROM alerts WHERE fingerprint='dead-event:event-dead'"
        ).fetchone()
        self.assertEqual(alert[0], "resolved")

    def test_success_does_not_resolve_other_event_type_alert(self):
        self.control.enqueue("event-dead", "health", {}, max_attempts=1)
        dead = dict(self.control.claim())
        self.control.conn.execute(
            "UPDATE events SET created_at='2026-01-01T00:00:00+00:00' WHERE event_id='event-dead'"
        )
        self.assertEqual(self.control.fail_event(dead, "boom"), "dead")
        self.control.enqueue("event-other", "unknown", {})
        self.control.conn.execute(
            "UPDATE events SET created_at='2026-01-02T00:00:00+00:00' WHERE event_id='event-other'"
        )
        self.control.finish_event("event-other", {"ok": True})
        alert = self.control.conn.execute(
            "SELECT status FROM alerts WHERE fingerprint='dead-event:event-dead'"
        ).fetchone()
        self.assertEqual(alert[0], "active")

    def test_expired_lease_is_reclaimed(self):
        self.control.enqueue("event-lease", "health", {})
        self.control.claim()
        self.control.conn.execute(
            "UPDATE events SET lease_until=0 WHERE event_id='event-lease'"
        )
        self.assertEqual(self.control.reclaim_expired(), 1)
        row = self.control.conn.execute(
            "SELECT status FROM events WHERE event_id='event-lease'"
        ).fetchone()
        self.assertEqual(row[0], "retry")

    def test_alerts_are_deduplicated(self):
        self.assertTrue(self.control.emit_alert("fingerprint", "warning", "same"))
        self.assertFalse(self.control.emit_alert("fingerprint", "warning", "same"))
        self.assertEqual(len(self.control.notifications), 1)

    def test_exact_duplicate_candidates_are_archived(self):
        candidate_dir = self.root / "candidates"
        candidate_dir.mkdir()
        old_dir = MODULE.MEMORY_CANDIDATE_DIR
        MODULE.MEMORY_CANDIDATE_DIR = candidate_dir
        try:
            for index in (1, 2):
                payload = {
                    "candidate_id": "candidate-%s" % index,
                    "title": "same",
                    "content": "identical durable conclusion",
                    "status": "pending",
                    "risk_level": "low",
                    "recommendation": "auto_commit_allowed",
                    "labels": [],
                }
                (candidate_dir / ("candidate-%s.json" % index)).write_text(
                    json.dumps(payload), encoding="utf-8"
                )
            result = self.control.handle_memory_govern("govern", {})
            self.assertEqual(len(result["archived_exact_duplicates"]), 1)
            statuses = [
                json.loads(path.read_text(encoding="utf-8"))["status"]
                for path in candidate_dir.glob("*.json")
            ]
            self.assertEqual(sorted(statuses), ["archived", "pending"])
        finally:
            MODULE.MEMORY_CANDIDATE_DIR = old_dir

    def test_protected_low_risk_candidate_is_not_committed(self):
        candidate_dir = self.root / "candidates-protected"
        candidate_dir.mkdir()
        path = candidate_dir / "protected.json"
        path.write_text(
            json.dumps(
                {
                    "candidate_id": "protected",
                    "title": "gold production rule",
                    "content": "verified result",
                    "status": "pending",
                    "risk_level": "low",
                    "recommendation": "auto_commit_allowed",
                    "labels": [],
                }
            ),
            encoding="utf-8",
        )
        old_dir = MODULE.MEMORY_CANDIDATE_DIR
        MODULE.MEMORY_CANDIDATE_DIR = candidate_dir
        self.control.config["auto_commit_low_risk_memory"] = True
        try:
            with mock.patch.object(MODULE, "run_command") as command:
                result = self.control.handle_memory_govern("govern-protected", {})
            command.assert_not_called()
            self.assertEqual(result["protected_auto_commit_candidates"], ["protected"])
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["status"], "pending")
        finally:
            MODULE.MEMORY_CANDIDATE_DIR = old_dir

    def test_low_information_candidate_is_archived_before_protection(self):
        candidate_dir = self.root / "candidates-low-information"
        candidate_dir.mkdir()
        path = candidate_dir / "session-visible.json"
        path.write_text(
            json.dumps(
                {
                    "candidate_id": "session-visible",
                    "title": "SESSION_VISIBLE",
                    "content": "SESSION_VISIBLE",
                    "status": "pending",
                    "risk_level": "high",
                    "recommendation": "manual_review_required",
                    "labels": [],
                }
            ),
            encoding="utf-8",
        )
        old_dir = MODULE.MEMORY_CANDIDATE_DIR
        MODULE.MEMORY_CANDIDATE_DIR = candidate_dir
        try:
            with mock.patch.object(MODULE, "run_command") as command:
                result = self.control.handle_memory_govern("govern-low-information", {})
            command.assert_not_called()
            self.assertEqual(result["archived_low_information"], ["session-visible"])
            archived = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(archived["status"], "archived")
            self.assertEqual(
                archived["archive_reason"],
                "low_information_marker:session_visible",
            )
        finally:
            MODULE.MEMORY_CANDIDATE_DIR = old_dir

    def test_generic_runtime_ack_is_low_information_only_when_both_fields_match(self):
        self.assertEqual(
            MODULE.low_information_memory_reason("worker_ready", "worker_ready"),
            "low_information_runtime_marker:worker_ready",
        )
        self.assertIsNone(
            MODULE.low_information_memory_reason("worker_ready", "worker completed migration")
        )

    def test_low_information_finish_suppresses_memory_candidate(self):
        self.control.upsert_runtime_run(
            "run-visible", "task-visible", "agent:main:test", True, "running", "start"
        )
        with mock.patch.object(MODULE, "run_command", return_value={"outputs": {}}) as command:
            result = self.control.handle_turn_finish(
                "finish-visible",
                {"runId": "run-visible", "success": True, "summary": "SESSION_VISIBLE"},
            )
        argv = [str(item) for item in command.call_args.args[0]]
        self.assertIn("--suppress-memory-candidate", argv)
        self.assertTrue(result["memory_candidate_suppressed"])
        self.assertEqual(
            result["memory_suppression_reason"],
            "low_information_marker:session_visible",
        )

    def test_meaningful_finish_keeps_memory_candidate_enabled(self):
        self.control.upsert_runtime_run(
            "run-meaningful", "task-meaningful", "agent:main:test", True, "running", "start"
        )
        with mock.patch.object(MODULE, "run_command", return_value={"outputs": {}}) as command:
            result = self.control.handle_turn_finish(
                "finish-meaningful",
                {
                    "runId": "run-meaningful",
                    "success": True,
                    "summary": "Implemented idempotent runtime reconciliation and verified the failure path.",
                },
            )
        argv = [str(item) for item in command.call_args.args[0]]
        self.assertNotIn("--suppress-memory-candidate", argv)
        self.assertFalse(result["memory_candidate_suppressed"])

    def test_failed_finish_schedules_resume(self):
        self.control.upsert_runtime_run(
            "run-failed", "task-failed", "agent:main:test", True, "running", "start"
        )
        with mock.patch.object(MODULE, "run_command", return_value={"outputs": {}}):
            result = self.control.handle_turn_finish(
                "finish-failed", {"runId": "run-failed", "success": False, "error": "timeout"}
            )
        self.assertEqual(result["status"], "waiting")
        row = self.control.conn.execute(
            "SELECT status,event_type FROM events WHERE event_id='resume:run-failed:1'"
        ).fetchone()
        self.assertEqual(tuple(row), ("queued", "resume_run"))

    def test_resume_message_contains_lineage_marker(self):
        self.control.upsert_runtime_run(
            "run-parent", "task-parent", "agent:main:test", True, "waiting", "finish"
        )
        calls = []

        def fake_command(argv, **_kwargs):
            calls.append([str(item) for item in argv])
            return {"returncode": 0, "stdout": "ok", "stderr": ""}

        with mock.patch.object(MODULE, "run_command", side_effect=fake_command):
            result = self.control.handle_resume_run(
                "resume:run-parent:1",
                {"runId": "run-parent", "taskId": "task-parent", "error": "timeout"},
            )
        self.assertEqual(result["action"], "resume_enqueued")
        joined = " ".join(calls[0])
        self.assertIn("--expect-final", calls[0])
        self.assertIn(
            "[CLAWWARDEN_RECOVERY run=run-parent task=task-parent event=resume:run-parent:1]",
            joined,
        )
        row = self.control.runtime_run("run-parent")
        self.assertEqual((row["status"], row["resume_attempts"]), ("resuming", 1))

    def test_recovery_turn_reuses_original_task(self):
        self.control.upsert_runtime_run(
            "run-parent", "task-parent", "agent:main:test", True, "resuming", "resume"
        )
        self.control.conn.execute(
            "UPDATE runtime_runs SET resume_attempts=1 WHERE run_id='run-parent'"
        )
        with mock.patch.object(MODULE, "run_command", return_value={"outputs": {"checkpoint": {}}}):
            result = self.control.handle_turn_start(
                "turn_start:run-child",
                {
                    "runId": "run-child",
                    "sessionKey": "agent:main:test",
                    "prompt": "[CLAWWARDEN_RECOVERY run=run-parent task=task-parent event=resume:run-parent:1] continue",
                    "recoveryOfRunId": "run-parent",
                    "recoveryTaskId": "task-parent",
                },
            )
        self.assertEqual(result["action"], "recovery_turn_start")
        child = self.control.runtime_run("run-child")
        parent = self.control.runtime_run("run-parent")
        self.assertEqual((child["task_id"], child["status"], child["resume_attempts"]), ("task-parent", "running", 1))
        self.assertEqual(parent["status"], "resumed")

    def test_stale_runtime_task_schedules_resume(self):
        task_dir = self.root / "tasks"
        task_dir.mkdir()
        task = {
            "task_id": "task-stale",
            "title": "stale",
            "status": "waiting",
            "updated_at": "2020-01-01T00:00:00+00:00",
        }
        (task_dir / "task-stale.json").write_text(json.dumps(task), encoding="utf-8")
        self.control.upsert_runtime_run(
            "run-stale", "task-stale", "agent:main:test", True, "waiting", "finish"
        )
        old_dir = MODULE.WORKFLOW_TASK_DIR
        MODULE.WORKFLOW_TASK_DIR = task_dir
        try:
            result = self.control.handle_recover_stale("recover", {})
        finally:
            MODULE.WORKFLOW_TASK_DIR = old_dir
        self.assertEqual(result["resume_scheduled"], ["task-stale"])
        row = self.control.conn.execute(
            "SELECT event_type FROM events WHERE event_id='resume:run-stale:1'"
        ).fetchone()
        self.assertEqual(row[0], "resume_run")

    def test_cleanly_done_session_reconciles_missing_finish(self):
        agents_dir = self.root / "agents"
        store_dir = agents_dir / "main" / "sessions"
        store_dir.mkdir(parents=True)
        session_key = "agent:main:test-reconcile"
        (store_dir / "sessions.json").write_text(
            json.dumps(
                {
                    session_key: {
                        "status": "done",
                        "abortedLastRun": False,
                        "updatedAt": MODULE.epoch_now() * 1000,
                    }
                }
            ),
            encoding="utf-8",
        )
        self.control.config["openclaw_agents_dir"] = str(agents_dir)
        self.control.config["runtime_reconcile_grace_seconds"] = 60
        self.control.upsert_runtime_run(
            "run-reconcile", "task-reconcile", session_key, True, "running", "start"
        )
        self.control.conn.execute(
            "UPDATE runtime_runs SET created_at='2020-01-01T00:00:00+00:00' WHERE run_id='run-reconcile'"
        )
        task_dir = self.root / "empty-tasks"
        task_dir.mkdir()
        old_task_dir = MODULE.WORKFLOW_TASK_DIR
        MODULE.WORKFLOW_TASK_DIR = task_dir
        try:
            with mock.patch.object(MODULE, "run_command", return_value={"action": "finish_candidates"}) as command:
                result = self.control.handle_recover_stale("recover-reconcile", {})
        finally:
            MODULE.WORKFLOW_TASK_DIR = old_task_dir
        reconciled = result["runtime_reconciliation"]["reconciled"]
        self.assertEqual([item["run_id"] for item in reconciled], ["run-reconcile"])
        self.assertEqual(self.control.runtime_run("run-reconcile")["status"], "completed_reconciled")
        argv = [str(item) for item in command.call_args.args[0]]
        self.assertIn("--suppress-memory-candidate", argv)

    def test_running_session_is_not_reconciled(self):
        agents_dir = self.root / "agents-running"
        store_dir = agents_dir / "main" / "sessions"
        store_dir.mkdir(parents=True)
        session_key = "agent:main:test-running"
        (store_dir / "sessions.json").write_text(
            json.dumps(
                {
                    session_key: {
                        "status": "running",
                        "abortedLastRun": False,
                        "updatedAt": MODULE.epoch_now() * 1000,
                    }
                }
            ),
            encoding="utf-8",
        )
        self.control.config["openclaw_agents_dir"] = str(agents_dir)
        self.control.config["runtime_reconcile_grace_seconds"] = 60
        self.control.upsert_runtime_run(
            "run-still-running", "task-still-running", session_key, True, "running", "start"
        )
        self.control.conn.execute(
            "UPDATE runtime_runs SET created_at='2020-01-01T00:00:00+00:00' WHERE run_id='run-still-running'"
        )
        with mock.patch.object(MODULE, "run_command") as command:
            result = self.control.reconcile_runtime_runs("reconcile-running")
        command.assert_not_called()
        self.assertEqual(result["reconciled"], [])
        self.assertEqual(self.control.runtime_run("run-still-running")["status"], "running")

    def test_status_initializes_observation_baseline(self):
        self.control.config["observation_started_at"] = "2026-07-11T22:31:06+08:00"
        old_status = MODULE.STATUS_JSON
        old_candidate_dir = MODULE.MEMORY_CANDIDATE_DIR
        candidate_dir = self.root / "status-candidates"
        candidate_dir.mkdir()
        MODULE.STATUS_JSON = self.root / "status.json"
        MODULE.MEMORY_CANDIDATE_DIR = candidate_dir
        try:
            payload = self.control.status()
        finally:
            MODULE.STATUS_JSON = old_status
            MODULE.MEMORY_CANDIDATE_DIR = old_candidate_dir
        self.assertIsNotNone(payload["observation"])
        self.assertEqual(payload["observation"]["baseline"]["metrics"]["dead_events"], 0)
        self.assertEqual(payload["operational_metrics"]["managed_completion_rate_pct"], 100.0)
        self.assertEqual(payload["operational_metrics"]["low_information_pending"], 0)
        self.assertEqual(payload["operational_metrics"]["duplicate_side_effects_reported"], 0)

    def test_gateway_continuation_reuses_latest_open_task(self):
        self.control.upsert_runtime_run(
            "run-before-restart", "task-original", "agent:main:test", True, "running", "start"
        )
        with mock.patch.object(MODULE, "run_command", return_value={"outputs": {"checkpoint": {}}}):
            result = self.control.handle_turn_start(
                "turn_start:run-after-restart",
                {
                    "runId": "run-after-restart",
                    "sessionKey": "agent:main:test",
                    "prompt": "previous turn was interrupted by a gateway restart",
                    "gatewayContinuation": True,
                },
            )
        self.assertEqual(result["action"], "gateway_continuation_start")
        child = self.control.runtime_run("run-after-restart")
        parent = self.control.runtime_run("run-before-restart")
        self.assertEqual((child["task_id"], child["status"]), ("task-original", "running"))
        self.assertEqual(parent["status"], "resumed")


if __name__ == "__main__":
    unittest.main()
