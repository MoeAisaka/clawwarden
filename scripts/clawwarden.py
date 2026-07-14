#!/usr/bin/env python3
"""Clawwarden orchestrator.

Phase 4 goal:
- provide one stable CLI entry for P1/P2/P3;
- make trigger decisions explicit and repeatable;
- keep runtime integration conservative until smoke-tested by heartbeat.

This script coordinates existing tools. It does not write live skills, apply
Skill Workshop proposals, or force-commit high-risk memories.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
BASE_DIR = ROOT / "memory" / "clawwarden"
RUN_DIR = BASE_DIR / "runs"
LEDGER = BASE_DIR / "orchestrator-ledger.jsonl"
STATUS_JSON = BASE_DIR / "status.json"
STATUS_MD = BASE_DIR / "status.md"
PATROL_LOG = BASE_DIR / "patrol-ledger.jsonl"
REVIEW_JSON = BASE_DIR / "review-queue.json"
REVIEW_MD = BASE_DIR / "review-queue.md"
HOOK_CONFIG = BASE_DIR / "runtime-hooks.json"
RUNTIME_INTEGRATION = BASE_DIR / "runtime-integration.json"
LOCK_FILE = BASE_DIR / ".clawwarden.lock"
RUNTIME_EVENT_LOG = BASE_DIR / "runtime-events.jsonl"
REPLAY_DIR = BASE_DIR / "replay"
RECOVERY_DIR = BASE_DIR / "recovery"
GOVERNANCE_DIR = BASE_DIR / "governance"
SKILL_CYCLE_DIR = BASE_DIR / "skill-cycle"

NMEM_ADAPTER = SCRIPT_DIR / "nmem_adapter.py"
WORKFLOW_LEDGER = SCRIPT_DIR / "workflow_ledger.py"
SKILL_STAGING = SCRIPT_DIR / "skill_staging.py"
QUOTA_HELPER = Path(os.environ.get("CLAWWARDEN_QUOTA_HELPER", SCRIPT_DIR / "check_quota.py"))
ARTIFACT_HELPER = Path(os.environ.get("CLAWWARDEN_ARTIFACT_HELPER", SCRIPT_DIR / "artifact_helper.py"))

COMPLEX_PATTERNS = [
    r"跨轮次",
    r"多阶段",
    r"长期",
    r"恢复",
    r"checkpoint",
    r"ledger",
    r"生产",
    r"交易系统",
    r"实盘",
    r"架构",
    r"重构",
    r"调试",
    r"自动化",
    r"落地",
    r"代码",
    r"仓库",
    r"测试",
    r"nmem",
    r"OpenClaw",
    r"Clawwarden",
]

SKILL_PATTERNS = [
    r"重复",
    r"复用",
    r"技能",
    r"skill",
    r"workflow",
    r"流程",
    r"procedure",
    r"SOP",
]

QUOTA_PATTERNS = [
    r"Codex",
    r"GPT",
    r"代码",
    r"工程",
    r"调试",
    r"重构",
    r"测试",
    r"gpt-image",
]

DEFAULT_HOOK_CONFIG = {
    "enabled": True,
    "mode": "conservative",
    "kill_switch": False,
    "fail_open": True,
    "auto_start_complex": True,
    "auto_checkpoint": True,
    "auto_finish_candidates": True,
    "auto_skill_staging": False,
    "require_execute_flag": True,
    "max_memory_limit": 8,
    "max_review_queue": 20,
    "degrade_on_nmem_failure": True,
    "degrade_on_quota_failure": True,
    "min_codex_5h_for_heavy_hooks": 10,
}


@dataclass
class Decision:
    task_id: str
    complexity: str
    needs_nmem_context: bool
    needs_ledger: bool
    needs_quota_check: bool
    skill_candidate_suggested: bool
    reasons: list[str] = field(default_factory=list)


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug(value: str, limit: int = 64) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    if not normalized:
        normalized = f"task-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    return normalized[:limit]


def event_id(prefix: str = "event") -> str:
    return f"{prefix}-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"


def ensure_dirs() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)
    RECOVERY_DIR.mkdir(parents=True, exist_ok=True)
    GOVERNANCE_DIR.mkdir(parents=True, exist_ok=True)
    SKILL_CYCLE_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def clawwarden_lock():
    ensure_dirs()
    with LOCK_FILE.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def atomic_write_text(path: Path, text: str) -> None:
    ensure_dirs()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            tmp.write(text)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def log_event(event: dict[str, Any]) -> None:
    ensure_dirs()
    payload = {"time": now(), **event}
    with LEDGER.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def run_cmd(args: list[str], *, input_text: str | None = None, json_mode: bool = False) -> Any:
    result = subprocess.run(
        args,
        input=input_text,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{' '.join(args)} failed: {message}")
    output = result.stdout.strip()
    if json_mode:
        return json.loads(output) if output else {}
    return output


def nmem_available() -> bool:
    return NMEM_ADAPTER.is_file() and shutil.which("nmem") is not None


def has_any(text: str, patterns: list[str]) -> list[str]:
    return [pattern for pattern in patterns if re.search(pattern, text, flags=re.IGNORECASE)]


def decide(args: argparse.Namespace) -> Decision:
    text = "\n".join(
        [
            args.title or "",
            args.goal or "",
            args.query or "",
            args.summary or "",
        ]
    )
    complex_hits = has_any(text, COMPLEX_PATTERNS)
    skill_hits = has_any(text, SKILL_PATTERNS)
    quota_hits = has_any(text, QUOTA_PATTERNS)

    mode = args.mode
    if mode == "simple":
        complexity = "simple"
    elif mode == "full":
        complexity = "complex"
    else:
        complexity = "complex" if complex_hits else "simple"

    reasons: list[str] = []
    if complex_hits:
        reasons.append(f"complex_patterns={','.join(complex_hits[:6])}")
    if skill_hits:
        reasons.append(f"skill_patterns={','.join(skill_hits[:6])}")
    if quota_hits:
        reasons.append(f"quota_patterns={','.join(quota_hits[:6])}")
    if args.repeatable:
        reasons.append("repeatable=true")

    task_id = args.task_id or f"cw-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug(args.title or args.query or args.goal or 'task', 24)}"
    return Decision(
        task_id=task_id,
        complexity=complexity,
        needs_nmem_context=not args.no_context and (complexity == "complex" or bool(args.query)),
        needs_ledger=not args.no_ledger and complexity == "complex",
        needs_quota_check=not args.no_quota and bool(quota_hits),
        skill_candidate_suggested=bool(args.repeatable or skill_hits),
        reasons=reasons,
    )


def write_run(task_id: str, payload: dict[str, Any]) -> Path:
    ensure_dirs()
    path = RUN_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{slug(task_id, 80)}.json"
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def workflow_task_dir() -> Path:
    return ROOT / "memory" / "workflow-ledger" / "tasks"


def load_workflow_tasks(statuses: set[str] | None = None) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    directory = workflow_task_dir()
    if not directory.exists():
        return tasks
    for path in sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            task = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if statuses and task.get("status") not in statuses:
            continue
        task["_path"] = str(path)
        tasks.append(task)
    return tasks


def task_age_hours(task: dict[str, Any]) -> float | None:
    updated = parse_time(str(task.get("updated_at") or ""))
    if not updated:
        return None
    return (datetime.now() - updated).total_seconds() / 3600


def quota_remaining(quota: Any, key: str) -> int | None:
    if not isinstance(quota, dict):
        return None
    parsed = quota.get("parsed")
    if not isinstance(parsed, dict):
        return None
    value = parsed.get(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def load_hook_config() -> dict[str, Any]:
    ensure_dirs()
    if not HOOK_CONFIG.exists():
        atomic_write_text(HOOK_CONFIG, json.dumps(DEFAULT_HOOK_CONFIG, ensure_ascii=False, indent=2))
        return dict(DEFAULT_HOOK_CONFIG)
    data = read_json_file(HOOK_CONFIG) or {}
    return {**DEFAULT_HOOK_CONFIG, **data}


def write_status_page(payload: dict[str, Any]) -> None:
    ensure_dirs()
    atomic_write_text(STATUS_JSON, json.dumps(payload, ensure_ascii=False, indent=2))
    lines = [
        "# Clawwarden Status",
        "",
        f"- Checked: {payload.get('checked_at')}",
        f"- Overall: `{payload.get('overall')}`",
        f"- Severity: `{payload.get('severity')}`",
        f"- Alerts: {len(payload.get('alerts', []))}",
        "",
        "## Health",
        "",
        f"- nmem: `{payload.get('health', {}).get('nmem')}`",
        f"- py_compile: `{payload.get('health', {}).get('py_compile')}`",
    ]
    quota = payload.get("health", {}).get("quota", {})
    if quota:
        lines.extend(
            [
                f"- Codex 5h remaining: `{quota.get('codex_5h_remaining_pct')}`",
                f"- Codex weekly remaining: `{quota.get('codex_weekly_remaining_pct')}`",
                f"- Spark 5h remaining: `{quota.get('spark_5h_remaining_pct')}`",
            ]
        )
    lines.extend(["", "## Queues", ""])
    for key, value in payload.get("queues", {}).items():
        lines.append(f"- {key}: `{value}`")
    if payload.get("governance"):
        lines.extend(["", "## Governance", ""])
        governance = payload.get("governance", {})
        memory = governance.get("memory", {})
        skill_lifecycle = governance.get("skill_lifecycle", {})
        lines.append(f"- memory candidates: `{memory.get('total', 0)}` total, `{memory.get('auto_commit_eligible', 0)}` auto-commit eligible")
        lines.append(f"- skill lifecycle: `{skill_lifecycle.get('ready_for_workshop', 0)}` ready for workshop")
        lines.append(f"- recovery plans: `{governance.get('recovery_plans', 0)}`")
    artifact = payload.get("artifact_publisher")
    if artifact:
        lines.extend(["", "## Artifact Publisher", ""])
        lines.append(f"- status: `{artifact.get('status')}`")
        lines.append(f"- recent artifacts: `{artifact.get('recent_count')}`")
        lines.append(f"- expired recent artifacts: `{artifact.get('expired_recent_count')}`")
        lines.append(f"- backend: `{artifact.get('config', {}).get('default_backend')}`")
    if payload.get("stale_tasks"):
        lines.extend(["", "## Stale Tasks", ""])
        for task in payload["stale_tasks"]:
            lines.append(f"- `{task.get('task_id')}` {task.get('status')} age={task.get('age_hours')}h")
    if payload.get("alerts"):
        lines.extend(["", "## Alerts", ""])
        for alert in payload["alerts"]:
            lines.append(f"- [{alert.get('severity')}] {alert.get('message')}")
    atomic_write_text(STATUS_MD, "\n".join(lines) + "\n")


def log_patrol(payload: dict[str, Any]) -> None:
    ensure_dirs()
    with PATROL_LOG.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def log_runtime_event(payload: dict[str, Any]) -> Path:
    ensure_dirs()
    payload = {"recorded_at": now(), **payload}
    event = str(payload.get("event_id") or event_id("runtime"))
    payload["event_id"] = event
    with RUNTIME_EVENT_LOG.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    path = REPLAY_DIR / f"{slug(event, 120)}.json"
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def nmem_candidate_dir() -> Path:
    return ROOT / "memory" / "nmem-adapter" / "candidates"


def skill_candidate_dir() -> Path:
    return ROOT / "memory" / "skill-staging" / "candidates"


def collect_review_items(limit: int, stale_hours: float = 24.0) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    memory_dir = nmem_candidate_dir()
    if memory_dir.exists():
        for path in sorted(memory_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            data = read_json_file(path)
            if not data or data.get("status") != "pending":
                continue
            items.append(
                {
                    "id": data.get("candidate_id") or path.stem,
                    "type": "memory_candidate",
                    "source": "nmem-adapter",
                    "status": data.get("status"),
                    "title": data.get("title"),
                    "risk_level": data.get("risk_level"),
                    "recommendation": data.get("recommendation"),
                    "path": str(path),
                    "suggested_action": "review_commit_or_reject",
                }
            )
            if len(items) >= limit:
                return items

    skill_dir = skill_candidate_dir()
    if skill_dir.exists():
        for path in sorted(skill_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            data = read_json_file(path)
            if not data or data.get("status") != "ready_for_workshop":
                continue
            items.append(
                {
                    "id": data.get("candidate_id") or path.stem,
                    "type": "skill_candidate",
                    "source": "skill-staging",
                    "status": data.get("status"),
                    "title": data.get("name"),
                    "risk_level": data.get("risk_level"),
                    "recommendation": data.get("recommendation"),
                    "path": str(path),
                    "suggested_action": "create_skill_workshop_proposal_or_archive",
                }
            )
            if len(items) >= limit:
                return items

    for task in load_workflow_tasks({"in_progress", "waiting"}):
        age = task_age_hours(task)
        stale = age is not None and age >= stale_hours
        items.append(
            {
                "id": task.get("task_id"),
                "type": "workflow_task",
                "source": "workflow-ledger",
                "status": task.get("status"),
                "title": task.get("title"),
                "risk_level": "unknown",
                "recommendation": "resume_or_close" if stale else "monitor",
                "path": task.get("_path"),
                "age_hours": round(age, 1) if age is not None else None,
                "suggested_action": "recover_checkpoint_or_mark_status",
            }
        )
        if len(items) >= limit:
            return items

    return items


def review_inventory(stale_hours: float = 24.0) -> dict[str, int]:
    memory_total = len(load_memory_candidates(status="pending"))
    skill_total = len(load_skill_candidates(status="ready_for_workshop"))
    active_tasks = load_workflow_tasks({"in_progress"})
    waiting_tasks = load_workflow_tasks({"waiting"})
    workflow_total = len(active_tasks) + len(waiting_tasks)
    stale_total = sum(
        1
        for task in [*active_tasks, *waiting_tasks]
        if task_age_hours(task) is not None and task_age_hours(task) >= stale_hours
    )
    return {
        "memory_pending": memory_total,
        "skill_ready_for_workshop": skill_total,
        "active_tasks": len(active_tasks),
        "waiting_tasks": len(waiting_tasks),
        "workflow_review": workflow_total,
        "stale_tasks": stale_total,
        "total": memory_total + skill_total + workflow_total,
    }


def write_review_queue(
    items: list[dict[str, Any]],
    *,
    inventory: dict[str, int] | None = None,
    window_limit: int | None = None,
) -> dict[str, Any]:
    inventory = inventory or {"total": len(items)}
    total_available = int(inventory.get("total", len(items)))
    payload = {
        "generated_at": now(),
        "total": total_available,
        "window_count": len(items),
        "window_limit": window_limit,
        "truncated": total_available > len(items),
        "counts": inventory,
        "items": items,
        "status_files": {"json": str(REVIEW_JSON), "markdown": str(REVIEW_MD)},
    }
    atomic_write_text(REVIEW_JSON, json.dumps(payload, ensure_ascii=False, indent=2))
    lines = [
        "# Clawwarden Review Queue",
        "",
        f"- Generated: {payload['generated_at']}",
        f"- Total available: {total_available}",
        f"- Window: {len(items)} / {window_limit or len(items)}",
        f"- Truncated: `{payload['truncated']}`",
        "",
    ]
    if not items:
        lines.append("No pending review items.")
    else:
        for item in items:
            lines.extend(
                [
                    f"## {item.get('type')}: {item.get('title') or item.get('id')}",
                    "",
                    f"- ID: `{item.get('id')}`",
                    f"- Source: `{item.get('source')}`",
                    f"- Status: `{item.get('status')}`",
                    f"- Risk: `{item.get('risk_level')}`",
                    f"- Recommendation: `{item.get('recommendation')}`",
                    f"- Suggested action: `{item.get('suggested_action')}`",
                    f"- Path: `{item.get('path')}`",
                    "",
                ]
            )
    atomic_write_text(REVIEW_MD, "\n".join(lines).rstrip() + "\n")
    return payload


def load_memory_candidates(status: str | None = None) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    directory = nmem_candidate_dir()
    if not directory.exists():
        return candidates
    for path in sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        data = read_json_file(path)
        if not data:
            continue
        if status and data.get("status") != status:
            continue
        data["_path"] = str(path)
        candidates.append(data)
    return candidates


def load_skill_candidates(status: str | None = None) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    directory = skill_candidate_dir()
    if not directory.exists():
        return candidates
    for path in sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        data = read_json_file(path)
        if not data:
            continue
        if status and data.get("status") != status:
            continue
        data["_path"] = str(path)
        proposal = path.with_suffix(".proposal.md")
        if proposal.exists():
            data["_proposal_path"] = str(proposal)
        candidates.append(data)
    return candidates


def integration_manifest(profile: str, config: dict[str, Any]) -> dict[str, Any]:
    execute_suffix = "" if not config.get("require_execute_flag") else " --execute"
    return {
        "generated_at": now(),
        "profile": profile,
        "status": "ready_for_runtime_connection",
        "config_path": str(HOOK_CONFIG),
        "integration_path": str(RUNTIME_INTEGRATION),
        "runtime_event_log": str(RUNTIME_EVENT_LOG),
        "replay_dir": str(REPLAY_DIR),
        "commands": {
            "turn_start": f"{SCRIPT_DIR / 'clawwarden.py'} hook turn-start{execute_suffix} --source openclaw-runtime --event-id <event-id> --task-id <task-id> --title <title> --goal <goal> --query <query>",
            "checkpoint": f"{SCRIPT_DIR / 'clawwarden.py'} hook checkpoint{execute_suffix} --source openclaw-runtime --event-id <event-id> --task-id <task-id> --summary <summary>",
            "turn_finish": f"{SCRIPT_DIR / 'clawwarden.py'} hook turn-finish{execute_suffix} --source openclaw-runtime --event-id <event-id> --task-id <task-id> --summary <summary>",
            "heartbeat": f"{SCRIPT_DIR / 'clawwarden.py'} hook heartbeat{execute_suffix} --source openclaw-runtime --event-id <event-id>",
        },
        "safety": [
            "kill_switch must remain available through hook-config --kill-switch true",
            "fail_open keeps normal chat unblocked if Clawwarden subtools fail",
            "high-risk memory candidates remain pending review",
            "Skill Workshop proposals are not created by runtime hooks",
            "production/trading actions are never executed by Clawwarden hooks",
        ],
    }


def cmd_integrate(args: argparse.Namespace) -> dict[str, Any]:
    config = load_hook_config()
    changed: dict[str, Any] = {}
    if args.profile == "observe":
        desired = {
            "enabled": True,
            "mode": "observe",
            "require_execute_flag": True,
            "auto_start_complex": True,
            "auto_checkpoint": True,
            "auto_finish_candidates": True,
            "auto_skill_staging": False,
            "fail_open": True,
        }
    elif args.profile == "active":
        desired = {
            "enabled": True,
            "mode": "active",
            "require_execute_flag": False,
            "auto_start_complex": True,
            "auto_checkpoint": True,
            "auto_finish_candidates": True,
            "auto_skill_staging": False,
            "fail_open": True,
        }
    else:
        desired = {
            "enabled": True,
            "mode": "conservative",
            "require_execute_flag": True,
            "auto_start_complex": True,
            "auto_checkpoint": True,
            "auto_finish_candidates": True,
            "auto_skill_staging": False,
            "fail_open": True,
        }
    if args.kill_switch is not None:
        desired["kill_switch"] = args.kill_switch == "true"
    for key, value in desired.items():
        if config.get(key) != value:
            config[key] = value
            changed[key] = value
    manifest = integration_manifest(args.profile, config)
    if args.write:
        atomic_write_text(HOOK_CONFIG, json.dumps(config, ensure_ascii=False, indent=2))
        atomic_write_text(RUNTIME_INTEGRATION, json.dumps(manifest, ensure_ascii=False, indent=2))
        log_event({"action": "integrate", "profile": args.profile, "changed": changed, "manifest": str(RUNTIME_INTEGRATION)})
    return {
        "integrated_at": now(),
        "profile": args.profile,
        "written": bool(args.write),
        "changed": changed,
        "config": config,
        "manifest": manifest,
    }


def recovery_plan_for_task(task: dict[str, Any]) -> dict[str, Any]:
    checkpoints = task.get("checkpoints") or []
    last_checkpoint = checkpoints[-1] if checkpoints else {}
    next_step = task.get("next_step") or last_checkpoint.get("next_step") or "Review the recovery note and decide whether to continue, close, or mark blocked."
    recommendation = "resume"
    if task.get("status") == "waiting":
        recommendation = "ask_user_or_close"
    if task.get("blockers"):
        recommendation = "resolve_blocker"
    return {
        "task_id": task.get("task_id"),
        "title": task.get("title"),
        "status": task.get("status"),
        "updated_at": task.get("updated_at"),
        "age_hours": round(task_age_hours(task) or 0, 1),
        "goal": task.get("goal"),
        "current_step": task.get("current_step"),
        "next_step": next_step,
        "last_checkpoint": last_checkpoint,
        "artifacts": task.get("artifacts") or [],
        "blockers": task.get("blockers") or [],
        "context_path": task.get("context_path"),
        "recommendation": recommendation,
        "resume_command": f"{SCRIPT_DIR / 'workflow_ledger.py'} recover {task.get('task_id')}",
    }


def write_recovery_report(plans: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "generated_at": now(),
        "total": len(plans),
        "plans": plans,
        "json": str(RECOVERY_DIR / "latest.json"),
        "markdown": str(RECOVERY_DIR / "latest.md"),
    }
    atomic_write_text(RECOVERY_DIR / "latest.json", json.dumps(payload, ensure_ascii=False, indent=2))
    lines = ["# Clawwarden Recovery Plans", "", f"- Generated: {payload['generated_at']}", f"- Items: {len(plans)}", ""]
    if not plans:
        lines.append("No stale tasks need recovery.")
    for plan in plans:
        lines.extend(
            [
                f"## {plan.get('title') or plan.get('task_id')}",
                "",
                f"- Task ID: `{plan.get('task_id')}`",
                f"- Status: `{plan.get('status')}`",
                f"- Age: `{plan.get('age_hours')}h`",
                f"- Recommendation: `{plan.get('recommendation')}`",
                f"- Next step: {plan.get('next_step')}",
                f"- Recover command: `{plan.get('resume_command')}`",
                "",
            ]
        )
    atomic_write_text(RECOVERY_DIR / "latest.md", "\n".join(lines).rstrip() + "\n")
    return payload


def cmd_recover_stale(args: argparse.Namespace) -> dict[str, Any]:
    plans = []
    for task in load_workflow_tasks({"planned", "in_progress", "waiting", "blocked"}):
        age = task_age_hours(task)
        if age is None or age < args.stale_hours:
            continue
        plans.append(recovery_plan_for_task(task))
        if len(plans) >= args.limit:
            break
    report = write_recovery_report(plans)
    if args.checkpoint and plans:
        for plan in plans:
            if plan.get("status") in {"planned", "in_progress"}:
                run_cmd(
                    [
                        str(WORKFLOW_LEDGER),
                        "checkpoint",
                        str(plan["task_id"]),
                        "--status",
                        "waiting",
                        "--summary",
                        f"Auto recovery plan generated by Clawwarden at {now()}",
                        "--next-step",
                        str(plan.get("next_step") or ""),
                    ],
                    json_mode=True,
                )
    log_event({"action": "recover_stale", "total": len(plans), "checkpointed": bool(args.checkpoint)})
    return {"recovered_at": now(), "report": report, "checkpointed": bool(args.checkpoint)}


def memory_governance_item(candidate: dict[str, Any]) -> dict[str, Any]:
    duplicates = candidate.get("duplicate_results", {}) if isinstance(candidate.get("duplicate_results"), dict) else {}
    likely_duplicates = int(duplicates.get("likely_duplicates") or 0)
    risk = candidate.get("risk_level") or "unknown"
    recommendation = candidate.get("recommendation") or "review"
    action = "commit_candidate"
    if risk == "high":
        action = "manual_review_required"
    elif likely_duplicates > 0 or "dedupe" in recommendation:
        action = "dedupe_or_supersede_review"
    elif risk == "medium":
        action = "review_then_commit_or_archive"
    elif recommendation == "auto_commit_allowed":
        action = "auto_commit_eligible"
    return {
        "candidate_id": candidate.get("candidate_id"),
        "title": candidate.get("title"),
        "status": candidate.get("status"),
        "risk_level": risk,
        "recommendation": recommendation,
        "likely_duplicates": likely_duplicates,
        "unit_type": candidate.get("unit_type"),
        "importance": candidate.get("importance"),
        "path": candidate.get("_path"),
        "suggested_action": action,
    }


def cmd_memory_govern(args: argparse.Namespace) -> dict[str, Any]:
    candidates = load_memory_candidates(status="pending" if args.pending_only else None)
    all_items = [memory_governance_item(candidate) for candidate in candidates]
    items = all_items[: args.limit]
    summary = {
        "total": len(all_items),
        "window_count": len(items),
        "window_limit": args.limit,
        "truncated": len(all_items) > len(items),
        "high_risk": sum(1 for item in all_items if item["risk_level"] == "high"),
        "medium_risk": sum(1 for item in all_items if item["risk_level"] == "medium"),
        "auto_commit_eligible": sum(1 for item in all_items if item["suggested_action"] == "auto_commit_eligible"),
        "dedupe_or_supersede": sum(1 for item in all_items if item["suggested_action"] == "dedupe_or_supersede_review"),
    }
    payload = {
        "generated_at": now(),
        "summary": summary,
        "items": items,
        "json": str(GOVERNANCE_DIR / "memory-governance.json"),
        "markdown": str(GOVERNANCE_DIR / "memory-governance.md"),
    }
    atomic_write_text(GOVERNANCE_DIR / "memory-governance.json", json.dumps(payload, ensure_ascii=False, indent=2))
    lines = ["# Clawwarden Memory Governance", "", f"- Generated: {payload['generated_at']}"]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.append("")
    for item in items:
        lines.extend(
            [
                f"## {item.get('title') or item.get('candidate_id')}",
                "",
                f"- Candidate: `{item.get('candidate_id')}`",
                f"- Risk: `{item.get('risk_level')}`",
                f"- Duplicates: `{item.get('likely_duplicates')}`",
                f"- Suggested action: `{item.get('suggested_action')}`",
                f"- Path: `{item.get('path')}`",
                "",
            ]
        )
    if not items:
        lines.append("No memory candidates need governance.")
    atomic_write_text(GOVERNANCE_DIR / "memory-governance.md", "\n".join(lines).rstrip() + "\n")
    log_event({"action": "memory_govern", "summary": summary})
    return payload


def skill_cycle_item(candidate: dict[str, Any]) -> dict[str, Any]:
    risk = candidate.get("risk_level") or "unknown"
    status = candidate.get("status")
    recommendation = candidate.get("recommendation") or "review"
    if status == "ready_for_workshop" and risk == "low":
        action = "create_skill_workshop_proposal"
    elif status == "ready_for_workshop":
        action = "review_before_workshop"
    elif status == "workshop_created":
        action = "await_apply_reject_or_quarantine"
    elif status == "applied":
        action = "closed_applied"
    else:
        action = "review_or_archive"
    return {
        "candidate_id": candidate.get("candidate_id"),
        "name": candidate.get("name"),
        "status": status,
        "risk_level": risk,
        "recommendation": recommendation,
        "path": candidate.get("_path"),
        "proposal_path": candidate.get("_proposal_path"),
        "workshop_proposal_id": candidate.get("workshop_proposal_id"),
        "suggested_action": action,
        "payload_command": f"{SCRIPT_DIR / 'skill_staging.py'} workshop-payload {candidate.get('candidate_id')}",
    }


def cmd_skill_cycle(args: argparse.Namespace) -> dict[str, Any]:
    selected_status = None if args.status == "all" else args.status
    candidates = load_skill_candidates(status=selected_status)
    items = [skill_cycle_item(candidate) for candidate in candidates[: args.limit]]
    summary = {
        "total": len(items),
        "ready_for_workshop": sum(1 for item in items if item["status"] == "ready_for_workshop"),
        "workshop_created": sum(1 for item in items if item["status"] == "workshop_created"),
        "needs_review": sum(1 for item in items if item["suggested_action"] in {"review_before_workshop", "review_or_archive"}),
    }
    payload = {
        "generated_at": now(),
        "summary": summary,
        "items": items,
        "json": str(SKILL_CYCLE_DIR / "skill-cycle.json"),
        "markdown": str(SKILL_CYCLE_DIR / "skill-cycle.md"),
    }
    atomic_write_text(SKILL_CYCLE_DIR / "skill-cycle.json", json.dumps(payload, ensure_ascii=False, indent=2))
    lines = ["# Clawwarden Skill Lifecycle", "", f"- Generated: {payload['generated_at']}"]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.append("")
    for item in items:
        lines.extend(
            [
                f"## {item.get('name') or item.get('candidate_id')}",
                "",
                f"- Candidate: `{item.get('candidate_id')}`",
                f"- Status: `{item.get('status')}`",
                f"- Risk: `{item.get('risk_level')}`",
                f"- Suggested action: `{item.get('suggested_action')}`",
                f"- Payload command: `{item.get('payload_command')}`",
                f"- Path: `{item.get('path')}`",
                "",
            ]
        )
    if not items:
        lines.append("No skill candidates in the selected lifecycle slice.")
    atomic_write_text(SKILL_CYCLE_DIR / "skill-cycle.md", "\n".join(lines).rstrip() + "\n")
    log_event({"action": "skill_cycle", "summary": summary, "status": selected_status or "all"})
    return payload


def cmd_decide(args: argparse.Namespace) -> dict[str, Any]:
    decision = decide(args)
    payload = {"decided_at": now(), "decision": asdict(decision)}
    log_event({"action": "decide", "task_id": decision.task_id, "decision": asdict(decision)})
    return payload


def cmd_start(args: argparse.Namespace) -> dict[str, Any]:
    decision = decide(args)
    outputs: dict[str, Any] = {}

    outputs["nmem_status"] = run_cmd(["nmem", "status"]) if nmem_available() else "disabled"

    if decision.needs_quota_check and QUOTA_HELPER.is_file():
        outputs["quota"] = run_cmd([str(QUOTA_HELPER)], json_mode=True)
    elif decision.needs_quota_check:
        outputs["quota"] = {"status": "disabled", "reason": "helper_not_configured"}

    if decision.needs_nmem_context and nmem_available():
        query = args.query or " ".join(part for part in [args.title, args.goal] if part)
        outputs["nmem_context"] = run_cmd(
            [str(NMEM_ADAPTER), "before-turn", query, "--task-id", decision.task_id, "--limit", str(args.memory_limit)],
            json_mode=True,
        )

    if decision.needs_ledger:
        ledger_cmd = [
            str(WORKFLOW_LEDGER),
            "create",
            "--task-id",
            decision.task_id,
            "--title",
            args.title or decision.task_id,
            "--goal",
            args.goal or args.query or args.title or "Clawwarden orchestrated task",
            "--current-step",
            args.current_step or "Task started through Clawwarden orchestrator",
            "--next-step",
            args.next_step or "Run task and checkpoint meaningful progress",
            "-t",
            "clawwarden",
            "-t",
            "openclaw",
        ]
        if outputs.get("nmem_context", {}).get("markdown"):
            ledger_cmd.extend(["--context", outputs["nmem_context"]["markdown"]])
        outputs["workflow_task"] = run_cmd(ledger_cmd, json_mode=True)

    payload = {
        "started_at": now(),
        "phase": "start",
        "decision": asdict(decision),
        "outputs": outputs,
    }
    run_path = write_run(decision.task_id, payload)
    log_event({"action": "start", "task_id": decision.task_id, "run": str(run_path), "decision": asdict(decision)})
    payload["run_path"] = str(run_path)
    return payload


def cmd_checkpoint(args: argparse.Namespace) -> dict[str, Any]:
    task = run_cmd(
        [
            str(WORKFLOW_LEDGER),
            "checkpoint",
            args.task_id,
            "--status",
            args.status,
            "--summary",
            args.summary,
            "--next-step",
            args.next_step or "",
            *sum((["-a", item] for item in (args.artifact or [])), []),
        ],
        json_mode=True,
    )
    payload = {"checkpointed_at": now(), "task": task}
    log_event({"action": "checkpoint", "task_id": args.task_id, "status": args.status, "summary": args.summary})
    return payload


def cmd_finish(args: argparse.Namespace) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    if (
        args.summary
        and not bool(getattr(args, "suppress_memory_candidate", False))
        and nmem_available()
    ):
        outputs["memory_candidate"] = run_cmd(
            [
                str(NMEM_ADAPTER),
                "after-turn",
                "--task-id",
                args.task_id,
                "--title",
                args.title or args.summary.splitlines()[0][:60],
                "--summary",
                args.summary,
                "--unit-type",
                args.unit_type,
                "-l",
                "openclaw",
                "-l",
                "clawwarden",
            ],
            json_mode=True,
        )

    if args.repeatable and (args.procedure or args.procedure_file):
        skill_cmd = [
            str(SKILL_STAGING),
            "draft",
            "--name",
            args.skill_name or slug(args.title or args.task_id, 40),
            "--description",
            args.skill_description or "Reusable Clawwarden workflow candidate",
            "--goal",
            args.skill_goal or args.title or "Capture a repeatable Clawwarden workflow",
            "--task-id",
            args.task_id,
            "-l",
            "openclaw",
            "-l",
            "clawwarden",
        ]
        if args.procedure_file:
            skill_cmd.extend(["--procedure-file", args.procedure_file])
        else:
            skill_cmd.extend(["--procedure", args.procedure or ""])
        if args.evidence:
            skill_cmd.extend(["--evidence", args.evidence])
        outputs["skill_candidate"] = run_cmd(skill_cmd, json_mode=True)

    outputs["workflow_status"] = run_cmd(
        [
            str(WORKFLOW_LEDGER),
            "status",
            args.task_id,
            args.status,
            "--reason",
            args.status_reason or "Finished through Clawwarden orchestrator",
        ],
        json_mode=True,
    )

    payload = {"finished_at": now(), "task_id": args.task_id, "outputs": outputs}
    run_path = write_run(args.task_id, payload)
    log_event({"action": "finish", "task_id": args.task_id, "status": args.status, "run": str(run_path)})
    payload["run_path"] = str(run_path)
    return payload


def cmd_review(args: argparse.Namespace) -> dict[str, Any]:
    stale_hours = getattr(args, "stale_hours", 24.0)
    inventory = review_inventory(stale_hours)
    memory_pending = run_cmd([str(NMEM_ADAPTER), "list-candidates", "--pending", "--limit", str(args.limit)])
    skill_ready = run_cmd([str(SKILL_STAGING), "list", "--status", "ready_for_workshop", "--limit", str(args.limit)])
    active_tasks = run_cmd([str(WORKFLOW_LEDGER), "list", "--status", "in_progress", "--limit", str(args.limit)])
    waiting_tasks = run_cmd([str(WORKFLOW_LEDGER), "list", "--status", "waiting", "--limit", str(args.limit)])
    review_queue = write_review_queue(
        collect_review_items(args.limit, stale_hours),
        inventory=inventory,
        window_limit=args.limit,
    )
    payload = {
        "reviewed_at": now(),
        "memory_pending": [line for line in memory_pending.splitlines() if line.strip()],
        "skill_ready_for_workshop": [line for line in skill_ready.splitlines() if line.strip()],
        "active_tasks": [line for line in active_tasks.splitlines() if line.strip()],
        "waiting_tasks": [line for line in waiting_tasks.splitlines() if line.strip()],
        "counts": inventory,
        "review_queue": review_queue,
    }
    log_event(
        {
            "action": "review",
            "counts": {key: len(value) for key, value in payload.items() if isinstance(value, list)},
            "review_queue_total": review_queue["total"],
        }
    )
    return payload


def cmd_health(args: argparse.Namespace) -> dict[str, Any]:
    compile_targets = [
        NMEM_ADAPTER,
        WORKFLOW_LEDGER,
        SKILL_STAGING,
        Path(__file__).resolve(),
    ]
    compile_targets.extend(path for path in (QUOTA_HELPER, ARTIFACT_HELPER) if path.is_file())
    py_compile = run_cmd(
        [
            "python3",
            "-m",
            "py_compile",
            *map(str, compile_targets),
        ]
    )
    nmem_status = run_cmd(["nmem", "status"]) if nmem_available() else "disabled"
    quota: Any = None
    if not args.no_quota and QUOTA_HELPER.is_file():
        quota = run_cmd([str(QUOTA_HELPER)], json_mode=True)
    review = cmd_review(argparse.Namespace(limit=args.limit))
    payload = {
        "checked_at": now(),
        "py_compile": py_compile or "ok",
        "nmem_status": nmem_status,
        "quota": quota,
        "review": review,
    }
    log_event({"action": "health", "quota_checked": quota is not None})
    return payload


def cmd_patrol(args: argparse.Namespace) -> dict[str, Any]:
    alerts: list[dict[str, str]] = []
    health = cmd_health(argparse.Namespace(limit=args.limit, no_quota=args.no_quota))
    review = health.get("review", {})
    governance = cmd_memory_govern(argparse.Namespace(limit=args.limit, pending_only=True))
    skill_lifecycle = cmd_skill_cycle(argparse.Namespace(limit=args.limit, status="ready_for_workshop"))
    artifact_publisher: dict[str, Any] = {"status": "disabled"}
    if ARTIFACT_HELPER.is_file():
        try:
            artifact_publisher = run_cmd([str(ARTIFACT_HELPER), "health", "--limit", str(args.limit)], json_mode=True)
        except Exception as exc:
            artifact_publisher = {"status": "failed", "error": str(exc)[:300]}
            alerts.append({"severity": "warning", "message": f"Artifact helper health failed: {str(exc)[:160]}"})

    nmem_value = str(health.get("nmem_status", ""))
    nmem_state = "disabled" if nmem_value == "disabled" else ("ok" if "status   ok" in nmem_value else "failed")
    py_compile_state = str(health.get("py_compile") or "")
    if py_compile_state != "ok":
        alerts.append({"severity": "critical", "message": f"py_compile failed or returned unexpected output: {py_compile_state[:160]}"})
    if nmem_state == "failed":
        alerts.append({"severity": "critical", "message": "nmem status is not ok"})

    quota = health.get("quota")
    codex_5h = quota_remaining(quota, "codex_5h_remaining_pct")
    codex_weekly = quota_remaining(quota, "codex_weekly_remaining_pct")
    spark_5h = quota_remaining(quota, "spark_5h_remaining_pct")
    if codex_5h is None and not args.no_quota and QUOTA_HELPER.is_file():
        alerts.append({"severity": "warning", "message": "Configured quota helper did not return a usable value"})
    elif codex_5h is not None and codex_5h < args.quota_critical:
        alerts.append({"severity": "critical", "message": f"Codex 5h remaining is {codex_5h}%, below {args.quota_critical}%"})
    elif codex_5h is not None and codex_5h < args.quota_warning:
        alerts.append({"severity": "warning", "message": f"Codex 5h remaining is {codex_5h}%, below {args.quota_warning}%"})

    inventory = review.get("counts", {})
    queues = {
        "memory_pending": int(inventory.get("memory_pending", len(review.get("memory_pending", [])))),
        "skill_ready_for_workshop": int(inventory.get("skill_ready_for_workshop", len(review.get("skill_ready_for_workshop", [])))),
        "active_tasks": int(inventory.get("active_tasks", len(review.get("active_tasks", [])))),
        "waiting_tasks": int(inventory.get("waiting_tasks", len(review.get("waiting_tasks", [])))),
        "review_queue": int(inventory.get("total", review.get("review_queue", {}).get("total", 0))),
        "review_window": int(review.get("review_queue", {}).get("window_count", 0)),
    }
    if queues["memory_pending"] > 0:
        alerts.append({"severity": "info", "message": f"{queues['memory_pending']} memory candidate(s) pending review"})
    if queues["skill_ready_for_workshop"] > 0:
        alerts.append({"severity": "info", "message": f"{queues['skill_ready_for_workshop']} skill candidate(s) ready for workshop"})

    stale_tasks: list[dict[str, Any]] = []
    for task in load_workflow_tasks({"planned", "in_progress", "waiting"}):
        age = task_age_hours(task)
        if age is None or age < args.stale_hours:
            continue
        stale = {
            "task_id": task.get("task_id"),
            "title": task.get("title"),
            "status": task.get("status"),
            "updated_at": task.get("updated_at"),
            "age_hours": round(age, 1),
            "path": task.get("_path"),
        }
        stale_tasks.append(stale)
    if stale_tasks:
        alerts.append({"severity": "warning", "message": f"{len(stale_tasks)} workflow task(s) stale for >= {args.stale_hours}h"})
    recovery_report = write_recovery_report(
        [
            recovery_plan_for_task(task)
            for task in load_workflow_tasks({"planned", "in_progress", "waiting", "blocked"})
            if (task_age_hours(task) is not None and task_age_hours(task) >= args.stale_hours)
        ]
    )

    severity_order = {"critical": 3, "warning": 2, "info": 1}
    severity = "ok"
    if alerts:
        severity = max(alerts, key=lambda item: severity_order.get(item.get("severity", ""), 0))["severity"]
    payload = {
        "checked_at": now(),
        "overall": "ok" if not alerts else "attention",
        "severity": severity,
        "alerts": alerts,
        "health": {
            "nmem": nmem_state,
            "py_compile": py_compile_state or "ok",
            "quota": {
                "codex_5h_remaining_pct": codex_5h,
                "codex_weekly_remaining_pct": codex_weekly,
                "spark_5h_remaining_pct": spark_5h,
            },
        },
        "queues": queues,
        "stale_tasks": stale_tasks,
        "governance": {
            "memory": governance.get("summary", {}),
            "skill_lifecycle": skill_lifecycle.get("summary", {}),
            "recovery_plans": recovery_report.get("total", 0),
        },
        "artifact_publisher": artifact_publisher,
        "status_files": {
            "json": str(STATUS_JSON),
            "markdown": str(STATUS_MD),
        },
    }
    write_status_page(payload)
    log_patrol(payload)
    log_event({"action": "patrol", "overall": payload["overall"], "severity": severity, "alerts": len(alerts)})
    return payload


def cmd_hook(args: argparse.Namespace) -> dict[str, Any]:
    config = load_hook_config()
    runtime_event_id = args.event_id or event_id(args.event)
    existing_replay = REPLAY_DIR / f"{slug(runtime_event_id, 120)}.json"
    if existing_replay.exists():
        existing = read_json_file(existing_replay) or {}
        return {
            "hooked_at": now(),
            "event_id": runtime_event_id,
            "duplicate": True,
            "action": "idempotent_replay",
            "original": existing,
            "replay_path": str(existing_replay),
        }
    task_args = argparse.Namespace(
        task_id=args.task_id,
        title=args.title,
        goal=args.goal,
        query=args.query,
        summary=args.summary,
        mode=args.mode,
        repeatable=args.repeatable,
        no_context=args.no_context,
        no_ledger=args.no_ledger,
        no_quota=args.no_quota,
    )
    decision = decide(task_args)
    execute_requested = bool(args.execute)
    kill_switch = bool(config.get("kill_switch"))
    execute_allowed = bool(config.get("enabled")) and not kill_switch and (
        execute_requested or not bool(config.get("require_execute_flag"))
    )
    action = "observe"
    outputs: dict[str, Any] = {}
    errors: list[dict[str, str]] = []

    review_total = 0
    review_data = read_json_file(REVIEW_JSON)
    if review_data:
        try:
            review_total = int(review_data.get("total") or 0)
        except (TypeError, ValueError):
            review_total = 0

    review_backpressure = review_total >= int(config.get("max_review_queue", 200))

    if kill_switch:
        action = "disabled_kill_switch"
        execute_allowed = False

    def guarded(label: str, callback) -> None:
        nonlocal outputs
        try:
            outputs[label] = callback()
        except Exception as exc:
            error = {"label": label, "message": str(exc)[:500]}
            errors.append(error)
            if not bool(config.get("fail_open", True)):
                raise

    if action.startswith("disabled_"):
        pass
    elif args.event == "turn-start":
        should_start = bool(config.get("auto_start_complex")) and decision.complexity == "complex"
        if should_start:
            action = "start"
            if execute_allowed:
                start_args = argparse.Namespace(
                    **vars(task_args),
                    memory_limit=min(int(args.memory_limit or config.get("max_memory_limit", 8)), int(config.get("max_memory_limit", 8))),
                    current_step=args.current_step,
                    next_step=args.next_step,
                )
                guarded("start", lambda: cmd_start(start_args))
        else:
            action = "skip_simple"
    elif args.event == "checkpoint":
        action = "checkpoint"
        if execute_allowed and config.get("auto_checkpoint"):
            if not args.task_id or not args.summary:
                raise ValueError("checkpoint hook requires --task-id and --summary")
            guarded(
                "checkpoint",
                lambda: cmd_checkpoint(
                    argparse.Namespace(
                        task_id=args.task_id,
                        summary=args.summary,
                        status=args.status or "in_progress",
                        next_step=args.next_step,
                        artifact=args.artifact,
                    )
                )
            )
    elif args.event == "turn-finish":
        action = "finish_without_candidate_backpressure" if review_backpressure else "finish_candidates"
        if execute_allowed and config.get("auto_finish_candidates"):
            if not args.task_id:
                raise ValueError("turn-finish hook requires --task-id")
            guarded(
                "finish",
                lambda: cmd_finish(
                    argparse.Namespace(
                        task_id=args.task_id,
                        summary=args.summary,
                        title=args.title,
                        unit_type=args.unit_type,
                        status=args.status or "completed",
                        status_reason=args.status_reason,
                        repeatable=bool(args.repeatable and config.get("auto_skill_staging")),
                        procedure=args.procedure,
                        procedure_file=args.procedure_file,
                        skill_name=args.skill_name,
                        skill_description=args.skill_description,
                        skill_goal=args.skill_goal,
                        evidence=args.evidence,
                        suppress_memory_candidate=bool(
                            review_backpressure or getattr(args, "suppress_memory_candidate", False)
                        ),
                    )
                )
            )
    elif args.event == "heartbeat":
        action = "patrol"
        if execute_allowed:
            guarded(
                "patrol",
                lambda: cmd_patrol(
                    argparse.Namespace(
                        limit=args.limit,
                        stale_hours=args.stale_hours,
                        quota_warning=args.quota_warning,
                        quota_critical=args.quota_critical,
                        no_quota=args.no_quota,
                    )
                )
            )

    payload = {
        "hooked_at": now(),
        "event_id": runtime_event_id,
        "event": args.event,
        "action": action,
        "source": args.source,
        "execute_requested": execute_requested,
        "execute_allowed": execute_allowed,
        "review_queue_total": review_total,
        "review_backpressure": review_backpressure,
        "config": config,
        "decision": asdict(decision),
        "input": {
            "task_id": args.task_id,
            "title": args.title,
            "goal": args.goal,
            "query": args.query,
            "summary": args.summary,
            "mode": args.mode,
            "repeatable": args.repeatable,
        },
        "outputs": outputs,
        "errors": errors,
    }
    replay_path = log_runtime_event(payload)
    payload["replay_path"] = str(replay_path)
    log_event({"action": "runtime_hook", "event": args.event, "task_id": decision.task_id, "hook_action": action, "executed": bool(outputs)})
    return payload


def cmd_replay(args: argparse.Namespace) -> dict[str, Any]:
    if args.event_id:
        path = REPLAY_DIR / f"{slug(args.event_id, 120)}.json"
        if not path.exists():
            raise FileNotFoundError(f"replay event not found: {args.event_id}")
        payload = read_json_file(path)
        return {"loaded_at": now(), "event_id": args.event_id, "path": str(path), "event": payload}

    events: list[dict[str, Any]] = []
    if RUNTIME_EVENT_LOG.exists():
        lines = RUNTIME_EVENT_LOG.read_text(encoding="utf-8").splitlines()
        for line in reversed(lines[-args.limit :]):
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            events.append(
                {
                    "event_id": item.get("event_id"),
                    "recorded_at": item.get("recorded_at"),
                    "event": item.get("event"),
                    "action": item.get("action"),
                    "task_id": item.get("decision", {}).get("task_id"),
                    "execute_allowed": item.get("execute_allowed"),
                    "errors": item.get("errors", []),
                    "replay_path": str(REPLAY_DIR / f"{slug(str(item.get('event_id')), 120)}.json"),
                }
            )
    return {"listed_at": now(), "total": len(events), "events": events}


def cmd_hook_config(args: argparse.Namespace) -> dict[str, Any]:
    config = load_hook_config()
    changed: dict[str, Any] = {}
    if args.enabled is not None:
        config["enabled"] = args.enabled == "true"
        changed["enabled"] = config["enabled"]
    if args.kill_switch is not None:
        config["kill_switch"] = args.kill_switch == "true"
        changed["kill_switch"] = config["kill_switch"]
    if args.require_execute_flag is not None:
        config["require_execute_flag"] = args.require_execute_flag == "true"
        changed["require_execute_flag"] = config["require_execute_flag"]
    if args.fail_open is not None:
        config["fail_open"] = args.fail_open == "true"
        changed["fail_open"] = config["fail_open"]
    if args.max_review_queue is not None:
        config["max_review_queue"] = args.max_review_queue
        changed["max_review_queue"] = config["max_review_queue"]
    if changed:
        atomic_write_text(HOOK_CONFIG, json.dumps(config, ensure_ascii=False, indent=2))
        log_event({"action": "hook_config", "changed": changed})
    return {"configured_at": now(), "changed": changed, "config": config, "path": str(HOOK_CONFIG)}


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Clawwarden orchestrator CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_task_fields(p: argparse.ArgumentParser) -> None:
        p.add_argument("--task-id")
        p.add_argument("--title")
        p.add_argument("--goal")
        p.add_argument("--query")
        p.add_argument("--summary")
        p.add_argument("--mode", choices=["auto", "simple", "full"], default="auto")
        p.add_argument("--repeatable", action="store_true")
        p.add_argument("--no-context", action="store_true")
        p.add_argument("--no-ledger", action="store_true")
        p.add_argument("--no-quota", action="store_true")

    decide_p = sub.add_parser("decide", help="Print trigger decisions without executing P1/P2/P3")
    add_task_fields(decide_p)

    start = sub.add_parser("start", help="Start an orchestrated task")
    add_task_fields(start)
    start.add_argument("--memory-limit", type=int, default=8)
    start.add_argument("--current-step")
    start.add_argument("--next-step")

    checkpoint = sub.add_parser("checkpoint", help="Add a workflow checkpoint")
    checkpoint.add_argument("task_id")
    checkpoint.add_argument("--summary", required=True)
    checkpoint.add_argument("--status", default="in_progress")
    checkpoint.add_argument("--next-step")
    checkpoint.add_argument("-a", "--artifact", action="append")

    finish = sub.add_parser("finish", help="Finish a task and generate safe candidates")
    finish.add_argument("task_id")
    finish.add_argument("--summary")
    finish.add_argument("--title")
    finish.add_argument("--unit-type", default="learning")
    finish.add_argument("--status", default="completed")
    finish.add_argument("--status-reason")
    finish.add_argument("--repeatable", action="store_true")
    finish.add_argument("--procedure")
    finish.add_argument("--procedure-file")
    finish.add_argument("--skill-name")
    finish.add_argument("--skill-description")
    finish.add_argument("--skill-goal")
    finish.add_argument("--evidence")
    finish.add_argument("--suppress-memory-candidate", action="store_true")

    review = sub.add_parser("review", help="Show pending review queues")
    review.add_argument("--limit", type=int, default=20)
    review.add_argument("--stale-hours", type=float, default=24.0)

    health = sub.add_parser("health", help="Run health checks for Clawwarden components")
    health.add_argument("--limit", type=int, default=20)
    health.add_argument("--no-quota", action="store_true")

    patrol = sub.add_parser("patrol", help="Run anomaly-only background patrol and write status pages")
    patrol.add_argument("--limit", type=int, default=20)
    patrol.add_argument("--stale-hours", type=float, default=24.0)
    patrol.add_argument("--quota-warning", type=int, default=20)
    patrol.add_argument("--quota-critical", type=int, default=10)
    patrol.add_argument("--no-quota", action="store_true")

    hook = sub.add_parser("hook", help="Conservative runtime hook entrypoint for future OpenClaw integration")
    hook.add_argument("event", choices=["turn-start", "checkpoint", "turn-finish", "heartbeat"])
    hook.add_argument("--execute", action="store_true", help="Actually run allowed orchestration actions")
    hook.add_argument("--event-id")
    hook.add_argument("--source", default="manual")
    hook.add_argument("--task-id")
    hook.add_argument("--title")
    hook.add_argument("--goal")
    hook.add_argument("--query")
    hook.add_argument("--summary")
    hook.add_argument("--mode", choices=["auto", "simple", "full"], default="auto")
    hook.add_argument("--repeatable", action="store_true")
    hook.add_argument("--no-context", action="store_true")
    hook.add_argument("--no-ledger", action="store_true")
    hook.add_argument("--no-quota", action="store_true")
    hook.add_argument("--memory-limit", type=int)
    hook.add_argument("--current-step")
    hook.add_argument("--next-step")
    hook.add_argument("--status")
    hook.add_argument("--status-reason")
    hook.add_argument("-a", "--artifact", action="append")
    hook.add_argument("--unit-type", default="learning")
    hook.add_argument("--procedure")
    hook.add_argument("--procedure-file")
    hook.add_argument("--skill-name")
    hook.add_argument("--skill-description")
    hook.add_argument("--skill-goal")
    hook.add_argument("--evidence")
    hook.add_argument("--suppress-memory-candidate", action="store_true")
    hook.add_argument("--limit", type=int, default=20)
    hook.add_argument("--stale-hours", type=float, default=24.0)
    hook.add_argument("--quota-warning", type=int, default=20)
    hook.add_argument("--quota-critical", type=int, default=10)

    replay = sub.add_parser("replay", help="List or show runtime hook replay events")
    replay.add_argument("--event-id")
    replay.add_argument("--limit", type=int, default=20)

    hook_config = sub.add_parser("hook-config", help="View or update runtime hook safety config")
    hook_config.add_argument("--enabled", choices=["true", "false"])
    hook_config.add_argument("--kill-switch", choices=["true", "false"])
    hook_config.add_argument("--require-execute-flag", choices=["true", "false"])
    hook_config.add_argument("--fail-open", choices=["true", "false"])
    hook_config.add_argument("--max-review-queue", type=int)

    integrate = sub.add_parser("integrate", help="Write runtime integration manifest and optional hook profile")
    integrate.add_argument("--profile", choices=["observe", "conservative", "active"], default="conservative")
    integrate.add_argument("--write", action="store_true")
    integrate.add_argument("--kill-switch", choices=["true", "false"])

    recover_stale = sub.add_parser("recover-stale", help="Generate recovery plans for stale Workflow Ledger tasks")
    recover_stale.add_argument("--stale-hours", type=float, default=24.0)
    recover_stale.add_argument("--limit", type=int, default=20)
    recover_stale.add_argument("--checkpoint", action="store_true", help="Mark stale planned/in_progress tasks as waiting after writing recovery plans")

    memory_govern = sub.add_parser("memory-govern", help="Generate memory candidate governance report")
    memory_govern.add_argument("--limit", type=int, default=50)
    memory_govern.add_argument("--pending-only", action="store_true", default=True)

    skill_cycle = sub.add_parser("skill-cycle", help="Generate Skill Staging lifecycle report")
    skill_cycle.add_argument("--status", default="ready_for_workshop", help="Candidate status to inspect, or 'all'")
    skill_cycle.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()
    if args.command == "decide":
        print_json(cmd_decide(args))
    elif args.command == "start":
        print_json(cmd_start(args))
    elif args.command == "checkpoint":
        print_json(cmd_checkpoint(args))
    elif args.command == "finish":
        print_json(cmd_finish(args))
    elif args.command == "review":
        print_json(cmd_review(args))
    elif args.command == "health":
        print_json(cmd_health(args))
    elif args.command == "patrol":
        print_json(cmd_patrol(args))
    elif args.command == "hook":
        print_json(cmd_hook(args))
    elif args.command == "replay":
        print_json(cmd_replay(args))
    elif args.command == "hook-config":
        print_json(cmd_hook_config(args))
    elif args.command == "integrate":
        print_json(cmd_integrate(args))
    elif args.command == "recover-stale":
        print_json(cmd_recover_stale(args))
    elif args.command == "memory-govern":
        print_json(cmd_memory_govern(args))
    elif args.command == "skill-cycle":
        print_json(cmd_skill_cycle(args))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"clawwarden: {exc}", file=sys.stderr)
        raise SystemExit(1)
