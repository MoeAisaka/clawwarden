#!/usr/bin/env python3
"""Lightweight Workflow Ledger for OpenClaw tasks.

Phase 2 goal:
- keep task state durable across turns;
- record checkpoints and recovery hints;
- avoid a heavy orchestration framework until the workflow shape justifies it.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "memory" / "workflow-ledger"
TASK_DIR = BASE_DIR / "tasks"
EVENT_LOG = BASE_DIR / "workflow-ledger.jsonl"
LOCK_FILE = BASE_DIR / ".workflow-ledger.lock"

STATUSES = {"planned", "in_progress", "waiting", "blocked", "completed", "failed", "cancelled"}


@dataclass
class Checkpoint:
    time: str
    status: str
    summary: str
    next_step: str = ""
    artifacts: list[str] = field(default_factory=list)


@dataclass
class Task:
    task_id: str
    title: str
    goal: str
    status: str
    created_at: str
    updated_at: str
    owner: str = "openclaw"
    priority: str = "normal"
    tags: list[str] = field(default_factory=list)
    context_path: str | None = None
    current_step: str = ""
    next_step: str = ""
    artifacts: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    checkpoints: list[dict[str, Any]] = field(default_factory=list)


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug(value: str, limit: int = 52) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    if not normalized:
        normalized = f"task-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    return normalized[:limit]


def ensure_dirs() -> None:
    TASK_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def ledger_lock():
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


def task_path(task_id: str) -> Path:
    return TASK_DIR / f"{slug(task_id, 96)}.json"


def log_event(event: dict[str, Any]) -> None:
    ensure_dirs()
    payload = {"time": now(), **event}
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def load_task(task_id: str) -> Task:
    path = task_path(task_id)
    if not path.exists():
        raise FileNotFoundError(f"Task not found: {task_id}")
    data = json.loads(path.read_text(encoding="utf-8"))
    return Task(**data)


def save_task(task: Task) -> Path:
    ensure_dirs()
    task.updated_at = now()
    path = task_path(task.task_id)
    atomic_write_text(path, json.dumps(asdict(task), ensure_ascii=False, indent=2))
    return path


def create_task(args: argparse.Namespace) -> Task:
    with ledger_lock():
        task_id = args.task_id or f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug(args.title, 28)}"
        path = task_path(task_id)
        if path.exists() and not args.force:
            raise FileExistsError(f"Task already exists: {task_id}. Use --force to overwrite.")
        task = Task(
            task_id=task_id,
            title=args.title,
            goal=args.goal,
            status="planned",
            created_at=now(),
            updated_at=now(),
            owner=args.owner,
            priority=args.priority,
            tags=args.tag or [],
            context_path=args.context,
            current_step=args.current_step or "",
            next_step=args.next_step or "",
            artifacts=args.artifact or [],
        )
        save_task(task)
        log_event({"action": "create", "task_id": task.task_id, "title": task.title, "status": task.status})
        return task


def add_checkpoint(args: argparse.Namespace) -> Task:
    with ledger_lock():
        task = load_task(args.task_id)
        if args.status:
            if args.status not in STATUSES:
                raise ValueError(f"Unsupported status: {args.status}")
            task.status = args.status
        checkpoint = Checkpoint(
            time=now(),
            status=task.status,
            summary=args.summary,
            next_step=args.next_step or task.next_step,
            artifacts=args.artifact or [],
        )
        task.checkpoints.append(asdict(checkpoint))
        if args.current_step:
            task.current_step = args.current_step
        if args.next_step is not None:
            task.next_step = args.next_step
        if args.artifact:
            task.artifacts.extend([item for item in args.artifact if item not in task.artifacts])
        save_task(task)
        log_event({"action": "checkpoint", "task_id": task.task_id, "status": task.status, "summary": args.summary})
        return task


def set_status(args: argparse.Namespace) -> Task:
    if args.status not in STATUSES:
        raise ValueError(f"Unsupported status: {args.status}")
    with ledger_lock():
        task = load_task(args.task_id)
        task.status = args.status
        if args.reason:
            checkpoint = Checkpoint(time=now(), status=task.status, summary=args.reason, next_step=args.next_step or "")
            task.checkpoints.append(asdict(checkpoint))
        if args.next_step is not None:
            task.next_step = args.next_step
        if args.blocker:
            task.blockers.append(args.blocker)
        save_task(task)
        log_event({"action": "status", "task_id": task.task_id, "status": task.status, "reason": args.reason})
        return task


def list_tasks(args: argparse.Namespace) -> list[Task]:
    ensure_dirs()
    tasks = []
    for path in sorted(TASK_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            task = Task(**json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
        if args.status and task.status != args.status:
            continue
        if args.tag and args.tag not in task.tags:
            continue
        tasks.append(task)
        if len(tasks) >= args.limit:
            break
    return tasks


def recovery_text(task: Task) -> str:
    lines = [
        f"# Workflow Recovery: {task.title}",
        "",
        f"- Task ID: `{task.task_id}`",
        f"- Status: `{task.status}`",
        f"- Goal: {task.goal}",
        f"- Current step: {task.current_step or '(not set)'}",
        f"- Next step: {task.next_step or '(not set)'}",
    ]
    if task.context_path:
        lines.append(f"- Context pack: `{task.context_path}`")
    if task.blockers:
        lines.extend(["", "## Blockers", *[f"- {item}" for item in task.blockers]])
    if task.artifacts:
        lines.extend(["", "## Artifacts", *[f"- `{item}`" for item in task.artifacts]])
    if task.checkpoints:
        lines.extend(["", "## Recent Checkpoints"])
        for checkpoint in task.checkpoints[-5:]:
            lines.append(f"- {checkpoint.get('time')} [{checkpoint.get('status')}] {checkpoint.get('summary')}")
            if checkpoint.get("next_step"):
                lines.append(f"  Next: {checkpoint.get('next_step')}")
    return "\n".join(lines) + "\n"


def print_task(task: Task) -> None:
    print(json.dumps(asdict(task), ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenClaw Workflow Ledger CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Create a durable task record")
    create.add_argument("--task-id")
    create.add_argument("--title", required=True)
    create.add_argument("--goal", required=True)
    create.add_argument("--owner", default="openclaw")
    create.add_argument("--priority", default="normal")
    create.add_argument("--context")
    create.add_argument("--current-step")
    create.add_argument("--next-step")
    create.add_argument("-t", "--tag", action="append")
    create.add_argument("-a", "--artifact", action="append")
    create.add_argument("--force", action="store_true")

    checkpoint = sub.add_parser("checkpoint", help="Append a task checkpoint")
    checkpoint.add_argument("task_id")
    checkpoint.add_argument("--summary", required=True)
    checkpoint.add_argument("--status")
    checkpoint.add_argument("--current-step")
    checkpoint.add_argument("--next-step")
    checkpoint.add_argument("-a", "--artifact", action="append")

    status = sub.add_parser("status", help="Set task status")
    status.add_argument("task_id")
    status.add_argument("status")
    status.add_argument("--reason")
    status.add_argument("--next-step")
    status.add_argument("--blocker")

    show = sub.add_parser("show", help="Show task JSON")
    show.add_argument("task_id")

    recover = sub.add_parser("recover", help="Print a human-readable recovery note")
    recover.add_argument("task_id")

    listing = sub.add_parser("list", help="List recent task records")
    listing.add_argument("--status")
    listing.add_argument("--tag")
    listing.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    if args.command == "create":
        print_task(create_task(args))
    elif args.command == "checkpoint":
        print_task(add_checkpoint(args))
    elif args.command == "status":
        print_task(set_status(args))
    elif args.command == "show":
        print_task(load_task(args.task_id))
    elif args.command == "recover":
        print(recovery_text(load_task(args.task_id)), end="")
    elif args.command == "list":
        for task in list_tasks(args):
            print(f"{task.updated_at}\t{task.status}\t{task.task_id}\t{task.title}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"workflow_ledger: {exc}", file=sys.stderr)
        raise SystemExit(1)
