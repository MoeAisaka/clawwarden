#!/usr/bin/env python3
"""OpenClaw-facing nmem adapter.

This is a conservative CLI layer for the first phase of the OpenClaw memory
adapter plan:

- before-turn: create a task-scoped memory context pack.
- after-turn: create a reviewed memory candidate from a task summary.
- commit: write an approved candidate to nmem.

It does not hook into OpenClaw runtime automatically yet. Use it from
heartbeat, task scripts, or manual operator workflows first.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "memory" / "nmem-adapter"
CONTEXT_DIR = BASE_DIR / "contexts"
CANDIDATE_DIR = BASE_DIR / "candidates"
LEDGER = BASE_DIR / "adapter-ledger.jsonl"
LOCK_FILE = BASE_DIR / ".nmem-adapter.lock"

DEFAULT_LIMIT = 8

HIGH_RISK_PATTERNS = [
    r"api[_-]?key",
    r"secret",
    r"token",
    r"password",
    r"密码",
    r"私钥",
    r"cookie",
    r"session",
    r"SOUL\.md",
    r"AGENTS\.md",
    r"PREPROCESS\.md",
    r"长期规则",
    r"人格",
    r"生产",
    r"交易系统",
    r"实盘",
]

MEDIUM_RISK_PATTERNS = [
    r"记住",
    r"偏好",
    r"流程",
    r"账号",
    r"路径",
    r"自动",
    r"cron",
    r"浏览器登录态",
]

UNIT_TYPES = {"fact", "decision", "plan", "procedure", "learning", "context", "event"}


@dataclass
class AdapterPaths:
    markdown: str
    json: str


@dataclass
class MemoryCandidate:
    candidate_id: str
    created_at: str
    task_id: str
    title: str
    content: str
    unit_type: str
    importance: float
    labels: list[str]
    risk_level: str
    risk_reasons: list[str]
    duplicate_query: str
    duplicate_results: Any
    recommendation: str
    status: str = "pending"


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug(value: str, limit: int = 48) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    normalized = normalized.strip("-._")
    if not normalized:
        normalized = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return normalized[:limit]


def ensure_dirs() -> None:
    CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)


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


def candidate_path(candidate_id: str) -> Path:
    return CANDIDATE_DIR / f"{slug(candidate_id, 96)}.json"


def resolve_candidate_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.exists():
        return path
    path = candidate_path(value)
    if path.exists():
        return path
    raise FileNotFoundError(f"Candidate not found: {value}")


def run_nmem(args: list[str], input_text: str | None = None, json_mode: bool = False) -> Any:
    cmd = ["nmem", *args]
    result = subprocess.run(
        cmd,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {message}")
    output = result.stdout.strip()
    if json_mode:
        return json.loads(output) if output else {}
    return output


def log_event(event: dict[str, Any]) -> None:
    ensure_dirs()
    event = {"time": now(), **event}
    with LEDGER.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def classify_risk(text: str) -> tuple[str, list[str]]:
    reasons: list[str] = []
    for pattern in HIGH_RISK_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            reasons.append(f"high:{pattern}")
    if reasons:
        return "high", reasons

    for pattern in MEDIUM_RISK_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            reasons.append(f"medium:{pattern}")
    if reasons:
        return "medium", reasons
    return "low", []


def infer_unit_type(text: str, explicit: str | None) -> str:
    if explicit:
        if explicit not in UNIT_TYPES:
            raise ValueError(f"Unsupported unit type: {explicit}")
        return explicit
    lowered = text.lower()
    if any(word in text for word in ["决定", "确定", "采用", "改为", "不再"]):
        return "decision"
    if any(word in text for word in ["步骤", "流程", "用法", "执行", "操作"]):
        return "procedure"
    if any(word in text for word in ["经验", "教训", "注意", "踩坑", "验证"]):
        return "learning"
    if any(word in text for word in ["计划", "下一步", "Phase", "阶段"]):
        return "plan"
    if any(word in lowered for word in ["bug", "error", "fixed", "failed"]):
        return "learning"
    return "context"


def compact_search_results(results: Any, threshold: float = 0.82, snippet_chars: int = 360) -> dict[str, Any]:
    if not isinstance(results, dict):
        return {"total": 0, "likely_duplicates": 0, "memories": []}
    compact_memories = []
    likely_duplicates = 0
    for memory in results.get("memories", []):
        score = memory.get("score")
        try:
            numeric_score = float(score)
        except (TypeError, ValueError):
            numeric_score = 0.0
        if numeric_score >= threshold:
            likely_duplicates += 1
        content = str(memory.get("content", "")).strip()
        compact_memories.append(
            {
                "id": memory.get("id"),
                "title": memory.get("title"),
                "score": score,
                "unit_type": memory.get("unit_type"),
                "created_at": memory.get("created_at"),
                "snippet": content[:snippet_chars] + ("..." if len(content) > snippet_chars else ""),
            }
        )
    return {
        "query": results.get("query"),
        "total": results.get("total", len(compact_memories)),
        "likely_duplicates": likely_duplicates,
        "duplicate_score_threshold": threshold,
        "memories": compact_memories,
    }


def recommendation_for(risk_level: str, likely_duplicates: int) -> str:
    if risk_level == "high":
        return "manual_review_required"
    if likely_duplicates > 0:
        return "review_for_dedupe_or_supersede"
    if risk_level == "medium":
        return "review_recommended"
    return "auto_commit_allowed"


def build_context(args: argparse.Namespace) -> AdapterPaths:
    ensure_dirs()
    task_id = args.task_id or f"task-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug(args.query, 20)}"
    status = run_nmem(["status"])
    results = run_nmem(["m", "search", args.query, "--limit", str(args.limit), "-j"], json_mode=True)

    payload = {
        "generated_at": now(),
        "task_id": task_id,
        "query": args.query,
        "scope": "task-scoped read-only context",
        "nmem_status": status,
        "results": results,
        "use_rules": [
            "Use only for the current OpenClaw turn/task.",
            "Do not infer unrelated private preferences.",
            "Do not write or delete nmem from downstream agents.",
            "Return durable learnings as memory candidates for review.",
        ],
    }

    base = CONTEXT_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{slug(task_id)}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    atomic_write_text(json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    memories = results.get("memories", []) if isinstance(results, dict) else []
    lines = [
        "# OpenClaw nmem Context Pack",
        "",
        f"- Generated: {payload['generated_at']}",
        f"- Task ID: `{task_id}`",
        f"- Query: `{args.query}`",
        "- Scope: task-scoped, read-only",
        "",
        "## nmem Status",
        "",
        "```text",
        status,
        "```",
        "",
        "## Relevant Memories",
        "",
    ]
    if memories:
        for idx, memory in enumerate(memories, start=1):
            lines.extend(
                [
                    f"### {idx}. {memory.get('title', '(untitled)')}",
                    "",
                    f"- ID: `{memory.get('id', '')}`",
                    f"- Type: `{memory.get('unit_type', '')}`",
                    f"- Score: `{memory.get('score', '')}`",
                    "",
                    str(memory.get("content", "")).strip(),
                    "",
                ]
            )
    else:
        lines.append("No matching memories found.")
        lines.append("")
    lines.extend(
        [
            "## Use Rules",
            "",
            "- Use only for this task.",
            "- Do not write or delete nmem directly.",
            "- Report durable learnings back as memory candidates.",
        ]
    )
    atomic_write_text(md_path, "\n".join(lines) + "\n")
    log_event({"action": "before_turn", "task_id": task_id, "query": args.query, "context": str(md_path)})
    return AdapterPaths(markdown=str(md_path), json=str(json_path))


def load_summary(args: argparse.Namespace) -> str:
    if args.summary_file:
        return Path(args.summary_file).read_text(encoding="utf-8").strip()
    if args.summary:
        return args.summary.strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    raise ValueError("Provide --summary, --summary-file, or stdin content.")


def create_candidate(args: argparse.Namespace) -> MemoryCandidate:
    ensure_dirs()
    content = load_summary(args)
    if not content:
        raise ValueError("Summary content is empty.")
    title = args.title or content.splitlines()[0][:60]
    task_id = args.task_id or f"task-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    unit_type = infer_unit_type(content, args.unit_type)
    importance = args.importance if args.importance is not None else 0.75
    labels = args.label or []
    risk_level, risk_reasons = classify_risk(f"{title}\n{content}")
    duplicate_query = args.duplicate_query or title
    raw_duplicate_results = run_nmem(
        ["m", "search", duplicate_query, "--limit", str(args.duplicate_limit), "-j"],
        json_mode=True,
    )
    duplicate_results = compact_search_results(raw_duplicate_results, threshold=args.duplicate_threshold)
    likely_duplicates = int(duplicate_results.get("likely_duplicates", 0))
    recommendation = recommendation_for(risk_level, likely_duplicates)
    candidate_id = args.id or f"memcand-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug(title, 24)}"

    candidate = MemoryCandidate(
        candidate_id=candidate_id,
        created_at=now(),
        task_id=task_id,
        title=title,
        content=content,
        unit_type=unit_type,
        importance=float(importance),
        labels=labels,
        risk_level=risk_level,
        risk_reasons=risk_reasons,
        duplicate_query=duplicate_query,
        duplicate_results=duplicate_results,
        recommendation=recommendation,
    )

    path = candidate_path(candidate_id)
    atomic_write_text(path, json.dumps(asdict(candidate), ensure_ascii=False, indent=2))
    log_event(
        {
            "action": "after_turn",
            "task_id": task_id,
            "candidate_id": candidate_id,
            "risk_level": risk_level,
            "recommendation": recommendation,
            "candidate": str(path),
        }
    )

    if args.auto_commit:
        if recommendation != "auto_commit_allowed":
            raise RuntimeError(f"Refusing auto-commit: {recommendation}")
        commit_candidate(path, force=False)
    return candidate


def commit_candidate(path: Path, force: bool) -> str:
    candidate = json.loads(path.read_text(encoding="utf-8"))
    if candidate.get("status") == "committed":
        return candidate.get("nmem_result", "already committed")
    if candidate.get("recommendation") != "auto_commit_allowed" and not force:
        raise RuntimeError(
            f"Candidate requires review: {candidate.get('recommendation')}. "
            "Use --force only after manual approval."
        )

    cmd = [
        "m",
        "add",
        "--stdin",
        "--id",
        candidate["candidate_id"].replace("memcand-", "memory-"),
        "-t",
        candidate["title"],
        "-i",
        str(candidate["importance"]),
        "--unit-type",
        candidate["unit_type"],
        "--source",
        "openclaw-nmem-adapter",
    ]
    for label in candidate.get("labels", []):
        cmd.extend(["-l", label])
    result = run_nmem(cmd, input_text=candidate["content"])
    candidate["status"] = "committed"
    candidate["committed_at"] = now()
    candidate["nmem_result"] = result
    atomic_write_text(path, json.dumps(candidate, ensure_ascii=False, indent=2))
    log_event({"action": "commit", "candidate_id": candidate["candidate_id"], "candidate": str(path)})
    return result


def list_candidates(args: argparse.Namespace) -> list[str]:
    ensure_dirs()
    candidates = sorted(CANDIDATE_DIR.glob("*.json"), reverse=True)
    if args.pending:
        filtered = []
        for path in candidates:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if data.get("status") == "pending":
                filtered.append(path)
        candidates = filtered
    return [str(path) for path in candidates[: args.limit]]


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenClaw nmem adapter CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    before = sub.add_parser("before-turn", help="Build task-scoped context from nmem")
    before.add_argument("query")
    before.add_argument("--task-id")
    before.add_argument("--limit", type=int, default=DEFAULT_LIMIT)

    after = sub.add_parser("after-turn", help="Create a memory candidate from a task summary")
    after.add_argument("--task-id")
    after.add_argument("--summary")
    after.add_argument("--summary-file")
    after.add_argument("--title")
    after.add_argument("--id")
    after.add_argument("--unit-type")
    after.add_argument("--importance", type=float)
    after.add_argument("-l", "--label", action="append")
    after.add_argument("--duplicate-query")
    after.add_argument("--duplicate-limit", type=int, default=3)
    after.add_argument("--duplicate-threshold", type=float, default=0.82)
    after.add_argument("--auto-commit", action="store_true")

    commit = sub.add_parser("commit", help="Commit an approved candidate to nmem")
    commit.add_argument("candidate")
    commit.add_argument("--force", action="store_true", help="Commit reviewed medium/high-risk candidates")

    listing = sub.add_parser("list-candidates", help="List candidate JSON files")
    listing.add_argument("--pending", action="store_true")
    listing.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if args.command == "before-turn":
        paths = build_context(args)
        print(json.dumps(asdict(paths), ensure_ascii=False, indent=2))
    elif args.command == "after-turn":
        candidate = create_candidate(args)
        print(json.dumps(asdict(candidate), ensure_ascii=False, indent=2))
    elif args.command == "commit":
        print(commit_candidate(resolve_candidate_path(args.candidate), force=args.force))
    elif args.command == "list-candidates":
        for path in list_candidates(args):
            print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
