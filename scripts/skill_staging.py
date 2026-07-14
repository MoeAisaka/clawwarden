#!/usr/bin/env python3
"""Conservative Skill Staging workflow for OpenClaw.

Phase 3 goal:
- notice repeatable procedures and turn them into reviewed skill candidates;
- keep candidates outside live skills and outside Skill Workshop proposals;
- hand off final proposal creation to OpenClaw `skill_workshop`.

This script intentionally does not apply, install, or edit live skills.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "memory" / "skill-staging"
CANDIDATE_DIR = BASE_DIR / "candidates"
VALIDATION_DIR = BASE_DIR / "validation"
LEDGER = BASE_DIR / "skill-staging-ledger.jsonl"
SKILLS_DIR = ROOT / "skills"
LOCK_FILE = BASE_DIR / ".skill-staging.lock"

STATUSES = {"draft", "ready_for_workshop", "workshop_created", "rejected", "applied", "archived"}
STOP_TERMS = {
    "openclaw",
    "skill",
    "skills",
    "stage",
    "staging",
    "workflow",
    "local",
    "phase",
    "smoke",
    "candidate",
    "candidates",
    "review",
}

HIGH_RISK_PATTERNS = [
    r"api[_-]?key",
    r"secret",
    r"token",
    r"password",
    r"cookie",
    r"session",
    r"私钥",
    r"密码",
    r"登录态",
    r"生产",
    r"交易系统",
    r"实盘",
    r"SOUL\.md",
    r"AGENTS\.md",
    r"PREPROCESS\.md",
    r"长期规则",
    r"人格",
]

MEDIUM_RISK_PATTERNS = [
    r"自动",
    r"cron",
    r"浏览器",
    r"MCP",
    r"账号",
    r"路径",
    r"外部发送",
    r"公开发布",
]


@dataclass
class SkillCandidate:
    candidate_id: str
    created_at: str
    updated_at: str
    name: str
    description: str
    goal: str
    evidence: str
    proposal_content: str
    source_task_id: str = ""
    source_files: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    risk_level: str = "low"
    risk_reasons: list[str] = field(default_factory=list)
    existing_skill_matches: list[dict[str, str]] = field(default_factory=list)
    recommendation: str = "ready_for_workshop"
    status: str = "draft"
    workshop_proposal_id: str = ""
    notes: list[str] = field(default_factory=list)


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug(value: str, limit: int = 56) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    if not normalized:
        normalized = f"skill-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    return normalized[:limit]


def ensure_dirs() -> None:
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)


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


def proposal_path(candidate_id: str) -> Path:
    return CANDIDATE_DIR / f"{slug(candidate_id, 96)}.proposal.md"


def log_event(event: dict[str, Any]) -> None:
    ensure_dirs()
    payload = {"time": now(), **event}
    with LEDGER.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
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


def recommendation_for(risk_level: str, existing_matches: list[dict[str, str]]) -> str:
    if risk_level == "high":
        return "manual_review_required"
    if existing_matches:
        return "update_existing_skill_or_review_overlap"
    if risk_level == "medium":
        return "workshop_review_recommended"
    return "ready_for_workshop"


def load_text(value: str | None, path_value: str | None, stdin_label: str) -> str:
    if path_value:
        return Path(path_value).read_text(encoding="utf-8").strip()
    if value:
        return value.strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    raise ValueError(f"Provide --{stdin_label}, --{stdin_label}-file, or stdin content.")


def read_source_files(paths: list[str] | None) -> list[tuple[str, str]]:
    files: list[tuple[str, str]] = []
    for item in paths or []:
        path = Path(item).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        if path.exists() and path.is_file():
            files.append((str(path), path.read_text(encoding="utf-8", errors="replace")[:6000]))
    return files


def find_existing_skill_matches(name: str, description: str, limit: int = 8) -> list[dict[str, str]]:
    if not SKILLS_DIR.exists():
        return []
    needles = {
        part.lower()
        for part in re.split(r"[-_\s]+", f"{name} {description}")
        if len(part) >= 4 and part.lower() not in STOP_TERMS
    }
    matches: list[dict[str, str]] = []
    for path in sorted(SKILLS_DIR.glob("*/SKILL.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        haystack = f"{path.parent.name}\n{text[:1200]}".lower()
        score = sum(1 for needle in needles if needle in haystack)
        if score >= 2:
            matches.append({"skill": path.parent.name, "path": str(path), "match_terms": str(score)})
        if len(matches) >= limit:
            break
    return matches


def build_proposal_content(args: argparse.Namespace, procedure: str, source_files: list[tuple[str, str]]) -> str:
    lines = [
        f"# {args.name}",
        "",
        "## Purpose",
        "",
        args.goal.strip(),
        "",
        "## When To Use",
        "",
        args.when_to_use.strip() if args.when_to_use else "Use when this repeatable workflow appears again and the operator wants a reviewed, reusable procedure.",
        "",
        "## Procedure",
        "",
        procedure.strip(),
        "",
        "## Safety And Review Rules",
        "",
        "- This skill proposal must stay pending until explicitly applied by the user.",
        "- Do not apply, install, or modify live skills automatically.",
        "- Keep secrets, browser sessions, credentials, tokens, and private account material out of skill content.",
        "- If the workflow touches production, trading systems, external sends, or irreversible actions, require manual review before execution.",
        "- Prefer existing local tools, scripts, and established OpenClaw conventions before adding new abstractions.",
    ]
    if args.success_criteria:
        lines.extend(["", "## Success Criteria", "", args.success_criteria.strip()])
    if source_files:
        lines.extend(["", "## Source Evidence", ""])
        for path, text in source_files:
            lines.extend([f"### {path}", "", "```text", text, "```", ""])
    return "\n".join(lines).strip() + "\n"


def create_candidate(args: argparse.Namespace) -> SkillCandidate:
    ensure_dirs()
    procedure = load_text(args.procedure, args.procedure_file, "procedure")
    source_files = read_source_files(args.source_file)
    source_file_paths = [path for path, _ in source_files]
    proposal_content = args.proposal_content or build_proposal_content(args, procedure, source_files)
    source_text = "\n".join(text for _, text in source_files)
    risk_text = "\n".join([args.name, args.description, args.goal, args.evidence or "", procedure, source_text])
    risk_level, risk_reasons = classify_risk(risk_text)
    existing_matches = find_existing_skill_matches(args.name, args.description)
    recommendation = recommendation_for(risk_level, existing_matches)
    candidate_id = args.id or f"skillcand-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug(args.name, 24)}"
    status = "ready_for_workshop" if recommendation == "ready_for_workshop" else "draft"
    candidate = SkillCandidate(
        candidate_id=candidate_id,
        created_at=now(),
        updated_at=now(),
        name=args.name,
        description=args.description,
        goal=args.goal,
        evidence=args.evidence or "",
        proposal_content=proposal_content,
        source_task_id=args.task_id or "",
        source_files=source_file_paths,
        labels=args.label or [],
        risk_level=risk_level,
        risk_reasons=risk_reasons,
        existing_skill_matches=existing_matches,
        recommendation=recommendation,
        status=status,
    )
    atomic_write_text(candidate_path(candidate_id), json.dumps(asdict(candidate), ensure_ascii=False, indent=2))
    atomic_write_text(proposal_path(candidate_id), proposal_content)
    log_event(
        {
            "action": "draft",
            "candidate_id": candidate_id,
            "name": args.name,
            "risk_level": risk_level,
            "recommendation": recommendation,
            "status": status,
        }
    )
    return candidate


def load_candidate(candidate_id_or_path: str) -> SkillCandidate:
    path = Path(candidate_id_or_path)
    if not path.exists():
        path = candidate_path(candidate_id_or_path)
    if not path.exists():
        raise FileNotFoundError(f"Candidate not found: {candidate_id_or_path}")
    return SkillCandidate(**json.loads(path.read_text(encoding="utf-8")))


def save_candidate(candidate: SkillCandidate) -> None:
    candidate.updated_at = now()
    atomic_write_text(candidate_path(candidate.candidate_id), json.dumps(asdict(candidate), ensure_ascii=False, indent=2))
    atomic_write_text(proposal_path(candidate.candidate_id), candidate.proposal_content)


def list_candidates(args: argparse.Namespace) -> list[SkillCandidate]:
    ensure_dirs()
    candidates: list[SkillCandidate] = []
    for path in sorted(CANDIDATE_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            candidate = SkillCandidate(**json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
        if args.status and candidate.status != args.status:
            continue
        if args.label and args.label not in candidate.labels:
            continue
        candidates.append(candidate)
        if len(candidates) >= args.limit:
            break
    return candidates


def workshop_payload(candidate: SkillCandidate) -> dict[str, Any]:
    return {
        "action": "create",
        "name": candidate.name,
        "description": candidate.description,
        "goal": candidate.goal,
        "evidence": candidate.evidence,
        "proposal_content": candidate.proposal_content,
    }


def mark_candidate(args: argparse.Namespace) -> SkillCandidate:
    if args.status not in STATUSES:
        raise ValueError(f"Unsupported status: {args.status}")
    candidate = load_candidate(args.candidate)
    candidate.status = args.status
    if args.proposal_id:
        candidate.workshop_proposal_id = args.proposal_id
    if args.note:
        candidate.notes.append(f"{now()} {args.note}")
    save_candidate(candidate)
    log_event(
        {
            "action": "mark",
            "candidate_id": candidate.candidate_id,
            "status": candidate.status,
            "proposal_id": candidate.workshop_proposal_id,
        }
    )
    return candidate


def print_candidate(candidate: SkillCandidate) -> None:
    print(json.dumps(asdict(candidate), ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenClaw Skill Staging CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    draft = sub.add_parser("draft", help="Create a local skill candidate, not a Skill Workshop proposal")
    draft.add_argument("--name", required=True)
    draft.add_argument("--description", required=True)
    draft.add_argument("--goal", required=True)
    draft.add_argument("--evidence")
    draft.add_argument("--procedure")
    draft.add_argument("--procedure-file")
    draft.add_argument("--proposal-content")
    draft.add_argument("--when-to-use")
    draft.add_argument("--success-criteria")
    draft.add_argument("--task-id")
    draft.add_argument("--id")
    draft.add_argument("-l", "--label", action="append")
    draft.add_argument("-s", "--source-file", action="append")

    show = sub.add_parser("show", help="Show candidate JSON")
    show.add_argument("candidate")

    proposal = sub.add_parser("proposal", help="Print candidate proposal markdown")
    proposal.add_argument("candidate")

    payload = sub.add_parser("workshop-payload", help="Print JSON payload for OpenClaw skill_workshop create")
    payload.add_argument("candidate")

    mark = sub.add_parser("mark", help="Update candidate status after review or Skill Workshop action")
    mark.add_argument("candidate")
    mark.add_argument("status")
    mark.add_argument("--proposal-id")
    mark.add_argument("--note")

    listing = sub.add_parser("list", help="List staged skill candidates")
    listing.add_argument("--status")
    listing.add_argument("--label")
    listing.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    if args.command == "draft":
        print_candidate(create_candidate(args))
    elif args.command == "show":
        print_candidate(load_candidate(args.candidate))
    elif args.command == "proposal":
        print(load_candidate(args.candidate).proposal_content, end="")
    elif args.command == "workshop-payload":
        print(json.dumps(workshop_payload(load_candidate(args.candidate)), ensure_ascii=False, indent=2))
    elif args.command == "mark":
        print_candidate(mark_candidate(args))
    elif args.command == "list":
        for candidate in list_candidates(args):
            print(f"{candidate.updated_at}\t{candidate.status}\t{candidate.candidate_id}\t{candidate.name}\t{candidate.recommendation}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"skill_staging: {exc}", file=sys.stderr)
        raise SystemExit(1)
