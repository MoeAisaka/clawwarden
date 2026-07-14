# Security Policy

## Scope

Clawwarden processes prompts, summaries, session identifiers and task metadata. Treat its state directory and backups as sensitive even when no provider key is present.

## Reporting

Use GitHub private vulnerability reporting after publication. Never paste credentials, transcripts, runtime databases or production configuration into a public issue.

## Credential and state policy

- Provider credentials are not required by the core.
- Integrations must receive credentials through their own secret store or environment, never a tracked file.
- Real `openclaw.json`, nmem exports, SQLite databases, JSONL events, logs and archives must remain outside Git.
- Backup of OpenClaw configuration and nmem is opt-in because those artifacts can contain private material.
- Raw prompt and assistant-summary capture is opt-in. Hashes are stored by default for correlation without retaining content.
- Local state is created with owner-only permissions; keep its parent directory private as well.

If any credential reaches Git, revoke it immediately and rebuild or fully rewrite public history before publishing again.

Run `scripts/preflight.sh` before every commit and push. Local scanning and CI provide defense in depth, not a guarantee that an exposed key remains safe.
