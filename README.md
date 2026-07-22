# Clawwarden

A durable, policy-gated control plane for unattended OpenClaw workflows.

Clawwarden turns short OpenClaw lifecycle callbacks into persistent events processed by a separate worker. It provides idempotency, leases, bounded retry, dead letters, recovery lineage, review queues, a kill switch and independent liveness monitoring.

## Architecture

```text
OpenClaw lifecycle hooks
        ↓
Clawwarden Runtime plugin
        ↓
SQLite WAL event inbox
        ↓
Worker: lease → execute → retry/dead-letter
        ↓
Optional nmem, workflow, skill and artifact integrations
```

## Secure defaults

- Automatic low-risk memory commits are disabled.
- Automatic failed-run resumption is disabled until explicitly enabled.
- Gateway restart is disabled.
- OpenClaw configuration backup is disabled.
- nmem export is disabled.
- Destructive, trading, credential, publication and live-skill actions remain policy-gated.
- Optional quota and artifact helpers must be explicitly configured.
- Raw prompts and assistant summaries are represented by hashes only unless local capture is explicitly enabled.
- The SQLite state database is created with owner-only permissions.
- State, databases, logs, archives, credentials and real OpenClaw configuration are blocked from Git.

## Included components

- `scripts/control_plane.py`: SQLite WAL inbox, leases, retry, dead letters and recovery
- `scripts/clawwarden.py`: decision, ledger, review, governance and lifecycle CLI
- `scripts/worker_watchdog.py`: independent worker liveness watchdog
- `scripts/workflow_ledger.py`: durable task/checkpoint ledger
- `scripts/nmem_adapter.py`: optional nmem integration
- `scripts/skill_staging.py`: candidate-only reusable workflow staging
- `plugin/openclaw`: lifecycle bridge plugin

## Requirements

- Python 3.10+
- Node.js 20+ for the OpenClaw plugin
- OpenClaw with the lifecycle hook API used by the runtime plugin
- macOS or Linux with `fcntl`
- Optional: nmem

The current plugin compatibility floor is OpenClaw `2026.7.1-beta.2`; this tree was tested with the stable OpenClaw `2026.7.1` release.

## Quick start

```bash
cp config.example.json config.json
chmod 600 config.json
python3 scripts/control_plane.py init
python3 scripts/control_plane.py status
node --test plugin/openclaw/tests/*.test.mjs
```

Copy the OpenClaw plugin configuration example and set `scriptPath` to this repository's `scripts/control_plane.py`. Keep production configuration outside Git.

## Optional integrations

- Set `CLAWWARDEN_QUOTA_HELPER` to an executable JSON-producing quota helper.
- Set `CLAWWARDEN_ARTIFACT_HELPER` to an artifact lifecycle helper.
- Install nmem to enable semantic context and memory candidate workflows.

Missing optional integrations degrade to `disabled`; they do not silently trigger a fallback model or external service.

When a periodic event recovers, Clawwarden resolves an older dead-letter alert only when the successful event has the same type and a newer creation timestamp. Unrelated alerts remain active.

## Commit and push gate

```bash
git init -b main
./scripts/install_git_hooks.sh
./scripts/preflight.sh
```

The gate requires gitleaks and fails closed. Scanner output contains file, line and rule only, never the suspected value.

## Documentation

- [Chinese README](README.zh-CN.md)
- [Security policy](SECURITY.md)
- [Release checklist](RELEASE_CHECKLIST.md)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0.
