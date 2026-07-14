# Contributing

1. Keep core behavior deterministic and integrations optional.
2. Add tests for idempotency, retry, recovery and policy boundaries.
3. Run Python and Node tests.
4. Run `scripts/preflight.sh`.
5. Document compatibility and rollback effects in the pull request.

Never attach real OpenClaw state, nmem exports, transcripts, databases, logs, configuration or credentials.
