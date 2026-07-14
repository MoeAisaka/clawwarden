# Release checklist

- [ ] Confirm the repository came from the clean staging tree, not the production workspace.
- [ ] Confirm no original backup, memory, log, database, session or configuration directory was copied.
- [ ] Confirm public Git author name/email.
- [ ] Run `python3 -m unittest discover -s tests -v`.
- [ ] Run `node --test plugin/openclaw/tests/*.test.mjs`.
- [ ] Run `python3 scripts/secret_scan.py --tracked`.
- [ ] Run `gitleaks dir --redact --no-banner .`.
- [ ] Run `gitleaks git --redact --no-banner .` after the first commit.
- [ ] Inspect `git ls-files` manually.
- [ ] Validate plugin installation in a disposable OpenClaw environment.
- [ ] Validate kill switch, retry, dead letter and recovery behavior.
- [ ] Confirm optional integrations remain disabled unless explicitly configured.
- [ ] Create a signed release tag only after CI and secret scanning pass.
