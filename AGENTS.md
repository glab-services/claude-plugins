# Working agreement

- Use Node.js 24+ and pnpm. Read broker/README.md and channel/README.md before
  changing messaging behavior. Keep changes scoped and stop for review after
  each self-contained change.
- Messaging tools must not execute commands on another developer's machine or
  expose human-only resolve/reopen operations to agents.
- Use the existing Conventional Commit style: `feat(scope): ...`,
  `fix(scope): ...`, `test(scope): ...`, or `docs(scope): ...`. Keep review
  findings in separate commits with relevant regression coverage.
- Run the gates for the area changed: `pnpm broker:test` and
  `pnpm broker:smoke` for broker changes; `pnpm channel:test` and
  `pnpm channel:smoke` for channel changes. Run both sets when shared messaging
  behavior or workspace dependencies change. Check `git diff --check` too.
- Reproduce functional bugs before fixing them. Use real HTTP/stdio integration
  tests for delivery and lifecycle behavior. Documentation-only edits do not
  need new tests. Distinguish SDK smoke results from real Claude Code acceptance
  and record the exact scenario covered.
- Put test databases, tokens, configs, and diagnostic artifacts under `$TMPDIR`
  (never `/tmp`) or ignored `.tmp/`. Use unique names, stop spawned services,
  and remove what you create. Never commit real credentials or token files.
- Never print secrets: no tokens, `tokens.json` contents, `.env` values, or
  credentials in logs, error messages, test output, commit messages, or PR
  comments. Test a configuration value instead of echoing it.
