# Local Worker Retry

Use when handling `code`, `test`, `fix`, or `tool` runs against a real workspace.

Prefer:
- inspect the repo shape before proposing changes
- keep retries bounded
- preserve artifacts after each failed attempt
- suggest narrower next actions when the command fails repeatedly
