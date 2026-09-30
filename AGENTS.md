# AGENTS.md — apex-raft-forge

**Company:** GlacierEQ
**Domain:** Autonomous Systems Engineering

## Quick Rules
- **Test command:** `PYTHONPATH=src pytest tests/ -v`
- **Lint:** `ruff check src/ tests/`
- **No drive-by edits** — load the skill first.

## Architecture
- `src/apex_raft_forge/core.py` — Domain logic (Autonomous Systems Engineering)
- `tests/` — Verified test suite
- `.github/workflows/ci.yml` — Enforced CI pipeline
