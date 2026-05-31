# AGENTS.md — Operational Guidelines for AI Assistants

> **See also:** [`.github/copilot-instructions.md`](.github/copilot-instructions.md) for the complete project reference: architecture, API surface, design decisions, config, errors, and active TODOs.

Read `.github/copilot-instructions.md` **first** before making any change. It is the authoritative source of truth for this project.

---

## Before You Start Any Task

1. Read `.github/copilot-instructions.md` — know the architecture before touching code.
2. Run `go vet ./...` to get a baseline — understand what is already failing.
3. Run `go test ./...` to see the current test state.
4. Identify which **layer** your change touches (TSO, Raft Node, FSM, Transport, Txn, Config). Changes rarely span all layers; resist the urge to refactor beyond the scope requested.

---
