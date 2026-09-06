# raftkv

A distributed key-value store powered by a from-scratch [Raft](https://raft.github.io/) consensus implementation in Go.

**Status:** Phase 7 — chaos harness and safety invariants.

## Module

```text
github.com/Om-Jadon/raftkv
```

## Quick commands

```bash
make test   # unit tests
make race   # tests with the race detector
make vet    # go vet
make lint   # golangci-lint (skipped with a note if not installed)
make chaos  # 200 chaos seeds + 2-minute race soak (SEEDS=n to override)
```

## Roadmap

Phases 0–9 cover leader election, log replication, persistence, a KV API, gRPC transport, deterministic chaos testing, snapshots, and Docker/CI. Details and checklists live in `PLAN.md`.
