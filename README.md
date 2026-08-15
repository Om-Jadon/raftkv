# raftkv

A distributed key-value store powered by a from-scratch [Raft](https://raft.github.io/) consensus implementation in Go.

**Status:** Phase 0 scaffolding. See [PLAN.md](PLAN.md) for the full build roadmap.

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
```

## Roadmap

Phases 0–9 cover leader election, log replication, persistence, a KV API, gRPC transport, deterministic chaos testing, snapshots, and Docker/CI. Details and checklists live in `PLAN.md`.
