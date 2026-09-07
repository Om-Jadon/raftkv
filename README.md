# raftkv

A linearizable distributed key-value store on a from-scratch
[Raft](https://raft.github.io/) implementation in Go. No `hashicorp/raft`, no
`go.etcd.io/raft`.

The interesting part is not the HTTP API. It is the safety properties, the tests
that try to break them, and the two guards whose removal the suite actually
catches.

## What it does

Five nodes agree on a log. A KV state machine applies that log. Clients talk
HTTP/JSON (`PUT/GET/DELETE /kv/{key}`, `POST` to append). Writes go through the
leader; followers return `503` plus `X-Raft-Leader`. Reads also go through the
log (no ReadIndex), so they are linearizable by construction. Duplicate client
requests are ignored via a session table that is snapshotted with the data.

```bash
make test          # unit + harness tests
make race          # same, with the race detector
make chaos         # 200 seeded fault-injection runs + 2-minute race soak
./scripts/demo.sh  # 5-node Docker Compose: write, kill leader, restart, data stays
```

HTTP ports in Compose are `8081`–`8085`. Example:

```bash
go run ./cmd/raftkvctl -addrs http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085 put hello world
```

## Safety properties and how they are tested

| Property | How it is checked |
|---|---|
| Election safety (at most one leader per term) | Cluster harness + chaos checker, including across crash/restart |
| Leader append-only | Chaos checker: a leader’s last index does not shrink in a term; overlapping entries match |
| Log matching | Pairwise prefix check on live logs (snapshot sentinels compared on term only) |
| Leader completeness | A leader of term T contains entries committed in terms &lt; T |
| State-machine safety | Applied commands at each index agree across nodes |
| Durability | After heal, every live node has (or has compacted past) every committed entry |
| Linearizable client observations | Sequential reference model on acknowledged puts/gets during chaos |

The chaos harness injects drops, delays, reordering/duplication, partitions,
asymmetric blocks, and crash-restarts. Compaction is on. 200 seeds passed;
a 2-minute race soak passed.

## Broken-Raft proof

The suite is not just “tests are green.” Two correctness guards can be disabled
at runtime (`raft.SkipFigure8`, `raft.SkipVotePersist`):

- **Figure 8** — commit by replica count even when `log[N].term != currentTerm`.
  The unit test fails immediately: an old-term majority becomes committable.
  In a cluster that would surface as leader completeness / durability.
- **Vote persistence** — persist the term but store `votedFor = nil`. After a
  crash/reload the node grants a second candidate in the same term. The unit
  test fails on the second RequestVote.

Both are the kind of bug that looks fine in a happy-path demo.

## Persistence and snapshots

Hard state and the log are files with tmp+fsync+rename / CRC records. A crash
between fsync and rename keeps the previous file. Snapshots are written the
same way **before** the log prefix is dropped. The last-included entry remains
at `log[0]` so AppendEntries `prevLogIndex` checks still hit a real slot.
`InstallSnapshot` is un-chunked with a 16MiB cap (`Done` is always true).

## Not implemented (and how I would)

These are deferred on purpose, not unfinished:

- **Membership changes.** Joint consensus (Ongaro ch. 4): a C-old,new
  configuration is committed before C-new. Overlapping majorities are what
  stop a split brain during add/remove. Cold-start 5-node Compose is enough
  for the interview; live add/remove is a different project.
- **ReadIndex / lease reads.** Bypass the log for reads after confirming the
  leader still has a majority (or a time-bounded lease). I kept reads on the
  log so linearizability does not depend on a clock.
- **Pre-vote.** An extra round that does not increment `currentTerm`, to avoid
  disruption from a partitioned node. Vanilla Raft recovers; pre-vote would
  cut tail latency after partitions.

Also out of scope: pipelined AppendEntries, chunked snapshots, metrics
dashboards.

## Layout

```
cmd/raftkvd          server (gRPC peers + HTTP)
cmd/raftkvctl        retrying client
internal/raft        consensus
internal/kv          state machine + HTTP
internal/storage     file + fsync WAL
internal/transport   memory (tests) and gRPC (deployment)
test/harness         in-process cluster
test/chaos           seeded fault injection
deploy/              Dockerfile + Compose
```
