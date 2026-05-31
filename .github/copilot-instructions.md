# Copilot Instructions

> **See also:** [`AGENTS.md`](../AGENTS.md) for remaining playbooks and operational guidelines.

---

## Project Overview

**omashu** (`github.com/avatar31/omashu`) is a Go library that provides a distributed, transactional key-value store. It wraps [BadgerDB](https://github.com/dgraph-io/badger) with a Raft consensus layer (via `go.etcd.io/raft/v3`) and a Timestamp Oracle (TSO) to deliver ACID transactions with snapshot isolation across a multi-node cluster.

- **Language:** Go 1.26+
- **License:** Apache 2.0
- **Author:** Sachin S
- **Module path:** `github.com/avatar31/omashu`

---

## Two Modes of Operation

| Type | Constructor | Use case |
|---|---|---|
| `*Badger` | `NewBadger(ctx, cfg)` | Single-node embedded store (dev, testing, single-instance) |
| `*DistributedBadger` | `NewDistributedBadger(ctx, cfg)` | Replicated multi-node cluster with Raft consensus |

Both implement the same `Database[T]` generic interface, so application code is portable between modes.

---

## Key Dependencies

| Package | Purpose |
|---|---|
| `github.com/dgraph-io/badger/v4` | MVCC embedded key-value storage engine (opened in **managed** mode for MVCC timestamp control) |
| `go.etcd.io/raft/v3` | Raft consensus algorithm (etcd's battle-tested implementation) |
| `go.etcd.io/etcd/server/v3` | `rafthttp` transport, WAL, snapshotter |
| `go.uber.org/zap` | Structured logging throughout |
| `google.golang.org/protobuf` | Raft command serialisation and dynamic schema support |
| `github.com/google/uuid` | Transaction IDs |
| `github.com/dgraph-io/ristretto/v2` | Used internally by BadgerDB |

---

## Architecture

```
Your Application
      │  Database[T] interface
      ├──────────────────────────────────┐
      ▼                                  ▼
  Badger                        DistributedBadger
(single node)                  (replicated cluster)
      │                          │              │
      │                        TSO          Raft Node
      │                     (HLC tss)   (leader election,
      │                        │          log replication)
      │                   TxnManager           │
      │                        │           Storage
      │                        │        (WAL + Snapshotter)
      └────────────────────────▼
                    FSM  ←──── Apply committed entries
                 (BadgerDB MVCC)
```

### Component Responsibilities

| File | Type | Responsibility |
|---|---|---|
| `store.go` | Interfaces + constructors | `Database[T]`, `DBReadOps`, `DBWriteOps`, `DBDeleteOps` interfaces; `NewBadger`, `NewDistributedBadger` |
| `badger.go` | `*Badger` | Single-node read/write/delete implementation on top of BadgerDB |
| `distributed.go` | `*DistributedBadger` | Distributed implementation: routes reads through ReadIndex, routes writes through Raft proposals |
| `node.go` | `*Node` | Raft node lifecycle: ready-handler loop, propose-handler loop, leader change detection, membership changes |
| `fsm.go` | `*FSM` | Finite state machine: decodes and applies committed Raft log entries to BadgerDB |
| `tso.go` | `*TSO` | Timestamp Oracle: hybrid logical clock (HLC), in-flight read tracking, conflict detection, GC watermarks |
| `txn.go` | `*Txn`, `*TxnManager` | Transaction lifecycle: `BeginTxn` (assigns `readTs`), write buffering, `Commit` (assigns `commitTs`, conflict check, proposes to Raft), `Discard` |
| `transport.go` | `*Transport` | HTTP peer transport using `etcd/rafthttp`; serves and dials peer nodes |
| `replication.go` | `*Replicate`, `Replicator` | Full-database snapshot via BadgerDB `Backup`/`Load` for Raft snapshot transfer; the exported `Replicator` interface defines `TakeSnapshot`/`Restore` so the FSM can swap implementations |
| `storage.go` | `*Storage` | Bridges `raft.MemoryStorage`, WAL, and Snapshotter; `SaveState`, `Initialize` |
| `wal.go` | `*Wal` | Wraps etcd WAL and Snapshotter; handles open/replay/save/snapshot |
| `config.go` | `Config`, `RaftConfig`, `SchemaConfig` | Config struct, `Cluster` interface, validation logic |
| `errors.go` | sentinel errors | All exported error values |
| `logger.go` | adapters | Bridges `*zap.Logger` to raft and BadgerDB logger interfaces |
| `types/` | protobuf + registry | `Command` protobuf for Raft entries; dynamic descriptor registry for Protobuf schema validation |
| `utils/` | helpers | `Uint64ToBytes` / `BytesToUint64`; `CreateDirIfNotExists` |

---

## Database Interface

```go
type Database[T OTxn] interface {
    // Reads
    Get(ctx, key)                          ([]byte, bool, error)
    GetWithTxn(ctx, txn T, key)            ([]byte, bool, error)
    BulkGet(ctx, keys)                     (map[string][]byte, error)
    GetByPrefix(ctx, prefix)               (map[string][]byte, error)
    GetByPrefixWithTxn(ctx, txn T, prefix) (map[string][]byte, error)
    GetKeysByPrefix(ctx, prefix)           ([]string, error)
    GetKeysByPrefixWithTxn(ctx, txn T, prefix) ([]string, error)
    IterateByPrefix(ctx, prefix, cursor, limit, func(k,v []byte) bool) (string, error)
    Count(ctx, prefix)                     int
    Exists(ctx, key)                       bool
    HasChild(ctx, prefix)                  bool

    // Writes
    Set(ctx, key, value, ttl?)             error
    SetWithTxn(ctx, txn T, key, value, ttl?) error
    IncrBy(ctx, key, delta)                error
    DecrBy(ctx, key, delta)                error
    UpdateJson(ctx, key, delta map[string]any, ttl?) error
    UpdateProtobuf(ctx, key, delta proto.Message, ttl?) error

    // Deletes
    Delete(ctx, key)                       error
    DeleteByPrefix(ctx, prefix)            error

    // Transactions (DistributedBadger only — uses *Txn)
    NewTransaction(ctx, func(ctx, txn) error) error

    // Lifecycle
    GetBadger()                            *badger.DB
    Close(ctx)
}
```

`T` is constrained to `*Txn | *badger.Txn`. `Badger` uses `*badger.Txn`; `DistributedBadger` uses `*Txn`.

---

## Transaction Model

Transactions use **Snapshot Isolation** enforced by the TSO:

1. `TxnManager.BeginTxn(ctx, update)` → TSO issues a `readTs`
2. Reads use `readTs` as a BadgerDB MVCC snapshot — they never block writers
3. Writes are buffered in the `Txn` struct (not visible to BadgerDB yet)
4. `Txn.Commit()`:
   - Acquires `writeChLock`
   - Calls `TSO.Commit(txn, ...)` → assigns `commitTs`, checks for conflicts (any write key modified by a concurrent txn that committed after `readTs` → abort)
   - Serialises the command with `commitTs` and proposes to Raft
5. FSM applies the entry to BadgerDB at `commitTs` on all nodes

**Important:** Only the Raft **leader** accepts write transactions. Non-leader writes return `ErrNotLeader`.

### Internal Proposal Pipeline (DistributedBadger)

Single write operation path:

```
proposeTxnSubCommand → BeginTxn → performOps → Commit → proposeAndWait
```

1. `proposeTxnSubCommand` opens a `Txn`, runs the write op, calls `Txn.Commit` to assign `commitTs`, then passes the encoded command to `proposeAndWait`
2. `proposeAndWait` stores an `errCh` in `proposals` (`sync.Map`) keyed by `cmdID`, sends `propose{cmdID, cmd}` to `node.ProposeReqNotifier()`, and blocks up to `DefaultProposeTimeout`
3. `listenProposeResponses` goroutine (one per leadership term, started in `onLeaderChange`) reads from `node.ProposeRespNotifier()` and routes each result to the matching `errCh`
4. On leader loss, `leaderChangeNotifier` channel is closed, stopping `listenProposeResponses` and draining any pending proposals with `ErrNotLeader`

---

## Timestamp Oracle (TSO) — Hybrid Logical Clock

```
64-bit timestamp layout:
  63                   18 17        0
  +---------------------+-----------+
  | physical time (ms)  |  logical  |
  +---------------------+-----------+
  ts = (physical_ms << 18) | logical
```

- `LOGICAL_BITS = 18` → up to 262,143 timestamps per millisecond
- Upper-bound saved to BadgerDB key `_tso_last_timestamp` every `defaultUpperBoundWindow` (2 s) — ensures a new leader can safely advance past the previous leader's max ts without regression
- TSO only starts serving when this node **becomes the leader** (`StartServing` called in `onLeaderChange`)
- TSO stops on leader loss (`tso.Close()` called in `onLeaderChange`)

---

## Linearizable Reads

All reads on `DistributedBadger` go through `waitForReadState`:
1. `node.ReadIndex(ctx, key)` — sends a `MsgReadIndex` to Raft; returns a confirmed index
2. Waits (up to 5 s) for `appliedIndex >= confirmedIndex` via `readStatesNotifier` channel
3. Then serves data from BadgerDB — guarantees no stale reads post-leader-election

---

## Raft Command Types (FSM)

Commands serialised into Raft log entries (`types.Command` protobuf):

| `CommandType` | Applied by FSM as |
|---|---|
| `SET` | `badger.Txn.Set` at `commitTs` |
| `UPDATE` | JSON merge or Protobuf field-level merge at `commitTs` |
| `DELETE` | `badger.Txn.Delete` at `commitTs` |
| `DELETE_BY_PREFIX` | Iterates prefix, deletes all at `commitTs` |
| `INCR_BY` | Read-modify-write counter increment at `commitTs` |
| `DECR_BY` | Read-modify-write counter decrement at `commitTs` |
| `TRANSACTION` | Batch of sub-commands applied atomically at `commitTs` |

---

## Config

```go
type Config struct {
    Name           string            // node name used in logs (default: "omashu")
    BaseDir        string            // root dir for WAL, snapshots, db files — REQUIRED
    Logger         *zap.Logger       // nil = silent
    GCInterval     time.Duration
    GCDiscardRatio float64
    BadgerOptions  badger.Options    // pass-through to BadgerDB
    RaftConfig     *RaftConfig       // REQUIRED for distributed mode
    Cluster        Cluster           // REQUIRED — implements GetID, GetName, IsNodeRemoved
    SchemaConfig   *SchemaConfig     // REQUIRED — SchemaTypeJson or SchemaTypeProtobuf
    OnLeaderChange func(prev, next uint64)
    OnRemovedSelf  func()
}
```

Sub-directories created under `BaseDir`:
- `db/` — BadgerDB data files
- `wal/` — Raft WAL
- `snap/` — Raft snapshots

```go
type Cluster interface {
    GetID() uint64
    GetName() string
    IsNodeRemoved(id uint64) bool
}
```

---

## Error Reference

| Sentinel | Trigger |
|---|---|
| `ErrNotLeader` | Write on non-leader node |
| `ErrProposeTimeout` | Raft didn't commit within `DefaultProposeTimeout` (5 s) |
| `ErrBatchTooBig` | Txn write set exceeds `MaxBatchSize` |
| `ErrMissingRaftConf` | `RaftConfig` absent for distributed mode |
| `ErrMissingCluster` | `Cluster` not set |
| `ErrMissingBaseDir` | `BaseDir` not set |
| `ErrMissingSchemaConfig` | `SchemaConfig` not set |
| `ErrUnknownProtoMsg` | `UpdateProtobuf` with unregistered message type |
| `ErrUnknownOp` | FSM received unknown command type |

---

## Key Constants

| Constant | Value | Description |
|---|---|---|
| `MaxBatchSize` | `100` | Max sub-commands per `Txn.Commit`; checked in `Txn.Commit` before proposing to Raft |
| `DefaultProposeTimeout` | `5 * time.Second` | Raft proposal deadline in `proposeAndWait`; returns `ErrProposeTimeout` on expiry |
| `RetryCount` | `5` | Max retry attempts for recoverable node operations |
| `ReadStatesTimeout` | `1 * time.Second` | Per-ReadIndex wait deadline; `waitForReadState` in `distributed.go` passes `5s` |
| `defaultSnapshotCount` | `100000` | Log entries between automatic snapshots (`takeSnapshotIfNeeded` in `node.go`) |
| `LOGICAL_BITS` | `18` | Bits for logical counter in HLC timestamp |
| `MAX_LOGICAL` | `262143` | Max logical ticks per millisecond (`1<<18 - 1`) |
| `defaultUpperBoundWindow` | `2 * time.Second` | How often TSO persists its upper-bound timestamp to BadgerDB key `_tso_last_timestamp` |
| `maxRestorePendingTxns` | `256` | BadgerDB snapshot restore pipeline depth (`replication.go`) |
| `defaultReqTimeout` | `5 * time.Second` | HTTP server request timeout in `transport.go` |

---

## Build & Test Commands

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Benchmarks
go test -bench=. ./...

# Escape analysis (check allocations)
go build -gcflags="-m" ./...

# Vet
go vet ./...
```

Test fixtures live in `assets/fixtures.json`. Tests use an in-memory BadgerDB instance (`WithInMemory(true)`).

---

## Key Design Decisions (Rationale)

1. **BadgerDB in managed mode** — required so the TSO can control MVCC commit timestamps via `CommitAt(ts)` and `NewTransactionAt(ts)`. Normal Badger mode auto-assigns timestamps internally.

2. **TSO only active on leader** — timestamps must be monotonically increasing across the cluster. Running TSO on followers would risk timestamp regression after a leader change. The leader persists the upper-bound to BadgerDB so the next leader starts safely above it.

3. **Snapshot via Badger Backup/Load, `since=0`** — a full Raft snapshot must capture all key versions from the beginning so a lagging follower can reconstruct identical MVCC state. Passing `since=currentTs` would produce an empty snapshot.

4. **`writeChLock` in TSO** — ensures that the order in which `commitTs` values are issued matches the order in which commands are pushed to the Raft proposal channel. Prevents out-of-order MVCC entries.

5. **Linearizable reads via ReadIndex, not log reads** — avoids proposing a no-op to Raft just to confirm leadership. ReadIndex is cheaper and the standard etcd pattern.

6. **`rafthttp` transport** — reuses etcd's production-proven HTTP/2 transport rather than rolling a custom one. Snapshots are streamed separately via the Snapshotter.

---

## Active TODOs (as of May 2026)

**P0 (blocking correctness):**
- Linearizable reads end-to-end verification
- TSO and TxnManager re-initialization on leadership gain (currently done but needs testing)
- `transport.SendSnapshot` handling and retry logic
- TLS support for peer transport

**P1 (quality/production readiness):**
- Single shared channel for all proposals (currently one channel per proposal cycle)
- Metrics collection for Raft ops and DB ops
- Graceful shutdown and cleanup
- Dynamic cluster membership (add/remove nodes at runtime)
- Transaction retry on conflict

**P2 (future features):**
- Follower reads (non-linearizable, stale-read option)
- Sharding / partitioning
- Change data capture / streaming
- CLI for cluster management
- Web dashboard for monitoring

---

## Schema Support

**JSON schema** (default):
- `UpdateJson` merges a `map[string]any` delta into the stored JSON object
- No descriptor setup required

**Protobuf schema**:
- Requires `SchemaConfig.ProtoSchemaList` (`[]*descriptorpb.FileDescriptorSet`)
- `UpdateProtobuf` validates message type against the registered descriptor store before accepting
- Dynamic descriptor registry lives in `types/dynamic.go` as a singleton `ProtoDescriptorStore`

---

## Documentation

- **README** — [README.md](../README.md): full API reference, usage examples, architecture diagram, config reference, error table
- **Inline API docs** — godoc comments on every exported (and most unexported) symbol across all source files; visible on [pkg.go.dev/github.com/avatar31/omashu](https://pkg.go.dev/github.com/avatar31/omashu) after publishing
- **AI assistant reference** — `.github/copilot-instructions.md` (this file): complete architecture, design rationale, constants table, active TODOs
- **AI operational guidelines** — `AGENTS.md`: coding conventions, common workflows, file sensitivity tiers, known pitfalls
- **Notes/Design** — `notes.md`: reference links for Raft design; TSO architecture discussion
- **Roadmap** — `roadmap.md`: feature planning
- **Review notes** — `review.md`, `tso_notes.md`, `snapshot.md`, `transport_err_fix.md`, `ut.md`, `testing.md`: engineering decision logs and review threads
