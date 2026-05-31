# omashu

> A distributed, transactional key-value store for Go — built on BadgerDB and etcd/raft.

[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Module](https://img.shields.io/badge/module-github.com%2Favatar31%2Fomashu-informational)](https://pkg.go.dev/github.com/avatar31/omashu)

## Why This Exists

Most embedded databases are either fast-but-single-node or distributed-but-complex. omashu bridges that gap: it wraps [BadgerDB](https://github.com/dgraph-io/badger) with a full Raft consensus layer and a Timestamp Oracle (TSO) to give you a production-grade, multi-node key-value store that supports concurrent, long-running ACID transactions with snapshot isolation — all behind a single, ergonomic Go API.

You can start with a single-node `Badger` store and promote to a replicated `DistributedBadger` cluster without changing your application code.

---

## Quick Start

```bash
go get github.com/avatar31/omashu
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/avatar31/omashu"
    "go.uber.org/zap"
)

type myCluster struct{}

func (c *myCluster) GetID() uint64            { return 1 }
func (c *myCluster) GetName() string          { return "local" }
func (c *myCluster) IsNodeRemoved(id uint64) bool { return false }

func main() {
    ctx := context.Background()
    logger, _ := zap.NewDevelopment()

    db, err := omashu.NewBadger(ctx, &omashu.Config{
        BaseDir:      "/tmp/omashu-data",
        Cluster:      &myCluster{},
        Logger:       logger,
        SchemaConfig: &omashu.SchemaConfig{Type: omashu.SchemaTypeJson},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close(ctx)

    // Write
    if err := db.Set(ctx, "users:1", []byte(`{"name":"Alice"}`)); err != nil {
        log.Fatal(err)
    }

    // Read
    val, found, err := db.Get(ctx, "users:1")
    if err != nil {
        log.Fatal(err)
    }
    if found {
        fmt.Println(string(val)) // {"name":"Alice"}
    }
}
```

---

## Installation

**Prerequisites:** Go 1.26+

```bash
go get github.com/avatar31/omashu
```

---

## Usage

### Single-Node Store

`NewBadger` creates a local, non-replicated store backed by BadgerDB. Suitable for single-instance deployments or development.

```go
db, err := omashu.NewBadger(ctx, &omashu.Config{
    BaseDir:      "/var/data/omashu",
    Cluster:      cluster,
    Logger:       logger,
    SchemaConfig: &omashu.SchemaConfig{Type: omashu.SchemaTypeJson},
})
```

### Distributed (Replicated) Store

`NewDistributedBadger` starts a Raft node, joins the cluster, and enables linearizable reads and replicated writes. Every write is proposed to Raft and applied to BadgerDB only after a quorum commits it.

```go
import (
    "go.etcd.io/raft/v3"
    "github.com/avatar31/omashu"
)

db, err := omashu.NewDistributedBadger(ctx, &omashu.Config{
    Name:    "node-1",
    BaseDir: "/var/data/omashu",
    Cluster: cluster,
    Logger:  logger,
    SchemaConfig: &omashu.SchemaConfig{Type: omashu.SchemaTypeJson},
    RaftConfig: &omashu.RaftConfig{
        Config: raft.Config{
            ID:              1,
            ElectionTick:    10,
            HeartbeatTick:   1,
            MaxSizePerMsg:   4096,
            MaxInflightMsgs: 256,
            CheckQuorum:     true,
            PreVote:         true,
        },
        Nodename: "node-1",
        Peers: map[uint64]string{
            1: "http://node-1:7070",
            2: "http://node-2:7070",
            3: "http://node-3:7070",
        },
    },
    OnLeaderChange: func(prev, next uint64) {
        fmt.Printf("leader changed: %d → %d\n", prev, next)
    },
})
```

### Read Operations

```go
// Single key
val, found, err := db.Get(ctx, "users:1")

// Multiple keys at once
results, err := db.BulkGet(ctx, []string{"users:1", "users:2", "users:3"})

// All keys and values under a prefix
items, err := db.GetByPrefix(ctx, "users:")

// Just the keys (not values)
keys, err := db.GetKeysByPrefix(ctx, "users:")

// Paginated prefix scan
nextCursor, err := db.IterateByPrefix(ctx, "users:", "", &limit, func(k, v []byte) bool {
    // process each entry; return false to stop early
    return true
})

// Existence checks
exists  := db.Exists(ctx, "users:1")
hasKids := db.HasChild(ctx, "users:")
count   := db.Count(ctx, "users:")
```

### Write Operations

```go
// Set a value (with optional TTL)
err = db.Set(ctx, "sessions:abc", token, 24*time.Hour)

// Atomic counters
err = db.IncrBy(ctx, "stats:pageviews", 1)
err = db.DecrBy(ctx, "inventory:item-42", 5)

// Partial JSON update — merges the delta into the stored JSON object
err = db.UpdateJson(ctx, "users:1", map[string]any{"email": "alice@example.com"})

// Partial Protobuf update
err = db.UpdateProtobuf(ctx, "users:1", &pb.UserPatch{Email: "alice@example.com"})

// Delete
err = db.Delete(ctx, "sessions:abc")
err = db.DeleteByPrefix(ctx, "sessions:")
```

### Transactions

Use `NewTransaction` for atomic, multi-step operations with snapshot isolation. The transaction commits automatically when `performOps` returns `nil`, and discards on any error.

```go
err = db.NewTransaction(ctx, func(ctx context.Context, txn *omashu.Txn) error {
    // All reads in this transaction see a consistent snapshot
    val, found, err := db.GetWithTxn(ctx, txn, "accounts:alice:balance")
    if err != nil || !found {
        return err
    }

    balance := utils.BytesToUint64(val)
    if balance < 100 {
        return errors.New("insufficient funds")
    }

    if err := txn.DecrBy(ctx, "accounts:alice:balance", 100); err != nil {
        return err
    }
    return txn.IncrBy(ctx, "accounts:bob:balance", 100)
})
```

> **Note:** On `DistributedBadger`, only the **leader node** accepts write transactions. A write on a non-leader returns `omashu.ErrNotLeader`.

---

## Configuration Reference

### Config

| Field | Type | Required | Description |
|---|---|---|---|
| `Name` | `string` | No | Node name used in logs (default: `"omashu"`) |
| `BaseDir` | `string` | Yes | Root directory for WAL, snapshots, and database files |
| `Cluster` | `Cluster` | Yes | Interface providing cluster ID/name and removed-node detection |
| `Logger` | `*zap.Logger` | No | Structured logger; `nil` suppresses all logs |
| `SchemaConfig` | `*SchemaConfig` | Yes | `SchemaTypeJson` or `SchemaTypeProtobuf` |
| `BadgerOptions` | `badger.Options` | No | Passed directly to BadgerDB (e.g. `WithInMemory(true)`) |
| `GCInterval` | `time.Duration` | No | How often BadgerDB runs value-log GC |
| `GCDiscardRatio` | `float64` | No | Minimum discard ratio for GC (passed to BadgerDB) |
| `RaftConfig` | `*RaftConfig` | Distributed only | Raft node ID, tuning, and peer addresses |
| `OnLeaderChange` | `func(prev, next uint64)` | No | Callback invoked when the Raft leader changes |
| `OnRemovedSelf` | `func()` | No | Callback invoked when this node is removed from the cluster |

### SchemaConfig

```go
// JSON schema — no descriptor needed
SchemaConfig: &omashu.SchemaConfig{
    Type: omashu.SchemaTypeJson,
}

// Protobuf schema — descriptor set required for UpdateProtobuf validation
SchemaConfig: &omashu.SchemaConfig{
    Type:            omashu.SchemaTypeProtobuf,
    ProtoSchemaList: []*descriptorpb.FileDescriptorSet{myFileDescSet},
}
```

### RaftConfig

All fields from `go.etcd.io/raft/v3.Config` are embedded directly. Recommended starting values:

```go
RaftConfig: &omashu.RaftConfig{
    Config: raft.Config{
        ID:              nodeID,   // unique non-zero uint64 per node
        ElectionTick:    10,
        HeartbeatTick:   1,
        MaxSizePerMsg:   4 * 1024,
        MaxInflightMsgs: 256,
        CheckQuorum:     true,    // leader steps down if it loses quorum contact
        PreVote:         true,    // prevents disruptive elections on network partition
    },
    Nodename: "node-1",
    Peers: map[uint64]string{
        1: "http://node-1:7070",
        2: "http://node-2:7070",
        3: "http://node-3:7070",
    },
}
```

---

## Architecture

```
┌─────────────────────────────────────────┐
│              Your Application           │
└──────────────────┬──────────────────────┘
                   │  Database[T] interface
       ┌───────────┴──────────────┐
       │                          │
  ┌────▼──────┐       ┌───────────▼──────────────┐
  │  Badger   │       │    DistributedBadger      │
  │(single    │       │   (replicated cluster)    │
  │ node)     │       └──────┬───────────┬────────┘
  └─────┬─────┘              │           │
        │              ┌─────▼──┐   ┌────▼───────┐
        │              │  TSO   │   │ Raft Node  │
        │              │        │   │            │
        │              └──┬─────┘   └────┬───────┘
        │           TxnManager           │
        │                │          ┌────▼───────┐
        │                │          │  Storage   │
        │                │          │ (WAL+Snap) │
        │                │          └────────────┘
        │           ┌────▼────────────────┐
        └───────────►        FSM          │
                    │   (BadgerDB MVCC)   │
                    └─────────────────────┘
```

### Component Summary

| Component | Responsibility |
|---|---|
| **Badger** | Single-node embedded MVCC store backed by BadgerDB in managed mode |
| **DistributedBadger** | Distributed store: routes reads through linearizable ReadIndex, routes writes through Raft proposals |
| **TSO** (Timestamp Oracle) | Generates monotonically increasing hybrid logical clock timestamps; tracks in-flight read timestamps for conflict detection |
| **TxnManager** | Manages transaction lifecycle: assigns `readTs`, buffers writes, detects conflicts at `commitTs` |
| **Node** | Drives the etcd/raft state machine; owns the ready-handler loop, proposal pipeline, leader election, and membership changes |
| **FSM** | Applies committed Raft log entries and snapshots to BadgerDB |
| **Storage** | Persists Raft hard state, WAL entries, and snapshots to disk |
| **Transport** | HTTP peer-to-peer message passing using etcd's `rafthttp` package |

### Transaction Flow (Distributed)

```
1. BeginTxn()     →  TSO issues readTs  (snapshot timestamp for this transaction)
2. Read ops       →  read from BadgerDB at readTs  (consistent, isolated snapshot)
3. Write ops      →  buffered in Txn struct  (not yet written to storage)
4. Commit()       →  TSO issues commitTs, conflict check against concurrent commits
5. Propose        →  serialized command proposed to Raft leader
6. Quorum commit  →  Raft replicates entry to majority of nodes
7. FSM.Apply()    →  writes applied to BadgerDB at commitTs on all replicas
```

Conflict detection aborts any transaction where a key it wrote was also written by a concurrent transaction that committed after `readTs`.

### Linearizable Reads

All reads on `DistributedBadger` are **linearizable** by default. Before serving data, omashu sends a `ReadIndex` request to Raft and waits until the local applied index reaches the confirmed read index. This prevents stale reads even immediately after a leader election.

### Hybrid Logical Clock

The TSO uses a 64-bit timestamp composed of a physical component (current wall-clock milliseconds) and a 18-bit logical counter for sub-millisecond ordering:

```
63                           18 17        0
+------------------------------+-----------+
| physical time (ms)           | logical   |
+------------------------------+-----------+

ts = (physical_ms << 18) | logical
```

The upper bound is persisted to BadgerDB so that a newly elected leader can safely advance the clock past the previous leader's maximum without risk of timestamp regression.

---

## Error Reference

| Error | When it occurs |
|---|---|
| `ErrNotLeader` | Write or transaction attempted on a non-leader node |
| `ErrProposeTimeout` | Raft did not commit the proposal within the timeout (default: 5 s) |
| `ErrBatchTooBig` | Transaction write set exceeds `MaxBatchSize` |
| `ErrMissingRaftConf` | `RaftConfig` not provided when calling `NewDistributedBadger` |
| `ErrMissingCluster` | `Cluster` field not set in `Config` |
| `ErrMissingBaseDir` | `BaseDir` field not set in `Config` |
| `ErrMissingSchemaConfig` | `SchemaConfig` field not set in `Config` |
| `ErrUnknownProtoMsg` | `UpdateProtobuf` called with a message type not in the registered schema |
| `ErrUnknownOp` | FSM received an unrecognised command type (indicates an internal bug) |
| `badger.ErrReadOnlyTxn` | Write operation called on a read-only `Txn` |
| `badger.ErrDiscardedTxn` | Any operation called on a `Txn` after `Discard()` or after `Commit()` |

---

## Running Tests

```bash
# All tests
go test ./...

# Benchmarks
go test -bench=. ./...

# With race detector
go test -race ./...

# Build optimisation analysis
go build -gcflags="-m" ./...
```

---

## Directory Structure

```
omashu/
├── store.go          # Database[T] interface and NewBadger / NewDistributedBadger constructors
├── distributed.go    # DistributedBadger: read/write ops, Raft proposal pipeline
├── badger.go         # Badger (single-node) read/write implementation
├── node.go           # Raft node: ready-handler, propose-handler, leader change hooks
├── fsm.go            # FSM: applies committed log entries and snapshots to BadgerDB
├── tso.go            # Timestamp Oracle: hybrid logical clocks, conflict tracking, GC
├── txn.go            # Txn struct: write buffering, conflict key tracking, commit/discard
├── transport.go      # HTTP Raft peer transport (etcd rafthttp)
├── replication.go    # Snapshot: BadgerDB backup (TakeSnapshot) and restore (Restore)
├── storage.go        # Raft storage: WAL + snapshotter wiring
├── wal.go            # WAL wrapper around etcd's wal package
├── config.go         # Config, RaftConfig, SchemaConfig, Cluster interface, validation
├── errors.go         # Sentinel error values
├── logger.go         # zap ↔ raft/badger logger adapters
├── types/
│   ├── types.proto   # Protobuf definitions for Raft command types (Set, Delete, IncrBy, …)
│   ├── types.pb.go   # Generated protobuf code
│   ├── dynamic.go    # Dynamic Protobuf descriptor registry for UpdateProtobuf validation
│   └── methods.go    # Command builder helpers (NewSetCommand, NewDeleteCommand, …)
├── utils/
│   └── utils.go      # Byte ↔ uint64 conversion helpers
└── assets/
    └── fixtures.json # Test data loaded by the test suite
```

---

## License

Apache 2.0 © 2026 Sachin S — see [LICENSE](LICENSE).
