# Integration Test Cases — Omashu Distributed KV Store

> **Generated:** May 31, 2026  
> **System Under Test:** `github.com/avatar31/omashu` — DistributedBadger  
> **Cluster Size:** 3–5 nodes  
> **Consistency Model:** Linearizable reads (ReadIndex), Snapshot Isolation (transactions)  
> **Replication:** Raft consensus via etcd/raft v3; BadgerDB MVCC managed mode  

---

## Table of Contents
1. [Cluster Formation & Membership](#1-cluster-formation--membership)
2. [Replication & Log Propagation](#2-replication--log-propagation)
3. [TSO (Timestamp Oracle) Lifecycle](#3-tso-timestamp-oracle-lifecycle)
4. [Linearizable Reads (ReadIndex)](#4-linearizable-reads-readindex)
5. [Distributed Transactions](#5-distributed-transactions)
6. [Concurrent Operations & Conflict Detection](#6-concurrent-operations--conflict-detection)
7. [Leader Election & Consensus](#7-leader-election--consensus)
8. [Network Failure Scenarios](#8-network-failure-scenarios)
9. [Node Failure & Recovery](#9-node-failure--recovery)
10. [Snapshot & Restore](#10-snapshot--restore)
11. [Data Integrity & MVCC](#11-data-integrity--mvcc)
12. [Configuration & Schema Validation](#12-configuration--schema-validation)
13. [Performance & Load](#13-performance--load)
14. [Chaos Engineering](#14-chaos-engineering)

---

## 1. Cluster Formation & Membership

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| CF-01 | Single-node cluster bootstrap | Verify a single `DistributedBadger` node starts, elects itself leader, and accepts writes | Node binary built; `BaseDir` writable | 1. Call `NewDistributedBadger` with 1-peer config. 2. Wait for `OnLeaderChange` callback (prev=0, next=nodeID). 3. Call `Set`. **Expected:** `Set` returns nil; `Get` returns the value. | Critical |
| CF-02 | 3-node cluster bootstrap | Verify all three nodes form a cluster, one leader is elected, and writes replicate | 3 nodes with unique IDs and addresses; matching peer maps | 1. Start all 3 nodes concurrently. 2. Wait for a single leader via `OnLeaderChange`. 3. `Set` on leader. **Expected:** Leader elected within ElectionTick × HeartbeatTick × 100ms; all nodes reflect the write. | Critical |
| CF-03 | 5-node cluster bootstrap | Verify a 5-node cluster forms with quorum (3) and elects a stable leader | 5 nodes configured | Same as CF-02 for 5 nodes. **Expected:** Leader elected; 2 node failures still permit writes. | High |
| CF-04 | Node discovery via peers map | Verify each node connects to all configured peers on startup | 3-node cluster | 1. Start node A. 2. Start nodes B and C. 3. Inspect transport peer counts. **Expected:** Each node has 2 registered peers; rafthttp connections established. | High |
| CF-05 | Cluster health after all nodes start | Verify `IsLeader()` / `Leader()` return consistent values across all nodes | 3-node cluster, leader elected | 1. Query `node.IsLeader()` on all 3 nodes. **Expected:** Exactly 1 node returns true; all others return the same leader ID via `Leader()`. | High |
| CF-06 | Dynamic `AddNode` via ConfChange | Verify a 4th node can join a running 3-node cluster | 3-node cluster with active writes | 1. Propose `ConfChangeAddNode` for node 4. 2. Start node 4. 3. Wait for node 4 to catch up (snapshot or log replay). 4. Write from leader. **Expected:** Node 4 applies all entries; `Get` returns correct values. | High |
| CF-07 | Dynamic `RemoveNode` via ConfChange | Verify a node can be safely removed and the cluster continues | 3-node cluster | 1. Propose `ConfChangeRemoveNode` for follower node. 2. Confirm `OnRemovedSelf` fires on removed node. 3. Continue writes from leader. **Expected:** Cluster operates with remaining 2 nodes; removed node stops processing. | High |
| CF-08 | Node rejoins after clean restart | Verify a restarted node replays WAL and rejoins the cluster | 3-node cluster, 50 entries written | 1. Stop follower cleanly. 2. Restart it with same config. 3. Write 10 more entries to leader. **Expected:** Restarted node applies all entries; `Get` returns the latest values. | Critical |
| CF-09 | Duplicate node ID rejected | Verify two nodes with the same ID cannot both operate in the cluster | 3-node cluster running | 1. Start a 4th node with the same ID as an existing node. **Expected:** Cluster rejects the duplicate; existing node continues; duplicate cannot propose entries. | High |
| CF-10 | Cluster name consistency | Verify `Cluster.GetName()` is used uniformly in transport stats and does not cause peer registration failures | 3-node cluster | 1. Inspect `ServerStats` and `LeaderStats` in rafthttp transport. **Expected:** Stats carry the expected cluster name; no nil-dereference panics. | Medium |

---

## 2. Replication & Log Propagation

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| RP-01 | Single `Set` replicates to all followers | Verify a leader write is visible on all followers after commit | 3-node cluster, leader elected | 1. `Set(ctx, "key1", val)` on leader. 2. Read `key1` from each follower node's BadgerDB directly. **Expected:** All nodes have identical value at the same commitTs. | Critical |
| RP-02 | `Delete` replicates to all followers | Verify a delete is applied on all replicas | 3-node cluster, `key1` exists on all nodes | 1. `Delete(ctx, "key1")` on leader. 2. `Exists` on all nodes. **Expected:** All nodes report `false`. | Critical |
| RP-03 | `DeleteByPrefix` replicates atomically | Verify prefix deletion is applied across the cluster as a single atomic entry | 3-node cluster, 20 keys under prefix `"users:"` | 1. `DeleteByPrefix(ctx, "users:")` on leader. 2. `GetByPrefix` on each node. **Expected:** 0 results on all nodes; no partial deletion. | High |
| RP-04 | `IncrBy` replicates correct counter value | Verify counter increments are consistently replicated | 3-node cluster, counter=0 | 1. `IncrBy(ctx, "counter", 5)` on leader. 2. Read counter from all nodes. **Expected:** All nodes return `5`. | High |
| RP-05 | `DecrBy` replicates correct counter value | Verify counter decrements are consistently replicated | 3-node cluster, counter=10 | 1. `DecrBy(ctx, "counter", 3)` on leader. 2. Read from all nodes. **Expected:** All return `7`. | High |
| RP-06 | `UpdateJson` partial merge replicates | Verify JSON merge-patch delta is applied identically on all replicas | 3-node cluster, JSON object stored at `"cfg"` | 1. `UpdateJson(ctx, "cfg", delta)` on leader. 2. Read and unmarshal from all nodes. **Expected:** All nodes show the merged result; unmodified fields preserved. | High |
| RP-07 | `UpdateProtobuf` field merge replicates | Verify Protobuf field-level merge replicates without data loss | 3-node cluster, Protobuf schema registered | 1. `UpdateProtobuf(ctx, "user:1", patch)` on leader. 2. Read and deserialize from all nodes. **Expected:** Field update visible on all nodes; other fields unchanged. | High |
| RP-08 | Large-value replication (1 MB payload) | Verify large values replicate without truncation or corruption | 3-node cluster | 1. `Set` a 1 MB byte slice. 2. Compute checksum on leader. 3. Read from each follower and compare checksum. **Expected:** Checksums match across all nodes. | High |
| RP-09 | Rapid sequential writes replicate in order | Verify 1,000 sequential writes maintain commit-timestamp order on all replicas | 3-node cluster | 1. Write keys `k-0000` to `k-0999` sequentially on leader. 2. Iterate from each follower. **Expected:** All entries present; commitTs strictly increasing. | High |
| RP-10 | Batch transaction replicates atomically | Verify a `NewTransaction` with 50 sub-commands is applied atomically on all followers | 3-node cluster | 1. `NewTransaction` with 50 `Set` sub-commands. 2. Read all 50 keys from each follower. **Expected:** Either all 50 keys are present or none (atomicity). | Critical |
| RP-11 | TTL propagation on replication | Verify TTL metadata is preserved across replicas and keys expire at consistent times | 3-node cluster | 1. `Set(ctx, "tmp", val, 2*time.Second)`. 2. After 3 seconds, `Exists` on all nodes. **Expected:** All nodes return `false` after expiry. | Medium |
| RP-12 | WAL persistence survives leader restart | Verify WAL-persisted entries are re-applied after leader restart | 3-node cluster | 1. Write 100 entries. 2. Restart leader. 3. Verify all 100 entries accessible after restart. **Expected:** No data loss; leader rejoins and catches up. | Critical |

---

## 3. TSO (Timestamp Oracle) Lifecycle

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| TS-01 | TSO starts only after leader election | Verify `TSO.StartServing` is not called before leadership is won | 3-node cluster | 1. Intercept `OnLeaderChange`. 2. Attempt `NewTransaction` before election. **Expected:** `ErrNotLeader` until election; TSO only serving on leader. | Critical |
| TS-02 | TSO upper bound persisted on startup | Verify `_tso_last_timestamp` is written to BadgerDB when TSO starts serving | Single-node cluster | 1. Start leader. 2. Directly read `_tso_last_timestamp` from BadgerDB. **Expected:** Key exists; value > current Unix ms. | High |
| TS-03 | New leader loads TSO upper bound | Verify a newly elected leader reads the persisted upper bound and starts above it | 3-node cluster; leader writes 50 entries; leader stops | 1. Stop original leader. 2. New leader is elected. 3. Inspect `tso.saved.physical`. **Expected:** `saved.physical >= previous_leader_saved_physical`; no timestamp regression. | Critical |
| TS-04 | TSO upper bound window updates periodically | Verify `_tso_last_timestamp` is refreshed every `defaultUpperBoundWindow` (2s) | Single-node cluster | 1. Read `_tso_last_timestamp` at T=0 and T=5s. **Expected:** Value at T=5s > value at T=0s. | Medium |
| TS-05 | TSO stops on leadership loss | Verify `TSO.Close()` is called and TSO stops issuing timestamps after leader steps down | 3-node cluster | 1. Stop leader. 2. New leader elected. 3. Old leader attempts `NewTransaction`. **Expected:** Old leader returns `ErrNotLeader`; old TSO is closed. | Critical |
| TS-06 | TSO `allocate` produces strictly increasing timestamps | Verify all allocated `commitTs` values are monotonically increasing within a leader term | Single-node cluster | 1. Perform 1,000 `Set` ops sequentially. 2. Collect all `commitTs` from BadgerDB MVCC versions. **Expected:** Strictly increasing sequence; no ties or regressions. | Critical |
| TS-07 | TSO logical counter handles 262,143 ticks/ms | Verify TSO advances physical time when `logical > MAX_LOGICAL` | Single-node cluster | 1. Mock system clock to freeze millisecond. 2. Issue > 262,143 timestamps in same millisecond. **Expected:** TSO waits for next millisecond; no timestamp duplication. | High |
| TS-08 | TSO clock calibration after system clock regresses | Verify `calibrate()` sleeps until system clock catches up after a backward jump | Single-node cluster | 1. Mock clock jump backward by 500ms. 2. Start TSO. **Expected:** TSO waits ≥500ms before serving; first timestamp > previous max. | High |
| TS-09 | `txnMark` watermark advances after commit | Verify `txnMark.Done(commitTs)` unblocks `ReadTs` for subsequent transactions | Single-node cluster | 1. Begin 2 transactions simultaneously. 2. Commit txn A. 3. Start txn B. **Expected:** `txn B.readTs >= txn A.commitTs`; txn B sees txn A's writes. | Critical |
| TS-10 | `readMark` watermark advances after Discard | Verify `readMark.Done(readTs)` is called when a transaction is discarded | Single-node cluster | 1. Begin transaction; hold open. 2. Check `readMark.DoneUntil()`. 3. Discard transaction. 4. Re-check `readMark.DoneUntil()`. **Expected:** Watermark advances after Discard. | High |
| TS-11 | TSO `committedTxns` cleanup fires correctly | Verify `cleanupCommittedTransactions` removes entries once all readers past their `readTs` | Single-node cluster | 1. Commit 100 transactions. 2. Verify `len(tso.committedTxns) <= committed` after all reads complete. **Expected:** Committed transactions are pruned; list does not grow unboundedly. | High |
| TS-12 | TSO `readMark` not stuck on concurrent `BeginTxn` | Verify multiple transactions starting at the same `readTs` all properly unblock `readMark` | Single-node cluster | 1. Issue 10 concurrent `BeginTxn` calls with no writes between them (same `readTs`). 2. Discard all. 3. Check `readMark.DoneUntil()`. **Expected:** Watermark advances past all 10 readTs values. | Critical |

---

## 4. Linearizable Reads (ReadIndex)

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| LR-01 | `Get` returns committed value — leader | Verify `Get` on leader returns the most recently committed value | 3-node cluster, leader known | 1. `Set(ctx, "k", v1)`. 2. Immediately `Get(ctx, "k")`. **Expected:** Returns `v1`, not stale data. | Critical |
| LR-02 | `Get` respects ReadIndex on follower node instance | Verify a follower's `DistributedBadger.Get` uses ReadIndex before reading | 3-node cluster | 1. Write `k=v1` via leader. 2. Immediately call `Get("k")` via a `DistributedBadger` instance pointing to a follower-backed FSM. **Expected:** Returns `v1` (linearizable). | Critical |
| LR-03 | `Exists` uses ReadIndex protocol | Verify `Exists` calls `waitForReadState` and does not return stale `false` | 3-node cluster | 1. `Set("k", v)`. 2. `Exists("k")` in parallel from a second goroutine. **Expected:** `Exists` returns `true` once write is committed. | High |
| LR-04 | Read during leader election returns error | Verify reads fail gracefully when no leader is available | 3-node cluster; leader stopped, election in progress | 1. Call `Get(ctx, "k")` with short-timeout context during election. **Expected:** Returns `context.DeadlineExceeded` or ReadIndex error; no panic. | High |
| LR-05 | `WaitForReadState` respects 5-second timeout | Verify `waitForReadState` returns an error rather than hanging indefinitely when Raft is unavailable | Isolated single node (no quorum) | 1. Disconnect node from peers. 2. Call `Get(ctx, "k")` with 10s context. **Expected:** Returns error within ~5s (ReadIndex timeout). | High |
| LR-06 | 50 concurrent `Get` calls resolve correctly | Verify concurrent linearizable reads all return consistent data | 3-node cluster, 50 keys pre-written | 1. Launch 50 goroutines each calling `Get` on different keys. **Expected:** All return correct values; no data races or wrong responses. | High |
| LR-07 | Read after leadership change sees all committed data | Verify a `Get` after leader failover returns data committed by the previous leader | 3-node cluster | 1. Write `k=v1` via leader A. 2. Kill leader A. 3. New leader B elected. 4. `Get("k")` from leader B. **Expected:** Returns `v1`. | Critical |
| LR-08 | `Count` and `HasChild` bypass ReadIndex (local) | Verify `Count` and `HasChild` do not trigger ReadIndex (documented local-read behaviour) | 3-node cluster | 1. Write 10 keys under prefix `"x:"`. 2. Call `Count(ctx, "x:")` and `HasChild(ctx, "x:")` on all nodes immediately. **Expected:** Returns consistent counts; no ReadIndex call. | Medium |
| LR-09 | `BulkGet` returns all requested keys | Verify multi-key fetch returns all committed keys | 3-node cluster, 20 keys | 1. `Set` 20 keys. 2. `BulkGet` all 20 keys. **Expected:** Map contains all 20 entries; missing keys omitted. | High |
| LR-10 | `IterateByPrefix` pagination is consistent | Verify cursor-based iteration returns all entries without duplicates or gaps | 3-node cluster, 1,000 keys | 1. Write 1,000 `"page:XXXX"` keys. 2. Iterate with `limit=100` until cursor is empty. **Expected:** Exactly 1,000 unique keys returned across all pages. | High |

---

## 5. Distributed Transactions

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| DT-01 | Single-key transaction commit | Verify a single-op transaction commits successfully and is visible after Raft apply | 3-node cluster | 1. `NewTransaction` with one `txn.Set`. **Expected:** Returns nil; `Get` returns the value. | Critical |
| DT-02 | Multi-key transaction atomic commit | Verify 50 writes in one transaction all become visible simultaneously | 3-node cluster | 1. `NewTransaction` with 50 `txn.Set` on distinct keys. **Expected:** All 50 keys visible after commit; none visible before. | Critical |
| DT-03 | Transaction rollback on `performOps` error | Verify no writes reach BadgerDB when `performOps` returns an error | 3-node cluster | 1. `NewTransaction` returns error from `performOps`. **Expected:** No Raft proposal issued; `Get` on any written key returns not-found. | Critical |
| DT-04 | Transaction conflict: write-after-read | Verify a transaction is aborted when a key it read is written by a concurrent committed transaction | 3-node cluster | 1. Txn A: `GetWithTxn("balance")` → hold open. 2. Txn B: `Set("balance", newVal)` → commit. 3. Txn A: commit. **Expected:** Txn A returns `badger.ErrConflict`. | Critical |
| DT-05 | No conflict: disjoint key sets | Verify two concurrent transactions on non-overlapping keys both commit successfully | 3-node cluster | 1. Txn A writes `"a:1"`. 2. Txn B writes `"b:1"`. Both commit concurrently. **Expected:** Both return nil; both key sets present. | High |
| DT-06 | `MaxBatchSize` (100) transaction succeeds | Verify a transaction with exactly 100 sub-commands is accepted | 3-node cluster | 1. `NewTransaction` with exactly 100 `txn.Set` calls. **Expected:** Returns nil. | High |
| DT-07 | `MaxBatchSize+1` (101) returns `ErrBatchTooBig` | Verify a transaction exceeding the batch limit is rejected before Raft proposal | 3-node cluster | 1. `NewTransaction` with 101 sub-commands. **Expected:** Returns `ErrBatchTooBig`; no Raft entry created. | High |
| DT-08 | `ErrNotLeader` on non-leader `NewTransaction` | Verify follower rejects transactions | 3-node cluster | 1. Call `NewTransaction` on a non-leader `DistributedBadger`. **Expected:** Returns `ErrNotLeader` immediately. | Critical |
| DT-09 | Transaction `GetWithTxn` reads at snapshot timestamp | Verify reads inside a transaction are isolated to `readTs` and do not see concurrent uncommitted writes | 3-node cluster | 1. Txn A: `GetWithTxn("k")` → sees `v0`. 2. Concurrent goroutine: `Set("k", v1)`. 3. Txn A: `GetWithTxn("k")` again. **Expected:** Still returns `v0` (snapshot isolation). | Critical |
| DT-10 | `GetByPrefixWithTxn` registers all keys for conflict detection | Verify prefix reads inside a transaction add keys to the conflict set | 3-node cluster | 1. Txn A: `GetByPrefixWithTxn("user:")` → returns 10 keys. 2. Concurrent write to `"user:1"`. 3. Txn A commits. **Expected:** Txn A returns `badger.ErrConflict`. | High |
| DT-11 | `DeleteByPrefix` inside transaction is atomic | Verify prefix deletion within a transaction removes all matching keys or none | 3-node cluster, 20 keys under `"tmp:"` | 1. `NewTransaction` with `txn.DeleteByPrefix("tmp:")`. 2. Return error from `performOps` after deletion. **Expected:** All 20 `"tmp:"` keys still present. | High |
| DT-12 | `txn.IncrBy` inside transaction buffers correctly | Verify `IncrBy` inside a transaction does not write to BadgerDB until commit | 3-node cluster, counter=5 | 1. `NewTransaction`: `txn.IncrBy("ctr", 10)`. 2. Read counter before commit. 3. Commit. 4. Read counter after commit. **Expected:** Counter reads 5 before; 15 after. | High |
| DT-13 | `txn.UpdateJson` merge inside transaction | Verify JSON delta buffered in transaction produces correct merged result on commit | 3-node cluster | 1. `NewTransaction`: `txn.UpdateJson("cfg", delta)`. 2. Commit. 3. `Get("cfg")` and unmarshal. **Expected:** Delta fields updated; other fields unchanged. | High |
| DT-14 | `txn.UpdateProtobuf` field merge inside transaction | Verify Protobuf merge buffered in transaction produces correct result on commit | 3-node cluster, schema registered | 1. `NewTransaction`: `txn.UpdateProtobuf("user:1", patch)`. 2. Commit. **Expected:** Patched fields updated; other fields unchanged. | High |
| DT-15 | `txn.Discard` after `Commit` is a no-op | Verify calling `Discard` after a successful `Commit` does not double-advance the watermark | 3-node cluster | 1. Open transaction; commit. 2. Call `Discard` on the same `*Txn`. **Expected:** No panic; `readMark` not double-decremented; watermark consistent. | Medium |
| DT-16 | Read-only transaction produces no Raft proposal | Verify a `NewTransaction` that only reads (no writes) does not submit any entry to Raft | 3-node cluster | 1. `NewTransaction` with only `GetWithTxn` calls; return nil. **Expected:** `txn.Commit()` returns `(nil, nil)`; no Raft proposal; `proposeAndWait` not called. | High |
| DT-17 | Transaction timeout — `ErrProposeTimeout` | Verify a transaction waiting for Raft that never commits returns `ErrProposeTimeout` | Isolated single node (no quorum) | 1. Disconnect node from peers. 2. Start `NewTransaction`; commit. **Expected:** Returns `ErrProposeTimeout` within `DefaultProposeTimeout` (5s). | High |
| DT-18 | `ErrUnknownProtoMsg` for unregistered Protobuf type | Verify `txn.UpdateProtobuf` rejects unregistered message types before creating a sub-command | 3-node cluster, schema registered but not including test type | 1. Call `txn.UpdateProtobuf("k", unknownMsg)`. **Expected:** Returns `ErrUnknownProtoMsg`; no sub-command added; transaction still usable. | High |

---

## 6. Concurrent Operations & Conflict Detection

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| CO-01 | Concurrent `Set` on same key — last write wins | Verify concurrent single-op writes to the same key result in one consistent value | 3-node cluster | 1. 10 goroutines call `Set("k", unique_val)` concurrently. **Expected:** No errors (each is independent op); `Get("k")` returns one of the values; no corruption. | High |
| CO-02 | Concurrent transactions on overlapping keys — conflict resolved | Verify at most one transaction with overlapping read-write sets commits successfully | 3-node cluster | 1. 5 goroutines each start a transaction, read `"balance"`, write `"balance"`. **Expected:** Exactly 1 commits; others return `badger.ErrConflict`. | Critical |
| CO-03 | Concurrent `IncrBy` produces consistent total | Verify 100 concurrent `IncrBy(1)` operations produce counter=100 | 3-node cluster, counter=0 | 1. 100 goroutines each call `IncrBy("ctr", 1)`. **Expected:** `Get("ctr")` = 100 after all complete; no lost increments. | Critical |
| CO-04 | Concurrent `DeleteByPrefix` and `GetByPrefix` | Verify no partial state is visible when prefix deletion races with a prefix read | 3-node cluster | 1. 20 `"item:"` keys. 2. `DeleteByPrefix("item:")` and `GetByPrefix("item:")` concurrently. **Expected:** `GetByPrefix` returns either all 20 or 0 (atomicity). | High |
| CO-05 | Write-write conflict: two transactions write same key | Verify the TSO `hasConflict` detects write-read overlap and aborts the loser | 3-node cluster | 1. Txn A reads `"seat:42"`. 2. Txn B writes `"seat:42"` and commits. 3. Txn A writes `"seat:42"` and commits. **Expected:** Txn A returns `badger.ErrConflict`. | Critical |
| CO-06 | No false conflicts: disjoint read/write sets | Verify transactions with fully disjoint key sets never conflict | 3-node cluster | 1. Txn A: reads/writes `"a:*"` keys only. 2. Txn B: reads/writes `"b:*"` keys only. Both commit concurrently. **Expected:** Both succeed. | High |
| CO-07 | `proposals` sync.Map no deadlock under concurrency | Verify concurrent `proposeAndWait` calls do not deadlock or leak entries in the proposals map | 3-node cluster | 1. 50 goroutines concurrently call `Set`. 2. All complete. **Expected:** `proposals` map is empty after all calls return; no goroutine leaks. | High |
| CO-08 | Concurrent `GetWithTxn` and `Set` — snapshot isolation | Verify an in-progress read does not see a concurrent write that commits mid-transaction | 3-node cluster | 1. Txn A opens at `readTs=T`. 2. `Set("k", v_new)` commits at `commitTs=T+1`. 3. Txn A calls `GetWithTxn("k")`. **Expected:** Returns value at `T`, not `T+1`. | Critical |
| CO-09 | 1,000 concurrent read operations — no stale data | Verify all concurrent `Get` calls return up-to-date committed data | 3-node cluster, 1,000 pre-written keys | 1. 1,000 goroutines each `Get` their assigned key. **Expected:** All return correct values; no goroutine returns stale data detectable by checksum. | High |
| CO-10 | Proposal channel contention under high write load | Verify `proposeReqNotifier` (buffer=1) does not cause unbounded blocking or dropped proposals under 100 concurrent writers | 3-node cluster | 1. 100 goroutines each attempt `Set` with 10s context timeout. **Expected:** All complete eventually; no ErrProposeTimeout under normal cluster conditions. | High |

---

## 7. Leader Election & Consensus

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| LE-01 | Leader elected within timeout | Verify a leader is elected within `ElectionTick × HeartbeatTick × 100ms` | 3-node cluster starting fresh | 1. Start all 3 nodes simultaneously. 2. Record time of first `OnLeaderChange`. **Expected:** Leader elected within the configured timeout window. | Critical |
| LE-02 | Leader failover on leader stop | Verify a new leader is elected after the current leader is gracefully stopped | 3-node cluster, leader known | 1. Stop leader node. 2. Wait for `OnLeaderChange` on remaining nodes. **Expected:** New leader elected; writes accepted within election timeout. | Critical |
| LE-03 | TSO reinitializes on new leader | Verify `TSO.StartServing` is called on the new leader and `_tso_last_timestamp` is read | 3-node cluster | 1. Stop original leader. 2. New leader elected. 3. Write to new leader. **Expected:** New leader issues timestamps > old leader's max; no timestamp regression. | Critical |
| LE-04 | `OnLeaderChange` callback fires with correct IDs | Verify `OnLeaderChange(prev, next)` carries accurate node IDs | 3-node cluster | 1. Register `OnLeaderChange` hook. 2. Trigger leader election. **Expected:** Callback fires; `prev` = old leader ID or 0; `next` = new leader ID. | High |
| LE-05 | `leaderChangeNotifier` is closed and recreated each term | Verify `listenProposeResponses` goroutine exits on leadership loss and a new one starts on next election | 3-node cluster | 1. Induce 2 consecutive leader changes. 2. Count `listenProposeResponses` goroutine lifecycle events via logs. **Expected:** Each lost-leadership event closes the notifier; each gained-leadership event creates a new one. | High |
| LE-06 | Non-leader `Set` returns `ErrNotLeader` | Verify follower rejects write operations | 3-node cluster | 1. Call `Set` on a known follower. **Expected:** Returns `ErrNotLeader` immediately without Raft proposal. | Critical |
| LE-07 | `PreVote` prevents stale candidate disruption | Verify a long-partitioned node cannot disrupt the cluster's term on rejoin | 3-node cluster with `PreVote: true` | 1. Isolate node C for > election timeout. 2. Heal partition. **Expected:** Node C does not cause spurious elections; existing leader retains leadership. | High |
| LE-08 | `CheckQuorum` leader step-down on quorum loss | Verify leader steps down when it cannot contact a majority | 3-node cluster with `CheckQuorum: true` | 1. Partition leader from both followers. 2. Wait one `CheckQuorum` interval. **Expected:** Leader steps down (`IsLeader()` → false); writes return `ErrNotLeader`. | High |
| LE-09 | Rapid leader elections — cluster stabilizes | Verify the cluster stabilizes after 5 rapid consecutive leader changes | 3-node cluster | 1. Kill and restart the leader 5 times in 10-second intervals. **Expected:** A stable leader exists after the last restart; writes succeed. | High |
| LE-10 | In-flight proposals drained on leader loss | Verify all pending `proposeAndWait` calls return `ErrProposeTimeout` (not hang) when leadership is lost mid-flight | 3-node cluster | 1. Send 10 proposals from leader. 2. Immediately kill leader before proposals commit. **Expected:** All 10 callers return `ErrProposeTimeout` within `DefaultProposeTimeout`. | High |
| LE-11 | Single-node cluster is its own leader | Verify a 1-node cluster immediately elects itself leader | 1-node config | 1. `NewDistributedBadger` with single-peer map. **Expected:** `OnLeaderChange` fires with `next = nodeID` before any write attempts. | Medium |

---

## 8. Network Failure Scenarios

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| NF-01 | Network partition: 1+2 split in 3-node cluster | Verify majority partition (2 nodes) continues accepting writes; minority (1 node) rejects writes | 3-node cluster | 1. Isolate leader from 1 follower. 2. Write from isolated leader. **Expected:** If isolated node has majority (impossible in 2+1 split with leader isolated) it rejects. Majority side elects new leader and accepts writes. | Critical |
| NF-02 | Split-brain prevention: two leaders impossible | Verify two nodes cannot simultaneously believe they are leader | 3-node cluster; leader partitioned | 1. Create network partition. 2. Query `IsLeader()` on all 3 nodes during election. **Expected:** At most 1 node returns `true` at any point. | Critical |
| NF-03 | Majority partition elects new leader and serves writes | Verify the 2-node majority in a 3-node cluster elects a leader | 3-node cluster | 1. Isolate 1 follower. 2. Kill the leader. 3. Verify 1 remaining node elects a new leader. **Expected:** New leader elected; writes succeed on majority side. | High |
| NF-04 | Minority partition rejects writes | Verify the isolated 1-node minority cannot accept writes | 3-node cluster; 1 node isolated | 1. Attempt `Set` on the isolated node. **Expected:** Returns `ErrNotLeader` (isolated node cannot win election without quorum). | High |
| NF-05 | Network partition heals — minority node catches up | Verify the isolated node rejoins and synchronizes all missed entries | 3-node cluster; 50 writes during partition | 1. Heal partition. 2. Wait for node to rejoin. 3. `Get` on recovered node. **Expected:** All 50 entries are visible; no data loss. | Critical |
| NF-06 | High network latency — writes succeed within extended timeout | Verify writes succeed under 2s one-way latency | 3-node cluster with 2s synthetic latency on all links | 1. `Set` with 15s context. **Expected:** Returns nil within extended timeout; replication confirmed. | High |
| NF-07 | Packet loss on follower link — replication still occurs | Verify rafthttp retries deliver messages despite 30% packet loss | 3-node cluster; 30% packet loss on follower1 ↔ leader | 1. Write 100 entries over 30s. **Expected:** All 100 entries eventually appear on follower1; no permanent replication failure. | Medium |
| NF-08 | One-way network failure (follower cannot reach leader) | Verify cluster survives asymmetric connectivity | 3-node cluster; follower1 can receive from leader but not send | 1. Block follower1 → leader traffic. 2. Write 20 entries. 3. Verify follower1 received entries via ReadIndex. **Expected:** Follower1 applies entries (leader can still push); reads succeed. | Medium |
| NF-09 | All peers temporarily unreachable — leader returns `ErrProposeTimeout` | Verify leader returns `ErrProposeTimeout` for writes when all followers are unreachable | 3-node cluster; both followers disconnected | 1. `Set` with 6s context. **Expected:** Returns `ErrProposeTimeout` after 5s. | High |
| NF-10 | Transport reconnects after peer restart | Verify rafthttp transport reconnects to a restarted peer without manual intervention | 3-node cluster | 1. Stop follower. 2. Restart follower after 30s. **Expected:** Transport detects reconnection; replication resumes automatically. | High |

---

## 9. Node Failure & Recovery

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| NR-01 | Leader crash — cluster recovers | Verify followers elect a new leader and serve writes after sudden leader death | 3-node cluster | 1. `kill -9` leader process. 2. Wait for election. 3. Write to new leader. **Expected:** New leader elected; no data loss for committed entries. | Critical |
| NR-02 | Follower crash — cluster unaffected | Verify 2-node majority continues when 1 follower crashes | 3-node cluster | 1. Kill one follower. 2. Write 50 entries to leader. **Expected:** All 50 commits succeed; surviving follower replicates all entries. | High |
| NR-03 | Follower restart — log replay recovery | Verify a crashed follower rejoins and replays WAL to catch up | 3-node cluster; follower killed, 20 entries written | 1. Restart follower. 2. Wait for `appliedIndex` to reach leader's. **Expected:** Follower applies all missed entries; data consistent with leader. | Critical |
| NR-04 | Follower restart — snapshot-based recovery | Verify a far-behind follower receives a snapshot instead of full log replay | 3-node cluster; follower off for `defaultSnapshotCount+1` entries | 1. Restart stale follower. 2. Observe snapshot transfer in logs. **Expected:** Follower receives snapshot; applies it; data consistent with leader. | Critical |
| NR-05 | Two followers crash — leader loses quorum | Verify the leader stops accepting writes when quorum is lost | 3-node cluster | 1. Kill both followers. 2. Attempt `Set` on leader. **Expected:** Returns `ErrProposeTimeout`; leader does not commit unilaterally. | Critical |
| NR-06 | Leader crash mid-transaction — no partial state | Verify an uncommitted transaction is not applied after leader crash | 3-node cluster | 1. Begin transaction on leader. 2. Kill leader before commit. 3. New leader elected. **Expected:** Transaction data absent on new leader; no partial state visible. | Critical |
| NR-07 | All nodes restart — full WAL recovery | Verify the cluster recovers all data after a controlled shutdown of all 3 nodes | 3-node cluster, 500 entries written | 1. Gracefully stop all 3 nodes. 2. Restart all 3. 3. Wait for leader election. **Expected:** All 500 entries accessible; WAL replayed correctly. | Critical |
| NR-08 | Node crash during snapshot creation | Verify a partial snapshot is discarded and the node recovers on restart | 3-node cluster; kill node mid-`takeSnapshotIfNeeded` | 1. Trigger snapshot. 2. Kill node during FSM backup. 3. Restart node. **Expected:** Node falls back to WAL replay or receives a new snapshot; no corrupt state. | High |
| NR-09 | Disk full on follower — node fails gracefully | Verify disk-full error on WAL write does not corrupt the leader or other followers | 3-node cluster | 1. Fill follower disk. 2. Continue writes on leader. **Expected:** Follower reports error and stops; leader and other follower continue unaffected. | High |
| NR-10 | Process kill during `DeleteByPrefix` | Verify a killed node mid-apply leaves other nodes consistent | 3-node cluster; kill follower during `applyDeleteByPrefix` | 1. Trigger large `DeleteByPrefix`. 2. Kill one follower mid-apply. 3. Verify leader and surviving follower are consistent. **Expected:** No divergence; killed follower catches up on restart. | High |
| NR-11 | `Node.Stop` returns without goroutine leaks | Verify `Stop()` terminates all goroutines started by `Start()` | Single node | 1. Call `node.Stop(ctx)`. 2. Wait 2s. 3. Check goroutine count. **Expected:** `readyHandler`, `proposeHandler`, `windowUpdator` goroutines all exit. | High |

---

## 10. Snapshot & Restore

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| SR-01 | Snapshot created after `defaultSnapshotCount` entries | Verify `takeSnapshotIfNeeded` triggers a snapshot automatically | Single-node cluster | 1. Write `defaultSnapshotCount + 1` entries. 2. Check snapshot file exists on disk. **Expected:** Snapshot file created; WAL compacted. | High |
| SR-02 | Forced snapshot succeeds | Verify `takeSnapshotIfNeeded(ctx, true)` always creates a snapshot | Single-node cluster | 1. Call `takeSnapshotIfNeeded(ctx, force=true)` after 5 writes. **Expected:** Snapshot file created regardless of log size. | Medium |
| SR-03 | Snapshot with `since=0` captures all MVCC versions | Verify Badger `Backup(w, 0)` includes every committed version | Single-node cluster; 3 versions of `"k"` committed | 1. `TakeSnapshot`. 2. Inspect backup for all 3 versions. **Expected:** Snapshot contains all commitTs versions of `"k"`. | Critical |
| SR-04 | Lagging follower receives snapshot from leader | Verify a far-behind follower gets a snapshot transferred via `MsgSnap` | 3-node cluster; follower partitioned for >100k entries | 1. Re-connect follower. 2. Monitor `processReady` for snapshot application. **Expected:** `applySnapshotToFSM` called; follower's `appliedIndex` jumps to snapshot index. | Critical |
| SR-05 | `RestoreSnapshot` replaces entire database | Verify `Restore(ctx, data)` completely replaces BadgerDB content | Single-node cluster with existing data | 1. Take snapshot. 2. Write more data. 3. Restore snapshot. 4. `GetByPrefix("")`. **Expected:** Only data from snapshot present; post-snapshot writes absent. | Critical |
| SR-06 | Snapshot consistency during active write workload | Verify snapshot captures a consistent point-in-time with no torn writes | 3-node cluster; 10 goroutines writing concurrently | 1. Trigger snapshot while writers are active. 2. Restore snapshot to a new node. **Expected:** Restored state is transactionally consistent; no partial transactions visible. | Critical |
| SR-07 | Snapshot includes TSO `_tso_last_timestamp` | Verify the TSO upper-bound key is captured in the snapshot so a new leader starts safely | Single-node cluster | 1. Write entries; take snapshot. 2. Restore to new node; start TSO. **Expected:** TSO reads `_tso_last_timestamp` from restored data and starts above it. | High |
| SR-08 | Snapshot applied to FSM advances `appliedIndex` | Verify `node.setAppliedIndex(snapshot.Metadata.Index)` is called after restore | 3-node cluster | 1. Restore snapshot at index 500. 2. Check `node.getAppliedIndex()`. **Expected:** Returns ≥ 500. | High |
| SR-09 | Stale snapshot rejected | Verify a snapshot with `index < appliedIndex` is rejected | 3-node cluster; node at appliedIndex=300 | 1. Deliver snapshot with `Metadata.Index=200`. **Expected:** `applySnapshotToFSM` returns an error; no regression of `appliedIndex`. | High |
| SR-10 | `confState` patched into `MsgSnap` before send | Verify `getMessagesToPublish` patches current `confState` into any `MsgSnap` message | 3-node cluster with ConfChange committed after last snapshot | 1. Commit a `ConfChangeAddNode` after snapshot. 2. Force new snapshot. 3. Inspect `MsgSnap.Snapshot.Metadata.ConfState`. **Expected:** ConfState reflects the post-ConfChange membership. | High |
| SR-11 | `maxRestorePendingTxns=256` — restore completes without OOM | Verify the pipelined restore does not exhaust memory on large snapshots | Single-node cluster; 100k keys | 1. `TakeSnapshot`. 2. `Restore(ctx, data)` on a new instance. **Expected:** Restore completes; memory stays bounded; all 100k keys accessible. | High |

---

## 11. Data Integrity & MVCC

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| DI-01 | No data loss after leader failover | Verify all committed entries survive a leader crash | 3-node cluster; 1,000 writes confirmed committed | 1. Kill leader. 2. New leader elected. 3. Read all 1,000 keys. **Expected:** All 1,000 keys present with correct values. | Critical |
| DI-02 | Committed data survives full cluster restart | Verify WAL/snapshot persistence preserves all data across a full cluster shutdown | 3-node cluster; 500 writes | 1. Stop all nodes gracefully. 2. Restart all. 3. Read all keys. **Expected:** All 500 keys present. | Critical |
| DI-03 | MVCC preserves old versions during active reads | Verify that an old version is readable at `readTs` even after a newer version is committed | Single-node cluster | 1. Write `k=v1` at `ts=T1`. 2. Open transaction at `readTs=T1`. 3. Write `k=v2` at `ts=T2`. 4. `GetWithTxn` at `readTs=T1`. **Expected:** Returns `v1`. | Critical |
| DI-04 | `deleteByPrefix` removes exactly matching keys | Verify prefix deletion does not remove keys with a longer non-matching prefix | 3-node cluster; keys `"ab"`, `"abc"`, `"ab:1"` | 1. `DeleteByPrefix("ab:")`. 2. Check `"ab"` and `"abc"`. **Expected:** Only `"ab:1"` removed; `"ab"` and `"abc"` unchanged. | High |
| DI-05 | `IncrBy` persists counter as `uint64` big-endian bytes | Verify the binary encoding is consistent between `IncrBy` writes and `BytesToUint64` reads | Single-node cluster | 1. `IncrBy("ctr", 42)`. 2. `Get("ctr")`. 3. `BytesToUint64(val)`. **Expected:** Returns `42`. | High |
| DI-06 | Replica values match leader at same `commitTs` | Verify that all replicas store the same MVCC version for a key | 3-node cluster | 1. Write `k=v` at `commitTs=T`. 2. Directly inspect BadgerDB on all 3 nodes at version `T`. **Expected:** Identical raw bytes. | Critical |
| DI-07 | `UpdateJson` preserves fields not in delta | Verify partial JSON update does not lose existing fields | Single-node cluster | 1. Store `{"name":"alice","age":30}`. 2. `UpdateJson("k", {"age":31})`. 3. Read and unmarshal. **Expected:** `name` still `"alice"`; `age` is `31`. | High |
| DI-08 | `UpdateProtobuf` preserves fields not in delta | Verify Protobuf merge does not zero unmodified fields | Single-node cluster, schema registered | 1. Store message with fields A=1, B=2. 2. Merge patch with A=5. **Expected:** A=5, B=2. | High |
| DI-09 | No phantom reads within a transaction | Verify keys written concurrently after `readTs` are not visible inside the transaction | 3-node cluster | 1. Open transaction at `readTs=T`. 2. Concurrent `Set("new-key", v)` commits at `T+1`. 3. `GetWithTxn("new-key")` inside transaction. **Expected:** Returns not-found. | High |
| DI-10 | `DiscardAt()` returns safe GC watermark | Verify the value returned by `DiscardAt()` is ≤ all active `readTs` values | Single-node cluster with active readers | 1. Start 5 read transactions at various timestamps. 2. Check `tso.DiscardAt()`. **Expected:** `DiscardAt() <= min(active readTs)`; discarding versions at or below this value does not break any active reader. | High |

---

## 12. Configuration & Schema Validation

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| SV-01 | Missing `BaseDir` returns `ErrMissingBaseDir` | Verify config validation fails fast with the correct sentinel error | New config with empty `BaseDir` | 1. `NewBadger(ctx, cfg)` with `cfg.BaseDir=""`. **Expected:** Returns `ErrMissingBaseDir`. | High |
| SV-02 | Missing `Cluster` returns `ErrMissingCluster` | Verify nil `Cluster` is caught at construction time | New config | 1. `NewDistributedBadger(ctx, cfg)` with `cfg.Cluster=nil`. **Expected:** Returns `ErrMissingCluster`. | High |
| SV-03 | Missing `SchemaConfig` returns `ErrMissingSchemaConfig` | Verify nil `SchemaConfig` is caught at construction time | New config | 1. `NewBadger(ctx, cfg)` with `cfg.SchemaConfig=nil`. **Expected:** Returns `ErrMissingSchemaConfig`. | High |
| SV-04 | Missing `RaftConfig` for distributed mode returns `ErrMissingRaftConf` | Verify distributed mode requires `RaftConfig` | New distributed config without `RaftConfig` | 1. `NewDistributedBadger(ctx, cfg)` with `cfg.RaftConfig=nil`. **Expected:** Returns `ErrMissingRaftConf`. | High |
| SV-05 | JSON schema `UpdateJson` succeeds | Verify `UpdateJson` works end-to-end with `SchemaTypeJson` | Single-node cluster with JSON schema | 1. Store JSON. 2. `UpdateJson`. 3. Read and parse. **Expected:** Returns updated object. | High |
| SV-06 | Protobuf schema `UpdateProtobuf` with registered type succeeds | Verify the registered descriptor is used for validation | Single-node cluster with Protobuf schema and descriptor registered | 1. `UpdateProtobuf("k", registeredMsg)`. **Expected:** Returns nil; data stored correctly. | High |
| SV-07 | Protobuf schema with empty `ProtoSchemaList` returns `ErrMissingSchemaConfig` | Verify Protobuf mode requires at least one descriptor set | Config with `SchemaTypeProtobuf` but empty `ProtoSchemaList` | 1. `NewBadger(ctx, cfg)`. **Expected:** Returns `ErrMissingSchemaConfig`. | Medium |
| SV-08 | `BadgerOptions.InMemory=true` stores nothing on disk | Verify in-memory mode does not write to the filesystem | Config with `InMemory: true` and dummy `BaseDir` | 1. `NewBadger`. 2. Write 100 keys. 3. Inspect `BaseDir` for files. **Expected:** No files created in `BaseDir`; data accessible in-memory. | Medium |
| SV-09 | `PeerDialTimeout` returns `1s + election_duration` | Verify the dial timeout formula is correct for given `ElectionTick` and `HeartbeatTick` | Config with `ElectionTick=10`, `HeartbeatTick=1` | 1. `cfg.PeerDialTimeout()`. **Expected:** Returns `1s + 10ms = 1.01s`. | Low |
| SV-10 | Default `Name` applied when empty | Verify `cfg.Name` defaults to `"omashu"` when not set | Config with `Name=""` | 1. `validate(false)`. 2. Check `cfg.Name`. **Expected:** `cfg.Name == "omashu"`. | Low |
| SV-11 | Nil `Logger` defaults to `zap.NewNop()` | Verify nil logger does not cause nil-pointer panics | Config with `Logger: nil` | 1. `NewBadger(ctx, cfg)` with nil logger. 2. Write and read. **Expected:** No panics; all log calls silently no-op. | High |

---

## 13. Performance & Load

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| PL-01 | 1,000 sequential writes within 30s | Verify sequential write throughput meets a minimum baseline | 3-node cluster | 1. Write `k-0000` to `k-0999` sequentially. 2. Record total duration. **Expected:** All 1,000 writes commit in < 30s (≥ 33 ops/sec). | High |
| PL-02 | 100 concurrent writes — no data loss | Verify parallel writes under the proposal-channel constraint complete without loss | 3-node cluster | 1. 100 goroutines each `Set` unique keys simultaneously. **Expected:** All 100 keys present after all goroutines complete. | High |
| PL-03 | Sustained 50 ops/sec write workload for 60s | Verify the cluster handles steady-state write load without degradation | 3-node cluster | 1. Issue 50 writes/sec via a rate-limited goroutine pool for 60s. **Expected:** p99 latency < `DefaultProposeTimeout`; no `ErrProposeTimeout` errors. | High |
| PL-04 | Large value (1 MB) write and read | Verify large payloads replicate and are retrievable without truncation | 3-node cluster | 1. `Set("big", make([]byte, 1<<20))`. 2. `Get("big")`. 3. Verify length and checksum. **Expected:** Returns 1 MB exactly; checksum matches. | Medium |
| PL-05 | `GetByPrefix` on 10,000 keys — returns all | Verify prefix scan scales to large key counts | Single-node cluster | 1. Write 10,000 keys under `"bulk:"`. 2. `GetByPrefix("bulk:")`. **Expected:** Map contains exactly 10,000 entries. | Medium |
| PL-06 | `IterateByPrefix` with 10,000 keys — full pagination | Verify cursor pagination traverses all keys without gaps or duplicates | Single-node cluster | 1. Write 10,000 `"page:XXXX"` keys. 2. Paginate with `limit=500`. **Expected:** 20 pages × 500 keys = 10,000 unique keys; no duplicates. | Medium |
| PL-07 | `BulkGet` for 500 keys — single read transaction | Verify `BulkGet` is more efficient than 500 individual `Get` calls | Single-node cluster, 500 keys | 1. Time 500× `Get`. 2. Time 1× `BulkGet(500 keys)`. **Expected:** `BulkGet` completes in less time. | Low |
| PL-08 | `MaxBatchSize=100` transaction — Raft entry size | Verify a 100-sub-command transaction does not exceed `MaxSizePerMsg` configured in `RaftConfig` | 3-node cluster; `MaxSizePerMsg=4096` | 1. `NewTransaction` with 100 small `Set` ops. 2. Check Raft message size in logs. **Expected:** Entries may be split by rafthttp but all 100 sub-commands eventually applied atomically. | Medium |
| PL-09 | Long-running workload stability — no goroutine leaks | Verify 30 minutes of steady writes does not accumulate leaked goroutines | 3-node cluster | 1. Baseline goroutine count at T=0. 2. Run 1 write/sec for 30m. 3. Check goroutine count at T=30m. **Expected:** Goroutine count stable (≤ 5% growth). | Medium |
| PL-10 | `TSO.cleanupCommittedTransactions` prevents unbounded growth | Verify `committedTxns` slice does not grow monotonically under sustained write load | Single-node cluster; 10,000 writes | 1. Monitor `len(tso.committedTxns)` over time. **Expected:** Length stays bounded as old readers complete and watermark advances. | High |

---

## 14. Chaos Engineering

| Test ID | Test Name | Objective | Preconditions | Steps / Validation | Priority |
|---------|-----------|-----------|---------------|--------------------|----------|
| CH-01 | Random leader kill during write storm | Verify no committed data is lost when the leader is killed under 50 concurrent writers | 3-node cluster; 50-goroutine writer pool | 1. Start writers. 2. Kill leader at random interval. 3. Verify all acknowledged writes exist on new leader. **Expected:** All writes that received nil return are present on new leader. | Critical |
| CH-02 | Network partition during `NewTransaction` commit | Verify a transaction in-flight during a network partition does not produce partial state | 3-node cluster | 1. Start `NewTransaction` with 20 sub-commands. 2. Partition leader from followers after `proposeAndWait` sends. **Expected:** Transaction either fully applied on all nodes or not applied on any. | Critical |
| CH-03 | Kill follower during snapshot transfer | Verify snapshot transfer restarts correctly when recipient is killed | 3-node cluster; snapshot in progress | 1. Kill follower receiving snapshot. 2. Restart follower. **Expected:** Leader retries snapshot; follower eventually catches up. | High |
| CH-04 | Kill leader during TSO `StartServing` | Verify killing the new leader while its TSO is initializing does not corrupt the TSO upper bound | 3-node cluster; leadership transition in progress | 1. Kill new leader during `StartServing`. 2. A third leader is elected. **Expected:** Third leader reads a consistent `_tso_last_timestamp`; no timestamp regression. | High |
| CH-05 | Two simultaneous follower failures — leader retains quorum | Verify a 5-node cluster survives 2 simultaneous follower crashes | 5-node cluster | 1. Kill 2 followers simultaneously. 2. Continue writes on leader. **Expected:** Leader maintains quorum (3/5); writes succeed; downed nodes catch up on restart. | High |
| CH-06 | Repeated leader elections under write load | Verify data consistency is maintained through 10 rapid leader changes while 20 writers are active | 3-node cluster | 1. Kill and restart the current leader 10 times at 5-second intervals. 2. After all elections, read back all acknowledged writes. **Expected:** All committed writes present; no duplicates or corruption. | Critical |
| CH-07 | `ConfChange` during active write workload | Verify adding a new node via `ConfChangeAddNode` while writes are in progress is safe | 3-node cluster; 20 concurrent writers | 1. Propose `ConfChangeAddNode` for node 4. 2. Simultaneously write 100 keys. **Expected:** Node 4 eventually receives snapshot/log; all 100 keys consistent. | High |
| CH-08 | Slow follower — leader accumulates pending entries | Verify the leader does not crash or lose data when one follower is extremely slow | 3-node cluster; follower with artificial 2s apply delay | 1. Write 200 entries. 2. Verify leader and fast follower commit all 200. 3. Resume slow follower. **Expected:** Slow follower catches up; all 3 nodes consistent. | Medium |
| CH-09 | Rapid `Close` after `NewDistributedBadger` | Verify calling `Close` immediately after startup does not panic or deadlock | Single-node cluster | 1. `NewDistributedBadger`. 2. Immediately `Close`. **Expected:** Returns without panic or goroutine leak. | High |
| CH-10 | Combined chaos: network partition + follower kill + write storm | Verify 3-node cluster survives a combined failure scenario | 3-node cluster; 50 concurrent writers | 1. Start writers. 2. After 5s: kill 1 follower AND partition leader from the other follower. 3. After 10s: heal partition and restart killed follower. 4. Stop writers. **Expected:** Cluster regains full health; acknowledged writes present; no split-brain or data corruption. | Critical |
| CH-11 | `DeleteByPrefix` on non-existent prefix — no-op | Verify deleting a prefix with no matching keys returns nil without Raft proposal | 3-node cluster | 1. `DeleteByPrefix(ctx, "nonexistent:")`. **Expected:** Returns nil; no Raft entry created; no side effects. | Medium |
| CH-12 | `Set` after `Close` panics or returns error | Verify calling `Set` on a closed `DistributedBadger` does not corrupt state | Single-node cluster | 1. `Close`. 2. `Set`. **Expected:** Returns an error or panics cleanly; no silent data corruption or goroutine starts. | Medium |

---

*Total: 158 test cases across 14 categories.*
