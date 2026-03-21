/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-FileCopyrightText: © 2026 Sachin S
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by Sachin S, 2026
 */

package omashu

import (
	"context"
	"sync"
	"time"

	"github.com/avatar31/omashu/utils"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
	"go.uber.org/zap"
)

// Reference:
// https://github.com/tikv/pd/wiki/Timestamp-Oracle
// https://www.pingcap.com/blog/how-an-open-source-distributed-newsql-database-delivers-time-services/
// https://github.com/tikv/pd/blob/master/pkg/tso/tso.go
// https://github.com/dgraph-io/badger/blob/main/txn.go

const (
	LOGICAL_BITS            = 18
	MAX_LOGICAL             = (1 << LOGICAL_BITS) - 1
	defaultUpperBoundWindow = 2 * time.Second

	persistentKey = "_tso_last_timestamp"
)

// timeStamp represents a hybrid logical clock timestamp with physical and logical components.
// The physical component is the current time in milliseconds and the logical component is a
// counter that increments when multiple timestamps are generated within the same millisecond.
// ```
// | Physical Time (ms) | Logical Counter |
// ```
//
// Typical layout (64 bits):
// ```
// 63                           18 17        0
// +------------------------------+-----------+
// | physical time (ms)           | logical   |
// +------------------------------+-----------+
// ```
//
// Example:
// ts = (physical_ms << 18) | logical
//
// This allows:
// * Physical time ordering
// * Multiple timestamps in the same millisecond
type timeStamp struct {
	physical int64
	logical  int64
}

func newEmptyTimeStamp() *timeStamp {
	return &timeStamp{physical: 0, logical: 0}
}

// Reset() sets the physical time to newTs and resets the logical counter to 0. It should be called
// when the physical time advances to ensure that the logical counter starts fresh for the new millisecond.
func (ts *timeStamp) Reset(newTs int64) {
	ts.physical = newTs
	ts.logical = 0
}

// Incr() increments the logical counter by 1. It should be called when multiple timestamps are
// generated within the same millisecond to ensure that each timestamp is unique.
// If the logical counter exceeds MAX_LOGICAL, it indicates that too many timestamps have been
// generated within the same millisecond and the system should wait for the next millisecond
// before generating more timestamps.
func (ts *timeStamp) Incr() {
	ts.logical += 1
}

// ExceedLogicalMax() checks if the logical counter has exceeded the maximum allowed value (MAX_LOGICAL).
func (ts *timeStamp) ExceedLogicalMax() bool {
	return ts.logical > MAX_LOGICAL
}

// Compose combines the physical and logical components of the timestamp into a single uint64 value.
func (ts *timeStamp) Compose() uint64 {
	return uint64((ts.physical << LOGICAL_BITS) | ts.logical)
}

// committedTxn represents a transaction that has been committed with a specific timestamp (ts) and a set of conflict keys.
type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[string]struct{}
}

// TSO (Timestamp Oracle) is responsible for generating unique, monotonically increasing timestamps
// for transactions. It maintains the current timestamp, a saved upper bound timestamp, and tracks
// in-flight read timestamps to manage transaction visibility and conflict detection.
// The TSO ensures that transactions are assigned commit timestamps in a way that respects their read
// timestamps and any conflicts with previously committed transactions. It also handles the persistence
// of the upper bound timestamp to ensure durability across restarts and provides mechanisms for
// cleaning up old committed transactions that are no longer relevant for conflict detection.
type TSO struct {
	// current is the current timestamp that is being allocated.
	// It is updated whenever a new timestamp is allocated or when the physical time advances.
	current *timeStamp

	// saved is the upper bound timestamp that has been persisted to the database. To ensure that
	// the next elected Leader can successfully calibrate the time after the current Leader is down.
	saved *timeStamp

	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeChLock sync.Mutex

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	txnMark *y.WaterMark

	// Used to determine which versions can be permanently discarded during compaction.
	readMark *y.WaterMark

	// readers tracks in-flight read timestamps so that DoneRead() can advance the readMark watermark.
	readers       map[uint64]struct{}
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer       *z.Closer
	stopNotifier chan struct{}

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn

	db  *DistributedBadger
	log *zap.Logger
	mu  sync.Mutex
}

// newTSO() initializes a new TSO instance. It loads the last saved upper bound timestamp from the database,
// calibrates the TSO clock if necessary, and starts a background goroutine to periodically update the upper bound timestamp.
// It also initializes the read and transaction watermarks based on the current timestamp.
func newTSO(ctx context.Context, db *DistributedBadger, log *zap.Logger) (*TSO, error) {
	tso := &TSO{
		current: newEmptyTimeStamp(),
		saved:   newEmptyTimeStamp(),

		readMark:     &y.WaterMark{Name: "badger.PendingReads"},
		txnMark:      &y.WaterMark{Name: "badger.TxnTimestamp"},
		closer:       z.NewCloser(2),
		stopNotifier: make(chan struct{}),

		readers: make(map[uint64]struct{}),

		db:  db,
		log: log.With(zap.String("sub_module", "tso")),
	}
	tso.readMark.Init(tso.closer)
	tso.txnMark.Init(tso.closer)

	val, found, err := db.Get(ctx, persistentKey)
	if err != nil {
		tso.log.Error("Failed to load last TSO from DB", zap.Error(err))
		return nil, err
	}

	if found {
		last := int64(utils.BytesToUint64(val))
		now := time.Now().UnixMilli()
		if now <= last {
			now = last + 1
		}

		tso.current.Reset(now)
		tso.calibrate()
	} else {
		tso.current.Reset(time.Now().UnixMilli())
	}

	go tso.windowUpdator(ctx)

	lastIndex := tso.current.Compose()
	tso.readMark.SetDoneUntil(lastIndex)
	tso.txnMark.SetDoneUntil(lastIndex)

	return tso, nil
}

// windowUpdator() is a background goroutine that periodically checks if the current physical time
// plus a predefined upper bound window exceeds the saved upper bound timestamp.
func (tso *TSO) windowUpdator(ctx context.Context) {
	if err := tso.UpdateWindow(ctx); err != nil {
		tso.log.Error("Failed to update TSO window even after multiple retries", zap.Error(err))
	}

	tso.log.Info("TSO window updated successfully, starting ticker for periodic updates with interval",
		zap.Duration("interval", defaultUpperBoundWindow))
	ticker := time.NewTicker(defaultUpperBoundWindow)

	for {
		select {
		case <-ticker.C:
			if err := tso.UpdateWindow(ctx); err != nil {
				tso.log.Error("Failed to update TSO window", zap.Error(err))
			}
		case <-tso.stopNotifier:
			tso.log.Info("Received stop signal, stopping TSO window updator")
			ticker.Stop()
			return
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// calibrate() checks if the current physical time is behind the TSO's current physical timestamp.
// If it is, it means that the system clock has moved backwards, which can lead to non-monotonic
// timestamps being generated. To prevent this, the TSO will wait until the system clock catches
// up to the current physical timestamp before allowing any new timestamps to be allocated.
// This ensures that all generated timestamps are monotonically increasing and consistent with
// the actual passage of time.
func (tso *TSO) calibrate() {
	now := time.Now().UnixMilli()
	if now < tso.current.physical {
		wait := time.Duration(tso.current.physical-now) * time.Millisecond
		tso.log.Warn("Fencing TSO clock", zap.Int64("wait_ms", int64(wait/time.Millisecond)))
		time.Sleep(wait)
	}
}

// allocate() generates a new timestamp. It first checks if the physical time has advanced since
// the last allocated timestamp and resets the logical counter if it has.
func (tso *TSO) allocate() uint64 { // Must be called under tso.mu.Lock
	now := time.Now().UnixMilli()

	// Step 1: advance physical time if possible
	if now > tso.current.physical {
		tso.current.Reset(now)
	}

	// Step 2: Enforce upper bound safety. We must NOT generate timestamps beyond persisted window
	if tso.current.physical >= tso.saved.physical {
		tso.log.Error("TSO reached upper bound, waiting for window update",
			zap.Int64("current", tso.current.physical), zap.Int64("saved", tso.saved.physical))
		startTime := time.Now()
		for {
			// TODO: P1: Should we have a timeout here and return error if window is not updated for too long?
			// This could be a sign of a problem with the TSO or the database.
			now = time.Now().UnixMilli()
			if now < tso.saved.physical {
				tso.current.Reset(now)
				break
			}
			// wait for window to be updated
			time.Sleep(time.Millisecond / 2)
		}
		tso.log.Info("Window updated, resuming timestamp allocation", zap.Duration("wait_time", time.Since(startTime)))
	}

	// Step 3: handle logical overflow
	if tso.current.ExceedLogicalMax() {
		tso.log.Warn("TSO logical overflow, waiting for physical time to advance")
		startTime := time.Now()
		for {
			now = time.Now().UnixMilli()
			if now > tso.current.physical {
				tso.current.Reset(now)
				break
			}
			// wait for physical time to advance
			time.Sleep(time.Millisecond / 2)
		}
		tso.log.Info("Physical time advanced, resuming timestamp allocation", zap.Duration("wait_time", time.Since(startTime)))
	}

	// Step 4: allocate timestamp
	tso.current.Incr()
	return tso.current.Compose()
}

// UpdateWindow() checks if the current physical time plus a predefined upper bound window exceeds the
// saved upper bound timestamp. If it does, it updates the upper bound timestamp in the database and
// resets the saved timestamp to the new upper bound.
// We are using a window to reduce the frequency of updates to the database, which can be expensive.
// The TSO will only update the upper bound timestamp when the current time approaches the saved upper
// bound, ensuring that there is always a buffer of time for generating new timestamps without needing
// to immediately persist a new upper bound.
func (tso *TSO) UpdateWindow(ctx context.Context) error {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	upperBound := tso.current.physical + defaultUpperBoundWindow.Milliseconds()
	if upperBound <= tso.saved.physical {
		// no need to update
		return nil
	}

	// persist the upper bound to DB
	// TODO: P1: Use retry on failure due to transaction conflicts
	err := tso.db.NewTransaction(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.Set(ctx, persistentKey, utils.Uint64ToBytes(uint64(upperBound)))
	})
	if err != nil {
		tso.log.Error("Failed to update TSO upper bound in DB", zap.Error(err))
		return err
	}

	tso.saved.Reset(upperBound)
	tso.log.Info("Updated persistent key to new upperbound", zap.Int64("upperbound", upperBound))
	return nil
}

// CurrentReadTs() returns the current read timestamp, which is the composed value of the current timeStamp.
func (tso *TSO) CurrentReadTs() uint64 {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	return tso.currentReadTs()
}

func (tso *TSO) currentReadTs() uint64 {
	return tso.current.Compose()
}

// ReadTs() returns a read timestamp for a transaction. It marks the read timestamp in the readMark watermark and
// waits for all transactions that have been assigned a commit timestamp but have not yet completed their write
// process to be visible. This ensures that any transaction that reads at this timestamp will see all committed
// transactions up to this point, maintaining consistency.
func (tso *TSO) ReadTs(ctx context.Context) (uint64, error) {
	var readTs uint64
	tso.mu.Lock()
	readTs = tso.currentReadTs()
	tso.readMark.Begin(readTs)

	// Record readTs in the readers map so that DoneRead() can later call
	// readMark.Done(). Without this, the readMark watermark never advances,
	// cleanupCommittedTransactions() never fires, and committedTxns leaks.
	tso.readers[readTs] = struct{}{}
	tso.mu.Unlock()

	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		tso.log.Debug("TSO ReadTs wait time", zap.Duration("elapsed", elapsed), zap.Uint64("readTs", readTs))
	}()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	err := tso.txnMark.WaitForMark(ctx, readTs)
	if err != nil {
		tso.readMark.Done(readTs)
		tso.log.Error("Failed to wait for txn mark in ReadTs", zap.Error(err), zap.Uint64("readTs", readTs))
		return 0, err
	}
	return readTs, nil
}

// DiscardAt() returns the timestamp until which versions can be safely discarded during compaction.
// It is determined by the readMark watermark, which tracks the oldest in-flight read timestamp.
// Any versions with timestamps less than or equal to this value can be discarded without affecting
// any active transactions, as they will not be visible to any ongoing reads.
func (tso *TSO) DiscardAt() uint64 {
	// TODO: P0: Implement this
	// This will works only in leader node. Followers should query leader for discardTs
	// Either we have to maintain the readMark watermark in followers as well and return min(allnodes)
	// or we can have followers query the leader for the discardTs.
	return tso.readMark.DoneUntil()
}

// newCommitTs() generates a new commit timestamp for a transaction. It first checks for conflicts
// with previously committed transactions based on the transaction's read timestamp and conflict keys.
// If there are conflicts, it returns 0 and a boolean indicating that a conflict was detected.
// If there are no conflicts, it marks the transaction's read timestamp as done, cleans up old committed
// transactions, allocates a new commit timestamp, and records the committed transaction with
// its conflict keys for future conflict detection.
func (tso *TSO) newCommitTs(txn *Txn) (uint64, bool) {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	if tso.hasConflict(txn) {
		return 0, true
	}

	tso.doneRead(txn.readTs)
	tso.cleanupCommittedTransactions()

	ts := tso.allocate()
	tso.txnMark.Begin(ts)
	tso.committedTxns = append(tso.committedTxns, committedTxn{
		ts:           ts,
		conflictKeys: txn.conflictKeys,
	})

	return ts, false
}

// Commit() is a helper function that generates a commit timestamp for a transaction and executes 
// the provided function with that timestamp.
func (tso *TSO) Commit(txn *Txn, fn func(ts uint64) error) error {
	ts, conflict := tso.newCommitTs(txn)
    if conflict {
        return badger.ErrConflict
    }

    defer tso.DoneCommit(ts)
	return fn(ts)
}

// hasConflict() checks if the given transaction has any conflicts with previously committed transactions.
// A conflict occurs if there is a committed transaction with a commit timestamp greater than the
// transaction's read timestamp that has written to any of the keys that the transaction has read.
// This ensures that the transaction will not read stale data and maintains consistency.
// TODO: P1: O(N²) worst-case
// Improve later:
// key → latest commit ts map
// or bloom filters
func (tso *TSO) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}

	for _, committedTxn := range tso.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it
		if committedTxn.ts <= txn.readTs {
			continue
		}

		for _, key := range txn.reads {
			if _, has := committedTxn.conflictKeys[key]; has {
				return true
			}
		}
	}
	return false
}

// cleanupCommittedTransactions() removes committed transactions from the committedTxns slice that have
// commit timestamps less than or equal to the maximum read timestamp of any in-flight transaction.
// TODO: P1: Possibility of Memory growth risk
// Improve:
// Use bounded structure
// Or index by timestamp
// Or compact conflict keys (hash/fingerprint)
func (tso *TSO) cleanupCommittedTransactions() { // Must be called under tso.mu.Lock
	maxReadTs := tso.readMark.DoneUntil()

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == tso.lastCleanupTs {
		return
	}

	tso.lastCleanupTs = maxReadTs

	tmp := tso.committedTxns[:0]
	for _, txn := range tso.committedTxns {
		if txn.ts <= maxReadTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	tso.committedTxns = tmp
}

// DoneRead() marks the given read timestamp as done in the readMark watermark and removes it from
// the readers map. This should be called when a transaction that has read at this timestamp has
// completed, allowing the TSO to advance the read watermark and potentially clean up old committed
// transactions.
func (tso *TSO) DoneRead(readTs uint64) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.doneRead(readTs)
}

// doneRead() is the internal implementation of DoneRead that assumes the caller has already
// acquired the tso.mu lock.
func (tso *TSO) doneRead(readTs uint64) {
	if _, ok := tso.readers[readTs]; ok {
		tso.readMark.Done(readTs)
		delete(tso.readers, readTs)
	}
}

// DoneCommit() marks the given commit timestamp as done in the txnMark watermark. This should be
// called when a transaction that has been assigned this commit timestamp has completed its write
// process, allowing the TSO to advance the transaction watermark and ensure visibility of committed
// transactions to new reads.
func (tso *TSO) DoneCommit(cts uint64) {
	tso.txnMark.Done(cts)
}

// Close() stops the TSO's watermarks and any background goroutines. It should be called when the
// TSO is no longer needed to clean up resources.
func (tso *TSO) Close() {
	close(tso.stopNotifier)
	tso.closer.Done()
}
