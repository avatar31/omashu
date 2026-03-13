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
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
	"go.uber.org/zap"
)

// https://github.com/tikv/pd/wiki/Timestamp-Oracle

const (
	LOGICAL_BITS            = 18
	MAX_LOGICAL             = (1 << LOGICAL_BITS) - 1
	defaultUpperBoundWindow = 2 * time.Second

	persistentKey = "_tso_last_timestamp"
)

type timeStamp struct {
	sync.RWMutex
	physical int64
	logical  int64
}

func newEmptyTimeStamp() *timeStamp {
	return &timeStamp{physical: 0, logical: 0}
}

func (ts *timeStamp) Reset(newTs int64) {
	ts.Lock()
	defer ts.Unlock()
	ts.physical = newTs
	ts.logical = 0
}

func (ts *timeStamp) Incr() {
	ts.Lock()
	defer ts.Unlock()
	ts.logical += 1
}

func (ts *timeStamp) ExceedLogicalMax() bool {
	ts.RLock()
	defer ts.RUnlock()
	return ts.logical > MAX_LOGICAL
}

func (ts *timeStamp) Compose() uint64 {
	ts.RLock()
	defer ts.RUnlock()
	return uint64((ts.physical << LOGICAL_BITS) | ts.logical)
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[string]struct{}
}

type TSO struct {
	current *timeStamp
	saved   *timeStamp

	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeChLock sync.Mutex

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	txnMark *y.WaterMark

	// Used to determine which versions can be permanently discarded during compaction.
	readMark *y.WaterMark

	readers       map[uint64]struct{}
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer *z.Closer

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn

	db  *DBStore
	log *zap.Logger
	mu  sync.Mutex
}

func NewTSO(ctx context.Context, db *DBStore, logger *zap.Logger) (*TSO, error) {
	tso := &TSO{
		current: newEmptyTimeStamp(),
		saved:   newEmptyTimeStamp(),

		readMark: &y.WaterMark{Name: "badger.PendingReads"},
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"},
		closer:   z.NewCloser(2),

		db:  db,
		log: logger,
	}
	tso.readMark.Init(tso.closer)
	tso.txnMark.Init(tso.closer)

	val, found, err := db.Get(ctx, persistentKey)
	if err != nil {
		tso.log.Error("Failed to load last TSO from DB", zap.Error(err))
		return nil, err
	}

	if found {
		tso.current.Reset(int64(utils.BytesToUint64(val)))
		tso.calibrate()
	} else {
		tso.current.Reset(time.Now().UnixMilli())
	}

	// TODO: P0: Re-enable TSO window update
	// if err := tso.UpdateWindow(ctx); err != nil {
	// 	return nil, err
	// }

	lastIndex := tso.current.Compose()
	tso.readMark.SetDoneUntil(lastIndex)
	tso.txnMark.SetDoneUntil(lastIndex)

	return tso, nil
}

func (tso *TSO) calibrate() {
	now := time.Now().UnixMilli()
	if now < tso.current.physical {
		wait := time.Duration(tso.current.physical-now) * time.Millisecond
		tso.log.Warn("Fencing TSO clock", zap.Int64("wait_ms", int64(wait/time.Millisecond)))
		time.Sleep(wait)
	}
}

func (tso *TSO) allocate() *timeStamp {
	now := time.Now().UnixMilli()

	// Step 1: advance physical time if possible
	if now > tso.current.physical {
		tso.current.Reset(now)
	}

	// Step 2: handle logical overflow
	if tso.current.ExceedLogicalMax() {
		// need to wait for current physical millisecond
		wait := time.Duration(1) * time.Millisecond
		tso.log.Warn("TSO logical overflow, waiting for current millisecond",
			zap.Int64("wait_ms", int64(wait/time.Millisecond)))
		time.Sleep(wait)
		tso.current.Reset(time.Now().UnixMilli())
	}

	// Step 3: allocate timestamp
	tso.current.Incr()
	return tso.current
}

func (tso *TSO) UpdateWindow(ctx context.Context) error {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	upperBound := tso.current.physical + defaultUpperBoundWindow.Milliseconds()
	if upperBound <= tso.saved.physical {
		// no need to update
		return nil
	}

	// persist the upper bound to DB
	err := tso.db.NewTransaction(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.IncrBy(ctx, persistentKey, uint64(upperBound))
	})
	if err != nil {
		tso.log.Error("Failed to update TSO upper bound in DB", zap.Error(err))
		return err
	}

	tso.saved.physical = upperBound
	return nil
}

func (tso *TSO) CurrentReadTs() uint64 {
	return tso.current.Compose()
}

func (tso *TSO) ReadTs(ctx context.Context) uint64 {
	var readTs uint64
	tso.mu.Lock()
	readTs = tso.CurrentReadTs()
	tso.readMark.Begin(readTs)
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
		tso.log.Panic("Failed to wait for txn mark in ReadTs", zap.Error(err), zap.Uint64("readTs", readTs))
	}
	return readTs
}

func (tso *TSO) DiscardAt() uint64 {
	return tso.readMark.DoneUntil()
}

func (tso *TSO) NewCommitTs(txn *Txn) (uint64, bool) {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	if tso.hasConflict(txn) {
		return 0, true
	}

	tso.doneRead(txn.readTs)
	tso.cleanupCommittedTransactions()

	ts := tso.allocate().Compose()
	tso.txnMark.Begin(ts)
	tso.committedTxns = append(tso.committedTxns, committedTxn{
		ts:           ts,
		conflictKeys: txn.conflictKeys,
	})

	return ts, false
}

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

func (tso *TSO) DoneRead(readTs uint64) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.doneRead(readTs)
}

func (tso *TSO) doneRead(readTs uint64) {
	if _, ok := tso.readers[readTs]; ok {
		tso.readMark.Done(readTs)
		delete(tso.readers, readTs)
	}
}

func (tso *TSO) doneCommit(cts uint64) {
	tso.txnMark.Done(cts)
}

func (tso *TSO) Close() {
	tso.closer.Done()
}
