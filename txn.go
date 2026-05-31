/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/types"
)

// TxnManager creates and tracks transactions for a [DistributedBadger] node.
// It is only active when the node is the Raft leader; the TSO must be
// serving before any transaction can be started.
type TxnManager struct {
	db  *DistributedBadger
	tso *TSO
	log *zap.Logger
}

// newTxnManager creates a TxnManager for db backed by tso.
func newTxnManager(db *DistributedBadger, tso *TSO, log *zap.Logger) *TxnManager {
	return &TxnManager{db: db, tso: tso, log: log}
}

// BeginTxn opens a new transaction. It requests a read timestamp from the
// TSO, which blocks until all previously committed writes are visible at
// that timestamp. Set update to true for read-write transactions; false
// returns a read-only transaction whose write methods return
// badger.ErrReadOnlyTxn.
func (tm *TxnManager) BeginTxn(ctx context.Context, update bool) (*Txn, error) {
	readTs, err := tm.tso.ReadTs(ctx)
	if err != nil {
		tm.log.Error("Error getting read timestamp from TSO", zap.Error(err))
		return nil, err
	}

	cmd := types.NewTransactionCommand(ctx)
	cmd.ReadTs = readTs
	return &Txn{
		id:     uuid.New().String(),
		readTs: readTs,
		tso:    tm.tso,
		db:     tm.db,
		update: update,
		cmd:    cmd,
		log:    tm.log,

		conflictKeys: make(map[string]struct{}),
		writes:       make([]string, 0),
		reads:        make([]string, 0),
	}, nil
}

// Txn is a read-write transaction with snapshot isolation. Reads are served
// from the MVCC snapshot at readTs; writes are buffered as sub-commands on
// cmd and only become visible after Commit successfully proposes them to
// Raft at commitTs. Txn is not safe for concurrent use.
type Txn struct {
	id  string
	cmd *types.Command
	db  *DistributedBadger
	tso *TSO
	log *zap.Logger

	readTs   uint64
	commitTs uint64

	reads     []string
	readsLock sync.Mutex

	conflictKeys map[string]struct{}
	writes       []string

	update    bool
	discarded bool
}

// addReadKey records key as having been read during this transaction.
// Called by read helpers so the TSO can check for write-read conflicts at
// commit time. Safe to call from multiple goroutines.
func (txn *Txn) addReadKey(key string) {
	txn.readsLock.Lock()
	defer txn.readsLock.Unlock()
	txn.reads = append(txn.reads, key)
}

// AddWriteKey registers key as a write participant without adding a
// sub-command. Used internally by validate and by callers that must track
// a write for conflict detection before the command is constructed.
func (txn *Txn) AddWriteKey(key string) {
	txn.writes = append(txn.writes, key)
}

// Commit requests a commit timestamp from the TSO, verifies there are no
// write-read conflicts with concurrent transactions, stamps the buffered
// command with the commit timestamp, and returns it ready for Raft
// proposal. Returns (nil, nil) when the transaction has no writes.
// Always calls Discard before returning.
func (txn *Txn) Commit() (*types.Command, error) {
	if txn.discarded {
		return nil, badger.ErrDiscardedTxn
	}

	defer txn.Discard()

	if len(txn.writes) == 0 && len(txn.cmd.SubCommands) == 0 {
		return nil, nil
	}

	if len(txn.cmd.SubCommands) > MaxBatchSize {
		return nil, ErrBatchTooBig
	}

	orc := txn.tso

	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel. So, we
	// acquire a writeChLock before getting a commit timestamp, and only release
	// it after pushing the entries to it.
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()

	err := orc.Commit(txn, func(commitTs uint64) error {
		txn.cmd.CommitTs = commitTs
		txn.commitTs = commitTs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return txn.cmd, nil
}

// Discard marks the transaction as done and advances the TSO readMark
// watermark for this transaction's readTs, allowing the TSO to clean up
// committed transactions that are no longer needed for conflict detection.
// Discard is idempotent and safe to call after Commit.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}

	txn.discarded = true
	txn.tso.DoneRead(txn.readTs)
}

// DB Ops

// IncrBy buffers an INCR_BY sub-command that increments the counter at key
// by delta when the transaction commits.
func (txn *Txn) IncrBy(ctx context.Context, key string, delta uint64) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewIncrByCommand(ctx, key, delta))
	return nil
}

// DecrBy buffers a DECR_BY sub-command that decrements the counter at key
// by delta when the transaction commits.
func (txn *Txn) DecrBy(ctx context.Context, key string, delta uint64) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewDecrByCommand(ctx, key, delta))
	return nil
}

// Set buffers a SET sub-command that stores value at key (with optional TTL)
// when the transaction commits.
func (txn *Txn) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewSetCommand(ctx, key, value, ttl...))
	return nil
}

// Delete buffers a DELETE sub-command that removes key when the transaction
// commits. A missing key is a no-op.
func (txn *Txn) Delete(ctx context.Context, key string) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewDeleteCommand(ctx, key))
	return nil
}

// DeleteByPrefix buffers a DELETE_BY_PREFIX sub-command that removes all
// keys matching prefix when the transaction commits. It resolves the current
// set of matching keys immediately (at readTs) for conflict tracking.
func (txn *Txn) DeleteByPrefix(ctx context.Context, prefix string) error {
	keys, err := txn.db.GetKeysByPrefixWithTxn(ctx, txn, prefix)
	if err != nil {
		return err
	}

	for _, key := range keys {
		err := txn.validate(key)
		if err != nil {
			return err
		}
	}

	txn.cmd.AddSubCommand(types.NewDeleteByPrefixCommand(ctx, prefix))
	return nil
}

// UpdateJson buffers an UPDATE sub-command that JSON-merges delta into the
// existing value at key when the transaction commits.
func (txn *Txn) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	b, err := json.Marshal(delta)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewUpdateCommand(ctx, key, b, types.UpdateDeltaType_JSON, "", ttl...))
	return nil
}

// UpdateProtobuf buffers an UPDATE sub-command that performs a field-level
// Protobuf merge into the existing value at key when the transaction commits.
// Returns [ErrUnknownProtoMsg] if deltaProtoMsg's type is not registered in
// the global descriptor store.
func (txn *Txn) UpdateProtobuf(ctx context.Context, key string, deltaProtoMsg proto.Message,
	ttl ...time.Duration) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	delta, err := proto.Marshal(deltaProtoMsg)
	if err != nil {
		return err
	}

	msgName := string(proto.MessageName(deltaProtoMsg))
	valid, err := types.GetProtoDescriptorStore().IsValidMessage(msgName)
	if err != nil {
		txn.log.Error("Error validating protobuf message", zap.String("messageName", msgName), zap.Error(err))
		return ErrUnknownProtoMsg
	}

	if !valid {
		return ErrUnknownProtoMsg
	}

	subCmd := types.NewUpdateCommand(ctx, key, delta, types.UpdateDeltaType_PROTOBUF, msgName, ttl...)
	txn.cmd.AddSubCommand(subCmd)
	return nil
}

// validate checks that the transaction is still usable and that key is
// non-empty. On success it records key in the conflict set and the write
// list for TSO conflict detection at commit time.
func (txn *Txn) validate(key string) error {
	switch {
	case !txn.update:
		return badger.ErrReadOnlyTxn
	case txn.discarded:
		return badger.ErrDiscardedTxn
	case len(key) == 0:
		return badger.ErrEmptyKey
	}

	txn.conflictKeys[key] = struct{}{}
	txn.writes = append(txn.writes, key)
	return nil
}
