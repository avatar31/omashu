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

type TxnManager struct {
	db  *DistributedBadger
	tso *TSO
	log *zap.Logger
}

func newTxnManager(db *DistributedBadger, tso *TSO, log *zap.Logger) *TxnManager {
	return &TxnManager{db: db, tso: tso, log: log}
}

func (tm *TxnManager) BeginTxn(ctx context.Context, update bool) (*Txn, error) {
	readTs, err := tm.tso.ReadTs(ctx)
	if err != nil {
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

func (txn *Txn) addReadKey(key string) {
	txn.reads = append(txn.reads, key)
}

func (txn *Txn) AddWriteKey(key string) {
	txn.writes = append(txn.writes, key)
}

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

func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}

	txn.discarded = true
	txn.tso.DoneRead(txn.readTs)
}

// DB Ops

func (txn *Txn) IncrBy(ctx context.Context, key string, delta uint64) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewIncrByCommand(ctx, key, delta))
	return nil
}

func (txn *Txn) DecrBy(ctx context.Context, key string, delta uint64) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewDecrByCommand(ctx, key, delta))
	return nil
}

func (txn *Txn) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewSetCommand(ctx, key, value, ttl...))
	return nil
}

func (txn *Txn) Delete(ctx context.Context, key string) error {
	err := txn.validate(key)
	if err != nil {
		return err
	}

	txn.cmd.AddSubCommand(types.NewDeleteCommand(ctx, key))
	return nil
}

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
