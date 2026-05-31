/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// OTxn constrains the transaction type parameter used by [Database],
// [DBReadOps], [DBWriteOps], and [DBDeleteOps]. A [*Txn] is used with
// [DistributedBadger]; a [*badger.Txn] is used with [Badger].
type OTxn interface {
	*Txn | *badger.Txn
}

// DBReadOps is the read-only subset of the [Database] interface. All
// methods are safe to call on any cluster node regardless of Raft
// leadership.
type DBReadOps[T OTxn] interface {
	Count(ctx context.Context, prefix string) int
	Exists(ctx context.Context, key string) bool
	HasChild(ctx context.Context, prefix string) bool

	Get(ctx context.Context, key string) ([]byte, bool, error)
	GetWithTxn(ctx context.Context, txn T, key string) ([]byte, bool, error)

	GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error)
	GetByPrefixWithTxn(ctx context.Context, txn T, prefix string) (map[string][]byte, error)

	GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error)
	GetKeysByPrefixWithTxn(ctx context.Context, txn T, prefix string) ([]string, error)

	BulkGet(ctx context.Context, keys []string) (map[string][]byte, error)
	IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int, process func(k, v []byte) bool) (string, error)
}

// DBWriteOps is the write subset of the [Database] interface. On a
// [DistributedBadger], each method returns [ErrNotLeader] when called
// on a non-leader node.
type DBWriteOps[T OTxn] interface {
	DecrBy(ctx context.Context, key string, delta uint64) error
	DecrByWithTxn(ctx context.Context, txn T, key string, delta uint64) error

	IncrBy(ctx context.Context, key string, delta uint64) error
	IncrByWithTxn(ctx context.Context, txn T, key string, delta uint64) error

	Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error
	SetWithTxn(ctx context.Context, txn T, key string, value []byte, ttl ...time.Duration) error

	UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error
	UpdateJsonWithTxn(ctx context.Context, txn T, key string, delta map[string]any, ttl ...time.Duration) error

	UpdateProtobuf(ctx context.Context, key string, delta proto.Message, ttl ...time.Duration) error
	UpdateProtobufWithTxn(ctx context.Context, txn T, key string, delta proto.Message, ttl ...time.Duration) error
}

// DBDeleteOps is the delete subset of the [Database] interface. On a
// [DistributedBadger], each method returns [ErrNotLeader] when called
// on a non-leader node.
type DBDeleteOps[T OTxn] interface {
	Delete(ctx context.Context, key string) error
	DeleteWithTxn(ctx context.Context, txn T, key string) error

	DeleteByPrefix(ctx context.Context, prefix string) error
	DeleteByPrefixWithTxn(ctx context.Context, txn T, prefix string) error
}

// Database is the unified key-value store interface combining reads,
// writes, and deletes. T is constrained to [*Txn] (for
// [DistributedBadger]) or [*badger.Txn] (for [Badger]), making
// application code portable between modes without any changes.
type Database[T OTxn] interface {
	DBReadOps[T]
	DBWriteOps[T]
	DBDeleteOps[T]

	GetBadger() *badger.DB
	NewTransaction(ctx context.Context, performOps func(context.Context, T) error) error
	Close(ctx context.Context)
}

// DistributedBadger implements [Database][*Txn] for a replicated
// multi-node cluster. Writes and deletes are serialised through the
// Raft consensus log; reads use the linearizable ReadIndex protocol.
// Only the current Raft leader accepts write and delete operations;
// all other nodes return [ErrNotLeader].
type DistributedBadger struct {
	proposals            sync.Map
	leaderChangeNotifier chan struct{}
	muLCNotifier         sync.RWMutex 	// TODO: P0: Do we need this mutex?

	fsm  *FSM
	node *Node
	tso  *TSO
	tm   *TxnManager
	log  *zap.Logger

	onLeaderChangeHook func(prevLeader, newLeader uint64)
	onRemovedSelfHook  func()
}

// Badger implements [Database][*badger.Txn] for a single-node embedded
// store. It wraps BadgerDB opened in managed mode so the TSO can
// control MVCC timestamps directly. Use this for development, testing,
// or single-instance deployments that do not require replication.
type Badger struct {
	managed        bool
	path           string
	gcInterval     time.Duration
	gcDiscardRatio float64
	db             *badger.DB
	oracle         *TSO
	log            *zap.Logger
}

// NewBadger creates and opens a single-node [Badger] store using cfg.
// BadgerDB is opened in managed mode to allow direct MVCC timestamp
// control. Returns an error if cfg fails validation or the database
// cannot be opened.
func NewBadger(ctx context.Context, cfg *Config) (*Badger, error) {
	if err := cfg.validate(false); err != nil {
		return nil, err
	}

	return initBadger(ctx, false, cfg)
}

// NewDistributedBadger creates and starts a replicated multi-node store
// using cfg. It opens the underlying BadgerDB, initialises the Raft
// node, and starts the peer transport. The TSO does not begin serving
// timestamps until this node wins a leader election. Returns an error
// if cfg is invalid or any subsystem fails to start.
func NewDistributedBadger(ctx context.Context, cfg *Config) (*DistributedBadger, error) {
	if err := cfg.validate(true); err != nil {
		return nil, err
	}

	db, err := initBadger(ctx, true, cfg)
	if err != nil {
		return nil, err
	}

	return initDistributed(ctx, db, cfg)
}
