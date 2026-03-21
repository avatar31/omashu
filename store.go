package omashu

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type OTxn interface {
	*Txn | *badger.Txn
}

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

type DBDeleteOps[T OTxn] interface {
	Delete(ctx context.Context, key string) error
	DeleteWithTxn(ctx context.Context, txn T, key string) error

	DeleteByPrefix(ctx context.Context, prefix string) error
	DeleteByPrefixWithTxn(ctx context.Context, txn T, prefix string) error
}

type Database[T OTxn] interface {
	DBReadOps[T]
	DBWriteOps[T]
	DBDeleteOps[T]

	GetBadger() *badger.DB
	NewTransaction(ctx context.Context, performOps func(context.Context, T) error) error
	Close(ctx context.Context)
}

type DistributedBadger struct {
	proposals            sync.Map
	leaderChangeNotifier chan struct{}

	fsm  *FSM
	node *Node
	tso  *TSO
	tm   *TxnManager
	log  *zap.Logger

	// mu guards tso, tm, and leaderChangeNotifier
	mu sync.RWMutex

	onLeaderChangeHook func(prevLeader, newLeader uint64)
	onRemovedSelfHook  func()
}

type Badger struct {
	managed        bool
	path           string
	gcInterval     time.Duration
	gcDiscardRatio float64
	db             *badger.DB
	oracle         *TSO
	log            *zap.Logger
}

func NewBadger(ctx context.Context, cfg *Config) (*Badger, error) {
	if err := cfg.validate(false); err != nil {
		return nil, err
	}

	return initBadger(ctx, false, cfg)
}

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
