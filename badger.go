package omashu

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/avatar31/omashu/types"
	"github.com/avatar31/omashu/utils"
)

type Database interface {
	BulkGet(ctx context.Context, keys []string) (map[string][]byte, error)
	BatchWrite(ctx context.Context, ops []*types.Command) error
	BatchWriteWithTxn(ctx context.Context, txn *badger.Txn, ops []*types.Command) error
	Count(ctx context.Context, prefix string) int
	DecrBy(ctx context.Context, key string, delta uint64) (uint64, error)
	DecrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) (uint64, error)
	Delete(ctx context.Context, key string) error
	DeleteWithTxn(ctx context.Context, txn *badger.Txn, key string) error
	DeleteByPrefix(ctx context.Context, prefix string) error
	DeleteByPrefixWithTxn(ctx context.Context, txn *badger.Txn, prefix string)
	Exists(ctx context.Context, key string) bool
	GetBadgerInstance() *badger.DB
	Get(ctx context.Context, key string) ([]byte, bool, error)
	GetAt(ctx context.Context, key string, readTs uint64) ([]byte, bool, error)
	GetWithTxn(ctx context.Context, txn *badger.Txn, key string) ([]byte, bool, error)
	GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error)
	GetByPrefixWithTxn(ctx context.Context, txn *badger.Txn, prefix string) (map[string][]byte, error)
	GetByPrefixAt(ctx context.Context, prefix string, readTs uint64) (map[string][]byte, error)
	GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error)
	GetKeysByPrefixAt(ctx context.Context, prefix string, readTs uint64) ([]string, error)
	GetKeysByPrefixWithTxn(ctx context.Context, txn *badger.Txn, prefix string) ([]string, error)
	HasChild(ctx context.Context, prefix string) bool
	IncrBy(ctx context.Context, key string, delta uint64) (uint64, error)
	IncrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) (uint64, error)
	IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int, process func(k, v []byte) bool) (string, error)
	NewTransaction(ctx context.Context, readOnly bool, performOps func(context.Context, *badger.Txn) error) error
	NewTransactionAt(ctx context.Context, readTs, commitTs uint64,
		performOps func(context.Context, *badger.Txn) error) error
	Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error
	SetWithTxn(ctx context.Context, txn *badger.Txn, key string, value []byte, ttl ...time.Duration) error
	UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error
	UpdateJsonWithTxn(ctx context.Context, txn *badger.Txn, key string, delta map[string]any, ttl ...time.Duration) error
	UpdateProtobuf(ctx context.Context, key string, delta proto.Message, ttl ...time.Duration) error
	UpdateProtobufWithTxn(ctx context.Context, txn *badger.Txn, key string, delta proto.Message, ttl ...time.Duration) error
}

type BadgerDB struct {
	managed bool
	Path    string
	DB      *badger.DB
	log 	*zap.Logger
}

const (
	MaxBatchSize = 1000
)

var (
	dbonce     sync.Once
	instance *BadgerDB

	// ErrBatchTooBig indicates that the batch size exceeds the maximum limit.
	ErrBatchTooBig = errors.New("Batch size exceeds maximum limit")

	// ErrUnknownOp indicates that an unknown operation was encountered.
	ErrUnknownOp = errors.New("Unknown operation")
)

func InitDB(ctx context.Context, managed bool, log *zap.Logger) error {
	// dbPath := filepath.Join(config.GetConfig().MetaDataPath, "metadb")
	dbPath := ""
	var err error
	dbonce.Do(func() {
		log.Info("Initializing DB")

		opts := badger.DefaultOptions(filepath.Join(dbPath, "application")).
			// WithLogger(logger.NewSubModuleLogger("pichus3.db"))
			WithLogger(nil)

		var db *badger.DB
		if managed {
			db, err = badger.OpenManaged(opts)
			if err != nil {
				return
			}
		} else {
			db, err = badger.Open(opts)
			if err != nil {
				return
			}
		}

		instance = &BadgerDB{DB: db, managed: managed, Path: dbPath}
		errCh := make(chan error, 1)
		// go instance.SubscribeToDBPublisher(ctx, errCh)
		select {
		case err = <-errCh:
			db.Close()
			return
		case <-time.After(1 * time.Second):
			// Subscription successful
		}

		go instance.RunVlogGC(ctx)
	})

	return err
}

func GetDB(ctx context.Context) Database {
	return instance
}

func (bdb *BadgerDB) GetBadgerInstance() *badger.DB {
	return bdb.DB
}

func (bdb *BadgerDB) Exists(ctx context.Context, key string) bool {
	err := bdb.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false
		}
		bdb.log.Error("Error while checking key exist or not", zap.String("key", key), zap.Error(err))
		return false
	}

	return true
}

func (bdb *BadgerDB) Count(ctx context.Context, prefix string) int {
	count := 0
	prefixBytes := []byte(prefix)

	iterate := func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We're only interested in keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			count++
		}

		return nil
	}

	bdb.DB.View(func(txn *badger.Txn) error {
		return iterate(txn)
	})

	return count
}

func (bdb *BadgerDB) GetKeysByPrefixAt(ctx context.Context, prefix string, readTs uint64) ([]string, error) {
	keys := []string{}
	var err error

	bdb.NewReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

func (bdb *BadgerDB) GetKeysByPrefixWithTxn(ctx context.Context, txn *badger.Txn,
	prefix string) ([]string, error) {
	keys := []string{}
	prefixBytes := []byte(prefix)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // We're only interested in keys
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		keys = append(keys, string(it.Item().KeyCopy(nil)))
	}

	return keys, nil
}

func (bdb *BadgerDB) GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	var err error
	bdb.DB.View(func(txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

func (bdb *BadgerDB) HasChild(ctx context.Context, prefix string) bool {
	prefixBytes := []byte(prefix)
	has := false
	bdb.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We're only interested in keys
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			has = true
			return nil
		}
		return nil
	})
	return has
}

func (bdb *BadgerDB) IncrBy(ctx context.Context, key string, delta uint64) (uint64, error) {
	var newVal uint64
	err := bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
		var err error
		newVal, err = bdb.IncrByWithTxn(ctx, txn, key, delta)
		return err
	})
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func (bdb *BadgerDB) IncrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) (uint64, error) {
	b, ok, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return 0, err
	}

	if ok {
		currentVal := utils.BytesToUint64(b)
		delta = currentVal + delta
	}

	err = bdb.SetWithTxn(ctx, txn, key, utils.Uint64ToBytes(delta))
	if err != nil {
		return 0, err
	}

	return delta, nil
}

func (bdb *BadgerDB) DecrBy(ctx context.Context, key string, delta uint64) (uint64, error) {
	var newVal uint64
	err := bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
		var err error
		newVal, err = bdb.DecrByWithTxn(ctx, txn, key, delta)
		return err
	})
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func (bdb *BadgerDB) DecrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) (uint64, error) {
	b, ok, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return 0, err
	}

	if ok {
		currentVal := utils.BytesToUint64(b)
		if currentVal < delta {
			delta = 0
		} else {
			delta = currentVal - delta
		}
	} else {
		delta = 0
	}

	err = bdb.SetWithTxn(ctx, txn, key, utils.Uint64ToBytes(delta))
	if err != nil {
		return 0, err
	}

	return delta, nil
}

func (bdb *BadgerDB) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	var exist bool
	err := bdb.DB.View(func(txn *badger.Txn) error {
		var err error
		value, exist, err = bdb.GetWithTxn(ctx, txn, key)
		return err
	})

	return value, exist, err
}

func (bdb *BadgerDB) GetAt(ctx context.Context, key string, readTs uint64) ([]byte, bool, error) {
	var val []byte
	var exist bool
	err := bdb.NewReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
		var txErr error
		val, exist, txErr = bdb.GetWithTxn(ctx, txn, key)
		if txErr != nil {
			return txErr
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return val, exist, nil
}

func (bdb *BadgerDB) BulkGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := bdb.DB.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			value, exist, err := bdb.GetWithTxn(ctx, txn, k)
			if err != nil {
				return err
			}
			if exist {
				result[k] = value
			}
		}
		return nil
	})

	return result, err
}

func (bdb *BadgerDB) GetWithTxn(ctx context.Context, txn *badger.Txn, key string) ([]byte, bool, error) {
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, false, nil
		}

		bdb.log.Error("Failed to get key", zap.String("key", key), zap.Error(err))
		return nil, false, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		bdb.log.Error("Failed to copy value", zap.String("key", key), zap.Error(err))
		return nil, false, err
	}

	return value, true, nil
}

func (bdb *BadgerDB) GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	result := map[string][]byte{}

	err := bdb.DB.View(func(txn *badger.Txn) error {
		var err error
		result, err = bdb.GetByPrefixWithTxn(ctx, txn, prefix)
		return err
	})
	if err != nil {
		bdb.log.Error("Error while reading data from database", zap.String("prefix", prefix), zap.Error(err))
		return nil, err
	}

	return result, nil
}

func (bdb *BadgerDB) GetByPrefixAt(ctx context.Context, prefix string, readTs uint64) (map[string][]byte, error) {
	result := map[string][]byte{}

	err := bdb.NewReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
		var err error
		result, err = bdb.GetByPrefixWithTxn(ctx, txn, prefix)
		return err
	})
	if err != nil {
		bdb.log.Error("Error while reading data from database", zap.String("prefix", prefix),
			zap.Uint64("readTs", readTs), zap.Error(err))
		return nil, err
	}

	return result, nil
}

func (bdb *BadgerDB) GetByPrefixWithTxn(ctx context.Context, txn *badger.Txn,
	prefix string) (map[string][]byte, error) {
	result := map[string][]byte{}
	prefixBytes := []byte(prefix)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		item := it.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		result[string(k)] = v
	}

	return result, nil
}

// https://docs.hypermode.com/badger/quickstart#possible-pagination-implementation-using-prefix-scans
func (bdb *BadgerDB) IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int,
	process func(k, v []byte) bool) (string, error) {
	nextCursor := ""
	err := bdb.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// if no cursor provided prefix scan starts from the beginning
		p := prefix
		if startCursor != "" {
			p = startCursor
		}

		prefixBytes := []byte(p)
		iterNum := 0
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Once the starting point for iteration is found, revert the prefix
			// back to original prefix to continue iterating sequentially.
			// Otherwise, iteration would stop after a single prefix-key match.
			prefixBytes = []byte(prefix)

			item := it.Item()
			k := item.KeyCopy(nil)
			if limit != nil && iterNum >= *limit {
				nextCursor = string(k)
				break
			}

			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			// Return true from process if the item is processed successfully.
			if process(k, v) {
				iterNum++
			}
		}

		// If the number of iterations is less than the limit,
		// it means there are no more items for the prefix.
		if limit != nil && iterNum < *limit {
			nextCursor = ""
		}

		return nil
	})
	if err != nil {
		bdb.log.Error("Error while iterating with prefix", zap.String("prefix", prefix),
			zap.String("startCursor", startCursor), zap.Intp("limit", limit), zap.Error(err))
		return "", err
	}

	return nextCursor, nil
}

func (bdb *BadgerDB) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if bdb.managed {
		return bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
			return bdb.SetWithTxn(ctx, txn, key, value, ttl...)
		})
	}

	return bdb.DB.Update(func(txn *badger.Txn) error {
		return bdb.SetWithTxn(ctx, txn, key, value, ttl...)
	})
}

// TODO: Can we avoid race condition by using Item metadata
func (bdb *BadgerDB) SetWithTxn(ctx context.Context, txn *badger.Txn, key string, value []byte,
	ttl ...time.Duration) error {
	entry := badger.NewEntry([]byte(key), value)
	if len(ttl) > 0 {
		entry.WithTTL(ttl[0])
	}

	// logger.GetLogger(ctx).Debugf("Writing data in database: %s", string(value))
	return txn.SetEntry(entry) // TODO: Handle badger.ErrTxnTooBig. https://docs.hypermode.com/badger/quickstart#read-write-transactions
}

func (bdb *BadgerDB) Delete(ctx context.Context, key string) error {
	var err error
	if bdb.managed {
		err = bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
			return bdb.DeleteWithTxn(ctx, txn, key)
		})
	} else {
		err = bdb.DB.Update(func(txn *badger.Txn) error {
			return bdb.DeleteWithTxn(ctx, txn, key)
		})
	}

	if err == nil || err == badger.ErrKeyNotFound {
		// Treat missing keys as non-errors
		return nil
	}

	bdb.log.Error("Failed to delete data in database", zap.String("key", key), zap.Error(err))
	return err
}

func (bdb *BadgerDB) DeleteByPrefix(ctx context.Context, prefix string) error {
	var err error
	if bdb.managed {
		err = bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
			bdb.DeleteByPrefixWithTxn(ctx, txn, prefix)
			return nil
		})
	} else {
		err = bdb.DB.Update(func(txn *badger.Txn) error {
			bdb.DeleteByPrefixWithTxn(ctx, txn, prefix)
			return nil
		})
	}
	if err != nil {
		bdb.log.Error("Error while deleting data in database", zap.String("prefix", prefix), zap.Error(err))
		return err
	}

	return nil
}

func (bdb *BadgerDB) DeleteByPrefixWithTxn(ctx context.Context, txn *badger.Txn, prefix string) {
	prefixBytes := []byte(prefix)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // We're only interested in keys
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		item := it.Item()
		key := string(item.KeyCopy(nil))
		err := bdb.DeleteWithTxn(ctx, txn, key)
		if err != nil && err != badger.ErrKeyNotFound {
			bdb.log.Warn("Error while deleting data in database", zap.String("key", key), zap.Error(err))
			continue
		}
	}
}

func (bdb *BadgerDB) DeleteWithTxn(ctx context.Context, txn *badger.Txn, key string) error {
	return txn.Delete([]byte(key))
}

func (bdb *BadgerDB) NewTransaction(ctx context.Context, readOnly bool,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.DB.NewTransaction(!readOnly)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (bdb *BadgerDB) NewTransactionAt(ctx context.Context, readTs, commitTs uint64,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.DB.NewTransactionAt(readTs, true)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return txn.CommitAt(commitTs, nil)
}

func (bdb *BadgerDB) NewReadOnlyTransactionAt(ctx context.Context, readTs uint64,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.DB.NewTransactionAt(readTs, false)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return nil
}

func (bdb *BadgerDB) BatchWrite(ctx context.Context, ops []*types.Command) error {
	count := len(ops)
	if count == 0 {
		return nil
	}

	if count > MaxBatchSize {
		return ErrBatchTooBig
	}

	bdb.log.Info("Performing batch operations", zap.Int("count", count))
	err := bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.BatchWriteWithTxn(ctx, txn, ops)
	})
	if err != nil {
		bdb.log.Error("Error while performing batch operations", zap.Error(err))
		return err
	}

	return nil
}

func (bdb *BadgerDB) BatchWriteWithTxn(ctx context.Context, txn *badger.Txn, ops []*types.Command) error {
	for i := range ops {
		switch ops[i].Type {
		case types.CommandType_SET:
			ttl := []time.Duration{}
			if ops[i].Ttl != nil && ops[i].Ttl.Seconds > 0 {
				ttl = append(ttl, ops[i].Ttl.AsDuration())
			}

			err := bdb.SetWithTxn(ctx, txn, ops[i].Key, ops[i].Value, ttl...)
			if err != nil {
				bdb.log.Error("Error while adding set operation to transaction", zap.String("key", ops[i].Key),
					zap.Error(err))
				return err
			}

		case types.CommandType_UPDATE:
			ttl := []time.Duration{}
			if ops[i].Ttl != nil && ops[i].Ttl.Seconds > 0 {
				ttl = append(ttl, ops[i].Ttl.AsDuration())
			}

			switch ops[i].UpdateMeta.UpdateDeltaType {
			case types.UpdateDeltaType_PROTOBUF:
				v, err := ops[i].UnmarshalUpdateDelta()
				if err != nil {
					bdb.log.Error("Failed to unmarshal delta protobuf", zap.String("key", ops[i].Key), zap.Error(err))
					return err
				}

				msg, _ := v.(proto.Message)
				err = bdb.UpdateProtobufWithTxn(ctx, txn, ops[i].Key, msg, ttl...)
				if err != nil {
					bdb.log.Error("Error while adding merge protobuf operation to transaction",
						zap.String("key", ops[i].Key), zap.Error(err))
					return err
				}
			case types.UpdateDeltaType_JSON:
				v, err := ops[i].UnmarshalUpdateDelta()
				if err != nil {
					bdb.log.Error("Failed to unmarshal delta JSON", zap.String("key", ops[i].Key), zap.Error(err))
					return err
				}

				msg, _ := v.(map[string]any)
				err = bdb.UpdateJsonWithTxn(ctx, txn, ops[i].Key, msg, ttl...)
				if err != nil {
					bdb.log.Error("Error while adding merge json operation to transaction",
						zap.String("key", ops[i].Key), zap.Error(err))
					return err
				}
			default:
				bdb.log.Error("Unknown merge delta type", zap.String("key", ops[i].Key),
					zap.String("deltaType", ops[i].UpdateMeta.UpdateDeltaType.String()))
				return ErrUnknownOp
			}

		case types.CommandType_DELETE:
			err := bdb.DeleteWithTxn(ctx, txn, ops[i].Key)
			if err != nil {
				bdb.log.Error("Error while adding delete operation to transaction",
					zap.String("key", ops[i].Key), zap.Error(err))
				return err
			}

		case types.CommandType_INCR_BY:
			_, err := bdb.IncrByWithTxn(ctx, txn, ops[i].Key, ops[i].IncrOrDecrDelta)
			if err != nil {
				bdb.log.Error("Error while adding incrby operation to transaction", zap.String("key", ops[i].Key),
					zap.Error(err))
				return err
			}

		case types.CommandType_DECR_BY:
			_, err := bdb.DecrByWithTxn(ctx, txn, ops[i].Key, ops[i].IncrOrDecrDelta)
			if err != nil {
				bdb.log.Error("Error while adding decrby operation to transaction", zap.String("key", ops[i].Key),
					zap.Error(err))
				return err
			}

		default:
			bdb.log.Error("Unknown batch operation:", zap.String("key", ops[i].Key),
				zap.String("opType", ops[i].Type.String()))
			return ErrUnknownOp
		}

		bdb.log.Debug("Applied batch operation", zap.String("key", ops[i].Key),
			zap.String("opType", ops[i].Type.String()))
	}
	return nil
}

func (bdb *BadgerDB) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	return bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateJsonWithTxn(ctx, txn, key, delta, ttl...)
	})
}

func (bdb *BadgerDB) UpdateJsonWithTxn(ctx context.Context, txn *badger.Txn, key string,
	delta map[string]any, ttl ...time.Duration) error {
	fullMsgBytes, exist, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return err
	}

	var fullMsg map[string]any
	if exist {
		err = json.Unmarshal(fullMsgBytes, &fullMsg)
		if err != nil {
			bdb.log.Error("Failed to unmarshal existing JSON", zap.String("key", key), zap.Error(err))
			return err
		}
	} else {
		fullMsg = make(map[string]any)
	}

	maps.Copy(fullMsg, delta)

	mergedBytes, err := json.Marshal(fullMsg)
	if err != nil {
		bdb.log.Error("Failed to marshal merged JSON", zap.String("key", key), zap.Error(err))
		return err
	}

	return bdb.SetWithTxn(ctx, txn, key, mergedBytes, ttl...)
}

func (bdb *BadgerDB) UpdateProtobuf(ctx context.Context, key string, delta proto.Message,
	ttl ...time.Duration) error {
	return bdb.NewTransaction(ctx, false, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateProtobufWithTxn(ctx, txn, key, delta, ttl...)
	})
}

func (bdb *BadgerDB) UpdateProtobufWithTxn(ctx context.Context, txn *badger.Txn, key string,
	delta proto.Message, ttl ...time.Duration) error {
	fullMsgBytes, exist, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return err
	}

	var mergedBytes []byte
	if exist {
		fullMsg := dynamicpb.NewMessage(delta.ProtoReflect().Descriptor())
		err = proto.Unmarshal(fullMsgBytes, fullMsg)
		if err != nil {
			bdb.log.Error("Failed to unmarshal existing Protobuf", zap.String("key", key), zap.Error(err))
			return err
		}

		err = types.MergeProtobufMessages(fullMsg, delta)
		if err != nil {
			bdb.log.Error("Failed to merge Protobuf messages", zap.String("key", key), zap.Error(err))
			return err
		}

		mergedBytes, err = proto.Marshal(fullMsg)
		if err != nil {
			bdb.log.Error("Failed to marshal merged Protobuf", zap.String("key", key), zap.Error(err))
			return err
		}
	} else {
		mergedBytes, err = proto.Marshal(delta)
		if err != nil {
			bdb.log.Error("Failed to marshal delta Protobuf", zap.String("key", key), zap.Error(err))
			return err
		}
	}
	return bdb.SetWithTxn(ctx, txn, key, mergedBytes, ttl...)
}

// https://docs.hypermode.com/badger/quickstart#garbage-collection
// https://github.com/hypermodeinc/dgraph/blob/e6980befe54103c67f353ffaa311345747ebb147/x/x.go#L1182-L1219
// RunVlogGC runs value log gc on store. It runs GC unconditionally after every 10 minute.
// TODO: Is 10 mins is suitable interval?
func (bdb *BadgerDB) RunVlogGC(ctx context.Context) {
	bdb.log.Info("Starting Badger value log GC routine")

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	var lastSz int64
	bdb.runGC(ctx, &lastSz)
	for {
		select {
		case <-ctx.Done():
			bdb.log.Info("Stopping Badger value log GC routine")
			return
		case <-ticker.C:
			bdb.runGC(ctx, &lastSz)
		}
	}
}

func (bdb *BadgerDB) runGC(ctx context.Context, lastSz *int64) {
	bdb.log.Info("Running Badger value log GC")
	if bdb.managed {
		// TODO: Where to add this logic?
		// discardTs := bdb.Oracle.ComputeDiscardTs()
		// bdb.DB.SetDiscardTs(discardTs)
		// log.Info("Setting discardTs to:", discardTs)
	}

	for err := error(nil); err == nil; {
		// If a GC is successful, immediately run it again.
		err = bdb.DB.RunValueLogGC(0.5)
	}

	abs := func(a, b int64) int64 {
		if a > b {
			return a - b
		}
		return b - a
	}

	_, sz := bdb.DB.Size()
	if abs(*lastSz, sz) > 512<<20 {
		bdb.log.Info("Value log size", zap.String("size", humanize.Bytes(uint64(sz))),
			zap.String("lastSize", humanize.Bytes(uint64(*lastSz))))
		lastSz = &sz
	}
}

// func (bdb *BadgerDB) SubscribeToDBPublisher(ctx context.Context, errCh chan error) {
// 	maches := make([]pb.Match, 0, len(subscribers))
// 	for i := range subscribers {
// 		maches = append(maches, pb.Match{Prefix: []byte(subscribers[i])})
// 	}

// 	err := bdb.DB.Subscribe(ctx, func(kv *badger.KVList) error {
// 		for _, item := range kv.Kv {
// 			log.Infof("Received notification - Key: %s, Value: %s", string(item.Key), string(item.Value))
// 		}
// 		return nil
// 	}, maches)

// 	if err != nil && !errors.Is(err, context.Canceled) {
// 		log.WithError(err).Error("Failed to subscribe to DB publisher")
// 		if errCh != nil {
// 			errCh <- err
// 		}
// 		return
// 	}

// 	log.Info("Stopped DB publisher subscription")
// }

func DbClose(ctx context.Context, log *zap.Logger) {
	if instance != nil {
		log.Info("Closing DB connection")
		if err := instance.DB.Close(); err != nil {
			log.Error("Error closing application DB", zap.Error(err))
		}
	}
}

var _ Database = (*BadgerDB)(nil)
