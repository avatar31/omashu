/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/types"
	"github.com/avatar31/omashu/utils"
)

// MaxBatchSize is the maximum number of sub-commands allowed in a single
// distributed transaction. Transactions that exceed this limit return [ErrBatchTooBig].
const (
	MaxBatchSize = 100
)

// initBadger opens (or creates) a BadgerDB instance using the settings in cfg.
// When managed is true the database is opened with OpenManaged, which allows the
// TSO to control MVCC commit timestamps via CommitAt. When managed is false the
// standard unmanaged Open is used (suitable for the single-node Badger store).
// The GC goroutine is started immediately and runs until ctx is cancelled.
func initBadger(ctx context.Context, managed bool, cfg *Config) (*Badger, error) {
	var db *badger.DB
	var err error
	if managed {
		db, err = badger.OpenManaged(cfg.BadgerOptions)
		if err != nil {
			return nil, err
		}
	} else {
		db, err = badger.Open(cfg.BadgerOptions)
		if err != nil {
			return nil, err
		}
	}

	instance := &Badger{
		db:             db,
		managed:        managed,
		path:           cfg.BadgerOptions.Dir,
		log:            cfg.Logger,
		gcInterval:     cfg.GCInterval,
		gcDiscardRatio: cfg.GCDiscardRatio,
	}

	go instance.runVlogGC(ctx)
	return instance, nil
}

// DBReadOps Interface

// Count returns the number of keys that share the given prefix. The scan runs
// inside a read-only transaction and stops early if ctx is cancelled, in which
// case 0 is returned.
func (bdb *Badger) Count(ctx context.Context, prefix string) int {
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

	err := bdb.db.View(func(txn *badger.Txn) error {
		return iterate(txn)
	})
	if err != nil {
		return 0
	}

	return count
}

// Exists reports whether key is present in the store. It returns false for any
// storage error — callers that need to distinguish a missing key from a read
// error should use [Badger.Get] instead.
func (bdb *Badger) Exists(ctx context.Context, key string) bool {
	err := bdb.db.View(func(txn *badger.Txn) error {
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

// HasChild reports whether at least one key exists with the given prefix. It
// stops scanning after the first match, making it cheaper than [Badger.Count]
// when only a presence check is needed.
func (bdb *Badger) HasChild(ctx context.Context, prefix string) bool {
	prefixBytes := []byte(prefix)
	has := false
	err := bdb.db.View(func(txn *badger.Txn) error {
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
	if err != nil {
		return false
	}

	return has
}

// Get retrieves the value stored at key. The second return value indicates
// whether the key was found. A missing key is not an error: found is false and
// err is nil. The returned slice is a copy owned by the caller.
func (bdb *Badger) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	var exist bool
	err := bdb.db.View(func(txn *badger.Txn) error {
		var err error
		value, exist, err = bdb.GetWithTxn(ctx, txn, key)
		return err
	})

	return value, exist, err
}

// GetWithTxn retrieves the value stored at key within the caller-supplied
// BadgerDB transaction. Use this when you need to perform multiple reads inside
// the same snapshot. A missing key returns (nil, false, nil).
func (bdb *Badger) GetWithTxn(ctx context.Context, txn *badger.Txn, key string) ([]byte, bool, error) {
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

// GetByPrefix returns all key-value pairs whose key begins with prefix. The
// result map is empty (not nil) when no keys match. An unexpected storage
// failure is returned as an error; a prefix with no matches is not an error.
func (bdb *Badger) GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	result := map[string][]byte{}

	err := bdb.db.View(func(txn *badger.Txn) error {
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

// GetByPrefixWithTxn returns all key-value pairs whose key begins with prefix,
// using the caller-supplied transaction. The scan is cancelled early if ctx is
// done, in which case a partial result and ctx.Err() are returned.
func (bdb *Badger) GetByPrefixWithTxn(ctx context.Context, txn *badger.Txn,
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

// GetKeysByPrefix returns the keys (but not values) whose key begins with
// prefix. Values are not fetched, making this cheaper than [Badger.GetByPrefix]
// when only key names are needed.
func (bdb *Badger) GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	var err error
	err = bdb.db.View(func(txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

// GetKeysByPrefixWithTxn returns the keys (but not values) whose key begins
// with prefix, using the caller-supplied transaction. The scan is cancelled
// early if ctx is done, returning the keys collected so far and ctx.Err().
func (bdb *Badger) GetKeysByPrefixWithTxn(ctx context.Context, txn *badger.Txn,
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

// BulkGet fetches multiple keys in a single read-only transaction. The
// returned map contains only the keys that were found; missing keys are silently
// omitted. An error from any individual key read aborts the entire batch.
func (bdb *Badger) BulkGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := bdb.db.View(func(txn *badger.Txn) error {
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

// IterateByPrefix performs a paginated prefix scan. For each matching key-value
// pair it calls process; if process returns false the item is not counted toward
// limit, allowing the caller to skip unwanted entries without advancing the page.
//
// startCursor is an optional key at which to resume a previous scan. Pass an
// empty string to start from the beginning of the prefix range.
//
// limit caps the number of successfully processed items. When limit is nil all
// matching items are visited. When the page is full, IterateByPrefix returns the
// key of the next unprocessed item so the caller can pass it as startCursor on
// the following call. An empty return cursor means the prefix range is exhausted.
//
// The scan is cancelled early if ctx is done.
func (bdb *Badger) IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int,
	process func(k, v []byte) bool) (string, error) {
	nextCursor := ""
	err := bdb.db.View(func(txn *badger.Txn) error {
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

// DBWriteOps Interface

// DecrBy atomically decrements the counter stored at key by delta. The value
// is stored and read as a big-endian uint64. If the stored value is smaller
// than delta the result is clamped to zero. If the key does not exist the
// result is zero.
//
// DecrBy panics if called on a managed Badger instance. On a [DistributedBadger]
// use its own DecrBy, which routes the operation through Raft.
func (bdb *Badger) DecrBy(ctx context.Context, key string, delta uint64) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.DecrByWithTxn(ctx, txn, key, delta)
	})
}

// DecrByWithTxn performs the decrement inside the caller-supplied transaction.
// See [Badger.DecrBy] for counter semantics.
func (bdb *Badger) DecrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) error {
	b, ok, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return err
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

	return bdb.SetWithTxn(ctx, txn, key, utils.Uint64ToBytes(delta))
}

// IncrBy atomically increments the counter stored at key by delta. The value
// is stored and read as a big-endian uint64. If the key does not exist it is
// treated as zero before adding delta.
//
// IncrBy panics if called on a managed Badger instance. On a [DistributedBadger]
// use its own IncrBy, which routes the operation through Raft.
func (bdb *Badger) IncrBy(ctx context.Context, key string, delta uint64) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.IncrByWithTxn(ctx, txn, key, delta)
	})
}

// IncrByWithTxn performs the increment inside the caller-supplied transaction.
// See [Badger.IncrBy] for counter semantics.
func (bdb *Badger) IncrByWithTxn(ctx context.Context, txn *badger.Txn, key string, delta uint64) error {
	b, ok, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return err
	}

	if ok {
		currentVal := utils.BytesToUint64(b)
		delta = currentVal + delta
	}

	return bdb.SetWithTxn(ctx, txn, key, utils.Uint64ToBytes(delta))
}

// Set writes value to key, overwriting any existing value. An optional TTL
// duration may be passed to expire the entry automatically; if omitted the
// entry persists indefinitely.
//
// Set panics if called on a managed Badger instance. On a [DistributedBadger]
// use its own Set, which routes the operation through Raft.
func (bdb *Badger) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.db.Update(func(txn *badger.Txn) error {
		return bdb.SetWithTxn(ctx, txn, key, value, ttl...)
	})
}

// SetWithTxn writes value to key within the caller-supplied transaction.
// If ttl is provided the entry will expire after that duration.
func (bdb *Badger) SetWithTxn(ctx context.Context, txn *badger.Txn, key string, value []byte,
	ttl ...time.Duration) error {
	entry := badger.NewEntry([]byte(key), value)
	if len(ttl) > 0 {
		entry.WithTTL(ttl[0])
	}

	return txn.SetEntry(entry)
}

// UpdateJson merges delta into the JSON object stored at key using a shallow
// merge: top-level keys present in delta overwrite the corresponding stored keys,
// and keys absent from delta are left unchanged. If the key does not exist, delta
// itself becomes the initial value.
//
// UpdateJson panics if called on a managed Badger instance. On a
// [DistributedBadger] use its own UpdateJson, which routes the operation through Raft.
func (bdb *Badger) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateJsonWithTxn(ctx, txn, key, delta, ttl...)
	})
}

// UpdateJsonWithTxn performs the JSON merge inside the caller-supplied
// transaction. See [Badger.UpdateJson] for merge semantics.
func (bdb *Badger) UpdateJsonWithTxn(ctx context.Context, txn *badger.Txn, key string,
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

// UpdateProtobuf merges the fields of delta into the Protobuf message stored at
// key using proto.Merge semantics: singular fields are overwritten by delta, while
// repeated fields and map entries are appended. If the key does not exist, delta
// is stored as the initial value.
//
// UpdateProtobuf panics if called on a managed Badger instance. On a
// [DistributedBadger] use its own UpdateProtobuf, which routes the operation
// through Raft.
func (bdb *Badger) UpdateProtobuf(ctx context.Context, key string, delta proto.Message,
	ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateProtobufWithTxn(ctx, txn, key, delta, ttl...)
	})
}

// UpdateProtobufWithTxn performs the Protobuf merge inside the caller-supplied
// transaction. See [Badger.UpdateProtobuf] for merge semantics.
func (bdb *Badger) UpdateProtobufWithTxn(ctx context.Context, txn *badger.Txn, key string,
	delta proto.Message, ttl ...time.Duration) error {
	fullMsgBytes, exist, err := bdb.GetWithTxn(ctx, txn, key)
	if err != nil {
		return err
	}

	var mergedBytes []byte
	if exist {
		mergedBytes, err = types.MergeProtoDelta(fullMsgBytes, delta)
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

// DBDeleteOps Interface

// Delete removes key from the store. A missing key is treated as a successful
// no-op: Delete returns nil when the key does not exist. Any other storage
// error is logged and returned.
//
// Delete panics if called on a managed Badger instance. On a [DistributedBadger]
// use its own Delete, which routes the operation through Raft.
func (bdb *Badger) Delete(ctx context.Context, key string) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	err := bdb.db.Update(func(txn *badger.Txn) error {
		return bdb.DeleteWithTxn(ctx, txn, key)
	})

	if err == nil || err == badger.ErrKeyNotFound {
		// Treat missing keys as non-errors
		return nil
	}

	bdb.log.Error("Failed to delete data in database", zap.String("key", key), zap.Error(err))
	return err
}

// DeleteWithTxn removes key within the caller-supplied transaction.
func (bdb *Badger) DeleteWithTxn(ctx context.Context, txn *badger.Txn, key string) error {
	return txn.Delete([]byte(key))
}

// DeleteByPrefix removes all keys that begin with prefix in a single read-write
// transaction. If no keys match the prefix the operation is a no-op.
//
// DeleteByPrefix panics if called on a managed Badger instance. On a
// [DistributedBadger] use its own DeleteByPrefix, which routes the operation
// through Raft.
func (bdb *Badger) DeleteByPrefix(ctx context.Context, prefix string) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	err := bdb.db.Update(func(txn *badger.Txn) error {
		return bdb.DeleteByPrefixWithTxn(ctx, txn, prefix)
	})
	if err != nil {
		bdb.log.Error("Error while deleting data in database", zap.String("prefix", prefix), zap.Error(err))
		return err
	}

	return nil
}

// DeleteByPrefixWithTxn removes all keys that begin with prefix within the
// caller-supplied transaction. The scan is aborted early if ctx is done.
func (bdb *Badger) DeleteByPrefixWithTxn(ctx context.Context, txn *badger.Txn, prefix string) error {
	prefixBytes := []byte(prefix)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // We're only interested in keys
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		item := it.Item()
		key := string(item.KeyCopy(nil))
		err := bdb.DeleteWithTxn(ctx, txn, key)
		if err != nil && err != badger.ErrKeyNotFound {
			bdb.log.Warn("Error while deleting data in database", zap.String("key", key), zap.Error(err))
			return err
		}
	}

	return nil
}

// Database Interface

// GetBadger returns the underlying *badger.DB instance. Use this for operations
// that fall outside the [Database] interface, such as running a manual backup
// or accessing BadgerDB-specific APIs directly.
func (bdb *Badger) GetBadger() *badger.DB {
	return bdb.db
}

// NewTransaction executes performOps inside a read-write BadgerDB transaction.
// If performOps returns nil the transaction is committed; any non-nil error
// discards the transaction and is returned to the caller. The transaction is
// also discarded if the commit itself fails.
func (bdb *Badger) NewTransaction(ctx context.Context,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.db.NewTransaction(true)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// Close flushes pending writes and closes the underlying BadgerDB instance.
// After Close returns, all operations on bdb will fail. Close should be called
// exactly once when the store is no longer needed, typically via defer.
func (bdb *Badger) Close(ctx context.Context) {
	if err := bdb.db.Close(); err != nil {
		bdb.log.Error("Error closing Badger DB instance", zap.Error(err))
		return
	}
	bdb.log.Info("Closed Badger DB instance")
}

// Helpers

// getKeysByPrefixAt returns the keys (not values) under prefix as of the given
// MVCC read timestamp. Used by the distributed store to serve transactional
// prefix-key reads at a consistent snapshot.
func (bdb *Badger) getKeysByPrefixAt(ctx context.Context, prefix string, readTs uint64) ([]string, error) {
	keys := []string{}
	var err error

	err = bdb.newReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

// getAt reads the value at key as of the given MVCC read timestamp. Used by
// the distributed store to serve transactional point reads at a consistent
// snapshot.
func (bdb *Badger) getAt(ctx context.Context, key string, readTs uint64) ([]byte, bool, error) {
	var val []byte
	var exist bool
	err := bdb.newReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
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

// getByPrefixAt returns all key-value pairs under prefix as of the given MVCC
// read timestamp. Used by the distributed store to serve transactional prefix
// reads at a consistent snapshot.
func (bdb *Badger) getByPrefixAt(ctx context.Context, prefix string, readTs uint64) (map[string][]byte, error) {
	result := map[string][]byte{}

	err := bdb.newReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
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

// newReadonlyTransaction opens a read-only BadgerDB transaction, executes
// performOps, and discards it on return. The transaction is never committed.
func (bdb *Badger) newReadonlyTransaction(ctx context.Context,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.db.NewTransaction(false)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// newTransactionAt opens a managed BadgerDB transaction at readTs, executes
// performOps, and commits the result at commitTs via CommitAt. This is the
// write path used by the FSM when applying committed Raft log entries to
// BadgerDB at a specific MVCC timestamp.
func (bdb *Badger) newTransactionAt(ctx context.Context, readTs, commitTs uint64,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.db.NewTransactionAt(readTs, true)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return txn.CommitAt(commitTs, nil)
}

// newReadOnlyTransactionAt opens a managed read-only BadgerDB transaction at
// readTs, executes performOps, and always discards the transaction afterward.
// No commit is performed.
func (bdb *Badger) newReadOnlyTransactionAt(ctx context.Context, readTs uint64,
	performOps func(context.Context, *badger.Txn) error) error {
	txn := bdb.db.NewTransactionAt(readTs, false)
	defer txn.Discard()

	err := performOps(ctx, txn)
	if err != nil {
		return err
	}

	return nil
}

// setOracle wires the TSO to this Badger instance so the GC routine can obtain
// a safe discard watermark from the oracle before running value-log GC.
// Panics if called on an unmanaged instance.
func (bdb *Badger) setOracle(oracle *TSO) {
	if !bdb.managed {
		panic("operation not supported")
	}

	bdb.oracle = oracle
}

// runVlogGC runs BadgerDB value-log GC on a ticker driven by gcInterval.
// On each tick it sets the discard watermark obtained from the TSO (managed
// mode only) and then calls RunValueLogGC repeatedly until no further space
// can be reclaimed. The goroutine exits cleanly when ctx is cancelled.
//
// References:
//   - https://docs.hypermode.com/badger/quickstart#garbage-collection
//   - https://github.com/hypermodeinc/dgraph/blob/e6980befe54103c67f353ffaa311345747ebb147/x/x.go#L1182-L1219
func (bdb *Badger) runVlogGC(ctx context.Context) {
	bdb.log.Info("Starting Badger value log GC routine")

	ticker := time.NewTicker(bdb.gcInterval)
	defer ticker.Stop()

	abs := func(a, b int64) int64 {
		if a > b {
			return a - b
		}
		return b - a
	}

	var lastSz int64
	runGC := func() {
		bdb.log.Info("Running Badger value log GC")
		if bdb.managed {
			if bdb.oracle == nil {
				bdb.log.Warn("Oracle is not set for managed Badger DB. Skipping running GC.")
				return
			}
			discardTs := bdb.oracle.DiscardAt()
			bdb.db.SetDiscardTs(discardTs)
			bdb.log.Info("Setting discardTs", zap.Uint64("discardTs", discardTs))
		}

		for err := error(nil); err == nil; {
			// If a GC is successful, immediately run it again.
			err = bdb.db.RunValueLogGC(bdb.gcDiscardRatio)
		}

		_, sz := bdb.db.Size()
		if abs(lastSz, sz) > 512<<20 {
			bdb.log.Info("Value log size", zap.String("size", humanize.Bytes(uint64(sz))),
				zap.String("lastSize", humanize.Bytes(uint64(lastSz))))
			lastSz = sz
		}
	}

	for {
		select {
		case <-ctx.Done():
			bdb.log.Info("Stopping Badger value log GC routine")
			return
		case <-ticker.C:
			runGC()
		}
	}
}

var _ Database[*badger.Txn] = (*Badger)(nil)
