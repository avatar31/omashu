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

const (
	MaxBatchSize = 1000
)

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

	bdb.db.View(func(txn *badger.Txn) error {
		return iterate(txn)
	})

	return count
}

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

func (bdb *Badger) HasChild(ctx context.Context, prefix string) bool {
	prefixBytes := []byte(prefix)
	has := false
	bdb.db.View(func(txn *badger.Txn) error {
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

func (bdb *Badger) GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	var err error
	bdb.db.View(func(txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

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

// https://docs.hypermode.com/badger/quickstart#possible-pagination-implementation-using-prefix-scans
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

func (bdb *Badger) DecrBy(ctx context.Context, key string, delta uint64) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.DecrByWithTxn(ctx, txn, key, delta)
	})
}

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

func (bdb *Badger) IncrBy(ctx context.Context, key string, delta uint64) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.IncrByWithTxn(ctx, txn, key, delta)
	})
}

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

func (bdb *Badger) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.db.Update(func(txn *badger.Txn) error {
		return bdb.SetWithTxn(ctx, txn, key, value, ttl...)
	})
}

func (bdb *Badger) SetWithTxn(ctx context.Context, txn *badger.Txn, key string, value []byte,
	ttl ...time.Duration) error {
	entry := badger.NewEntry([]byte(key), value)
	if len(ttl) > 0 {
		entry.WithTTL(ttl[0])
	}

	return txn.SetEntry(entry)
}

func (bdb *Badger) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateJsonWithTxn(ctx, txn, key, delta, ttl...)
	})
}

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

func (bdb *Badger) UpdateProtobuf(ctx context.Context, key string, delta proto.Message,
	ttl ...time.Duration) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	return bdb.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
		return bdb.UpdateProtobufWithTxn(ctx, txn, key, delta, ttl...)
	})
}

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

func (bdb *Badger) DeleteWithTxn(ctx context.Context, txn *badger.Txn, key string) error {
	return txn.Delete([]byte(key))
}

func (bdb *Badger) DeleteByPrefix(ctx context.Context, prefix string) error {
	if bdb.managed {
		panic("operation not supported in managed db")
	}

	err := bdb.db.Update(func(txn *badger.Txn) error {
		bdb.DeleteByPrefixWithTxn(ctx, txn, prefix)
		return nil
	})
	if err != nil {
		bdb.log.Error("Error while deleting data in database", zap.String("prefix", prefix), zap.Error(err))
		return err
	}

	return nil
}

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

func (bdb *Badger) GetBadger() *badger.DB {
	return bdb.db
}

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

func (bdb *Badger) Close(ctx context.Context) {
	if err := bdb.db.Close(); err != nil {
		bdb.log.Error("Error closing Badger DB instance", zap.Error(err))
		return
	}
	bdb.log.Info("Closed Badger DB instance")
}

// Helpers

func (bdb *Badger) getKeysByPrefixAt(ctx context.Context, prefix string, readTs uint64) ([]string, error) {
	keys := []string{}
	var err error

	bdb.newReadOnlyTransactionAt(ctx, readTs, func(ctx context.Context, txn *badger.Txn) error {
		keys, err = bdb.GetKeysByPrefixWithTxn(ctx, txn, prefix)
		return err
	})

	return keys, err
}

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

func (bdb *Badger) setOracle(oracle *TSO) {
	if !bdb.managed {
		panic("operation not supported")
	}

	bdb.oracle = oracle
}

// https://docs.hypermode.com/badger/quickstart#garbage-collection
// https://github.com/hypermodeinc/dgraph/blob/e6980befe54103c67f353ffaa311345747ebb147/x/x.go#L1182-L1219
// runVlogGC runs value log gc on store. It runs GC unconditionally after every configured interval.
func (bdb *Badger) runVlogGC(ctx context.Context) {
	bdb.log.Info("Starting Badger value log GC routine")

	ticker := time.NewTicker(bdb.gcInterval)
	defer ticker.Stop()

	var lastSz int64
	for {
		select {
		case <-ctx.Done():
			bdb.log.Info("Stopping Badger value log GC routine")
			return
		case <-ticker.C:
			bdb.runGC(&lastSz)
		}
	}
}

func (bdb *Badger) runGC(lastSz *int64) {
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

	abs := func(a, b int64) int64 {
		if a > b {
			return a - b
		}
		return b - a
	}

	_, sz := bdb.db.Size()
	if abs(*lastSz, sz) > 512<<20 {
		bdb.log.Info("Value log size", zap.String("size", humanize.Bytes(uint64(sz))),
			zap.String("lastSize", humanize.Bytes(uint64(*lastSz))))
		lastSz = &sz
	}
}

var _ Database[*badger.Txn] = (*Badger)(nil)
