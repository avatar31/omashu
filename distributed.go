/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/types"
)

// TODO'S: P0
// - Make sure readState works fine end-to-end for linearizable reads
// - Handle leadership changes and re-initialize TSO and TxnManager on leadership gain
// - Improve logging for proposal lifecycle (submission, processing, completion)
// - Add Snapshot and Restore in Raft FSM for state persistence
// - Optimize code paths for common operations (Get, Set, Delete)
// - Retry for failed transactions due to conflicts
//
// TODO'S: P1:
// - Optimize proposeAndWait to use a single channel for all proposals with a map of cmdID to channels
// - Implement snapshotting in Raft FSM to reduce log size and speed up recovery
// - Add metrics collection for Raft operations and DBStore operations
// - Implement better error handling and retries for transient errors during propose
// - Add unit tests and integration tests for DBStore and Raft interactions
// - Implement graceful shutdown and cleanup of resources
// - Add support for dynamic cluster membership changes (adding/removing nodes)
// - Implement monitoring and alerting for Raft node health and performance
// - Optimize read operations to reduce latency on follower nodes
// - Implement backup and restore functionality for the DBStore
// - Add documentation and usage examples for DBStore API
// - Implement security features like encryption and authentication for Raft communication
// - Optimize transaction management for high concurrency scenarios
// - Implement advanced conflict resolution strategies for transactions
// - Add support for complex queries and indexing in the DBStore
// - Implement performance benchmarking and optimization for DBStore operations
// - Implement a web-based dashboard for monitoring and managing the DBStore and Raft cluster
// - Add support for custom command types and extensibility in the DBStore API
// - Implement load balancing and failover strategies for Raft nodes
// - Implement advanced logging and tracing for debugging and performance analysis
// - Add support for real-time data streaming and change data capture from the DBStore
// - Implement advanced data consistency models (e.g., eventual consistency, strong consistency)
// - Add support for data sharding and partitioning in the DBStore
// - Implement a comprehensive testing framework for DBStore and Raft interactions
// - Implement advanced data analytics and reporting features for the DBStore
// - Implement a command-line interface (CLI) for managing and interacting with the DBStore
// - Add support for data versioning and auditing in the DBStore
// - Implement a comprehensive documentation portal for the DBStore and its features
// - Implement advanced data visualization and exploration tools for the DBStore
// - Implement advanced data governance and compliance features for the DBStore

const (
	DefaultProposeTimeout = 5 * time.Second

	// Subdirectories for different components of the store
	DBSubDir   = "db"
	WALSubDir  = "wal"
	SnapSubDir = "snap"
)

func initDistributed(ctx context.Context, db *Badger, cfg *Config) (*DistributedBadger, error) {
	instance := &DistributedBadger{
		log:                cfg.Logger.With(zap.Uint64("nodeId", cfg.RaftConfig.ID)),
		onLeaderChangeHook: cfg.OnLeaderChange,
		onRemovedSelfHook:  cfg.OnRemovedSelf,
	}

	// At this point, the TSO and TxnManager are initialized but not serving.
	// They will start serving when this node becomes the leader.
	// It's DistributedBadger responsibility to request for readtTs and commitTs only on leader node
	instance.tso = newTSO(instance, instance.log)
	instance.tm = newTxnManager(instance, instance.tso, instance.log)
	fsm, err := newFSM(db, instance.log)
	if err != nil {
		return nil, err
	}
	instance.fsm = fsm
	instance.fsm.db.setOracle(instance.tso)

	// Init Raft Node
	node, err := newNode(cfg.RaftConfig.ID, cfg.RaftConfig.Nodename, cfg.RaftConfig.Peers, instance.fsm, instance.log)
	if err != nil {
		instance.Close(ctx)
		return nil, err
	}

	err = node.Start(ctx, cfg)
	if err != nil {
		instance.Close(ctx)
		return nil, err
	}

	instance.node = node
	instance.node.WithLeaderChangeHook(instance.onLeaderChange)
	instance.node.WithRemovedSelfHook(instance.onRemovedSelf)

	return instance, nil
}

func (s *DistributedBadger) onLeaderChange(ctx context.Context, prevLeader, newLeader uint64) {
	if newLeader == s.node.id {
		s.log.Info("This node is now the leader, starting TSO server")

		// Hold the write-lock for the shortest possible window: just the pointer
		// swaps. TSO construction (StartServing) reads from Badger and can be slow,
		// so it must happen before acquiring the lock.
		s.muLCNotifier.Lock()
		s.leaderChangeNotifier = make(chan struct{})
		s.muLCNotifier.Unlock()

		go s.listenProposeResponses(ctx)

		err := s.tso.StartServing(ctx)
		if err != nil {
			s.Close(ctx)
			s.log.Panic("Failed to start TSO server on leadership gain", zap.Error(err))
		}
	}

	if prevLeader == s.node.id {
		s.log.Info("This node is no longer the leader, stopping TSO and TxnManager")

		s.muLCNotifier.Lock()
		notifier := s.leaderChangeNotifier
		s.leaderChangeNotifier = nil
		s.muLCNotifier.Unlock()

		s.tso.Close()
		if notifier != nil {
			close(notifier)
		}
	}

	if s.onLeaderChangeHook != nil {
		s.onLeaderChangeHook(prevLeader, newLeader)
	}
}

func (s *DistributedBadger) onRemovedSelf() {
	if s.onRemovedSelfHook != nil {
		s.onRemovedSelfHook()
	}
}

func (s *DistributedBadger) listenProposeResponses(ctx context.Context) {
	// Capture the notifier channel for THIS leadership term under a read-lock.
	// onLeaderChange replaces s.leaderChangeNotifier with a fresh channel each
	// time leadership is gained. If we read s.leaderChangeNotifier directly
	// inside the select without a captured reference, reading the field from
	// two goroutines (this one and onLeaderChange's writer) is a data race.
	s.muLCNotifier.RLock()
	notifier := s.leaderChangeNotifier
	s.muLCNotifier.RUnlock()

	s.log.Info("Started listening for ProposeResponses")
	for {
		select {
		case r := <-s.node.ProposeRespNotifier():
			s.log.Debug("Received propose response", zap.String("cmdID", r.cmdID), zap.Error(r.ErrResp))
			if ch, ok := s.proposals.Load(r.cmdID); ok {
				errCh, _ := ch.(chan error)
				errCh <- r.ErrResp
			} else {
				s.log.Warn("No proposal channel found for response", zap.String("cmdID", r.cmdID))
				if r.ErrResp != nil {
					s.log.Error("Proposal failed with error", zap.String("cmdID", r.cmdID), zap.Error(r.ErrResp))
				}
			}
		case <-notifier:
			s.log.Info("Stopping listening ProposeResponses due to leadership change")
			return
		case <-ctx.Done():
			s.log.Info("Stopping listening ProposeResponses as context is done")
			return
		}
	}
}

func (s *DistributedBadger) waitForReadState(ctx context.Context, key string) error {
	var appliedIndex, rsIndex uint64
	start := time.Now()
	defer func() {
		s.log.Debug("waitForReadState completed", zap.String("key", key),
			zap.Duration("duration", time.Since(start)), zap.Uint64("appliedIndex", appliedIndex),
			zap.Uint64("readStateIndex", rsIndex))
	}()

	// Request read index
	appliedIndex, err := s.node.ReadIndex(ctx, []byte(key))
	if err != nil {
		s.log.Error("ReadIndex failed", zap.String("key", key), zap.Error(err))
		return err
	}

	// Wait for read state
	rs, err := s.node.WaitForReadState(ctx, 5*time.Second)
	if err != nil {
		s.log.Error("WaitForReadState failed", zap.String("key", key), zap.Error(err))
		return err
	}

	rsIndex = rs.Index
	return nil
}

// isLeader returns true if this node is the leader
func (s *DistributedBadger) isLeader() bool {
	return s.node.IsLeader()
}

// DBReadOps Interface

func (s *DistributedBadger) Count(ctx context.Context, prefix string) int {
	return s.fsm.db.Count(ctx, prefix)
}

func (s *DistributedBadger) Exists(ctx context.Context, key string) bool {
	if err := s.waitForReadState(ctx, key); err != nil {
		return false
	}
	return s.fsm.db.Exists(ctx, key)
}

func (s *DistributedBadger) HasChild(ctx context.Context, prefix string) bool {
	return s.fsm.db.HasChild(ctx, prefix)
}

func (s *DistributedBadger) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return s.getVal(ctx, key, false)
}

func (s *DistributedBadger) getVal(ctx context.Context, key string, skipLinearizable bool) ([]byte, bool, error) {
	if !skipLinearizable {
		if err := s.waitForReadState(ctx, key); err != nil {
			return nil, false, err
		}
	}
	return s.fsm.db.Get(ctx, key)
}

func (s *DistributedBadger) GetWithTxn(ctx context.Context, txn *Txn, key string) ([]byte, bool, error) {
	if err := s.waitForReadState(ctx, key); err != nil {
		return nil, false, err
	}
	result, found, err := s.fsm.db.getAt(ctx, key, txn.readTs)
	if err != nil || !found {
		return nil, found, err
	}

	txn.addReadKey(key)
	return result, found, nil
}

func (s *DistributedBadger) GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	return s.fsm.db.GetByPrefix(ctx, prefix)
}

func (s *DistributedBadger) GetByPrefixWithTxn(ctx context.Context, txn *Txn, prefix string) (map[string][]byte, error) {
	result, err := s.fsm.db.getByPrefixAt(ctx, prefix, txn.readTs)
	if err != nil {
		return nil, err
	}

	for k := range result {
		txn.addReadKey(k)
	}
	return result, nil
}

func (s *DistributedBadger) GetKeysByPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.fsm.db.GetKeysByPrefix(ctx, prefix)
}

func (s *DistributedBadger) GetKeysByPrefixWithTxn(ctx context.Context, txn *Txn, prefix string) ([]string, error) {
	keys, err := s.fsm.db.getKeysByPrefixAt(ctx, prefix, txn.readTs)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		txn.addReadKey(k)
	}
	return keys, nil
}

func (s *DistributedBadger) BulkGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return s.fsm.db.BulkGet(ctx, keys)
}

func (s *DistributedBadger) IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int,
	process func(k, v []byte) bool) (string, error) {
	return s.fsm.db.IterateByPrefix(ctx, prefix, startCursor, limit, process)
}

// DBWriteOps Interface

func (s *DistributedBadger) DecrBy(ctx context.Context, key string, delta uint64) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.DecrByWithTxn(ctx, txn, key, delta)
	})
}

func (s *DistributedBadger) DecrByWithTxn(ctx context.Context, txn *Txn, key string, delta uint64) error {
	return txn.DecrBy(ctx, key, delta)
}

func (s *DistributedBadger) IncrBy(ctx context.Context, key string, delta uint64) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.IncrByWithTxn(ctx, txn, key, delta)
	})
}

func (s *DistributedBadger) IncrByWithTxn(ctx context.Context, txn *Txn, key string, delta uint64) error {
	return txn.IncrBy(ctx, key, delta)
}

func (s *DistributedBadger) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.SetWithTxn(ctx, txn, key, value, ttl...)
	})
}

func (s *DistributedBadger) SetWithTxn(ctx context.Context, txn *Txn, key string, value []byte, ttl ...time.Duration) error {
	return txn.Set(ctx, key, value, ttl...)
}

func (s *DistributedBadger) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.UpdateJsonWithTxn(ctx, txn, key, delta, ttl...)
	})
}

func (s *DistributedBadger) UpdateJsonWithTxn(ctx context.Context, txn *Txn, key string, delta map[string]any,
	ttl ...time.Duration) error {
	return txn.UpdateJson(ctx, key, delta, ttl...)
}

func (s *DistributedBadger) UpdateProtobuf(ctx context.Context, key string, delta proto.Message,
	ttl ...time.Duration) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	if delta == nil {
		return nil
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.UpdateProtobufWithTxn(ctx, txn, key, delta, ttl...)
	})
}

func (s *DistributedBadger) UpdateProtobufWithTxn(ctx context.Context, txn *Txn, key string, delta proto.Message,
	ttl ...time.Duration) error {
	return txn.UpdateProtobuf(ctx, key, delta, ttl...)
}

// DBDeleteOps Interface

func (s *DistributedBadger) Delete(ctx context.Context, key string) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.DeleteWithTxn(ctx, txn, key)
	})
}

func (s *DistributedBadger) DeleteWithTxn(ctx context.Context, txn *Txn, key string) error {
	return txn.Delete(ctx, key)
}

func (s *DistributedBadger) DeleteByPrefix(ctx context.Context, prefix string) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return s.DeleteByPrefixWithTxn(ctx, txn, prefix)
	})
}

func (s *DistributedBadger) DeleteByPrefixWithTxn(ctx context.Context, txn *Txn, prefix string) error {
	return txn.DeleteByPrefix(ctx, prefix)
}

// Database Interface

func (s *DistributedBadger) GetBadger() *badger.DB {
	return s.fsm.db.GetBadger()
}

func (s *DistributedBadger) NewTransaction(ctx context.Context, performOps func(context.Context, *Txn) error) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	txn, err := s.tm.BeginTxn(ctx, true)
	if err != nil {
		s.log.Error("Failed to begin transaction", zap.Error(err))
		return err
	}
	defer txn.Discard()

	s.log.Debug("Starting new transaction", zap.Uint64("readTs", txn.readTs), zap.String("txnId", txn.id))

	err = performOps(ctx, txn)
	if err != nil {
		s.log.Error("Failed to add subcommands to transaction", zap.Error(err))
		return err
	}

	cmd, err := txn.Commit()
	if err != nil {
		s.log.Error("Failed to commit transaction", zap.Error(err))
		return err
	}

	s.log.Info("Proposing transaction with subcommands", zap.String("txnId", txn.id), zap.String("cmdId", cmd.Id),
		zap.Int("subCommandsCount", len(cmd.SubCommands)))
	return s.proposeAndWait(cmd)
}

func (s *DistributedBadger) Close(ctx context.Context) {
	if s.tso != nil {
		s.tso.Close()
	}

	if s.node != nil {
		s.node.Stop(ctx)
	}

	if s.fsm != nil {
		s.fsm.Close(ctx)
	}
}

// Helpers

func (s *DistributedBadger) proposeTxnSubCommand(ctx context.Context, performOps func(context.Context, *Txn) error) error {
	txn, err := s.tm.BeginTxn(ctx, true)
	if err != nil {
		s.log.Error("Failed to begin transaction", zap.Error(err))
		return err
	}
	defer txn.Discard()

	err = performOps(ctx, txn)
	if err != nil {
		s.log.Error("Failed to perform operations in transaction", zap.Error(err))
		return err
	}

	txnCmd, err := txn.Commit()
	if err != nil {
		s.log.Error("Failed to commit transaction", zap.Error(err))
		return err
	}

	if len(txnCmd.SubCommands) == 0 {
		s.log.Debug("No subcommands to propose")
		return nil
	}

	cmd := txnCmd.SubCommands[0]
	cmd.ReadTs = txn.readTs
	cmd.CommitTs = txn.commitTs

	return s.proposeAndWait(cmd)
}

func (s *DistributedBadger) proposeAndWait(cmd *types.Command) error {
	b, err := cmd.Encode()
	if err != nil {
		return err
	}
	errCh := make(chan error)
	s.proposals.Store(cmd.Id, errCh)

	start := time.Now()
	s.node.ProposeReqNotifier() <- propose{cmdID: cmd.Id, cmd: b}

	select {
	case err := <-errCh:
		s.proposals.Delete(cmd.Id)
		close(errCh)

		s.log.Debug("Propose completed", zap.String("cmdID", cmd.Id),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return err
	case <-time.After(DefaultProposeTimeout):
		s.proposals.Delete(cmd.Id)
		close(errCh)
		return ErrProposeTimeout
	}
}

var _ Database[*Txn] = (*DistributedBadger)(nil)
