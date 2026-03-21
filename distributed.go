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
// - Add support for multi-region deployments and cross-region replication
// - Implement backup and restore functionality for the DBStore
// - Add documentation and usage examples for DBStore API
// - Implement security features like encryption and authentication for Raft communication
// - Optimize transaction management for high concurrency scenarios
// - Add support for different storage backends (e.g., in-memory, distributed storage)
// - Implement advanced conflict resolution strategies for transactions
// - Add support for complex queries and indexing in the DBStore
// - Implement performance benchmarking and optimization for DBStore operations
// - Add support for schema migrations and versioning in the DBStore
// - Implement a web-based dashboard for monitoring and managing the DBStore and Raft cluster
// - Add support for custom command types and extensibility in the DBStore API
// - Implement load balancing and failover strategies for Raft nodes
// - Add support for event-driven architecture and notifications for DBStore changes
// - Implement advanced logging and tracing for debugging and performance analysis
// - Add support for distributed transactions across multiple DBStore instances
// - Implement a plugin system for extending DBStore functionality
// - Add support for real-time data streaming and change data capture from the DBStore
// - Implement advanced data consistency models (e.g., eventual consistency, strong consistency)
// - Add support for data sharding and partitioning in the DBStore
// - Implement a comprehensive testing framework for DBStore and Raft interactions
// - Add support for integration with external systems and services (e.g., message queues, caches)
// - Implement advanced data analytics and reporting features for the DBStore
// - Add support for user-defined functions and stored procedures in the DBStore
// - Implement a command-line interface (CLI) for managing and interacting with the DBStore
// - Add support for data versioning and auditing in the DBStore
// - Implement advanced security features like role-based access control (RBAC) for the DBStore
// - Add support for multi-tenant deployments of the DBStore
// - Implement advanced data compression and optimization techniques for the DBStore
// - Add support for real-time collaboration and concurrent editing in the DBStore
// - Implement a comprehensive documentation portal for the DBStore and its features
// - Add support for community contributions and open-source collaboration on the DBStore project
// - Implement advanced data visualization and exploration tools for the DBStore
// - Add support for machine learning and AI integration with the DBStore
// - Implement a roadmap and feature planning process for the DBStore project
// - Add support for continuous integration and deployment (CI/CD) for the DBStore project
// - Implement advanced data governance and compliance features for the DBStore
// - Add support for internationalization and localization in the DBStore
// - Implement a user feedback and feature request system for the DBStore project
// - Add support for community forums and discussion boards for the DBStore project
// - Implement advanced data lifecycle management and retention policies for the DBStore
// - Add support for third-party integrations and plugins for the DBStore
// - Implement a comprehensive security audit and vulnerability assessment for the DBStore
// - Add support for advanced data replication and synchronization strategies for the DBStore
// - Implement a community-driven roadmap and feature prioritization process for the DBStore project
// - Add support for advanced data recovery and disaster recovery strategies for the DBStore

const (
	DefaultProposeTimeout = 5 * time.Second

	// Subdirectories for different components of the store
	DBSubDir   = "db"
	WALSubDir  = "wal"
	SnapSubDir = "snap"
)

func initDistributed(ctx context.Context, db *Badger, cfg *Config) (*DistributedBadger, error) {
	instance := &DistributedBadger{
		log:                cfg.Logger,
		onLeaderChangeHook: cfg.OnLeaderChange,
		onRemovedSelfHook:  cfg.OnRemovedSelf,
	}

	fsm, err := newFSM(db, instance.log)
	if err != nil {
		return nil, err
	}
	instance.fsm = fsm

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

	if instance.isLeader() {
		instance.log.Info("Initializing TSO and TxnManager in Raft Leader node")

		tso, err := newTSO(ctx, instance, instance.log)
		if err != nil {
			instance.Close(ctx)
			return nil, err
		}

		instance.mu.Lock()
		instance.tso = tso
		instance.fsm.db.setOracle(tso)
		instance.tm = newTxnManager(instance, tso, instance.log)
		instance.leaderChangeNotifier = make(chan struct{})
		instance.mu.Unlock()

		go instance.listenProposeResponses(ctx)
	}

	return instance, nil
}

func (s *DistributedBadger) onLeaderChange(ctx context.Context, prevLeader, newLeader uint64) {
	if newLeader == s.node.id {
		s.log.Info("This node is now the leader, initializing TSO and TxnManager")
		tso, err := newTSO(ctx, s, s.log)
		if err != nil {
			s.Close(ctx)
			s.log.Panic("Failed to initialize TSO on leadership gain", zap.Error(err))
		}

		// Hold the write-lock for the shortest possible window: just the pointer
		// swaps. TSO construction (newTSO) reads from Badger and can be slow,
		// so it must happen before acquiring the lock.
		s.mu.Lock()
		s.tso = tso
		s.tm = newTxnManager(s, tso, s.log)
		s.leaderChangeNotifier = make(chan struct{})
		s.mu.Unlock()

		go s.listenProposeResponses(ctx)
	}

	if prevLeader == s.node.id {
		s.log.Info("This node is no longer the leader, stopping TSO and TxnManager")

		s.mu.Lock()
		prevTSO := s.tso
		s.tso = nil
		s.tm = nil
		notifier := s.leaderChangeNotifier
		s.leaderChangeNotifier = nil
		s.mu.Unlock()

		if prevTSO != nil {
			prevTSO.Close()
		}

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

// getTM safely returns the active TxnManager under a read-lock.
// Any leader-only operation must call this instead of accessing s.tm directly,
// because onLeaderChange (running in node.run() goroutine) can nil s.tm at any time.
func (s *DistributedBadger) getTM() (*TxnManager, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tm == nil {
		return nil, ErrNotLeader
	}
	return s.tm, nil
}

func (s *DistributedBadger) listenProposeResponses(ctx context.Context) {
	// Capture the notifier channel for THIS leadership term under a read-lock.
	// onLeaderChange replaces s.leaderChangeNotifier with a fresh channel each
	// time leadership is gained. If we read s.leaderChangeNotifier directly
	// inside the select without a captured reference, reading the field from
	// two goroutines (this one and onLeaderChange's writer) is a data race.
	s.mu.RLock()
	notifier := s.leaderChangeNotifier
	s.mu.RUnlock()

	for {
		select {
		case r := <-s.node.ProposeRespNotifier():
			if ch, ok := s.proposals.Load(r.CmdID); ok {
				errCh, _ := ch.(chan error)
				errCh <- r.Err
			} else {
				s.log.Warn("No proposal channel found for response", zap.String("cmdID", r.CmdID))
				if r.Err != nil {
					s.log.Error("Proposal failed with error", zap.String("cmdID", r.CmdID), zap.Error(r.Err))
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
	if !s.isLeader() {
		return nil
	}

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
	if err := s.waitForReadState(ctx, key); err != nil {
		return nil, false, err
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

	tm, err := s.getTM()
	if err != nil {
		return err
	}
	txn, err := tm.BeginTxn(ctx, true)
	if err != nil {
		s.log.Error("Failed to begin transaction", zap.Error(err))
		return err
	}
	defer txn.Discard()

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
	// Re-check under lock: leadership may have changed between the !IsLeader()
	// guard in the caller and reaching here (TOCTOU). getTM() is the
	// authoritative, race-free gate for all leader-only mutations.
	tm, err := s.getTM()
	if err != nil {
		return err
	}
	txn, err := tm.BeginTxn(ctx, true)
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
	s.node.ProposeNotifier() <- b

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
