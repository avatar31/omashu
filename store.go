package omashu

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/db"
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

type DBStore struct {
	proposals sync.Map

	fsm  *FSM
	node *Node
	tso  *TSO
	tm   *TxnManager
	log  *zap.Logger
}

const (
	DefaultProposeTimeout = 5 * time.Second
)

var (
	dbstoreInstance *DBStore
	once            sync.Once

	ErrNotLeader      = errors.New("operation can only be performed on leader node")
	ErrProposeTimeout = errors.New("raft propose timeout")
)

func InitDBStore(ctx context.Context, id uint64, nodename string, peers map[uint64]string, log *zap.Logger) {
	once.Do(func() {
		// Init DB
		// clusterMode := config.GetConfig().Mode == config.MODE_CLUSTER
		clusterMode := false
		err := db.InitDB(ctx, clusterMode, log)
		if err != nil {
			log.Panic("Failed to open application DB", zap.Error(err))
		}

		if !clusterMode {
			return
		}

		bdb := db.GetDB(ctx)

		fsm := NewFSM(bdb, log)

		// Init Raft Node
		node, err := NewNode(ctx, id, nodename, peers, fsm, log)
		if err != nil {
			db.Close(ctx, log)
			log.Panic("Failed to create raft node", zap.Error(err))
		}

		err = node.Start(ctx)
		if err != nil {
			db.Close(ctx, log)
			log.Panic("Failed to start raft node", zap.Error(err))
		}

		dbstoreInstance = &DBStore{
			fsm:  fsm,
			node: node,
			log:  log,
		}

		if dbstoreInstance.IsLeader() {
			dbstoreInstance.log.Info("Initializing TSO and TxnManager in Raft Leader node")

			tso, err := NewTSO(ctx, dbstoreInstance, log)
			if err != nil {
				node.Stop(ctx)
				db.Close(ctx, log)
				log.Panic("Failed to create TSO", zap.Error(err))
			}

			dbstoreInstance.tso = tso
			dbstoreInstance.fsm.SetTSO(tso)
			dbstoreInstance.tm = NewTxnManager(dbstoreInstance, log)

			go dbstoreInstance.listenProposeResponses(ctx)
		}
	})

	if dbstoreInstance == nil {
		log.Panic("Unknown error in initializing DB store instance")
	}
}

func GetDBStore(ctx context.Context) *DBStore {
	return dbstoreInstance
}

// IsLeader returns true if this node is the leader
func (s *DBStore) IsLeader() bool {
	return s.node.IsLeader()
}

func (s *DBStore) listenProposeResponses(ctx context.Context) {
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
		case <-ctx.Done():
			s.log.Info("Stopping listening ProposeResponses as context is done")
			return
		}
	}
}

func (s *DBStore) waitForReadState(ctx context.Context, key string) error {
	if !s.IsLeader() {
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

func (s *DBStore) BulkGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return s.fsm.GetDB().BulkGet(ctx, keys)
}

func (s *DBStore) Count(ctx context.Context, prefix string) int {
	return s.fsm.GetDB().Count(ctx, prefix)
}

func (s *DBStore) DecrBy(ctx context.Context, key string, delta uint64) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.DecrBy(ctx, key, delta)
	})
}

func (s *DBStore) Delete(ctx context.Context, key string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.Delete(ctx, key)
	})
}

func (s *DBStore) DeleteByPrefix(ctx context.Context, prefix string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.DeleteByPrefix(ctx, prefix)
	})
}

func (s *DBStore) Exists(ctx context.Context, key string) bool {
	if err := s.waitForReadState(ctx, key); err != nil {
		return false
	}
	return s.fsm.GetDB().Exists(ctx, key)
}

func (s *DBStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := s.waitForReadState(ctx, key); err != nil {
		return nil, false, err
	}
	return s.fsm.GetDB().Get(ctx, key)
}

func (s *DBStore) GetWithTxn(ctx context.Context, txn *Txn, key string) ([]byte, bool, error) {
	if err := s.waitForReadState(ctx, key); err != nil {
		return nil, false, err
	}
	result, found, err := s.fsm.GetDB().GetAt(ctx, key, txn.readTs)
	if err != nil || !found {
		return nil, found, err
	}

	txn.addReadKey(key)
	return result, found, nil
}

func (s *DBStore) GetByPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	return s.fsm.GetDB().GetByPrefix(ctx, prefix)
}

func (s *DBStore) GetByPrefixWithTxn(ctx context.Context, txn *Txn, prefix string) (map[string][]byte, error) {
	result, err := s.fsm.GetDB().GetByPrefixAt(ctx, prefix, txn.readTs)
	if err != nil {
		return nil, err
	}

	for k := range result {
		txn.addReadKey(k)
	}
	return result, nil
}

func (s *DBStore) GetKeysByPrefixWithTxn(ctx context.Context, txn *Txn, prefix string) ([]string, error) {
	keys, err := s.fsm.GetDB().GetKeysByPrefixAt(ctx, prefix, txn.readTs)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		txn.addReadKey(k)
	}
	return keys, nil
}

func (s *DBStore) HasChild(ctx context.Context, prefix string) bool {
	return s.fsm.GetDB().HasChild(ctx, prefix)
}

func (s *DBStore) IncrBy(ctx context.Context, key string, delta uint64) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.IncrBy(ctx, key, delta)
	})
}

func (s *DBStore) IterateByPrefix(ctx context.Context, prefix, startCursor string, limit *int,
	process func(k, v []byte) bool) (string, error) {
	return s.fsm.GetDB().IterateByPrefix(ctx, prefix, startCursor, limit, process)
}

func (s *DBStore) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.Set(ctx, key, value, ttl...)
	})
}

func (s *DBStore) UpdateJson(ctx context.Context, key string, delta map[string]any, ttl ...time.Duration) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.UpdateJson(ctx, key, delta, ttl...)
	})
}

func (s *DBStore) UpdateProtobuf(ctx context.Context, key string, deltaProtoMsg proto.Message,
	ttl ...time.Duration) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	if deltaProtoMsg == nil {
		return nil
	}

	return s.proposeTxnSubCommand(ctx, func(ctx context.Context, txn *Txn) error {
		return txn.UpdateProtobuf(ctx, key, deltaProtoMsg, ttl...)
	})
}

func (s *DBStore) BatchWrite(ctx context.Context, addSubCommands func(*types.Command) *types.Command) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	cmd := types.NewBatchWriteCommand(ctx)
	addSubCommands(cmd)
	if len(cmd.SubCommands) == 0 {
		return nil
	}

	if len(cmd.SubCommands) > db.MaxBatchSize {
		return db.ErrBatchTooBig
	}

	// Getting readTs and commitTs for batch to find conflicts
	// in the transactions running in same timeline
	txn := s.tm.BeginTxn(ctx, true)
	defer txn.Discard()

	for _, subCmd := range cmd.SubCommands {
		txn.conflictKeys[subCmd.Key] = struct{}{}
		txn.writes = append(txn.writes, subCmd.Key)
	}

	_, err := txn.Commit()
	if err != nil && !errors.Is(err, badger.ErrConflict) {
		// We are not interested in conflict errors for batch write as we are only using
		// txn to get timestamps and to find conflict in other transactions
		s.log.Error("Failed to get timestamps for batch write", zap.Error(err))
		return err
	}

	cmd.ReadTs = txn.readTs
	cmd.CommitTs = txn.commitTs
	return s.proposeAndWait(cmd)
}

func (s *DBStore) NewTransaction(ctx context.Context, performOps func(context.Context, *Txn) error) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	txn := s.tm.BeginTxn(ctx, true)
	defer txn.Discard()

	err := performOps(ctx, txn)
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

func (s *DBStore) proposeTxnSubCommand(ctx context.Context, performOps func(context.Context, *Txn) error) error {
	txn := s.tm.BeginTxn(ctx, true)
	defer txn.Discard()

	err := performOps(ctx, txn)
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

func (s *DBStore) proposeAndWait(cmd *types.Command) error {
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

func (s *DBStore) Close(ctx context.Context) {
	if s.tso != nil {
		s.tso.Close()
	}

	if s.node != nil {
		s.node.Stop(ctx)
	}

	db.Close(ctx, s.log)

	// err := s.log.Sync()
	// if err != nil {
	// 	logger.GetLogger(ctx).WithError(err).Error("Error while syncing raft zap logs")
	// }
}
