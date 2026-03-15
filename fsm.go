package omashu

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/types"
)

type FSM struct {
	db  Database
	tso *TSO

	mu  sync.RWMutex
	log *zap.Logger
}

func NewFSM(database Database, logger *zap.Logger) *FSM {
	return &FSM{
		db:  database,
		log: logger,
	}
}

func (fsm *FSM) SetTSO(tso *TSO) {
	fsm.tso = tso
}

// Apply applies a committed log entry to the state machine
func (fsm *FSM) Apply(ctx context.Context, data []byte) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	c, err := types.DecodeCommand(data)
	if err != nil {
		fsm.log.Error("Failed to entry data", zap.Error(err))
		return err
	}

	var applyErr error
	switch c.Type {
	case types.CommandType_SET:
		applyErr = fsm.applySet(ctx, c)
	case types.CommandType_UPDATE:
		applyErr = fsm.applyUpdate(ctx, c)
	case types.CommandType_DELETE:
		applyErr = fsm.applyDelete(ctx, c)
	case types.CommandType_DELETE_BY_PREFIX:
		applyErr = fsm.applyDeleteByPrefix(ctx, c)
	case types.CommandType_BATCH_WRITE:
		applyErr = fsm.applyBatchWrite(ctx, c)
	case types.CommandType_INCR_BY:
		_, applyErr = fsm.applyIncrBy(ctx, c)
	case types.CommandType_DECR_BY:
		_, applyErr = fsm.applyDecrBy(ctx, c)
	case types.CommandType_TRANSACTION:
		applyErr = fsm.applyTransaction(ctx, c)
	default:
		fsm.log.Error("Unknown command type", zap.String("command", c.Type.String()))
		applyErr = ErrUnknownOp
	}

	return applyErr
}

func (fsm *FSM) getTtl(cmd *types.Command) []time.Duration {
	ttl := make([]time.Duration, 0)
	if cmd.Ttl != nil {
		ttl = append(ttl, cmd.Ttl.AsDuration())
	}
	return ttl
}

func (fsm *FSM) applySet(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.SetWithTxn(ctx, txn, cmd.Key, cmd.Value, fsm.getTtl(cmd)...)
	})
}

func (fsm *FSM) applyUpdate(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		switch cmd.UpdateMeta.UpdateDeltaType {
		case types.UpdateDeltaType_PROTOBUF:
			v, err := cmd.UnmarshalUpdateDelta()
			if err != nil {
				fsm.log.Error("Failed to unmarshal delta protobuf", zap.Error(err))
				return err
			}

			msg, _ := v.(proto.Message)
			err = fsm.db.UpdateProtobufWithTxn(ctx, txn, cmd.Key, msg, fsm.getTtl(cmd)...)
			if err != nil {
				return err
			}
		case types.UpdateDeltaType_JSON:
			v, err := cmd.UnmarshalUpdateDelta()
			if err != nil {
				fsm.log.Error("Failed to unmarshal delta JSON", zap.Error(err))
				return err
			}

			msg, _ := v.(map[string]any)
			err = fsm.db.UpdateJsonWithTxn(ctx, txn, cmd.Key, msg, fsm.getTtl(cmd)...)
			if err != nil {
				return err
			}
		default:
			fsm.log.Error("Unknown merge delta type:", zap.String("deltaMergeType",
				cmd.UpdateMeta.UpdateDeltaType.String()))
			return ErrUnknownOp
		}

		return nil
	})
}

func (fsm *FSM) applyDelete(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.DeleteWithTxn(ctx, txn, cmd.Key)
	})
}

func (fsm *FSM) applyDeleteByPrefix(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		fsm.db.DeleteByPrefixWithTxn(ctx, txn, cmd.Prefix)
		return nil
	})
}

func (fsm *FSM) applyBatchWrite(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.BatchWriteWithTxn(ctx, txn, cmd.SubCommands)
	})
}

func (fsm *FSM) applyIncrBy(ctx context.Context, cmd *types.Command) (uint64, error) {
	newVal := uint64(0)
	err := fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		var opErr error
		newVal, opErr = fsm.db.IncrByWithTxn(ctx, txn, cmd.Key, cmd.IncrOrDecrDelta)
		return opErr
	})
	return newVal, err
}

func (fsm *FSM) applyDecrBy(ctx context.Context, cmd *types.Command) (uint64, error) {
	newVal := uint64(0)
	err := fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		var opErr error
		newVal, opErr = fsm.db.DecrByWithTxn(ctx, txn, cmd.Key, cmd.IncrOrDecrDelta)
		return opErr
	})
	return newVal, err
}

func (fsm *FSM) applyTransaction(ctx context.Context, cmd *types.Command) error {
	return fsm.db.NewTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		for _, subCmd := range cmd.SubCommands {
			switch subCmd.Type {
			case types.CommandType_SET:
				err := fsm.db.SetWithTxn(ctx, txn, subCmd.Key, subCmd.Value, fsm.getTtl(subCmd)...)
				if err != nil {
					return err
				}
			case types.CommandType_UPDATE:
				ttl := fsm.getTtl(subCmd)
				switch subCmd.UpdateMeta.UpdateDeltaType {
				case types.UpdateDeltaType_PROTOBUF:
					v, err := subCmd.UnmarshalUpdateDelta()
					if err != nil {
						fsm.log.Error("Failed to unmarshal delta protobuf", zap.Error(err))
						return err
					}

					msg, _ := v.(proto.Message)
					err = fsm.db.UpdateProtobufWithTxn(ctx, txn, subCmd.Key, msg, ttl...)
					if err != nil {
						return err
					}
				case types.UpdateDeltaType_JSON:
					v, err := subCmd.UnmarshalUpdateDelta()
					if err != nil {
						fsm.log.Error("Failed to unmarshal delta JSON", zap.Error(err))
						return err
					}

					msg, _ := v.(map[string]any)
					err = fsm.db.UpdateJsonWithTxn(ctx, txn, subCmd.Key, msg, ttl...)
					if err != nil {
						return err
					}
				default:
					fsm.log.Error("Unknown merge delta type:", zap.String("deltaMergeType",
						subCmd.UpdateMeta.UpdateDeltaType.String()))
					return ErrUnknownOp
				}
			case types.CommandType_DELETE:
				err := fsm.db.DeleteWithTxn(ctx, txn, subCmd.Key)
				if err != nil {
					return err
				}
			case types.CommandType_INCR_BY:
				_, err := fsm.db.IncrByWithTxn(ctx, txn, subCmd.Key, subCmd.IncrOrDecrDelta)
				if err != nil {
					return err
				}
			case types.CommandType_DECR_BY:
				_, err := fsm.db.DecrByWithTxn(ctx, txn, subCmd.Key, subCmd.IncrOrDecrDelta)
				if err != nil {
					return err
				}
			default:
				return ErrUnknownOp
			}
		}
		return nil
	})
}

func (fsm *FSM) GetDB() Database {
	return fsm.db
}

func (fsm *FSM) RestoreSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	replicator, err := NewReplicator(fsm.db, fsm.log)
	if err != nil {
		fsm.log.Error("Failed to create DB replicator", zap.Error(err))
		return err
	}

	if err := replicator.Restore(ctx, snapshot.Data); err != nil {
		fsm.log.Error("Failed to restore snapshot to DB", zap.Error(err))
		return err
	}
	return nil
}

func (fsm *FSM) CreateSnapshot(ctx context.Context) (uint64, []byte, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fsm.tso == nil {
		fsm.log.Panic("TSO is not set in FSM")
	}

	replicator, err := NewReplicator(fsm.db, fsm.log)
	if err != nil {
		return 0, nil, err
	}

	generation := fsm.tso.CurrentReadTs()
	fsm.log.Info("Creating snapshot with generation", zap.Uint64("generation", generation))
	upto, snapName, err := replicator.TakeSnapshot(ctx, generation)
	if err != nil {
		return 0, nil, err
	}
	defer replicator.DeleteSnapshot(ctx, snapName)

	content, err := replicator.ReadSnapshotContent(ctx, snapName)
	if err != nil {
		return 0, nil, err
	}

	return upto, content, nil
}
