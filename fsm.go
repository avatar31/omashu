/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/avatar31/omashu/types"
)

// FSM (Finite State Machine) applies committed Raft log entries to the
// underlying BadgerDB store. It is the bridge between the Raft consensus
// layer and durable key-value storage.
type FSM struct {
	db  *Badger
	log *zap.Logger
}

// newFSM creates an FSM that applies committed entries to db.
func newFSM(db *Badger, log *zap.Logger) (*FSM, error) {
	return &FSM{db: db, log: log}, nil
}

// Apply decodes a committed Raft log entry and dispatches it to the
// appropriate apply* helper based on the command type. Returns
// [ErrUnknownOp] if the command type is not recognised.
func (fsm *FSM) Apply(ctx context.Context, data []byte) error {
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
	case types.CommandType_INCR_BY:
		applyErr = fsm.applyIncrBy(ctx, c)
	case types.CommandType_DECR_BY:
		applyErr = fsm.applyDecrBy(ctx, c)
	case types.CommandType_TRANSACTION:
		applyErr = fsm.applyTransaction(ctx, c)
	default:
		fsm.log.Error("Unknown command type", zap.String("command", c.Type.String()))
		applyErr = ErrUnknownOp
	}

	return applyErr
}

// getTtl extracts the optional TTL duration from cmd. Returns an empty slice
// when the command carries no TTL, which signals no expiry to BadgerDB.
func (fsm *FSM) getTtl(cmd *types.Command) []time.Duration {
	ttl := make([]time.Duration, 0)
	if cmd.Ttl != nil {
		ttl = append(ttl, cmd.Ttl.AsDuration())
	}
	return ttl
}

// applySet writes the key-value pair from cmd to BadgerDB at the command's
// commit timestamp inside a managed MVCC transaction.
func (fsm *FSM) applySet(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.SetWithTxn(ctx, txn, cmd.Key, cmd.Value, fsm.getTtl(cmd)...)
	})
}

// applyUpdate merges a JSON or Protobuf delta into the existing value stored
// at cmd.Key, writing the result at the command's commit timestamp.
func (fsm *FSM) applyUpdate(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
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

// applyDelete removes cmd.Key from BadgerDB at the command's commit
// timestamp.
func (fsm *FSM) applyDelete(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.DeleteWithTxn(ctx, txn, cmd.Key)
	})
}

// applyDeleteByPrefix removes all keys with cmd.Prefix from BadgerDB at the
// command's commit timestamp.
func (fsm *FSM) applyDeleteByPrefix(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.DeleteByPrefixWithTxn(ctx, txn, cmd.Prefix)
	})
}

// applyIncrBy increments the counter at cmd.Key by cmd.IncrOrDecrDelta at
// the command's commit timestamp.
func (fsm *FSM) applyIncrBy(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.IncrByWithTxn(ctx, txn, cmd.Key, cmd.IncrOrDecrDelta)
	})
}

// applyDecrBy decrements the counter at cmd.Key by cmd.IncrOrDecrDelta at
// the command's commit timestamp.
func (fsm *FSM) applyDecrBy(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
		return fsm.db.DecrByWithTxn(ctx, txn, cmd.Key, cmd.IncrOrDecrDelta)
	})
}

// applyTransaction applies a batch of sub-commands atomically inside a single
// managed BadgerDB transaction at the parent command's commit timestamp. All
// sub-commands succeed or none of them are persisted.
func (fsm *FSM) applyTransaction(ctx context.Context, cmd *types.Command) error {
	return fsm.db.newTransactionAt(ctx, cmd.ReadTs, cmd.CommitTs, func(ctx context.Context, txn *badger.Txn) error {
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
				err := fsm.db.IncrByWithTxn(ctx, txn, subCmd.Key, subCmd.IncrOrDecrDelta)
				if err != nil {
					return err
				}
			case types.CommandType_DECR_BY:
				err := fsm.db.DecrByWithTxn(ctx, txn, subCmd.Key, subCmd.IncrOrDecrDelta)
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

// RestoreSnapshot replaces the entire database with the contents of a Raft
// snapshot. Called when a follower falls too far behind the leader and must
// receive a full state transfer instead of incremental log entries.
func (fsm *FSM) RestoreSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
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

// CreateSnapshot produces a full, consistent backup of the database.
// Returns the highest MVCC version included (upto) and the serialised bytes.
// Called by takeSnapshotIfNeeded to supply data for a new Raft snapshot.
func (fsm *FSM) CreateSnapshot(ctx context.Context) (uint64, []byte, error) {
	replicator, err := NewReplicator(fsm.db, fsm.log)
	if err != nil {
		return 0, nil, err
	}

	upto, content, err := replicator.TakeSnapshot(ctx)
	if err != nil {
		return 0, nil, err
	}

	return upto, content, nil
}

// Close closes the underlying BadgerDB instance. Safe to call when db is nil.
func (fsm *FSM) Close(ctx context.Context) {
	if fsm.db != nil {
		fsm.db.Close(ctx)
	}
}
