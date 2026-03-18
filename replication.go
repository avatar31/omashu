package omashu

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

const (
	// maxRestorePendingTxns controls how many transactions Badger pipelines
	// concurrently while loading a snapshot. The original value of 1 forced
	// single-threaded ingestion which is extremely slow on large databases.
	// 256 keeps memory pressure reasonable while allowing Badger to batch
	// writes efficiently.
	maxRestorePendingTxns = 256
)

type Replicator interface {
	// TakeSnapshot captures a complete, consistent backup of the database and
	// returns the highest version included along with the serialised bytes.
	TakeSnapshot(ctx context.Context) (uint64, []byte, error)
	Restore(ctx context.Context, data []byte) error
}

type Replicate struct {
	db  *BadgerDB
	log *zap.Logger
}

func NewReplicator(db Database, log *zap.Logger) (Replicator, error) {
	bdb, ok := db.(*BadgerDB)
	if !ok {
		return nil, errors.New("invalid db instance")
	}

	return &Replicate{db: bdb, log: log}, nil
}

// TakeSnapshot creates a full in-memory backup of the database.
//
// Why since=0:
// Badger's Backup(w, since) includes only key versions with commitTs >= since.
// The TSO's CurrentReadTs() returns a composite timestamp (physical_ms << 18 | logical)
// that represents the *current* (maximum) version. Passing it as `since` would include
// only versions committed *after* now — i.e. nothing — producing an empty snapshot.
// Passing 0 captures every version of every key from the beginning, which is exactly
// what a full raft snapshot must contain so a follower can reconstruct identical state.
//
// Why bytes.Buffer instead of a temp file:
// The snapshot data must become []byte to embed in raftpb.Snapshot.Data regardless.
// The previous file roundtrip (Create → Backup → Close → Stat → ReadFull → Remove)
// added unnecessary disk I/O and latency without any benefit. Writing directly into
// a bytes.Buffer eliminates the intermediate disk step entirely.
func (r *Replicate) TakeSnapshot(ctx context.Context) (uint64, []byte, error) {
	var buf bytes.Buffer
	upto, err := r.db.DB.Backup(&buf, 0)
	if err != nil {
		return 0, nil, err
	}
	return upto, buf.Bytes(), nil
}

// Restore replaces the entire database with the contents of a raft snapshot.
//
// Why DropAll before Load:
// DB.Load() is a merge operation — it inserts snapshot key-versions into the
// existing database but does NOT remove any data that is already there.
// For raft snapshot restoration this is incorrect: the snapshot represents the
// authoritative, complete state of the FSM at a specific log index. Any keys
// (or key-versions) that exist in the local DB but are absent from the snapshot
// — e.g. after a diverged log branch, a previous partial restore that crashed,
// or a follower catching up from behind a compaction boundary — must be erased.
// DropAll() clears all data while keeping the DB instance open and usable,
// providing the clean slate that Load() needs to faithfully reconstruct state.
func (r *Replicate) Restore(ctx context.Context, data []byte) error {
	if err := r.db.DB.DropAll(); err != nil {
		return fmt.Errorf("failed to drop existing data before snapshot restore: %w", err)
	}

	if err := r.db.DB.Load(bytes.NewReader(data), maxRestorePendingTxns); err != nil {
		return err
	}
	r.log.Info("Restored database from snapshot successfully")
	return nil
}
