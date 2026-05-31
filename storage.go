/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Storage implements the raft.Storage interface and bridges the etcd
// raft.MemoryStorage with a persistent WAL and Snapshotter. It serialises
// concurrent reads and writes with a read-write mutex.
type Storage struct {
	wal        *Wal
	memStorage *raft.MemoryStorage

	mu  sync.RWMutex
	log *zap.Logger
}

// newStorage creates a Storage backed by a Wal rooted at baseDir. The
// in-memory raft.MemoryStorage is not populated until Initialize is called.
func newStorage(baseDir string, log *zap.Logger) (*Storage, error) {
	s := &Storage{log: log}
	wal, err := newWal(baseDir, s.log)
	if err != nil {
		return nil, err
	}

	s.wal = wal
	return s, nil
}

// Initialize replays the WAL into raft.MemoryStorage. It loads the latest
// snapshot (if any), restores the hard state, and appends all subsequent WAL
// entries so that the storage is ready for raft.RestartNode or
// raft.StartNode. Must be called exactly once after newStorage.
func (s *Storage) Initialize(ctx context.Context) error {
	s.memStorage = raft.NewMemoryStorage()
	latestSnap, hardState, entries, err := s.wal.Open(ctx)
	if err != nil {
		return err
	}

	if latestSnap != nil && !raft.IsEmptySnap(*latestSnap) {
		err = s.memStorage.ApplySnapshot(*latestSnap)
		if err != nil {
			s.log.Error("Error applying snapshot to memory storage", zap.Error(err))
			/* trunk-ignore(golangci-lint2/errcheck) */
			s.wal.Close()
			return err
		}
		s.log.Info("Loaded snapshot from wal", zap.String("snapshot", raft.DescribeSnapshot(*latestSnap)))
	} else {
		s.log.Info("No existing snapshots found, starting with empty state")
	}

	// Restore hard state (term, vote, commit index)
	if !raft.IsEmptyHardState(*hardState) {
		err = s.memStorage.SetHardState(*hardState)
		if err != nil {
			s.log.Error("Error setting hard state to memory storage", zap.Error(err))
			/* trunk-ignore(golangci-lint2/errcheck) */
			s.wal.Close()
			return err
		}
		s.log.Info("Hard state restored from wal", zap.String("hardState", raft.DescribeHardState(*hardState)))
	}

	// Append WAL entries to memory storage
	if len(entries) > 0 {
		err = s.memStorage.Append(entries)
		if err != nil {
			s.log.Error("Error appending entries to memory storage", zap.Error(err))
			/* trunk-ignore(golangci-lint2/errcheck) */
			s.wal.Close()
			return err
		}
		s.log.Info(fmt.Sprintf("Replayed %d WAL entries (index %d to %d)",
			len(entries), entries[0].Index, entries[len(entries)-1].Index))
	}

	s.log.Info("Storage initialized successfully")
	return nil
}

// SaveState persists the Raft Ready state: snapshot file, hard state, and
// log entries. The snapshot and WAL entry are always written first to
// ensure safe recovery after a crash mid-Ready. The in-memory storage is
// then updated to match. Must be called on every Ready before Advance.
func (s *Storage) SaveState(ctx context.Context, rd *raft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Must save the snapshot file and WAL snapshot entry before saving any other entries
	// or hardstate to ensure that recovery after a snapshot restore is possible.
	if err := s.saveStateToWal(rd); err != nil {
		return err
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := s.memStorage.ApplySnapshot(rd.Snapshot); err != nil {
			if err != raft.ErrSnapOutOfDate {
				return err
			}
			s.log.Warn("Snapshot out of date, skipping save to storage", zap.Error(err))
		}
	}

	if !raft.IsEmptyHardState(rd.HardState) {
		if err := s.memStorage.SetHardState(rd.HardState); err != nil {
			s.log.Error("Error setting hard state to memory storage", zap.Error(err))
			return err
		}
	}

	if err := s.memStorage.Append(rd.Entries); err != nil {
		s.log.Error("Error appending entries to memory storage", zap.Error(err))
		return err
	}

	return nil
}

// saveStateToWal writes the snapshot file (if present), hard state, and log
// entries to the WAL, then syncs and releases the WAL lock. Callers must
// hold s.mu (exclusive). The snapshot is persisted before the WAL entry to
// avoid an unrecoverable WAL entry with no corresponding snapshot file.
func (s *Storage) saveStateToWal(rd *raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := s.wal.SaveSnap(&rd.Snapshot); err != nil {
			return err
		}
		s.log.Info("Snapshot and state are saved to wal", zap.Uint64("index", rd.Snapshot.Metadata.Index),
			zap.String("snapshot", raft.DescribeSnapshot(rd.Snapshot)),
			zap.Int("sizeInBytes", len(rd.Snapshot.Data)))
	}

	if err := s.wal.SaveState(rd.HardState, rd.Entries); err != nil {
		return err
	}

	// Force WAL to fsync its hard state before Release() releases
	// old data from the WAL. Otherwise could get an error like:
	// Was the raft log corrupted, truncated, or lost?
	// See https://github.com/etcd-io/etcd/issues/10219 for more details.
	if err := s.wal.Sync(); err != nil {
		return err
	}

	if err := s.wal.Release(rd.Snapshot.Metadata.Index); err != nil {
		return err
	}

	return nil
}

// Existing reports whether the WAL directory contained data before this
// storage was opened. The node uses this to distinguish a new cluster member
// (raft.StartNode) from a rejoining one (raft.RestartNode).
func (s *Storage) Existing() bool {
	return s.wal.ExistingWAL()
}

// LastSnapshotIndex returns the Raft log index of the most recent snapshot
// held in memory storage, or 0 if no snapshot exists yet.
func (s *Storage) LastSnapshotIndex() uint64 {
	snap, err := s.memStorage.Snapshot()
	if err != nil {
		return 0
	}
	return snap.Metadata.Index
}

// CreateSnapshot captures the application state (data) at the given log index,
// writes the snapshot file and WAL entry, applies the snapshot to memory
// storage, syncs, and compacts the memory log up to index. Typically called
// by takeSnapshotIfNeeded after the FSM produces a snapshot.
func (s *Storage) CreateSnapshot(index uint64, confState raftpb.ConfState, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get term for the index
	term, err := s.memStorage.Term(index)
	if err != nil {
		return fmt.Errorf("failed to get term for index %d: %w", index, err)
	}

	snapshot := raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: confState,
		},
	}

	// Save snapshot using etcd's snapshotter
	if err := s.wal.SaveSnap(&snapshot); err != nil {
		return err
	}

	// Apply the new snapshot to in-memory storage so that Storage.Snapshot() returns
	// up-to-date data. Without this, when raft needs to bring a lagging follower up-to-date
	// it would call Storage.Snapshot(), get the stale old snapshot from memStorage, and send
	// that outdated data — even though log entries up to this index are about to be compacted.
	// The follower would receive a snapshot that is already behind the compaction point,
	// causing it to loop forever asking for entries that no longer exist.
	if err := s.memStorage.ApplySnapshot(snapshot); err != nil && err != raft.ErrSnapOutOfDate {
		return fmt.Errorf("failed to apply snapshot to memory storage: %w", err)
	}

	// Force WAL to fsync its hard state before Release() releases
	// old data from the WAL. Otherwise could get an error like:
	// Was the raft log corrupted, truncated, or lost?
	// See https://github.com/etcd-io/etcd/issues/10219 for more details.
	if err := s.wal.Sync(); err != nil {
		return err
	}

	if err := s.wal.Release(snapshot.Metadata.Index); err != nil {
		return err
	}

	if err := s.compact(index); err != nil {
		return err
	}

	s.log.Info("Snapshot created and memory compacted", zap.Uint64("index", snapshot.Metadata.Index),
		zap.Uint64("term", snapshot.Metadata.Term),
		zap.Int("sizeInBytes", len(snapshot.Data)))
	return nil
}

// Compact advances the memory storage compaction frontier to index, freeing
// log entries no longer needed. Safe to call if storage is already compacted
// past index.
func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compact(index)
}

// compact removes log entries from memory storage up to and including index.
// Callers must hold s.mu (exclusive). ErrCompacted is swallowed silently
// since it means storage is already at least as compact as requested.
func (s *Storage) compact(index uint64) error {
	if err := s.memStorage.Compact(index); err != nil {
		if err == raft.ErrCompacted {
			// Already compacted to this index or beyond, not an error
			s.log.Debug("Memory storage already compacted to index", zap.Uint64("index", index))
			return nil
		}
		return err
	}

	s.log.Info("Memory storage compacted to index", zap.Uint64("index", index))
	return nil
}

// Close flushes and closes the WAL. Must be called exactly once when the
// Raft node shuts down.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

// The following methods implement the raft.Storage interface. Each acquires
// a read lock and delegates to the underlying raft.MemoryStorage.

// InitialState returns the saved HardState and ConfState for this node.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.InitialState()
}

// Entries returns log entries in the half-open range [lo, hi). maxSize
// limits the total byte size of the returned slice.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.Entries(lo, hi, maxSize)
}

// Term returns the term of the log entry at index i.
func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.Term(i)
}

// LastIndex returns the index of the last entry in the log.
func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.LastIndex()
}

// FirstIndex returns the index of the first log entry that has not yet been
// compacted (entries before it are no longer available).
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.FirstIndex()
}

// Snapshot returns the most recent snapshot held in memory storage.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memStorage.Snapshot()
}
