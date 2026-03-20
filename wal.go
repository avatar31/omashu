package omashu

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/avatar31/omashu/utils"
)

type Wal struct {
	waldir      string
	snapdir     string
	wal         *wal.WAL
	snapshotter *snap.Snapshotter
	existingWAL bool

	log *zap.Logger
	mu  sync.RWMutex
}

func newWal(baseDir string, log *zap.Logger) (*Wal, error) {
	waldir := filepath.Join(baseDir, "wal")
	if err := utils.CreateDirIfNotExists(waldir); err != nil {
		log.Error("Error creating wal directory", zap.Error(err))
		return nil, err
	}

	snapdir := filepath.Join(baseDir, "snap")
	if err := utils.CreateDirIfNotExists(snapdir); err != nil {
		log.Error("Error creating snap directory", zap.Error(err))
		return nil, err
	}

	err := utils.RemoveMatchFile(snapdir, func(filename string) bool {
		return strings.HasPrefix(filename, "tmp")
	})
	if err != nil {
		log.Error("Error removing tmp files in snap directory", zap.Error(err))
		return nil, err
	}

	return &Wal{
		waldir:      waldir,
		snapdir:     snapdir,
		log:         log,
		existingWAL: wal.Exist(waldir),
	}, nil
}

func (w *Wal) Open(ctx context.Context) (*raftpb.Snapshot, *raftpb.HardState, []raftpb.Entry, error) {
	w.snapshotter = snap.New(w.log, w.snapdir)
	snapshot, err := w.loadLatestSnapshot()
	if err != nil {
		return nil, nil, nil, err
	}

	walInt, err := w.openWAL(snapshot)
	if err != nil {
		return nil, nil, nil, err
	}
	w.wal = walInt

	// TODO: P2: Can we make use of metadata to store some useful info
	_, hardState, entries, err := w.wal.ReadAll()
	if err != nil {
		w.log.Error("Error reading wal records", zap.Error(err))
		return nil, nil, nil, err
	}

	w.log.Info("Current WAL state", zap.String("HardState", raft.DescribeHardState(hardState)))

	// TODO: P1: Do we need to make reading wal as async
	// rc.snapshotterReadyNotifier <- rc.snapshotter

	return snapshot, &hardState, entries, nil
}

func (w *Wal) loadLatestSnapshot() (*raftpb.Snapshot, error) {
	if !wal.Exist(w.waldir) {
		w.log.Info("WAL does not exist, starting fresh")
		return &raftpb.Snapshot{}, nil
	}

	// Find a snapshot to start/restart a raft node
	walSnaps, err := wal.ValidSnapshotEntries(w.log, w.waldir)
	if err != nil {
		w.log.Error("Error listing wal snapshots", zap.Error(err))
		return nil, err
	}

	// snapshot files can be orphaned if app crashes after writing them
	// but before writing the corresponding bwal log entries
	snapshot, err := w.snapshotter.LoadNewestAvailable(walSnaps)
	if err != nil && !errors.Is(err, snap.ErrNoSnapshot) {
		w.log.Error("Error loading latest snapshot", zap.Error(err))
		return nil, err
	}

	return snapshot, nil
}

func (w *Wal) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(w.waldir) {
		walInt, err := wal.Create(w.log, w.waldir, nil)
		if err != nil {
			w.log.Error("Error creating new wal", zap.Error(err))
			return nil, err
		}
		w.log.Info("Created new WAL")
		walInt.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	w.log.Info(fmt.Sprintf("Loading WAL at term %d and index %d", walsnap.Term, walsnap.Index))
	walInt, err := wal.Open(w.log, w.waldir, walsnap)
	if err != nil {
		w.log.Error("Error opening wal", zap.Error(err))
		return nil, err
	}
	return walInt, nil
}

func (w *Wal) SaveState(hardState raftpb.HardState, entries []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wal.Save(hardState, entries); err != nil {
		w.log.Error("Error saving wal state", zap.Error(err))
		return err
	}
	return nil
}

func (w *Wal) SaveSnap(snap *raftpb.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := w.snapshotter.SaveSnap(*snap); err != nil {
		w.log.Error("Error saving snapshot file", zap.Error(err))
		return err
	}

	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	if err := w.wal.SaveSnapshot(walSnap); err != nil {
		w.log.Error("Error saving wal snapshot", zap.Error(err))
		return err
	}

	return nil
}

func (w *Wal) Release(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wal.ReleaseLockTo(index); err != nil {
		w.log.Error("Error releasing wal lock for index", zap.Error(err), zap.Uint64("index", index))
		return err
	}
	return nil
}

// TODO: P1: Analyze impact of Sync
// Because I am getting "bad file descriptor" error on calling w.wal.Close()
func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wal.Sync(); err != nil {
		w.log.Error("Error syncing wal to disk", zap.Error(err))
		return err
	}
	return nil
}

func (w *Wal) ExistingWAL() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.existingWAL
}

// Exists checks if WAL and snapshot directories exist and contain data
// Used to determine if this is a new node or restarting node
func (w *Wal) Exists() (walExists bool, snapExists bool, err error) {
	// Check WAL existence
	walExists = wal.Exist(w.waldir)
	snapExists = true

	// Check snapshot existence using etcd's snapshotter
	// If Load() succeeds, a valid snapshot exists
	_, err = w.snapshotter.Load()
	if err != nil {
		if !errors.Is(err, snap.ErrNoSnapshot) {
			// For errors other than no snapshot, return the error
			return walExists, false, err
		}
		snapExists = false
		err = nil
	}

	return walExists, snapExists, err
}

func (w *Wal) Close() error {
	if w.wal != nil {
		return w.wal.Close()
	}
	return nil
}
