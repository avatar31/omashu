package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/avatar31/omashu/utils"
)

const (
	SNAPSHOT_DIR_FORMAT = "2006_01_02_15_04_05.000000"
	SNAPSHOT_DIR        = ".snapshots"
)

type Replicator interface {
	GetLatestSnapshot(ctx context.Context) (string, error)
	ListSnapshots(ctx context.Context) ([]string, error)
	TakeSnapshot(ctx context.Context, generation uint64) (uint64, string, error)
	ReadSnapshotContent(ctx context.Context, snapName string) ([]byte, error)
	DeleteSnapshot(ctx context.Context, snapName string) error
	Restore(ctx context.Context, data []byte) error
}

type Replicate struct {
	Path string
	db   *BadgerDB
	log  zap.Logger
}

func NewReplicator(db Database) (Replicator, error) {
	bdb, ok := db.(*BadgerDB)
	if !ok {
		return nil, errors.New("invalid db instance")
	}

	err := utils.CreateDirIfNotExists(filepath.Join(bdb.Path, SNAPSHOT_DIR))
	if err != nil {
		return nil, err
	}

	return &Replicate{db: bdb}, nil
}

func (r *Replicate) TakeSnapshot(ctx context.Context, generation uint64) (uint64, string, error) {
	snapName := time.Now().UTC().Format(SNAPSHOT_DIR_FORMAT) + ".bak"
	file, err := os.Create(r.getSnapPath(snapName))
	if err != nil {
		return 0, "", err
	}
	defer file.Close()

	upto, err := r.db.DB.Backup(file, generation)
	return upto, snapName, err
}

func (r *Replicate) ReadSnapshotContent(ctx context.Context, snapName string) ([]byte, error) {
	file, err := os.Open(r.getSnapPath(snapName))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	snapshotData := make([]byte, stat.Size())
	_, err = file.Read(snapshotData)
	if err != nil {
		return nil, err
	}

	return snapshotData, nil
}

func (r *Replicate) DeleteSnapshot(ctx context.Context, snapName string) error {
	return os.Remove(r.getSnapPath(snapName))
}

func (r *Replicate) getSnapPath(snapName string) string {
	return filepath.Join(r.db.Path, SNAPSHOT_DIR, snapName)
}

func (r *Replicate) GetLatestSnapshot(ctx context.Context) (string, error) {
	return "", nil
}

func (r *Replicate) ListSnapshots(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (r *Replicate) Restore(ctx context.Context, data []byte) error {
	err := r.db.DB.Load(bytes.NewReader(data), 1) // Question: What is 2nd argument here?
	if err != nil {
		return err
	}

	r.log.Info("Restored database from snapshot successfully")
	return nil
}
