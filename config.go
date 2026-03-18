package omashu

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

type Cluster interface {
	GetClusterID() uint64
	IsNodeRemoved(id uint64) bool
}

type RaftConfig struct {
	raft.Config
	Nodename string
	Peers    map[uint64]string
}

type Config struct {
	Name    string
	BaseDir string

	BadgerOptions badger.Options
	RaftConfig    *RaftConfig
	Cluster       Cluster

	Managed bool
	Logger  *zap.Logger

	// Hooks
	OnLeaderChange func(prevLeader, newLeader uint64)
	OnRemovedSelf  func()
}

func (cfg *Config) validate() error {
	if cfg.Cluster == nil {
		return ErrMissingCluster
	}

	if cfg.BaseDir == "" {
		return ErrMissingBaseDir
	}

	if cfg.Managed && cfg.RaftConfig == nil {
		return ErrMissingRaftConf
	}

	if cfg.Name == "" {
		cfg.Name = "omashu"
	}

	cfg.BadgerOptions = cfg.BadgerOptions.WithDir(fmt.Sprintf("%s/%s", cfg.BaseDir, DBSubDir))
	return nil
}

func (cfg *Config) initializeLog() {
	if cfg.Logger == nil {
		cfg.BadgerOptions = cfg.BadgerOptions.WithLogger(nil)
		cfg.Logger = zap.NewNop()
		return
	}

	cfg.BadgerOptions = cfg.BadgerOptions.WithLogger(newLogger(fmt.Sprintf("%s.badger", cfg.Name), cfg.Logger))
}

func newLogger(module string, log *zap.Logger) raft.Logger {
	return &zapBadgerLogger{log: log.With(zap.String("sub_module", module))}
}

type zapBadgerLogger struct {
	log *zap.Logger
}

func (zl *zapBadgerLogger) Error(args ...any) {
	zl.log.Error(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Errorf(format string, args ...any) {
	zl.log.Error(fmt.Sprintf(format, args...))
}

func (zl *zapBadgerLogger) Warning(args ...any) {
	zl.log.Warn(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Warningf(format string, args ...any) {
	zl.log.Warn(fmt.Sprintf(format, args...))
}

func (zl *zapBadgerLogger) Info(args ...any) {
	zl.log.Info(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Infof(format string, args ...any) {
	zl.log.Info(fmt.Sprintf(format, args...))
}

func (zl *zapBadgerLogger) Debug(args ...any) {
	zl.log.Debug(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Debugf(format string, args ...any) {
	zl.log.Debug(fmt.Sprintf(format, args...))
}

func (zl *zapBadgerLogger) Fatal(args ...any) {
	zl.log.Fatal(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Fatalf(format string, args ...any) {
	zl.log.Fatal(fmt.Sprintf(format, args...))
}

func (zl *zapBadgerLogger) Panic(args ...any) {
	zl.log.Panic(fmt.Sprint(args...))
}

func (zl *zapBadgerLogger) Panicf(format string, args ...any) {
	zl.log.Panic(fmt.Sprintf(format, args...))
}
