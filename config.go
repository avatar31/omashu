/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/avatar31/omashu/types"
)

type SchemaType string

var (
	SchemaTypeJson     SchemaType = "json"
	SchemaTypeProtobuf SchemaType = "protobuf"
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

type SchemaConfig struct {
	Type            SchemaType
	ProtoSchemaList []*descriptorpb.FileDescriptorSet
}

type Config struct {
	Name    string
	BaseDir string
	Logger  *zap.Logger

	GCInterval     time.Duration
	GCDiscardRatio float64
	BadgerOptions  badger.Options

	RaftConfig *RaftConfig
	Cluster    Cluster

	SchemaConfig *SchemaConfig

	// Hooks
	OnLeaderChange func(prevLeader, newLeader uint64)
	OnRemovedSelf  func()
}

func (cfg *Config) validate(distributed bool) error {
	if cfg.Cluster == nil {
		return ErrMissingCluster
	}

	if cfg.BaseDir == "" {
		return ErrMissingBaseDir
	}

	if cfg.SchemaConfig == nil {
		return ErrMissingSchemaConfig
	}

	if cfg.SchemaConfig.Type == SchemaTypeProtobuf {
		if len(cfg.SchemaConfig.ProtoSchemaList) == 0 {
			return ErrMissingSchemaConfig
		}

		types.NewProtoDescriptorStore(cfg.SchemaConfig.ProtoSchemaList)
	}

	if distributed && cfg.RaftConfig == nil {
		return ErrMissingRaftConf
	}

	if cfg.Name == "" {
		cfg.Name = "omashu"
	}

	cfg.BadgerOptions = cfg.BadgerOptions.WithDir(fmt.Sprintf("%s/%s", cfg.BaseDir, DBSubDir))

	if cfg.Logger == nil {
		cfg.BadgerOptions = cfg.BadgerOptions.WithLogger(nil)
		cfg.Logger = zap.NewNop()
	} else {
		cfg.BadgerOptions = cfg.BadgerOptions.WithLogger(newLogger(fmt.Sprintf("%s.badger", cfg.Name), cfg.Logger))
	}

	return nil
}
