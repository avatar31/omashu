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

// SchemaType identifies the serialisation format used for UPDATE
// operations on a [DistributedBadger] store.
type SchemaType string

var (
	// SchemaTypeJson selects JSON merge-patch semantics for UpdateJson.
	SchemaTypeJson SchemaType = "json"

	// SchemaTypeProtobuf selects field-level Protobuf merge for UpdateProtobuf.
	SchemaTypeProtobuf SchemaType = "protobuf"
)

// Cluster is implemented by the application to describe its view of
// the Raft cluster membership. It must be set in [Config.Cluster] for
// both [Badger] and [DistributedBadger] stores.
type Cluster interface {
	// GetID returns this node's unique cluster identifier. Must be
	// non-zero and stable across restarts.
	GetID() uint64

	// GetName returns a human-readable label for this node (used in logs).
	GetName() string

	// IsNodeRemoved reports whether the node with the given id has been
	// removed from the cluster and should no longer receive Raft messages.
	IsNodeRemoved(id uint64) bool
}

// RaftConfig extends [raft.Config] with the cluster topology needed to
// start the Raft node. Nodename is used in logs; Peers maps every
// cluster member's node ID to its "http://host:port" address.
type RaftConfig struct {
	raft.Config
	Nodename string
	Peers    map[uint64]string
}

// SchemaConfig selects the update-delta serialisation format and, for
// Protobuf mode, provides the registered message descriptor sets.
type SchemaConfig struct {
	Type            SchemaType
	ProtoSchemaList []*descriptorpb.FileDescriptorSet
}

// Config holds all configuration for creating a [Badger] or
// [DistributedBadger] store. Required fields are BaseDir, Cluster, and
// SchemaConfig; RaftConfig is additionally required for distributed
// mode. See the field comments for defaults and optional behaviour.
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

// validate checks that all required Config fields are set, applies
// defaults (Name, Logger, BadgerOptions), and initialises the Protobuf
// descriptor store when SchemaTypeProtobuf is selected. When
// distributed is true it also requires RaftConfig.
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

	if cfg.BadgerOptions.InMemory {
		cfg.BaseDir = ""
	}
	return nil
}

// PeerDialTimeout returns a recommended dial timeout for connecting to
// a peer node: 1 second plus one election-timeout duration. Used by
// the HTTP transport when establishing peer connections. Panics if
// RaftConfig is nil.
func (c *Config) PeerDialTimeout() time.Duration {
	if c.RaftConfig == nil {
		c.Logger.Panic("RaftConfig is required for distributed mode")
	}

	// 1s for queue wait and election timeout
	return time.Second + time.Duration(c.RaftConfig.ElectionTick*int(c.RaftConfig.HeartbeatTick))*time.Millisecond
}
