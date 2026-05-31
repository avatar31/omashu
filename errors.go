/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import "errors"

var (
	// ErrMissingRaftConf is returned by [NewDistributedBadger] when
	// [Config.RaftConfig] is nil.
	ErrMissingRaftConf = errors.New("raft config required for distributed db")

	// ErrMissingCluster is returned when [Config.Cluster] is nil.
	ErrMissingCluster = errors.New("cluster details are missing")

	// ErrMissingBaseDir is returned when [Config.BaseDir] is empty.
	ErrMissingBaseDir = errors.New("baseDir is missing")

	// ErrMissingSchemaConfig is returned when [Config.SchemaConfig] is
	// nil or, for Protobuf schema mode, when ProtoSchemaList is empty.
	ErrMissingSchemaConfig = errors.New("schemaConfig is missing")

	// ErrNotLeader is returned by write and delete operations when the
	// receiving node is not the current Raft leader.
	ErrNotLeader = errors.New("operation can only be performed on leader node")

	// ErrProposeTimeout is returned when a Raft proposal is not committed
	// within [DefaultProposeTimeout].
	ErrProposeTimeout = errors.New("raft propose timeout")

	// ErrBatchTooBig is returned when the number of sub-commands in a
	// transaction exceeds [MaxBatchSize].
	ErrBatchTooBig = errors.New("batch size exceeds maximum limit")

	// ErrUnknownOp is returned by the FSM when it receives a [Command]
	// with an unrecognised CommandType.
	ErrUnknownOp = errors.New("unknown operation")

	// ErrUnknownProtoMsg is returned when [UpdateProtobuf] is called
	// with a proto.Message whose type is not registered in the
	// descriptor store.
	ErrUnknownProtoMsg = errors.New("unknown protobuf message type")
)
