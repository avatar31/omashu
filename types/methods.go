/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package types

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Encode serialises c into its binary Protobuf wire format.
func (c *Command) Encode() ([]byte, error) {
	return proto.Marshal(c)
}

// AddSubCommand appends subCmd to c's sub-command list. Panics if
// subCmd is nil or if its type is CommandType_TRANSACTION (nesting
// transactions is not supported).
func (c *Command) AddSubCommand(subCmd *Command) {
	if subCmd == nil || subCmd.Type == CommandType_TRANSACTION {
		panic("dev stage panic: invalid sub command")
	}

	c.SubCommands = append(c.SubCommands, subCmd)
}

// UnmarshalUpdateDelta decodes the update delta in c.Value according
// to c.UpdateMeta.UpdateDeltaType. Returns map[string]any for JSON
// deltas or a proto.Message for Protobuf deltas.
func (c *Command) UnmarshalUpdateDelta() (any, error) {
	if c.Type != CommandType_UPDATE ||
		c.UpdateMeta == nil ||
		c.UpdateMeta.UpdateDeltaType == UpdateDeltaType_UNKNOWN_DELTA {
		return nil, errors.New("invalid update command")
	}

	if c.UpdateMeta.UpdateDeltaType == UpdateDeltaType_JSON {
		var deltaJson map[string]any
		err := json.Unmarshal(c.Value, &deltaJson)
		if err != nil {
			return nil, err
		}
		return deltaJson, nil
	}

	msg, err := GetProtoDescriptorStore().GetMessageFromDescriptorSet(c.UpdateMeta.MessageName)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(c.Value, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// NewSetCommand creates a SET command that stores value at key. An
// optional TTL causes BadgerDB to expire the entry after the duration.
func NewSetCommand(ctx context.Context, key string, value []byte, ttl ...time.Duration) *Command {
	c := &Command{
		Id:    getNewCmdId(),
		Type:  CommandType_SET,
		Key:   key,
		Value: value,
	}

	if len(ttl) > 0 {
		c.Ttl = durationpb.New(ttl[0])
	}
	return c
}

// NewUpdateCommand creates an UPDATE command that merges delta into the
// existing value at key using deltaType (JSON or Protobuf). For
// Protobuf deltas, msgName must match the registered message name.
func NewUpdateCommand(ctx context.Context, key string, delta []byte, deltaType UpdateDeltaType, msgName string,
	ttl ...time.Duration) *Command {
	c := &Command{
		Id:         getNewCmdId(),
		Type:       CommandType_UPDATE,
		Key:        key,
		Value:      delta,
		UpdateMeta: &UpdateMeta{UpdateDeltaType: deltaType, MessageName: msgName},
	}

	if len(ttl) > 0 {
		c.Ttl = durationpb.New(ttl[0])
	}
	return c
}

// NewDeleteCommand creates a DELETE command that removes key from the
// store.
func NewDeleteCommand(ctx context.Context, key string) *Command {
	return &Command{
		Id:   getNewCmdId(),
		Type: CommandType_DELETE,
		Key:  key,
	}
}

// NewDeleteByPrefixCommand creates a DELETE_BY_PREFIX command that
// removes all keys beginning with prefix.
func NewDeleteByPrefixCommand(ctx context.Context, prefix string) *Command {
	return &Command{
		Id:     getNewCmdId(),
		Type:   CommandType_DELETE_BY_PREFIX,
		Prefix: prefix,
	}
}

// NewIncrByCommand creates an INCR_BY command that increments the
// counter at key by delta.
func NewIncrByCommand(ctx context.Context, key string, delta uint64) *Command {
	return &Command{
		Id:              getNewCmdId(),
		Type:            CommandType_INCR_BY,
		Key:             key,
		IncrOrDecrDelta: delta,
	}
}

// NewDecrByCommand creates a DECR_BY command that decrements the
// counter at key by delta.
func NewDecrByCommand(ctx context.Context, key string, delta uint64) *Command {
	return &Command{
		Id:              getNewCmdId(),
		Type:            CommandType_DECR_BY,
		Key:             key,
		IncrOrDecrDelta: delta,
	}
}

// NewTransactionCommand creates a TRANSACTION command shell. Add
// sub-commands via [Command.AddSubCommand]; the FSM applies them all
// atomically at the parent command's commit timestamp.
func NewTransactionCommand(ctx context.Context) *Command {
	return &Command{
		Id:          getNewCmdId(),
		Type:        CommandType_TRANSACTION,
		SubCommands: make([]*Command, 0),
	}
}

// DecodeCommand deserialises a Command from its binary Protobuf wire
// format. This is the inverse of [Command.Encode].
func DecodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := proto.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// getNewCmdId returns a new random UUID string for use as a unique
// command identifier.
func getNewCmdId() string {
	return uuid.New().String()
}
