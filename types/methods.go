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

func (c *Command) Encode() ([]byte, error) {
	return proto.Marshal(c)
}

func (c *Command) AddSubCommand(subCmd *Command) {
	if subCmd == nil ||
		subCmd.Type == CommandType_TRANSACTION ||
		subCmd.Type == CommandType_BATCH_WRITE ||
		subCmd.Type == CommandType_DELETE_BY_PREFIX {
	}

	c.SubCommands = append(c.SubCommands, subCmd)
}

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

	msg, err := GetMessageFromDescriptorSet(c.UpdateMeta.MessageName, c.UpdateMeta.MessageDescriptors)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(c.Value, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func NewSetCommand(ctx context.Context, key string, value []byte, ttl ...time.Duration) *Command {
	c := &Command{
		Id:    getCmdId(ctx),
		Type:  CommandType_SET,
		Key:   key,
		Value: value,
	}

	if len(ttl) > 0 {
		c.Ttl = durationpb.New(ttl[0])
	}
	return c
}

func NewUpdateCommand(ctx context.Context, key string, delta []byte, deltaType UpdateDeltaType,
	ttl ...time.Duration) *Command {
	c := &Command{
		Id:         getCmdId(ctx),
		Type:       CommandType_UPDATE,
		Key:        key,
		Value:      delta,
		UpdateMeta: &UpdateMeta{UpdateDeltaType: deltaType},
	}

	if len(ttl) > 0 {
		c.Ttl = durationpb.New(ttl[0])
	}
	return c
}

func NewDeleteCommand(ctx context.Context, key string) *Command {
	return &Command{
		Id:   getCmdId(ctx),
		Type: CommandType_DELETE,
		Key:  key,
	}
}

func NewDeleteByPrefixCommand(ctx context.Context, prefix string) *Command {
	return &Command{
		Id:     getCmdId(ctx),
		Type:   CommandType_DELETE_BY_PREFIX,
		Prefix: prefix,
	}
}

func NewIncrByCommand(ctx context.Context, key string, delta uint64) *Command {
	return &Command{
		Id:              getCmdId(ctx),
		Type:            CommandType_INCR_BY,
		Key:             key,
		IncrOrDecrDelta: delta,
	}
}

func NewDecrByCommand(ctx context.Context, key string, delta uint64) *Command {
	return &Command{
		Id:              getCmdId(ctx),
		Type:            CommandType_DECR_BY,
		Key:             key,
		IncrOrDecrDelta: delta,
	}
}

func NewTransactionCommand(ctx context.Context) *Command {
	return &Command{
		Id:          getCmdId(ctx),
		Type:        CommandType_TRANSACTION,
		SubCommands: make([]*Command, 0),
	}
}

func NewBatchWriteCommand(ctx context.Context) *Command {
	return &Command{
		Id:          getCmdId(ctx),
		Type:        CommandType_BATCH_WRITE,
		SubCommands: make([]*Command, 0),
	}
}

func DecodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := proto.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func getCmdId(ctx context.Context) string {
	// traceId := ctx.Value(utils.CtxTraceIdKey)
	// if id, ok := traceId.(string); ok {
	// 	return id
	// }

	return uuid.New().String()
}
