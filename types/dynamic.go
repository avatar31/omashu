/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package types

import (
	"errors"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// descriptorsCache is the package-level singleton protoDescriptorStore.
// Initialised at most once via [NewProtoDescriptorStore].
var (
	descriptorsCache *protoDescriptorStore
	once             sync.Once
)

// protoDescriptorStore holds the Protobuf FileDescriptorSets registered
// for this process and provides lookup and instantiation of dynamic
// messages.
type protoDescriptorStore struct {
	store []*descriptorpb.FileDescriptorSet
}

// NewProtoDescriptorStore initialises the package-level singleton with
// the given FileDescriptorSets. It is safe for concurrent use but only
// the first call has any effect; subsequent calls are no-ops (sync.Once
// semantics). Called automatically by [Config].validate when
// SchemaTypeProtobuf is selected.
func NewProtoDescriptorStore(set []*descriptorpb.FileDescriptorSet) {
	once.Do(func() {
		descriptorsCache = &protoDescriptorStore{store: set}
	})
}

// GetProtoDescriptorStore returns the package-level singleton. Returns
// nil if [NewProtoDescriptorStore] has not been called yet.
func GetProtoDescriptorStore() *protoDescriptorStore {
	return descriptorsCache
}

// IsValidMessage reports whether msgName identifies a known message
// type in any of the registered FileDescriptorSets.
func (p *protoDescriptorStore) IsValidMessage(msgName string) (bool, error) {
	for _, set := range p.store {
		// Load descriptors
		files, err := protodesc.NewFiles(set)
		if err != nil {
			return false, err
		}

		desc, err := files.FindDescriptorByName(protoreflect.FullName(msgName))
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return false, err
		}

		_, ok := desc.(protoreflect.MessageDescriptor)
		if ok {
			return true, nil
		}
	}

	return false, nil
}

// GetMessageFromDescriptorSet returns a new empty dynamic proto.Message
// for the type identified by msgName. Returns protoregistry.NotFound if
// the name is absent from all registered FileDescriptorSets.
func (p *protoDescriptorStore) GetMessageFromDescriptorSet(msgName string) (proto.Message, error) {
	for _, set := range p.store {
		// Load descriptors
		files, err := protodesc.NewFiles(set)
		if err != nil {
			return nil, err
		}

		desc, err := files.FindDescriptorByName(protoreflect.FullName(msgName))
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}

		md, ok := desc.(protoreflect.MessageDescriptor)
		if ok {
			return dynamicpb.NewMessage(md), nil
		}
	}

	return nil, protoregistry.NotFound
}

// MergeProtobufMessages merges delta into original using field-level
// semantics: list fields in delta replace (rather than append to) the
// corresponding lists in original before calling proto.Merge. Both
// messages must share the same descriptor.
func MergeProtobufMessages(original, delta proto.Message) error {
	originalMsg := original.ProtoReflect()
	deltaMsg := delta.ProtoReflect()

	if originalMsg.Descriptor() != deltaMsg.Descriptor() {
		return errors.New("message types do not match for merging")
	}

	deltaMsg.Range(func(fd protoreflect.FieldDescriptor, newVal protoreflect.Value) bool {
		if fd.IsList() {
			// proto.Merge doesn't replace list fields instead it will append to them.
			// Eg. if original has [1] and delta has [1,2], after merge it will be [1,1,2]
			// So, we are clearing the original list first then calling proto.Merge.
			originalList := originalMsg.Mutable(fd).List()
			originalList.Truncate(0)
		}
		return true
	})

	proto.Merge(original, delta)
	return nil
}

// MergeProtoDelta decodes stored as an instance of delta's type,
// applies [MergeProtobufMessages] with delta, and returns the updated
// serialised bytes. Called by the FSM when applying Protobuf UPDATE
// commands.
func MergeProtoDelta(stored []byte, delta proto.Message) ([]byte, error) {
	// Create empty message of same concrete type
	base := proto.Clone(delta)
	proto.Reset(base)

	if reflect.TypeOf(base) != reflect.TypeOf(delta) {
		return nil, errors.New("message types do not match for merging")
	}

	// Decode stored data
	if err := proto.Unmarshal(stored, base); err != nil {
		return nil, err
	}

	// Merge delta
	if err := MergeProtobufMessages(base, delta); err != nil {
		return nil, err
	}

	// Save back
	return proto.Marshal(base)
}
