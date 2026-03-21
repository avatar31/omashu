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

var (
	descriptorsCache *protoDescriptorStore
	once             sync.Once
)

type protoDescriptorStore struct {
	store []*descriptorpb.FileDescriptorSet
}

func NewProtoDescriptorStore(set []*descriptorpb.FileDescriptorSet) {
	once.Do(func() {
		descriptorsCache = &protoDescriptorStore{store: set}
	})
}

func GetProtoDescriptorStore() *protoDescriptorStore {
	return descriptorsCache
}

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
	MergeProtobufMessages(base, delta)

	// Save back
	return proto.Marshal(base)
}
