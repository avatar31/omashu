package types

import (
	"errors"
	reflect "reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	descriptorsCache sync.Map // msgName -> *descriptorpb.FileDescriptorSet
)

// TODO: P0
// Common patterns:
// Send descriptor set during handshake
// Send schema separately
// Reference schema by ID afterward

func GetFileDescriptorSet(msg proto.Message) (string, *descriptorpb.FileDescriptorSet) {
	// Extract descriptor
	md := msg.ProtoReflect().Descriptor()
	msgName := string(md.FullName())

	if v, ok := descriptorsCache.Load(msgName); ok {
		return msgName, v.(*descriptorpb.FileDescriptorSet)
	}

	visited := make(map[string]bool)
	var files []*descriptorpb.FileDescriptorProto

	var visit func(fd protoreflect.FileDescriptor)
	visit = func(fd protoreflect.FileDescriptor) {
		path := fd.Path()
		if visited[path] {
			return
		}
		visited[path] = true

		// Visit imports first
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			visit(imports.Get(i))
		}

		// Convert to FileDescriptorProto
		files = append(files, protodesc.ToFileDescriptorProto(fd))
	}

	// Recursive visit
	visit(md.ParentFile())

	descriptors := &descriptorpb.FileDescriptorSet{File: files}
	descriptorsCache.Store(msgName, descriptors)
	return msgName, descriptors
}

func GetMessageFromDescriptorSet(msgName string, descriptorSet *descriptorpb.FileDescriptorSet) (proto.Message, error) {
	// Load descriptors
	files, err := protodesc.NewFiles(descriptorSet)
	if err != nil {
		return nil, err
	}

	desc, err := files.FindDescriptorByName(
		protoreflect.FullName(msgName),
	)

	md := desc.(protoreflect.MessageDescriptor)
	return dynamicpb.NewMessage(md), nil
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
