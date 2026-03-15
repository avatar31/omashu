package omashu

import "errors"

var (
	// Config validation errors
	ErrMissingRaftConf = errors.New("raft config required for managed db")
	ErrMissingCluster  = errors.New("cluster details are missing")
	ErrMissingBaseDir  = errors.New("baseDir is missing")

	ErrNotLeader      = errors.New("operation can only be performed on leader node")
	ErrProposeTimeout = errors.New("raft propose timeout")

	// ErrBatchTooBig indicates that the batch size exceeds the maximum limit.
	ErrBatchTooBig = errors.New("Batch size exceeds maximum limit")

	// ErrUnknownOp indicates that an unknown operation was encountered.
	ErrUnknownOp = errors.New("Unknown operation")
)
