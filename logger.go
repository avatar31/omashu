/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"fmt"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

// newLogger creates a [raft.Logger] adapter backed by log.
// module is attached as a "subModule" field on every log entry,
// making it easy to filter Raft library output in production logs.
func newLogger(module string, log *zap.Logger) raft.Logger {
	return &zapRaftLogger{log: log.With(zap.String("subModule", module))}
}

// zapRaftLogger bridges go.uber.org/zap to the [raft.Logger] interface
// used by the etcd/raft state machine. Every level delegates directly
// to the corresponding zap method (no format string buffering).
type zapRaftLogger struct {
	log *zap.Logger
}

func (zl *zapRaftLogger) Error(args ...any) {
	zl.log.Error(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Errorf(format string, args ...any) {
	zl.log.Error(fmt.Sprintf(format, args...))
}

func (zl *zapRaftLogger) Warning(args ...any) {
	zl.log.Warn(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Warningf(format string, args ...any) {
	zl.log.Warn(fmt.Sprintf(format, args...))
}

func (zl *zapRaftLogger) Info(args ...any) {
	zl.log.Info(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Infof(format string, args ...any) {
	zl.log.Info(fmt.Sprintf(format, args...))
}

func (zl *zapRaftLogger) Debug(args ...any) {
	zl.log.Debug(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Debugf(format string, args ...any) {
	zl.log.Debug(fmt.Sprintf(format, args...))
}

func (zl *zapRaftLogger) Fatal(args ...any) {
	zl.log.Fatal(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Fatalf(format string, args ...any) {
	zl.log.Fatal(fmt.Sprintf(format, args...))
}

func (zl *zapRaftLogger) Panic(args ...any) {
	zl.log.Panic(fmt.Sprint(args...))
}

func (zl *zapRaftLogger) Panicf(format string, args ...any) {
	zl.log.Panic(fmt.Sprintf(format, args...))
}
