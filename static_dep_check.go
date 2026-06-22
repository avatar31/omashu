/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
)

type (
	_ rafthttp.Transport
	_ snap.Snapshotter
	_ wal.WAL
)

const (
	_ = rafthttp.LocalCodebaseMarker == "v3.6.8-custom"
	_ = snap.LocalCodebaseMarker == "v3.6.8-custom"
	_ = wal.LocalCodebaseMarker == "v3.6.8-custom"
)
