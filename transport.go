/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package omashu

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// TODO: P0: Handle transport.SendSnapshot
// 1. Handle transport errors and implement retry logic
// 2. Implement TLS support for secure communication between nodes
// 3. Add metrics and logging for transport operations

const (
	defaultReqTimeout = 5 * time.Second
)

type Transport struct {
	id     uint64
	raftTr *rafthttp.Transport
	server *http.Server

	mu    sync.Mutex
	peers map[uint64]string // nodeID -> address

	log *zap.Logger
}

func NewTransport(id uint64, peers map[uint64]string, log *zap.Logger) *Transport {
	return &Transport{
		id:    id,
		peers: peers,
		log:   log,
	}
}

func (tr *Transport) Start(ctx context.Context, cfg *Config, node *Node, snapshotter *snap.Snapshotter) {
	tr.raftTr = &rafthttp.Transport{
		Logger:      tr.log, //TODO: P0: Fix me
		DialTimeout: cfg.PeerDialTimeout(),
		ID:          etcdtypes.ID(tr.id),
		ClusterID:   etcdtypes.ID(node.cluster.GetID()),
		Raft:        node,
		Snapshotter: snapshotter, // TODO: P0: Why do we need this?
		ServerStats: stats.NewServerStats(node.cluster.GetName(), strconv.FormatUint(node.cluster.GetID(), 10)),
		LeaderStats: stats.NewLeaderStats(tr.log, strconv.FormatUint(tr.id, 10)),
		ErrorC:      node.errChan,
	}

	err := tr.raftTr.Start()
	if err != nil {
		node.errChan <- err
	}

	addr, ok := tr.peers[tr.id]
	if !ok {
		node.errChan <- fmt.Errorf("no address found for node %d", tr.id)
	}

	parsedAddr, err := url.Parse(addr)
	if err != nil {
		node.errChan <- err
	}

	for i := range tr.peers {
		if i != tr.id {
			tr.raftTr.AddPeer(etcdtypes.ID(i), []string{tr.peers[i]})
		}
	}

	tr.server = &http.Server{
		Addr:         parsedAddr.Host,
		Handler:      tr.raftTr.Handler(),
		ReadTimeout:  defaultReqTimeout,
		WriteTimeout: defaultReqTimeout,
	}

	tr.log.Info("Serving raft in address", zap.String("address", addr))
	go func() {
		if err := tr.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			node.errChan <- err
		}
	}()
}

func (tr *Transport) Send(ctx context.Context, messages []raftpb.Message) {
	tr.raftTr.Send(messages)
}

func (tr *Transport) AddPeer(ctx context.Context, id uint64, addr string) {
	tr.raftTr.AddPeer(etcdtypes.ID(id), []string{addr})
	tr.mu.Lock()
	tr.peers[id] = addr
	tr.mu.Unlock()
	tr.log.Info(fmt.Sprintf("Added peer %d with address %s", id, addr))
}

func (tr *Transport) RemovePeer(ctx context.Context, id uint64) {
	tr.raftTr.RemovePeer(etcdtypes.ID(id))
	tr.mu.Lock()
	delete(tr.peers, id)
	tr.mu.Unlock()
	tr.log.Info(fmt.Sprintf("Removed peer %d", id))
}

func (tr *Transport) UpdatePeer(ctx context.Context, id uint64, addr string) {
	tr.raftTr.UpdatePeer(etcdtypes.ID(id), []string{addr})
	tr.mu.Lock()
	tr.peers[id] = addr
	tr.mu.Unlock()
	tr.log.Info(fmt.Sprintf("Updated peer %d with new address %s", id, addr))
}

func (tr *Transport) Stop() error {
	tr.raftTr.Stop()
	if tr.server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := tr.server.Shutdown(shutdownCtx); err != nil {
			tr.log.Error("Error while shutting down raft server", zap.Error(err))
			return err
		}
	}
	return nil
}
