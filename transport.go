package omashu

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	defaultReqTimeout = 5 * time.Second
)

type Transport struct {
	id     uint64
	raftTr *rafthttp.Transport
	server *http.Server

	mu    sync.Mutex
	peers map[uint64]string // nodeID -> address

	httpstopc chan struct{}
	log       *zap.Logger
}

func NewTransport(id uint64, peers map[uint64]string, log *zap.Logger) *Transport {
	return &Transport{
		id:        id,
		peers:     peers,
		log:       log,
		httpstopc: make(chan struct{}),
	}
}

func (tr *Transport) Start(ctx context.Context, cluster Cluster, node *Node, snapshotter *snap.Snapshotter, errorC chan error) {
	tr.raftTr = &rafthttp.Transport{
		ID:          etcdtypes.ID(tr.id),
		ClusterID:   etcdtypes.ID(cluster.GetClusterID()),
		Raft:        node,
		ErrorC:      errorC,
		Logger:      tr.log, //TODO: P0: Fix me
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(tr.log, fmt.Sprintf("%d", tr.id)),
		Snapshotter: snapshotter, // TODO: Why do we need this?
	}

	err := tr.raftTr.Start()
	if err != nil {
		errorC <- err
	}

	addr, ok := tr.peers[tr.id]
	if !ok {
		errorC <- fmt.Errorf("no address found for node %d", tr.id)
	}

	parsedAddr, err := url.Parse(addr)
	if err != nil {
		errorC <- err
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

	localErrorC := make(chan error)
	tr.log.Info("Serving raft in address", zap.String("address", addr))
	go func(localErrorC chan error) {
		if err := tr.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			localErrorC <- err
		}
	}(localErrorC)

	select {
	case err := <-localErrorC:
		errorC <- err
	case <-time.After(1 * time.Second):
		// Assume server started successfully if no error after 1 second
		tr.log.Info("Raft HTTP server started successfully")
	}
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
