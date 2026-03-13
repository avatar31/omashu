package omashu

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/avatar31/omashu/types"
)

const (
	defaultSnapshotCount = 100000 // Number of entries between snapshots
	RetryCount           = 5
	ReadStatesTimeout    = time.Second
)

// server/etcdserver/server.go
// server/etcdserver/raft.go
// server/etcdserver/bootstrap.go

type ProposeResp struct {
	CmdID string
	Err   error
}

type Node struct {
	id    uint64
	name  string
	peers map[uint64]string // nodeID -> address

	n         raft.Node
	storage   *Storage
	transport *Transport
	fsm       *FSM

	confState   raftpb.ConfState
	confStateMu sync.RWMutex

	appliedIndex atomic.Uint64
	// committedIndex atomic.Uint64
	// term           atomic.Uint64
	lead atomic.Uint64

	confChangeNotifier chan raftpb.ConfChange
	proposeNotifier    chan []byte
	stopNotifier       chan struct{}
	readStatesNotifier chan raft.ReadState
	respNotifier       chan ProposeResp

	mu  sync.RWMutex
	log *zap.Logger
}

func NewNode(ctx context.Context, id uint64, nodeName string, peers map[uint64]string, fsm *FSM,
	logger *zap.Logger) (*Node, error) {
	node := &Node{
		id:                 id,
		name:               nodeName,
		peers:              peers,
		fsm:                fsm,
		confChangeNotifier: make(chan raftpb.ConfChange),
		proposeNotifier:    make(chan []byte, 1),
		stopNotifier:       make(chan struct{}),
		readStatesNotifier: make(chan raft.ReadState, 2),
		respNotifier:       make(chan ProposeResp, 1),
		log:                logger,
	}

	return node, nil
}

func (node *Node) Start(ctx context.Context) error {
	storage, err := NewStorage(node.log)
	if err != nil {
		return err
	}

	node.storage = storage
	err = node.storage.Initialize(ctx)
	if err != nil {
		return err
	}

	peers := make([]raft.Peer, 0, len(node.peers))
	for id := range node.peers {
		peers = append(peers, raft.Peer{ID: id})
	}

	// raft.SetLogger(logger.GetLogger(ctx))
	raftConf := &raft.Config{
		ID:                        node.id,
		ElectionTick:              10, // 1s
		HeartbeatTick:             1,  // 100ms
		Storage:                   node.storage,
		MaxSizePerMsg:             1024 * 1024, // 1 MB		// TODO: P0: Tune this value
		MaxInflightMsgs:           256,         // TODO: P0: Tune this value
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,    // rest middleware will take care of forwarding
		MaxUncommittedEntriesSize: 1 << 30, // 1GB			// TODO: P0: Tune this value
	}

	if node.storage.Existing() {
		node.log.Info("Existing wal storage found. Restarting raft node.")
		node.n = raft.RestartNode(raftConf)
	} else {
		node.log.Info("Starting new raft node.")
		node.n = raft.StartNode(raftConf, peers)
	}

	errCh := make(chan error)
	node.transport = NewTransport(node.id, node.peers, node.log)
	node.transport.Start(ctx, node, storage.wal.snapshotter, errCh)

	go node.run(ctx)

	select {
	case err := <-errCh:
		node.log.Error("Failed to start transport. Stopping raft node.", zap.Error(err))
		node.Stop(ctx)
		return err
	case <-time.After(5 * time.Second):
		// Assuming transport started successfully
	}

	node.log.Info("Raft node started successfully")
	return nil
}

func (node *Node) run(ctx context.Context) {
	defer node.Stop(ctx)

	// go func() {
	snap, err := node.storage.Snapshot()
	if err != nil {
		node.log.Error("Failed to get snapshot from storage", zap.Error(err))
		node.stopNotifier <- struct{}{}
		return
	}

	node.setConfState(snap.Metadata.ConfState)
	node.setAppliedIndex(snap.Metadata.Index)
	// node.setCommittedIndex(snap.Metadata.Index)
	// node.setTerm(snap.Metadata.Term)
	// }()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	node.setLead(node.n.Status().Lead)

	for {
		select {
		case <-ticker.C:
			// TODO: P0: etcd tick within mutex. Need to investigate why..
			node.n.Tick()
		case prop := <-node.proposeNotifier:
			c, _ := types.DecodeCommand(prop)
			err := node.n.Propose(ctx, prop)
			if err != nil {
				node.log.Error("Failed to propose entry", zap.Error(err), zap.String("cmdID", c.Id))
			}
			node.respNotifier <- ProposeResp{CmdID: c.Id, Err: err}
		case cc := <-node.confChangeNotifier:
			err := node.n.ProposeConfChange(ctx, cc)
			if err != nil {
				node.log.Error("Failed to propose conf change. Stopping raft node.", zap.Error(err))
				return
			}
		case rd := <-node.n.Ready():
			if err := node.processReady(ctx, &rd); err != nil {
				node.log.Error("Failed to process ready. Stopping raft node.", zap.Error(err))
				return
			}
		case <-ctx.Done():
			node.log.Info("Context cancelled. Stopping raft node.")
			return
		case <-node.stopNotifier:
			node.log.Info("Stop notification received. Stopping raft node.")
			return
		}
	}
}

// Steps:
// 1. Save snapshot (if any)
// 2. Save state and entries to storage
// 3. Apply snapshot to FSM (if any)
// 4. Send messages to other nodes
// 5. Apply committed entries to FSM
func (node *Node) processReady(ctx context.Context, rd *raft.Ready) error {
	// node.updateCommittedIndex(rd)
	// node.setTerm(rd.Term)
	if rd.SoftState != nil {
		node.log.Info("Soft state changed", zap.String("softState", raft.DescribeSoftState(*rd.SoftState)))
		node.setLead(rd.SoftState.Lead)
	}

	pendingReadStates := make([]raft.ReadState, 0)
	if rd.ReadStates != nil {
		// Handle read states before applying entries
		pendingReadStates = node.respondToReadStates(rd.ReadStates)
	}
	err := node.storage.SaveState(ctx, rd)
	if err != nil {
		node.log.Error("Failed to save current state to storage", zap.Error(err))
		return err
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := node.applySnapshotToFSM(ctx, rd.Snapshot); err != nil {
			node.log.Error("Failed to apply snapshot to FSM", zap.Error(err))
			return err
		}
	}

	node.transport.Send(ctx, node.getMessagesToPublish(rd))

	if entries := node.getEntriesToApply(rd.CommittedEntries); len(entries) > 0 {
		selfRemove, err := node.applyEntries(ctx, entries)
		if err != nil {
			node.log.Error("Failed to apply entries to FSM", zap.Error(err))
			return err
		}
		if selfRemove {
			node.log.Info("I've been removed from the cluster! Shutting down.")
			return nil
		}
	}

	if rd.ReadStates != nil {
		// Handle any pending read states after applying entries
		pendingReadStates = node.respondToReadStates(pendingReadStates)

		count := len(pendingReadStates)
		if count > 0 {
			// TODO: P0: How to handle this scenario better?
			node.log.Warn("Some read states are still pending after applying entries",
				zap.Int("pendingReadStatesCount", count), zap.Uint64("appliedIndex", node.getAppliedIndex()),
				zap.Uint64("topPendingReadStateIndex", pendingReadStates[count-1].Index))
		}
	}

	go node.takeSnapshotIfNeeded(ctx, false)
	node.n.Advance()
	return nil
}

func (node *Node) respondToReadStates(readStates []raft.ReadState) []raft.ReadState {
	if len(readStates) == 0 {
		return []raft.ReadState{}
	}

	appliedIndex := node.getAppliedIndex()
	confirmedIndex := readStates[len(readStates)-1].Index
	if appliedIndex >= confirmedIndex {
		node.readStatesNotifier <- readStates[len(readStates)-1]
		return []raft.ReadState{}
	}

	for i := len(readStates) - 1; i >= 0; i-- {
		if appliedIndex >= readStates[i].Index {
			node.readStatesNotifier <- readStates[i]
			return readStates[i+1:]
		}
	}

	return readStates
}

// func (node *Node) updateCommittedIndex(rd *raft.Ready) {
// 	var committedIndex uint64
// 	if len(rd.Entries) != 0 {
// 		committedIndex = rd.Entries[len(rd.Entries)-1].Index
// 	}
// 	if rd.Snapshot.Metadata.Index > committedIndex {
// 		committedIndex = rd.Snapshot.Metadata.Index
// 	}
// 	if committedIndex != 0 {
// 		node.setCommittedIndex(committedIndex)
// 	}
// }

func (node *Node) applySnapshotToFSM(ctx context.Context, snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}

	appliedIndex := node.getAppliedIndex()
	if snapshot.Metadata.Index <= appliedIndex {
		return fmt.Errorf("snapshot index %d less then applied index %d", snapshot.Metadata.Index, appliedIndex)
	}

	if err := node.fsm.RestoreSnapshot(ctx, snapshot); err != nil {
		return fmt.Errorf("failed to restore FSM snapshot: %w", err)
	}

	// TODO: Do I need this here?
	node.setAppliedIndex(snapshot.Metadata.Index)
	// node.setCommittedIndex(snapshot.Metadata.Index)
	// node.setTerm(snapshot.Metadata.Term)
	node.setConfState(snapshot.Metadata.ConfState)

	node.log.Info("Restored fsm from snapshot at index", zap.Uint64("index", snapshot.Metadata.Index))
	return nil
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (node *Node) getMessagesToPublish(rd *raft.Ready) []raftpb.Message {
	confState := node.getConfState()
	msgs := make([]raftpb.Message, 0, len(rd.Messages))
	for _, msg := range rd.Messages {
		if msg.Type == raftpb.MsgSnap {
			msg.Snapshot.Metadata.ConfState = confState
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func (node *Node) getEntriesToApply(committedEntries []raftpb.Entry) []raftpb.Entry {
	if len(committedEntries) == 0 {
		return committedEntries
	}

	appliedIndex := node.getAppliedIndex()
	firstIndex := committedEntries[0].Index
	for range RetryCount {
		if firstIndex > appliedIndex+1 {
			node.log.Warn(fmt.Sprintf("First index of committed entries (%d) is greater than applied index + 1 (%d). Retrying...",
				firstIndex, appliedIndex+1))
			time.Sleep(100 * time.Millisecond)
			appliedIndex = node.getAppliedIndex()
		}
	}

	if firstIndex > appliedIndex+1 {
		node.log.Panic(fmt.Sprintf("First index of committed entries (%d) is greater than applied index + 1 (%d)",
			firstIndex, appliedIndex+1))
	}

	if appliedIndex-firstIndex+1 < uint64(len(committedEntries)) {
		return committedEntries[appliedIndex-firstIndex+1:]
	}
	return []raftpb.Entry{}
}

func (node *Node) applyEntries(ctx context.Context, entries []raftpb.Entry) (bool, error) {
	selfRemove := false
	publishedEntries := 0
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) != 0 {
				publishedEntries++
				node.applyNormalEntry(ctx, &entries[i])
			}
		case raftpb.EntryConfChange:
			publishedEntries++
			selfRemove = node.applyConfChange(ctx, &entries[i])
		default:
			return false, fmt.Errorf("unknown entry type: %v", entries[i].Type.String())
		}
	}

	node.setAppliedIndex(entries[len(entries)-1].Index)
	node.log.Info(fmt.Sprintf("Applied %d entries, out of %d entries.", publishedEntries, len(entries)))
	return selfRemove, nil
}

func (node *Node) applyNormalEntry(ctx context.Context, entry *raftpb.Entry) {
	appliedIndex := node.getAppliedIndex()
	if entry.Index <= appliedIndex {
		node.log.Warn("Skipping already applied entry", zap.Uint64("index", entry.Index),
			zap.Uint64("appliedIndex", appliedIndex))
		return
	}

	if err := node.fsm.Apply(ctx, entry.Data); err != nil {
		node.log.Error("Failed to apply normal entry to FSM", zap.Uint64("index", entry.Index), zap.Error(err))
		// TODO: How to handle this error?
	}
}

func (node *Node) applyConfChange(ctx context.Context, entry *raftpb.Entry) bool {
	var cc raftpb.ConfChange
	cc.Unmarshal(entry.Data)
	confState := node.n.ApplyConfChange(cc)
	node.setConfState(*confState)

	switch cc.Type {
	case raftpb.ConfChangeAddNode: // TODO: Do we need raftpb.ConfChangeAddLearnerNode?
		if len(cc.Context) > 0 {
			// node.transport.AddPeer(ctx, cc.NodeID, string(cc.Context))
		}
	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == node.id {
			return true
		}
		node.transport.RemovePeer(ctx, cc.NodeID)
	case raftpb.ConfChangeUpdateNode:
		// TODO: Handle update node if needed
	}

	node.log.Info("Applied configuration change", zap.String("entry", raft.DescribeEntry(*entry, nil)),
		zap.String("newConfState", raft.DescribeConfState(node.getConfState())))
	return false
}

func (node *Node) takeSnapshotIfNeeded(ctx context.Context, force bool) {
	confState := node.getConfState()
	snapshotIndex := node.storage.LastSnapshotIndex()
	appliedIndex := node.getAppliedIndex()
	if !force && appliedIndex-snapshotIndex < defaultSnapshotCount {
		return
	}

	upto, data, err := node.fsm.CreateSnapshot(ctx)
	if err != nil {
		node.log.Error("Failed to create FSM snapshot", zap.Error(err))
		return
	}

	node.log.Info("Creating snapshot at appliedIndex", zap.Uint64("appliedIndex", appliedIndex),
		zap.Uint64("tsoUpto", upto))

	// TODO: Do I need to persist TSO upto in snapshot or storage?
	if err := node.storage.CreateSnapshot(appliedIndex, confState, data); err != nil {
		node.log.Error("Failed to save snapshot", zap.Error(err))
		return
	}

	node.log.Info("Snapshot created successfully at index", zap.Uint64("appliedIndex", appliedIndex))
}

func (node *Node) IsLeader() bool {
	return node.getLead() == node.id
}

// Leader returns the current leader ID
func (node *Node) Leader() uint64 {
	return node.getLead()
}

// ReadIndex requests a read index for linearizable reads
func (node *Node) ReadIndex(ctx context.Context, rctx []byte) (uint64, error) {
	err := node.n.ReadIndex(ctx, rctx)
	if err != nil {
		return 0, err
	}
	return node.getAppliedIndex(), nil
}

// WaitForReadState waits for a read state with timeout
func (node *Node) WaitForReadState(ctx context.Context, timeout time.Duration) (*raft.ReadState, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case rs := <-node.readStatesNotifier:
		return &rs, nil
	case <-timer.C:
		return nil, errors.New("read state timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (node *Node) ProposeNotifier() chan<- []byte {
	return node.proposeNotifier
}

func (node *Node) ProposeRespNotifier() <-chan ProposeResp {
	return node.respNotifier
}

func (node *Node) setAppliedIndex(v uint64) {
	node.log.Debug("Setting applied index", //zap.Stack("stack"),	// TODO: Remove debug log before production
		zap.Uint64("oldIndex", node.getAppliedIndex()), zap.Uint64("newIndex", v))
	node.appliedIndex.Store(v)
}

func (node *Node) getAppliedIndex() uint64 {
	return node.appliedIndex.Load()
}

// func (node *Node) setCommittedIndex(v uint64) {
// 	node.log.Debug("Setting committed index", //zap.Stack("stack"),	// TODO: Remove debug log before production
// 		zap.Uint64("oldIndex", node.getCommittedIndex()), zap.Uint64("newIndex", v))
// 	node.committedIndex.Store(v)
// }

// func (node *Node) getCommittedIndex() uint64 {
// 	return node.committedIndex.Load()
// }

// func (node *Node) setTerm(v uint64) {
// 	node.log.Debug("Setting Term", //zap.Stack("stack"),	// TODO: Remove debug log before production
// 		zap.Uint64("oldTerm", node.getTerm()), zap.Uint64("newTerm", v))
// 	node.term.Store(v)
// }

// func (node *Node) getTerm() uint64 {
// 	return node.term.Load()
// }

func (node *Node) setLead(v uint64) {
	node.log.Debug("Setting Lead", //zap.Stack("stack"),	// TODO: Remove debug log before production
		zap.Uint64("oldLead", node.getLead()), zap.Uint64("newLead", v))
	node.lead.Store(v)
}

func (node *Node) getLead() uint64 {
	return node.lead.Load()
}

func (node *Node) setConfState(confState raftpb.ConfState) {
	node.confStateMu.Lock()
	node.log.Debug("Setting ConfState", //zap.Stack("stack"),	// TODO: Remove debug log before production
		zap.String("oldConfState", raft.DescribeConfState(node.confState)),
		zap.String("newConfState", raft.DescribeConfState(confState)))
	node.confState = confState
	node.confStateMu.Unlock()
}

func (node *Node) getConfState() raftpb.ConfState {
	node.confStateMu.RLock()
	defer node.confStateMu.RUnlock()
	return node.confState
}

func (node *Node) Stop(ctx context.Context) {
	node.log.Info("Stopping transport for raft node")
	err := node.transport.Stop()
	if err != nil {
		node.log.Error("Error while stopping transport for raft node", zap.Error(err))
	}

	node.log.Info("Closing storage for raft node")
	err = node.storage.Close()
	if err != nil {
		node.log.Error("Error while closing storage for raft node", zap.Error(err))
	}

	node.n.Stop()
	node.log.Info("Raft node stopped")
}

func (node *Node) Process(ctx context.Context, m raftpb.Message) error {
	return node.n.Step(ctx, m)
}

func (node *Node) IsIDRemoved(id uint64) bool {
	return false // TODO: Implement this
}

func (node *Node) ReportUnreachable(id uint64) {
	node.n.ReportUnreachable(id)
}

func (node *Node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.n.ReportSnapshot(id, status)
}
