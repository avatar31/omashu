/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

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
)

// defaultSnapshotCount is the number of committed log entries between
// automatic snapshots. RetryCount is the maximum retry attempts for
// recoverable operations. ReadStatesTimeout is the per-read-index wait
// deadline before returning a timeout error.
const (
	defaultSnapshotCount = 100000 // Number of entries between snapshots
	RetryCount           = 5
	ReadStatesTimeout    = time.Second
)

// server/etcdserver/server.go
// server/etcdserver/raft.go
// server/etcdserver/bootstrap.go

// propose carries a single Raft proposal through the request/response
// pipeline. cmdID uniquely identifies the proposal; ErrResp is non-nil
// when the Raft layer reports a failure.
type propose struct {
	cmdID   string
	cmd     []byte
	ErrResp error
}

// Node manages the lifecycle of a single Raft cluster member. It drives the
// etcd/raft state machine by processing Ready events, forwarding proposals,
// detecting leader changes, applying committed entries to the FSM, and taking
// periodic snapshots to bound recovery time after a restart.
type Node struct {
	id    uint64
	name  string
	peers map[uint64]string // nodeID -> address

	n         raft.Node
	storage   *Storage
	transport *Transport
	fsm       *FSM
	cluster   Cluster

	confState   raftpb.ConfState
	confStateMu sync.RWMutex

	appliedIndex atomic.Uint64
	lead         atomic.Uint64
	selfRemoved  atomic.Bool
	// committedIndex atomic.Uint64
	// term           atomic.Uint64

	confChangeNotifier  chan raftpb.ConfChange
	proposeReqNotifier  chan propose
	proposeRespNotifier chan propose
	stopNotifier        chan struct{}
	errChan             chan error
	readStatesNotifier  chan raft.ReadState

	// callbacks
	onLeaderChange func(ctx context.Context, prevLeader, newLeader uint64)
	onRemovedSelf  func()

	log *zap.Logger
}

// newNode allocates and initialises a Node for the given id and nodename.
// peers maps every cluster member's ID (including this node) to its HTTP
// address. The node is not started until Start is called.
func newNode(id uint64, nodename string, peers map[uint64]string, fsm *FSM, log *zap.Logger) (*Node, error) {
	node := &Node{
		id:                  id,
		name:                nodename,
		peers:               peers,
		fsm:                 fsm,
		confChangeNotifier:  make(chan raftpb.ConfChange),
		proposeReqNotifier:  make(chan propose, 1),
		proposeRespNotifier: make(chan propose, 1),
		stopNotifier:        make(chan struct{}),
		readStatesNotifier:  make(chan raft.ReadState, 2),
		errChan:             make(chan error),
		log:                 log,
	}

	return node, nil
}

// Start initialises the Raft storage, configures the raft.Node from cfg,
// starts the peer transport, and launches the readyHandler and
// proposeHandler goroutines. If a WAL already exists the node is restarted
// from its persisted state; otherwise it starts as a new cluster member.
func (node *Node) Start(ctx context.Context, cfg *Config) error {
	node.cluster = cfg.Cluster
	storage, err := newStorage(cfg.BaseDir, node.log)
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

	raft.SetLogger(newLogger(fmt.Sprintf("%s.raft", cfg.Name), cfg.Logger))
	raftConf := &raft.Config{
		ID:                          cfg.RaftConfig.ID,
		ElectionTick:                cfg.RaftConfig.ElectionTick,
		HeartbeatTick:               cfg.RaftConfig.HeartbeatTick,
		Storage:                     node.storage,
		MaxSizePerMsg:               cfg.RaftConfig.MaxSizePerMsg,
		MaxCommittedSizePerReady:    cfg.RaftConfig.MaxCommittedSizePerReady,
		MaxUncommittedEntriesSize:   cfg.RaftConfig.MaxUncommittedEntriesSize,
		MaxInflightMsgs:             cfg.RaftConfig.MaxInflightMsgs,
		MaxInflightBytes:            cfg.RaftConfig.MaxInflightBytes,
		CheckQuorum:                 cfg.RaftConfig.CheckQuorum,
		PreVote:                     cfg.RaftConfig.PreVote,
		ReadOnlyOption:              cfg.RaftConfig.ReadOnlyOption,
		DisableProposalForwarding:   cfg.RaftConfig.DisableProposalForwarding,
		DisableConfChangeValidation: cfg.RaftConfig.DisableConfChangeValidation,
		StepDownOnRemoval:           cfg.RaftConfig.StepDownOnRemoval,
	}

	if node.storage.Existing() {
		node.log.Info("Existing wal storage found. Restarting raft node.")
		node.n = raft.RestartNode(raftConf)
	} else {
		node.log.Info("Starting new raft node.")
		node.n = raft.StartNode(raftConf, peers)
	}

	node.transport = NewTransport(node.id, node.peers, node.log)
	node.transport.Start(ctx, cfg, node, storage.wal.snapshotter)

	go node.readyHandler(ctx)
	go node.proposeHandler(ctx)

	node.log.Info("Raft node initiated")
	return nil
}

// readyHandler is the main Raft event loop. It ticks the Raft timer every
// 100 ms and processes each Ready batch — persisting state, sending messages,
// applying snapshots, and committing entries to the FSM — before calling
// Advance. Runs in its own goroutine until ctx is cancelled or a fatal error
// is received.
func (node *Node) readyHandler(ctx context.Context) {
	defer node.Stop(ctx)

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
			node.n.Tick()
		case err := <-node.errChan:
			node.log.Error("Received error from node", zap.Error(err))
			return
		case rd := <-node.n.Ready():
			if node.getSelfRemoved() {
				node.log.Warn("Node has been removed from cluster. Not processing any new requests.")
				continue
			}

			if err := node.processReady(ctx, &rd); err != nil {
				node.log.Error("Failed to process ready. Stopping raft Ready handler.", zap.Error(err))
				return
			}
		case <-ctx.Done():
			node.log.Info("Context cancelled. Stopping raft Ready handler.")
			return
		case <-node.stopNotifier:
			node.log.Info("Stop notification received. Stopping raft Ready handler.")
			return
		}
	}
}

// proposeHandler receives proposals from proposeReqNotifier, forwards them
// to the Raft node, and returns the result on proposeRespNotifier. It also
// handles configuration-change proposals. Runs in its own goroutine until
// ctx is cancelled or a fatal error is sent to errChan.
func (node *Node) proposeHandler(ctx context.Context) {
	for {
		select {
		case prop := <-node.proposeReqNotifier:
			if node.getSelfRemoved() {
				node.log.Warn("Node has been removed from cluster. Not processing any new requests.")
				node.proposeRespNotifier <- propose{cmdID: prop.cmdID, ErrResp: errors.New("node has been removed from cluster")}
				continue
			}

			node.log.Info("Received proposal to propose to raft", zap.Uint64("lead", node.getLead()), zap.String("cmdID", prop.cmdID))
			err := node.n.Propose(ctx, prop.cmd)
			if err != nil {
				node.log.Error("Failed to propose entry", zap.Error(err), zap.String("cmdID", prop.cmdID))
				node.errChan <- err
				return
			}
			node.log.Info("Proposal sent to raft", zap.String("cmdID", prop.cmdID))
			node.proposeRespNotifier <- propose{cmdID: prop.cmdID, ErrResp: err}
		case cc := <-node.confChangeNotifier:
			if node.getSelfRemoved() {
				node.log.Warn("Node has been removed from cluster. Not processing any new requests.")
				continue
			}

			err := node.n.ProposeConfChange(ctx, cc)
			if err != nil {
				node.log.Error("Failed to propose conf change. Stopping raft Propose handler.", zap.Error(err))
				node.errChan <- err
				return
			}
		case <-ctx.Done():
			node.log.Info("Context cancelled. Stopping raft Propose handler.")
			return
		case <-node.stopNotifier:
			node.log.Info("Stop notification received. Stopping raft Propose handler.")
			return
		}
	}
}

// processReady handles a single Raft Ready batch in the correct order
// mandated by the etcd raft protocol:
//  1. Update soft state and detect leader changes.
//  2. Persist snapshot, hard state, and entries to WAL.
//  3. Send outbound messages (MsgSnap must be sent before snapshot is applied).
//  4. Apply snapshot to FSM (if present).
//  5. Apply committed entries to FSM.
//  6. Schedule a snapshot if the log has grown past the threshold.
//  7. Call Advance to signal that this Ready has been fully processed.
func (node *Node) processReady(ctx context.Context, rd *raft.Ready) error {
	// node.updateCommittedIndex(rd)
	// node.setTerm(rd.Term)
	if rd.SoftState != nil {
		node.log.Info("Soft state changed", zap.String("softState", raft.DescribeSoftState(*rd.SoftState)))
		node.handleLeaderChange(ctx, rd.Lead)
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

	// Per etcd raft protocol, messages MUST be sent before the snapshot is applied to the
	// state machine. Specifically, any outbound MsgSnap (leader → lagging follower) must
	// be dispatched first so the follower can start receiving the snapshot data while we
	// proceed with local state updates. Applying the snapshot before sending would delay
	// the transfer and, in edge cases where applySnapshotToFSM is slow or fails, could
	// leave peers waiting indefinitely for a message that was never dispatched.
	node.transport.Send(ctx, node.getMessagesToPublish(rd))

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := node.applySnapshotToFSM(ctx, rd.Snapshot); err != nil {
			node.log.Error("Failed to apply snapshot to FSM", zap.Error(err))
			return err
		}
	}

	if entries := node.getEntriesToApply(rd.CommittedEntries); len(entries) > 0 {
		selfRemove, err := node.applyEntries(ctx, entries)
		if err != nil {
			node.log.Error("Failed to apply entries to FSM", zap.Error(err))
			return err
		}
		if selfRemove {
			node.scheduleSelfRemoval()
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

// scheduleSelfRemoval handles a ConfChange that removes this node from the
// cluster. It sets the selfRemoved flag, gives outstanding work a 1-second
// grace period, fires the onRemovedSelf callback, and then signals the ready
// loop to stop.
func (node *Node) scheduleSelfRemoval() {
	node.log.Info("I've been removed from the cluster! Shutting down.")
	node.setSelfRemoved(true)

	// Give a small grace period to finish outstanding work
	time.Sleep(1 * time.Second)
	if node.onRemovedSelf != nil {
		node.onRemovedSelf()
	}

	node.stopNotifier <- struct{}{}
}

// handleLeaderChange updates the cached leader ID and invokes the
// onLeaderChange callback when the Raft leader changes. No-ops when the
// leader is unchanged.
func (node *Node) handleLeaderChange(ctx context.Context, newLeader uint64) {
	// Update local state and invoke hook. Avoid repeating if unchanged.
	if newLeader == node.getLead() {
		return
	}

	prevLeader := node.getLead()
	node.setLead(newLeader)
	if node.onLeaderChange != nil {
		node.onLeaderChange(ctx, prevLeader, newLeader)
	}
}

// respondToReadStates attempts to satisfy pending linearizable-read waits by
// forwarding read states whose confirmed index is at or below the current
// applied index onto readStatesNotifier. Returns the slice of read states
// that could not yet be satisfied.
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

// applySnapshotToFSM restores the FSM from a Raft snapshot and advances the
// applied index and confState to match the snapshot metadata. No-ops on an
// empty snapshot or a snapshot that is not newer than the current applied
// index.
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

	// TODO: P1: Do I need this here?
	node.setAppliedIndex(snapshot.Metadata.Index)
	// node.setCommittedIndex(snapshot.Metadata.Index)
	// node.setTerm(snapshot.Metadata.Term)
	node.setConfState(snapshot.Metadata.ConfState)

	node.log.Info("Restored fsm from snapshot at index", zap.Uint64("index", snapshot.Metadata.Index))
	return nil
}

// getMessagesToPublish returns the outbound messages from rd with the current
// confState patched into any MsgSnap entries. This is required because a
// ConfChange committed after the snapshot was created makes the snapshot's
// embedded confState stale; the follower must receive the up-to-date one.
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

// getEntriesToApply filters committedEntries to the subset that has not been
// applied yet (index > appliedIndex). Panics if there is a gap between the
// first committed entry and the expected next index.
func (node *Node) getEntriesToApply(committedEntries []raftpb.Entry) []raftpb.Entry {
	if len(committedEntries) == 0 {
		return committedEntries
	}

	appliedIndex := node.getAppliedIndex()
	firstIndex := committedEntries[0].Index
	if firstIndex > appliedIndex+1 {
		node.log.Panic(fmt.Sprintf("First index of committed entries (%d) is greater than applied index + 1 (%d)",
			firstIndex, appliedIndex+1))
	}

	if appliedIndex-firstIndex+1 < uint64(len(committedEntries)) {
		return committedEntries[appliedIndex-firstIndex+1:]
	}
	return []raftpb.Entry{}
}

// applyEntries applies each entry in order, dispatching normal entries to
// applyNormalEntry and configuration changes to applyConfChange. Advances
// appliedIndex after all entries are processed. Returns (true, nil) when a
// ConfChange removes this node from the cluster.
func (node *Node) applyEntries(ctx context.Context, entries []raftpb.Entry) (bool, error) {
	selfRemove := false
	publishedEntries := 0
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) != 0 {
				publishedEntries++
				if err := node.applyNormalEntry(ctx, entries[i]); err != nil {
					return selfRemove, err
				}
			}
		case raftpb.EntryConfChange:
			publishedEntries++
			var err error
			selfRemove, err = node.applyConfChange(ctx, entries[i])
			if err != nil {
				return selfRemove, err
			}
		default:
			return selfRemove, fmt.Errorf("unknown entry type: %v", entries[i].Type.String())
		}
	}

	node.setAppliedIndex(entries[len(entries)-1].Index)
	node.log.Info(fmt.Sprintf("Applied %d entries, out of %d entries.", publishedEntries, len(entries)))
	return selfRemove, nil
}

// applyNormalEntry decodes and applies a single committed data entry to the
// FSM. Skips entries whose index is at or below the current applied index
// (already applied, typically after a snapshot restore).
func (node *Node) applyNormalEntry(ctx context.Context, entry raftpb.Entry) error {
	appliedIndex := node.getAppliedIndex()
	if entry.Index <= appliedIndex {
		node.log.Warn("Skipping already applied entry", zap.Uint64("index", entry.Index),
			zap.Uint64("appliedIndex", appliedIndex))
		return nil
	}

	if err := node.fsm.Apply(ctx, entry.Data); err != nil {
		node.log.Error("Failed to apply normal entry to FSM", zap.Uint64("index", entry.Index), zap.Error(err))
		return err
	}
	return nil
}

// applyConfChange processes a membership configuration change. It applies
// the change to the Raft internal state, updates the local confState, and
// adds or removes the affected peer from the transport. Returns (true, nil)
// when the change removes this node from the cluster.
func (node *Node) applyConfChange(ctx context.Context, entry raftpb.Entry) (bool, error) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		node.log.Error("Failed to unmarshal conf change entry", zap.Uint64("index", entry.Index), zap.Error(err))
		return false, err
	}

	// Apply to raft internal state (required)
	// Note: Node.ApplyConfChange returns the new ConfState. We need to update our local confState with it.
	confState := node.n.ApplyConfChange(cc)
	node.setConfState(*confState)

	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if len(cc.Context) > 0 {
			node.transport.AddPeer(ctx, cc.NodeID, string(cc.Context))
		}
	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == node.id {
			return true, nil
		}
		node.transport.RemovePeer(ctx, cc.NodeID)
	case raftpb.ConfChangeUpdateNode:
		if len(cc.Context) > 0 {
			node.transport.UpdatePeer(ctx, cc.NodeID, string(cc.Context))
		}
	}

	node.log.Info("Applied configuration change", zap.String("entry", raft.DescribeEntry(entry, nil)),
		zap.String("newConfState", raft.DescribeConfState(node.getConfState())))
	return false, nil
}

// takeSnapshotIfNeeded creates a Raft snapshot when the number of committed
// entries since the last snapshot exceeds defaultSnapshotCount, or when
// force is true. The snapshot flow is:
//
//	Leader side                           Follower side
//	──────────────────────────            ─────────────────────────────────
//	FSM.CreateSnapshot()         ──────>  processReady()
//	storage.CreateSnapshot()               storage.SaveState()  (persist snap)
//	  wal.SaveSnap()                        transport.Send()
//	  memStorage.Compact()                  FSM.RestoreSnapshot()
//	getMessagesToPublish()        ──────>  patches confState into MsgSnap
//	transport.Send(MsgSnap)
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

	// TODO: P1: Do I need to persist TSO upto in snapshot or storage?
	if err := node.storage.CreateSnapshot(appliedIndex, confState, data); err != nil {
		node.log.Error("Failed to save snapshot", zap.Error(err))
		return
	}

	node.log.Info("Snapshot created successfully at index", zap.Uint64("appliedIndex", appliedIndex))
}

// IsLeader reports whether this node is the current Raft leader.
func (node *Node) IsLeader() bool {
	return node.getLead() == node.id
}

// Leader returns the node ID of the current Raft leader, or 0 if no leader
// has been elected yet.
func (node *Node) Leader() uint64 {
	return node.getLead()
}

// ReadIndex sends a MsgReadIndex to the Raft leader and returns the current
// applied index once the cluster confirms this node is not stale. Callers
// must then wait (via WaitForReadState) until the applied index reaches the
// returned value before serving the read.
func (node *Node) ReadIndex(ctx context.Context, rctx []byte) (uint64, error) {
	err := node.n.ReadIndex(ctx, rctx)
	if err != nil {
		return 0, err
	}
	return node.getAppliedIndex(), nil
}

// WaitForReadState blocks until a ReadState is delivered on the
// readStatesNotifier channel or timeout elapses. Used in the linearizable
// read path after ReadIndex to confirm the applied index has caught up.
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

// ProposeReqNotifier returns the send-only channel used to submit proposals
// to the proposeHandler goroutine.
func (node *Node) ProposeReqNotifier() chan<- propose {
	return node.proposeReqNotifier
}

// ProposeRespNotifier returns the receive-only channel on which the
// proposeHandler delivers proposal results back to the caller.
func (node *Node) ProposeRespNotifier() <-chan propose {
	return node.proposeRespNotifier
}

// setAppliedIndex atomically stores the highest log index applied to the FSM.
func (node *Node) setAppliedIndex(v uint64) {
	node.log.Debug("Setting applied index",
		zap.Uint64("oldIndex", node.getAppliedIndex()), zap.Uint64("newIndex", v))
	node.appliedIndex.Store(v)
}

// getAppliedIndex returns the highest log index that has been applied to the FSM.
func (node *Node) getAppliedIndex() uint64 {
	return node.appliedIndex.Load()
}

// func (node *Node) setCommittedIndex(v uint64) {
// 	node.log.Debug("Setting committed index",
// 		zap.Uint64("oldIndex", node.getCommittedIndex()), zap.Uint64("newIndex", v))
// 	node.committedIndex.Store(v)
// }

// func (node *Node) getCommittedIndex() uint64 {
// 	return node.committedIndex.Load()
// }

// func (node *Node) setTerm(v uint64) {
// 	node.log.Debug("Setting Term",
// 		zap.Uint64("oldTerm", node.getTerm()), zap.Uint64("newTerm", v))
// 	node.term.Store(v)
// }

// func (node *Node) getTerm() uint64 {
// 	return node.term.Load()
// }

// setLead atomically stores the current leader's node ID.
func (node *Node) setLead(v uint64) {
	node.log.Debug("Setting Lead",
		zap.Uint64("oldLead", node.getLead()), zap.Uint64("newLead", v))
	node.lead.Store(v)
}

// getLead returns the current leader's node ID.
func (node *Node) getLead() uint64 {
	return node.lead.Load()
}

// setSelfRemoved atomically marks whether this node has been removed from
// the cluster. Once true, the node stops processing new requests.
func (node *Node) setSelfRemoved(v bool) {
	node.log.Debug("Setting selfRemoved",
		zap.Bool("oldValue", node.selfRemoved.Load()), zap.Bool("newValue", v))
	node.selfRemoved.Store(v)
}

// getSelfRemoved reports whether this node has been removed from the cluster.
func (node *Node) getSelfRemoved() bool {
	return node.selfRemoved.Load()
}

// setConfState stores the current Raft cluster configuration state under the
// confStateMu write lock.
func (node *Node) setConfState(confState raftpb.ConfState) {
	node.confStateMu.Lock()
	node.log.Debug("Setting ConfState",
		zap.String("oldConfState", raft.DescribeConfState(node.confState)),
		zap.String("newConfState", raft.DescribeConfState(confState)))
	node.confState = confState
	node.confStateMu.Unlock()
}

// getConfState returns the current Raft cluster configuration state under
// the confStateMu read lock.
func (node *Node) getConfState() raftpb.ConfState {
	node.confStateMu.RLock()
	defer node.confStateMu.RUnlock()
	return node.confState
}

// WithLeaderChangeHook registers a callback that is invoked whenever the
// Raft leader changes. prevLeader and newLeader are the old and new leader
// node IDs respectively (0 means no leader).
func (node *Node) WithLeaderChangeHook(hook func(ctx context.Context, prevLeader, newLeader uint64)) {
	node.onLeaderChange = hook
}

// WithRemovedSelfHook registers a callback that is invoked when a
// ConfChange removes this node from the cluster.
func (node *Node) WithRemovedSelfHook(hook func()) {
	node.onRemovedSelf = hook
}

// Stop shuts down the peer transport and signals the ready and propose
// handler goroutines to exit.
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

// Process implements rafthttp.Raft. It delivers an inbound Raft message
// received from a peer into the local Raft state machine.
func (node *Node) Process(ctx context.Context, m raftpb.Message) error {
	return node.n.Step(ctx, m)
}

// IsIDRemoved implements rafthttp.Raft. It reports whether the node with the
// given id has been removed from the cluster.
func (node *Node) IsIDRemoved(id uint64) bool {
	return node.cluster.IsNodeRemoved(id)
}

// ReportUnreachable implements rafthttp.Raft. It notifies the Raft state
// machine that the node with the given id is temporarily unreachable.
func (node *Node) ReportUnreachable(id uint64) {
	node.n.ReportUnreachable(id)
}

// ReportSnapshot implements rafthttp.Raft. It reports the outcome of a
// snapshot send to the node identified by id.
func (node *Node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.n.ReportSnapshot(id, status)
}
