Critical bugs I can see:

The first major issue is that after a snapshot restore, the TSO isn't reinitialized on the leader. When the badger DB gets restored from a snapshot, the TSO's watermarks become stale since they were based on the old DB state. If the leader then applies more entries after the restore, the TSO could hand out timestamps that conflict with previously issued ones.

I'm also realizing there's a question about when a leader would even apply a snapshot to itself—in etcd-raft, typically only followers receive snapshots from the leader, so I need to think through whether this is actually a scenario that occurs. The issue is that after DropAll() clears the database, the TSO's persistent key is lost, so when the node becomes leader it reinitializes from the current time rather than the snapshot's value—but the database might already contain transactions with higher composite timestamps, creating a conflict. There's also a race condition where a snapshot restore and leader election can happen simultaneously, and if the node becomes leader mid-snapshot, the TSO initialization could use stale wall clock values while the database has already moved forward. So the realistic issue is that a node applies a snapshot as a follower, then later becomes leader and initializes TSO from the restored DB—which should work correctly since the snapshot is already applied by that point. The edge case would be if a node somehow becomes leader in the same Ready batch containing the snapshot, but that's unlikely.

The real problem I'm seeing is that UpdateWindow is commented out, so the persistent TSO upper bound value in Badger never gets updated. This means when newTSO reads the value after becoming leader, it gets whatever was last stored (or falls back to time.Now() if nothing exists), which could be stale or incorrect.

Then when setting the watermarks, it uses tso.current.Compose() to initialize both readMark and txnMark, which should theoretically be correct—but only if the persistent value was actually up-to-date, which it isn't because the window update is disabled. position IF persistentKey was properly persisted. But again, UpdateWindow is commented out so persistentKey is never being written to Badger. newTSO initializes from time.Now() if not found, which is fine.

Actually wait - newTSO does write persistentKey indirectly? Let me re-read... The code attempts to load the last TSO from the database, and if it exists, resets to that value and recalibrates. If not found, it falls back to the current time. But here's the issue: if data was committed at a higher logical counter within the same millisecond, the composite TSO (physical_ms shifted left 18 bits OR'd with logical) could be ahead of what time.Now() would produce, since logical counters can go up to 262,143 within a single millisecond.

When newTSO initializes, it starts at time.Now().UnixMilli() << 18 | 0, which has the same physical component as old committed data but a lower logical component. Since time.Now() keeps advancing, the physical time of new transactions will always be >= the original physical time, so new commit timestamps will be >= old ones. The calibration logic ensures this ordering is maintained correctly.

Now I'm seeing there's a race condition in listenProposeResponses during leadership transitions. When the node loses leadership, it closes the leaderChangeNotifier channel and stops listening for responses. But when it regains leadership and creates a new channel to restart listening, there's a window where proposeAndWait might still be running in another goroutine and could race with the channel recreation.

The timeout mechanism does catch this—if a response never arrives, the caller waits up to 5 seconds before giving up. But there's also a potential blocking issue if the proposeNotifier channel fills up, since proposeAndWait will block when trying to send to a full buffered channel. proposals, the second one would block forever. Let me check...

proposeNotifier is a buffered channel with capacity 1. If multiple goroutines call proposeAndWait concurrently, they'd block trying to send to s.node.ProposeNotifier() since the node.run() loop processes proposals one at a time. This could starve callers.

Now looking at onLeaderChange - it's called from within node.run() but modifies several unprotected fields like s.tso, s.fsm.tso, s.tm, and s.leaderChangeNotifier without any mutex. This creates a real data race: if another goroutine reads these fields while onLeaderChange is modifying them, you could hit a nil pointer dereference. For instance, s.Set() might check IsLeader() and get true, but before it calls s.tm.BeginTxn(), leadership changes and s.tm becomes nil, causing a panic.

The waitForReadState function only works correctly on the leader - followers return immediately and read directly from Badger, which means reads aren't linearizable on followers. That's intentional for stale reads but could confuse callers expecting consistency.

There's also an issue with applySnapshotToFSM: after a snapshot is applied with DropAll + Load, the TSO in the FSM isn't being updated properly.

But the most critical problem I'm seeing is a data race in DBStore. The s.tso and s.tm fields are being accessed concurrently without synchronization - onLeaderChange writes to them while Set, Get, and NewTransaction read them from different goroutines. I need to add a sync.RWMutex to protect these fields.

Looking at the onLeaderChange implementation more closely, I can see the race conditions happening directly in the code where TSO and transaction manager are initialized when this node becomes leader, and then cleared when it steps down.

The problem is clear when I trace through the execution path: a client calls Set(), which checks IsLeader() and passes, but then by the time proposeTxnSubCommand() tries to access s.tm, the leadership could have changed and s.tm is now nil, causing a crash.

The fix needs to add synchronization around these shared fields to prevent concurrent access during leadership transitions. to DBStore

In onLeaderChange, take exclusive lock
In proposeTxnSubCommand, NewTransaction, BatchWrite — take read lock and check tm is not nil
The issue is that checking IsLeader and then using tm is inherently racy—these need to happen atomically. A cleaner approach would be wrapping leader-only operations in a helper that atomically checks leadership and returns the current tm/tso under lock. For the minimal fix though, I'll add a mutex to DBStore protecting tso, tm, and leaderChangeNotifier, acquire it exclusively in onLeaderChange when modifying these fields, and take read locks in the transaction methods before accessing them.

Now I need to think through the actual implementation: I'll add a helper function that safely retrieves the transaction manager under lock, then update onLeaderChange to hold the lock during modifications, and have proposeTxnSubCommand, NewTransaction, BatchWrite, and BatchCreate use this helper. I'm also wondering about potential race conditions when TSO calls DropAll during RestoreSnapshot—when the FSM lock is held and the database is being cleared and reopened, there could be issues with concurrent access.

Actually, DropAll doesn't reopen the database, it just marks data for deletion, and then DB.Load() reloads it. If the TSO is running on the leader and in the middle of NewTransactionAt when DropAll happens, that could corrupt state, but this scenario (leader receiving a snapshot) is extremely rare and an edge case in etcd-raft.

Let me focus on the practical bugs: the real issue is the race condition on s.tso and s.tm causing nil pointer panics. When leadership changes, one thread could be in Set() calling proposeTxnSubCommand() waiting to call s.tm.BeginTxn() while another thread in onLeaderChange() sets s.tm to nil as the node becomes a follower.

There's also a potential deadlock with leaderChangeNotifier — when onLeaderChange tries to send on the channel and close it from within the node.run() goroutine, but listenProposeResponses has already exited, the send would block forever. The fix for both is to add proper mutex protection and make the channel non-blocking or handle the send more carefully.

Now I'm planning the actual implementation: I'll add a read-write mutex to DBStore to guard access to the transaction manager, TSO, and leader change notifier. I'll create a helper method to safely retrieve the transaction manager under a read lock, and use a write lock in onLeaderChange when the leadership state changes.

Writing the mutex and helper methods... I see the issue now - if I make the send non-blocking, the listener might still be running and miss the signal, but closing the channel solves that since reading from a closed channel immediately returns the zero value, which will unblock the listener. So the approach is to attempt a non-blocking send first, then close the channel to ensure the listener stops regardless.

Now I need to think through the nil pointer panic more carefully by examining how Set() calls proposeTxnSubCommand() and what happens when the context is cancelled.

The issue is that !s.IsLeader() and the actual use of s.tm aren't atomic—between the leadership check passing and accessing the transaction manager, the node could lose leadership and s.tm could become nil. The safest fix is to add a nil check for s.tm inside proposeTxnSubCommand() under the lock, returning ErrNotLeader if it's nil. This way I don't need to remove the existing IsLeader() checks, but they become a first line of defense rather than the only protection.

Now I'm ready to write out the actual code changes needed. Looking at the DBStore struct, there's no existing mutex, so I need to add one. My plan is to add mu sync.RWMutex to the struct, then wrap the leader change logic and transaction manager access points with appropriate locking.

But I'm realizing there's a deadlock risk: if I hold a read lock through the entire proposeAndWait call, and onLeaderChange tries to acquire a write lock, we're stuck. So instead I'll capture the transaction manager reference under a brief read lock, then release it before proceeding with the blocking operation.

Implementing a helper method...

Still writing helper methods... The real issue is that Txn holds a reference to the entire DBStore and accesses s.tso directly during operations like Commit() and Discard(), so even if we safely retrieve the TxnManager, the transaction itself will still hit nil pointers when the leader state gets invalidated. The cleanest solution would be to capture the tso instance at transaction creation time rather than accessing it dynamically through the store. I'm leaning toward Option A with the mutex protection, but let me reconsider what the actual minimal fix needs to be. The core panic happens when proposeTxnSubCommand tries to call BeginTxn() after leadership is lost and s.tm becomes nil, so I should add nil checks at those specific call sites under a lock. However, that leaves a similar race condition in Txn.Commit() and Txn.Discard() that's less likely but still possible, so I need to decide whether a complete thread-safe redesign is worth it or if targeted fixes are sufficient.

Looking at the actual failure modes: the main panic risk is accessing s.tm when it's nil, the secondary risk is accessing s.tso through a transaction after the store loses leadership, and there's also a potential deadlock if the leader change notification channel blocks. The cleanest approach is to have transactions hold their own TSO reference directly, add a mutex to protect the state transitions, and use non-blocking sends on the notification channel. I'll create a helper function that safely retrieves both the transaction manager and TSO under a lock, then update the relevant call sites to use this protected access pattern.

Implementing this in txn.go means storing the TSO directly on the transaction so Commit and Discard don't need to reach back through the store. The tricky part is that transactions hold a reference to the DBStore itself, which gets passed to callbacks, so I need to be careful about how the initialization flows. Now I need to review the full txn.go file to identify all the places where the TSO field needs to be added and used, particularly in the Commit and Discard methods where they currently reference the database's TSO instead of the transaction's own copy.