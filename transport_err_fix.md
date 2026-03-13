// Good news first: **this log is not an error in the Raft sense**.
// It’s `rafthttp` doing an **intentional, orderly teardown of a peer connection**.

// The confusing part is that it *looks* scary because it’s noisy and full of `warn`s.

// Let’s decode exactly what’s happening and then I’ll tell you **when this is OK vs when it’s a real bug**.

// ---

// ## What actually happened (timeline)

// Read the first line carefully:

// ```
// "stopping remote peer","remote-peer-id":"3"
// ```

// That line is the *cause*. Everything after it is **effect**.

// ### Step-by-step

// 1. **Your code (or raft) told the transport to stop peer 3**

//    * This happens when:

//      * node 3 is removed from ConfState
//      * transport is shutting down
//      * you called `transport.RemovePeer(3)`
//      * context was canceled during shutdown

// 2. `rafthttp` closes all streams to peer 3

//    * MsgApp v2 stream
//    * general message stream
//    * HTTP pipeline

// 3. Closing streams cancels contexts

//    * Readers see `context canceled`
//    * Writers log “closed TCP streaming connection”

// 4. Peer is marked inactive

//    * Then fully stopped

// Everything you see is **cleanup noise**.

// ---

// ## Why you see `context canceled`

// This line is the key:

// ```
// error":"context canceled"
// ```

// That means:

// * The connection was **closed intentionally**
// * Not a network failure
// * Not data corruption
// * Not a Raft bug

// `rafthttp` uses contexts to coordinate stream lifecycles.
// When a peer is stopped, all contexts are canceled → readers log warnings.

// ---

// ## When this log is 100% normal

// This log is expected if **any** of these are true:

// ### ✅ Node shutdown

// * You’re stopping a node
// * Process is exiting
// * Test cleanup

// ### ✅ Membership change

// * Peer 3 was removed
// * ConfState changed
// * Snapshot with new ConfState applied

// ### ✅ Transport reconfiguration

// * You re-created transport
// * Restarted HTTP server
// * Changed peer URLs

// ### ✅ Dev / test environment

// * Nodes restart frequently
// * Ports reused
// * Contexts canceled aggressively

// In dev, you’ll see this *a lot*.

// ---

// ## When this is NOT normal (red flags)

// Now the important part.

// This log **is a problem** if:

// ❌ It happens repeatedly while the cluster is supposed to be stable
// ❌ Peer flips active → inactive → active in a loop
// ❌ Leader keeps stepping down
// ❌ You see `MsgSnap` never completing
// ❌ Applied index stops advancing

// Then this log is a *symptom*, not the root cause.

// ---

// ## Most common real causes (in your stage)

// Given everything you’ve said so far, **top 3 likely causes** are:

// ### 1️⃣ ConfState mismatch after snapshot

// If:

// * Snapshot ConfState doesn’t match applied index
// * Peer 3 isn’t actually in the cluster anymore

// → Transport stops peer 3 correctly, but you didn’t expect it.

// **Check**:

// ```go
// snapshot.Metadata.ConfState
// ```

// ---

// ### 2️⃣ Transport lifecycle not aligned with Raft lifecycle

// Classic mistake in custom stacks:

// * Raft node restarted
// * Transport reused
// * Or transport restarted before raft

// Result:

// * Context canceled
// * Streams torn down
// * Peer deactivated

// **Rule**:

// > Transport lifetime must match Raft node lifetime.

// ---

// ### 3️⃣ Snapshot install in progress, then node shutdown

// During snapshot:

// * Streams are long-lived
// * Shutdown cancels them
// * Logs look scary

// Totally normal during dev restarts.

// ---

// ## How to confirm which case you’re in (quick checks)

// Ask yourself:

// 1. Did peer 3 just get removed or restarted?
// 2. Did I recently:

//    * apply a snapshot?
//    * restart a node?
//    * recreate transport?
// 3. Does the cluster recover and continue?

// If yes → **this is fine**.

// ---

// ## How etcd treats these logs (important context)

// In real etcd clusters:

// * These logs appear **constantly**
// * They are not alerts
// * Operators mostly ignore them unless cluster stalls

// So don’t chase them blindly.

// ---

// ## One-line diagnosis

// > You’re seeing this because `rafthttp.Transport` intentionally stopped peer 3 and canceled its streams; the warnings are cleanup noise, not a Raft failure.

// ---

// ## What I’d look at next (if you want)

// If you suspect this *is* causing issues, we can:

// * Inspect your **ConfState evolution**
// * Verify **transport + raft startup/shutdown ordering**
// * Trace **snapshot apply → peer removal**
// * Add a small guard to log *who* called `RemovePeer`

// If you paste:

// * when this happens (startup / snapshot / shutdown)
// * whether peer 3 comes back

// …I can pinpoint it very fast.

