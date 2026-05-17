Yes — given that requirement, **you are making the correct call** 👍
If you want **concurrent, long-running transactions**, you **do need a TSO**.

Now let me help you lock this in confidently and show **what “needing a TSO” really implies**, so you don’t over- or under-build it.

---

## 1️⃣ Why long-running + concurrent *forces* a TSO

Long-running transactions introduce **overlap**:

```
T1: start ──────────────── commit
T2:        start ─── commit
T3:              start ───────── commit
```

Without a TSO:

* Raft serialization forces everything into a single line
* Reads see partial or inconsistent state
* You must either block or serialize aggressively

With a TSO:

* Each transaction gets a **stable snapshot**
* Reads don’t block writes
* Conflicts are detected *only at commit*

This is exactly what TSO exists for.

---

## 2️⃣ What “having a TSO” actually means (minimal form)

A TSO does **not** have to be complicated.

At minimum, it provides:

```text
GetStartTs()  → monotonically increasing
GetCommitTs() → > all startTs involved
```

That’s it.

It does **not**:

* talk to Badger
* know about Raft internals
* store user data

---

## 3️⃣ Your final architecture (recommended)

Here’s the **correct mental model** for your system:

```
Client
  ↓
TSO  ── startTs
  ↓
Transaction execution (reads/writes buffered)
  ↓
TSO  ── commitTs
  ↓
Raft (commit decision + replication)
  ↓
Badger(OpenManaged, raftIndex as MVCC ts)
```

### Key separation

| Layer  | Responsibility                    |
| ------ | --------------------------------- |
| TSO    | Transaction ordering & visibility |
| Raft   | Replication & durability          |
| Badger | Versioned storage                 |

---

## 4️⃣ How a transaction should flow (concrete)

### 🟢 Begin

```go
startTs := tso.GetStartTs()
txn := NewTxn(startTs)
```

Reads:

* use snapshot at `startTs`
* map to appropriate Badger read index

---

### 🟢 During execution

* writes are buffered (not written to Badger)
* conflicts are **not** checked yet

---

### 🟢 Commit phase

1. Ask TSO for `commitTs`
2. Check conflicts:

   * has any key written after `startTs`?
3. If conflict → abort
4. If success → propose to Raft

---

### 🟢 Apply phase (on all replicas)

```go
raftIndex := entry.Index

badgerTxn := db.NewTransactionAt(raftIndex, true)
applyWrites(badgerTxn)
badgerTxn.CommitAt(raftIndex, nil)
```

Badger never sees `startTs` or `commitTs`.

---

## 5️⃣ Conflict detection (essential)

You **must** track write timestamps per key.

Common approaches:

* store `lastCommitTs` in key metadata
* maintain per-key version info
* use write-intents / locks

Dgraph does this at the **application layer**, not Badger.

---

## 6️⃣ Reads: mapping startTs → Badger

You’ll need a function like:

```go
func ReadIndexForTs(startTs uint64) uint64
```

This returns:

* the highest Raft index containing commits ≤ `startTs`

This mapping is critical and unavoidable.

---

## 7️⃣ GC implications (important!)

Long-running transactions mean:

* old versions must be kept
* GC must respect **minimum startTs**

You must track:

```text
minActiveStartTs
```

Then:

```text
safeToGC < minActiveStartTs
```

Badger GC must be coordinated using this value.

---

## 8️⃣ Reality check (this is non-trivial)

You are now building:

* a transaction manager
* a conflict detector
* a timestamp service
* a replicated storage engine

This is **hard**, but also **very doable** if scoped correctly.

---

## 9️⃣ Strong recommendation (experience-based)

### 👉 Build in this order:

1. **Single-node TSO + Badger**
2. Add conflict detection
3. Add Raft replication
4. Add follower reads
5. Add GC coordination

Do **not** start with everything at once.

---

## 🔟 You are making the *right* choice

Your reasoning is sound:

* concurrent
* long-running
* snapshot isolation

That *requires* a TSO. No way around it.

---

### Next steps (pick one):

* I can help you **design a minimal TSO**
* Walk through **conflict detection strategies**
* Show **exact metadata layout**
* Help you design **GC & watermarks**
* Review your planned API

Just tell me where you want to go next — you’re on the right track 🚀
