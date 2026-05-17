Here are some **real-world patterns** where `TestMain` + `testing.M` are actually useful—especially for integration tests.

---

# 🐘 1. Database setup (common in backend services)

Imagine you need a database running before tests.

```go id="y0c4j6"
var db *sql.DB

func TestMain(m *testing.M) {
    // Setup: connect to test DB
    var err error
    db, err = sql.Open("postgres", "postgres://localhost/testdb?sslmode=disable")
    if err != nil {
        panic(err)
    }

    // Optional: run migrations
    runMigrations(db)

    code := m.Run()

    // Cleanup: close DB
    db.Close()

    os.Exit(code)
}
```

### Why use `TestMain` here?

* Avoid reconnecting DB for every test
* Run migrations only once
* Faster + consistent tests

---

# 🐳 2. Using containers (Docker-based integration tests)

Many Go projects use libraries like **testcontainers-go**.

```go id="t00m9p"
var postgresContainer testcontainers.Container

func TestMain(m *testing.M) {
    ctx := context.Background()

    // Start container
    postgresContainer = startPostgresContainer(ctx)

    // Run tests
    code := m.Run()

    // Stop container
    postgresContainer.Terminate(ctx)

    os.Exit(code)
}
```

### What this gives you

* Fresh, isolated environment
* No dependency on local setup
* CI-friendly tests

---

# 🌐 3. Start a test HTTP server once

```go id="h9wq3x"
var server *http.Server

func TestMain(m *testing.M) {
    server = &http.Server{Addr: ":8081"}

    go server.ListenAndServe()

    // Give server time to start (better: use sync or health check)
    time.Sleep(time.Second)

    code := m.Run()

    server.Close()

    os.Exit(code)
}
```

### Why this pattern?

* Avoid restarting server per test
* Useful for API / integration tests

---

# ⚙️ 4. Global config / environment setup

```go id="0k9n0g"
func TestMain(m *testing.M) {
    os.Setenv("APP_ENV", "test")
    os.Setenv("LOG_LEVEL", "debug")

    code := m.Run()

    os.Unsetenv("APP_ENV")
    os.Unsetenv("LOG_LEVEL")

    os.Exit(code)
}
```

---

# 🧪 5. Seeding shared test data

```go id="b9sz2n"
func TestMain(m *testing.M) {
    seedTestData()

    code := m.Run()

    cleanupTestData()

    os.Exit(code)
}
```

---

# ⚠️ Best practices (important)

### ✅ Keep it lightweight

* Don’t put heavy logic unless necessary

### ✅ Prefer per-test isolation when possible

* Use `t.Cleanup()` for test-specific setup

### ✅ Avoid global state conflicts

* Parallel tests can break if sharing mutable globals

---

# 🚫 Common mistakes

### ❌ Forgetting `m.Run()`

→ No tests will execute

### ❌ Forgetting `os.Exit(code)`

→ Exit code may be wrong

### ❌ Overusing `TestMain`

→ Makes tests harder to reason about

---

# 🧠 When you should reach for it

Use `TestMain` when you need:

* One-time setup for **entire package**
* Expensive resources (DB, containers, servers)
* Global lifecycle control

---

# 💡 Rule of thumb

* Unit tests → use `testing.T`
* Integration/system tests → consider `testing.M`

---

If you want, I can show a **complete working example (DB + HTTP + tests)** like you'd see in a production Go service.

---

Yes—you *can* set up a DB in `TestMain()` and reuse it across all tests in a package. It’s a common pattern, but you need to do it carefully to avoid flaky tests.

---

# ✅ Basic pattern (shared DB from `TestMain`)

```go
package mypkg

import (
    "database/sql"
    "os"
    "testing"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
    var err error

    testDB, err = sql.Open("postgres", "postgres://localhost/testdb?sslmode=disable")
    if err != nil {
        panic(err)
    }

    // Optional: migrations
    runMigrations(testDB)

    code := m.Run()

    testDB.Close()
    os.Exit(code)
}
```

Now all your tests can use `testDB`:

```go
func TestCreateUser(t *testing.T) {
    _, err := testDB.Exec("INSERT INTO users (name) VALUES ($1)", "Alice")
    if err != nil {
        t.Fatal(err)
    }
}
```

---

# ⚠️ The real problem: test isolation

If you stop here, your tests will:

* Interfere with each other ❌
* Become order-dependent ❌
* Break when run in parallel ❌

So you need isolation.

---

# 🛡️ Best practice: wrap each test in a transaction

This is the **most common and safest approach**.

```go
func withTx(t *testing.T) *sql.Tx {
    tx, err := testDB.Begin()
    if err != nil {
        t.Fatal(err)
    }

    // Rollback after test finishes
    t.Cleanup(func() {
        tx.Rollback()
    })

    return tx
}
```

### Usage:

```go
func TestCreateUser(t *testing.T) {
    tx := withTx(t)

    _, err := tx.Exec("INSERT INTO users (name) VALUES ($1)", "Alice")
    if err != nil {
        t.Fatal(err)
    }

    // No cleanup needed — auto rollback
}
```

---

# 🔁 Alternative strategies

## 1. Truncate tables between tests

```go
func cleanupDB(t *testing.T) {
    _, err := testDB.Exec("TRUNCATE users, orders RESTART IDENTITY")
    if err != nil {
        t.Fatal(err)
    }
}
```

✔ Simple
❌ Slower
❌ Risky with parallel tests

---

## 2. Separate database per test (heavy but clean)

* Create a new DB per test
* Drop it afterward

✔ Perfect isolation
❌ Slow
❌ Complex

---

## 3. Use containers (most realistic)

Using **testcontainers-go**:

* Start DB once in `TestMain`
* Use transactions per test

✔ Real environment
✔ CI-friendly
✔ Good balance

---

# ⚡ Recommended setup (production-style)

👉 `TestMain()`:

* Start DB / container
* Run migrations

👉 Each test:

* Start transaction
* Rollback via `t.Cleanup()`

This gives:

* Speed ✅
* Isolation ✅
* Determinism ✅

---

# 🧠 Mental model

* `TestMain` → **lifecycle (start/stop DB)**
* Each test → **sandbox (transaction)**

---

# 🚫 When NOT to share DB

Avoid shared DB if:

* Tests mutate global state heavily
* You rely on parallel tests (`t.Parallel()`)
* You can use mocks instead (true unit tests)

---

# 💡 Final takeaway

Yes, you can share a DB from `TestMain`, but:

👉 **Always isolate each test (prefer transactions)**
Otherwise your tests will become flaky very quickly.

---

If you want, I can show a **full example with Postgres + migrations + testcontainers + transactions** like a real backend repo.
