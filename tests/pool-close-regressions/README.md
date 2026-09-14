# Pool-close backport regressions

This independent consumer exercises the core-only backport of
[SQLx issue 3217 / PR 3952](https://github.com/transact-rs/sqlx/commit/5f8fc6b752b20bbfbc80d1d381e469d48b05b561).
It uses the registry SQLx and driver crates at 0.8.0, patching only `sqlx-core`
to this checkout. This is the dependency shape Agent Harbor ships, including
the existing SQLite 0.28 compatibility constraint and the earlier num-idle fix.

With a real disposable PostgreSQL server available through `DATABASE_URL`, run
from the SQLx repository root:

```sh
cargo test --locked --manifest-path tests/pool-close-regressions/Cargo.toml
```

Agent Harbor contributors should run this through their normal native `just`
test harness and ephemeral-PostgreSQL wrapper. Missing PostgreSQL is an error,
not a successful skip. Each test retains a unique complete log; failures print
its path and size. `SQLX_POOL_CLOSE_TEST_LOG_DIR` optionally selects the log root.

The eighteen cases use the same source files registered by the main SQLx workspace:

- Real idle and held SQLite workers, counters, and native Windows exclusive-open.
- A barrier in the actual return callback, proving the final return is drained.
- Empty and partially populated child pools, parent capacity, and a fresh sibling
  that must not steal a manufactured permit while both parent slots are held.
- Concurrent alias close and cancellation of a waiting close.
- A bounded, real SQLite worker-destruction barrier proving raw-close
  acknowledgement and serialization of aliases even with unused parent capacity.
- A child with a previously closed checkout and a second checkout still held,
  distinguishing retained stolen permits from the current connection count.
- A sparse child whose retained spare permits must serialize aliases through
  the actual worker-destruction barrier, without waiting for unborrowed capacity.
- Multiple parent borrows queued before capacity becomes available, proving the
  child cap, release of reserved parent permits, cancellation and real sibling capacity.
- A still-pending parent borrow cancelled at the child cap before a later parent
  credit, while the losing child borrower remains alive and unpolled.
- A real connection-open callback barrier after reuse of a retained child permit,
  proving close waits for the in-flight open and its eventual held checkout.
- Cancellation after an actual parent-to-child transfer whose token belongs to an
  older queued child waiter, proving close and child drop retain exact ownership.
- Continued live borrowing after such a donation, repeated donations at cap three,
  and a separately scheduled donor requiring real wakeups rather than manual polls.
- An actual LISTEN/NOTIFY stream whose checkout must be released before close.

Independent review rejected the initial candidate: the complete upstream
backport failed `child_close_waits_with_retained_spare_permit`, returning before
the remaining checkout is released and then underflowing the size counter.
The identical real-worker diagnostic passes the exact previous close algorithm.
The local correction therefore intentionally extends the upstream patch:
it tracks total permit ownership independently of live size and freezes child
borrowing atomically when closing. Successful parent transfers are counted,
credited to the child, and acquired through the original queued child future;
unused transferred capacity remains child-owned until drop. Transfer bookkeeping
does not hold a lock across asynchronous waits or semaphore/listener wakeups.
Re-review also rejected an initial ownership correction: a still-live borrower
could not reborrow after its first transfer served an older child waiter. Each
completed parent future transfers once, but an acquisition still waiting below
the child cap now arms a new parent future and schedules its next poll. The
original child queue position is preserved across repeated donations; wakeups
remain outside the ownership lock. Independent source/mutation review now passes
with eighteen consumer cases, twenty-one native Agent Harbor integration cases
and all 828 REST library cases, without skips or retries. Strict affected REST
Clippy also passes. Parent audit and publication acceptance remain required;
production pins are restored to their previous revision. Earlier fourteen-case
passes are historical and do not cover this liveness correction.

Mutation review retained an initial surviving pending-parent-cancellation fault
and added the delayed-credit regression above; the identical fault then fails.
The native destruction barrier now observes an owned close thread with its own
runtime, records the actual destruction thread, and checks close completion and
alias serialization before explicit release. SQLite can legally destroy its last
shared connection reference on the close caller itself, so requiring a manual
first poll on the observer to yield is not a valid universal oracle. For separate
worker-thread destruction, an actual first-poll receipt still detects omitted
raw-close await. Cleanup releases the bounded barrier before joining the owned
thread, and preserves the saved primary verdict. Original failure epochs remain
evidence; passing this bounded consumer is not whole-upstream acceptance.
Waiting-close and transferred-borrower cancellation
is covered; general cancellation halfway through raw worker drain is not claimed.

The harness is not the full upstream test suite. The original 0.8.0 PostgreSQL
workspace member has never-type-fallback inference errors in unchanged source
with Rust 1.92; compiling the entire SQLx facade therefore fails before these tests
run. This consumer does not modify that driver, change lint settings, or change
the production core backport to work around those unrelated errors. The main
workspace test targets remain registered for compatible upstream environments.

The checked-in harness lock was seeded from Agent Harbor's existing production
lock and pruned by Cargo metadata. Keeping it is important: its already-locked
`spin` 0.9.8 dependency is yanked and cannot be selected by a fresh resolution.
The registry drivers and `libsqlite3-sys` must remain at 0.8.0 and 0.28.0;
only `sqlx-core` comes from the local fork.
