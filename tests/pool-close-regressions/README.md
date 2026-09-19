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

The twenty-five cases use the same source files registered by the main SQLx workspace:

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
- A parent pool whose `close()` must complete while a still-open child pool
  holds a donated permit, and while a CLOSED, fully drained child pool handle
  holds one with no connection live anywhere.
- A parent whose `close()` must stay bounded when the child handle is gone but a
  real checkout still keeps the child's inner pool -- and its permit -- alive.
- Exact parent capacity across a donation that is returned and a donation lost
  for good through `PoolConnection::leak`, proving the parent counts what it can
  actually reacquire rather than what it once donated.
- Two negative controls against an over-eager close: a parent must still block on
  its OWN held checkout, and must still drain its own late return that lands in
  the idle queue after the final permit, both while a child holds a donated
  permit.
- A donation parked, through a test-only seam, in the exact window between the
  borrower's parent-close-event poll and its read of the parent's closed state.
  The parent is closed and its close polled once (so it has already read its
  frozen target) before the donation resumes; the donation must then be REFUSED
  and the parent's `close()` must still complete. See "Closed-parent donation
  refusal" below for why no ordinary test can reach that branch.

## Parent and child close semantics

A child pool starts with zero permits and permanently takes ("steals") capacity
from its parent; a stolen permit returns only when the CHILD POOL IS DROPPED,
not when it is closed. Each pool's `close()` therefore waits only for the
capacity that pool still owns:

- Closing a parent does not wait for capacity a child borrowed, and does not
  close or drain the child's connections. It does stop further borrowing, so the
  child's later `acquire()` calls fail with `PoolClosed`.
- Closing a child waits for every permit it borrowed, including spares it is no
  longer using, and returns none of them to the parent.
- Ownership moves in a single critical section, under a documented total lock
  order (a pool may lock its own permit count and then an ancestor's, never the
  reverse; the ancestor chain is finite and acyclic because `PoolOptions::parent`
  can only name an already constructed pool). A donation is refused, and the
  permit handed straight back, if the parent has already frozen its count by
  closing; that refusal is pinned by execution in
  `parent_close_refuses_a_donation_that_races_its_mark_closed`.
- On drop, a child credits its ancestor with exactly the number of permits it
  actually releases into the ancestor's semaphore, never with the number it once
  received. `PoolConnection::leak` keeps a permit out of circulation for good, so
  the two can differ; crediting the donated total instead would make the
  ancestor's `close()` wait for a permit nobody can supply.

Without this symmetry a parent's `close()` blocks until every borrowing child
handle is dropped -- even when the child is closed, fully drained and holding no
connection at all. All six symmetry cases above fail on the pre-correction source
and pass after it; the seventh, the closed-parent refusal control, is a different
kind of proof and is described in its own section below.

A pool that has removed a connection with `PoolConnection::leak` can no longer
complete its OWN `close()`: the permit never returns to that pool's semaphore and
its `size` is never decremented. That is pre-existing `leak()` behaviour, is
identical on base 166, and applies to a parentless pool exactly as to a child.
Only the PARENT's close is unaffected, because the parent is credited with what
was actually released to it rather than with what it donated.

The tests originated in the separately preserved 2026-09-13 candidate
`d41a6af52749fba033cd170cb360019690f3cf2b`. This landing repair is based instead
on production `166dbaa99d0609daa94982d5d775de5bad6f4aab`, retaining its migration
ordering fix. Historical candidate results are not acceptance of this new base.
Independent review of that earlier work rejected the initial candidate: the complete upstream
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
remain outside the ownership lock. The new-base test-first run reached the real
PostgreSQL listener test and failed because close returned with its checkout
still held; default fail-fast left the other seventeen cases unrun. Corrected
qualification must run every case with `--no-fail-fast`, plus Agent
Harbor's deterministic real-PostgreSQL late-return/retained-alias regression.
Independent review and publication acceptance remain required. Do not replace
production dependency pins with the historical candidate.

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

## Current-base author qualification (2026-09-18)

Native Rust 1.92 through Agent Harbor's normal Just/Repro resource-guarded
ephemeral-PostgreSQL recipe passes all eighteen cases of that epoch without skips
or retries. (See the parent-close correction below for the current count.)
A temporary local-core override also passes all 64 `ah-admin-api` cases,
including the new deterministic PostgreSQL late-return regression: its retained
alias observes zero live/idle connections and the independent observer sees zero
exact server backends. That regression failed on production 166. Both production
manifests and locks were restored byte-for-byte after the local run.

Fresh runtime mutations replace owned capacity with live size (the held-child
control fails before releasing its actual checkout) and omit the raw-close await
(both actual worker-shutdown/alias controls fail). Saved failures survive
successful fixture cleanup. Scoped rustfmt and strict all-target consumer Clippy
pass. Primary-core strict Clippy remains blocked by sixteen diagnostics in
unchanged 166 code (driver cfgs, existing doc/style and lifetime syntax); no lint
is suppressed. Selecting core as a consumer dependency and seeing exit zero is
not primary-core strict acceptance. Independent review, fork publication and
permanent Agent Harbor dependency/source-pin integration are still pending.
The final post-mutation replay stalled before tests in the native Repro cleanup
prerequisite and was stopped with approval. It has no test verdict. Restored core
bytes exactly match the earlier passing eighteen/64-case runs; independent review
must repeat the restored suites rather than relabel those earlier results.

## Parent-close correction (2026-09-18)

A second independent review qualified the eighteen-case base at runtime and then
found one real, untested behavioural change: the repaired `close()` never reduced
a parent's owned-permit count when a child borrowed capacity, so a parent's
`close()` blocked until every borrowing child handle was dropped. A matched
probe pair settled it by execution, not argument: on base 166
`parent_close_completed_with_live_child` was `true`; on the repaired source it
was `false` within a ten-second bound, and `true` only after the child handle was
dropped.

The bounded correction makes the accounting symmetric, as described under
"Parent and child close semantics" above, and adds the six regressions listed
with the other cases. Red/green was recorded on the same recipe: against the
pre-correction source all eighteen existing cases passed and all six new cases
failed (24 run, 18 passed, 6 failed, 0 skipped, 201.973 s, each new failure a
real bounded-wait timeout, not a setup or compile error); after the correction
all twenty-four pass with zero skips in 14.799 s. No existing assertion was
weakened, no test is ignored or skipped, and the deterministic 166 migration
ordering and the earlier num-idle correction are untouched.

## Closed-parent donation refusal (2026-09-18)

Independent mutation review of the correction above killed three of four
mutants. The survivor deleted the closed-parent refusal in the donation critical
section (`if parent.0.is_closed() {` -> `if false && parent.0.is_closed() {`) and
all twenty-four cases still passed. The guard is nonetheless load-bearing:
`PoolInner::close()` calls `mark_closed()` eagerly, when the future is created,
and only afterwards re-takes the lock to read `permits_to_acquire`. A donation
landing in that window without the refusal debits a count the parent's `close()`
may already have read, so the parent waits forever for a permit the child now
owns -- an unconditional parent-close deadlock.

No ordinary test can reach that branch. The only other guard,
`parent_close_event` at the top of the same `poll_fn`, fully covers "already
closed on entry" and "closed later, task woken"; what it cannot cover is a store
landing after it was polled within the current poll. There is no await point in
that window, so every deterministic single-threaded schedule hits the event
guard first.

Three options were evaluated. A loom or shuttle model check was rejected because
both require every primitive on the path -- `std::sync::Mutex`, `AtomicBool`,
`event_listener::Event` and tokio's semaphore inside `AsyncSemaphore` -- to be
the model checker's own; instrumenting them would model a rewrite rather than the
shipped code, and so could not kill a mutation in the shipped code. A
probabilistic multi-thread stress loop was rejected because the window is a few
instructions wide, must coincide with an actual parent-permit handoff, has a
failure mode that is a 30 s timeout, and would intermittently redden CI while
only sometimes killing the mutation.

What landed instead is a test-only synchronisation seam: sqlx-core's non-default
`_test-pool-donation-barrier` feature and the `pool::donation_barrier` module.
`#[cfg(test)]` is not usable, because these regressions live in an external
consumer crate where sqlx-core is compiled as a dependency. With the feature off
neither the module, nor its static, nor its call site exists, so a production
build is bit-for-bit unaffected and the public API is unchanged; Agent Harbor's
dependency does not enable it. The single call site sits after the parent permit
is in hand and before any bookkeeping lock is taken -- parking a thread any later
would deadlock against the parent's own `mark_closed`.

Because the seam is cfg-gated, the one case that uses it is cfg-gated too, on a
feature of the same name declared by whichever package registers
`tests/sqlite/pool_close.rs`. That keeps the main SQLx workspace building: the
`sqlx` package forwards `_test-pool-donation-barrier` to `sqlx-core` but never
enables it by default, so `cargo test --features sqlite,runtime-tokio` compiles
the `sqlite-pool-close` target and runs twenty-three of its twenty-four cases,
and `cargo test --features sqlite,runtime-tokio,_test-pool-donation-barrier` runs
all twenty-four. In this consumer the feature is instead a DEFAULT feature and is
listed in the target's `required-features`, so the case cannot quietly go missing
from a green regression run: with the seam off cargo refuses to build the target
at all and the whole SQLite file drops out of the count.

`parent_close_refuses_a_donation_that_races_its_mark_closed` uses it to park a
real donation in the window, close the parent, poll the close once so it has
already read its frozen target, and then resume the donation. The refusal must
happen, the parent's `close()` must complete within the usual bound, the
borrower must observe `PoolClosed`, and the child must be marked closed with no
borrowed capacity left behind. Applying exactly the surviving mutation makes that
test -- and only that test -- fail, with "parent close never finished after a
donation raced its mark_closed" (23 of the 24 SQLite cases passed, 1 failed);
the restored source passes 30 consecutive runs of that case with no failure and
no run approaching its bound.

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
