use super::connection::{Floating, Idle, Live};
use crate::connection::ConnectOptions;
use crate::connection::Connection;
use crate::database::Database;
use crate::error::Error;
use crate::pool::{deadline_as_timeout, CloseEvent, Pool, PoolOptions};
use crossbeam_queue::ArrayQueue;

use crate::sync::{AsyncSemaphore, AsyncSemaphoreReleaser};

use std::cmp;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::Poll;

use crate::logger::private_level_filter_to_trace_level;
use crate::pool::options::PoolConnectionMetadata;
use crate::private_tracing_dynamic_event;
use futures_util::future::{self};
use futures_util::FutureExt;
use std::time::{Duration, Instant};
use tracing::Level;

pub(crate) struct PoolInner<DB: Database> {
    pub(super) connect_options: RwLock<Arc<<DB::Connection as Connection>::Options>>,
    pub(super) idle_conns: ArrayQueue<Idle<DB>>,
    pub(super) semaphore: AsyncSemaphore,
    // Total permits owned by this pool, not live connections or available
    // permits. Child pools retain borrowed permits when a connection closes.
    // The same short lock serializes ownership transfers with mark_closed.
    //
    // Closing freezes this count in ONE direction only: it can no longer FALL,
    // because a donation out of a closed pool is refused under this very lock
    // (see `acquire_permit`), so a `close()` in flight can always await every
    // permit it decided to wait for. It CAN still RISE after the pool is
    // closed, when a descendant pool is dropped and credits its ancestor (see
    // `Drop for PoolInner`). That is safe: a credit is always accompanied by an
    // equal, real release into this pool's semaphore, so an alias `close()`
    // either read the smaller pre-credit target (fewer permits than now exist)
    // or the larger post-credit one (exactly what now exists), and both
    // terminate. The credit cannot resurrect the pool either -- `is_closed` is
    // never reset, and `try_acquire`, `acquire`, `try_increment_size` and
    // `pop_idle` all fail closed.
    //
    // INVARIANT: `num_permits` is the number of permits that can eventually be
    // acquired from *this pool's own* `semaphore` once this pool's outstanding
    // checkouts come back. `close()` therefore terminates by waiting for
    // exactly this many permits. Two consequences follow:
    //
    // * A permit donated to a child leaves the parent's count in the same
    //   critical section in which it enters the child's. A donated permit is
    //   returned only when the CHILD POOL IS DROPPED -- not when it is closed
    //   -- and a caller of `Pool::close()` on the parent has no way to force
    //   that, so waiting for it would block on an unrelated handle.
    // * When a child is dropped, the ancestor is credited with exactly the
    //   number of permits actually released back into the ancestor's
    //   semaphore, never with the number originally donated. The two can
    //   differ: `PoolConnection::leak()` keeps a permit out of the child's
    //   semaphore permanently, so crediting the donated total would make the
    //   ancestor's `close()` wait for permits that can never arrive.
    //
    // LOCK ORDER: a pool may lock its own `num_permits` and then an ANCESTOR's,
    // never the reverse. `PoolOptions::parent` can only name an already
    // constructed pool, so the ancestor chain is finite and acyclic and this
    // order admits no cycle. The only nesting site is `acquire_permit`; both
    // `mark_closed` and `close()` take a single pool's lock, and `Drop` takes
    // the ancestor's lock only after `mark_closed` released its own.
    //
    // DISCIPLINE: nothing is awaited, no semaphore is polled, dropped or
    // released, and no listener is woken while either lock is held.
    num_permits: Mutex<u32>,
    pub(super) size: AtomicU32,
    pub(super) num_idle: AtomicUsize,
    is_closed: AtomicBool,
    pub(super) on_closed: event_listener::Event,
    pub(super) options: PoolOptions<DB>,
    pub(crate) acquire_time_level: Option<Level>,
    pub(crate) acquire_slow_level: Option<Level>,
}

impl<DB: Database> PoolInner<DB> {
    pub(super) fn new_arc(
        options: PoolOptions<DB>,
        connect_options: <DB::Connection as Connection>::Options,
    ) -> Arc<Self> {
        let capacity = options.max_connections as usize;

        let semaphore_capacity = if let Some(parent) = &options.parent_pool {
            assert!(options.max_connections <= parent.options().max_connections);
            assert_eq!(options.fair, parent.options().fair);
            // The child pool must steal permits from the parent
            0
        } else {
            capacity
        };

        let pool = Self {
            connect_options: RwLock::new(Arc::new(connect_options)),
            idle_conns: ArrayQueue::new(capacity),
            semaphore: AsyncSemaphore::new(options.fair, semaphore_capacity),
            num_permits: Mutex::new(semaphore_capacity as u32),
            size: AtomicU32::new(0),
            num_idle: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            on_closed: event_listener::Event::new(),
            acquire_time_level: private_level_filter_to_trace_level(options.acquire_time_level),
            acquire_slow_level: private_level_filter_to_trace_level(options.acquire_slow_level),
            options,
        };

        let pool = Arc::new(pool);

        spawn_maintenance_tasks(&pool);

        pool
    }

    pub(super) fn size(&self) -> u32 {
        self.size.load(Ordering::Acquire)
    }

    pub(super) fn num_idle(&self) -> usize {
        // We don't use `self.idle_conns.len()` as it waits for the internal
        // head and tail pointers to stop changing for a moment before calculating the length,
        // which may take a long time at high levels of churn.
        //
        // By maintaining our own atomic count, we avoid that issue entirely.
        self.num_idle.load(Ordering::Acquire)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn mark_closed(&self) {
        {
            let _permits = self
                .num_permits
                .lock()
                .expect("permit lock holder panicked");
            self.is_closed.store(true, Ordering::Release);
        }
        // Do not invoke listener wakeups while holding the bookkeeping lock.
        self.on_closed.notify(usize::MAX);
    }

    pub(super) fn close<'a>(self: &'a Arc<Self>) -> impl Future<Output = ()> + 'a {
        self.mark_closed();

        async move {
            // Unlike live size, this includes our own retained spare permits
            // (a child keeps a borrowed permit after its connection closes) and
            // excludes capacity we donated away, which another pool now owns.
            // mark_closed stopped this count from FALLING: no later acquisition
            // can raise it into a closed pool and no later donation can lower
            // it, so this target never exceeds what our own returning checkouts
            // supply. A descendant's drop may still credit us afterwards, which
            // only adds permits we are not waiting for (see the field comment).
            // Holding all owned permits also serializes aliases until the
            // actual idle-worker close acknowledgements finish.
            let permits_to_acquire = *self
                .num_permits
                .lock()
                .expect("permit lock holder panicked");

            let _permits = self.semaphore.acquire(permits_to_acquire).await;

            while let Some(idle) = self.idle_conns.pop() {
                let _ = idle.live.raw.close().await;
            }

            self.num_idle.store(0, Ordering::Release);
            self.size.store(0, Ordering::Release);
        }
    }

    pub(crate) fn close_event(&self) -> CloseEvent {
        CloseEvent {
            listener: (!self.is_closed()).then(|| self.on_closed.listen()),
        }
    }

    /// Attempt to pull a permit from `self.semaphore` or steal one from the parent.
    ///
    /// A successful transfer becomes child capacity until the child is dropped,
    /// including when opening a connection is cancelled or fails. A parent
    /// permit that was only queued/reserved is returned when its future drops.
    async fn acquire_permit<'a>(self: &'a Arc<Self>) -> Result<AsyncSemaphoreReleaser<'a>, Error> {
        let parent = self
            .parent()
            // If we're already at the max size, we shouldn't try to steal from the parent.
            // This is just going to cause unnecessary churn in `acquire()`.
            .filter(|_| self.size() < self.options.max_connections);

        let acquire_self = self.semaphore.acquire(1).fuse();
        let mut close_event = self.close_event();

        if let Some(parent) = parent {
            let acquire_parent = parent.0.semaphore.acquire(1).fuse();
            let parent_close_event = parent.0.close_event();

            futures_util::pin_mut!(
                acquire_parent,
                acquire_self,
                close_event,
                parent_close_event
            );

            let mut poll_parent = false;

            future::poll_fn(|cx| {
                if close_event.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(Error::PoolClosed));
                }

                if parent_close_event.as_mut().poll(cx).is_ready() {
                    // Propagate the parent's close event to the child.
                    self.mark_closed();
                    return Poll::Ready(Err(Error::PoolClosed));
                }

                if let Poll::Ready(permit) = acquire_self.as_mut().poll(cx) {
                    return Poll::Ready(Ok(permit));
                }

                // Don't try the parent right away.
                if poll_parent {
                    // Polling or dropping a semaphore acquisition can wake its
                    // waiters, so neither happens under our bookkeeping lock.
                    let mut parent_permit = match acquire_parent.as_mut().poll(cx) {
                        Poll::Ready(permit) => Some(permit),
                        Poll::Pending => None,
                    };
                    // Test-only seam (non-default `_test-pool-donation-barrier`
                    // feature; the whole statement is absent otherwise). This is
                    // the ONLY point at which an external regression test can
                    // park a donation inside the window the refusal below
                    // guards: the parent's close event has already been polled
                    // in this poll, the parent's closed state has not been read
                    // yet, and no bookkeeping lock is held -- blocking any later
                    // would deadlock against the parent's own `mark_closed`.
                    // See `crate::pool::donation_barrier` for the full argument.
                    #[cfg(feature = "_test-pool-donation-barrier")]
                    if parent_permit.is_some() {
                        crate::pool::donation_barrier::enter_donation_window();
                    }
                    // Set when the parent froze its own count underneath us, so
                    // the refusal can be reported after both locks are released.
                    let mut parent_closed = false;
                    let (transferred, at_capacity) = {
                        let mut num_permits = self
                            .num_permits
                            .lock()
                            .expect("permit lock holder panicked");
                        // Close may have raced the earlier event check. Transfer
                        // and close publication must share this critical section.
                        if self.is_closed() {
                            return Poll::Ready(Err(Error::PoolClosed));
                        }
                        let transferred = if *num_permits < self.options.max_connections {
                            if parent_permit.is_some() {
                                // Nested exactly once, descendant then ancestor
                                // (see the `num_permits` lock order). Ownership
                                // has to move in ONE critical section: the
                                // parent must stop counting this permit at the
                                // same instant we start counting it, or one of
                                // the two closes would wait for capacity the
                                // other side owns.
                                let mut parent_permits = parent
                                    .0
                                    .num_permits
                                    .lock()
                                    .expect("permit lock holder panicked");
                                if parent.0.is_closed() {
                                    // The parent already froze its owned count
                                    // and its close is waiting for exactly that
                                    // many permits. Taking one away now would
                                    // strand that close forever, so refuse the
                                    // donation; the permit is handed straight
                                    // back below.
                                    parent_closed = true;
                                    false
                                } else {
                                    // Cannot underflow: we are holding a permit
                                    // acquired from the parent's semaphore, and
                                    // every permit in that semaphore is counted
                                    // here, so the count is at least one.
                                    //
                                    // Checked, not saturating, and panicking on
                                    // purpose: this is NOT a drop path, so a
                                    // panic here cannot abort the process the
                                    // way one in `Drop` would. If a future
                                    // change ever broke the invariant, clamping
                                    // at zero would silently shrink the parent
                                    // forever and make its `close()` return
                                    // early with capacity still outstanding --
                                    // a lifecycle bug that only shows up in
                                    // release builds. Failing closed is safer.
                                    let remaining = parent_permits.checked_sub(1).expect(
                                        "BUG: transferred a permit the parent does not own",
                                    );
                                    *parent_permits = remaining;
                                    let permit = parent_permit
                                        .take()
                                        .expect("BUG: parent permit vanished under the lock");
                                    permit.disarm();
                                    *num_permits += 1;
                                    true
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        (transferred, *num_permits == self.options.max_connections)
                    };
                    // Publish an already-counted transfer without an intervening
                    // await. A racing close includes it and waits for this credit.
                    if transferred {
                        self.semaphore.release(1);
                    }
                    // Returns a merely reserved or refused permit to the parent.
                    // Dropping it can wake the parent's waiters, so it happens
                    // only after both bookkeeping locks are released.
                    drop(parent_permit);
                    if parent_closed {
                        // Same propagation the parent's close event performs,
                        // just observed synchronously under the parent's lock.
                        self.mark_closed();
                        return Poll::Ready(Err(Error::PoolClosed));
                    }
                    if at_capacity {
                        // Another borrower may have filled the child. Cancel
                        // even a reserved parent acquisition outside the lock.
                        acquire_parent.set(future::Fuse::terminated());
                    }
                    // Keep the original queued child acquisition. Never return
                    // a parent-origin guard after recording child ownership:
                    // cancellation must release capacity to the child semaphore.
                    match acquire_self.as_mut().poll(cx) {
                        Poll::Ready(permit) => Poll::Ready(Ok(permit)),
                        Poll::Pending => {
                            if transferred && !at_capacity {
                                // This transfer may have served an older child
                                // waiter. Each parent future transfers once,
                                // but this borrower must keep making progress
                                // while child capacity remains. Keep its queue
                                // position and arrange a real poll of a NEW
                                // parent future, rather than a terminated Fuse.
                                acquire_parent.set(parent.0.semaphore.acquire(1).fuse());
                                cx.waker().wake_by_ref();
                            }
                            Poll::Pending
                        }
                    }
                } else {
                    poll_parent = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        } else {
            close_event.do_until(acquire_self).await
        }
    }

    fn parent(&self) -> Option<&Pool<DB>> {
        self.options.parent_pool.as_ref()
    }

    #[inline]
    pub(super) fn try_acquire(self: &Arc<Self>) -> Option<Floating<DB, Idle<DB>>> {
        if self.is_closed() {
            return None;
        }

        let permit = self.semaphore.try_acquire(1)?;

        self.pop_idle(permit).ok()
    }

    fn pop_idle<'a>(
        self: &'a Arc<Self>,
        permit: AsyncSemaphoreReleaser<'a>,
    ) -> Result<Floating<DB, Idle<DB>>, AsyncSemaphoreReleaser<'a>> {
        if let Some(idle) = self.idle_conns.pop() {
            // Saturating: never underflow even if a concurrent `release` hasn't yet published
            // its increment. An underflow would wrap `num_idle` to `usize::MAX` and wedge the
            // maintenance task in a non-yielding spin (see `release` for the full invariant).
            let _ = self
                .num_idle
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    Some(n.saturating_sub(1))
                });
            Ok(Floating::from_idle(idle, (*self).clone(), permit))
        } else {
            Err(permit)
        }
    }

    pub(super) fn release(&self, floating: Floating<DB, Live<DB>>) {
        // `options.after_release` and other checks are in `PoolConnection::return_to_pool()`.

        let Floating { inner: idle, guard } = floating.into_idle();

        // Bump the idle counter *before* the connection becomes acquirable, so a concurrent
        // `pop_idle` can never observe a popped connection without a matching increment.
        // (Otherwise `num_idle.fetch_sub` can underflow a `usize` to `usize::MAX`, which makes
        // the maintenance task's `for _ in 0..num_idle()` loop spin ~forever, pegging a CPU.)
        // Over-counting transiently (incremented, not yet pushed) is harmless: `pop_idle`
        // simply finds an empty queue and returns the permit without decrementing.
        self.num_idle.fetch_add(1, Ordering::AcqRel);

        if self.idle_conns.push(idle).is_err() {
            panic!("BUG: connection queue overflow in release()");
        }

        // NOTE: we need to make sure we drop the permit *after* we push to the idle queue
        // don't decrease the size
        guard.release_permit();
    }

    /// Try to atomically increment the pool size for a new connection.
    ///
    /// Returns `Err` if the pool is at max capacity already or is closed.
    pub(super) fn try_increment_size<'a>(
        self: &'a Arc<Self>,
        permit: AsyncSemaphoreReleaser<'a>,
    ) -> Result<DecrementSizeGuard<DB>, AsyncSemaphoreReleaser<'a>> {
        let result = self
            .size
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                if self.is_closed() {
                    return None;
                }

                size.checked_add(1)
                    .filter(|size| size <= &self.options.max_connections)
            });

        match result {
            // we successfully incremented the size
            Ok(_) => Ok(DecrementSizeGuard::from_permit((*self).clone(), permit)),
            // the pool is at max capacity or is closed
            Err(_) => Err(permit),
        }
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> Result<Floating<DB, Live<DB>>, Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }

        let acquire_started_at = Instant::now();
        let deadline = acquire_started_at + self.options.acquire_timeout;

        let acquired = crate::rt::timeout(
            self.options.acquire_timeout,
            async {
                loop {
                    // Handles the close-event internally
                    let permit = self.acquire_permit().await?;


                    // First attempt to pop a connection from the idle queue.
                    let guard = match self.pop_idle(permit) {

                        // Then, check that we can use it...
                        Ok(conn) => match check_idle_conn(conn, &self.options).await {

                            // All good!
                            Ok(live) => return Ok(live),

                            // if the connection isn't usable for one reason or another,
                            // we get the `DecrementSizeGuard` back to open a new one
                            Err(guard) => guard,
                        },
                        Err(permit) => if let Ok(guard) = self.try_increment_size(permit) {
                            // we can open a new connection
                            guard
                        } else {
                            // This can happen for a child pool that's at its connection limit,
                            // or if the pool was closed between `acquire_permit()` and
                            // `try_increment_size()`.
                            tracing::debug!("woke but was unable to acquire idle connection or open new one; retrying");
                            // If so, we're likely in the current-thread runtime if it's Tokio,
                            // and so we should yield to let any spawned return_to_pool() tasks
                            // execute.
                            crate::rt::yield_now().await;
                            continue;
                        }
                    };

                    // Attempt to connect...
                    return self.connect(deadline, guard).await;
                }
            }
        )
            .await
            .map_err(|_| Error::PoolTimedOut)??;

        let acquired_after = acquire_started_at.elapsed();

        let acquire_slow_level = self
            .acquire_slow_level
            .filter(|_| acquired_after > self.options.acquire_slow_threshold);

        if let Some(level) = acquire_slow_level {
            private_tracing_dynamic_event!(
                target: "sqlx::pool::acquire",
                level,
                aquired_after_secs = acquired_after.as_secs_f64(),
                slow_acquire_threshold_secs = self.options.acquire_slow_threshold.as_secs_f64(),
                "acquired connection, but time to acquire exceeded slow threshold"
            );
        } else if let Some(level) = self.acquire_time_level {
            private_tracing_dynamic_event!(
                target: "sqlx::pool::acquire",
                level,
                aquired_after_secs = acquired_after.as_secs_f64(),
                "acquired connection"
            );
        }

        Ok(acquired)
    }

    pub(super) async fn connect(
        self: &Arc<Self>,
        deadline: Instant,
        guard: DecrementSizeGuard<DB>,
    ) -> Result<Floating<DB, Live<DB>>, Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }

        let mut backoff = Duration::from_millis(10);
        let max_backoff = deadline_as_timeout(deadline)? / 5;

        loop {
            let timeout = deadline_as_timeout(deadline)?;

            // clone the connect options arc so it can be used without holding the RwLockReadGuard
            // across an async await point
            let connect_options = self
                .connect_options
                .read()
                .expect("write-lock holder panicked")
                .clone();

            // result here is `Result<Result<C, Error>, TimeoutError>`
            // if this block does not return, sleep for the backoff timeout and try again
            match crate::rt::timeout(timeout, connect_options.connect()).await {
                // successfully established connection
                Ok(Ok(mut raw)) => {
                    // See comment on `PoolOptions::after_connect`
                    let meta = PoolConnectionMetadata {
                        age: Duration::ZERO,
                        idle_for: Duration::ZERO,
                    };

                    let res = if let Some(callback) = &self.options.after_connect {
                        callback(&mut raw, meta).await
                    } else {
                        Ok(())
                    };

                    match res {
                        Ok(()) => return Ok(Floating::new_live(raw, guard)),
                        Err(error) => {
                            tracing::error!(%error, "error returned from after_connect");
                            // The connection is broken, don't try to close nicely.
                            let _ = raw.close_hard().await;

                            // Fall through to the backoff.
                        }
                    }
                }

                // an IO error while connecting is assumed to be the system starting up
                Ok(Err(Error::Io(e))) if e.kind() == std::io::ErrorKind::ConnectionRefused => (),

                // We got a transient database error, retry.
                Ok(Err(Error::Database(error))) if error.is_transient_in_connect_phase() => (),

                // Any other error while connection should immediately
                // terminate and bubble the error up
                Ok(Err(e)) => return Err(e),

                // timed out
                Err(_) => return Err(Error::PoolTimedOut),
            }

            // If the connection is refused, wait in exponentially
            // increasing steps for the server to come up,
            // capped by a factor of the remaining time until the deadline
            crate::rt::sleep(backoff).await;
            backoff = cmp::min(backoff * 2, max_backoff);
        }
    }

    /// Try to maintain `min_connections`, returning any errors (including `PoolTimedOut`).
    pub async fn try_min_connections(self: &Arc<Self>, deadline: Instant) -> Result<(), Error> {
        while self.size() < self.options.min_connections {
            // Don't wait for a semaphore permit.
            //
            // If no extra permits are available then we shouldn't be trying to spin up
            // connections anyway.
            let Some(permit) = self.semaphore.try_acquire(1) else {
                return Ok(());
            };

            // We must always obey `max_connections`.
            let Some(guard) = self.try_increment_size(permit).ok() else {
                return Ok(());
            };

            // We skip `after_release` since the connection was never provided to user code
            // besides `after_connect`, if they set it.
            self.release(self.connect(deadline, guard).await?);
        }

        Ok(())
    }

    /// Attempt to maintain `min_connections`, logging if unable.
    pub async fn min_connections_maintenance(self: &Arc<Self>, deadline: Option<Instant>) {
        let deadline = deadline.unwrap_or_else(|| {
            // Arbitrary default deadline if the caller doesn't care.
            Instant::now() + Duration::from_secs(300)
        });

        match self.try_min_connections(deadline).await {
            Ok(()) => (),
            Err(Error::PoolClosed) => (),
            Err(Error::PoolTimedOut) => {
                tracing::debug!("unable to complete `min_connections` maintenance before deadline")
            }
            Err(error) => tracing::debug!(%error, "error while maintaining min_connections"),
        }
    }
}

impl<DB: Database> Drop for PoolInner<DB> {
    fn drop(&mut self) {
        self.mark_closed();

        if let Some(parent) = &self.options.parent_pool {
            // Release the stolen permits.
            //
            // Compute the amount ONCE and use the same value for the parent's
            // owned-permit count and for the semaphore release, so the two can
            // never disagree. We deliberately give back what is actually in our
            // semaphore rather than what was donated: `PoolConnection::leak()`
            // removes a permit from this pool for good, and crediting the
            // donated total would make the parent's `close()` wait forever for
            // a permit nobody can supply. Those permits are simply lost to the
            // parent, which is the pre-existing behaviour of `leak()`.
            //
            // We cannot run while one of our connections is checked out:
            // `PoolConnection` owns an `Arc<PoolInner>`, so every live guard has
            // already released its permit back into this semaphore by now.
            //
            // NO ACCOUNTING ERROR MAY PANIC HERE. `Drop` can run while the
            // thread is already unwinding, and a panic during unwinding aborts
            // the whole process -- strictly worse than any accounting error, and
            // it destroys the original panic's report as well. That rules out
            // `debug_assert!` just as much as `expect`: `debug_assert!` DOES
            // panic, in exactly the debug configuration every test run -- ours
            // and every downstream user's -- uses. So both conversions below
            // simply fall back to a DEFINED, DOCUMENTED value; neither is
            // asserted. They are unreachable anyway (`available` is at most this
            // pool's `max_connections`, which is itself a `u32`, and an
            // ancestor's count is bounded by its own `max_connections`), and both
            // clamps err in the only safe direction (see below), so an assertion
            // could only ever turn a survivable accounting fault into an abort.
            // The credit/release symmetry that actually matters is pinned by the
            // pool-close regression suite, not by an assertion.
            //
            // The only panics left in this block are the pre-existing
            // `.expect(..)` on a POISONED mutex, here and inside `mark_closed`.
            // Those do not report an accounting error: they report that another
            // holder of that lock already panicked, so the bookkeeping this
            // block would repair is no longer trustworthy at all.
            let available = self.semaphore.permits();
            // Clamping keeps the credit and the release EQUAL even in the
            // unreachable case, which is the property that actually matters:
            // the release below uses this same number, not `available`.
            let returned = u32::try_from(available).unwrap_or(u32::MAX);
            {
                // No nesting here: `mark_closed` above already released our own
                // lock, and an ancestor's lock is never held while a
                // descendant's is taken (see the `num_permits` lock order).
                let mut parent_permits = parent
                    .0
                    .num_permits
                    .lock()
                    .expect("permit lock holder panicked");
                // Safe even if the parent is already closed: its completed
                // close released the permits it was holding, and a concurrent
                // alias close either sees the old target (fewer permits than are
                // now available) or the new one (exactly what is available).
                // Either way it terminates, and the parent stays closed.
                //
                // Saturating, not checked-and-panicking: see the no-panic note
                // above. The clamp also errs in the only safe direction -- an
                // ancestor that saturated would count FEWER permits than its
                // semaphore actually holds, so its `close()` waits for fewer
                // than exist and still terminates. (Under-counting can never
                // strand a close; over-counting is what deadlocks it.)
                let credited = parent_permits.saturating_add(returned);
                *parent_permits = credited;
            }
            // Wakeups stay outside the bookkeeping lock.
            parent.0.semaphore.release(returned as usize);
        }
    }
}

/// Returns `true` if the connection has exceeded `options.max_lifetime` if set, `false` otherwise.
pub(super) fn is_beyond_max_lifetime<DB: Database>(
    live: &Live<DB>,
    options: &PoolOptions<DB>,
) -> bool {
    options
        .max_lifetime
        .map_or(false, |max| live.created_at.elapsed() > max)
}

/// Returns `true` if the connection has exceeded `options.idle_timeout` if set, `false` otherwise.
fn is_beyond_idle_timeout<DB: Database>(idle: &Idle<DB>, options: &PoolOptions<DB>) -> bool {
    options
        .idle_timeout
        .map_or(false, |timeout| idle.idle_since.elapsed() > timeout)
}

async fn check_idle_conn<DB: Database>(
    mut conn: Floating<DB, Idle<DB>>,
    options: &PoolOptions<DB>,
) -> Result<Floating<DB, Live<DB>>, DecrementSizeGuard<DB>> {
    if options.test_before_acquire {
        // Check that the connection is still live
        if let Err(error) = conn.ping().await {
            // an error here means the other end has hung up or we lost connectivity
            // either way we're fine to just discard the connection
            // the error itself here isn't necessarily unexpected so WARN is too strong
            tracing::info!(%error, "ping on idle connection returned error");
            // connection is broken so don't try to close nicely
            return Err(conn.close_hard().await);
        }
    }

    if let Some(test) = &options.before_acquire {
        let meta = conn.metadata();
        match test(&mut conn.live.raw, meta).await {
            Ok(false) => {
                // connection was rejected by user-defined hook, close nicely
                return Err(conn.close().await);
            }

            Err(error) => {
                tracing::warn!(%error, "error from `before_acquire`");
                // connection is broken so don't try to close nicely
                return Err(conn.close_hard().await);
            }

            Ok(true) => {}
        }
    }

    // No need to re-connect; connection is alive or we don't care
    Ok(conn.into_live())
}

fn spawn_maintenance_tasks<DB: Database>(pool: &Arc<PoolInner<DB>>) {
    // NOTE: use `pool_weak` for the maintenance tasks
    // so they don't keep `PoolInner` from being dropped.
    let pool_weak = Arc::downgrade(pool);

    let period = match (pool.options.max_lifetime, pool.options.idle_timeout) {
        (Some(it), None) | (None, Some(it)) => it,

        (Some(a), Some(b)) => cmp::min(a, b),

        (None, None) => {
            if pool.options.min_connections > 0 {
                crate::rt::spawn(async move {
                    if let Some(pool) = pool_weak.upgrade() {
                        pool.min_connections_maintenance(None).await;
                    }
                });
            }

            return;
        }
    };

    // Immediately cancel this task if the pool is closed.
    let mut close_event = pool.close_event();

    crate::rt::spawn(async move {
        let _ = close_event
            .do_until(async {
                // If the last handle to the pool was dropped while we were sleeping
                while let Some(pool) = pool_weak.upgrade() {
                    if pool.is_closed() {
                        return;
                    }

                    let next_run = Instant::now() + period;

                    // Go over all idle connections, check for idleness and lifetime,
                    // and if we have fewer than min_connections after reaping a connection,
                    // open a new one immediately. Note that other connections may be popped from
                    // the queue in the meantime - that's fine, there is no harm in checking more.
                    //
                    // Cap the iteration count at `max_connections` so that even a corrupt
                    // `num_idle` (e.g. an underflow to `usize::MAX`) can never make this
                    // synchronous, non-yielding loop spin unboundedly and starve the runtime.
                    let checks = cmp::min(pool.num_idle(), pool.options.max_connections as usize);
                    for _ in 0..checks {
                        if let Some(conn) = pool.try_acquire() {
                            if is_beyond_idle_timeout(&conn, &pool.options)
                                || is_beyond_max_lifetime(&conn, &pool.options)
                            {
                                let _ = conn.close().await;
                                pool.min_connections_maintenance(Some(next_run)).await;
                            } else {
                                pool.release(conn.into_live());
                            }
                        }
                    }

                    // Don't hold a reference to the pool while sleeping.
                    drop(pool);

                    if let Some(duration) = next_run.checked_duration_since(Instant::now()) {
                        // `async-std` doesn't have a `sleep_until()`
                        crate::rt::sleep(duration).await;
                    } else {
                        // `next_run` is in the past, just yield.
                        crate::rt::yield_now().await;
                    }
                }
            })
            .await;
    });
}

/// RAII guard returned by `Pool::try_increment_size()` and others.
///
/// Will decrement the pool size if dropped, to avoid semantically "leaking" connections
/// (where the pool thinks it has more connections than it does).
pub(in crate::pool) struct DecrementSizeGuard<DB: Database> {
    pub(crate) pool: Arc<PoolInner<DB>>,
    cancelled: bool,
}

impl<DB: Database> DecrementSizeGuard<DB> {
    /// Create a new guard that will release a semaphore permit on-drop.
    pub fn new_permit(pool: Arc<PoolInner<DB>>) -> Self {
        Self {
            pool,
            cancelled: false,
        }
    }

    pub fn from_permit(pool: Arc<PoolInner<DB>>, permit: AsyncSemaphoreReleaser<'_>) -> Self {
        // here we effectively take ownership of the permit
        permit.disarm();
        Self::new_permit(pool)
    }

    /// Release the semaphore permit without decreasing the pool size.
    ///
    /// If the permit was stolen from the pool's parent, it will be returned to the child's semaphore.
    fn release_permit(self) {
        self.pool.semaphore.release(1);
        self.cancel();
    }

    pub fn cancel(mut self) {
        self.cancelled = true;
    }
}

impl<DB: Database> Drop for DecrementSizeGuard<DB> {
    fn drop(&mut self) {
        if !self.cancelled {
            self.pool.size.fetch_sub(1, Ordering::AcqRel);

            // and here we release the permit we got on construction
            self.pool.semaphore.release(1);
        }
    }
}
