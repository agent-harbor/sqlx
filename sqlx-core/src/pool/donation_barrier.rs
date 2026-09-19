//! Test-only synchronisation seam for the parent-to-child permit donation.
//!
//! WHY THIS EXISTS
//! ===============
//! `PoolInner::acquire_permit` refuses to donate a parent permit to a child once
//! the parent has frozen its owned-permit count by closing (the
//! `parent.0.is_closed()` check taken under the parent's `num_permits` lock).
//! That refusal is load-bearing: `PoolInner::close()` calls `mark_closed()`
//! *eagerly*, when the future is created, and only afterwards re-takes the lock
//! to read `permits_to_acquire`. A donation that lands in that window without
//! the refusal debits a count the parent's `close()` may already have read, and
//! the parent then waits forever for a permit the child now owns.
//!
//! The only other guard is `parent_close_event` at the top of the same
//! `poll_fn`. It covers "already closed when `acquire_permit` started" (the
//! `CloseEvent` listener is `None`, so the poll is immediately `Ready`) and
//! "closed later, task woken" (the listener has been notified, so the next poll
//! is `Ready`). What it cannot cover is a store that lands *after* it was polled
//! within the current poll. There is no await point in that window, so in any
//! deterministic single-threaded schedule the event guard always fires first and
//! the refusal branch is unreachable — which is exactly why no ordinary
//! regression test can observe the refusal's absence.
//!
//! This module makes the window externally observable so a real regression test
//! can park a donation inside it, close the parent from another thread, and then
//! let the donation proceed. It is compiled ONLY under the non-default
//! `_test-pool-donation-barrier` feature: with the feature off neither this
//! module nor its call site in `inner.rs` exists, so there is no runtime cost,
//! no static, and nothing added to the public API surface.
//!
//! It is deliberately NOT a general-purpose hook: the single call site sits
//! between the parent-permit acquisition and the first bookkeeping lock, so an
//! installed barrier can block a thread without holding any pool lock. Blocking
//! any later (for example under the parent's `num_permits` lock) would deadlock
//! against `mark_closed`, which needs that very lock.

use std::sync::{Arc, RwLock};

/// A callback invoked on the donating task's thread, with no pool lock held,
/// immediately after a parent permit has been acquired and before the parent's
/// closed state is read. It may block.
pub type DonationBarrier = Arc<dyn Fn() + Send + Sync + 'static>;

static BARRIER: RwLock<Option<DonationBarrier>> = RwLock::new(None);

/// Install (`Some`) or remove (`None`) the process-wide donation barrier.
///
/// Tests must remove the barrier before finishing, ideally from a drop guard, so
/// a failing assertion cannot leave a later donation parked.
pub fn set_donation_barrier(barrier: Option<DonationBarrier>) {
    let mut slot = BARRIER
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *slot = barrier;
}

/// Invoked from `PoolInner::acquire_permit`. The guard is released before the
/// callback runs, so an installed barrier may block for as long as it likes
/// without wedging `set_donation_barrier` or another donating task.
pub(crate) fn enter_donation_window() {
    let barrier = BARRIER
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    if let Some(barrier) = barrier {
        barrier();
    }
}
