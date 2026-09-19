//! Regression controls for https://github.com/transact-rs/sqlx/issues/3217.
//! These use actual SQLite workers, never simulated counters or filesystem locks.

use anyhow::{ensure, Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
// Needed to close a connection that was permanently removed from a pool with
// `PoolConnection::leak`, which no longer has the pool's inherent `close`.
use sqlx::Connection as _;
use sqlx::SqlitePool;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const COMPLETION_BOUND: Duration = Duration::from_secs(30);
const HELD_OBSERVATION: Duration = Duration::from_secs(1);

struct Evidence {
    log: File,
    log_path: PathBuf,
    fixture: tempfile::TempDir,
}

impl Evidence {
    fn new(name: &str) -> Result<Self> {
        let directory = std::env::var_os("SQLX_POOL_CLOSE_TEST_LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-logs/pool-close")
            });
        fs::create_dir_all(&directory)?;
        let (log, log_path) = tempfile::Builder::new()
            .prefix(name)
            .suffix(".log")
            .tempfile_in(&directory)?
            .keep()?;
        let fixture = tempfile::Builder::new()
            .prefix(name)
            .tempdir_in(&directory)?;
        let mut evidence = Self {
            log,
            log_path,
            fixture,
        };
        writeln!(
            evidence.log,
            "test={name} root={}",
            evidence.fixture.path().display()
        )?;
        Ok(evidence)
    }

    fn options(&self) -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .filename(self.fixture.path().join("state.sqlite"))
            .create_if_missing(true)
    }

    fn state(&mut self, label: &str, pool: &SqlitePool) -> Result<()> {
        writeln!(
            self.log,
            "{label}: closed={} size={} idle={}",
            pool.is_closed(),
            pool.size(),
            pool.num_idle()
        )?;
        Ok(())
    }

    fn drained(&mut self, pool: &SqlitePool) -> Result<()> {
        self.state("after_close", pool)?;
        ensure!(pool.is_closed(), "close did not mark pool closed");
        ensure!(pool.size() == 0, "close returned with live connections");
        ensure!(
            pool.num_idle() == 0,
            "close retained idle connections/counter"
        );
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // Unlike unlink on Unix, this proves the SQLite worker released the
            // actual file before the fixture or pool owner gets dropped.
            let database = self.fixture.path().join("state.sqlite");
            if database.exists() {
                let opened = fs::OpenOptions::new()
                    .read(true)
                    .share_mode(0)
                    .open(&database);
                writeln!(self.log, "native_exclusive_open={opened:?}")?;
                opened.context("closed pool still owns its SQLite file")?;
            }
        }
        Ok(())
    }

    fn finish(mut self, result: Result<()>) -> Result<()> {
        let root = self.fixture.path().to_path_buf();
        let cleaned = self.fixture.close();
        let final_result = match result {
            Err(error) => Err(error).context(format!("checked cleanup after failure={cleaned:?}")),
            Ok(()) => cleaned
                .context("checked owned fixture cleanup failed")
                .and_then(|()| {
                    ensure!(!root.exists(), "checked cleanup left its exact root");
                    Ok(())
                }),
        };
        let written = writeln!(
            self.log,
            "checked_cleanup_root_absent={} result={final_result:#?}",
            !root.exists()
        )
        .and_then(|()| self.log.flush());
        let bytes = fs::metadata(&self.log_path).map(|meta| meta.len());
        let context = format!(
            "full log: {} (size={bytes:?} bytes)",
            self.log_path.display()
        );
        match final_result {
            Err(error) => Err(error).context(format!("{context}; final write={written:?}")),
            Ok(()) => written.context(context),
        }
    }
}

fn pool_options(maximum: u32) -> SqlitePoolOptions {
    // No maintenance task may create/remove a peer behind the test's back.
    SqlitePoolOptions::new()
        .max_connections(maximum)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
}

async fn close(pool: &SqlitePool) -> Result<()> {
    tokio::time::timeout(COMPLETION_BOUND, pool.close())
        .await
        .context("pool close did not complete within the unchanged lifecycle bound")
}

async fn assert_exact_parent_capacity(
    evidence: &mut Evidence,
    parent: &SqlitePool,
    maximum: u32,
) -> Result<()> {
    let mut held = Vec::new();
    for _ in 0..maximum {
        held.push(parent.acquire().await?);
    }
    ensure!(
        parent.size() == maximum,
        "child drop did not return the exact stolen permit"
    );
    ensure!(
        parent.try_acquire().is_none(),
        "child close over-credited parent capacity"
    );
    let sibling = pool_options(1)
        .parent(parent.clone())
        .connect_lazy_with(evidence.options());
    let stolen = tokio::time::timeout(HELD_OBSERVATION, sibling.acquire()).await;
    let incorrectly_acquired = match stolen {
        Ok(Ok(connection)) => {
            connection.close().await?;
            true
        }
        Ok(Err(error)) => {
            return Err(error)
                .context("sibling negative control failed to acquire for an unrelated reason");
        }
        Err(_) => false,
    };
    writeln!(
        evidence.log,
        "parent_capacity={maximum} sibling_acquired_while_all_parent_slots_held={incorrectly_acquired}"
    )?;
    for connection in held {
        connection.close().await?;
    }
    close(&sibling).await?;
    drop(sibling);
    ensure!(
        !incorrectly_acquired,
        "child close manufactured a permit that a fresh sibling could steal"
    );
    Ok(())
}

// Direct handle registration transfers this closure to sqlite3_create_collation_v2.
// SQLite invokes xDestroy while closing the real worker connection:
// https://www.sqlite.org/c3ref/create_collation.html
// Never panic across that C callback; a bounded wait and RAII release also make
// every failure path release the worker without manufacturing a successful close.
#[derive(Default)]
struct WorkerShutdownState {
    destruction_thread: std::sync::Mutex<Option<std::thread::ThreadId>>,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    entered: Notify,
    finished: std::sync::atomic::AtomicBool,
    timed_out: std::sync::atomic::AtomicBool,
}

impl WorkerShutdownState {
    fn release(&self) {
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *released = true;
        self.changed.notify_all();
    }
}

struct ReleaseWorkerOnDrop(Arc<WorkerShutdownState>);

impl Drop for ReleaseWorkerOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct WorkerShutdownBarrier(Arc<WorkerShutdownState>);

impl Drop for WorkerShutdownBarrier {
    fn drop(&mut self) {
        let released = self
            .0
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *self
            .0
            .destruction_thread
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(std::thread::current().id());
        self.0.entered.notify_one();
        let (released, _) = self
            .0
            .changed
            .wait_timeout_while(released, COMPLETION_BOUND, |released| !*released)
            .unwrap_or_else(|error| error.into_inner());
        self.0
            .timed_out
            .store(!*released, std::sync::atomic::Ordering::Release);
        self.0
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

struct ClosePoolOnThread {
    thread: Option<std::thread::JoinHandle<()>>,
    result: tokio::sync::oneshot::Receiver<Result<()>>,
    first_poll: tokio::sync::oneshot::Receiver<bool>,
    completed: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<WorkerShutdownState>,
}

impl ClosePoolOnThread {
    fn start(pool: SqlitePool, release: Arc<WorkerShutdownState>) -> Result<Self> {
        let (result_tx, result) = tokio::sync::oneshot::channel();
        let (first_tx, first_poll) = tokio::sync::oneshot::channel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = completed.clone();
        let thread = std::thread::Builder::new()
            .name("sqlx-close-regression-owner".into())
            .spawn(move || {
                let outcome = (|| -> Result<()> {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    let mut closing = Box::pin(pool.close());
                    let mut first_tx = Some(first_tx);
                    runtime.block_on(async {
                        tokio::time::timeout(
                            COMPLETION_BOUND,
                            std::future::poll_fn(|cx| {
                                let result = std::future::Future::poll(closing.as_mut(), cx);
                                if let Some(tx) = first_tx.take() {
                                    let _ = tx.send(result.is_pending());
                                }
                                result
                            }),
                        )
                        .await
                        .context("owned close thread exceeded the unchanged completion bound")
                    })
                })();
                finished.store(true, std::sync::atomic::Ordering::Release);
                let _ = result_tx.send(outcome);
            })?;
        Ok(Self {
            thread: Some(thread),
            result,
            first_poll,
            completed,
            release,
        })
    }

    fn id(&self) -> Result<std::thread::ThreadId> {
        Ok(self
            .thread
            .as_ref()
            .context("close thread already joined")?
            .thread()
            .id())
    }

    async fn first_poll_pending(&mut self) -> Result<bool> {
        tokio::time::timeout(COMPLETION_BOUND, &mut self.first_poll)
            .await
            .context("owned close thread did not report its first poll")?
            .context("owned close thread ended before its first poll")
    }

    async fn finish(&mut self) -> Result<()> {
        let result = tokio::time::timeout(COMPLETION_BOUND, &mut self.result)
            .await
            .context("owned close result exceeded the unchanged completion bound")?
            .context("owned close thread ended without a result")?;
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("owned close thread panicked"))?;
        }
        result
    }
}

impl Drop for ClosePoolOnThread {
    fn drop(&mut self) {
        // Release first, including on any earlier assertion error. The close
        // runtime and native callback both retain the existing finite bounds.
        self.release.release();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn assert_worker_shutdown_is_serialized(
    evidence: &mut Evidence,
    pool: &SqlitePool,
    retain_spare: bool,
) -> Result<()> {
    let state = Arc::new(WorkerShutdownState::default());
    let release_on_drop = ReleaseWorkerOnDrop(state.clone());
    let barrier = WorkerShutdownBarrier(state.clone());
    let mut held = pool.acquire().await?;
    if retain_spare {
        let spare = pool.acquire().await?;
        ensure!(
            pool.size() == 2 && pool.num_idle() == 0,
            "two actual child checkouts required"
        );
        spare.close().await?;
        ensure!(
            pool.size() == 1 && pool.num_idle() == 0,
            "spare worker did not close"
        );
        evidence.state("retained_child_spare_before_worker_barrier", pool)?;
    }
    {
        let mut handle = held.lock_handle().await?;
        handle.create_collation("close_barrier", move |left, right| {
            // Keep the complete guard alive until SQLite destroys the
            // registered callback, not merely until this comparison ends.
            let _keep_until_worker_close = &barrier;
            left.cmp(right)
        })?;
    }
    let compared: i64 = sqlx::query_scalar("SELECT 'a' < 'b' COLLATE close_barrier")
        .fetch_one(&mut *held)
        .await?;
    ensure!(
        compared == 1,
        "actual worker collation positive control failed"
    );
    // This is the normal return operation itself, awaited so the idle
    // fixture is established without a scheduler-timing assumption.
    held.return_to_pool().await;
    drop(held);
    ensure!(
        pool.size() == 1 && pool.num_idle() == 1,
        "worker must start idle"
    );
    evidence.state("before_worker_shutdown", pool)?;

    // Native SQLite destruction may execute on this close caller when its
    // shared Arc is the final owner. A dedicated thread/runtime keeps even a
    // legally blocking Drop from stalling the observer that releases xDestroy.
    let mut first = ClosePoolOnThread::start(pool.clone(), state.clone())?;
    tokio::time::timeout(COMPLETION_BOUND, state.entered.notified())
        .await
        .context("native destruction did not enter SQLite xDestroy callback")?;
    let destruction_thread = *state
        .destruction_thread
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let synchronous = destruction_thread == Some(first.id()?);
    // If destruction runs on the close thread, its first poll is deliberately
    // blocked inside Drop. Otherwise require a receipt from the completed poll:
    // mere task scheduling delay must not masquerade as awaiting worker ACK.
    let first_pending = if synchronous {
        None
    } else {
        Some(first.first_poll_pending().await?)
    };
    let first_completed = first.completed.load(std::sync::atomic::Ordering::Acquire);
    let barrier_active = !state.finished.load(std::sync::atomic::Ordering::Acquire);
    writeln!(evidence.log,
        "close_thread={:?} destruction_thread={destruction_thread:?} synchronous_destruction={synchronous} first_poll_pending={first_pending:?} close_completed_before_release={first_completed} barrier_active={barrier_active}",
        first.id()?)?;

    // The first close removed the idle worker, but actual native destruction
    // is positively blocked. An alias must still await the permit owner.
    let mut alias = Box::pin(pool.close());
    let alias_pending = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(alias.as_mut(), cx).is_pending())
    })
    .await;
    writeln!(
        evidence.log,
        "alias_pending_during_actual_worker_shutdown={alias_pending}"
    )?;
    let verdict = (|| -> Result<()> {
        ensure!(
            barrier_active,
            "native destruction barrier expired before deliberate release"
        );
        ensure!(
            !first_completed && (synchronous || first_pending == Some(true)),
            "close returned without awaiting real worker shutdown"
        );
        ensure!(
            alias_pending,
            "alias close returned while first close still owned a live worker"
        );
        Ok(())
    })();
    release_on_drop.0.release();
    let cleanup = async {
        first.finish().await?;
        if alias_pending {
            tokio::time::timeout(COMPLETION_BOUND, &mut alias)
                .await
                .context("alias close did not finish after exact worker release")?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    drop((first, alias));
    if let Err(error) = verdict {
        return Err(error).context(format!(
            "owned close cleanup after saved failure={cleanup:?}"
        ));
    }
    cleanup?;
    ensure!(
        state.finished.load(std::sync::atomic::Ordering::Acquire)
            && !state.timed_out.load(std::sync::atomic::Ordering::Acquire),
        "real worker shutdown did not finish through explicit barrier release"
    );
    evidence.drained(pool)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn idle_worker_shutdown_is_awaited_and_serializes_alias_closes() -> Result<()> {
    let mut evidence = Evidence::new("worker-shutdown-")?;
    let result = async {
        // One idle worker plus one unused parent permit: using size instead of
        // maximum would let the alias consume the spare permit prematurely.
        let pool = pool_options(2).connect_with(evidence.options()).await?;
        assert_worker_shutdown_is_serialized(&mut evidence, &pool, false).await
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn sparse_child_spare_permits_serialize_real_worker_shutdown() -> Result<()> {
    let mut evidence = Evidence::new("sparse-child-worker-")?;
    let result = async {
        let parent = pool_options(4).connect_lazy_with(evidence.options());
        // Child max3 owns only two permits: one idle worker and one retained
        // spare. Acquiring size1 is too few; acquiring configured max3 is too many.
        let child = pool_options(3)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        assert_worker_shutdown_is_serialized(&mut evidence, &child, true).await?;
        drop(child);
        assert_exact_parent_capacity(&mut evidence, &parent, 4).await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn queued_child_borrows_respect_capacity_and_release_parent_reservations() -> Result<()> {
    let mut evidence = Evidence::new("queued-child-borrows-")?;
    let result = async {
        let parent = pool_options(4).connect_lazy_with(evidence.options());
        let mut parent_held = Vec::new();
        for _ in 0..4 {
            parent_held.push(parent.acquire().await?);
        }
        ensure!(
            parent.size() == 4 && parent.num_idle() == 0,
            "parent capacity must be held"
        );
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut queued = vec![
            Box::pin(child.acquire()),
            Box::pin(child.acquire()),
            Box::pin(child.acquire()),
        ];
        // The first poll defers borrowing; the second actually queues every
        // parent acquisition while the child is still empty and the parent full.
        for _ in 0..2 {
            for acquiring in &mut queued {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(
                        std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                    )
                })
                .await;
                ensure!(
                    pending,
                    "borrower acquired before parent capacity was released"
                );
            }
        }
        evidence.state("three_borrows_queued_with_empty_child", &child)?;
        for connection in parent_held {
            connection.close().await?;
        }
        let held = tokio::time::timeout(COMPLETION_BOUND, queued.remove(0)).await??;
        ensure!(
            child.size() == 1 && child.num_idle() == 0,
            "child max1 checkout missing"
        );
        for acquiring in &mut queued {
            let pending = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(
                    std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                )
            })
            .await;
            ensure!(
                pending,
                "queued borrower exceeded child connection capacity"
            );
        }
        // The other two parent futures had permits reserved, but lost the
        // child-capacity race. A real sibling must be able to use ALL remaining
        // three parent slots while those acquisition futures are still alive.
        let sibling = pool_options(3)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut sibling_held = Vec::new();
        for _ in 0..3 {
            sibling_held.push(tokio::time::timeout(COMPLETION_BOUND, sibling.acquire()).await??);
        }
        ensure!(
            sibling.size() == 3 && sibling.num_idle() == 0,
            "queued parent reservations were stranded"
        );
        writeln!(
            evidence.log,
            "child_held=1 sibling_held=3 queued_acquires=2 parent_capacity=4"
        )?;
        let mut closing = Box::pin(child.close());
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        })
        .await;
        ensure!(pending, "close missed the actual child checkout");
        // One queued acquisition is cancelled, and one observes PoolClosed.
        drop(queued.pop());
        let rejected = tokio::time::timeout(COMPLETION_BOUND, queued.remove(0)).await?;
        ensure!(
            matches!(rejected, Err(sqlx::Error::PoolClosed)),
            "queued child acquisition ignored close"
        );
        held.close().await?;
        tokio::time::timeout(COMPLETION_BOUND, &mut closing).await?;
        drop(closing);
        // Sibling handles still own the same database, so assert portable child
        // drain now and the native exclusive open after those exact owners end.
        ensure!(
            child.is_closed() && child.size() == 0 && child.num_idle() == 0,
            "child retained connections"
        );
        drop(child);
        for connection in sibling_held {
            connection.close().await?;
        }
        close(&sibling).await?;
        evidence.drained(&sibling)?;
        drop(sibling);
        assert_exact_parent_capacity(&mut evidence, &parent, 4).await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn cancelled_borrower_preserves_transferred_child_permit_until_waiter_release() -> Result<()>
{
    let mut evidence = Evidence::new("cancel-transferred-borrow-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let parent_first = parent.acquire().await?;
        let parent_second = parent.acquire().await?;
        ensure!(
            parent.size() == 2 && parent.num_idle() == 0,
            "parent must be fully held"
        );
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut older = Box::pin(child.acquire());
        let mut newer = Box::pin(child.acquire());
        for _ in 0..2 {
            for acquiring in [&mut older, &mut newer] {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(
                        std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                    )
                })
                .await;
                ensure!(pending, "child acquired before parent release");
            }
        }
        parent_first.close().await?;
        parent_second.close().await?;
        // Poll the newer borrower first: its successful transfer credits the
        // older queued CHILD acquisition, while it still awaits child capacity.
        let newer_pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(newer.as_mut(), cx).is_pending())
        })
        .await;
        ensure!(
            newer_pending && child.size() == 0 && child.num_idle() == 0,
            "newer borrower must transfer without opening a connection"
        );
        drop(newer);
        evidence.state("newer_cancelled_older_owns_transferred_reservation", &child)?;
        let mut closing = Box::pin(child.close());
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        })
        .await;
        writeln!(
            evidence.log,
            "close_pending_for_size_zero_transferred_reservation={pending}"
        )?;
        // Cancel precisely the older child reservation (and its unused parent
        // reservation), rather than relying on a worker or a timeout to release it.
        drop(older);
        let finished = if pending {
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_ready())
            })
            .await
        } else {
            false
        };
        drop(closing);
        ensure!(
            pending,
            "close missed transferred capacity after its borrower was cancelled"
        );
        ensure!(
            finished,
            "cancelled child reservation did not return the owned permit"
        );
        evidence.drained(&child)?;
        drop(child);
        assert_exact_parent_capacity(&mut evidence, &parent, 2).await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn queued_borrower_can_reborrow_after_transfer_to_older_waiter() -> Result<()> {
    let mut evidence = Evidence::new("live-transferred-borrow-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let parent_first = parent.acquire().await?;
        let parent_second = parent.acquire().await?;
        let child = pool_options(2)
            .acquire_timeout(HELD_OBSERVATION)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut older = Box::pin(child.acquire());
        let mut newer = Box::pin(child.acquire());
        for _ in 0..2 {
            for acquiring in [&mut older, &mut newer] {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(
                        std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                    )
                })
                .await;
                ensure!(pending, "borrower acquired while every parent slot was held");
            }
        }
        parent_first.close().await?;
        parent_second.close().await?;
        // Let B transfer first. The corrected ownership algorithm gives that
        // token to older queued CHILD waiter A; B must remain able to borrow
        // again while child capacity is still available. The old algorithm
        // instead gives B its own parent token; both orderings must progress.
        let mut newer_result = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match std::future::Future::poll(newer.as_mut(), cx) {
                std::task::Poll::Ready(result) => Some(result),
                std::task::Poll::Pending => None,
            })
        })
        .await;
        let held_older = tokio::time::timeout(COMPLETION_BOUND, &mut older).await??;
        drop(older);
        evidence.state("older_held_after_newer_first_transfer", &child)?;
        // A valid implementation may already reserve B's next parent token.
        // Require simultaneous child checkouts, not an intermediate spare
        // parent checkout that would constrain that legal scheduling choice.
        if newer_result.is_none() {
            newer_result = Some(
                tokio::time::timeout(COMPLETION_BOUND, &mut newer)
                    .await
                    .context("outer completion bound, not a semantic verdict")?,
            );
        }
        drop(newer);
        let acquired = match newer_result.context("newer acquisition outcome missing")? {
            Ok(connection) => {
                ensure!(
                    child.size() == 2 && child.num_idle() == 0,
                    "both real child checkouts must be simultaneously held"
                );
                writeln!(evidence.log, "newer_acquired_while_older_still_held=true")?;
                connection.close().await?;
                true
            }
            Err(error) => {
                writeln!(evidence.log, "newer_acquired_while_older_still_held=false error={error:?}")?;
                false
            }
        };
        held_older.close().await?;
        close(&child).await?;
        evidence.drained(&child)?;
        drop(child);
        close(&parent).await?;
        evidence.drained(&parent)?;
        ensure!(
            acquired,
            "live borrower exhausted its configured acquisition deadline despite unused child/parent capacity"
        );
        Ok(())
    }
    .await;
    evidence.finish(result)
}

// Keep the donor on its original child queue entry. The manual schedule proves
// more than one donation; the spawned schedule additionally requires the
// product to arrange the donor's next poll, not a test-side manual re-poll.
async fn live_borrower_progress(maximum: u32, scheduled: bool, name: &str) -> Result<()> {
    let mut evidence = Evidence::new(name)?;
    let result = async {
        let parent = pool_options(maximum).connect_lazy_with(evidence.options());
        let mut parent_held = Vec::new();
        for _ in 0..maximum {
            parent_held.push(parent.acquire().await?);
        }
        let child = pool_options(maximum)
            .acquire_timeout(HELD_OBSERVATION)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut older = (1..maximum)
            .map(|_| Box::pin(child.acquire()))
            .collect::<Vec<_>>();
        let mut donor = Box::pin(child.acquire());
        for _ in 0..2 {
            for acquiring in older.iter_mut().chain(std::iter::once(&mut donor)) {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(
                        std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                    )
                })
                .await;
                ensure!(
                    pending,
                    "borrower acquired while all real parent slots were held"
                );
            }
        }
        for connection in parent_held {
            connection.close().await?;
        }

        let mut held = Vec::new();
        let donor_result = if scheduled {
            let first_poll = Arc::new(Notify::new());
            let polled = first_poll.clone();
            let mut initial = true;
            let mut task = tokio::spawn(std::future::poll_fn(move |cx| {
                let result = std::future::Future::poll(donor.as_mut(), cx);
                if initial {
                    initial = false;
                    // This wakes ONLY the test coordinator. The donor must
                    // receive its own product/semaphore wakeup to run again.
                    polled.notify_one();
                }
                result
            }));
            let _abort_on_exit = AbortTaskOnDrop(task.abort_handle());
            tokio::time::timeout(COMPLETION_BOUND, first_poll.notified()).await?;
            for waiter in older {
                held.push(tokio::time::timeout(COMPLETION_BOUND, waiter).await??);
            }
            tokio::time::timeout(COMPLETION_BOUND, &mut task)
                .await
                .context("outer scheduled completion bound, not a semantic verdict")??
        } else {
            let mut early = None;
            for waiter in older {
                if early.is_none() {
                    early = std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(
                            match std::future::Future::poll(donor.as_mut(), cx) {
                                std::task::Poll::Ready(result) => Some(result),
                                std::task::Poll::Pending => None,
                            },
                        )
                    })
                    .await;
                }
                // With cap3, C donates first to A and then to B. C still
                // needs another parent future for its OWN real checkout.
                held.push(tokio::time::timeout(COMPLETION_BOUND, waiter).await??);
            }
            match early {
                Some(result) => result,
                None => tokio::time::timeout(COMPLETION_BOUND, &mut donor)
                    .await
                    .context("outer repeated completion bound, not a semantic verdict")?,
            }
        };
        let acquired = match donor_result {
            Ok(connection) => {
                held.push(connection);
                ensure!(
                    held.len() == maximum as usize
                        && child.size() == maximum
                        && child.num_idle() == 0,
                    "all actual child checkouts must be simultaneously held"
                );
                writeln!(
                    evidence.log,
                    "donor_progress=true scheduled={scheduled} simultaneous_held={maximum}"
                )?;
                true
            }
            Err(error) => {
                writeln!(
                    evidence.log,
                    "donor_progress=false scheduled={scheduled} error={error:?}"
                )?;
                false
            }
        };
        for connection in held {
            connection.close().await?;
        }
        close(&child).await?;
        evidence.drained(&child)?;
        drop(child);
        assert_exact_parent_capacity(&mut evidence, &parent, maximum).await?;
        close(&parent).await?;
        evidence.drained(&parent)?;
        ensure!(
            acquired,
            "live donor exhausted its SQLx deadline despite remaining capacity"
        );
        Ok(())
    }
    .await;
    evidence.finish(result)
}

struct AbortTaskOnDrop(tokio::task::AbortHandle);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn queued_borrower_can_repeat_donations_up_to_child_capacity() -> Result<()> {
    live_borrower_progress(3, false, "repeated-live-donation-").await
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn scheduled_borrower_wakes_after_donation_to_older_waiter() -> Result<()> {
    live_borrower_progress(2, true, "scheduled-live-donation-").await
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn capped_child_releases_still_pending_parent_borrow_before_later_credit() -> Result<()> {
    let mut evidence = Evidence::new("pending-parent-release-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let first_parent = parent.acquire().await?;
        let second_parent = parent.acquire().await?;
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let mut winner = Box::pin(child.acquire());
        let mut waiting = Box::pin(child.acquire());
        for _ in 0..2 {
            for acquiring in [&mut winner, &mut waiting] {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(
                        std::future::Future::poll(acquiring.as_mut(), cx).is_pending(),
                    )
                })
                .await;
                ensure!(
                    pending,
                    "parent slots must be held while child borrowers queue"
                );
            }
        }
        first_parent.close().await?;
        let held = tokio::time::timeout(COMPLETION_BOUND, &mut winner).await??;
        drop(winner);
        ensure!(
            child.size() == 1 && child.num_idle() == 0,
            "actual capped child checkout missing"
        );

        // The loser is polled at the child cap while the remaining parent token
        // is STILL held. Its parent future is pending, not already completed or
        // reserved. It must cancel that queue entry before a later parent credit.
        let waiting_pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(waiting.as_mut(), cx).is_pending())
        })
        .await;
        ensure!(
            waiting_pending,
            "second child acquired beyond its live capacity"
        );
        second_parent.close().await?;
        let sibling = pool_options(1)
            .acquire_timeout(HELD_OBSERVATION)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        // Do not poll or cancel the losing child borrower to rescue capacity.
        // SQLx's public deadline is distinct from the unchanged outer watchdog.
        let (sibling_held, verdict) =
            match tokio::time::timeout(COMPLETION_BOUND, sibling.acquire()).await {
                Ok(Ok(connection)) => (Some(connection), Ok(())),
                Ok(Err(error)) => (
                    None,
                    Err(error).context("pending parent borrow stranded the later credit"),
                ),
                Err(error) => (
                    None,
                    Err(error).context("outer completion bound, not a semantic verdict"),
                ),
            };
        writeln!(
            evidence.log,
            "sibling_acquired_while_capped_child_and_loser_live={} verdict={verdict:?}",
            sibling_held.is_some()
        )?;
        let cleanup = async {
            if let Some(connection) = sibling_held {
                ensure!(
                    child.size() == 1
                        && sibling.size() == 1
                        && child.num_idle() == 0
                        && sibling.num_idle() == 0,
                    "both actual child and sibling checkouts must remain simultaneously held"
                );
                connection.close().await?;
            }
            drop(waiting);
            held.close().await?;
            close(&child).await?;
            close(&sibling).await?;
            evidence.drained(&child)?;
            evidence.drained(&sibling)?;
            drop(child);
            drop(sibling);
            assert_exact_parent_capacity(&mut evidence, &parent, 2).await?;
            close(&parent).await?;
            evidence.drained(&parent)
        }
        .await;
        match verdict {
            Err(error) => Err(error).context(format!(
                "cleanup after saved acquisition failure={cleanup:?}"
            )),
            Ok(()) => cleanup,
        }
    }
    .await;
    evidence.finish(result)
}

struct ReleaseNotifyOnDrop(Arc<Notify>);

impl Drop for ReleaseNotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn child_close_waits_for_inflight_open_after_spare_reuse() -> Result<()> {
    let mut evidence = Evidence::new("child-inflight-open-")?;
    let result = async {
        let block = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let release_on_drop = ReleaseNotifyOnDrop(release.clone());
        let hook_block = block.clone();
        let hook_entered = entered.clone();
        let hook_release = release.clone();
        let parent = pool_options(3).connect_lazy_with(evidence.options());
        let child = pool_options(2).parent(parent.clone()).after_connect(move |_, _| {
            let block = hook_block.clone();
            let entered = hook_entered.clone();
            let release = hook_release.clone();
            Box::pin(async move {
                if block.load(std::sync::atomic::Ordering::Acquire) {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(())
            })
        }).connect_lazy_with(evidence.options());
        child.acquire().await?.close().await?;
        ensure!(child.size() == 0 && child.num_idle() == 0, "retained-spare fixture did not close");
        block.store(true, std::sync::atomic::Ordering::Release);
        let mut opening = Box::pin(child.acquire());
        tokio::time::timeout(COMPLETION_BOUND, async {
            tokio::select! {
                _ = entered.notified() => Ok(()),
                result = &mut opening => anyhow::bail!("open completed before deliberate hook release: {result:?}"),
            }
        }).await??;
        ensure!(child.size() == 1 && child.num_idle() == 0, "inflight open was not counted");
        evidence.state("inflight_open_at_actual_after_connect_barrier", &child)?;
        let mut closing = Box::pin(child.close());
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        }).await;
        release_on_drop.0.notify_one();
        ensure!(pending, "close completed while actual connection opening was in flight");
        let held = tokio::time::timeout(COMPLETION_BOUND, &mut opening).await??;
        drop(opening);
        let still_pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        }).await;
        held.close().await?;
        ensure!(still_pending, "close missed the checkout returned by inflight open");
        tokio::time::timeout(COMPLETION_BOUND, &mut closing).await?;
        drop(closing);
        evidence.drained(&child)?;
        drop(child);
        assert_exact_parent_capacity(&mut evidence, &parent, 3).await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }.await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn child_close_waits_with_retained_spare_permit() -> Result<()> {
    let mut evidence = Evidence::new("held-child-spare-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let child = pool_options(2)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        let held = child.acquire().await?;
        let removed = child.acquire().await?;
        ensure!(
            child.size() == 2 && child.num_idle() == 0,
            "two actual child checkouts required"
        );
        removed.close().await?;
        ensure!(
            child.size() == 1 && child.num_idle() == 0,
            "exact first checkout did not close"
        );
        evidence.state("held_child_with_retained_spare_before_close", &child)?;
        let mut closing = Box::pin(child.close());
        let early = tokio::time::timeout(HELD_OBSERVATION, &mut closing)
            .await
            .is_ok();
        writeln!(
            evidence.log,
            "child_close_returned_before_exact_held_release={early}"
        )?;
        held.close().await?;
        if !early {
            tokio::time::timeout(COMPLETION_BOUND, &mut closing)
                .await
                .context("child close did not finish after exact held checkout release")?;
        }
        drop(closing);
        evidence.state("child_after_exact_held_release", &child)?;
        ensure!(
            !early,
            "child close returned while an actual checkout was still held"
        );
        evidence.drained(&child)?;
        drop(child);
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn idle_only_close_drains_workers_and_counters() -> Result<()> {
    let mut evidence = Evidence::new("idle-only-")?;
    let result = async {
        let pool = pool_options(2).connect_with(evidence.options()).await?;
        ensure!(
            pool.size() == 1 && pool.num_idle() == 1,
            "missing real idle connection"
        );
        evidence.state("before_close", &pool)?;
        close(&pool).await?;
        evidence.drained(&pool)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn held_checkout_with_idle_peer_blocks_close() -> Result<()> {
    let mut evidence = Evidence::new("held-peer-")?;
    let result = async {
        let pool = pool_options(2).connect_with(evidence.options()).await?;
        let held = pool.acquire().await?;
        // A second actual checkout supplies the idle peer through the normal
        // return path, which is precisely the semaphore invariant being tested.
        let peer = pool.acquire().await?;
        drop(peer);
        tokio::time::timeout(COMPLETION_BOUND, async {
            while pool.num_idle() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("real peer did not return to idle")?;
        evidence.state("before_close", &pool)?;
        ensure!(
            pool.size() == 2 && pool.num_idle() == 1,
            "missing held/idle positive control"
        );
        let mut closing = Box::pin(pool.close());
        let early = tokio::time::timeout(HELD_OBSERVATION, &mut closing)
            .await
            .is_ok();
        writeln!(evidence.log, "completed_before_exact_held_release={early}")?;
        // Always release the exact held worker, preserving any earlier verdict.
        held.close().await?;
        if !early {
            tokio::time::timeout(COMPLETION_BOUND, &mut closing)
                .await
                .context("close did not finish after exact held release")?;
        }
        drop(closing);
        evidence.state("after_exact_release", &pool)?;
        ensure!(
            !early,
            "pool close returned while exact checkout was still held"
        );
        evidence.drained(&pool)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn return_started_before_close_is_drained_after_final_permit() -> Result<()> {
    let mut evidence = Evidence::new("late-return-")?;
    let result = async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let hook_entered = entered.clone();
        let hook_release = release.clone();
        let pool = pool_options(1)
            .after_release(move |_, _| {
                let entered = hook_entered.clone();
                let release = hook_release.clone();
                Box::pin(async move {
                    // The product checked is_closed BEFORE this callback. The
                    // real ping and idle insertion occur AFTER this barrier.
                    entered.notify_one();
                    release.notified().await;
                    Ok(true)
                })
            })
            .connect_with(evidence.options())
            .await?;
        let held = pool.acquire().await?;
        drop(held);
        tokio::time::timeout(COMPLETION_BOUND, entered.notified())
            .await
            .context("real after_release callback was not entered")?;
        ensure!(
            pool.size() == 1 && pool.num_idle() == 0,
            "return is not held at barrier"
        );
        let mut closing = Box::pin(pool.close());
        // Poll exactly once. Both old and fixed close must await the held
        // permit; this proves close started before allowing the actual return.
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        })
        .await;
        writeln!(evidence.log, "close_pending_at_return_barrier={pending}")?;
        release.notify_one();
        ensure!(
            pending,
            "close returned before blocked real return completed"
        );
        tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .context("close did not finish after releasing actual callback")?;
        drop(closing);
        evidence.drained(&pool)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn empty_child_close_finishes_without_parent_permits() -> Result<()> {
    let mut evidence = Evidence::new("empty-child-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let child = pool_options(2)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        ensure!(
            child.size() == 0 && child.num_idle() == 0,
            "child must start truly empty"
        );
        evidence.state("empty_child_before", &child)?;
        // Acquiring max_connections would wait forever: the child starts with
        // zero permits. A fixed implementation completes on this first poll,
        // rather than treating an outer timeout as a regression kill.
        let mut closing = Box::pin(child.close());
        let immediate = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_ready())
        })
        .await;
        drop(closing);
        ensure!(
            immediate,
            "empty child close incorrectly waits for parent-owned permits"
        );
        evidence.drained(&child)?;
        drop(child);
        let first = parent.acquire().await?;
        let second = parent.acquire().await?;
        ensure!(parent.size() == 2, "empty child changed parent capacity");
        first.close().await?;
        second.close().await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn partial_child_close_then_drop_returns_exact_parent_capacity() -> Result<()> {
    let mut evidence = Evidence::new("partial-child-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let child = pool_options(2)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            child.size() == 1 && child.num_idle() == 1,
            "child must have one of two slots"
        );
        evidence.state("partial_child_before", &child)?;
        close(&child).await?;
        evidence.drained(&child)?;
        // This drop is contractually significant: closed child ownership keeps
        // its stolen permits until the child pool itself is released.
        drop(child);
        assert_exact_parent_capacity(&mut evidence, &parent, 2).await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn concurrent_alias_closes_wait_for_same_held_checkout() -> Result<()> {
    let mut evidence = Evidence::new("alias-close-")?;
    let result = async {
        let pool = pool_options(1).connect_with(evidence.options()).await?;
        let alias = pool.clone();
        let held = pool.acquire().await?;
        let mut first = Box::pin(pool.close());
        let mut second = Box::pin(alias.close());
        let pending = std::future::poll_fn(|cx| {
            let first_pending = std::future::Future::poll(first.as_mut(), cx).is_pending();
            let second_pending = std::future::Future::poll(second.as_mut(), cx).is_pending();
            std::task::Poll::Ready(first_pending && second_pending)
        })
        .await;
        writeln!(
            evidence.log,
            "both_aliases_pending_with_held_checkout={pending}"
        )?;
        held.close().await?;
        ensure!(
            pending,
            "an alias close returned before held checkout release"
        );
        tokio::time::timeout(COMPLETION_BOUND, async {
            tokio::join!(&mut first, &mut second);
        })
        .await
        .context("concurrent alias close deadlocked after exact held release")?;
        drop((first, second));
        evidence.drained(&pool)?;
        evidence.drained(&alias)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn cancelling_waiting_close_does_not_lose_pool_permits() -> Result<()> {
    let mut evidence = Evidence::new("cancel-close-")?;
    let result = async {
        let pool = pool_options(1).connect_with(evidence.options()).await?;
        let held = pool.acquire().await?;
        let mut cancelled = Box::pin(pool.close());
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(cancelled.as_mut(), cx).is_pending())
        })
        .await;
        drop(cancelled);
        ensure!(
            pending && pool.is_closed(),
            "cancel fixture did not reach closed permit wait"
        );
        held.close().await?;
        // A distinct owner may finish a deliberately cancelled close. This
        // is not a retry/polling workaround for a completed-but-broken close.
        close(&pool).await?;
        evidence.drained(&pool)
    }
    .await;
    evidence.finish(result)
}

// ---------------------------------------------------------------------------
// Parent-side ownership controls.
//
// A child pool permanently takes ("steals") a permit from its parent, and that
// permit only returns when the CHILD POOL ITSELF is dropped -- not when the
// child is closed. A parent's `close()` must therefore wait only for the
// capacity the parent still owns, otherwise closing an unrelated parent blocks
// on a handle the caller may never drop. The four cases below pin the new
// semantics, the two after them are the negative controls that stop the fix
// from degenerating into "close() always returns early", and the last one pins
// the closed-parent refusal that keeps a donation from escaping a parent close
// which has already frozen its target.
// ---------------------------------------------------------------------------

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_close_completes_with_live_child_holding_donated_permit() -> Result<()> {
    let mut evidence = Evidence::new("parent-close-live-child-")?;
    let result = async {
        let parent = pool_options(2).connect_with(evidence.options()).await?;
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            parent.size() == 1 && parent.num_idle() == 1,
            "parent must keep one real connection of its own"
        );
        ensure!(
            child.size() == 1 && child.num_idle() == 1,
            "child must hold one actually donated parent permit"
        );
        evidence.state("parent_before_close", &parent)?;
        evidence.state("live_child_before_parent_close", &child)?;
        let mut closing = Box::pin(parent.close());
        let completed = tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .is_ok();
        drop(closing);
        writeln!(
            evidence.log,
            "parent_close_completed_with_live_child={completed}"
        )?;
        ensure!(
            completed,
            "parent close never finished while an OPEN child pool held a donated permit; \
             a parent must wait only for capacity it still owns"
        );
        ensure!(
            parent.is_closed() && parent.size() == 0 && parent.num_idle() == 0,
            "parent close completed without draining its own real connection"
        );
        // The donated permit belongs to the child: the parent's close must
        // neither reclaim it nor destroy the child's actual worker.
        ensure!(
            !child.is_closed() && child.size() == 1 && child.num_idle() == 1,
            "closing the parent disturbed the live child's own connection"
        );
        close(&child).await?;
        evidence.drained(&child)?;
        drop(child);
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_close_completes_with_closed_drained_child_handle_alive() -> Result<()> {
    let mut evidence = Evidence::new("parent-close-closed-child-")?;
    let result = async {
        let parent = pool_options(2).connect_with(evidence.options()).await?;
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            child.size() == 1 && child.num_idle() == 1,
            "child must hold one actually donated parent permit"
        );
        close(&child).await?;
        // The parent still owns a real idle worker on the same file here, so
        // this uses the counter view rather than the native exclusive-open
        // check; the exclusive-open proof is made on the parent at the end.
        evidence.state("closed_drained_child", &child)?;
        // Worst case: no connection is live anywhere, the child is closed and
        // fully drained, and only its HANDLE still owns the donated permit.
        ensure!(
            child.is_closed() && child.size() == 0 && child.num_idle() == 0,
            "child did not fully drain before the parent-close observation"
        );
        evidence.state("parent_before_close", &parent)?;
        let mut closing = Box::pin(parent.close());
        let completed = tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .is_ok();
        drop(closing);
        writeln!(
            evidence.log,
            "parent_close_completed_with_closed_child_handle={completed}"
        )?;
        ensure!(
            completed,
            "parent close never finished although the child pool was CLOSED and fully drained \
             and no connection was live anywhere; only the child handle was still alive"
        );
        ensure!(
            parent.is_closed() && parent.size() == 0 && parent.num_idle() == 0,
            "parent close completed without draining its own real connection"
        );
        drop(child);
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_close_is_bounded_when_a_dropped_child_handle_still_has_a_live_checkout(
) -> Result<()> {
    let mut evidence = Evidence::new("parent-dropped-child-checkout-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let held = child.acquire().await?;
        ensure!(
            child.size() == 1,
            "child must hold one actually donated parent permit"
        );
        // The handle goes away while a real checkout keeps the child's inner
        // pool -- and therefore the donated permit -- alive. `PoolConnection`
        // owns an `Arc<PoolInner>`, so the child's `Drop` cannot run yet.
        drop(child);
        evidence.state("parent_with_dropped_child_handle", &parent)?;
        let mut closing = Box::pin(parent.close());
        let completed = tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .is_ok();
        drop(closing);
        writeln!(
            evidence.log,
            "parent_close_completed_with_dropped_child_handle={completed}"
        )?;
        ensure!(
            completed,
            "parent close hung on a donated permit held by a live checkout of an \
             already-dropped child handle"
        );
        // Releasing the last checkout drops the child's inner pool, which really
        // does give the permit back. A second close must stay bounded and must
        // not be confused by that late credit.
        held.close().await?;
        close(&parent).await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_capacity_accounting_stays_exact_across_donation_return_and_loss() -> Result<()> {
    let mut evidence = Evidence::new("parent-exact-accounting-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());

        // 1. A donation that is genuinely returned: the parent regains exactly
        //    one permit, no more (a manufactured permit would let the sibling
        //    negative control inside the helper steal capacity).
        let returning = pool_options(1)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            returning.size() == 1,
            "first child must hold one actually donated parent permit"
        );
        close(&returning).await?;
        evidence.drained(&returning)?;
        drop(returning);
        assert_exact_parent_capacity(&mut evidence, &parent, 2).await?;

        // 2. A donation that can never come back. `PoolConnection::leak` keeps
        //    the permit out of the child's semaphore for good, so the child's
        //    `Drop` releases strictly fewer permits than were donated. Counting
        //    the donated total instead of the released total here would make the
        //    parent's close wait forever.
        let leaking = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        let leaked = leaking.acquire().await?.leak();
        ensure!(
            leaking.size() == 1,
            "leaked checkout must still count against the child"
        );
        drop(leaking);
        writeln!(
            evidence.log,
            "parent_capacity_after_permanent_loss_expected=1"
        )?;
        assert_exact_parent_capacity(&mut evidence, &parent, 1).await?;

        // The parent now owns exactly one permit. A close that still believed in
        // the permanently lost one could never finish.
        let mut closing = Box::pin(parent.close());
        let completed = tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .is_ok();
        drop(closing);
        writeln!(
            evidence.log,
            "parent_close_completed_after_permanent_permit_loss={completed}"
        )?;
        ensure!(
            completed,
            "parent close waited for a permit that a dropped child could never return"
        );
        leaked.close().await?;
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_close_still_waits_for_its_own_checkout_while_child_holds_donated_permit(
) -> Result<()> {
    let mut evidence = Evidence::new("parent-own-checkout-")?;
    let result = async {
        let parent = pool_options(2).connect_lazy_with(evidence.options());
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            child.size() == 1,
            "child must hold one actually donated parent permit"
        );
        let held = parent.acquire().await?;
        ensure!(
            parent.size() == 1 && parent.num_idle() == 0,
            "parent must hold its own real checkout"
        );
        evidence.state("parent_with_own_checkout", &parent)?;
        let mut closing = Box::pin(parent.close());
        let early = tokio::time::timeout(HELD_OBSERVATION, &mut closing)
            .await
            .is_ok();
        writeln!(
            evidence.log,
            "parent_close_returned_before_own_release={early}"
        )?;
        // Always release the exact held worker, preserving any earlier verdict.
        held.close().await?;
        if !early {
            tokio::time::timeout(COMPLETION_BOUND, &mut closing)
                .await
                .context(
                    "parent close did not finish after its OWN checkout was released, \
                     even though the only other permit belongs to the child",
                )?;
        }
        drop(closing);
        ensure!(
            !early,
            "parent close returned while its OWN checkout was still held; not waiting for \
             donated capacity must not turn into not waiting for owned capacity"
        );
        ensure!(
            parent.is_closed() && parent.size() == 0 && parent.num_idle() == 0,
            "parent close left its own capacity undrained"
        );
        close(&child).await?;
        evidence.drained(&child)?;
        drop(child);
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn parent_late_return_is_drained_although_a_child_holds_a_donated_permit() -> Result<()> {
    let mut evidence = Evidence::new("parent-late-return-")?;
    let result = async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let hook_entered = entered.clone();
        let hook_release = release.clone();
        let parent = pool_options(2)
            .after_release(move |_, _| {
                let entered = hook_entered.clone();
                let release = hook_release.clone();
                Box::pin(async move {
                    // The product checked is_closed BEFORE this callback. The
                    // real ping and idle insertion occur AFTER this barrier.
                    entered.notify_one();
                    release.notified().await;
                    Ok(true)
                })
            })
            .connect_lazy_with(evidence.options());
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_with(evidence.options())
            .await?;
        ensure!(
            child.size() == 1,
            "child must hold one actually donated parent permit"
        );
        let held = parent.acquire().await?;
        drop(held);
        tokio::time::timeout(COMPLETION_BOUND, entered.notified())
            .await
            .context("real after_release callback was not entered")?;
        ensure!(
            parent.size() == 1 && parent.num_idle() == 0,
            "parent return is not held at barrier"
        );
        let mut closing = Box::pin(parent.close());
        // Poll exactly once: the parent must still be waiting for its own
        // in-flight return, which will only reach the idle queue afterwards.
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        })
        .await;
        writeln!(
            evidence.log,
            "parent_close_pending_at_return_barrier={pending}"
        )?;
        release.notify_one();
        ensure!(
            pending,
            "parent close returned before its own blocked real return completed"
        );
        tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .context("parent close did not finish after releasing the actual return callback")?;
        drop(closing);
        ensure!(
            parent.is_closed() && parent.size() == 0 && parent.num_idle() == 0,
            "parent close did not drain the late return that landed in idle"
        );
        close(&child).await?;
        evidence.drained(&child)?;
        drop(child);
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}

// ---------------------------------------------------------------------------
// Closed-parent donation refusal.
//
// `acquire_permit` refuses to take a permit out of a parent that has already
// frozen its owned-permit count (`parent.0.is_closed()`, read under the
// parent's own `num_permits` lock). Deleting that refusal is an UNCONDITIONAL
// parent-close deadlock on a real interleaving, yet it is invisible to every
// ordinary test: `PoolInner::close()` runs `mark_closed()` eagerly at future
// creation and only afterwards re-takes the lock to read its target, so the
// dangerous window is the few instructions between the borrower's
// `parent_close_event` poll and its `parent.0.is_closed()` load. There is no
// await point in that window, so in ANY deterministic single-threaded schedule
// the close event fires first and the refusal branch is unreachable.
//
// Three ways to make it observable were considered:
//
//   (a) a test-only injectable barrier in the donation path, behind a
//       non-default cargo feature. `#[cfg(test)]` is NOT usable here: these
//       regressions are an external consumer crate, so sqlx-core is compiled as
//       a dependency and its `cfg(test)` items do not exist.
//   (b) a loom/shuttle model check. Rejected: both require every synchronisation
//       primitive on the path to be the model checker's own. The path runs
//       through `std::sync::Mutex`, `AtomicBool`, `event_listener::Event` AND
//       tokio's semaphore inside `sqlx_core::sync::AsyncSemaphore`; none of them
//       is loom/shuttle-instrumented, and replacing them would mean model
//       checking a rewrite rather than the shipped code -- which cannot kill a
//       mutation in the shipped code.
//   (c) a probabilistic multi-thread stress loop. Rejected: the window is a few
//       instructions wide and must coincide with an actual parent-permit
//       handoff, so the hit rate is far too low to bound, its failure mode is a
//       30 s timeout, and a stress test that only SOMETIMES kills the mutation
//       is worse than none -- it would also intermittently redden CI.
//
// (a) is therefore what this control uses. The seam
// (`sqlx_core::pool::donation_barrier`, feature
// `_test-pool-donation-barrier`) is compiled out entirely in production builds,
// including its static, and its single call site sits after the parent permit
// is in hand and before any bookkeeping lock is taken -- parking anywhere later
// would deadlock against the parent's own `mark_closed`.
// ---------------------------------------------------------------------------

// Everything from here to the end of the file is compiled only when the
// ENCLOSING package forwards sqlx-core's `_test-pool-donation-barrier` seam,
// because it is the only material in this file that names a cfg-gated
// sqlx-core item. Both packages that register this file declare a feature of
// that name:
//
//   * `sqlx` (the main workspace) declares it NON-DEFAULT, so the plain
//     `cargo test --features sqlite,runtime-tokio` still BUILDS and runs every
//     other case here; add `,_test-pool-donation-barrier` to run this one too.
//   * `sqlx-pool-close-regressions` (tests/pool-close-regressions) declares it
//     as a DEFAULT feature AND in this target's `required-features`, so the
//     regression gate can never quietly drop this case: with the seam off that
//     consumer does not build the target at all, and the suite's case count
//     falls by the whole file rather than silently by one.
//
// The gate is per-item rather than a `mod`, so the test's path -- and therefore
// its name in every harness and every retained log -- is unchanged.

/// Deterministic control for the donation/parent-close race.
///
/// The barrier runs synchronously on the borrowing task's thread with no pool
/// lock held, so parking it is safe. It parks only the FIRST donation and its
/// own wait is bounded, so a mistake here cannot wedge the process.
#[cfg(feature = "_test-pool-donation-barrier")]
#[derive(Default)]
struct DonationRace {
    armed: std::sync::atomic::AtomicBool,
    entered: Notify,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    timed_out: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "_test-pool-donation-barrier")]
impl DonationRace {
    fn hold_first_donation(&self) {
        if self.armed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            // The seam is process-wide; never hold a donation this test did not
            // set up, so a later one cannot be parked by accident.
            return;
        }
        self.entered.notify_one();
        let released = self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (released, _) = self
            .changed
            .wait_timeout_while(released, COMPLETION_BOUND, |released| !*released)
            .unwrap_or_else(|error| error.into_inner());
        self.timed_out
            .store(!*released, std::sync::atomic::Ordering::Release);
    }

    fn release(&self) {
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *released = true;
        self.changed.notify_all();
    }

    fn timed_out(&self) -> bool {
        self.timed_out.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Removes the process-wide barrier on every exit path, including a failing
/// assertion or a panic, so a dead test cannot park a later donation.
#[cfg(feature = "_test-pool-donation-barrier")]
struct InstalledDonationBarrier;

#[cfg(feature = "_test-pool-donation-barrier")]
impl InstalledDonationBarrier {
    fn install(barrier: sqlx_core::pool::donation_barrier::DonationBarrier) -> Self {
        sqlx_core::pool::donation_barrier::set_donation_barrier(Some(barrier));
        Self
    }
}

#[cfg(feature = "_test-pool-donation-barrier")]
impl Drop for InstalledDonationBarrier {
    fn drop(&mut self) {
        sqlx_core::pool::donation_barrier::set_donation_barrier(None);
    }
}

/// @tag integration
/// @tag cleanup
///
/// Two worker threads are the minimum this needs: the borrowing task parks a
/// real worker inside the donation window while the test body, which runs on
/// the `block_on` thread, closes the parent and drives the close future.
#[cfg(feature = "_test-pool-donation-barrier")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_close_refuses_a_donation_that_races_its_mark_closed() -> Result<()> {
    let mut evidence = Evidence::new("parent-donation-close-race-")?;
    let result = async {
        // The parent owns exactly ONE permit and one real worker, so the parked
        // donation is its entire capacity: a transfer that escapes the refusal
        // leaves the parent's close waiting for a permit the child now owns,
        // which can only come back when the CHILD POOL is dropped -- and this
        // test deliberately keeps the child alive past the oracle.
        let parent = pool_options(1).connect_with(evidence.options()).await?;
        ensure!(
            parent.size() == 1 && parent.num_idle() == 1,
            "parent must own one real idle worker before the race"
        );
        let child = pool_options(1)
            .parent(parent.clone())
            .connect_lazy_with(evidence.options());
        ensure!(
            child.size() == 0 && !child.is_closed(),
            "child must not have borrowed anything before the race"
        );
        evidence.state("parent_before_race", &parent)?;

        let race = Arc::new(DonationRace::default());
        let parked = race.clone();
        let _barrier = InstalledDonationBarrier::install(Arc::new(move || {
            parked.hold_first_donation();
        }));

        let borrower = tokio::spawn({
            let child = child.clone();
            async move { child.acquire().await.map(drop) }
        });
        tokio::time::timeout(COMPLETION_BOUND, race.entered.notified())
            .await
            .context("the actual donation never reached the parent-close window")?;

        // Prove the borrower really holds the parent's only permit, so the
        // window below is the real one and not a vacuous fixture.
        let permit_held_by_borrower = parent.try_acquire().is_none();
        writeln!(
            evidence.log,
            "parent_permit_held_by_parked_donation={permit_held_by_borrower}"
        )?;
        ensure!(
            permit_held_by_borrower,
            "the parked borrower does not actually hold the parent's permit; \
             the barrier did not land inside the donation window"
        );

        // `PoolInner::close` marks closed EAGERLY, when the future is created,
        // so `is_closed` is published here while the donation is still parked.
        let mut closing = Box::pin(parent.close());
        ensure!(
            parent.is_closed(),
            "creating the parent's close future did not publish is_closed"
        );
        // Poll exactly once so the close reads its frozen target and queues its
        // own acquire BEFORE the donation resumes. Without this the close could
        // read a post-donation count of zero and finish vacuously, which is
        // precisely the ordering the refusal has to survive.
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(closing.as_mut(), cx).is_pending())
        })
        .await;
        writeln!(
            evidence.log,
            "parent_close_pending_at_donation_window={pending}"
        )?;
        ensure!(
            pending,
            "parent close completed while its only permit was still parked in a donation"
        );

        // Resume the donation. It must now observe the frozen parent and hand
        // the permit straight back instead of taking it.
        race.release();
        let completed = tokio::time::timeout(COMPLETION_BOUND, &mut closing)
            .await
            .is_ok();
        drop(closing);
        writeln!(
            evidence.log,
            "parent_close_completed_after_racing_donation={completed} barrier_timed_out={}",
            race.timed_out()
        )?;
        ensure!(
            !race.timed_out(),
            "the donation barrier was never released within its own bound; \
             that is a harness failure, not a pool verdict"
        );
        ensure!(
            completed,
            "parent close never finished after a donation raced its mark_closed; \
             a transfer out of a pool that already froze its owned-permit count \
             must be REFUSED, or that frozen target can never be satisfied"
        );
        ensure!(
            parent.is_closed() && parent.size() == 0 && parent.num_idle() == 0,
            "parent close completed without draining its own real worker"
        );

        // The refusal must propagate exactly like the parent's close event, and
        // must leave no capacity behind in the child.
        let borrowed = tokio::time::timeout(COMPLETION_BOUND, borrower)
            .await
            .context("the refused borrower never finished")?
            .context("the borrower task panicked")?;
        let refused = matches!(borrowed, Err(sqlx::Error::PoolClosed));
        writeln!(
            evidence.log,
            "refused_borrower={borrowed:?} propagated_pool_closed={refused}"
        )?;
        ensure!(
            refused,
            "a donation out of an already-closed parent was served instead of refused"
        );
        ensure!(
            child.is_closed(),
            "the refusal did not propagate the parent's close to the child"
        );
        ensure!(
            child.size() == 0 && child.num_idle() == 0,
            "the refused donation left borrowed capacity behind in the child"
        );

        close(&child).await?;
        drop(child);
        evidence.drained(&parent)
    }
    .await;
    evidence.finish(result)
}
