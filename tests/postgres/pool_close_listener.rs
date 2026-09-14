//! Real PostgreSQL stream ownership control for SQLx issue 3217 and PR 3952.

use anyhow::{ensure, Context, Result};
use futures::TryStreamExt;
use sqlx::postgres::{PgListener, PgPoolOptions};
use std::{fs, io::Write, path::PathBuf, time::Duration};

/// @tag integration
/// @tag cleanup
#[tokio::test]
async fn listener_stream_holds_checkout_until_dropped_pg() -> Result<()> {
    let directory = std::env::var_os("SQLX_POOL_CLOSE_TEST_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-logs/pool-close")
        });
    fs::create_dir_all(&directory)?;
    let (mut log, path) = tempfile::Builder::new()
        .prefix("pg-listener-")
        .suffix(".log")
        .tempfile_in(&directory)?
        .keep()?;
    let result = async {
        // Missing infrastructure is an ordinary prerequisite failure, not a
        // successful skip. The AH test recipe supplies a real ephemeral PG.
        let url = std::env::var("DATABASE_URL")
            .context("real ephemeral PostgreSQL DATABASE_URL is required")?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(2)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect(&url)
            .await?;
        let channel = path
            .file_stem()
            .context("unique log needs filename")?
            .to_string_lossy()
            .replace('-', "_");
        let mut listener = PgListener::connect_with(&pool).await?;
        listener.listen(&channel).await?;
        let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut listener)
            .await?;
        let mut stream = listener.into_stream();
        let payload = path
            .file_name()
            .context("unique log needs filename")?
            .to_string_lossy()
            .into_owned();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind(&payload)
            .execute(&pool)
            .await?;
        let received = tokio::time::timeout(Duration::from_secs(30), stream.try_next())
            .await
            .context("real PostgreSQL notification not delivered")??
            .context("listener stream ended before actual notification")?;
        writeln!(
            log,
            "actual_listener_backend={backend} channel={} payload={} pool_size={} pool_idle={}",
            received.channel(),
            received.payload(),
            pool.size(),
            pool.num_idle()
        )?;
        ensure!(
            received.channel() == channel && received.payload() == payload,
            "LISTEN/NOTIFY positive control disagrees"
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            while pool.num_idle() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("actual notification query checkout did not return to idle")?;
        ensure!(
            pool.size() == 2 && pool.num_idle() == 1,
            "listener must hold one actual checkout beside an idle peer"
        );
        let mut closing = Box::pin(pool.close());
        let early = tokio::time::timeout(Duration::from_secs(1), &mut closing)
            .await
            .is_ok();
        writeln!(
            log,
            "close_returned_before_stream_drop={early} closed={} size={} idle={}",
            pool.is_closed(),
            pool.size(),
            pool.num_idle()
        )?;
        // Match the corrected postgres/listen example: stream ownership must
        // end before awaiting pool close. No polling/retry of completed close.
        drop(stream);
        if !early {
            tokio::time::timeout(Duration::from_secs(30), &mut closing)
                .await
                .context("close did not finish after exact listener stream drop")?;
        }
        drop(closing);
        writeln!(
            log,
            "after_stream_drop closed={} size={} idle={}",
            pool.is_closed(),
            pool.size(),
            pool.num_idle()
        )?;
        ensure!(
            !early,
            "close returned while a live listener stream still held its checkout"
        );
        ensure!(
            pool.is_closed() && pool.size() == 0 && pool.num_idle() == 0,
            "listener close did not release all actual pool resources"
        );
        Ok(())
    }
    .await;
    let written = writeln!(log, "result={result:#?}").and_then(|()| log.flush());
    let bytes = fs::metadata(&path).map(|meta| meta.len());
    let context = format!("full log: {} (size={bytes:?} bytes)", path.display());
    match result {
        Err(error) => Err(error).context(format!("{context}; final write={written:?}")),
        Ok(()) => written.context(context),
    }
}
