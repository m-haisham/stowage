//! Queue-based move using tokio channels.
//!
//! Demonstrates moving every file from one storage backend to another using a
//! classic producer / worker-pool pattern backed by a bounded [`tokio::sync::mpsc`]
//! channel.
//!
//! ```text
//!   ┌──────────┐   IDs    ┌──────────────────────────────┐
//!   │ producer │ ───────► │  mpsc channel (capacity = 8) │
//!   └──────────┘          └──────────┬───────────────────┘
//!                                    │
//!                   ┌────────────────┼────────────────┐
//!                   ▼                ▼                ▼
//!              [worker 0]       [worker 1]  ...  [worker N]
//!           move_to(dest)    move_to(dest)    move_to(dest)
//! ```
//!
//! Key properties of this approach:
//!
//! - **Streaming** — the producer feeds IDs into the channel as it receives
//!   them from [`Storage::list`], so the full list never has to sit in memory
//!   all at once.
//! - **Back-pressure** — the bounded channel capacity means the producer
//!   automatically pauses whenever the workers fall behind.
//! - **Concurrency** — multiple worker tasks run in parallel, each holding the
//!   channel lock only long enough to pop a single ID.
//! - **Graceful shutdown** — dropping the sender (`tx`) closes the channel;
//!   workers exit their loops naturally when `recv()` returns `None`.
//!
//! Run with:
//! ```sh
//! cargo run --example queue_move --features memory
//! ```

use std::sync::Arc;

use futures::StreamExt as _;
use stowage::{MemoryStorage, Storage, StorageExt};
use tokio::sync::{Mutex, mpsc};

/// Number of concurrent worker tasks draining the channel.
const WORKERS: usize = 4;

/// Bound on the in-flight ID queue.  The producer blocks once this many IDs
/// are waiting, giving the workers time to catch up.
const CHANNEL_CAPACITY: usize = 8;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Setup ─────────────────────────────────────────────────────────────────

    let source = Arc::new(MemoryStorage::new());
    let dest = Arc::new(MemoryStorage::new());

    // Seed the source with 20 files spread across two virtual directories.
    for i in 1..=10u32 {
        source
            .put_bytes(
                format!("incoming/batch-a/{i:03}.bin"),
                format!("content-a-{i}").as_bytes(),
            )
            .await?;
        source
            .put_bytes(
                format!("incoming/batch-b/{i:03}.bin"),
                format!("content-b-{i}").as_bytes(),
            )
            .await?;
    }

    let initial_count = source.as_ref().list(None).await?.count().await;
    println!("Source seeded with {initial_count} files.");
    println!("Moving to dest with {WORKERS} workers...\n");

    // ── Channel ───────────────────────────────────────────────────────────────

    let (tx, rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);

    // ── Producer ──────────────────────────────────────────────────────────────
    // Lists the source and pushes every ID into the channel.  Dropping `tx`
    // at the end of the async block closes the channel so workers know when
    // to stop.

    let producer_source = Arc::clone(&source);
    let producer = tokio::spawn(async move {
        let stream = producer_source
            .as_ref()
            .list(None)
            .await
            .expect("failed to list source");

        futures::pin_mut!(stream);
        while let Some(result) = stream.next().await {
            match result {
                Ok(id) => {
                    if tx.send(id).await.is_err() {
                        // All receivers dropped — workers exited early (e.g. on error).
                        break;
                    }
                }
                Err(e) => eprintln!("[producer] skipping unreadable ID: {e}"),
            }
        }
        // `tx` is implicitly dropped here, closing the channel.
    });

    // ── Workers ───────────────────────────────────────────────────────────────
    // Each worker shares the same receiver behind an Arc<Mutex>.  The lock is
    // held only for the instant it takes to pop one ID, so workers spend the
    // vast majority of their time doing actual I/O in parallel.

    let rx = Arc::new(Mutex::new(rx));

    let mut handles = Vec::with_capacity(WORKERS);
    for worker_id in 0..WORKERS {
        let rx = Arc::clone(&rx);
        let source = Arc::clone(&source);
        let dest = Arc::clone(&dest);

        handles.push(tokio::spawn(async move {
            let mut moved = 0usize;
            let mut errors = 0usize;

            loop {
                // Acquire the lock only to pop the next ID, then release it
                // immediately so other workers can proceed concurrently.
                let id = rx.lock().await.recv().await;

                match id {
                    // Channel closed and drained — we're done.
                    None => break,

                    Some(id) => match source.move_to(&id, dest.as_ref()).await {
                        Ok(()) => {
                            println!("  [worker {worker_id}] ✓ moved {id}");
                            moved += 1;
                        }
                        Err(e) => {
                            eprintln!("  [worker {worker_id}] ✗ failed to move {id}: {e}");
                            errors += 1;
                        }
                    },
                }
            }

            (moved, errors)
        }));
    }

    // ── Collect results ───────────────────────────────────────────────────────

    // Wait for the producer to finish listing before joining workers, so we
    // don't exit before the channel is fully drained.
    producer.await?;

    let mut total_moved = 0usize;
    let mut total_errors = 0usize;
    for handle in handles {
        let (moved, errors) = handle.await?;
        total_moved += moved;
        total_errors += errors;
    }

    // ── Summary ───────────────────────────────────────────────────────────────

    println!();
    println!("── Summary ──────────────────────────────────");
    println!("  Moved:  {total_moved}");
    println!("  Errors: {total_errors}");

    let source_remaining = source.as_ref().list(None).await?.count().await;
    let dest_count = dest.as_ref().list(None).await?.count().await;

    println!("  Source remaining: {source_remaining}");
    println!("  Dest total:       {dest_count}");

    assert_eq!(total_errors, 0, "expected no errors");
    assert_eq!(source_remaining, 0, "source must be empty after move");
    assert_eq!(dest_count, initial_count, "dest must contain all files");

    println!("\nAll {dest_count} files moved successfully. ✓");

    Ok(())
}
