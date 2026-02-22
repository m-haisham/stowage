//! Demonstrates the multi-storage adapters: Mirror, Fallback, and Migration.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_storage_example --features="memory"
//! ```

use std::time::Duration;

use futures::StreamExt as _;
use stowage::multi::migration::{ConflictStrategy, MigrateOptions, migrate};
use stowage::multi::{FallbackStorage, MirrorStorage, ReturnPolicy, WriteStrategy};
use stowage::{MemoryStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    example_mirror().await?;
    example_fallback().await?;
    example_migration_copy().await?;
    example_migration_move().await?;
    example_migration_conflicts().await?;
    Ok(())
}

// ── Mirror ────────────────────────────────────────────────────────────────────

/// [`MirrorStorage`] fans writes out to every backend and reads from the first
/// healthy one.  [`WriteStrategy::Quorum`] means a write only needs to succeed
/// on the majority (2 of 3 here) to be considered successful.
async fn example_mirror() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Mirror ===");

    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::Quorum { rollback: false })
        .return_policy(ReturnPolicy::Optimistic)
        .backend_timeout(Duration::from_secs(5))
        .build();

    storage.put_bytes("hello.txt".to_string(), b"hello").await?;

    let content = storage.get_string(&"hello.txt".to_string()).await?;
    println!("Read back: {content}");

    println!();
    Ok(())
}

// ── Fallback ──────────────────────────────────────────────────────────────────

/// [`FallbackStorage`] tries the primary for reads; if it fails it falls back
/// to the secondary.  With `write_through` enabled, writes go to both so the
/// secondary stays in sync.
async fn example_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Fallback ===");

    let storage =
        FallbackStorage::new(MemoryStorage::new(), MemoryStorage::new()).with_write_through(true);

    storage
        .put_bytes("config.toml".to_string(), b"key = 1")
        .await?;

    // Both backends now hold the file.
    let in_primary = Storage::exists(storage.primary(), &"config.toml".to_string()).await?;
    let in_secondary = Storage::exists(storage.secondary(), &"config.toml".to_string()).await?;
    println!("In primary: {in_primary}, in secondary: {in_secondary}");

    println!();
    Ok(())
}

// ── Migration: copy ───────────────────────────────────────────────────────────

/// [`migrate`] bulk-copies every item from `source` to `dest`.  The source is
/// left untouched — this is a copy, not a move.
async fn example_migration_copy() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Migration: copy ===");

    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    for i in 1..=5u8 {
        source.put_bytes(format!("file-{i}.txt"), &[i]).await?;
    }

    let result = migrate(&source, &dest, MigrateOptions::default()).await?;
    println!("{result}");

    let source_count = Storage::list(&source, None).await?.count().await;
    println!("Source still has {source_count} files.");

    println!();
    Ok(())
}

// ── Migration: move ───────────────────────────────────────────────────────────

/// Setting `delete_source: true` gives move semantics — items are removed from
/// the source after a successful copy.  Combined with a `prefix`, only the
/// matching subset is moved.
async fn example_migration_move() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Migration: move a prefix ===");

    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    for year in [2022u16, 2023, 2024] {
        source
            .put_bytes(format!("archive/{year}.tar.gz"), b"data")
            .await?;
    }
    source
        .put_bytes("live/current.db".to_string(), b"live")
        .await?;

    let result = source
        .migrate_to(
            &dest,
            MigrateOptions {
                prefix: Some("archive/".to_string()),
                delete_source: true,
                ..Default::default()
            },
        )
        .await?;
    println!("{result}");

    let source_remaining: Vec<String> = Storage::list(&source, None)
        .await?
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    println!("Remaining in source: {source_remaining:?}");

    println!();
    Ok(())
}

// ── Migration: conflict strategies ───────────────────────────────────────────

/// When the destination already holds a file with the same ID, [`ConflictStrategy`]
/// decides what happens:
///
/// - **`Overwrite`** *(default)* — replace the existing file.
/// - **`Skip`** — leave the destination file untouched; count it as skipped.
/// - **`Fail`** — record it as a per-item error (the rest of the batch still runs).
async fn example_migration_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Migration: conflict strategies ===");

    let source = MemoryStorage::new();
    source.put_bytes("a.txt".to_string(), b"new").await?;
    source.put_bytes("b.txt".to_string(), b"new").await?;

    // Each strategy gets a fresh dest pre-populated with a stale "a.txt" so
    // the results are independent and comparable.

    // Skip — "a.txt" stays as "old"; "b.txt" is copied normally.
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"old").await?;
    let skip = migrate(
        &source,
        &dest,
        MigrateOptions {
            conflict: ConflictStrategy::Skip,
            ..Default::default()
        },
    )
    .await?;
    println!("Skip:      {skip}");

    // Overwrite (default) — "a.txt" is replaced with "new".
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"old").await?;
    let overwrite = migrate(&source, &dest, MigrateOptions::default()).await?;
    println!("Overwrite: {overwrite}");

    // Fail — "a.txt" conflict is recorded as an error; "b.txt" still succeeds.
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"old").await?;
    let fail = migrate(
        &source,
        &dest,
        MigrateOptions {
            conflict: ConflictStrategy::Fail,
            ..Default::default()
        },
    )
    .await?;
    println!("Fail:      {fail}");
    if let Some((id, err)) = fail.errors.first() {
        println!("  error on '{id}': {err}");
    }

    println!();
    Ok(())
}
