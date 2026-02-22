//! Examples showing multi-storage patterns.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_storage_example --features="memory"
//! ```

use futures::StreamExt as _;
use std::time::Duration;
use stowage::multi::migration::{ConflictStrategy, MigrateOptions, migrate};
use stowage::multi::{FallbackStorage, MirrorStorage, ReturnPolicy, WriteStrategy};
use stowage::{MemoryStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Multi-Storage Examples ===\n");

    // Example 1: Mirror - replicate writes across backends
    example_mirror().await?;

    // Example 2: Fallback - use secondary when primary fails
    example_fallback().await?;

    // Example 3: Migration - bulk-copy items from one backend to another
    example_migration().await?;

    // Example 4: Migration with move semantics and prefix filtering
    example_migration_move().await?;

    // Example 5: Migration with conflict handling
    example_migration_conflicts().await?;

    println!("\n=== Done! ===");
    Ok(())
}

// ── Example 1 ─────────────────────────────────────────────────────────────────

/// Mirror Storage — replicate data across multiple backends.
async fn example_mirror() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Mirror Storage ---");

    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::Quorum { rollback: false }) // Need 2/3
        .return_policy(ReturnPolicy::Optimistic) // Return early!
        .backend_timeout(Duration::from_secs(5))
        .build();

    let data = b"data";
    storage
        .put(
            "file.txt".to_string(),
            &mut std::io::Cursor::new(data),
            Some(data.len() as u64),
        )
        .await?;

    let mut buf = Vec::new();
    storage.get_into(&"file.txt".to_string(), &mut buf).await?;
    assert_eq!(buf, b"data");

    println!("Wrote to 3 backends with quorum strategy, read from primary");
    println!();
    Ok(())
}

// ── Example 2 ─────────────────────────────────────────────────────────────────

/// Fallback Storage — writes to both, reads from primary.
async fn example_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Fallback Storage ---");

    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    let storage = FallbackStorage::new(primary, secondary).with_write_through(true);

    let data = b"replicated data";
    storage
        .put(
            "file.txt".to_string(),
            &mut std::io::Cursor::new(data),
            Some(data.len() as u64),
        )
        .await?;

    assert!(storage.primary().exists(&"file.txt".to_string()).await?);
    assert!(storage.secondary().exists(&"file.txt".to_string()).await?);

    let mut buf = Vec::new();
    storage.get_into(&"file.txt".to_string(), &mut buf).await?;
    assert_eq!(buf, b"replicated data");

    println!("Writes to both primary and secondary, reads from primary");
    println!();
    Ok(())
}

// ── Example 3 ─────────────────────────────────────────────────────────────────

/// Migration — bulk-copy every item from one storage backend to another.
///
/// This is the simplest migration: copy everything from `source` into `dest`
/// with the default options (overwrite conflicts, concurrency of 4, keep
/// source intact).
async fn example_migration() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Migration: copy all items ---");

    // Imagine `source` is your old storage and `dest` is the new one.
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    // Seed the source with some files.
    for i in 1..=5u8 {
        source
            .put_bytes(
                format!("report-{i}.txt"),
                format!("report content {i}").as_bytes(),
            )
            .await?;
    }

    println!("Source has 5 files before migration.");

    // Migrate everything with default options.
    let result = migrate(&source, &dest, MigrateOptions::default()).await?;

    println!("{result}");
    assert_eq!(result.transferred_count(), 5);
    assert!(result.is_complete(), "no errors expected");

    // Source is unchanged — this is a copy, not a move.
    let source_stream = source.list(None).await?;
    let source_count = source_stream.count().await;
    assert_eq!(source_count, 5, "source still has all files");

    println!("Source still has {source_count} files (copy, not move).");
    println!();
    Ok(())
}

// ── Example 4 ─────────────────────────────────────────────────────────────────

/// Migration with move semantics and prefix filtering.
///
/// Only files under `"archive/"` are migrated, and they are deleted from the
/// source once successfully copied — effectively moving them to the new backend.
async fn example_migration_move() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Migration: move a prefix ---");

    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    // A mix of files — only the "archive/" ones should move.
    source
        .put_bytes("archive/2022.tar.gz".to_string(), b"2022 data")
        .await?;
    source
        .put_bytes("archive/2023.tar.gz".to_string(), b"2023 data")
        .await?;
    source
        .put_bytes("archive/2024.tar.gz".to_string(), b"2024 data")
        .await?;
    source
        .put_bytes("live/current.db".to_string(), b"live data")
        .await?;
    source
        .put_bytes("live/index.db".to_string(), b"index data")
        .await?;

    println!("Source before: {} archive files, 2 live files.", 3);

    let result = source
        .migrate_to(
            &dest,
            MigrateOptions {
                prefix: Some("archive/".to_string()),
                delete_source: true, // move semantics
                concurrency: 4,
                ..Default::default()
            },
        )
        .await?;

    println!("{result}");

    assert_eq!(result.transferred_count(), 3);
    assert_eq!(result.deleted_count(), 3);
    assert!(result.is_complete());

    // Archive files are gone from source.
    assert!(!source.exists(&"archive/2022.tar.gz".to_string()).await?);
    assert!(!source.exists(&"archive/2023.tar.gz".to_string()).await?);

    // Live files are untouched.
    assert!(source.exists(&"live/current.db".to_string()).await?);
    assert!(source.exists(&"live/index.db".to_string()).await?);

    // Archive files are present in dest.
    assert!(dest.exists(&"archive/2022.tar.gz".to_string()).await?);
    assert!(dest.exists(&"archive/2023.tar.gz".to_string()).await?);

    println!("Archive files moved to dest; live files remain in source.");
    println!();
    Ok(())
}

// ── Example 5 ─────────────────────────────────────────────────────────────────

/// Migration with conflict handling.
///
/// Demonstrates the three [`ConflictStrategy`] variants when the destination
/// already contains some of the files being migrated.
async fn example_migration_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Migration: conflict strategies ---");

    // ── Skip ─────────────────────────────────────────────────────────────────
    {
        let source = MemoryStorage::new();
        let dest = MemoryStorage::new();

        source.put_bytes("a.txt".to_string(), b"new a").await?;
        source.put_bytes("b.txt".to_string(), b"new b").await?;
        source.put_bytes("c.txt".to_string(), b"new c").await?;

        // Pre-populate dest with a stale copy of "a.txt".
        dest.put_bytes("a.txt".to_string(), b"old a").await?;

        let result = migrate(
            &source,
            &dest,
            MigrateOptions {
                conflict: ConflictStrategy::Skip,
                ..Default::default()
            },
        )
        .await?;

        println!("[Skip]  {result}");
        assert_eq!(result.transferred_count(), 2); // b.txt and c.txt
        assert_eq!(result.skipped_count(), 1); // a.txt was skipped
        assert!(result.is_complete()); // skips are not errors

        // The original "a.txt" in dest must be preserved.
        let kept = StorageExt::get_bytes(&dest, &"a.txt".to_string()).await?;
        assert_eq!(kept, b"old a", "original content must be preserved");
    }

    // ── Overwrite (default) ───────────────────────────────────────────────────
    {
        let source = MemoryStorage::new();
        let dest = MemoryStorage::new();

        source.put_bytes("a.txt".to_string(), b"new a").await?;
        dest.put_bytes("a.txt".to_string(), b"old a").await?;

        let result = migrate(&source, &dest, MigrateOptions::default()).await?;

        println!("[Overwrite] {result}");
        assert_eq!(result.transferred_count(), 1);

        let overwritten = StorageExt::get_bytes(&dest, &"a.txt".to_string()).await?;
        assert_eq!(overwritten, b"new a", "content must be overwritten");
    }

    // ── Fail ─────────────────────────────────────────────────────────────────
    {
        let source = MemoryStorage::new();
        let dest = MemoryStorage::new();

        source.put_bytes("a.txt".to_string(), b"new a").await?;
        source.put_bytes("b.txt".to_string(), b"new b").await?;
        dest.put_bytes("a.txt".to_string(), b"old a").await?;

        let result = migrate(
            &source,
            &dest,
            MigrateOptions {
                conflict: ConflictStrategy::Fail,
                ..Default::default()
            },
        )
        .await?;

        println!("[Fail]  {result}");
        assert_eq!(result.transferred_count(), 1); // b.txt succeeded
        assert_eq!(result.error_count(), 1); // a.txt recorded as error

        // The migration itself does NOT return Err — per-item errors are
        // collected inside the result so the rest of the batch can proceed.
        let (failed_id, err) = &result.errors[0];
        println!("  conflict error on '{failed_id}': {err}");
    }

    println!();
    Ok(())
}
