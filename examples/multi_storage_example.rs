//! Example demonstrating multi-storage patterns and their composition.
//!
//! This example shows how to use FallbackStorage and MirrorStorage both
//! independently and composed together to create sophisticated storage architectures.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_storage_example --features="memory,local"
//! ```

use stowage::multi::{FallbackStorage, MirrorStorage, WriteStrategy};
use stowage::{LocalStorage, MemoryStorage, Storage, StorageExt};
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Multi-Storage Pattern Examples ===\n");

    // Example 1: Simple Fallback
    example_1_simple_fallback().await?;

    // Example 2: Simple Mirror
    example_2_simple_mirror().await?;

    // Example 3: Composed - Fallback with Mirror
    example_3_fallback_with_mirror().await?;

    // Example 4: Read-Only Storage
    example_4_readonly().await?;

    // Example 5: Mirror Error Details
    example_5_mirror_error_details().await?;

    // Example 6: Advanced Composition
    example_6_advanced_composition().await?;

    println!("\n=== All examples completed successfully! ===");
    Ok(())
}

/// Example 1: Simple Fallback Storage
async fn example_1_simple_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: Simple Fallback ---");

    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    secondary
        .put_bytes("old-file.txt".to_string(), b"This was in the backup")
        .await?;

    let storage = FallbackStorage::new(primary, secondary);

    storage
        .put_bytes("new-file.txt".to_string(), b"Fresh data")
        .await?;

    let new_data = storage.get_string(&"new-file.txt".to_string()).await?;
    println!("  New file (from primary): {}", new_data);

    let old_data = storage.get_string(&"old-file.txt".to_string()).await?;
    println!("  Old file (from secondary fallback): {}", old_data);

    assert!(
        !storage
            .primary()
            .exists(&"old-file.txt".to_string())
            .await?
    );
    println!("  ✓ Fallback works correctly\n");

    Ok(())
}

/// Example 2: Simple Mirror Storage
async fn example_2_simple_mirror() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 2: Simple Mirror ---");

    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: false })
        .build();

    storage
        .put_bytes("mirrored.txt".to_string(), b"Replicated data")
        .await?;

    println!("  Wrote to {} backends", storage.backend_count());

    for i in 0..storage.backend_count() {
        let exists = storage
            .backend(i)
            .unwrap()
            .exists(&"mirrored.txt".to_string())
            .await?;
        println!("    Backend {}: {}", i, if exists { "✓" } else { "✗" });
    }

    let data = storage.get_string(&"mirrored.txt".to_string()).await?;
    println!("  Read data: {}", data);
    println!("  ✓ Mirroring works correctly\n");

    Ok(())
}

/// Example 3: Fallback with Mirror
async fn example_3_fallback_with_mirror() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 3: Fallback with Mirrored Primary ---");

    let mirrored_primary = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::Quorum)
        .build();

    let cache = MemoryStorage::new();
    cache
        .put_bytes("cached-file.txt".to_string(), b"From cache")
        .await?;

    let storage = FallbackStorage::new(mirrored_primary, cache);

    storage
        .put_bytes("new-data.txt".to_string(), b"Fresh mirrored data")
        .await?;

    let mirror = storage.primary();
    assert!(
        mirror
            .backend(0)
            .unwrap()
            .exists(&"new-data.txt".to_string())
            .await?
    );
    assert!(
        mirror
            .backend(1)
            .unwrap()
            .exists(&"new-data.txt".to_string())
            .await?
    );

    let data = storage.get_string(&"new-data.txt".to_string()).await?;
    println!("  ✓ Read from primary: {}", data);

    let cached = storage.get_string(&"cached-file.txt".to_string()).await?;
    println!("  ✓ Read from cache fallback: {}", cached);

    assert!(
        !mirror
            .backend(0)
            .unwrap()
            .exists(&"cached-file.txt".to_string())
            .await?
    );
    println!("  ✓ Composition works correctly\n");

    Ok(())
}

/// Example 4: Read-Only Storage
async fn example_4_readonly() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 4: Read-Only Storage ---");

    use stowage::multi::ReadOnlyStorage;

    let inner = MemoryStorage::new();
    inner
        .put_bytes("readonly-file.txt".to_string(), b"Cannot modify this")
        .await?;

    let storage = ReadOnlyStorage::new(inner);

    let data = storage.get_string(&"readonly-file.txt".to_string()).await?;
    println!("  Read data: {}", data);

    let result = storage.put_bytes("new.txt".to_string(), b"data").await;
    assert!(result.is_err());
    println!("  ✓ Write rejected\n");
    Ok(())
}

/// Example 5: Mirror Error Details
async fn example_5_mirror_error_details() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 5: Mirror Error Details ---");

    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: false })
        .build();

    storage.put_bytes("test.txt".to_string(), b"data").await?;
    println!("  ✓ Write succeeded");

    let storage_with_rollback = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: true })
        .build();

    storage_with_rollback
        .put_bytes("file.txt".to_string(), b"test")
        .await?;
    println!("  ✓ Rollback enabled for atomic operations\n");

    Ok(())
}

/// Example 6: Advanced Composition
async fn example_6_advanced_composition() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 6: Advanced Multi-Tier Architecture ---");

    // Create temporary directories for local storage
    let temp1 = TempDir::new()?;
    let temp2 = TempDir::new()?;
    let temp_fallback = TempDir::new()?;

    println!("  Architecture:");
    println!(
        "    Tier 1 (Hot): Mirror[Local({}), Local({})]",
        temp1.path().display(),
        temp2.path().display()
    );
    println!(
        "    Tier 2 (Warm): Local({})",
        temp_fallback.path().display()
    );

    // Tier 1: Mirrored fast storage (in production, these might be different regions/clouds)
    let tier1_mirror = MirrorStorage::builder()
        .add_backend(LocalStorage::new(temp1.path()))
        .add_backend(LocalStorage::new(temp2.path()))
        .write_strategy(WriteStrategy::AllOrFail { rollback: true })
        .build();

    // Tier 2: Fallback storage (older/archived data)
    let tier2_fallback = LocalStorage::new(temp_fallback.path());

    // Pre-populate tier 2 with archived data
    tier2_fallback
        .put_bytes(
            "archive/old-report.pdf".to_string(),
            b"Archived report data",
        )
        .await?;

    // Compose the complete storage system
    let storage = FallbackStorage::new(tier1_mirror, tier2_fallback).with_write_through(false); // Only write to tier 1

    println!("\n  Scenario 1: Write new data (goes to tier 1 mirror)");
    storage
        .put_bytes(
            "reports/2024-report.pdf".to_string(),
            b"Current report data",
        )
        .await?;
    println!("    ✓ Wrote to both tier 1 backends");

    // Verify it's in both tier 1 backends
    let tier1 = storage.primary();
    assert!(
        tier1
            .backend(0)
            .unwrap()
            .exists(&"reports/2024-report.pdf".to_string())
            .await?
    );
    assert!(
        tier1
            .backend(1)
            .unwrap()
            .exists(&"reports/2024-report.pdf".to_string())
            .await?
    );
    println!("    ✓ Confirmed in both mirror backends");

    println!("\n  Scenario 2: Read current data (from tier 1)");
    let current = storage
        .get_string(&"reports/2024-report.pdf".to_string())
        .await?;
    println!("    ✓ Read: {} bytes", current.len());

    println!("\n  Scenario 3: Read archived data (falls back to tier 2)");
    let archived = storage
        .get_string(&"archive/old-report.pdf".to_string())
        .await?;
    println!("    ✓ Read from tier 2: {} bytes", archived.len());

    // Verify it's NOT in tier 1
    assert!(
        !tier1
            .backend(0)
            .unwrap()
            .exists(&"archive/old-report.pdf".to_string())
            .await?
    );
    println!("    ✓ Confirmed fallback from tier 2");

    println!("\n  Scenario 4: Delete from all tiers");
    // Put data in tier 2 first
    storage
        .secondary()
        .put_bytes("to-delete.txt".to_string(), b"Will be deleted")
        .await?;

    storage.delete(&"to-delete.txt".to_string()).await?;
    println!("    ✓ Deleted from all tiers");

    println!("\n  ✓ Advanced composition works correctly!\n");

    Ok(())
}
