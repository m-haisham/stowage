//! Simple examples showing multi-storage patterns.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_storage_example --features="memory"
//! ```

use std::time::Duration;
use stowage::multi::{FallbackStorage, MirrorStorage, ReturnPolicy, WriteStrategy};
use stowage::{MemoryStorage, Storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Multi-Storage Examples ===\n");

    // Example 1: Mirror - replicate writes across backends
    example_mirror().await?;

    // Example 2: Fallback - use secondary when primary fails
    example_fallback().await?;

    println!("\n=== Done! ===");
    Ok(())
}

/// Example 1: Mirror Storage - replicate data across multiple backends
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

    // Write to all backends (with Optimistic policy, returns early)
    let data = b"data";
    storage
        .put(
            "file.txt".to_string(),
            &mut std::io::Cursor::new(data),
            Some(data.len() as u64),
        )
        .await?;

    // Read from primary
    let mut buf = Vec::new();
    storage.get_into(&"file.txt".to_string(), &mut buf).await?;
    assert_eq!(buf, b"data");

    println!("Wrote to 3 backends with quorum strategy, read from primary");
    println!();
    Ok(())
}

/// Example 2: Fallback Storage - writes to both, reads from primary
async fn example_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Fallback Storage ---");

    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    let storage = FallbackStorage::new(primary, secondary).with_write_through(true);

    // Write goes to both primary and secondary
    let data = b"replicated data";
    storage
        .put(
            "file.txt".to_string(),
            &mut std::io::Cursor::new(data),
            Some(data.len() as u64),
        )
        .await?;

    // Verify it's in both backends
    assert!(storage.primary().exists(&"file.txt".to_string()).await?);
    assert!(storage.secondary().exists(&"file.txt".to_string()).await?);

    // Read from primary
    let mut buf = Vec::new();
    storage.get_into(&"file.txt".to_string(), &mut buf).await?;
    assert_eq!(buf, b"replicated data");

    println!("Writes to both primary and secondary, reads from primary");
    println!();
    Ok(())
}
