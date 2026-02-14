//! Basic example showing simple storage operations.
//!
//! Run with:
//! ```sh
//! cargo run --example basic --features="memory"
//! ```

use futures::StreamExt;
use stowage::{MemoryStorage, Storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Basic Storage Example ===\n");

    // Create a simple in-memory storage
    let storage = MemoryStorage::new();

    // Write some data
    let data = b"Hello, World!";
    storage
        .put(
            "hello.txt".to_string(),
            &mut std::io::Cursor::new(data),
            Some(data.len() as u64),
        )
        .await?;
    println!("Wrote hello.txt");

    // Check if file exists
    if storage.exists(&"hello.txt".to_string()).await? {
        println!("File exists");
    }

    // Read the data back
    let mut buf = Vec::new();
    let bytes_read = storage.get_into(&"hello.txt".to_string(), &mut buf).await?;
    println!(
        "Read back {} bytes: {}",
        bytes_read,
        String::from_utf8_lossy(&buf)
    );

    // List all files
    let mut files = storage.list(None).await?;
    println!("Files in storage:");
    while let Some(entry) = files.next().await {
        let id = entry?;
        println!("  - {}", id);
    }

    // Delete the file
    storage.delete(&"hello.txt".to_string()).await?;
    println!("Deleted hello.txt");

    // Verify deletion
    assert!(!storage.exists(&"hello.txt".to_string()).await?);
    println!("File no longer exists");

    println!("\n=== Done! ===");
    Ok(())
}
