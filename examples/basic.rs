//! Basic CRUD operations using [`MemoryStorage`].
//!
//! Run with:
//! ```sh
//! cargo run --example basic --features="memory"
//! ```

use futures::StreamExt as _;
use stowage::{MemoryStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = MemoryStorage::new();

    // Write
    storage
        .put_bytes("hello.txt".to_string(), b"Hello, World!")
        .await?;

    // Read
    let content = storage.get_string(&"hello.txt".to_string()).await?;
    println!("hello.txt: {content}");

    // Add a second file so listing is more interesting.
    storage
        .put_bytes("world.txt".to_string(), b"Goodbye, World!")
        .await?;

    // List
    let mut files = Storage::list(&storage, None).await?;
    println!("Files:");
    while let Some(id) = files.next().await {
        println!("  {}", id?);
    }

    // Delete
    storage.delete(&"hello.txt".to_string()).await?;
    println!("Deleted hello.txt");
    println!(
        "Exists: {}",
        storage.exists(&"hello.txt".to_string()).await?
    );

    Ok(())
}
