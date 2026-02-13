//! Example demonstrating SFTP storage adapter usage.
//!
//! Run with:
//! ```
//! cargo run --example sftp_example --features sftp
//! ```
//!
//! Set environment variables:
//! - SFTP_HOST: SFTP server address (e.g., "sftp.example.com:22")
//! - SFTP_USER: Username
//! - SFTP_PASS: Password
//! - SFTP_PATH: Optional base path (e.g., "/uploads")

use std::env;
use stowage::{SftpStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read configuration from environment variables
    let host = env::var("SFTP_HOST").unwrap_or_else(|_| "localhost:22".to_string());
    let user = env::var("SFTP_USER").expect("SFTP_USER environment variable not set");
    let pass = env::var("SFTP_PASS").expect("SFTP_PASS environment variable not set");
    let base_path: Option<std::path::PathBuf> = env::var("SFTP_PATH").ok().map(|p| p.into());

    println!("Connecting to SFTP server: {}", host);

    // Connect to SFTP server
    let storage = SftpStorage::new(host, user, pass, base_path).await?;

    println!("✓ Connected successfully");

    // Example 1: Upload a text file
    println!("\n1. Uploading text file...");
    let test_content = b"Hello from Stowage SFTP adapter!";
    storage
        .put_bytes("test.txt".to_string(), test_content)
        .await?;
    println!("✓ Uploaded test.txt");

    // Example 2: Check if file exists
    println!("\n2. Checking if file exists...");
    let exists = storage.exists(&"test.txt".to_string()).await?;
    println!("✓ File exists: {}", exists);

    // Example 3: Download and read file
    println!("\n3. Downloading file...");
    let downloaded = storage.get_string(&"test.txt".to_string()).await?;
    println!("✓ Downloaded content: {}", downloaded);

    // Example 4: Upload binary data
    println!("\n4. Uploading binary file...");
    let binary_data: Vec<u8> = (0..100).collect();
    storage
        .put_bytes("binary.dat".to_string(), &binary_data)
        .await?;
    println!("✓ Uploaded binary.dat ({} bytes)", binary_data.len());

    // Example 5: List files
    println!("\n5. Listing files...");
    let files = storage.list(None).await?;
    use futures::stream::StreamExt;
    let file_list: Vec<_> = files.collect::<Vec<_>>().await;
    println!("✓ Found {} files:", file_list.len());
    for (i, file) in file_list.iter().enumerate() {
        match file {
            Ok(name) => println!("  {}. {}", i + 1, name),
            Err(e) => println!("  {}. Error: {}", i + 1, e),
        }
    }

    // Example 6: Download binary file
    println!("\n6. Downloading binary file...");
    let downloaded_binary = storage.get_bytes(&"binary.dat".to_string()).await?;
    println!(
        "✓ Downloaded {} bytes, matches: {}",
        downloaded_binary.len(),
        downloaded_binary == binary_data
    );

    // Example 7: Delete files
    println!("\n7. Cleaning up (deleting files)...");
    storage.delete(&"test.txt".to_string()).await?;
    println!("✓ Deleted test.txt");

    storage.delete(&"binary.dat".to_string()).await?;
    println!("✓ Deleted binary.dat");

    // Example 8: Verify deletion (idempotent)
    println!("\n8. Verifying deletion...");
    let exists = storage.exists(&"test.txt".to_string()).await?;
    println!("✓ File exists after deletion: {}", exists);

    // Deleting again should succeed (idempotent)
    storage.delete(&"test.txt".to_string()).await?;
    println!("✓ Second delete succeeded (idempotent)");

    println!("\n✅ All SFTP operations completed successfully!");

    Ok(())
}
