# Stowage

A unified async storage abstraction for Rust supporting multiple backends.

## Features

A single `Storage` trait works across all backends:

### File Transfer Protocols
- **SFTP** - SSH File Transfer Protocol (`sftp` feature)
- **FTP** - File Transfer Protocol (`ftp` feature)

### Cloud Storage
- **AWS S3** - Amazon S3 and S3-compatible services (`s3` feature)
- **Azure Blob Storage** (`azure` feature)
- **Google Drive** (`gdrive` feature)
- **Microsoft OneDrive** (`onedrive` feature)
- **Dropbox** (`dropbox` feature)
- **Box.com** (`box_storage` feature)

### Network Storage
- **WebDAV** - WebDAV protocol for Nextcloud, ownCloud, etc. (`webdav` feature)

### Local Storage
- **Local filesystem** (`local` feature)
- **In-memory storage** (`memory` feature)

## Installation

Add stowage to your `Cargo.toml`:

```toml
[dependencies]
stowage = { version = "0.1", features = ["sftp", "ftp"] }
```



## Usage

### SFTP Storage

```rust
use stowage::{SftpStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to SFTP server
    let storage = SftpStorage::new(
        "sftp.example.com:22",
        "username",
        "password",
        Some("/home/user/uploads"), // Optional base path
    ).await?;

    // Upload a file
    let data = b"Hello, SFTP!";
    storage.put_bytes("test.txt".to_string(), data).await?;

    // Download a file
    let content = storage.get_string(&"test.txt".to_string()).await?;
    println!("File content: {}", content);

    // Check if file exists
    if storage.exists(&"test.txt".to_string()).await? {
        println!("File exists!");
    }

    // Check if folder exists
    if storage.folder_exists(&"uploads".to_string()).await? {
        println!("Folder exists!");
    }

    // List files
    let files = storage.list(None).await?;
    use futures::stream::StreamExt;
    let files: Vec<_> = files.collect().await;
    println!("Files: {:?}", files);

    // Delete a file
    storage.delete(&"test.txt".to_string()).await?;

    Ok(())
}
```

### FTP Storage

```rust
use stowage::{FtpStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to FTP server
    let storage = FtpStorage::new(
        "ftp.example.com:21",
        "username",
        "password",
        Some("/uploads".into()), // Optional base path
    ).await?;

    // Upload a file
    storage.put_bytes("document.pdf".to_string(), &pdf_data).await?;

    // Download a file
    let bytes = storage.get_bytes(&"document.pdf".to_string()).await?;

    // Stream upload from a file
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    
    let mut file = File::open("large-file.bin").await?;
    let file_size = file.metadata().await?.len();
    storage.put("remote.bin".to_string(), file, Some(file_size)).await?;

    Ok(())
}
```

### Local Filesystem

```rust
use stowage::{LocalStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = LocalStorage::new("/path/to/storage");

    storage.put_bytes("file.txt".to_string(), b"Hello, world!").await?;
    let content = storage.get_string(&"file.txt".to_string()).await?;

    Ok(())
}
```

### AWS S3

```rust
use stowage::{S3Storage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = S3Storage::new("my-bucket", "us-east-1").await?;

    storage.put_bytes("data/file.json".to_string(), b"{}").await?;
    let data = storage.get_bytes(&"data/file.json".to_string()).await?;

    Ok(())
}
```

### WebDAV (Nextcloud, ownCloud, etc.)

```rust
use stowage::{WebDAVStorage, Storage, StorageExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WebDAVStorage::new(
        "https://cloud.example.com/remote.php/dav/files/username",
        "username",
        "password",
    );

    storage.put_bytes("Documents/notes.txt".to_string(), b"My notes").await?;

    Ok(())
}
```

## Core Traits

### Storage

All adapters implement the `Storage` trait with methods for:
- `exists` - Check if an item exists
- `folder_exists` - Check if a folder/directory exists
- `put` - Store data from an `AsyncRead` stream
- `get_into` - Retrieve data to an `AsyncWrite` stream
- `delete` - Remove an item
- `list` - List items with optional prefix filtering

### Path-Based vs ID-Based Adapters

**Path-based adapters** use string paths as identifiers:
- Local, S3, Azure, WebDAV, SFTP, FTP, Dropbox

**ID-based adapters** use native item IDs:
- Google Drive, OneDrive, Box

For ID-based adapters, you must resolve paths to IDs before calling storage methods. 
Each adapter provides helper methods:

```rust
// Google Drive - find folder by name
let folder_id = storage.find_folder_by_name("Documents", None).await?;
if let Some(id) = folder_id {
    if storage.folder_exists(&id).await? {
        println!("Folder exists!");
    }
}

// OneDrive - get folder ID by path
let folder_id = storage.get_folder_id_by_path("Documents/Work").await?;
if storage.folder_exists(&folder_id).await? {
    println!("Folder exists!");
}

// Box - find folder by name in parent folder
let folder_id = storage.find_folder_by_name("Projects").await?;
if let Some(id) = folder_id {
    if storage.folder_exists(&id).await? {
        println!("Folder exists!");
    }
}
```

### StorageExt

Convenience methods built on `Storage`:
- `get_bytes` - Download as `Vec<u8>`
- `get_string` - Download as UTF-8 string
- `put_bytes` - Upload from byte slice
- `copy_to` - Copy between storage backends

## Security

Sensitive fields like passwords and tokens are protected using the `secrecy` crate and will not appear in debug output.

## Error Handling

All operations return `stowage::Result<T>` with a unified `Error` type.

## License

This project is licensed under the MIT License.
