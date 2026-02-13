# Stowage

A unified, async storage abstraction for Rust with support for multiple backends including local filesystem, cloud storage, and file transfer protocols.

## Features

Stowage provides a single `Storage` trait that works across multiple storage backends:

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

Or enable specific features:

```toml
[dependencies]
stowage = { version = "0.1", features = ["sftp", "ftp", "s3", "local"] }
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

## The Storage Trait

All adapters implement the `Storage` trait:

```rust
pub trait Storage: Send + Sync + Debug {
    type Id: Clone + Debug + Send + Sync + 'static;

    async fn exists(&self, id: &Self::Id) -> Result<bool>;
    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: R,
        len: Option<u64>,
    ) -> Result<()>;
    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> Result<u64>;
    async fn delete(&self, id: &Self::Id) -> Result<()>;
    async fn list(
        &self,
        prefix: Option<&Self::Id>,
    ) -> Result<BoxStream<'_, Result<Self::Id>>>;
}
```

## StorageExt Helpers

The `StorageExt` trait provides convenient helper methods:

- `get_bytes(&self, id)` - Download file as `Vec<u8>`
- `get_string(&self, id)` - Download and decode as UTF-8 string
- `put_bytes(&self, id, bytes)` - Upload from byte slice
- `copy_to(&self, id, dest)` - Copy between storage backends

## Security

Sensitive fields like passwords and tokens are protected using the `secrecy` crate and will not appear in debug output.

## Error Handling

All operations return `stowage::Result<T>` with a unified `Error` type:

- `Error::NotFound` - File not found
- `Error::PermissionDenied` - Authentication or authorization failure
- `Error::Connection` - Network/connection errors
- `Error::Io` - I/O errors
- `Error::Generic` - Other errors with context

## Protocol-Specific Notes

### SFTP
- Uses SSH2 library for secure file transfers
- Supports password authentication (key-based auth can be added)
- Thread-safe with connection pooling via `Arc<Mutex<Session>>`
- Automatically creates parent directories for uploads

### FTP
- Uses suppaftp library with async support
- Currently supports basic FTP (FTPS/TLS support can be added)
- Binary transfer mode enabled by default
- Idempotent operations (delete returns success if file doesn't exist)

## License

This project is licensed under the MIT License.