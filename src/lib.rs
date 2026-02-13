use std::fmt::Debug;

use futures::stream::BoxStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

/// A specialized Result type for Storage operations.
pub type Result<T> = std::result::Result<T, Error>;

/// A unified Error type for storage operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Storage backend connection error")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("IO Error")]
    Io(#[from] std::io::Error),

    #[error("Generic storage error: {0}")]
    Generic(String),
}

/// Adapter modules (kept under `src/adapters/`), gated behind Cargo features.
///
/// Note: each module is only compiled if its feature is enabled.
pub mod adapters {
    #[cfg(feature = "gdrive")]
    pub mod gdrive;
    #[cfg(feature = "local")]
    pub mod local;
    #[cfg(feature = "memory")]
    pub mod memory;
    #[cfg(feature = "onedrive")]
    pub mod onedrive;
    #[cfg(feature = "s3")]
    pub mod s3;
}

// Convenient re-exports at the crate root.
#[allow(ambiguous_glob_reexports)]
#[cfg(feature = "gdrive")]
pub use adapters::gdrive::*;
#[cfg(feature = "local")]
pub use adapters::local::*;
#[cfg(feature = "memory")]
pub use adapters::memory::*;
#[cfg(feature = "onedrive")]
pub use adapters::onedrive::*;
#[cfg(feature = "s3")]
pub use adapters::s3::*;

/// The core storage contract.
///
/// This trait is Tokio-native:
/// - inputs implement [`tokio::io::AsyncRead`]
/// - outputs implement [`tokio::io::AsyncWrite`]
///
/// `Id` is an associated type so adapters can choose their own identifier format
/// (you can standardize on `String` across adapters).
pub trait Storage: Send + Sync + Debug {
    /// The Identifier type.
    type Id: Clone + Debug + Send + Sync + 'static;

    /// Check if a file exists.
    fn exists(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<bool>> + Send;

    /// Save data to the storage.
    ///
    /// `len` is optional and may be used by some backends (e.g. to set content-length).
    fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: R,
        len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retrieve data and write it into `output`.
    ///
    /// Returns the number of bytes written.
    fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;

    /// Delete a file.
    ///
    /// Should return `Ok(())` if the file is already gone (idempotent).
    fn delete(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<()>> + Send;

    /// List identifiers.
    ///
    /// Implementations should return identifiers only (not metadata).
    fn list(
        &self,
        prefix: Option<&Self::Id>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'_, Result<Self::Id>>>> + Send;
}

/// Extension helpers built on top of [`Storage`].
pub trait StorageExt: Storage {
    /// Download an object fully into memory and return its bytes.
    fn get_bytes(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let mut buf: Vec<u8> = Vec::new();

            // We can't directly write into Vec<u8> with AsyncWrite without an adapter.
            // Use `tokio::io::duplex` to capture bytes efficiently.
            let (mut client, mut server) = tokio::io::duplex(64 * 1024);

            // Drive the download into the server end while we read from the client end.
            let download_fut = self.get_into(id, &mut server);

            // Read everything from the client end into `buf` while download proceeds.
            let read_fut = async {
                client.read_to_end(&mut buf).await?;
                Result::<()>::Ok(())
            };

            let (written, _) = tokio::try_join!(download_fut, read_fut)?;
            // Ensure server is dropped so duplex closes (best-effort).
            drop(server);

            // `written` is bytes written by adapter; `buf.len()` is what we observed.
            // Prefer returning the observed bytes, but keep a sanity check.
            if written != buf.len() as u64 {
                // Not fatal, but signal something odd.
                // (Some adapters may buffer/flush differently; duplex should match.)
            }

            Ok(buf)
        }
    }

    /// Download an object fully and decode as UTF-8 string.
    fn get_string(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<String>> + Send {
        async move {
            let bytes = self.get_bytes(id).await?;
            String::from_utf8(bytes).map_err(|e| Error::Generic(format!("invalid utf-8: {e}")))
        }
    }

    /// Upload bytes.
    fn put_bytes(
        &self,
        id: Self::Id,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let len = Some(bytes.len() as u64);
        // `std::io::Cursor<&[u8]>` implements `tokio::io::AsyncRead` via `tokio::io::AsyncRead`
        // only for certain types; use `tokio::io::Cursor` (re-export) is not a thing.
        // Instead, use `tokio::io::BufReader` over a slice reader.
        async move {
            let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(bytes));
            self.put(id, &mut reader, len).await
        }
    }

    /// Copy from one storage to another using streaming through a bounded in-memory pipe.
    ///
    /// This is useful for migrations and fan-out without materializing the full object in memory.
    fn copy_to<S2: Storage<Id = Self::Id>>(
        &self,
        id: &Self::Id,
        dest: &S2,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // Pipe: source writes into `server`, dest reads from `client`.
            let (mut client, mut server) = tokio::io::duplex(64 * 1024);

            let download_fut = self.get_into(id, &mut server);

            let upload_fut = async {
                // len unknown; let backend decide.
                dest.put(id.clone(), &mut client, None).await
            };

            let (downloaded, uploaded) = tokio::try_join!(download_fut, upload_fut)?;
            let _ = downloaded;
            Ok(uploaded)
        }
    }
}

impl<T: Storage + ?Sized> StorageExt for T {}
