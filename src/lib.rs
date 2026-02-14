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

    #[error("Mirror operation failed: {success_count} succeeded, {failure_count} failed")]
    MirrorFailure {
        success_count: usize,
        failure_count: usize,
        /// Indices of backends that succeeded
        successes: Vec<usize>,
        /// Indices and error messages of backends that failed
        failures: Vec<(usize, String)>,
    },
}

/// Adapter modules, gated behind Cargo features.
pub mod adapters {
    #[cfg(feature = "azure")]
    pub mod azure;
    #[cfg(feature = "box_storage")]
    pub mod box_storage;
    #[cfg(feature = "dropbox")]
    pub mod dropbox;
    #[cfg(feature = "ftp")]
    pub mod ftp;
    #[cfg(feature = "gdrive")]
    pub mod gdrive;
    #[cfg(feature = "local")]
    pub mod local;
    #[cfg(feature = "memory")]
    pub mod memory;
    pub mod multi;
    #[cfg(feature = "onedrive")]
    pub mod onedrive;
    #[cfg(feature = "s3")]
    pub mod s3;
    #[cfg(feature = "sftp")]
    pub mod sftp;
    #[cfg(feature = "webdav")]
    pub mod webdav;
}

// Convenient re-exports at the crate root.
#[allow(ambiguous_glob_reexports)]
#[cfg(feature = "azure")]
pub use adapters::azure::*;
#[cfg(feature = "box_storage")]
pub use adapters::box_storage::*;
#[cfg(feature = "dropbox")]
pub use adapters::dropbox::*;
#[cfg(feature = "ftp")]
pub use adapters::ftp::*;
#[cfg(feature = "gdrive")]
pub use adapters::gdrive::*;
#[cfg(feature = "local")]
pub use adapters::local::*;
#[cfg(feature = "memory")]
pub use adapters::memory::*;
pub use adapters::multi;
#[cfg(feature = "onedrive")]
pub use adapters::onedrive::*;
#[cfg(feature = "s3")]
pub use adapters::s3::*;
#[cfg(feature = "sftp")]
pub use adapters::sftp::*;
#[cfg(feature = "webdav")]
pub use adapters::webdav::*;

/// The core storage trait.
///
/// Uses Tokio's [`AsyncRead`] and [`AsyncWrite`].
/// `Id` is an associated type allowing adapters to choose their identifier format.
///
/// ## Identifier Types
/// - **Path-based**: Local, S3, Azure, WebDAV, SFTP, FTP, Dropbox use string paths
/// - **ID-based**: Google Drive, OneDrive, Box use native item IDs
///
/// For ID-based adapters, you must resolve paths to IDs using the native API before
/// calling storage methods.
pub trait Storage: Send + Sync + Debug {
    /// The identifier type for this storage backend.
    type Id: Clone + Debug + Send + Sync + 'static;

    /// Check if an item exists.
    fn exists(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<bool>> + Send;

    /// Check if a folder exists.
    ///
    /// **Path-based backends** (Local, S3, Azure, WebDAV, SFTP, FTP, Dropbox):
    /// Pass the folder path as a string.
    ///
    /// **ID-based backends** (Google Drive, OneDrive, Box):
    /// Pass the folder's ID. To resolve a path to an ID, use the adapter's native API
    /// or search functionality.
    fn folder_exists(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<bool>> + Send;

    /// Store data. `len` is optional and may be used by some backends.
    fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: R,
        len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retrieve data and write to `output`. Returns bytes written.
    fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;

    /// Delete an item. Idempotent (returns `Ok(())` if already deleted).
    fn delete(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<()>> + Send;

    /// List identifiers matching an optional prefix.
    fn list(
        &self,
        prefix: Option<&Self::Id>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'_, Result<Self::Id>>>> + Send;
}

/// Convenience methods built on [`Storage`].
pub trait StorageExt: Storage {
    /// Download an item into memory as bytes.
    fn get_bytes(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let mut buf: Vec<u8> = Vec::new();
            let (mut client, mut server) = tokio::io::duplex(64 * 1024);

            let download_fut = self.get_into(id, &mut server);
            let read_fut = async {
                client.read_to_end(&mut buf).await?;
                Result::<()>::Ok(())
            };

            let (_written, _) = tokio::try_join!(download_fut, read_fut)?;
            drop(server);

            Ok(buf)
        }
    }

    /// Download an item as a UTF-8 string.
    fn get_string(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<String>> + Send {
        async move {
            let bytes = self.get_bytes(id).await?;
            String::from_utf8(bytes).map_err(|e| Error::Generic(format!("invalid utf-8: {e}")))
        }
    }

    /// Upload a byte slice.
    fn put_bytes(
        &self,
        id: Self::Id,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let len = Some(bytes.len() as u64);
        async move {
            let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(bytes));
            self.put(id, &mut reader, len).await
        }
    }

    /// Copy an item from this storage to another via streaming.
    fn copy_to<S2: Storage<Id = Self::Id>>(
        &self,
        id: &Self::Id,
        dest: &S2,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let (mut client, mut server) = tokio::io::duplex(64 * 1024);

            let download_fut = self.get_into(id, &mut server);

            let upload_fut = async { dest.put(id.clone(), &mut client, None).await };

            let (downloaded, uploaded) = tokio::try_join!(download_fut, upload_fut)?;
            let _ = downloaded;
            Ok(uploaded)
        }
    }
}

impl<T: Storage + ?Sized> StorageExt for T {}
