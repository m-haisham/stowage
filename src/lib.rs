use std::fmt::Debug;

use futures::stream::BoxStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

#[cfg(feature = "memory")]
pub use adapters::memory::MemoryStorage;

pub use adapters::multi::migration::{ConflictStrategy, MigrateOptions, MigrationResult};

pub use adapters::multi;

/// A specialized Result type for Storage operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Details about a mirror operation failure.
///
/// Contains backend indices and full error objects for successes/failures,
/// plus any rollback errors if rollback was attempted.
///
/// ```
/// # use stowage::{Error, MirrorFailureDetails, StorageExt};
/// # use stowage::multi::{MirrorStorage, WriteStrategy};
/// # use stowage::MemoryStorage;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let storage = MirrorStorage::builder()
/// #     .add_backend(MemoryStorage::new())
/// #     .add_backend(MemoryStorage::new())
/// #     .write_strategy(WriteStrategy::AllOrFail { rollback: true })
/// #     .build();
/// match storage.put_bytes("file.txt".to_string(), b"data").await {
///     Err(Error::MirrorFailure(details)) => {
///         println!("{} of {} failed", details.failure_count(), details.total_backends());
///         for (idx, error) in &details.failures {
///             eprintln!("Backend {}: {:?}", idx, error);
///         }
///     }
///     _ => {}
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MirrorFailureDetails {
    /// Indices of backends that succeeded
    pub successes: Vec<usize>,
    /// Indices and errors of backends that failed
    pub failures: Vec<(usize, Box<Error>)>,
    /// Errors that occurred during rollback (if any)
    pub rollback_errors: Vec<(usize, Box<Error>)>,
}

impl MirrorFailureDetails {
    /// Total number of backends involved
    pub fn total_backends(&self) -> usize {
        self.successes.len() + self.failures.len()
    }

    /// Number of successful backends
    pub fn success_count(&self) -> usize {
        self.successes.len()
    }

    /// Number of failed backends
    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }

    /// Check if any backends succeeded
    pub fn has_successes(&self) -> bool {
        !self.successes.is_empty()
    }

    /// Check if any backends failed
    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    /// Check if rollback was attempted and had errors
    pub fn has_rollback_errors(&self) -> bool {
        !self.rollback_errors.is_empty()
    }

    /// Get indices of all failed backends
    pub fn failed_indices(&self) -> Vec<usize> {
        self.failures.iter().map(|(idx, _)| *idx).collect()
    }

    /// Get indices of all successful backends
    pub fn successful_indices(&self) -> &[usize] {
        &self.successes
    }
}

impl std::fmt::Display for MirrorFailureDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Mirror operation failed: {} succeeded, {} failed",
            self.success_count(),
            self.failure_count()
        )?;
        if self.has_rollback_errors() {
            write!(f, ", {} rollback errors", self.rollback_errors.len())?;
        }
        Ok(())
    }
}

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

    #[error("{0}")]
    MirrorFailure(MirrorFailureDetails),
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

            let download_fut = async {
                let result = self.get_into(id, &mut server).await;
                drop(server);
                result
            };

            let read_fut = async {
                client.read_to_end(&mut buf).await?;
                Result::<()>::Ok(())
            };

            let (_written, _) = tokio::try_join!(download_fut, read_fut)?;

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

            let download_fut = async {
                let result = self.get_into(id, &mut server).await;
                drop(server); // Close server end so client's read_to_end can complete
                result
            };

            let upload_fut = async { dest.put(id.clone(), &mut client, None).await };

            let (downloaded, uploaded) = tokio::try_join!(download_fut, upload_fut)?;
            let _ = downloaded;
            Ok(uploaded)
        }
    }

    /// Move an item from this storage to another by copying then deleting the source.
    ///
    /// This is a streaming copy followed by a source delete.  If the copy
    /// fails the source item is left untouched.  If the delete fails after a
    /// successful copy, the error is returned — at that point the item exists
    /// in both storages and the caller should handle the duplicate.
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[cfg(feature = "memory")]
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use stowage::{MemoryStorage, Storage, StorageExt};
    ///
    /// let source = MemoryStorage::new();
    /// let dest   = MemoryStorage::new();
    ///
    /// source.put_bytes("file.txt".to_string(), b"hello").await?;
    ///
    /// source.move_to(&"file.txt".to_string(), &dest).await?;
    ///
    /// assert!(!source.exists(&"file.txt".to_string()).await?);
    /// assert!(dest.exists(&"file.txt".to_string()).await?);
    /// # Ok(())
    /// # }
    /// ```
    fn move_to<S2: Storage<Id = Self::Id>>(
        &self,
        id: &Self::Id,
        dest: &S2,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            self.copy_to(id, dest).await?;
            self.delete(id).await
        }
    }

    /// Migrate all items from this storage to `dest`.
    ///
    /// This is a convenience wrapper around [`multi::migration::migrate`].  See
    /// [`MigrateOptions`] for the full set of knobs (prefix filtering, conflict
    /// strategy, concurrency, and optional source deletion).
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[cfg(feature = "memory")]
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use stowage::{MemoryStorage, StorageExt, MigrateOptions, ConflictStrategy};
    ///
    /// let source = MemoryStorage::new();
    /// let dest   = MemoryStorage::new();
    ///
    /// source.put_bytes("a.txt".to_string(), b"hello").await?;
    /// source.put_bytes("b.txt".to_string(), b"world").await?;
    ///
    /// let result = source
    ///     .migrate_to(&dest, MigrateOptions {
    ///         conflict: ConflictStrategy::Skip,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// assert_eq!(result.transferred_count(), 2);
    /// assert!(result.is_complete());
    /// # Ok(())
    /// # }
    /// ```
    fn migrate_to<S2: Storage<Id = Self::Id>>(
        &self,
        dest: &S2,
        options: MigrateOptions<Self::Id>,
    ) -> impl std::future::Future<Output = Result<MigrationResult<Self::Id>>> + Send
    where
        Self: Sized,
    {
        adapters::multi::migration::migrate(self, dest, options)
    }
}

impl<T: Storage + ?Sized> StorageExt for T {}
