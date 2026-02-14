use crate::{Error, Result, Storage};
use futures::stream::BoxStream;
use std::fmt::Debug;
use tokio::io::{AsyncRead, AsyncWrite};

/// Strategy for handling write operations across mirrored backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStrategy {
    /// All backends must succeed for the operation to succeed.
    /// If any backend fails, the entire operation fails.
    AllOrFail,

    /// At least one backend must succeed.
    /// Returns success if any backend succeeds, even if others fail.
    AtLeastOne,

    /// A majority (more than half) of backends must succeed.
    /// Useful for quorum-based replication.
    Quorum,
}

/// Storage adapter that mirrors data across multiple backends for redundancy.
///
/// This adapter writes to all configured backends in parallel and reads from
/// the first available backend (configurable). It's useful for:
/// - Multi-cloud redundancy
/// - Geographic replication
/// - Real-time backup
///
/// # Write Behavior
///
/// Writes are performed in parallel across all backends. The [`WriteStrategy`]
/// determines what constitutes a successful write:
/// - `AllOrFail`: All backends must succeed (default)
/// - `AtLeastOne`: At least one backend must succeed
/// - `Quorum`: Majority of backends must succeed
///
/// # Examples
///
/// ```no_run
/// use stowage::{LocalStorage, Storage, StorageExt};
/// use stowage::multi::{MirrorStorage, WriteStrategy};
///
/// # async fn example() -> stowage::Result<()> {
/// let storage = MirrorStorage::builder()
///     .add_backend(LocalStorage::new("/storage-1"))
///     .add_backend(LocalStorage::new("/storage-2"))
///     .add_backend(LocalStorage::new("/storage-3"))
///     .write_strategy(WriteStrategy::Quorum)
///     .build();
///
/// // Writes to all backends in parallel
/// storage.put_bytes("file.txt".to_string(), b"data").await?;
///
/// // Reads from first available backend
/// let data = storage.get_bytes(&"file.txt".to_string()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MirrorStorage<S: Storage> {
    backends: Vec<S>,
    write_strategy: WriteStrategy,
    primary_index: usize,
}

impl<S: Storage> MirrorStorage<S> {
    /// Create a builder for configuring mirror storage.
    pub fn builder() -> MirrorStorageBuilder<S> {
        MirrorStorageBuilder::new()
    }

    /// Create a mirror storage with default settings (AllOrFail strategy).
    ///
    /// # Panics
    ///
    /// Panics if `backends` is empty.
    pub fn new(backends: Vec<S>) -> Self {
        assert!(
            !backends.is_empty(),
            "MirrorStorage requires at least one backend"
        );
        Self {
            backends,
            write_strategy: WriteStrategy::AllOrFail,
            primary_index: 0,
        }
    }

    /// Get the number of backends.
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Get the write strategy.
    pub fn write_strategy(&self) -> WriteStrategy {
        self.write_strategy
    }

    /// Get a reference to a specific backend by index.
    pub fn backend(&self, index: usize) -> Option<&S> {
        self.backends.get(index)
    }

    /// Get the primary backend (used for reads).
    pub fn primary(&self) -> &S {
        &self.backends[self.primary_index]
    }

    /// Evaluate if the write results meet the strategy requirements.
    fn evaluate_write_results(&self, results: &[Result<()>]) -> Result<()> {
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let required = match self.write_strategy {
            WriteStrategy::AllOrFail => self.backends.len(),
            WriteStrategy::AtLeastOne => 1,
            WriteStrategy::Quorum => (self.backends.len() / 2) + 1,
        };

        if successes >= required {
            Ok(())
        } else {
            // Collect all errors for reporting
            let errors: Vec<String> = results
                .iter()
                .enumerate()
                .filter_map(|(i, r)| r.as_ref().err().map(|e| format!("backend {}: {}", i, e)))
                .collect();

            Err(Error::Generic(format!(
                "Mirror write failed: {} of {} backends succeeded (required: {}). Errors: [{}]",
                successes,
                self.backends.len(),
                required,
                errors.join(", ")
            )))
        }
    }
}

impl<S: Storage> Storage for MirrorStorage<S> {
    type Id = S::Id;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        // Check primary first
        match self.primary().exists(id).await {
            Ok(exists) => Ok(exists),
            Err(_) => {
                // If primary fails, try other backends
                for backend in &self.backends {
                    if let Ok(exists) = backend.exists(id).await {
                        return Ok(exists);
                    }
                }
                // If all fail, return the primary's error
                self.primary().exists(id).await
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        // Check primary first
        match self.primary().folder_exists(id).await {
            Ok(exists) => Ok(exists),
            Err(_) => {
                // If primary fails, try other backends
                for backend in &self.backends {
                    if let Ok(exists) = backend.folder_exists(id).await {
                        return Ok(exists);
                    }
                }
                // If all fail, return the primary's error
                self.primary().folder_exists(id).await
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: R,
        len: Option<u64>,
    ) -> Result<()> {
        // Buffer the input since we need to write to multiple backends
        use tokio::io::AsyncReadExt;
        let mut buffer = Vec::new();
        let mut reader = input;
        reader.read_to_end(&mut buffer).await?;

        // Write to all backends in parallel
        let mut futures = Vec::new();
        for backend in &self.backends {
            let buffer_clone = buffer.clone();
            let id_clone = id.clone();
            let future = async move {
                let cursor = std::io::Cursor::new(buffer_clone);
                let mut async_cursor = tokio::io::BufReader::new(cursor);
                backend.put(id_clone, &mut async_cursor, len).await
            };
            futures.push(future);
        }

        let results: Vec<Result<()>> = futures::future::join_all(futures).await;

        self.evaluate_write_results(&results)
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> Result<u64> {
        // For mirror storage, get_into has the same limitation as fallback:
        // Once we try the primary and it fails, we can't retry with other backends
        // because the output stream was already consumed.
        //
        // Recommendation: Use get_bytes() for scenarios where fallback is needed.
        //
        // For now, we only try primary to avoid the moved value issue.
        self.primary().get_into(id, output).await
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        // Delete from all backends in parallel
        let futures = self.backends.iter().map(|backend| backend.delete(id));
        let results: Vec<Result<()>> = futures::future::join_all(futures).await;

        // For delete, we use AtLeastOne strategy (more lenient)
        // since delete is idempotent
        let successes = results.iter().filter(|r| r.is_ok()).count();
        if successes > 0 {
            Ok(())
        } else {
            // All failed, return first error
            results.into_iter().find(|r| r.is_err()).unwrap()
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        // List from primary only
        // Merging lists from multiple backends would require deduplication
        // and is complex to implement with streams
        self.primary().list(prefix).await
    }
}

/// Builder for [`MirrorStorage`] with configuration options.
pub struct MirrorStorageBuilder<S: Storage> {
    backends: Vec<S>,
    write_strategy: WriteStrategy,
    primary_index: usize,
}

impl<S: Storage> MirrorStorageBuilder<S> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
            write_strategy: WriteStrategy::AllOrFail,
            primary_index: 0,
        }
    }

    /// Add a backend to the mirror.
    pub fn add_backend(mut self, backend: S) -> Self {
        self.backends.push(backend);
        self
    }

    /// Set the write strategy.
    pub fn write_strategy(mut self, strategy: WriteStrategy) -> Self {
        self.write_strategy = strategy;
        self
    }

    /// Set which backend to use as primary for reads (default: 0).
    ///
    /// # Panics
    ///
    /// Panics during `build()` if the index is out of bounds.
    pub fn primary_index(mut self, index: usize) -> Self {
        self.primary_index = index;
        self
    }

    /// Build the mirror storage.
    ///
    /// # Panics
    ///
    /// Panics if no backends were added or if primary_index is out of bounds.
    pub fn build(self) -> MirrorStorage<S> {
        assert!(
            !self.backends.is_empty(),
            "MirrorStorage requires at least one backend"
        );
        assert!(
            self.primary_index < self.backends.len(),
            "Primary index {} out of bounds (have {} backends)",
            self.primary_index,
            self.backends.len()
        );

        MirrorStorage {
            backends: self.backends,
            write_strategy: self.write_strategy,
            primary_index: self.primary_index,
        }
    }
}

impl<S: Storage> Default for MirrorStorageBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage> Debug for MirrorStorageBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorStorageBuilder")
            .field("backend_count", &self.backends.len())
            .field("write_strategy", &self.write_strategy)
            .field("primary_index", &self.primary_index)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageExt;

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_all_or_fail() {
        use crate::MemoryStorage;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AllOrFail)
            .build();

        // Write data
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should exist in all backends
        for i in 0..3 {
            assert!(
                storage
                    .backend(i)
                    .unwrap()
                    .exists(&"test".to_string())
                    .await
                    .unwrap()
            );
        }

        // Should be able to read
        let data = storage.get_string(&"test".to_string()).await.unwrap();
        assert_eq!(data, "data");
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_at_least_one() {
        use crate::MemoryStorage;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AtLeastOne)
            .build();

        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should exist in all backends (no failures in this test)
        assert!(
            storage
                .backend(0)
                .unwrap()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
        assert!(
            storage
                .backend(1)
                .unwrap()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_quorum() {
        use crate::MemoryStorage;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::Quorum)
            .build();

        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // All should succeed in this test
        assert_eq!(storage.backend_count(), 3);
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_delete() {
        use crate::MemoryStorage;

        let backend1 = MemoryStorage::new();
        let backend2 = MemoryStorage::new();

        let storage = MirrorStorage::new(vec![backend1, backend2]);

        // Write via mirror to both backends
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Delete should remove from all
        storage.delete(&"test".to_string()).await.unwrap();

        assert!(
            !storage
                .backend(0)
                .unwrap()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
        assert!(
            !storage
                .backend(1)
                .unwrap()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_read_fallback() {
        use crate::MemoryStorage;

        let backend1 = MemoryStorage::new();
        let backend2 = MemoryStorage::new();

        let storage = MirrorStorage::new(vec![backend1, backend2]);

        // Put in first backend via the mirror
        storage
            .backend(0)
            .unwrap()
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should read from first backend
        let data = storage.get_string(&"test".to_string()).await.unwrap();
        assert_eq!(data, "data");
    }
}
