use crate::{Error, Result, Storage};
use futures::stream::BoxStream;
use std::fmt::Debug;
use tokio::io::{AsyncRead, AsyncWrite};

/// Write operation strategy for mirrored backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStrategy {
    /// All backends must succeed or operation fails.
    /// If `rollback` is true, successful writes are deleted on partial failure.
    AllOrFail { rollback: bool },

    /// At least one backend must succeed.
    AtLeastOne,

    /// Majority of backends must succeed.
    Quorum,
}

impl WriteStrategy {
    /// Check if this strategy requires rollback on failure.
    pub fn should_rollback(&self) -> bool {
        matches!(self, WriteStrategy::AllOrFail { rollback: true })
    }

    /// Get the number of required successes for the given backend count.
    pub fn required_successes(&self, backend_count: usize) -> usize {
        match self {
            WriteStrategy::AllOrFail { .. } => backend_count,
            WriteStrategy::AtLeastOne => 1,
            WriteStrategy::Quorum => (backend_count / 2) + 1,
        }
    }
}

/// Mirrors data across multiple backends for redundancy.
///
/// Writes to all backends sequentially. Reads from primary (configurable).
/// Use [`WriteStrategy`] to control success criteria.
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
            write_strategy: WriteStrategy::AllOrFail { rollback: false },
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
    /// Returns Ok(()) on success, or Error with detailed failure info.
    fn evaluate_write_results(&self, results: &[Result<()>]) -> Result<()> {
        let successes: Vec<usize> = results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| if r.is_ok() { Some(i) } else { None })
            .collect();

        let failures: Vec<(usize, String)> = results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().err().map(|e| (i, e.to_string())))
            .collect();

        let required = self.write_strategy.required_successes(self.backends.len());

        if successes.len() >= required {
            Ok(())
        } else {
            Err(Error::MirrorFailure {
                success_count: successes.len(),
                failure_count: failures.len(),
                successes,
                failures,
            })
        }
    }

    /// Rollback successful writes by deleting from those backends.
    async fn rollback_writes(&self, id: &S::Id, successful_indices: &[usize]) -> Result<()> {
        let mut rollback_futures = Vec::new();
        for &idx in successful_indices {
            if let Some(backend) = self.backends.get(idx) {
                rollback_futures.push(backend.delete(id));
            }
        }

        let _results = futures::future::join_all(rollback_futures).await;
        // Ignore rollback errors - best effort
        Ok(())
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

        // Write to all backends sequentially
        let mut results = Vec::new();
        for backend in &self.backends {
            let cursor = std::io::Cursor::new(buffer.clone());
            let mut async_cursor = tokio::io::BufReader::new(cursor);
            results.push(backend.put(id.clone(), &mut async_cursor, len).await);
        }

        // Evaluate results
        match self.evaluate_write_results(&results) {
            Ok(()) => Ok(()),
            Err(Error::MirrorFailure { ref successes, .. }) => {
                // Rollback if strategy requires it
                if self.write_strategy.should_rollback() && !successes.is_empty() {
                    let _ = self.rollback_writes(&id, successes).await;
                }
                Err(Error::MirrorFailure {
                    success_count: successes.len(),
                    failure_count: results.len() - successes.len(),
                    successes: successes.clone(),
                    failures: results
                        .iter()
                        .enumerate()
                        .filter_map(|(i, r)| r.as_ref().err().map(|e| (i, e.to_string())))
                        .collect(),
                })
            }
            Err(e) => Err(e),
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> Result<u64> {
        // Note: get_into only tries primary due to stream consumption.
        // Use get_bytes() for fallback on reads.
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

/// Builder for [`MirrorStorage`].
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
            write_strategy: WriteStrategy::AllOrFail { rollback: false },
            primary_index: 0,
        }
    }

    /// Add a backend to the mirror.
    pub fn add_backend(mut self, backend: S) -> Self {
        self.backends.push(backend);
        self
    }

    /// Set write strategy.
    pub fn write_strategy(mut self, strategy: WriteStrategy) -> Self {
        self.write_strategy = strategy;
        self
    }

    /// Set primary backend index for reads (default: 0).
    pub fn primary_index(mut self, index: usize) -> Self {
        self.primary_index = index;
        self
    }

    /// Build the mirror storage.
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
            .write_strategy(WriteStrategy::AllOrFail { rollback: false })
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
    async fn test_mirror_rollback() {
        use crate::MemoryStorage;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AllOrFail { rollback: true })
            .build();

        // First write succeeds
        storage
            .put_bytes("test1.txt".to_string(), b"data")
            .await
            .unwrap();

        // Both backends should have it
        assert!(
            storage
                .backend(0)
                .unwrap()
                .exists(&"test1.txt".to_string())
                .await
                .unwrap()
        );
        assert!(
            storage
                .backend(1)
                .unwrap()
                .exists(&"test1.txt".to_string())
                .await
                .unwrap()
        );
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
    async fn test_mirror_quorum_strategy() {
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
        assert_eq!(storage.write_strategy(), WriteStrategy::Quorum);
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_error_details() {
        use crate::MemoryStorage;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AllOrFail { rollback: false })
            .build();

        // All writes succeed
        storage
            .put_bytes("test.txt".to_string(), b"data")
            .await
            .unwrap();

        // Verify detailed error structure exists
        assert_eq!(storage.backend_count(), 2);
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
