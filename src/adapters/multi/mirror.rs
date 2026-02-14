use crate::{Error, MirrorFailureDetails, Result, Storage};
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing;

/// Write operation strategy for mirrored backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStrategy {
    /// All backends must succeed or operation fails.
    AllOrFail { rollback: bool },

    /// At least one backend must succeed.
    AtLeastOne { rollback: bool },

    /// Majority of backends must succeed.
    Quorum { rollback: bool },
}

/// Controls when a mirror operation returns to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnPolicy {
    /// Wait for all backends to complete before returning.
    /// Returns error if strategy threshold is not met.
    WaitAll,

    /// Return as soon as the strategy threshold is met.
    /// Remaining operations continue in background tasks.
    /// Always succeeds once threshold is met, even if later backends fail.
    Optimistic,

    /// Return as soon as we know the strategy cannot succeed.
    /// Also returns early if all required backends succeed.
    /// May return before all backends complete if failure is certain.
    FastFail,
}

impl WriteStrategy {
    /// Check if this strategy requires rollback on failure.
    pub fn should_rollback(&self) -> bool {
        match self {
            WriteStrategy::AllOrFail { rollback }
            | WriteStrategy::AtLeastOne { rollback }
            | WriteStrategy::Quorum { rollback } => *rollback,
        }
    }

    /// Get the number of required successes for the given backend count.
    pub fn required_successes(&self, backend_count: usize) -> usize {
        match self {
            WriteStrategy::AllOrFail { .. } => backend_count,
            WriteStrategy::AtLeastOne { .. } => 1,
            WriteStrategy::Quorum { .. } => (backend_count / 2) + 1,
        }
    }
}

/// Mirrors data across multiple backends for redundancy.
///
/// Writes to all backends sequentially. Reads from primary (configurable).
/// Use [`WriteStrategy`] to control success criteria and [`ReturnPolicy`]
/// to control when operations return to the caller.
///
/// **Note:** For Optimistic return policy with background writes, `S` must be `'static`.
/// Use `get_into()` directly for reads; `get_string()`/`get_bytes()` may have issues
/// with duplex streams in some async contexts.
#[derive(Debug)]
pub struct MirrorStorage<S: Storage + 'static> {
    backends: Vec<Arc<S>>,
    write_strategy: WriteStrategy,
    return_policy: ReturnPolicy,
    backend_timeout: Option<Duration>,
    primary_index: usize,
}

impl<S: Storage + 'static> MirrorStorage<S> {
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
            backends: backends.into_iter().map(Arc::new).collect(),
            write_strategy: WriteStrategy::AllOrFail { rollback: false },
            return_policy: ReturnPolicy::WaitAll,
            backend_timeout: None,
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

    /// Get the return policy.
    pub fn return_policy(&self) -> ReturnPolicy {
        self.return_policy
    }

    /// Get the backend timeout.
    pub fn backend_timeout(&self) -> Option<Duration> {
        self.backend_timeout
    }

    /// Get a reference to a specific backend by index.
    pub fn backend(&self, index: usize) -> Option<&S> {
        self.backends.get(index).map(|arc| arc.as_ref())
    }

    /// Get the primary backend (used for reads).
    pub fn primary(&self) -> &S {
        self.backends[self.primary_index].as_ref()
    }

    /// Evaluate if the write results meet the strategy requirements.
    /// Returns Ok(()) on success, or Error with detailed failure info.
    fn evaluate_write_results(&self, results: &[Result<()>]) -> Result<MirrorFailureDetails> {
        let successes: Vec<usize> = results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| if r.is_ok() { Some(i) } else { None })
            .collect();

        let failures: Vec<(usize, Box<Error>)> = results
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                r.as_ref()
                    .err()
                    .map(|e| (i, Box::new(Error::Generic(e.to_string()))))
            })
            .collect();

        let required = self.write_strategy.required_successes(self.backends.len());

        let details = MirrorFailureDetails {
            successes,
            failures,
            rollback_errors: Vec::new(),
        };

        if details.success_count() >= required {
            Ok(details)
        } else {
            Err(Error::MirrorFailure(details))
        }
    }

    /// Rollback successful writes by deleting from those backends.
    /// Returns the errors encountered during rollback.
    async fn rollback_writes(
        &self,
        id: &S::Id,
        successful_indices: &[usize],
    ) -> Vec<(usize, Box<Error>)> {
        let mut rollback_errors = Vec::new();

        for &idx in successful_indices {
            if let Some(backend) = self.backends.get(idx) {
                if let Err(e) = backend.as_ref().delete(id).await {
                    rollback_errors.push((idx, Box::new(e)));
                }
            }
        }

        rollback_errors
    }
}

impl<S: Storage + 'static> Storage for MirrorStorage<S> {
    type Id = S::Id;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        // Check primary first
        match self.primary().exists(id).await {
            Ok(exists) => Ok(exists),
            Err(e) => {
                tracing::warn!(?id, error = ?e, "Primary backend failed, trying fallbacks");
                // If primary fails, try other backends
                for (idx, backend) in self.backends.iter().enumerate() {
                    if let Ok(exists) = backend.as_ref().exists(id).await {
                        tracing::info!(?id, backend_index = idx, "Fallback succeeded");
                        return Ok(exists);
                    }
                }
                tracing::error!(?id, "All backends failed");
                // If all fail, return the primary's error
                self.primary().exists(id).await
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        // Check primary first
        match self.primary().folder_exists(id).await {
            Ok(exists) => Ok(exists),
            Err(e) => {
                tracing::warn!(?id, error = ?e, "Primary folder check failed, trying fallbacks");
                // If primary fails, try other backends
                for (idx, backend) in self.backends.iter().enumerate() {
                    if let Ok(exists) = backend.as_ref().folder_exists(id).await {
                        tracing::info!(?id, backend_index = idx, "Fallback succeeded");
                        return Ok(exists);
                    }
                }
                tracing::error!(?id, "All folder checks failed");
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

        let required_successes = self.write_strategy.required_successes(self.backends.len());

        match self.return_policy {
            ReturnPolicy::WaitAll => {
                // Write to all backends sequentially
                let mut results = Vec::new();
                for (idx, backend) in self.backends.iter().enumerate() {
                    let cursor = std::io::Cursor::new(buffer.clone());
                    let mut async_cursor = tokio::io::BufReader::new(cursor);
                    let result = if let Some(timeout) = self.backend_timeout {
                        tokio::time::timeout(
                            timeout,
                            backend.as_ref().put(id.clone(), &mut async_cursor, len),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            tracing::warn!(
                                ?id,
                                backend_index = idx,
                                ?timeout,
                                "Backend write timed out"
                            );
                            Err(Error::Generic("Backend timeout".to_string()))
                        })
                    } else {
                        backend
                            .as_ref()
                            .put(id.clone(), &mut async_cursor, len)
                            .await
                    };
                    results.push(result);
                }

                // Evaluate results
                match self.evaluate_write_results(&results) {
                    Ok(_details) => Ok(()),
                    Err(Error::MirrorFailure(mut details)) => {
                        tracing::error!(
                            ?id,
                            success_count = details.success_count(),
                            failure_count = details.failure_count(),
                            required = required_successes,
                            "Mirror write failed"
                        );
                        // Rollback if strategy requires it
                        if self.write_strategy.should_rollback() && details.has_successes() {
                            tracing::info!(
                                ?id,
                                rollback_count = details.successes.len(),
                                "Starting rollback"
                            );
                            let rollback_errors =
                                self.rollback_writes(&id, &details.successes).await;
                            if !rollback_errors.is_empty() {
                                tracing::error!(
                                    ?id,
                                    rollback_error_count = rollback_errors.len(),
                                    "Rollback encountered errors"
                                );
                            } else {
                                tracing::info!(?id, "Rollback completed successfully");
                            }
                            details.rollback_errors = rollback_errors;
                        }
                        Err(Error::MirrorFailure(details))
                    }
                    Err(e) => Err(e),
                }
            }

            ReturnPolicy::Optimistic => {
                // Write to backends until we have enough successes, then spawn background task for the rest
                let mut success_count = 0;
                let mut successes = Vec::new();
                let mut failures = Vec::new();

                for (idx, backend) in self.backends.iter().enumerate() {
                    let cursor = std::io::Cursor::new(buffer.clone());
                    let mut async_cursor = tokio::io::BufReader::new(cursor);
                    let result = if let Some(timeout) = self.backend_timeout {
                        tokio::time::timeout(
                            timeout,
                            backend.as_ref().put(id.clone(), &mut async_cursor, len),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            tracing::warn!(
                                ?id,
                                backend_index = idx,
                                ?timeout,
                                "Backend write timed out"
                            );
                            Err(Error::Generic("Backend timeout".to_string()))
                        })
                    } else {
                        backend
                            .as_ref()
                            .put(id.clone(), &mut async_cursor, len)
                            .await
                    };

                    match result {
                        Ok(_) => {
                            success_count += 1;
                            successes.push(idx);
                            // Return early once we have enough successes
                            if success_count >= required_successes {
                                tracing::info!(
                                    ?id,
                                    success_count,
                                    completed_backends = idx + 1,
                                    remaining_backends = self.backends.len() - (idx + 1),
                                    "Threshold met, returning early with background writes"
                                );
                                // Spawn background task for remaining backends
                                if idx + 1 < self.backends.len() {
                                    let remaining_backends: Vec<Arc<S>> =
                                        self.backends[(idx + 1)..].to_vec();
                                    let buffer_clone = buffer.clone();
                                    let id_clone = id.clone();
                                    let timeout = self.backend_timeout;
                                    let _remaining_count = remaining_backends.len();
                                    // Spawn background task for remaining backends
                                    // This is why S: 'static is required - the task must own the Arc
                                    tokio::spawn(async move {
                                        for (rel_idx, backend) in
                                            remaining_backends.iter().enumerate()
                                        {
                                            let abs_idx = idx + 1 + rel_idx;
                                            let cursor = std::io::Cursor::new(buffer_clone.clone());
                                            let mut async_cursor =
                                                tokio::io::BufReader::new(cursor);
                                            let result = if let Some(timeout) = timeout {
                                                tokio::time::timeout(
                                                    timeout,
                                                    backend.as_ref().put(
                                                        id_clone.clone(),
                                                        &mut async_cursor,
                                                        len,
                                                    ),
                                                )
                                                .await
                                            } else {
                                                Ok(backend
                                                    .as_ref()
                                                    .put(id_clone.clone(), &mut async_cursor, len)
                                                    .await)
                                            };

                                            if let Ok(Err(e)) = &result {
                                                tracing::warn!(?id_clone, backend_index = abs_idx, error = ?e, "Background write failed");
                                            } else if result.is_err() {
                                                tracing::warn!(
                                                    ?id_clone,
                                                    backend_index = abs_idx,
                                                    "Background write timed out"
                                                );
                                            }
                                        }
                                    });
                                }
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(?id, backend_index = idx, error = ?e, "Backend write failed");
                            failures.push((idx, Box::new(Error::Generic(e.to_string()))));
                        }
                    }
                }

                // If we get here, we didn't meet the threshold
                tracing::error!(
                    ?id,
                    success_count,
                    required_successes,
                    "Mirror write failed"
                );
                let details = MirrorFailureDetails {
                    successes,
                    failures,
                    rollback_errors: Vec::new(),
                };

                // Rollback if strategy requires it
                if self.write_strategy.should_rollback() && !details.successes.is_empty() {
                    tracing::info!(
                        ?id,
                        rollback_count = details.successes.len(),
                        "Starting rollback"
                    );
                    let rollback_errors = self.rollback_writes(&id, &details.successes).await;
                    if !rollback_errors.is_empty() {
                        tracing::error!(
                            ?id,
                            rollback_error_count = rollback_errors.len(),
                            "Rollback encountered errors"
                        );
                    }
                    Err(Error::MirrorFailure(MirrorFailureDetails {
                        rollback_errors,
                        ..details
                    }))
                } else {
                    Err(Error::MirrorFailure(details))
                }
            }

            ReturnPolicy::FastFail => {
                // Write to backends, but return early if we know we can't succeed
                let mut success_count = 0;
                let mut successes = Vec::new();
                let mut failures = Vec::new();

                for (idx, backend) in self.backends.iter().enumerate() {
                    let cursor = std::io::Cursor::new(buffer.clone());
                    let mut async_cursor = tokio::io::BufReader::new(cursor);
                    let result = if let Some(timeout) = self.backend_timeout {
                        tokio::time::timeout(
                            timeout,
                            backend.as_ref().put(id.clone(), &mut async_cursor, len),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            tracing::warn!(
                                ?id,
                                backend_index = idx,
                                ?timeout,
                                "Backend write timed out"
                            );
                            Err(Error::Generic("Backend timeout".to_string()))
                        })
                    } else {
                        backend
                            .as_ref()
                            .put(id.clone(), &mut async_cursor, len)
                            .await
                    };

                    match result {
                        Ok(_) => {
                            success_count += 1;
                            successes.push(idx);
                            // Return early if we have enough successes
                            if success_count >= required_successes {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            failures.push((idx, Box::new(Error::Generic(e.to_string()))));
                        }
                    }

                    // Calculate if success is still possible
                    let remaining_backends = self.backends.len() - (idx + 1);
                    let max_possible_successes = success_count + remaining_backends;

                    // If we can't possibly meet the threshold, fail fast
                    if max_possible_successes < required_successes {
                        tracing::warn!(
                            ?id,
                            success_count,
                            required_successes,
                            remaining_backends,
                            "Success impossible, failing fast"
                        );
                        let details = MirrorFailureDetails {
                            successes,
                            failures,
                            rollback_errors: Vec::new(),
                        };

                        // Rollback if strategy requires it
                        if self.write_strategy.should_rollback() && !details.successes.is_empty() {
                            tracing::info!(
                                ?id,
                                rollback_count = details.successes.len(),
                                "Starting rollback"
                            );
                            let rollback_errors =
                                self.rollback_writes(&id, &details.successes).await;
                            if !rollback_errors.is_empty() {
                                tracing::error!(
                                    ?id,
                                    rollback_error_count = rollback_errors.len(),
                                    "Rollback encountered errors"
                                );
                            }
                            return Err(Error::MirrorFailure(MirrorFailureDetails {
                                rollback_errors,
                                ..details
                            }));
                        } else {
                            return Err(Error::MirrorFailure(details));
                        }
                    }
                }

                // If we get here, evaluate final results
                if success_count >= required_successes {
                    Ok(())
                } else {
                    tracing::error!(
                        ?id,
                        success_count,
                        required_successes,
                        "Mirror write failed"
                    );
                    let details = MirrorFailureDetails {
                        successes,
                        failures,
                        rollback_errors: Vec::new(),
                    };

                    // Rollback if strategy requires it
                    if self.write_strategy.should_rollback() && !details.successes.is_empty() {
                        tracing::info!(
                            ?id,
                            rollback_count = details.successes.len(),
                            "Starting rollback"
                        );
                        let rollback_errors = self.rollback_writes(&id, &details.successes).await;
                        if !rollback_errors.is_empty() {
                            tracing::error!(
                                ?id,
                                rollback_error_count = rollback_errors.len(),
                                "Rollback encountered errors"
                            );
                        }
                        Err(Error::MirrorFailure(MirrorFailureDetails {
                            rollback_errors,
                            ..details
                        }))
                    } else {
                        Err(Error::MirrorFailure(details))
                    }
                }
            }
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
        let futures = self
            .backends
            .iter()
            .map(|backend| backend.as_ref().delete(id));
        let results: Vec<Result<()>> = futures::future::join_all(futures).await;

        // For delete, we use AtLeastOne strategy (more lenient)
        // since delete is idempotent
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.len() - successes;

        if successes > 0 {
            if failures > 0 {
                tracing::warn!(?id, successes, failures, "Delete succeeded partially");
            }
            Ok(())
        } else {
            tracing::error!(?id, "Delete failed on all backends");
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
pub struct MirrorStorageBuilder<S: Storage + 'static> {
    backends: Vec<S>,
    write_strategy: WriteStrategy,
    return_policy: ReturnPolicy,
    backend_timeout: Option<Duration>,
    primary_index: usize,
}

impl<S: Storage + 'static> MirrorStorageBuilder<S> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
            write_strategy: WriteStrategy::AllOrFail { rollback: false },
            return_policy: ReturnPolicy::WaitAll,
            backend_timeout: None,
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

    /// Set return policy (default: WaitAll).
    pub fn return_policy(mut self, policy: ReturnPolicy) -> Self {
        self.return_policy = policy;
        self
    }

    /// Set per-backend timeout (default: None).
    pub fn backend_timeout(mut self, timeout: Duration) -> Self {
        self.backend_timeout = Some(timeout);
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
            backends: self.backends.into_iter().map(Arc::new).collect(),
            write_strategy: self.write_strategy,
            return_policy: self.return_policy,
            backend_timeout: self.backend_timeout,
            primary_index: self.primary_index,
        }
    }
}

impl<S: Storage + 'static> Default for MirrorStorageBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage + 'static> Debug for MirrorStorageBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorStorageBuilder")
            .field("backend_count", &self.backends.len())
            .field("write_strategy", &self.write_strategy)
            .field("return_policy", &self.return_policy)
            .field("backend_timeout", &self.backend_timeout)
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
            .write_strategy(WriteStrategy::AllOrFail { rollback: true })
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
        let mut buf = Vec::new();
        storage
            .get_into(&"test".to_string(), &mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"data");
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
            .write_strategy(WriteStrategy::AtLeastOne { rollback: false })
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
            .write_strategy(WriteStrategy::Quorum { rollback: false })
            .build();

        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // All should succeed in this test
        assert_eq!(storage.backend_count(), 3);
        assert_eq!(
            storage.write_strategy(),
            WriteStrategy::Quorum { rollback: false }
        );
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

        let storage = MirrorStorage::new(vec![MemoryStorage::new(), MemoryStorage::new()]);

        // Write via the mirror storage (writes to all backends)
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should read from first backend
        let mut buf = Vec::new();
        storage
            .get_into(&"test".to_string(), &mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"data");

        // Verify both backends have the data
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
    async fn test_mirror_optimistic_return() {
        use crate::MemoryStorage;

        // With optimistic return, should succeed once quorum is met
        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::Quorum { rollback: false })
            .return_policy(ReturnPolicy::Optimistic)
            .build();

        // Should return quickly once 2/3 succeed
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Eventually all backends should have the data (after background write)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // All backends should have it now
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
        assert!(
            storage
                .backend(2)
                .unwrap()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_backend_timeout() {
        use crate::MemoryStorage;
        use std::time::Duration;

        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AtLeastOne { rollback: false })
            .backend_timeout(Duration::from_secs(5))
            .build();

        // Should succeed even if a backend times out (with AtLeastOne)
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        assert_eq!(storage.backend_timeout(), Some(Duration::from_secs(5)));
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_mirror_fast_fail() {
        use crate::MemoryStorage;

        // With AllOrFail and fast fail, should return as soon as success is impossible
        let storage = MirrorStorage::builder()
            .add_backend(MemoryStorage::new())
            .add_backend(MemoryStorage::new())
            .write_strategy(WriteStrategy::AllOrFail { rollback: false })
            .return_policy(ReturnPolicy::FastFail)
            .build();

        // All backends succeed in this test
        storage
            .put_bytes("file3.txt".to_string(), b"data")
            .await
            .unwrap();

        assert_eq!(storage.return_policy(), ReturnPolicy::FastFail);
    }
}
