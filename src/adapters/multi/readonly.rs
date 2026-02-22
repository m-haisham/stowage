use crate::{Error, Result, Storage};
use futures::stream::BoxStream;
use std::fmt::Debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing;

/// Wraps any storage backend and prevents all write operations.
///
/// Useful for enforcing read-only access or creating safe views.
///
/// ```
/// # use stowage::{Storage, StorageExt};
/// # use stowage::multi::ReadOnlyStorage;
/// # use stowage::MemoryStorage;
/// # async fn example() -> stowage::Result<()> {
/// let storage = ReadOnlyStorage::new(MemoryStorage::new());
/// let data = storage.get_bytes(&"file.txt".to_string()).await;
/// assert!(storage.put_bytes("file.txt".to_string(), b"data").await.is_err());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ReadOnlyStorage<S: Storage> {
    inner: S,
}

impl<S: Storage> ReadOnlyStorage<S> {
    /// Create a read-only wrapper around any storage backend.
    pub fn new(storage: S) -> Self {
        Self { inner: storage }
    }

    /// Get a reference to the inner storage.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Unwrap and return the inner storage.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Storage> Storage for ReadOnlyStorage<S> {
    type Id = S::Id;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        self.inner.exists(id).await
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        self.inner.folder_exists(id).await
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        _input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        tracing::warn!(?id, "Write operation blocked (read-only storage)");
        Err(Error::PermissionDenied(
            "write operations not allowed on read-only storage".to_string(),
        ))
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> Result<u64> {
        self.inner.get_into(id, output).await
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        tracing::warn!(?id, "Delete operation blocked (read-only storage)");
        Err(Error::PermissionDenied(
            "delete operations not allowed on read-only storage".to_string(),
        ))
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        self.inner.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageExt;

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_readonly_read() {
        use crate::adapters::memory::MemoryStorage;

        let inner = MemoryStorage::new();
        inner
            .put_bytes("test.txt".to_string(), b"data")
            .await
            .unwrap();

        let storage = ReadOnlyStorage::new(inner);

        let mut buf = Vec::new();
        storage
            .get_into(&"test.txt".to_string(), &mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"data");
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_readonly_write_rejected() {
        use crate::MemoryStorage;

        let storage = ReadOnlyStorage::new(MemoryStorage::new());

        let result = storage.put_bytes("test.txt".to_string(), b"data").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_readonly_delete_rejected() {
        use crate::MemoryStorage;

        let inner = MemoryStorage::new();
        inner
            .put_bytes("test.txt".to_string(), b"data")
            .await
            .unwrap();

        let storage = ReadOnlyStorage::new(inner);

        let result = storage.delete(&"test.txt".to_string()).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_readonly_exists() {
        use crate::MemoryStorage;

        let inner = MemoryStorage::new();
        inner
            .put_bytes("test.txt".to_string(), b"data")
            .await
            .unwrap();

        let storage = ReadOnlyStorage::new(inner);

        assert!(storage.exists(&"test.txt".to_string()).await.unwrap());
        assert!(!storage.exists(&"missing.txt".to_string()).await.unwrap());
    }
}
