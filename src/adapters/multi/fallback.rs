use crate::{Result, Storage};
use futures::stream::BoxStream;
use std::fmt::Debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing;

/// Automatically falls back to secondary storage when primary fails.
///
/// Writes go to primary only by default. Use [`with_write_through`](Self::with_write_through)
/// to write to both backends.
#[derive(Debug)]
pub struct FallbackStorage<P, S>
where
    P: Storage,
    S: Storage<Id = P::Id>,
{
    primary: P,
    secondary: S,
    write_through: bool,
}

impl<P, S> FallbackStorage<P, S>
where
    P: Storage,
    S: Storage<Id = P::Id>,
{
    /// Create fallback storage with primary and secondary backends.
    pub fn new(primary: P, secondary: S) -> Self {
        Self {
            primary,
            secondary,
            write_through: false,
        }
    }

    /// Enable/disable write-through to secondary (default: disabled).
    pub fn with_write_through(mut self, enabled: bool) -> Self {
        self.write_through = enabled;
        self
    }

    /// Get a reference to the primary storage.
    pub fn primary(&self) -> &P {
        &self.primary
    }

    /// Get a reference to the secondary storage.
    pub fn secondary(&self) -> &S {
        &self.secondary
    }

    /// Returns true if write-through is enabled.
    pub fn is_write_through(&self) -> bool {
        self.write_through
    }
}

impl<P, S> Storage for FallbackStorage<P, S>
where
    P: Storage,
    S: Storage<Id = P::Id>,
{
    type Id = P::Id;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        // Try primary first
        match self.primary.exists(id).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                // If not in primary, check secondary
                self.secondary.exists(id).await
            }
            Err(e) => {
                tracing::warn!(?id, error = ?e, "Primary failed, using fallback");
                // On primary error, try secondary
                self.secondary.exists(id).await
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        // Try primary first
        match self.primary.folder_exists(id).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                // If not in primary, check secondary
                self.secondary.folder_exists(id).await
            }
            Err(e) => {
                tracing::warn!(?id, error = ?e, "Primary folder check failed, using fallback");
                // On primary error, try secondary
                self.secondary.folder_exists(id).await
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: R,
        len: Option<u64>,
    ) -> Result<()> {
        if self.write_through {
            // Write-through: write to both backends
            // We need to buffer the input since we can't read from it twice
            use tokio::io::AsyncReadExt;
            let mut buffer = Vec::new();
            let mut reader = input;
            reader.read_to_end(&mut buffer).await?;

            // Write to primary first
            let primary_result = {
                let cursor = std::io::Cursor::new(&buffer);
                let mut async_cursor = tokio::io::BufReader::new(cursor);
                self.primary.put(id.clone(), &mut async_cursor, len).await
            };

            // Write to secondary
            let secondary_result = {
                let cursor = std::io::Cursor::new(&buffer);
                let mut async_cursor = tokio::io::BufReader::new(cursor);
                self.secondary.put(id.clone(), &mut async_cursor, len).await
            };

            if let Err(e) = &secondary_result {
                tracing::warn!(?id, error = ?e, "Secondary write failed (best-effort)");
            }

            // Return error if primary failed (secondary is best-effort in write-through)
            primary_result
        } else {
            // Default: write only to primary
            self.primary.put(id, input, len).await
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        output: W,
    ) -> Result<u64> {
        // Note: get_into only tries primary due to stream consumption.
        // Use get_bytes() for fallback on reads.
        self.primary.get_into(id, output).await
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        // Delete from both backends (idempotent, so safe to try both)
        let primary_result = self.primary.delete(id).await;
        let secondary_result = self.secondary.delete(id).await;

        // If both fail, return primary error
        // If either succeeds, consider it a success (idempotent operation)
        match (primary_result, secondary_result) {
            (Ok(()), _) => Ok(()),
            (_, Ok(())) => Ok(()),
            (Err(e), Err(e2)) => {
                tracing::error!(?id, primary_error = ?e, secondary_error = ?e2, "Delete failed on both backends");
                Err(e)
            }
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        // For list, we only query the primary
        // Merging lists from both backends would be complex and potentially confusing
        self.primary.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageExt;

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_fallback_read() {
        use crate::MemoryStorage;

        let primary = MemoryStorage::new();
        let secondary = MemoryStorage::new();

        // Put data in primary
        primary
            .put_bytes("test-file".to_string(), b"primary data")
            .await
            .unwrap();

        let storage = FallbackStorage::new(primary, secondary);

        // Should read from primary using get_into
        let mut buf = Vec::new();
        storage
            .get_into(&"test-file".to_string(), &mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"primary data");

        // Should error on not found
        let mut buf = Vec::new();
        assert!(
            storage
                .get_into(&"nowhere".to_string(), &mut buf)
                .await
                .is_err()
        );

        // Note: get_into doesn't support fallback to secondary due to stream consumption.
        // For fallback reads, use get_bytes() when the duplex stream issue is resolved.
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_fallback_write_default() {
        use crate::MemoryStorage;

        let primary = MemoryStorage::new();
        let secondary = MemoryStorage::new();

        let storage = FallbackStorage::new(primary, secondary);

        // Write data
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should exist in primary
        assert!(storage.primary().exists(&"test".to_string()).await.unwrap());

        // Should NOT exist in secondary (default behavior)
        assert!(
            !storage
                .secondary()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_fallback_write_through() {
        use crate::MemoryStorage;

        let primary = MemoryStorage::new();
        let secondary = MemoryStorage::new();

        let storage = FallbackStorage::new(primary, secondary).with_write_through(true);

        // Write data
        storage
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        // Should exist in both
        assert!(storage.primary().exists(&"test".to_string()).await.unwrap());
        assert!(
            storage
                .secondary()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );

        // Data should be same in both
        let mut primary_buf = Vec::new();
        storage
            .primary()
            .get_into(&"test".to_string(), &mut primary_buf)
            .await
            .unwrap();
        let mut secondary_buf = Vec::new();
        storage
            .secondary()
            .get_into(&"test".to_string(), &mut secondary_buf)
            .await
            .unwrap();
        assert_eq!(primary_buf, secondary_buf);
        assert_eq!(primary_buf, b"data");
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_fallback_delete() {
        use crate::MemoryStorage;

        let primary = MemoryStorage::new();
        let secondary = MemoryStorage::new();

        primary
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();
        secondary
            .put_bytes("test".to_string(), b"data")
            .await
            .unwrap();

        let storage = FallbackStorage::new(primary, secondary);

        // Delete should remove from both
        storage.delete(&"test".to_string()).await.unwrap();

        assert!(!storage.primary().exists(&"test".to_string()).await.unwrap());
        assert!(
            !storage
                .secondary()
                .exists(&"test".to_string())
                .await
                .unwrap()
        );
    }
}
