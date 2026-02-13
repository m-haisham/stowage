use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// A simple in-memory `Storage` adapter.
///
/// - `Id` is a `String`.
/// - Data is stored as raw bytes in a `HashMap`.
/// - Intended for tests, local development, and ephemeral usage.
///
/// This adapter uses Tokio I/O for `get_into`.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemoryStorage {
    /// Create a new empty in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new in-memory storage from an existing map.
    pub fn from_map(map: HashMap<String, Vec<u8>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    /// Returns the number of stored objects.
    pub fn len(&self) -> usize {
        self.inner.read().expect("poisoned lock").len()
    }

    /// Returns true if there are no stored objects.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all objects.
    pub fn clear(&self) {
        self.inner.write().expect("poisoned lock").clear();
    }

    /// Get a copy of the bytes for `id` (useful for tests).
    pub fn get_bytes(&self, id: &str) -> Result<Vec<u8>> {
        let map = self.inner.read().expect("poisoned lock");
        map.get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.to_string()))
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Avoid dumping potentially large in-memory contents.
        f.debug_struct("MemoryStorage")
            .field("len", &self.len())
            .finish()
    }
}

impl Storage for MemoryStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let map = self.inner.read().expect("poisoned lock");
        Ok(map.contains_key(id))
    }

    async fn put<I>(&self, id: Self::Id, mut input: I, _len: Option<u64>) -> Result<()>
    where
        I: tokio::io::AsyncRead + Send + Unpin,
    {
        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;

        let mut map = self.inner.write().expect("poisoned lock");
        map.insert(id, buf);
        Ok(())
    }

    async fn get_into<O>(&self, id: &Self::Id, mut output: O) -> Result<u64>
    where
        O: AsyncWrite + Send + Unpin,
    {
        let bytes = {
            let map = self.inner.read().expect("poisoned lock");
            map.get(id)
                .cloned()
                .ok_or_else(|| Error::NotFound(id.clone()))?
        };

        output.write_all(&bytes).await?;
        output.flush().await?;
        Ok(bytes.len() as u64)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let mut map = self.inner.write().expect("poisoned lock");
        map.remove(id);
        Ok(())
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let prefix = prefix.cloned();
        let map = self.inner.read().expect("poisoned lock");

        let mut ids: Vec<String> = map.keys().cloned().collect();
        ids.sort();

        let iter = ids.into_iter().filter(move |id| match &prefix {
            None => true,
            Some(p) => id.starts_with(p),
        });

        Ok(Box::pin(stream::iter(iter.map(Ok))))
    }
}
