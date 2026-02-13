use futures::io::AsyncRead;
use futures::stream::BoxStream;
use std::fmt::Debug;

// Re-export specific backends if they are enabled via features
// pub mod local;
// pub mod s3;

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

/// The core storage contract.
pub trait Storage: Send + Sync + Debug {
    /// The Identifier type.
    type Id: Clone + Debug + Send + Sync + 'static;

    /// Check if a file exists.
    fn exists(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<bool>> + Send;

    /// Save data to the storage.
    fn put<I: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        input: I,
        len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retrieve a stream of data.
    fn get<I: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<I>> + Send;

    /// Delete a file.
    ///
    /// Should return Ok(()) if the file is already gone (idempotent).
    fn delete(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<()>> + Send;

    /// List identifiers.
    fn list(
        &self,
        prefix: Option<&Self::Id>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'_, Result<Self::Id>>>> + Send;
}
