//! Multi-storage adapters for combining multiple storage backends.
//!
//! Composite patterns implementing `Storage` by coordinating multiple backends:
//!
//! - [`FallbackStorage`] - Falls back to secondary on primary failure
//! - [`MirrorStorage`] - Replicates data across multiple backends
//! - [`ReadOnlyStorage`] - Prevents all write operations

mod fallback;
mod mirror;
mod readonly;

pub use fallback::FallbackStorage;
pub use mirror::{MirrorStorage, MirrorStorageBuilder, WriteStrategy};
pub use readonly::ReadOnlyStorage;
