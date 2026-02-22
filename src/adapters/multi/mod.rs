//! Multi-storage adapters for combining multiple storage backends.
//!
//! Composite patterns implementing `Storage` by coordinating multiple backends:
//!
//! - [`FallbackStorage`] - Falls back to secondary on primary failure
//! - [`MirrorStorage`] - Replicates data across multiple backends
//! - [`ReadOnlyStorage`] - Prevents all write operations
//! - [`migration`] - Bulk-migrate items between any two storage backends

mod fallback;
pub mod migration;
mod mirror;
mod readonly;

pub use fallback::FallbackStorage;
pub use migration::{ConflictStrategy, MigrateOptions, MigrationResult, migrate};
pub use mirror::{MirrorStorage, MirrorStorageBuilder, ReturnPolicy, WriteStrategy};
pub use readonly::ReadOnlyStorage;
