//! Multi-storage adapters for combining multiple storage backends.
//!
//! This module provides composite storage patterns that implement the `Storage` trait
//! by coordinating multiple underlying storage backends. These patterns can be composed
//! together for complex storage architectures.
//!
//! # Available Patterns
//!
//! - [`FallbackStorage`] - Automatically falls back to secondary storage on primary failure
//! - [`MirrorStorage`] - Replicates data across multiple backends for redundancy
//!
//! # Examples
//!
//! ## Simple Fallback
//!
//! ```no_run
//! # use stowage::{Storage, StorageExt};
//! # async fn example() -> stowage::Result<()> {
//! use stowage::multi::FallbackStorage;
//! # use stowage::LocalStorage;
//!
//! let primary = LocalStorage::new("/fast-storage");
//! let backup = LocalStorage::new("/backup-storage");
//!
//! let storage = FallbackStorage::new(primary, backup);
//!
//! // Reads from primary, falls back to backup if not found
//! let data = storage.get_bytes(&"file.txt".to_string()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Composing Patterns
//!
//! ```no_run
//! # use stowage::{Storage, StorageExt};
//! # async fn example() -> stowage::Result<()> {
//! use stowage::multi::{FallbackStorage, MirrorStorage, WriteStrategy};
//! # use stowage::LocalStorage;
//!
//! // Create mirrored storage for redundancy
//! let mirror = MirrorStorage::builder()
//!     .add_backend(LocalStorage::new("/storage-1"))
//!     .add_backend(LocalStorage::new("/storage-2"))
//!     .write_strategy(WriteStrategy::AllOrFail)
//!     .build();
//!
//! // Add fallback to local cache
//! let storage = FallbackStorage::new(
//!     mirror,
//!     LocalStorage::new("/cache"),
//! );
//!
//! // Now writes go to both mirrored backends, reads fall back to cache
//! # Ok(())
//! # }
//! ```

mod fallback;
mod mirror;

pub use fallback::FallbackStorage;
pub use mirror::{MirrorStorage, MirrorStorageBuilder, WriteStrategy};
