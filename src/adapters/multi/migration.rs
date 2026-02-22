//! Storage migration utilities.
//!
//! Provides [`migrate`] and the supporting [`MigrateOptions`] /
//! [`MigrationResult`] / [`ConflictStrategy`] types for bulk-copying (or
//! bulk-moving) items from one storage backend to another.
//!
//! # Example
//!
//! ```rust
//! # #[cfg(feature = "memory")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use stowage::{MemoryStorage, StorageExt};
//! use stowage::multi::migration::{migrate, MigrateOptions, ConflictStrategy};
//!
//! let source = MemoryStorage::new();
//! let dest   = MemoryStorage::new();
//!
//! source.put_bytes("docs/a.txt".to_string(), b"hello").await?;
//! source.put_bytes("docs/b.txt".to_string(), b"world").await?;
//! source.put_bytes("other.txt".to_string(),  b"skip").await?;
//!
//! let options = MigrateOptions {
//!     prefix:        Some("docs/".to_string()),
//!     conflict:      ConflictStrategy::Skip,
//!     concurrency:   4,
//!     delete_source: false,
//! };
//!
//! let result = migrate(&source, &dest, options).await?;
//! println!("{}", result); // "Migration: 2 transferred, 0 skipped, 0 errors"
//! assert_eq!(result.transferred_count(), 2);
//! # Ok(())
//! # }
//! ```

use std::fmt::Debug;

use futures::StreamExt as _;

use crate::{Result, Storage, StorageExt as _};

// ── Conflict strategy ─────────────────────────────────────────────────────────

/// Determines how the migration behaves when an item already exists in the
/// destination storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Overwrite the existing item in the destination.  This is the default.
    #[default]
    Overwrite,

    /// Leave the existing destination item untouched and record it as skipped.
    Skip,

    /// Treat an existing destination item as a hard error and record it in
    /// [`MigrationResult::errors`].
    Fail,
}

// ── Options ───────────────────────────────────────────────────────────────────

/// Options that control how a migration is executed.
///
/// Construct with `MigrateOptions::default()` and override the fields you care
/// about, or build one explicitly.
///
/// ```rust
/// # use stowage::multi::migration::{MigrateOptions, ConflictStrategy};
/// let opts: MigrateOptions<String> = MigrateOptions {
///     prefix:        Some("backups/".to_string()),
///     conflict:      ConflictStrategy::Skip,
///     concurrency:   8,
///     delete_source: true,
/// };
/// ```
#[derive(Clone)]
pub struct MigrateOptions<Id> {
    /// Only migrate items whose identifier begins with this prefix.
    ///
    /// `None` migrates every item in the source (default).
    pub prefix: Option<Id>,

    /// How to handle items that already exist in the destination.
    ///
    /// Default: [`ConflictStrategy::Overwrite`].
    pub conflict: ConflictStrategy,

    /// Maximum number of items transferred concurrently.
    ///
    /// Must be at least 1; values of 0 are clamped to 1.  Default: `4`.
    pub concurrency: usize,

    /// When `true`, each item is deleted from the source after it has been
    /// successfully copied to the destination (move semantics).
    ///
    /// A failed delete is logged as a warning but does **not** cause the item
    /// to appear in [`MigrationResult::errors`] — the copy has already
    /// succeeded.  Default: `false`.
    pub delete_source: bool,
}

impl<Id> Default for MigrateOptions<Id> {
    fn default() -> Self {
        Self {
            prefix: None,
            conflict: ConflictStrategy::Overwrite,
            concurrency: 4,
            delete_source: false,
        }
    }
}

impl<Id: Debug> Debug for MigrateOptions<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrateOptions")
            .field("prefix", &self.prefix)
            .field("conflict", &self.conflict)
            .field("concurrency", &self.concurrency)
            .field("delete_source", &self.delete_source)
            .finish()
    }
}

// ── Result ────────────────────────────────────────────────────────────────────

/// A summary of a completed migration.
///
/// Migrations are *best-effort*: every item is attempted regardless of
/// individual failures. Inspect [`errors`](Self::errors) to find out which
/// items did not make it across.
#[derive(Debug)]
pub struct MigrationResult<Id> {
    /// Items successfully copied to the destination.
    pub transferred: Vec<Id>,

    /// Items that already existed at the destination and were left untouched
    /// (only populated when [`ConflictStrategy::Skip`] is used).
    pub skipped: Vec<Id>,

    /// Items that were deleted from the source after a successful copy
    /// (only populated when [`MigrateOptions::delete_source`] is `true`).
    pub deleted: Vec<Id>,

    /// Items that could not be migrated, together with the error that caused
    /// the failure.
    pub errors: Vec<(Id, crate::Error)>,
}

impl<Id> MigrationResult<Id> {
    fn new() -> Self {
        Self {
            transferred: Vec::new(),
            skipped: Vec::new(),
            deleted: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Total number of items that were *attempted*
    /// (`transferred + skipped + errors`).
    pub fn total_attempted(&self) -> usize {
        self.transferred.len() + self.skipped.len() + self.errors.len()
    }

    /// Returns `true` when every item migrated without error.
    pub fn is_complete(&self) -> bool {
        self.errors.is_empty()
    }

    /// Number of items successfully transferred to the destination.
    pub fn transferred_count(&self) -> usize {
        self.transferred.len()
    }

    /// Number of items skipped due to the conflict strategy.
    pub fn skipped_count(&self) -> usize {
        self.skipped.len()
    }

    /// Number of items that failed to migrate.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Number of items deleted from the source (move semantics).
    pub fn deleted_count(&self) -> usize {
        self.deleted.len()
    }
}

impl<Id: Debug> std::fmt::Display for MigrationResult<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Migration: {} transferred, {} skipped, {} errors",
            self.transferred_count(),
            self.skipped_count(),
            self.error_count(),
        )?;
        if self.deleted_count() > 0 {
            write!(f, ", {} deleted from source", self.deleted_count())?;
        }
        Ok(())
    }
}

// ── Internal per-item outcome ─────────────────────────────────────────────────

enum ItemOutcome<Id> {
    Transferred(Id),
    TransferredAndDeleted(Id),
    /// Copy succeeded but the subsequent source delete failed (logged, not fatal).
    TransferredDeleteFailed(Id),
    Skipped(Id),
    Error(Id, crate::Error),
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Migrate items from `source` to `dest`, according to `options`.
///
/// # Behaviour
///
/// 1. All matching item identifiers are listed from `source` upfront.
/// 2. Items are transferred concurrently, up to `options.concurrency` at a time.
/// 3. The operation is *best-effort*: per-item failures are collected in
///    [`MigrationResult::errors`] rather than aborting the whole run.
/// 4. If `options.delete_source` is `true`, each successfully copied item is
///    deleted from `source`.  A delete failure is logged as a warning but does
///    **not** move the item to [`MigrationResult::errors`].
///
/// # Errors
///
/// Returns `Err` only for unrecoverable setup failures (e.g. `source.list()`
/// returning an error).  Per-item errors are returned inside
/// [`MigrationResult::errors`].
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "memory")]
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use stowage::{MemoryStorage, StorageExt};
/// use stowage::multi::migration::{migrate, MigrateOptions, ConflictStrategy};
///
/// let source = MemoryStorage::new();
/// let dest   = MemoryStorage::new();
///
/// source.put_bytes("a.txt".to_string(), b"hello").await?;
/// source.put_bytes("b.txt".to_string(), b"world").await?;
///
/// let result = migrate(&source, &dest, MigrateOptions::default()).await?;
/// assert_eq!(result.transferred_count(), 2);
/// assert!(result.is_complete());
/// # Ok(())
/// # }
/// ```
pub async fn migrate<S1, S2>(
    source: &S1,
    dest: &S2,
    options: MigrateOptions<S1::Id>,
) -> Result<MigrationResult<S1::Id>>
where
    S1: Storage,
    S2: Storage<Id = S1::Id>,
{
    let MigrateOptions {
        prefix,
        conflict,
        concurrency,
        delete_source,
    } = options;

    let concurrency = concurrency.max(1);

    // Collect all matching IDs upfront so the borrow on `source` from the
    // list stream is released before we start copying.
    let list_stream = source.list(prefix.as_ref()).await?;

    let ids: Vec<S1::Id> = list_stream
        .filter_map(|r| async move {
            match r {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        "Failed to read an item ID while listing source; skipping"
                    );
                    None
                }
            }
        })
        .collect()
        .await;

    tracing::debug!(total = ids.len(), "Collected source IDs for migration");

    let outcomes: Vec<ItemOutcome<S1::Id>> = futures::stream::iter(ids)
        .map(|id| {
            let source = source;
            let dest = dest;
            async move {
                // ── Conflict check ────────────────────────────────────────
                if conflict != ConflictStrategy::Overwrite {
                    match dest.exists(&id).await {
                        Ok(true) => {
                            return match conflict {
                                ConflictStrategy::Skip => {
                                    tracing::debug!(?id, "Skipping: item already exists at destination");
                                    ItemOutcome::Skipped(id)
                                }
                                ConflictStrategy::Fail => {
                                    let msg = format!(
                                        "Item already exists at destination: {:?}",
                                        id
                                    );
                                    tracing::warn!(?id, "Migration conflict: item exists");
                                    ItemOutcome::Error(id, crate::Error::Generic(msg))
                                }
                                ConflictStrategy::Overwrite => unreachable!(),
                            };
                        }
                        Ok(false) => { /* proceed */ }
                        Err(e) => {
                            tracing::warn!(?id, error = ?e, "Failed to check destination existence");
                            return ItemOutcome::Error(id, e);
                        }
                    }
                }

                // ── Copy ──────────────────────────────────────────────────
                if let Err(e) = source.copy_to(&id, dest).await {
                    tracing::warn!(?id, error = ?e, "Failed to copy item during migration");
                    return ItemOutcome::Error(id, e);
                }

                // ── Optional source deletion (move semantics) ─────────────
                if delete_source {
                    match source.delete(&id).await {
                        Ok(()) => {
                            tracing::debug!(?id, "Deleted source item after successful copy");
                            ItemOutcome::TransferredAndDeleted(id)
                        }
                        Err(e) => {
                            tracing::warn!(
                                ?id,
                                error = ?e,
                                "Copy succeeded but failed to delete source item"
                            );
                            ItemOutcome::TransferredDeleteFailed(id)
                        }
                    }
                } else {
                    ItemOutcome::Transferred(id)
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut result = MigrationResult::new();

    for outcome in outcomes {
        match outcome {
            ItemOutcome::Transferred(id) => {
                result.transferred.push(id);
            }
            ItemOutcome::TransferredAndDeleted(id) => {
                result.deleted.push(id.clone());
                result.transferred.push(id);
            }
            ItemOutcome::TransferredDeleteFailed(id) => {
                result.transferred.push(id);
            }
            ItemOutcome::Skipped(id) => {
                result.skipped.push(id);
            }
            ItemOutcome::Error(id, e) => {
                result.errors.push((id, e));
            }
        }
    }

    tracing::info!(
        transferred = result.transferred_count(),
        skipped = result.skipped_count(),
        errors = result.error_count(),
        deleted = result.deleted_count(),
        "Migration complete"
    );

    Ok(result)
}
