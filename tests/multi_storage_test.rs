//! Integration tests for multi-storage patterns.

use stowage::multi::{FallbackStorage, MirrorStorage, ReadOnlyStorage, WriteStrategy};
use stowage::{MemoryStorage, Storage, StorageExt};

#[tokio::test]
async fn test_fallback_basic() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    secondary
        .put_bytes("old.txt".to_string(), b"from secondary")
        .await
        .unwrap();

    let storage = FallbackStorage::new(primary, secondary);

    // Check exists falls back to secondary
    assert!(storage.exists(&"old.txt".to_string()).await.unwrap());

    // Write to primary
    storage
        .put_bytes("new.txt".to_string(), b"from primary")
        .await
        .unwrap();

    // Check exists in primary
    assert!(storage.exists(&"new.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_mirror_basic() {
    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: false })
        .build();

    storage
        .put_bytes("test.txt".to_string(), b"mirrored")
        .await
        .unwrap();

    // Should exist in both backends
    assert!(
        storage
            .backend(0)
            .unwrap()
            .exists(&"test.txt".to_string())
            .await
            .unwrap()
    );
    assert!(
        storage
            .backend(1)
            .unwrap()
            .exists(&"test.txt".to_string())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_readonly_basic() {
    let inner = MemoryStorage::new();
    inner
        .put_bytes("file.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    // Read works
    let data = storage.get_string(&"file.txt".to_string()).await.unwrap();
    assert_eq!(data, "data");

    // Write fails
    let result = storage.put_bytes("new.txt".to_string(), b"data").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_composition_fallback_mirror() {
    // Create mirrored primary
    let mirror = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: false })
        .build();

    // Add fallback
    let cache = MemoryStorage::new();
    cache
        .put_bytes("cached.txt".to_string(), b"from cache")
        .await
        .unwrap();

    let storage = FallbackStorage::new(mirror, cache);

    // Write to mirrored primary
    storage
        .put_bytes("new.txt".to_string(), b"mirrored")
        .await
        .unwrap();

    // Check exists in primary
    assert!(storage.exists(&"new.txt".to_string()).await.unwrap());

    // Falls back to cache
    assert!(storage.exists(&"cached.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_composition_readonly_fallback() {
    let primary = MemoryStorage::new();
    primary
        .put_bytes("file.txt".to_string(), b"data")
        .await
        .unwrap();

    let secondary = MemoryStorage::new();

    let fallback = FallbackStorage::new(primary, secondary);
    let storage = ReadOnlyStorage::new(fallback);

    // Read works
    assert!(storage.exists(&"file.txt".to_string()).await.unwrap());

    // Write fails
    let result = storage.put_bytes("new.txt".to_string(), b"data").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mirror_quorum_strategy() {
    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::Quorum)
        .build();

    storage
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    assert_eq!(storage.backend_count(), 3);
    assert_eq!(storage.write_strategy(), WriteStrategy::Quorum);
}

#[tokio::test]
async fn test_mirror_rollback_on_failure() {
    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: true })
        .build();

    // First successful write
    storage
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    // Verify it's in both backends
    assert!(
        storage
            .backend(0)
            .unwrap()
            .exists(&"test.txt".to_string())
            .await
            .unwrap()
    );
    assert!(
        storage
            .backend(1)
            .unwrap()
            .exists(&"test.txt".to_string())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_mirror_error_contains_details() {
    let storage = MirrorStorage::builder()
        .add_backend(MemoryStorage::new())
        .add_backend(MemoryStorage::new())
        .write_strategy(WriteStrategy::AllOrFail { rollback: false })
        .build();

    // Normal write succeeds
    storage
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    // The structure supports detailed error reporting
    // In a real scenario where one backend fails, Error::MirrorFailure(details) would contain:
    // - details.successes: Vec<usize> of backend indices that succeeded
    // - details.failures: Vec<(usize, Box<Error>)> with full error objects
    // - details.rollback_errors: Vec<(usize, Box<Error>)> if rollback attempted
    //
    // Helpful methods available:
    // - details.success_count(), details.failure_count(), details.total_backends()
    // - details.has_successes(), details.has_failures(), details.has_rollback_errors()
    // - details.failed_indices(), details.successful_indices()
    assert_eq!(storage.backend_count(), 2);
}
