//! Common test utilities and reusable test suite for storage adapters
//!
//! This module provides a macro `storage_test_suite!` that generates
//! a comprehensive set of tests for any Storage implementation.

use futures::stream::StreamExt;
use stowage::{Error, Storage, StorageExt};

/// Macro to generate a complete test suite for a Storage implementation.
///
/// # Usage
///
/// ```ignore
/// storage_test_suite!(
///     setup = || async {
///         // Your storage setup code here
///         MyStorage::new()
///     },
///     cleanup = |storage| async move {
///         // Optional cleanup code
///     }
/// );
/// ```
#[macro_export]
macro_rules! storage_test_suite {
    (
        setup = $setup:expr
        $(, cleanup = $cleanup:expr)?
    ) => {
        mod storage_test_suite {
            use super::*;
            use $crate::common::*;

            #[tokio::test]
            async fn test_put_and_exists() {
                let storage = $setup.await;
                run_test_put_and_exists(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_put_and_get_bytes() {
                let storage = $setup.await;
                run_test_put_and_get_bytes(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_get_nonexistent() {
                let storage = $setup.await;
                run_test_get_nonexistent(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_delete_existing() {
                let storage = $setup.await;
                run_test_delete_existing(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_delete_idempotent() {
                let storage = $setup.await;
                run_test_delete_idempotent(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_overwrite() {
                let storage = $setup.await;
                run_test_overwrite(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_empty_data() {
                let storage = $setup.await;
                run_test_empty_data(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_large_data() {
                let storage = $setup.await;
                run_test_large_data(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_binary_data() {
                let storage = $setup.await;
                run_test_binary_data(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_get_into() {
                let storage = $setup.await;
                run_test_get_into(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_folder_exists() {
                let storage = $setup.await;
                run_test_folder_exists(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_folder_exists_nested() {
                let storage = $setup.await;
                run_test_folder_exists_nested(&storage).await;
                $( $cleanup(storage).await; )?
            }

            #[tokio::test]
            async fn test_special_characters_in_path() {
                let storage = $setup.await;
                run_test_special_characters(&storage).await;
                $( $cleanup(storage).await; )?
            }
        }
    };
}

// Individual test implementations that can be reused

pub async fn run_test_put_and_exists<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());

    assert!(!storage.exists(&id).await.unwrap());

    let data = b"hello world";
    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
}

pub async fn run_test_put_and_get_bytes<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn run_test_get_nonexistent<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("nonexistent.txt".to_string());

    let result = storage.get_bytes(&id).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

pub async fn run_test_delete_existing<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();
    assert!(storage.exists(&id).await.unwrap());

    storage.delete(&id).await.unwrap();
    assert!(!storage.exists(&id).await.unwrap());
}

pub async fn run_test_delete_idempotent<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());

    // Delete non-existent file should not error
    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();
}

pub async fn run_test_overwrite<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());

    storage.put_bytes(id.clone(), b"original").await.unwrap();
    storage.put_bytes(id.clone(), b"updated").await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, b"updated");
}

pub async fn run_test_empty_data<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("empty.txt".to_string());
    let data = b"";

    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn run_test_large_data<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("large.bin".to_string());
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn run_test_binary_data<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("binary.dat".to_string());
    let data: Vec<u8> = (0..=255).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn run_test_get_into<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut output = Vec::new();
    let bytes_written = Storage::get_into(storage, &id, &mut output).await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(output, data);
}

pub async fn run_test_folder_exists<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    // Create files under "folder/"
    storage
        .put_bytes(S::Id::from("folder/file1.txt".to_string()), b"data1")
        .await
        .unwrap();

    storage
        .put_bytes(S::Id::from("folder/file2.txt".to_string()), b"data2")
        .await
        .unwrap();

    // Check folder exists (with and without trailing slash)
    assert!(
        storage
            .folder_exists(&S::Id::from("folder".to_string()))
            .await
            .unwrap()
    );
    assert!(
        storage
            .folder_exists(&S::Id::from("folder/".to_string()))
            .await
            .unwrap()
    );

    // Non-existent folder should not exist
    assert!(
        !storage
            .folder_exists(&S::Id::from("nonexistent".to_string()))
            .await
            .unwrap()
    );
}

pub async fn run_test_folder_exists_nested<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    // Create nested structure
    storage
        .put_bytes(
            S::Id::from("root/level1/level2/file.txt".to_string()),
            b"data",
        )
        .await
        .unwrap();

    // All parent folders should exist
    assert!(
        storage
            .folder_exists(&S::Id::from("root".to_string()))
            .await
            .unwrap()
    );
    assert!(
        storage
            .folder_exists(&S::Id::from("root/level1".to_string()))
            .await
            .unwrap()
    );
    assert!(
        storage
            .folder_exists(&S::Id::from("root/level1/level2".to_string()))
            .await
            .unwrap()
    );
}

pub async fn run_test_special_characters<S: Storage>(storage: &S)
where
    S::Id: From<String> + std::fmt::Debug,
{
    let paths = vec![
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
    ];

    for path in paths {
        let id = S::Id::from(path.to_string());
        storage.put_bytes(id.clone(), b"data").await.unwrap();
        assert!(storage.exists(&id).await.unwrap());
        let data = storage.get_bytes(&id).await.unwrap();
        assert_eq!(data, b"data");
    }
}

/// Helper to create a unique test ID for parallel test execution
pub fn test_id(base: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-{}-{}", base, timestamp)
}
