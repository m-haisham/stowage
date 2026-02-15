//! Common test utilities and reusable test functions for storage adapters
//!
//! This module provides generic test functions that can be called from any
//! storage adapter's test file to avoid code duplication.

use stowage::{Error, Storage, StorageExt};

/// Run all common storage tests
pub async fn run_all_tests<S, F, Fut>(mut setup: F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug + PartialEq,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    test_put_and_exists(&mut setup).await;
    test_put_and_get_bytes(&mut setup).await;
    test_get_nonexistent(&mut setup).await;
    test_delete_existing(&mut setup).await;
    test_delete_idempotent(&mut setup).await;
    test_overwrite(&mut setup).await;
    test_empty_data(&mut setup).await;
    test_large_data(&mut setup).await;
    test_binary_data(&mut setup).await;
    test_get_into(&mut setup).await;
    test_folder_exists(&mut setup).await;
    test_folder_exists_nested(&mut setup).await;
    test_special_characters(&mut setup).await;
}

pub async fn test_put_and_exists<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());

    assert!(!storage.exists(&id).await.unwrap());

    let data = b"hello world";
    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
}

pub async fn test_put_and_get_bytes<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn test_get_nonexistent<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("nonexistent.txt".to_string());

    let result = storage.get_bytes(&id).await;
    assert!(result.is_err());
    // Accept either NotFound or Connection error (S3 can return Connection for 404)
    match result.unwrap_err() {
        Error::NotFound(_) | Error::Connection(_) => {}
        e => panic!("Expected NotFound or Connection error, got: {:?}", e),
    }
}

pub async fn test_delete_existing<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();
    assert!(storage.exists(&id).await.unwrap());

    storage.delete(&id).await.unwrap();
    assert!(!storage.exists(&id).await.unwrap());
}

pub async fn test_delete_idempotent<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());

    // Delete non-existent file should not error
    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();
}

pub async fn test_overwrite<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());

    storage.put_bytes(id.clone(), b"original").await.unwrap();
    storage.put_bytes(id.clone(), b"updated").await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, b"updated");
}

pub async fn test_empty_data<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("empty.txt".to_string());
    let data = b"";

    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn test_large_data<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("large.bin".to_string());
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn test_binary_data<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("binary.dat".to_string());
    let data: Vec<u8> = (0..=255).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

pub async fn test_get_into<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;
    let id = S::Id::from("test.txt".to_string());
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut output = Vec::new();
    let bytes_written = Storage::get_into(&storage, &id, &mut output).await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(output, data);
}

pub async fn test_folder_exists<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;

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

pub async fn test_folder_exists_nested<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;

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

pub async fn test_special_characters<S, F, Fut>(setup: &mut F)
where
    S: Storage,
    S::Id: From<String> + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    let storage = setup().await;

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
