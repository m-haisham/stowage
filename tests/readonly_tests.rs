//! Comprehensive tests for ReadOnlyStorage adapter

use futures::stream::StreamExt;
use stowage::multi::ReadOnlyStorage;
use stowage::{Error, MemoryStorage, Storage, StorageExt};

#[tokio::test]
async fn test_new_readonly_storage() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner);

    // Should be able to create readonly storage
    assert!(storage.inner().is_empty());
}

#[tokio::test]
async fn test_exists_works() {
    let inner = MemoryStorage::new();
    StorageExt::put_bytes(&inner, "test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    assert!(storage.exists(&"test.txt".to_string()).await.unwrap());
    assert!(!storage.exists(&"missing.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_folder_exists_works() {
    let inner = MemoryStorage::new();
    StorageExt::put_bytes(&inner, "folder/file.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    assert!(storage.folder_exists(&"folder".to_string()).await.unwrap());
    assert!(storage.folder_exists(&"folder/".to_string()).await.unwrap());
    assert!(!storage.folder_exists(&"missing".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_get_into_works() {
    let inner = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    StorageExt::put_bytes(&inner, id.clone(), data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let mut output = Vec::new();
    let bytes_written = Storage::get_into(&storage, &id, &mut output).await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_get_bytes_works() {
    let inner = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    StorageExt::put_bytes(&inner, id.clone(), data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_string_works() {
    let inner = MemoryStorage::new();
    let id = "test.txt".to_string();
    let text = "Hello, World! 你好世界";

    StorageExt::put_bytes(&inner, id.clone(), text.as_bytes())
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let retrieved = StorageExt::get_string(&storage, &id).await.unwrap();
    assert_eq!(retrieved, text);
}

#[tokio::test]
async fn test_list_works() {
    let inner = MemoryStorage::new();

    StorageExt::put_bytes(&inner, "a.txt".to_string(), b"1")
        .await
        .unwrap();
    StorageExt::put_bytes(&inner, "b.txt".to_string(), b"2")
        .await
        .unwrap();
    StorageExt::put_bytes(&inner, "c.txt".to_string(), b"3")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    assert_eq!(items.len(), 3);
    assert_eq!(items, vec!["a.txt", "b.txt", "c.txt"]);
}

#[tokio::test]
async fn test_list_with_prefix() {
    let inner = MemoryStorage::new();

    StorageExt::put_bytes(&inner, "docs/a.txt".to_string(), b"a")
        .await
        .unwrap();
    StorageExt::put_bytes(&inner, "docs/b.txt".to_string(), b"b")
        .await
        .unwrap();
    StorageExt::put_bytes(&inner, "other/c.txt".to_string(), b"c")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let prefix = "docs/".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    assert_eq!(items.len(), 2);
    assert!(items.contains(&"docs/a.txt".to_string()));
    assert!(items.contains(&"docs/b.txt".to_string()));
}

#[tokio::test]
async fn test_list_empty() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner);

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.collect::<Vec<_>>().await;

    assert_eq!(items.len(), 0);
}

#[tokio::test]
async fn test_put_is_rejected() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner);

    let id = "test.txt".to_string();
    let data = b"hello world";

    let result = StorageExt::put_bytes(&storage, id, data).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_delete_is_rejected() {
    let inner = MemoryStorage::new();
    StorageExt::put_bytes(&inner, "test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let result = storage.delete(&"test.txt".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_delete_nonexistent_is_rejected() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner);

    let result = storage.delete(&"nonexistent.txt".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_get_nonexistent_returns_error() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner);

    let result = StorageExt::get_bytes(&storage, &"nonexistent.txt".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_large_data() {
    let inner = MemoryStorage::new();
    let id = "large.bin".to_string();
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    StorageExt::put_bytes(&inner, id.clone(), &data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_empty_data() {
    let inner = MemoryStorage::new();
    let id = "empty.txt".to_string();
    let data = b"";

    StorageExt::put_bytes(&inner, id.clone(), data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_binary_data() {
    let inner = MemoryStorage::new();
    let id = "binary.dat".to_string();
    let data: Vec<u8> = (0..=255).collect();

    StorageExt::put_bytes(&inner, id.clone(), &data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_special_characters_in_id() {
    let inner = MemoryStorage::new();

    let ids = vec![
        "file with spaces.txt",
        "файл.txt", // Cyrillic
        "文件.txt", // Chinese
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
    ];

    for id in &ids {
        StorageExt::put_bytes(&inner, id.to_string(), b"data")
            .await
            .unwrap();
    }

    let storage = ReadOnlyStorage::new(inner);

    for id in ids {
        assert!(storage.exists(&id.to_string()).await.unwrap());
        let data = StorageExt::get_bytes(&storage, &id.to_string())
            .await
            .unwrap();
        assert_eq!(data, b"data");
    }
}

#[tokio::test]
async fn test_concurrent_reads() {
    let inner = MemoryStorage::new();

    // Prepare data
    for i in 0..10 {
        let id = format!("file{}.txt", i);
        let data = format!("data{}", i);
        StorageExt::put_bytes(&inner, id, data.as_bytes())
            .await
            .unwrap();
    }

    let storage = ReadOnlyStorage::new(inner);

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let inner = storage.inner().clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let expected = format!("data{}", i);
                let data = StorageExt::get_bytes(&inner, &id).await.unwrap();
                assert_eq!(data, expected.as_bytes());
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_copy_to_is_allowed() {
    let inner = MemoryStorage::new();
    let id = "source.txt".to_string();
    let data = b"copy data";

    StorageExt::put_bytes(&inner, id.clone(), data)
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);
    let dest = MemoryStorage::new();

    // copy_to reads from readonly (allowed) and writes to dest (allowed)
    StorageExt::copy_to(&storage, &id, &dest).await.unwrap();

    assert!(dest.exists(&id).await.unwrap());
    let dest_data = StorageExt::get_bytes(&dest, &id).await.unwrap();
    assert_eq!(dest_data, data);
}

#[tokio::test]
async fn test_copy_from_writable_to_readonly_fails() {
    let source = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"data";

    StorageExt::put_bytes(&source, id.clone(), data)
        .await
        .unwrap();

    let inner = MemoryStorage::new();
    let dest = ReadOnlyStorage::new(inner);

    // Copying to readonly should fail
    let result = StorageExt::copy_to(&source, &id, &dest).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_inner_reference() {
    let inner = MemoryStorage::new();
    StorageExt::put_bytes(&inner, "test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    // Should be able to access inner storage
    assert_eq!(storage.inner().len(), 1);
}

#[tokio::test]
async fn test_nested_directories() {
    let inner = MemoryStorage::new();

    StorageExt::put_bytes(&inner, "a/b/c/d/file.txt".to_string(), b"deep")
        .await
        .unwrap();

    let storage = ReadOnlyStorage::new(inner);

    assert!(
        storage
            .exists(&"a/b/c/d/file.txt".to_string())
            .await
            .unwrap()
    );

    let data = StorageExt::get_bytes(&storage, &"a/b/c/d/file.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"deep");
}

#[tokio::test]
async fn test_multiple_readonly_wrappers() {
    let inner = MemoryStorage::new();
    StorageExt::put_bytes(&inner, "test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage1 = ReadOnlyStorage::new(inner.clone());
    let storage2 = ReadOnlyStorage::new(inner.clone());

    // Both should read the same data
    let data1 = StorageExt::get_bytes(&storage1, &"test.txt".to_string())
        .await
        .unwrap();
    let data2 = StorageExt::get_bytes(&storage2, &"test.txt".to_string())
        .await
        .unwrap();

    assert_eq!(data1, data2);
    assert_eq!(data1, b"data");
}

#[tokio::test]
async fn test_modifications_through_inner_visible() {
    let inner = MemoryStorage::new();
    let storage = ReadOnlyStorage::new(inner.clone());

    // Initially empty
    assert!(!storage.exists(&"test.txt".to_string()).await.unwrap());

    // Modify through inner
    StorageExt::put_bytes(&inner, "test.txt".to_string(), b"new data")
        .await
        .unwrap();

    // Should be visible through readonly
    assert!(storage.exists(&"test.txt".to_string()).await.unwrap());
    let data = StorageExt::get_bytes(&storage, &"test.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"new data");
}
