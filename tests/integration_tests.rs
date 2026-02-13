//! Integration tests for Storage trait and StorageExt methods
//! Tests the trait contract across different storage implementations

use futures::stream::StreamExt;
use stowage::{Error, MemoryStorage, Storage, StorageExt};

#[tokio::test]
async fn test_storage_ext_get_bytes() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    // Test StorageExt::get_bytes using explicit trait call
    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_ext_get_string() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let text = "Hello, World! 🚀";

    storage
        .put_bytes(id.clone(), text.as_bytes())
        .await
        .unwrap();

    let retrieved = StorageExt::get_string(&storage, &id).await.unwrap();
    assert_eq!(retrieved, text);
}

#[tokio::test]
async fn test_storage_ext_get_string_invalid_utf8() {
    let storage = MemoryStorage::new();
    let id = "binary.dat".to_string();
    let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];

    storage.put_bytes(id.clone(), &invalid_utf8).await.unwrap();

    let result = StorageExt::get_string(&storage, &id).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Generic(msg) => assert!(msg.contains("invalid utf-8")),
        _ => panic!("Expected Generic error for invalid UTF-8"),
    }
}

#[tokio::test]
async fn test_storage_ext_put_bytes() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"test data";

    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_ext_copy_to_same_storage() {
    let storage = MemoryStorage::new();
    let id = "source.txt".to_string();
    let data = b"copy this data";

    storage.put_bytes(id.clone(), data).await.unwrap();

    // Copy to itself (overwrites with same data)
    StorageExt::copy_to(&storage, &id, &storage).await.unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_ext_copy_to_different_storage() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();
    let id = "file.txt".to_string();
    let data = b"transfer this";

    source.put_bytes(id.clone(), data).await.unwrap();

    StorageExt::copy_to(&source, &id, &dest).await.unwrap();

    // Verify both storages have the data
    assert!(source.exists(&id).await.unwrap());
    assert!(dest.exists(&id).await.unwrap());

    let source_data = StorageExt::get_bytes(&source, &id).await.unwrap();
    let dest_data = StorageExt::get_bytes(&dest, &id).await.unwrap();
    assert_eq!(source_data, dest_data);
    assert_eq!(source_data, data);
}

#[tokio::test]
async fn test_storage_ext_copy_to_large_file() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();
    let id = "large.bin".to_string();
    let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    source.put_bytes(id.clone(), &data).await.unwrap();

    StorageExt::copy_to(&source, &id, &dest).await.unwrap();

    let dest_data = StorageExt::get_bytes(&dest, &id).await.unwrap();
    assert_eq!(dest_data, data);
}

#[tokio::test]
async fn test_storage_ext_copy_to_nonexistent_source() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();
    let id = "nonexistent.txt".to_string();

    let result = StorageExt::copy_to(&source, &id, &dest).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_storage_trait_exists_on_empty() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    assert!(!storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_storage_trait_put_then_exists() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    storage.put_bytes(id.clone(), b"data").await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_storage_trait_delete_then_not_exists() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    storage.put_bytes(id.clone(), b"data").await.unwrap();
    assert!(storage.exists(&id).await.unwrap());

    storage.delete(&id).await.unwrap();
    assert!(!storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_storage_trait_list_empty() {
    let storage = MemoryStorage::new();

    let stream = storage.list(None).await.unwrap();
    let count = stream.count().await;

    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_storage_trait_list_multiple() {
    let storage = MemoryStorage::new();

    storage.put_bytes("a.txt".to_string(), b"1").await.unwrap();
    storage.put_bytes("b.txt".to_string(), b"2").await.unwrap();
    storage.put_bytes("c.txt".to_string(), b"3").await.unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn test_storage_trait_list_with_prefix() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("prefix_a.txt".to_string(), b"1")
        .await
        .unwrap();
    storage
        .put_bytes("prefix_b.txt".to_string(), b"2")
        .await
        .unwrap();
    storage
        .put_bytes("other.txt".to_string(), b"3")
        .await
        .unwrap();

    let prefix = "prefix_".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    assert_eq!(items.len(), 2);
    assert!(items.contains(&"prefix_a.txt".to_string()));
    assert!(items.contains(&"prefix_b.txt".to_string()));
}

#[tokio::test]
async fn test_storage_trait_get_into_returns_byte_count() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut output = Vec::new();
    let bytes_written = Storage::get_into(&storage, &id, &mut output).await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_storage_trait_put_with_length_hint() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";
    let len = data.len() as u64;

    let cursor = std::io::Cursor::new(data);
    let mut reader = tokio::io::BufReader::new(cursor);

    storage
        .put(id.clone(), &mut reader, Some(len))
        .await
        .unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_trait_put_without_length_hint() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    let cursor = std::io::Cursor::new(data);
    let mut reader = tokio::io::BufReader::new(cursor);

    storage.put(id.clone(), &mut reader, None).await.unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_trait_overwrite_updates_data() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    storage.put_bytes(id.clone(), b"original").await.unwrap();
    let first = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(first, b"original");

    storage.put_bytes(id.clone(), b"updated").await.unwrap();
    let second = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(second, b"updated");
}

#[tokio::test]
async fn test_storage_trait_delete_idempotent() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    storage.put_bytes(id.clone(), b"data").await.unwrap();

    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();

    assert!(!storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_round_trip_empty_data() {
    let storage = MemoryStorage::new();
    let id = "empty.txt".to_string();

    storage.put_bytes(id.clone(), b"").await.unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, b"");
}

#[tokio::test]
async fn test_round_trip_binary_data() {
    let storage = MemoryStorage::new();
    let id = "binary.dat".to_string();
    let data: Vec<u8> = (0..=255).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_round_trip_unicode_data() {
    let storage = MemoryStorage::new();
    let id = "unicode.txt".to_string();
    let text = "Hello 世界 🌍 Привет مرحبا";

    storage
        .put_bytes(id.clone(), text.as_bytes())
        .await
        .unwrap();

    let retrieved = StorageExt::get_string(&storage, &id).await.unwrap();
    assert_eq!(retrieved, text);
}

#[tokio::test]
async fn test_concurrent_operations() {
    let storage = MemoryStorage::new();

    let write_handles: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let data = format!("data{}", i);
                storage.put_bytes(id, data.as_bytes()).await
            })
        })
        .collect();

    for handle in write_handles {
        handle.await.unwrap().unwrap();
    }

    let read_handles: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let expected = format!("data{}", i);
                let data = StorageExt::get_string(&storage, &id).await.unwrap();
                assert_eq!(data, expected);
            })
        })
        .collect();

    for handle in read_handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_multiple_storage_instances_independent() {
    let storage1 = MemoryStorage::new();
    let storage2 = MemoryStorage::new();
    let id = "test.txt".to_string();

    storage1.put_bytes(id.clone(), b"storage1").await.unwrap();
    storage2.put_bytes(id.clone(), b"storage2").await.unwrap();

    let data1 = StorageExt::get_bytes(&storage1, &id).await.unwrap();
    let data2 = StorageExt::get_bytes(&storage2, &id).await.unwrap();

    assert_eq!(data1, b"storage1");
    assert_eq!(data2, b"storage2");
}

#[tokio::test]
async fn test_copy_chain_multiple_storages() {
    let storage1 = MemoryStorage::new();
    let storage2 = MemoryStorage::new();
    let storage3 = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"chain copy data";

    storage1.put_bytes(id.clone(), data).await.unwrap();

    StorageExt::copy_to(&storage1, &id, &storage2)
        .await
        .unwrap();
    StorageExt::copy_to(&storage2, &id, &storage3)
        .await
        .unwrap();

    let data1 = StorageExt::get_bytes(&storage1, &id).await.unwrap();
    let data2 = StorageExt::get_bytes(&storage2, &id).await.unwrap();
    let data3 = StorageExt::get_bytes(&storage3, &id).await.unwrap();

    assert_eq!(data1, data);
    assert_eq!(data2, data);
    assert_eq!(data3, data);
}

#[tokio::test]
async fn test_list_pagination_behavior() {
    let storage = MemoryStorage::new();

    // Add 100 files
    for i in 0..100 {
        let id = format!("file{:03}.txt", i);
        storage.put_bytes(id, b"data").await.unwrap();
    }

    let stream = storage.list(None).await.unwrap();
    let mut count = 0;

    let mut stream = std::pin::pin!(stream);
    while let Some(result) = stream.next().await {
        result.unwrap();
        count += 1;
    }

    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_error_types_not_found() {
    let storage = MemoryStorage::new();
    let id = "nonexistent.txt".to_string();

    let result = StorageExt::get_bytes(&storage, &id).await;
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::NotFound(msg) => assert!(msg.contains("nonexistent.txt")),
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_storage_ext_methods_available() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    // Verify StorageExt methods are available
    storage.put_bytes(id.clone(), b"test").await.unwrap();
    let _ = StorageExt::get_bytes(&storage, &id).await.unwrap();
    let _ = StorageExt::get_string(&storage, &id).await.unwrap();

    let dest = MemoryStorage::new();
    StorageExt::copy_to(&storage, &id, &dest).await.unwrap();
}

#[tokio::test]
async fn test_empty_prefix_lists_all() {
    let storage = MemoryStorage::new();

    storage.put_bytes("a.txt".to_string(), b"1").await.unwrap();
    storage.put_bytes("b.txt".to_string(), b"2").await.unwrap();

    let empty_prefix = "".to_string();
    let stream_with_empty = storage.list(Some(&empty_prefix)).await.unwrap();
    let items_with_empty: Vec<_> = stream_with_empty.collect().await;

    let stream_without = storage.list(None).await.unwrap();
    let items_without: Vec<_> = stream_without.collect().await;

    // Empty prefix should behave similarly to no prefix
    assert_eq!(items_with_empty.len(), items_without.len());
}
