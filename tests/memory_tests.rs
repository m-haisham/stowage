//! Comprehensive tests for MemoryStorage adapter

use futures::stream::StreamExt;
use std::collections::HashMap;
use stowage::{Error, MemoryStorage, Storage, StorageExt};

#[path = "test_common/mod.rs"]
mod test_common;

// Common test suite tests

#[tokio::test]
async fn test_put_and_exists() {
    test_common::test_put_and_exists(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_put_and_get_bytes() {
    test_common::test_put_and_get_bytes(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    test_common::test_get_nonexistent(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_delete_existing() {
    test_common::test_delete_existing(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_delete_idempotent() {
    test_common::test_delete_idempotent(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_overwrite() {
    test_common::test_overwrite(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_empty_data() {
    test_common::test_empty_data(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_large_data() {
    test_common::test_large_data(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_binary_data() {
    test_common::test_binary_data(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_get_into() {
    test_common::test_get_into(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_folder_exists() {
    test_common::test_folder_exists(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_folder_exists_nested() {
    test_common::test_folder_exists_nested(&mut || async { MemoryStorage::new() }).await;
}

#[tokio::test]
async fn test_special_characters() {
    test_common::test_special_characters(&mut || async { MemoryStorage::new() }).await;
}

// MemoryStorage-specific tests

#[tokio::test]
async fn test_new_storage_is_empty() {
    let storage = MemoryStorage::new();
    assert!(storage.is_empty());
    assert_eq!(storage.len(), 0);
}

#[tokio::test]
async fn test_from_map() {
    let mut map = HashMap::new();
    map.insert("key1".to_string(), b"value1".to_vec());
    map.insert("key2".to_string(), b"value2".to_vec());

    let storage = MemoryStorage::from_map(map);
    assert_eq!(storage.len(), 2);
    assert!(!storage.is_empty());
}

#[tokio::test]
async fn test_clear() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("file1.txt".to_string(), b"data1")
        .await
        .unwrap();
    storage
        .put_bytes("file2.txt".to_string(), b"data2")
        .await
        .unwrap();
    storage
        .put_bytes("file3.txt".to_string(), b"data3")
        .await
        .unwrap();

    assert_eq!(storage.len(), 3);

    storage.clear();

    assert_eq!(storage.len(), 0);
    assert!(storage.is_empty());
}

#[tokio::test]
async fn test_list_empty() {
    let storage = MemoryStorage::new();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.collect::<Vec<_>>().await;

    assert_eq!(items.len(), 0);
}

#[tokio::test]
async fn test_list_all() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("a.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("b.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("c.txt".to_string(), b"data")
        .await
        .unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert_eq!(items, vec!["a.txt", "b.txt", "c.txt"]);
}

#[tokio::test]
async fn test_list_with_prefix() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("files/a.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("files/b.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("docs/c.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("docs/d.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("readme.txt".to_string(), b"data")
        .await
        .unwrap();

    let prefix = "files/".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 2);
    assert!(items.contains(&"files/a.txt".to_string()));
    assert!(items.contains(&"files/b.txt".to_string()));
}

#[tokio::test]
async fn test_list_sorted() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("zebra.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("apple.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("mango.txt".to_string(), b"data")
        .await
        .unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items, vec!["apple.txt", "mango.txt", "zebra.txt"]);
}

#[tokio::test]
async fn test_get_string() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = "Hello, World! 你好世界";

    storage
        .put_bytes(id.clone(), data.as_bytes())
        .await
        .unwrap();

    let retrieved = storage.get_string(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_string_invalid_utf8() {
    let storage = MemoryStorage::new();
    let id = "binary.dat".to_string();
    let data = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let result = storage.get_string(&id).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Generic(_)));
}

#[tokio::test]
async fn test_clone_storage() {
    let storage1 = MemoryStorage::new();
    storage1
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage2 = storage1.clone();

    // Both should reference the same underlying storage
    assert_eq!(storage2.len(), 1);
    let data = StorageExt::get_bytes(&storage2, &"test.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"data");

    // Modifications through one should be visible in the other
    storage2
        .put_bytes("new.txt".to_string(), b"new data")
        .await
        .unwrap();
    assert_eq!(storage1.len(), 2);
}

#[tokio::test]
async fn test_concurrent_writes() {
    let storage = MemoryStorage::new();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let data = format!("data{}", i);
                storage.put_bytes(id, data.as_bytes()).await
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    assert_eq!(storage.len(), 10);
}

#[tokio::test]
async fn test_concurrent_reads() {
    let storage = MemoryStorage::new();

    // Prepare data
    for i in 0..10 {
        let id = format!("file{}.txt", i);
        let data = format!("data{}", i);
        storage.put_bytes(id, data.as_bytes()).await.unwrap();
    }

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let expected = format!("data{}", i);
                let data = StorageExt::get_bytes(&storage, &id).await.unwrap();
                assert_eq!(data, expected.as_bytes());
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_copy_to_same_storage() {
    let storage = MemoryStorage::new();
    let source_id = "source.txt".to_string();
    let data = b"test data";

    storage.put_bytes(source_id.clone(), data).await.unwrap();

    storage.copy_to(&source_id, &storage).await.unwrap();

    // Source should still exist
    assert!(storage.exists(&source_id).await.unwrap());

    // Note: copy_to uses the same ID, so it would overwrite
    let retrieved = StorageExt::get_bytes(&storage, &source_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_copy_to_different_storage() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"test data";

    source.put_bytes(id.clone(), data).await.unwrap();

    source.copy_to(&id, &dest).await.unwrap();

    // Both should have the data
    assert!(source.exists(&id).await.unwrap());
    assert!(dest.exists(&id).await.unwrap());

    let source_data = StorageExt::get_bytes(&source, &id).await.unwrap();
    let dest_data = StorageExt::get_bytes(&dest, &id).await.unwrap();
    assert_eq!(source_data, dest_data);
}

#[tokio::test]
async fn test_put_with_async_read() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello from async reader";

    let cursor = std::io::Cursor::new(data);
    let mut reader = tokio::io::BufReader::new(cursor);

    storage
        .put(id.clone(), &mut reader, Some(data.len() as u64))
        .await
        .unwrap();

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_into_with_async_write() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut buffer = Vec::new();
    let mut writer = tokio::io::BufWriter::new(&mut buffer);

    let bytes_written = Storage::get_into(&storage, &id, &mut writer).await.unwrap();
    use tokio::io::AsyncWriteExt;
    writer.flush().await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(buffer, data);
}

#[tokio::test]
async fn test_multiple_files_with_same_prefix() {
    let storage = MemoryStorage::new();

    storage
        .put_bytes("prefix_1.txt".to_string(), b"1")
        .await
        .unwrap();
    storage
        .put_bytes("prefix_2.txt".to_string(), b"2")
        .await
        .unwrap();
    storage
        .put_bytes("prefix_10.txt".to_string(), b"10")
        .await
        .unwrap();
    storage
        .put_bytes("other.txt".to_string(), b"other")
        .await
        .unwrap();

    let prefix = "prefix_".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert!(items.contains(&"prefix_1.txt".to_string()));
    assert!(items.contains(&"prefix_2.txt".to_string()));
    assert!(items.contains(&"prefix_10.txt".to_string()));
}

#[tokio::test]
async fn test_debug_impl() {
    let storage = MemoryStorage::new();
    storage
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    let debug_output = format!("{:?}", storage);
    assert!(debug_output.contains("MemoryStorage"));
    assert!(debug_output.contains("len"));
}

#[tokio::test]
async fn test_len_after_operations() {
    let storage = MemoryStorage::new();
    assert_eq!(storage.len(), 0);

    storage.put_bytes("a.txt".to_string(), b"1").await.unwrap();
    assert_eq!(storage.len(), 1);

    storage.put_bytes("b.txt".to_string(), b"2").await.unwrap();
    assert_eq!(storage.len(), 2);

    storage.delete(&"a.txt".to_string()).await.unwrap();
    assert_eq!(storage.len(), 1);

    storage.delete(&"b.txt".to_string()).await.unwrap();
    assert_eq!(storage.len(), 0);
}
