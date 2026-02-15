//! Comprehensive tests for FallbackStorage adapter

use futures::stream::StreamExt;
use stowage::multi::FallbackStorage;
use stowage::{Error, MemoryStorage, Storage, StorageExt};

#[tokio::test]
async fn test_new_fallback_storage() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    // Write-through is disabled by default
    assert!(!storage.is_write_through());
}

#[tokio::test]
async fn test_with_write_through() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone()).with_write_through(true);

    assert!(storage.is_write_through());
}

#[tokio::test]
async fn test_put_writes_to_primary_only_by_default() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();
    let data = b"hello world";

    StorageExt::put_bytes(&storage, id.clone(), data)
        .await
        .unwrap();

    assert!(primary.exists(&id).await.unwrap());
    assert!(!secondary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_get_from_primary() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();
    let data = b"primary data";

    StorageExt::put_bytes(&primary, id.clone(), data)
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, id.clone(), b"secondary data")
        .await
        .unwrap();

    // get_into only reads from primary
    let mut output = Vec::new();
    Storage::get_into(&storage, &id, &mut output).await.unwrap();
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_get_fails_when_primary_missing() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();
    let data = b"secondary data";

    // Put only in secondary
    StorageExt::put_bytes(&secondary, id.clone(), data)
        .await
        .unwrap();

    // get_into only reads from primary, so this should fail
    let mut output = Vec::new();
    let result = Storage::get_into(&storage, &id, &mut output).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_get_fails_when_both_missing() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    let id = "nonexistent.txt".to_string();

    let mut output = Vec::new();
    let result = Storage::get_into(&storage, &id, &mut output).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_exists_checks_both_storages() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id1 = "in_primary.txt".to_string();
    let id2 = "in_secondary.txt".to_string();
    let id3 = "nowhere.txt".to_string();

    StorageExt::put_bytes(&primary, id1.clone(), b"data")
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, id2.clone(), b"data")
        .await
        .unwrap();

    assert!(storage.exists(&id1).await.unwrap());
    assert!(storage.exists(&id2).await.unwrap());
    assert!(!storage.exists(&id3).await.unwrap());
}

#[tokio::test]
async fn test_delete_removes_from_both() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();

    StorageExt::put_bytes(&primary, id.clone(), b"primary")
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, id.clone(), b"secondary")
        .await
        .unwrap();

    storage.delete(&id).await.unwrap();

    // Both should be deleted
    assert!(!primary.exists(&id).await.unwrap());
    assert!(!secondary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_delete_idempotent() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    let id = "test.txt".to_string();

    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();
}

#[tokio::test]
async fn test_delete_succeeds_if_in_either() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();

    // Only in primary
    StorageExt::put_bytes(&primary, id.clone(), b"data")
        .await
        .unwrap();

    storage.delete(&id).await.unwrap();
    assert!(!primary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_write_through_enabled() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone()).with_write_through(true);

    let id = "test.txt".to_string();
    let data = b"write through data";

    StorageExt::put_bytes(&storage, id.clone(), data)
        .await
        .unwrap();

    // Both should have the data
    assert!(primary.exists(&id).await.unwrap());
    assert!(secondary.exists(&id).await.unwrap());

    let primary_data = StorageExt::get_bytes(&primary, &id).await.unwrap();
    let secondary_data = StorageExt::get_bytes(&secondary, &id).await.unwrap();
    assert_eq!(primary_data, data);
    assert_eq!(secondary_data, data);
}

#[tokio::test]
async fn test_write_through_disabled() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage =
        FallbackStorage::new(primary.clone(), secondary.clone()).with_write_through(false);

    let id = "test.txt".to_string();
    let data = b"no write through";

    StorageExt::put_bytes(&storage, id.clone(), data)
        .await
        .unwrap();

    // Only primary should have the data
    assert!(primary.exists(&id).await.unwrap());
    assert!(!secondary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_list_returns_primary_only() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    StorageExt::put_bytes(&primary, "file1.txt".to_string(), b"1")
        .await
        .unwrap();
    StorageExt::put_bytes(&primary, "file2.txt".to_string(), b"2")
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, "file3.txt".to_string(), b"3")
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, "file4.txt".to_string(), b"4")
        .await
        .unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    // Only primary files are listed
    assert_eq!(items.len(), 2);
    assert!(items.contains(&"file1.txt".to_string()));
    assert!(items.contains(&"file2.txt".to_string()));
}

#[tokio::test]
async fn test_list_with_prefix() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    StorageExt::put_bytes(&primary, "docs/a.txt".to_string(), b"a")
        .await
        .unwrap();
    StorageExt::put_bytes(&secondary, "docs/b.txt".to_string(), b"b")
        .await
        .unwrap();
    StorageExt::put_bytes(&primary, "other/c.txt".to_string(), b"c")
        .await
        .unwrap();

    let prefix = "docs/".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;

    // Only primary files with prefix
    assert_eq!(items.len(), 1);
    assert!(items.contains(&"docs/a.txt".to_string()));
}

#[tokio::test]
async fn test_list_empty() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.collect::<Vec<_>>().await;

    assert_eq!(items.len(), 0);
}

#[tokio::test]
async fn test_folder_exists_in_primary() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary);

    StorageExt::put_bytes(&primary, "folder/file.txt".to_string(), b"data")
        .await
        .unwrap();

    assert!(storage.folder_exists(&"folder".to_string()).await.unwrap());
    assert!(storage.folder_exists(&"folder/".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_folder_exists_in_secondary() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary.clone());

    StorageExt::put_bytes(&secondary, "folder/file.txt".to_string(), b"data")
        .await
        .unwrap();

    assert!(storage.folder_exists(&"folder".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_folder_exists_in_neither() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    assert!(
        !storage
            .folder_exists(&"nonexistent".to_string())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_overwrite_in_primary() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary);

    let id = "test.txt".to_string();

    StorageExt::put_bytes(&storage, id.clone(), b"first")
        .await
        .unwrap();
    StorageExt::put_bytes(&storage, id.clone(), b"second")
        .await
        .unwrap();

    let mut output = Vec::new();
    Storage::get_into(&storage, &id, &mut output).await.unwrap();
    assert_eq!(output, b"second");
}

#[tokio::test]
async fn test_copy_to_fallback_storage() {
    let primary1 = MemoryStorage::new();
    let secondary1 = MemoryStorage::new();
    let storage1 = FallbackStorage::new(primary1.clone(), secondary1);

    let primary2 = MemoryStorage::new();
    let secondary2 = MemoryStorage::new();
    let storage2 = FallbackStorage::new(primary2.clone(), secondary2);

    let id = "test.txt".to_string();
    let data = b"copy data";

    StorageExt::put_bytes(&storage1, id.clone(), data)
        .await
        .unwrap();
    StorageExt::copy_to(&storage1, &id, &storage2)
        .await
        .unwrap();

    assert!(primary2.exists(&id).await.unwrap());
    let retrieved = StorageExt::get_bytes(&storage2, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_concurrent_reads() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    // Prepare data in primary
    for i in 0..10 {
        let id = format!("file_{}.txt", i);
        StorageExt::put_bytes(&primary, id, format!("data{}", i).as_bytes())
            .await
            .unwrap();
    }

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let primary = primary.clone();
            tokio::spawn(async move {
                let id = format!("file_{}.txt", i);
                let expected = format!("data{}", i);
                let data = StorageExt::get_bytes(&primary, &id).await.unwrap();
                assert_eq!(data, expected.as_bytes());
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_writes() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let primary = primary.clone();
            tokio::spawn(async move {
                let id = format!("file{}.txt", i);
                let data = format!("data{}", i);
                StorageExt::put_bytes(&primary, id, data.as_bytes()).await
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    assert_eq!(primary.len(), 10);
}

#[tokio::test]
async fn test_write_to_primary_only() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "test.txt".to_string();
    StorageExt::put_bytes(&storage, id.clone(), b"data")
        .await
        .unwrap();

    // Verify it went to primary only
    assert!(primary.exists(&id).await.unwrap());
    assert!(!secondary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_large_data() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "large.bin".to_string();
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    StorageExt::put_bytes(&storage, id.clone(), &data)
        .await
        .unwrap();

    let mut output = Vec::new();
    Storage::get_into(&storage, &id, &mut output).await.unwrap();
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_empty_data() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary);

    let id = "empty.txt".to_string();
    let data = b"";

    StorageExt::put_bytes(&storage, id.clone(), data)
        .await
        .unwrap();

    let mut output = Vec::new();
    Storage::get_into(&storage, &id, &mut output).await.unwrap();
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_fallback_with_shared_backends() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage1 = FallbackStorage::new(primary.clone(), secondary.clone());

    StorageExt::put_bytes(&storage1, "test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage2 = FallbackStorage::new(primary.clone(), secondary.clone());

    // Should access same underlying storages
    assert!(storage2.exists(&"test.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_get_string_from_primary() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    let id = "text.txt".to_string();
    let text = "Hello from primary! 你好";

    StorageExt::put_bytes(&primary, id.clone(), text.as_bytes())
        .await
        .unwrap();

    let retrieved = StorageExt::get_string(&storage, &id).await.unwrap();
    assert_eq!(retrieved, text);
}

#[tokio::test]
async fn test_write_through_both_succeed() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone()).with_write_through(true);

    let id = "test.txt".to_string();
    let data = b"data";

    StorageExt::put_bytes(&storage, id.clone(), data)
        .await
        .unwrap();

    // Both should have succeeded
    assert!(primary.exists(&id).await.unwrap());
    assert!(secondary.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_primary_and_secondary_references() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary.clone(), secondary.clone());

    // Test that we can access the backends
    assert_eq!(storage.primary().len(), 0);
    assert_eq!(storage.secondary().len(), 0);
}

#[tokio::test]
async fn test_binary_data() {
    let primary = MemoryStorage::new();
    let secondary = MemoryStorage::new();
    let storage = FallbackStorage::new(primary, secondary);

    let id = "binary.dat".to_string();
    let data: Vec<u8> = (0..=255).collect();

    StorageExt::put_bytes(&storage, id.clone(), &data)
        .await
        .unwrap();

    let mut output = Vec::new();
    Storage::get_into(&storage, &id, &mut output).await.unwrap();
    assert_eq!(output, data);
}
