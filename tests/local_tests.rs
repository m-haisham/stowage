//! Comprehensive tests for LocalStorage adapter

use futures::stream::StreamExt;
use stowage::{Error, LocalStorage, Storage, StorageExt};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

/// Helper to create a temporary storage for testing
fn create_temp_storage() -> (LocalStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());
    (storage, temp_dir)
}

#[tokio::test]
async fn test_new_storage() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());

    assert_eq!(storage.root(), temp_dir.path());
}

#[tokio::test]
async fn test_put_and_exists() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();

    assert!(!storage.exists(&id).await.unwrap());

    let data = b"hello world";
    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_put_and_get_bytes() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_put_and_get_into() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut output = Vec::new();
    let bytes_written = storage.get_into(&id, &mut output).await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(output, data);
}

#[tokio::test]
async fn test_put_creates_parent_directories() {
    let (storage, _temp) = create_temp_storage();
    let id = "subdir/nested/deep/file.txt".to_string();
    let data = b"nested file";

    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_put_empty_data() {
    let (storage, _temp) = create_temp_storage();
    let id = "empty.txt".to_string();
    let data = b"";

    storage.put_bytes(id.clone(), data).await.unwrap();

    assert!(storage.exists(&id).await.unwrap());
    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_put_large_data() {
    let (storage, _temp) = create_temp_storage();
    let id = "large.bin".to_string();
    let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_nonexistent_returns_error() {
    let (storage, _temp) = create_temp_storage();
    let id = "nonexistent.txt".to_string();

    let result = storage.get_bytes(&id).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_get_into_nonexistent_returns_error() {
    let (storage, _temp) = create_temp_storage();
    let id = "nonexistent.txt".to_string();
    let mut output = Vec::new();

    let result = storage.get_into(&id, &mut output).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_exists_nonexistent_returns_false() {
    let (storage, _temp) = create_temp_storage();
    let id = "nonexistent.txt".to_string();

    assert!(!storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_delete_existing() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();
    assert!(storage.exists(&id).await.unwrap());

    storage.delete(&id).await.unwrap();

    assert!(!storage.exists(&id).await.unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent_is_idempotent() {
    let (storage, _temp) = create_temp_storage();
    let id = "nonexistent.txt".to_string();

    // Should not error
    storage.delete(&id).await.unwrap();
    storage.delete(&id).await.unwrap();
}

#[tokio::test]
async fn test_overwrite_existing() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();

    storage.put_bytes(id.clone(), b"original").await.unwrap();
    storage.put_bytes(id.clone(), b"updated").await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, b"updated");
}

#[tokio::test]
async fn test_reject_absolute_path() {
    let (storage, _temp) = create_temp_storage();
    let id = "/etc/passwd".to_string();

    let result = storage.put_bytes(id.clone(), b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_reject_parent_dir_traversal() {
    let (storage, _temp) = create_temp_storage();
    let id = "../outside.txt".to_string();

    let result = storage.put_bytes(id.clone(), b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_reject_parent_dir_in_middle() {
    let (storage, _temp) = create_temp_storage();
    let id = "dir/../../../outside.txt".to_string();

    let result = storage.put_bytes(id.clone(), b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_reject_empty_id() {
    let (storage, _temp) = create_temp_storage();
    let id = "".to_string();

    let result = storage.put_bytes(id.clone(), b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Generic(_)));
}

#[tokio::test]
async fn test_list_empty() {
    let (storage, _temp) = create_temp_storage();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.collect::<Vec<_>>().await;

    assert_eq!(items.len(), 0);
}

#[tokio::test]
async fn test_list_all() {
    let (storage, _temp) = create_temp_storage();

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
    assert!(items.contains(&"a.txt".to_string()));
    assert!(items.contains(&"b.txt".to_string()));
    assert!(items.contains(&"c.txt".to_string()));
}

#[tokio::test]
async fn test_list_with_nested_files() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("a.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("dir/b.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("dir/subdir/c.txt".to_string(), b"data")
        .await
        .unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert!(items.contains(&"a.txt".to_string()));
    assert!(items.contains(&"dir/b.txt".to_string()));
    assert!(items.contains(&"dir/subdir/c.txt".to_string()));
}

#[tokio::test]
async fn test_list_with_prefix() {
    let (storage, _temp) = create_temp_storage();

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
async fn test_list_nonexistent_prefix_returns_empty() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("file.txt".to_string(), b"data")
        .await
        .unwrap();

    let prefix = "nonexistent/".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.collect::<Vec<_>>().await;

    assert_eq!(items.len(), 0);
}

#[tokio::test]
async fn test_list_sorted() {
    let (storage, _temp) = create_temp_storage();

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
    let (storage, _temp) = create_temp_storage();
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
    let (storage, _temp) = create_temp_storage();
    let id = "binary.dat".to_string();
    let data = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let result = storage.get_string(&id).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Generic(_)));
}

#[tokio::test]
async fn test_clone_storage() {
    let temp_dir = TempDir::new().unwrap();
    let storage1 = LocalStorage::new(temp_dir.path());

    storage1
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage2 = storage1.clone();

    // Both should point to the same root
    assert_eq!(storage1.root(), storage2.root());

    // Data should be accessible from both
    let data = storage2.get_bytes(&"test.txt".to_string()).await.unwrap();
    assert_eq!(data, b"data");
}

#[tokio::test]
async fn test_copy_to_different_storage() {
    let (source, _temp1) = create_temp_storage();
    let (dest, _temp2) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"test data";

    source.put_bytes(id.clone(), data).await.unwrap();

    source.copy_to(&id, &dest).await.unwrap();

    // Both should have the data
    assert!(source.exists(&id).await.unwrap());
    assert!(dest.exists(&id).await.unwrap());

    let source_data = source.get_bytes(&id).await.unwrap();
    let dest_data = dest.get_bytes(&id).await.unwrap();
    assert_eq!(source_data, dest_data);
}

#[tokio::test]
async fn test_put_with_async_read() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello from async reader";

    let cursor = std::io::Cursor::new(data);
    let mut reader = tokio::io::BufReader::new(cursor);

    storage
        .put(id.clone(), &mut reader, Some(data.len() as u64))
        .await
        .unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_into_with_async_write() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut buffer = Vec::new();
    let mut writer = tokio::io::BufWriter::new(&mut buffer);

    let bytes_written = storage.get_into(&id, &mut writer).await.unwrap();
    writer.flush().await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(buffer, data);
}

#[tokio::test]
async fn test_debug_impl() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());

    let debug_output = format!("{:?}", storage);
    assert!(debug_output.contains("LocalStorage"));
    assert!(debug_output.contains("root"));
}

#[tokio::test]
async fn test_special_characters_in_id() {
    let (storage, _temp) = create_temp_storage();

    let ids = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
    ];

    for id in ids {
        storage.put_bytes(id.to_string(), b"data").await.unwrap();
        assert!(storage.exists(&id.to_string()).await.unwrap());
        let data = storage.get_bytes(&id.to_string()).await.unwrap();
        assert_eq!(data, b"data");
    }
}

#[tokio::test]
async fn test_binary_data() {
    let (storage, _temp) = create_temp_storage();
    let id = "binary.dat".to_string();
    let data: Vec<u8> = (0..=255).collect();

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_concurrent_writes_different_files() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());

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

    // Verify all files exist
    for i in 0..10 {
        let id = format!("file{}.txt", i);
        assert!(storage.exists(&id).await.unwrap());
    }
}

#[tokio::test]
async fn test_concurrent_reads() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());

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
                let data = storage.get_bytes(&id).await.unwrap();
                assert_eq!(data, expected.as_bytes());
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_persistence_across_instances() {
    let temp_dir = TempDir::new().unwrap();
    let id = "test.txt".to_string();
    let data = b"persistent data";

    // Write with first instance
    {
        let storage = LocalStorage::new(temp_dir.path());
        storage.put_bytes(id.clone(), data).await.unwrap();
    }

    // Read with second instance
    {
        let storage = LocalStorage::new(temp_dir.path());
        let retrieved = storage.get_bytes(&id).await.unwrap();
        assert_eq!(retrieved, data);
    }
}

#[tokio::test]
async fn test_atomic_write_on_overwrite() {
    let (storage, _temp) = create_temp_storage();
    let id = "atomic.txt".to_string();

    // Write initial data
    storage.put_bytes(id.clone(), b"initial").await.unwrap();

    // Overwrite with new data
    storage.put_bytes(id.clone(), b"updated").await.unwrap();

    // Should have the new data, not corrupted or mixed
    let retrieved = storage.get_bytes(&id).await.unwrap();
    assert_eq!(retrieved, b"updated");
}

#[tokio::test]
async fn test_list_deep_nested_structure() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("a/b/c/d/e/file.txt".to_string(), b"deep")
        .await
        .unwrap();
    storage
        .put_bytes("a/b/other.txt".to_string(), b"data")
        .await
        .unwrap();
    storage
        .put_bytes("a/file.txt".to_string(), b"data")
        .await
        .unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert!(items.contains(&"a/b/c/d/e/file.txt".to_string()));
    assert!(items.contains(&"a/b/other.txt".to_string()));
    assert!(items.contains(&"a/file.txt".to_string()));
}

#[tokio::test]
async fn test_exists_directory_returns_false() {
    let (storage, temp) = create_temp_storage();

    // Create a directory manually
    let dir_path = temp.path().join("just_a_dir");
    tokio::fs::create_dir(&dir_path).await.unwrap();

    // exists should return false for directories
    assert!(!storage.exists(&"just_a_dir".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_path_normalization_forward_slashes() {
    let (storage, _temp) = create_temp_storage();
    let id = "dir/subdir/file.txt".to_string();
    let data = b"test";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let stream = storage.list(None).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    // Should use forward slashes consistently
    assert!(items.contains(&"dir/subdir/file.txt".to_string()));
}
