//! Comprehensive tests for LocalStorage adapter
#![cfg(feature = "local")]

use futures::stream::StreamExt;
use stowage::adapters::local::LocalStorage;
use stowage::{Error, Storage, StorageExt};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

#[path = "test_common/mod.rs"]
mod test_common;

/// Helper to create a temporary storage for testing
/// Returns (storage, tempdir) where tempdir must be kept alive
fn create_temp_storage() -> (LocalStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());
    (storage, temp_dir)
}

// ============================================================================
// Common test suite using test_common helpers
// ============================================================================

#[tokio::test]
async fn test_put_and_exists() {
    test_common::test_put_and_exists(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_put_and_get_bytes() {
    test_common::test_put_and_get_bytes(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    test_common::test_get_nonexistent(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_delete_existing() {
    test_common::test_delete_existing(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_delete_idempotent() {
    test_common::test_delete_idempotent(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_overwrite() {
    test_common::test_overwrite(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_empty_data() {
    test_common::test_empty_data(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_large_data() {
    test_common::test_large_data(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_binary_data() {
    test_common::test_binary_data(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_get_into() {
    test_common::test_get_into(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_folder_exists() {
    test_common::test_folder_exists(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_folder_exists_nested() {
    test_common::test_folder_exists_nested(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

#[tokio::test]
async fn test_special_characters() {
    test_common::test_special_characters(&mut || async {
        let (storage, _temp) = create_temp_storage();
        storage
    })
    .await;
}

// ============================================================================
// LocalStorage-specific tests
// ============================================================================

#[tokio::test]
async fn test_new_storage() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(temp_dir.path());

    assert_eq!(storage.root(), temp_dir.path());
}

#[tokio::test]
async fn test_put_and_get_into() {
    let (storage, _temp) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"hello world";

    storage.put_bytes(id.clone(), data).await.unwrap();

    let mut output = Vec::new();
    let bytes_written = Storage::get_into(&storage, &id, &mut output).await.unwrap();

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
    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_into_nonexistent_returns_error() {
    let (storage, _temp) = create_temp_storage();
    let id = "nonexistent.txt".to_string();
    let mut output = Vec::new();

    let result = Storage::get_into(&storage, &id, &mut output).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
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
    assert_eq!(items, vec!["a.txt", "b.txt", "c.txt"]);
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

    let prefix = "files".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 2);
    assert!(items.contains(&"files/a.txt".to_string()));
    assert!(items.contains(&"files/b.txt".to_string()));
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

    let retrieved = StorageExt::get_string(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_get_string_invalid_utf8() {
    let (storage, _temp) = create_temp_storage();
    let id = "binary.dat".to_string();
    let data = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8

    storage.put_bytes(id.clone(), &data).await.unwrap();

    let result = StorageExt::get_string(&storage, &id).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Generic(_)));
}

#[tokio::test]
async fn test_copy_to_same_storage() {
    let (storage, _temp) = create_temp_storage();
    let source_id = "source.txt".to_string();
    let data = b"test data";

    storage.put_bytes(source_id.clone(), data).await.unwrap();

    StorageExt::copy_to(&storage, &source_id, &storage)
        .await
        .unwrap();

    // Source should still exist
    assert!(storage.exists(&source_id).await.unwrap());

    let retrieved = StorageExt::get_bytes(&storage, &source_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_copy_to_different_storage() {
    let (source, _temp1) = create_temp_storage();
    let (dest, _temp2) = create_temp_storage();
    let id = "test.txt".to_string();
    let data = b"test data";

    source.put_bytes(id.clone(), data).await.unwrap();

    StorageExt::copy_to(&source, &id, &dest).await.unwrap();

    // Both should have the data
    assert!(source.exists(&id).await.unwrap());
    assert!(dest.exists(&id).await.unwrap());

    let source_data = StorageExt::get_bytes(&source, &id).await.unwrap();
    let dest_data = StorageExt::get_bytes(&dest, &id).await.unwrap();
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

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
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

    let bytes_written = Storage::get_into(&storage, &id, &mut writer).await.unwrap();
    writer.flush().await.unwrap();

    assert_eq!(bytes_written, data.len() as u64);
    assert_eq!(buffer, data);
}

#[tokio::test]
async fn test_multiple_files_with_same_prefix() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("prefix/1.txt".to_string(), b"1")
        .await
        .unwrap();
    storage
        .put_bytes("prefix/2.txt".to_string(), b"2")
        .await
        .unwrap();
    storage
        .put_bytes("prefix/10.txt".to_string(), b"10")
        .await
        .unwrap();
    storage
        .put_bytes("other.txt".to_string(), b"other")
        .await
        .unwrap();

    let prefix = "prefix".to_string();
    let stream = storage.list(Some(&prefix)).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert!(items.contains(&"prefix/1.txt".to_string()));
    assert!(items.contains(&"prefix/2.txt".to_string()));
    assert!(items.contains(&"prefix/10.txt".to_string()));
}

#[tokio::test]
async fn test_folder_exists_with_trailing_slash() {
    let (storage, _temp) = create_temp_storage();

    // Create files under "folder/"
    storage
        .put_bytes("folder/file1.txt".to_string(), b"data1")
        .await
        .unwrap();
    storage
        .put_bytes("folder/file2.txt".to_string(), b"data2")
        .await
        .unwrap();

    // Check folder exists (without trailing slash - it's a directory on disk)
    assert!(storage.folder_exists(&"folder".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_folder_exists_nonexistent() {
    let (storage, _temp) = create_temp_storage();

    // Create some files
    storage
        .put_bytes("docs/readme.txt".to_string(), b"data")
        .await
        .unwrap();

    // Check non-existent folder
    assert!(!storage.folder_exists(&"images".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_folder_exists_empty_storage() {
    let (storage, _temp) = create_temp_storage();

    // No folders should exist in empty storage
    assert!(!storage.folder_exists(&"any".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_path_validation_absolute_path() {
    let (storage, _temp) = create_temp_storage();
    let id = "/etc/passwd".to_string();

    let result = storage.put_bytes(id, b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_path_validation_parent_traversal() {
    let (storage, _temp) = create_temp_storage();
    let id = "../outside.txt".to_string();

    let result = storage.put_bytes(id, b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_path_validation_nested_parent_traversal() {
    let (storage, _temp) = create_temp_storage();
    let id = "foo/../../outside.txt".to_string();

    let result = storage.put_bytes(id, b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::PermissionDenied(_)));
}

#[tokio::test]
async fn test_path_validation_empty_id() {
    let (storage, _temp) = create_temp_storage();
    let id = "".to_string();

    let result = storage.put_bytes(id, b"data").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Generic(_)));
}

#[tokio::test]
async fn test_concurrent_writes() {
    let (storage, _temp) = create_temp_storage();

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
    let (storage, _temp) = create_temp_storage();

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
async fn test_nested_directories() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("a/b/c/d/e/file.txt".to_string(), b"deeply nested")
        .await
        .unwrap();

    assert!(
        storage
            .exists(&"a/b/c/d/e/file.txt".to_string())
            .await
            .unwrap()
    );

    let data = StorageExt::get_bytes(&storage, &"a/b/c/d/e/file.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"deeply nested");
}

#[tokio::test]
async fn test_list_nested_structure() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("a/1.txt".to_string(), b"1")
        .await
        .unwrap();
    storage
        .put_bytes("a/b/2.txt".to_string(), b"2")
        .await
        .unwrap();
    storage
        .put_bytes("a/b/c/3.txt".to_string(), b"3")
        .await
        .unwrap();
    storage.put_bytes("x.txt".to_string(), b"x").await.unwrap();

    let stream = storage.list(Some(&"a".to_string())).await.unwrap();
    let items: Vec<_> = stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

    assert_eq!(items.len(), 3);
    assert!(items.contains(&"a/1.txt".to_string()));
    assert!(items.contains(&"a/b/2.txt".to_string()));
    assert!(items.contains(&"a/b/c/3.txt".to_string()));
}

#[tokio::test]
async fn test_delete_does_not_remove_empty_directories() {
    let (storage, _temp) = create_temp_storage();

    storage
        .put_bytes("dir/file.txt".to_string(), b"data")
        .await
        .unwrap();
    storage.delete(&"dir/file.txt".to_string()).await.unwrap();

    // Directory should still exist (empty)
    assert!(storage.folder_exists(&"dir".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_clone_storage() {
    let (storage1, _temp) = create_temp_storage();
    storage1
        .put_bytes("test.txt".to_string(), b"data")
        .await
        .unwrap();

    let storage2 = storage1.clone();

    // Both should reference the same root
    assert_eq!(storage2.root(), storage1.root());
    let data = StorageExt::get_bytes(&storage2, &"test.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"data");
}
