//! Tests for error handling and edge cases

use stowage::{Error, MemoryStorage, Storage, StorageExt};

#[tokio::test]
async fn test_not_found_error() {
    let storage = MemoryStorage::new();
    let id = "nonexistent.txt".to_string();

    let result = StorageExt::get_bytes(&storage, &id).await;
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::NotFound(msg) => {
            assert_eq!(msg, "nonexistent.txt");
        }
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_not_found_error_formatting() {
    let err = Error::NotFound("test.txt".to_string());
    let formatted = format!("{}", err);
    assert!(formatted.contains("File not found"));
    assert!(formatted.contains("test.txt"));
}

#[tokio::test]
async fn test_generic_error_formatting() {
    let err = Error::Generic("Something went wrong".to_string());
    let formatted = format!("{}", err);
    assert!(formatted.contains("Generic storage error"));
    assert!(formatted.contains("Something went wrong"));
}

#[tokio::test]
async fn test_permission_denied_error_formatting() {
    let err = Error::PermissionDenied("Access denied".to_string());
    let formatted = format!("{}", err);
    assert!(formatted.contains("Permission denied"));
    assert!(formatted.contains("Access denied"));
}

#[tokio::test]
async fn test_io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err: Error = io_err.into();

    match storage_err {
        Error::Io(_) => {
            // Successfully converted
        }
        _ => panic!("Expected Io error"),
    }
}

#[tokio::test]
async fn test_error_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Error>();
    assert_sync::<Error>();
}

#[tokio::test]
async fn test_error_debug_impl() {
    let err = Error::NotFound("test.txt".to_string());
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("NotFound"));
    assert!(debug_str.contains("test.txt"));
}

#[tokio::test]
async fn test_get_bytes_on_nonexistent_returns_not_found() {
    let storage = MemoryStorage::new();
    let result = StorageExt::get_bytes(&storage, &"missing.txt".to_string()).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_get_string_on_nonexistent_returns_not_found() {
    let storage = MemoryStorage::new();
    let result = StorageExt::get_string(&storage, &"missing.txt".to_string()).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_get_into_on_nonexistent_returns_not_found() {
    let storage = MemoryStorage::new();
    let mut output = Vec::new();
    let result = Storage::get_into(&storage, &"missing.txt".to_string(), &mut output).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_copy_to_nonexistent_source_returns_not_found() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();
    let result = StorageExt::copy_to(&source, &"nonexistent.txt".to_string(), &dest).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
}

#[tokio::test]
async fn test_get_string_invalid_utf8_returns_generic() {
    let storage = MemoryStorage::new();
    let id = "invalid.txt".to_string();
    let invalid_utf8 = vec![0xFF, 0xFE, 0xFD, 0xFC];

    storage.put_bytes(id.clone(), &invalid_utf8).await.unwrap();

    let result = StorageExt::get_string(&storage, &id).await;
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::Generic(msg) => {
            assert!(msg.contains("invalid utf-8"));
        }
        _ => panic!("Expected Generic error for invalid UTF-8"),
    }
}

#[tokio::test]
async fn test_error_chain_with_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
    let storage_err: Error = io_err.into();

    let formatted = format!("{}", storage_err);
    assert!(formatted.contains("IO Error"));
}

#[tokio::test]
async fn test_multiple_errors_are_distinct() {
    let err1 = Error::NotFound("file1.txt".to_string());
    let err2 = Error::NotFound("file2.txt".to_string());

    let str1 = format!("{}", err1);
    let str2 = format!("{}", err2);

    assert!(str1.contains("file1.txt"));
    assert!(str2.contains("file2.txt"));
    assert_ne!(str1, str2);
}

#[tokio::test]
async fn test_delete_nonexistent_succeeds() {
    let storage = MemoryStorage::new();
    let id = "never_existed.txt".to_string();

    // Delete should be idempotent and not error on nonexistent files
    let result = storage.delete(&id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_exists_on_nonexistent_returns_false_not_error() {
    let storage = MemoryStorage::new();
    let id = "nonexistent.txt".to_string();

    let result = storage.exists(&id).await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn test_empty_id_behavior() {
    let storage = MemoryStorage::new();
    let id = "".to_string();

    // Empty ID should be allowed in MemoryStorage (just a key)
    let result = storage.put_bytes(id.clone(), b"data").await;
    assert!(result.is_ok());

    let exists = storage.exists(&id).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn test_very_long_id() {
    let storage = MemoryStorage::new();
    let id = "a".repeat(10000);
    let data = b"data";

    let result = StorageExt::put_bytes(&storage, id.clone(), data).await;
    assert!(result.is_ok());

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_special_characters_in_error_messages() {
    let special_id = "file with spaces & special <chars>.txt".to_string();
    let err = Error::NotFound(special_id.clone());
    let formatted = format!("{}", err);

    assert!(formatted.contains(&special_id));
}

#[tokio::test]
async fn test_concurrent_errors_independent() {
    let storage = MemoryStorage::new();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let id = format!("nonexistent{}.txt", i);
                StorageExt::get_bytes(&storage, &id).await
            })
        })
        .collect();

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
    }
}

#[tokio::test]
async fn test_list_on_empty_storage_no_error() {
    let storage = MemoryStorage::new();
    let result = storage.list(None).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_list_with_nonexistent_prefix_no_error() {
    let storage = MemoryStorage::new();
    storage
        .put_bytes("file.txt".to_string(), b"data")
        .await
        .unwrap();

    let prefix = "nonexistent/".to_string();
    let result = storage.list(Some(&prefix)).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_result_type_alias() {
    fn returns_result() -> stowage::Result<String> {
        Ok("test".to_string())
    }

    let result = returns_result();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "test");
}

#[tokio::test]
async fn test_error_source_trait() {
    use std::error::Error as StdError;

    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err: Error = io_err.into();

    // Error should implement std::error::Error
    let _: &dyn StdError = &storage_err;
}

#[tokio::test]
async fn test_connection_error_with_boxed_error() {
    let inner_err =
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(inner_err);
    let storage_err = Error::Connection(boxed);

    let formatted = format!("{}", storage_err);
    assert!(formatted.contains("Storage backend connection error"));
}

#[tokio::test]
async fn test_error_from_multiple_sources() {
    // Test that we can create errors from different sources
    let not_found = Error::NotFound("test".to_string());
    let permission = Error::PermissionDenied("denied".to_string());
    let generic = Error::Generic("generic".to_string());

    let errors = vec![not_found, permission, generic];

    for err in errors {
        let _ = format!("{}", err);
        let _ = format!("{:?}", err);
    }
}

#[tokio::test]
async fn test_put_bytes_with_zero_length() {
    let storage = MemoryStorage::new();
    let id = "zero.txt".to_string();
    let data: &[u8] = &[];

    let result = StorageExt::put_bytes(&storage, id.clone(), data).await;
    assert!(result.is_ok());

    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved.len(), 0);
}

#[tokio::test]
async fn test_operations_after_error() {
    let storage = MemoryStorage::new();
    let id = "test.txt".to_string();

    // First operation fails
    let _ = StorageExt::get_bytes(&storage, &id).await;

    // Storage should still be usable
    storage.put_bytes(id.clone(), b"data").await.unwrap();
    let retrieved = StorageExt::get_bytes(&storage, &id).await.unwrap();
    assert_eq!(retrieved, b"data");
}
