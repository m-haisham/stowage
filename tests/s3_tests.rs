//! Integration tests for S3Storage adapter
//!
//! These tests require a local S3-compatible service like MinIO.
//!
//! ## Setup
//!
//! Run MinIO with Docker:
//! ```bash
//! docker run -d -p 9000:9000 -p 9001:9001 \
//!   -e "MINIO_ROOT_USER=minioadmin" \
//!   -e "MINIO_ROOT_PASSWORD=minioadmin" \
//!   minio/minio server /data --console-address ":9001"
//! ```
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all S3 tests (they're marked with #[ignore])
//! cargo test --features s3 -- --ignored
//!
//! # Or run a specific test
//! cargo test --features s3 test_s3_put_and_exists -- --ignored
//! ```

#[cfg(feature = "s3")]
mod s3_integration_tests {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::{Credentials, Region};
    use stowage::{Error, S3Storage, Storage, StorageExt};

    /// Create an S3 client configured for MinIO
    async fn create_minio_client() -> Client {
        let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://localhost:9000")
            .credentials_provider(creds)
            .force_path_style(true) // Required for MinIO
            .build();

        Client::from_conf(config)
    }

    /// Generate a unique bucket name for testing
    fn unique_bucket_name() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("stowage-test-{}-{}", timestamp, counter)
    }

    /// Setup a test bucket and return storage instance
    async fn setup_test_storage() -> S3Storage {
        let client = create_minio_client().await;
        let bucket_name = unique_bucket_name();

        // Create the bucket (ignore if it already exists)
        let create_result = client.create_bucket().bucket(&bucket_name).send().await;

        // Ignore BucketAlreadyOwnedByYou error
        match create_result {
            Ok(_) => {}
            Err(e) => {
                let err_str = e.to_string();
                if !err_str.contains("BucketAlreadyOwnedByYou")
                    && !err_str.contains("BucketAlreadyExists")
                {
                    panic!(
                        "Failed to create test bucket. Is MinIO running on localhost:9000?: {}",
                        e
                    );
                }
            }
        }

        // Give MinIO a moment to fully create the bucket
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        S3Storage::new(client, bucket_name)
    }

    /// Cleanup: delete all objects and the bucket
    async fn cleanup_storage(storage: &S3Storage) {
        let client = create_minio_client().await;
        let bucket = storage.bucket().to_string();

        // List and delete all objects
        if let Ok(objects) = client.list_objects_v2().bucket(&bucket).send().await {
            let contents = objects.contents();
            for obj in contents {
                if let Some(key) = obj.key() {
                    let _ = client.delete_object().bucket(&bucket).key(key).send().await;
                }
            }
        }

        // Delete the bucket
        let _ = client.delete_bucket().bucket(&bucket).send().await;
    }

    // Basic operations tests

    #[tokio::test]
    #[ignore]
    async fn test_s3_put_and_exists() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();

        assert!(!storage.exists(&key).await.unwrap());

        let data = b"hello world";
        storage.put_bytes(key.clone(), data).await.unwrap();

        assert!(storage.exists(&key).await.unwrap());

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_put_and_get_bytes() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();
        let data = b"hello world";

        storage.put_bytes(key.clone(), data).await.unwrap();

        let retrieved = storage.get_bytes(&key).await.unwrap();
        assert_eq!(retrieved, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_get_nonexistent() {
        let storage = setup_test_storage().await;
        let key = "nonexistent.txt".to_string();

        let result = storage.get_bytes(&key).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NotFound(_) => {}   // Expected
            Error::Connection(_) => {} // Also acceptable for S3 (404 can be wrapped)
            e => panic!("Expected NotFound or Connection error, got: {:?}", e),
        }

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_delete_existing() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();
        let data = b"hello world";

        storage.put_bytes(key.clone(), data).await.unwrap();
        assert!(storage.exists(&key).await.unwrap());

        storage.delete(&key).await.unwrap();
        assert!(!storage.exists(&key).await.unwrap());

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_delete_idempotent() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();

        // Delete non-existent file should not error
        storage.delete(&key).await.unwrap();
        storage.delete(&key).await.unwrap();

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_overwrite() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();

        storage.put_bytes(key.clone(), b"original").await.unwrap();
        storage.put_bytes(key.clone(), b"updated").await.unwrap();

        let retrieved = storage.get_bytes(&key).await.unwrap();
        assert_eq!(retrieved, b"updated");

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_empty_data() {
        let storage = setup_test_storage().await;
        let key = "empty.txt".to_string();
        let data = b"";

        storage.put_bytes(key.clone(), data).await.unwrap();

        assert!(storage.exists(&key).await.unwrap());
        let retrieved = storage.get_bytes(&key).await.unwrap();
        assert_eq!(retrieved, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_large_data() {
        let storage = setup_test_storage().await;
        let key = "large.bin".to_string();
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        storage.put_bytes(key.clone(), &data).await.unwrap();

        let retrieved = storage.get_bytes(&key).await.unwrap();
        assert_eq!(retrieved, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_binary_data() {
        let storage = setup_test_storage().await;
        let key = "binary.dat".to_string();
        let data: Vec<u8> = (0..=255).collect();

        storage.put_bytes(key.clone(), &data).await.unwrap();

        let retrieved = storage.get_bytes(&key).await.unwrap();
        assert_eq!(retrieved, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_get_into() {
        let storage = setup_test_storage().await;
        let key = "test.txt".to_string();
        let data = b"hello world";

        storage.put_bytes(key.clone(), data).await.unwrap();

        let mut output = Vec::new();
        let bytes_written = Storage::get_into(&storage, &key, &mut output)
            .await
            .unwrap();

        assert_eq!(bytes_written, data.len() as u64);
        assert_eq!(output, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_folder_exists() {
        let storage = setup_test_storage().await;

        // Create files under "folder/"
        storage
            .put_bytes("folder/file1.txt".to_string(), b"data1")
            .await
            .unwrap();

        storage
            .put_bytes("folder/file2.txt".to_string(), b"data2")
            .await
            .unwrap();

        // Check folder exists (with and without trailing slash)
        assert!(storage.folder_exists(&"folder".to_string()).await.unwrap());
        assert!(storage.folder_exists(&"folder/".to_string()).await.unwrap());

        // Non-existent folder should not exist
        assert!(
            !storage
                .folder_exists(&"nonexistent".to_string())
                .await
                .unwrap()
        );

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_folder_exists_nested() {
        let storage = setup_test_storage().await;

        // Create nested structure
        storage
            .put_bytes("root/level1/level2/file.txt".to_string(), b"data")
            .await
            .unwrap();

        // All parent folders should exist
        assert!(storage.folder_exists(&"root".to_string()).await.unwrap());
        assert!(
            storage
                .folder_exists(&"root/level1".to_string())
                .await
                .unwrap()
        );
        assert!(
            storage
                .folder_exists(&"root/level1/level2".to_string())
                .await
                .unwrap()
        );

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_special_characters() {
        let storage = setup_test_storage().await;

        let paths = vec![
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.multiple.dots.txt",
        ];

        for path in paths {
            let key = path.to_string();
            storage.put_bytes(key.clone(), b"data").await.unwrap();
            assert!(storage.exists(&key).await.unwrap());
            let data = storage.get_bytes(&key).await.unwrap();
            assert_eq!(data, b"data");
        }

        cleanup_storage(&storage).await;
    }

    // S3-specific tests

    #[tokio::test]
    #[ignore]
    async fn test_s3_empty_key_validation() {
        let storage = setup_test_storage().await;
        let empty_key = String::new();

        let result = storage.put_bytes(empty_key.clone(), b"data").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Generic(_)));

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_very_large_file() {
        let storage = setup_test_storage().await;
        let key = "large-file.bin".to_string();

        // Create 5MB of data
        let data: Vec<u8> = (0..5_000_000).map(|i| (i % 256) as u8).collect();

        storage.put_bytes(key.clone(), &data).await.unwrap();
        let retrieved = storage.get_bytes(&key).await.unwrap();

        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_deeply_nested_paths() {
        let storage = setup_test_storage().await;

        let paths = vec![
            "a/b/c/d/e/f/g/file.txt",
            "root/level1/level2/level3/level4/file.txt",
        ];

        for path in &paths {
            storage
                .put_bytes(path.to_string(), b"nested data")
                .await
                .unwrap();
        }

        for path in &paths {
            assert!(storage.exists(&path.to_string()).await.unwrap());
            let data = storage.get_bytes(&path.to_string()).await.unwrap();
            assert_eq!(data, b"nested data");
        }

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_keys_with_spaces() {
        let storage = setup_test_storage().await;

        let key = "file with spaces.txt".to_string();
        storage.put_bytes(key.clone(), b"space data").await.unwrap();
        assert!(storage.exists(&key).await.unwrap());

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_bucket_name_accessor() {
        let storage = setup_test_storage().await;
        let bucket = storage.bucket();

        assert!(bucket.starts_with("stowage-test-"));
        assert!(!bucket.is_empty());

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_concurrent_writes() {
        let storage = setup_test_storage().await;

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let storage = storage.clone();
                tokio::spawn(async move {
                    let key = format!("concurrent/file{}.txt", i);
                    let data = format!("data{}", i);
                    storage.put_bytes(key, data.as_bytes()).await
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all files were written
        for i in 0..10 {
            let key = format!("concurrent/file{}.txt", i);
            assert!(storage.exists(&key).await.unwrap());
        }

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_copy_to_different_storage() {
        let source = setup_test_storage().await;
        let dest = setup_test_storage().await;
        let key = "copy-test.txt".to_string();
        let data = b"data to copy";

        source.put_bytes(key.clone(), data).await.unwrap();

        source.copy_to(&key, &dest).await.unwrap();

        // Both should have the data
        assert!(source.exists(&key).await.unwrap());
        assert!(dest.exists(&key).await.unwrap());

        let source_data = source.get_bytes(&key).await.unwrap();
        let dest_data = dest.get_bytes(&key).await.unwrap();
        assert_eq!(source_data, dest_data);
        assert_eq!(source_data, data);

        cleanup_storage(&source).await;
        cleanup_storage(&dest).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_storage_clone() {
        let storage1 = setup_test_storage().await;
        let storage2 = storage1.clone();

        let key = "clone-test.txt".to_string();
        storage1.put_bytes(key.clone(), b"data").await.unwrap();

        // Both should access the same bucket
        assert!(storage2.exists(&key).await.unwrap());
        assert_eq!(storage1.bucket(), storage2.bucket());

        cleanup_storage(&storage1).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_unicode_content() {
        let storage = setup_test_storage().await;
        let key = "unicode.txt".to_string();
        let text = "Hello 世界 🌍 مرحبا Привет";

        storage
            .put_bytes(key.clone(), text.as_bytes())
            .await
            .unwrap();

        let retrieved = storage.get_string(&key).await.unwrap();
        assert_eq!(retrieved, text);

        cleanup_storage(&storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_list_not_implemented() {
        let storage = setup_test_storage().await;

        // S3Storage::list is not yet implemented
        let result = storage.list(None).await;
        assert!(result.is_err());
        if let Err(Error::Generic(msg)) = result {
            assert!(msg.contains("not implemented"));
        } else {
            panic!("Expected Generic error for unimplemented list");
        }

        cleanup_storage(&storage).await;
    }
}
