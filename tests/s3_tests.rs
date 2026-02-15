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

#[path = "test_common/mod.rs"]
mod test_common;

#[cfg(feature = "s3")]
mod s3_integration_tests {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::{Credentials, Region};
    use stowage::{Error, S3Storage, Storage, StorageExt};

    use super::test_common;

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

    // Common test suite tests

    #[tokio::test]
    #[ignore]
    async fn test_s3_put_and_exists() {
        test_common::test_put_and_exists(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_put_and_get_bytes() {
        test_common::test_put_and_get_bytes(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_get_nonexistent() {
        test_common::test_get_nonexistent(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_delete_existing() {
        test_common::test_delete_existing(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_delete_idempotent() {
        test_common::test_delete_idempotent(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_overwrite() {
        test_common::test_overwrite(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_empty_data() {
        test_common::test_empty_data(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_large_data() {
        test_common::test_large_data(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_binary_data() {
        test_common::test_binary_data(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_get_into() {
        test_common::test_get_into(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_folder_exists() {
        test_common::test_folder_exists(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_folder_exists_nested() {
        test_common::test_folder_exists_nested(&mut setup_test_storage).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_special_characters() {
        test_common::test_special_characters(&mut setup_test_storage).await;
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
