//! Adapter trait compliance tests
//!
//! These tests verify that all adapters correctly implement the Storage trait
//! with proper method signatures and behavior patterns.

use std::fmt::Debug;
use stowage::{Result, Storage};

/// Helper trait to check that a type implements Storage with correct bounds
trait StorageCompliance: Storage {
    fn check_id_bounds() -> bool
    where
        Self::Id: Clone + Debug + Send + Sync + 'static,
    {
        true
    }

    fn check_storage_bounds() -> bool
    where
        Self: Send + Sync + Debug,
    {
        true
    }
}

impl<T: Storage> StorageCompliance for T {}

/// Test that MemoryStorage implements Storage correctly
#[cfg(feature = "memory")]
#[test]
fn memory_storage_implements_storage() {
    use stowage::MemoryStorage;

    assert!(MemoryStorage::check_id_bounds());
    assert!(MemoryStorage::check_storage_bounds());

    let _storage = MemoryStorage::new();
    // Storage trait is not dyn-compatible due to impl Trait in return types
    // Just verify it compiles and has the right type
}

/// Test that LocalStorage implements Storage correctly
#[cfg(feature = "local")]
#[test]
fn local_storage_implements_storage() {
    use std::path::PathBuf;
    use stowage::adapters::local::LocalStorage;

    assert!(LocalStorage::check_id_bounds());
    assert!(LocalStorage::check_storage_bounds());

    let _storage = LocalStorage::new(PathBuf::from("/tmp"));
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that S3Storage implements Storage correctly
#[cfg(feature = "s3")]
#[test]
fn s3_storage_implements_storage() {
    use stowage::adapters::s3::S3Storage;

    assert!(S3Storage::check_id_bounds());
    assert!(S3Storage::check_storage_bounds());
}

/// Test that AzureStorage implements Storage correctly
#[cfg(feature = "azure")]
#[test]
fn azure_storage_implements_storage() {
    use stowage::adapters::azure::AzureStorage;

    assert!(AzureStorage::check_id_bounds());
    assert!(AzureStorage::check_storage_bounds());

    let _storage = AzureStorage::new("account", "container", "sas_token");
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that GoogleDriveStorage implements Storage correctly
#[cfg(feature = "gdrive")]
#[test]
fn gdrive_storage_implements_storage() {
    use secrecy::SecretString;
    use stowage::adapters::gdrive::{GoogleDriveStorage, TokenProvider};

    assert!(GoogleDriveStorage::check_id_bounds());
    assert!(GoogleDriveStorage::check_storage_bounds());

    let token_provider = TokenProvider::Static(SecretString::from("test_token".to_string()));
    let _storage = GoogleDriveStorage::new(reqwest::Client::new(), token_provider).unwrap();
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that OneDriveStorage implements Storage correctly
#[cfg(feature = "onedrive")]
#[test]
fn onedrive_storage_implements_storage() {
    use secrecy::SecretString;
    use stowage::adapters::onedrive::{OneDriveStorage, TokenProvider};

    assert!(OneDriveStorage::check_id_bounds());
    assert!(OneDriveStorage::check_storage_bounds());

    let token_provider = TokenProvider::Static(SecretString::from("test_token".to_string()));
    let _storage = OneDriveStorage::new(reqwest::Client::new(), token_provider).unwrap();
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that DropboxStorage implements Storage correctly
#[cfg(feature = "dropbox")]
#[test]
fn dropbox_storage_implements_storage() {
    use stowage::adapters::dropbox::DropboxStorage;

    assert!(DropboxStorage::check_id_bounds());
    assert!(DropboxStorage::check_storage_bounds());

    let _storage = DropboxStorage::new("test_token");
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that WebDAVStorage implements Storage correctly
#[cfg(feature = "webdav")]
#[test]
fn webdav_storage_implements_storage() {
    use stowage::adapters::webdav::WebDAVStorage;

    assert!(WebDAVStorage::check_id_bounds());
    assert!(WebDAVStorage::check_storage_bounds());

    let _storage = WebDAVStorage::new("https://example.com/dav", "username", "password");
    // Storage trait is not dyn-compatible due to impl Trait in return types
}

/// Test that BoxStorage implements Storage correctly
#[cfg(feature = "box_storage")]
#[test]
fn box_storage_implements_storage() {
    use stowage::adapters::box_storage::BoxStorage;

    assert!(BoxStorage::check_id_bounds());
    assert!(BoxStorage::check_storage_bounds());

    let _storage = BoxStorage::new("test_token");
    // Storage trait is not dyn-compatible due to impl Trait in return types

    // Test with folder
    let storage_with_folder = BoxStorage::with_folder("test_token", "folder_123");
    assert_eq!(storage_with_folder.parent_folder_id(), "folder_123");
}

/// Verify that Storage methods have correct signatures
#[cfg(feature = "memory")]
#[tokio::test]
async fn storage_method_signatures() {
    use stowage::MemoryStorage;

    let storage = MemoryStorage::new();
    let id = "test_id".to_string();

    // exists returns Result<bool>
    let _exists: Result<bool> = storage.exists(&id).await;

    // put takes id by value, reader, and optional length
    let data = b"test data";
    let reader = &data[..];
    let _put: Result<()> = storage
        .put(id.clone(), reader, Some(data.len() as u64))
        .await;

    // get_into takes id by reference and writer
    let mut output = Vec::new();
    let _get: Result<u64> = storage.get_into(&id, &mut output).await;

    // delete takes id by reference
    let _delete: Result<()> = storage.delete(&id).await;

    // list takes optional prefix and returns stream
    let _list = storage.list(None).await;
}

/// Test that all adapters properly implement Debug
#[cfg(feature = "memory")]
#[test]
fn adapters_implement_debug() {
    use stowage::MemoryStorage;

    let storage = MemoryStorage::new();
    let debug_str = format!("{:?}", storage);
    assert!(debug_str.contains("MemoryStorage"));
}

#[cfg(feature = "local")]
#[test]
fn local_storage_debug() {
    use std::path::PathBuf;
    use stowage::adapters::local::LocalStorage;

    let storage = LocalStorage::new(PathBuf::from("/tmp/test"));
    let debug_str = format!("{:?}", storage);
    assert!(debug_str.contains("LocalStorage"));
}

#[cfg(feature = "azure")]
#[test]
fn azure_storage_debug_hides_secrets() {
    use stowage::adapters::azure::AzureStorage;

    let storage = AzureStorage::new("account", "container", "super_secret_sas_token");
    let debug_str = format!("{:?}", storage);
    // Should not contain the actual secret
    assert!(!debug_str.contains("super_secret_sas_token"));
}

#[cfg(feature = "dropbox")]
#[test]
fn dropbox_storage_debug_hides_secrets() {
    use stowage::adapters::dropbox::DropboxStorage;

    let storage = DropboxStorage::new("super_secret_access_token");
    let debug_str = format!("{:?}", storage);
    // Should not contain the actual secret
    assert!(!debug_str.contains("super_secret_access_token"));
}

#[cfg(feature = "box_storage")]
#[test]
fn box_storage_debug_hides_secrets() {
    use stowage::adapters::box_storage::BoxStorage;

    let storage = BoxStorage::new("super_secret_box_token");
    let debug_str = format!("{:?}", storage);
    // Should not contain the actual secret
    assert!(!debug_str.contains("super_secret_box_token"));
}

#[cfg(feature = "webdav")]
#[test]
fn webdav_storage_debug_hides_secrets() {
    use stowage::adapters::webdav::WebDAVStorage;

    let storage = WebDAVStorage::new(
        "https://example.com/dav",
        "username",
        "super_secret_password",
    );
    let debug_str = format!("{:?}", storage);
    // Should not contain the actual password
    assert!(!debug_str.contains("super_secret_password"));
}

/// Test that adapters are Send + Sync (required for async)
#[cfg(feature = "memory")]
#[test]
fn adapters_are_send_sync() {
    use stowage::MemoryStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<MemoryStorage>();
    assert_sync::<MemoryStorage>();
}

#[cfg(feature = "azure")]
#[test]
fn azure_is_send_sync() {
    use stowage::adapters::azure::AzureStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<AzureStorage>();
    assert_sync::<AzureStorage>();
}

#[cfg(feature = "dropbox")]
#[test]
fn dropbox_is_send_sync() {
    use stowage::adapters::dropbox::DropboxStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<DropboxStorage>();
    assert_sync::<DropboxStorage>();
}

#[cfg(feature = "box_storage")]
#[test]
fn box_is_send_sync() {
    use stowage::adapters::box_storage::BoxStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<BoxStorage>();
    assert_sync::<BoxStorage>();
}

#[cfg(feature = "gdrive")]
#[test]
fn gdrive_is_send_sync() {
    use stowage::adapters::gdrive::GoogleDriveStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<GoogleDriveStorage>();
    assert_sync::<GoogleDriveStorage>();
}

#[cfg(feature = "onedrive")]
#[test]
fn onedrive_is_send_sync() {
    use stowage::adapters::onedrive::OneDriveStorage;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<OneDriveStorage>();
    assert_sync::<OneDriveStorage>();
}
