//! Integration tests for storage migration (`migrate` / `StorageExt::migrate_to`).

use stowage::{
    ConflictStrategy, MemoryStorage, MigrateOptions, Storage, StorageExt, multi::migration::migrate,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a `MemoryStorage` pre-populated with `keys`.
/// Each key's value is its own name as bytes.
async fn source_with(keys: &[&str]) -> MemoryStorage {
    let s = MemoryStorage::new();
    for k in keys {
        s.put_bytes(k.to_string(), k.as_bytes()).await.unwrap();
    }
    s
}

// ── Basic transfer ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_migrate_all_items() {
    let source = source_with(&["a.txt", "b.txt", "c.txt"]).await;
    let dest = MemoryStorage::new();

    let result = migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    assert_eq!(result.transferred_count(), 3, "all 3 items transferred");
    assert_eq!(result.skipped_count(), 0);
    assert_eq!(result.error_count(), 0);
    assert!(result.is_complete());
    assert_eq!(result.total_attempted(), 3);

    for key in ["a.txt", "b.txt", "c.txt"] {
        assert!(
            dest.exists(&key.to_string()).await.unwrap(),
            "{key} missing"
        );
        let data = StorageExt::get_bytes(&dest, &key.to_string())
            .await
            .unwrap();
        assert_eq!(data, key.as_bytes(), "{key} content mismatch");
    }
}

#[tokio::test]
async fn test_migrate_empty_source() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    let result = migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    assert_eq!(result.total_attempted(), 0);
    assert_eq!(result.transferred_count(), 0);
    assert!(result.is_complete());
}

#[tokio::test]
async fn test_source_unchanged_after_migration() {
    let source = source_with(&["x.txt", "y.txt"]).await;
    let dest = MemoryStorage::new();

    migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    // Source must still have all items
    assert!(source.exists(&"x.txt".to_string()).await.unwrap());
    assert!(source.exists(&"y.txt".to_string()).await.unwrap());
}

// ── Prefix filtering ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_migrate_with_prefix() {
    let source = source_with(&["docs/a.txt", "docs/b.txt", "images/logo.png"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        prefix: Some("docs/".to_string()),
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 2);
    assert!(dest.exists(&"docs/a.txt".to_string()).await.unwrap());
    assert!(dest.exists(&"docs/b.txt".to_string()).await.unwrap());
    assert!(
        !dest.exists(&"images/logo.png".to_string()).await.unwrap(),
        "out-of-prefix item must not appear in dest"
    );
}

#[tokio::test]
async fn test_migrate_prefix_no_matches() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        prefix: Some("zzz/".to_string()),
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 0);
    assert!(result.is_complete());
}

// ── Conflict strategies ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_conflict_overwrite() {
    let source = source_with(&["file.txt"]).await;
    let dest = MemoryStorage::new();
    dest.put_bytes("file.txt".to_string(), b"old content")
        .await
        .unwrap();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Overwrite,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 1);
    assert_eq!(result.skipped_count(), 0);
    assert_eq!(result.error_count(), 0);

    let data = StorageExt::get_bytes(&dest, &"file.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"file.txt", "destination content must be overwritten");
}

#[tokio::test]
async fn test_conflict_skip_existing() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"original")
        .await
        .unwrap();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Skip,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 1, "only b.txt transferred");
    assert_eq!(result.skipped_count(), 1, "a.txt skipped");
    assert!(result.skipped.contains(&"a.txt".to_string()));
    assert!(result.is_complete(), "skipping is not an error");

    // Original a.txt content must be untouched
    let data = StorageExt::get_bytes(&dest, &"a.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"original");
}

#[tokio::test]
async fn test_conflict_skip_none_existing() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Skip,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    // Nothing to skip — both items are new
    assert_eq!(result.transferred_count(), 2);
    assert_eq!(result.skipped_count(), 0);
}

#[tokio::test]
async fn test_conflict_fail_existing_item() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"original")
        .await
        .unwrap();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Fail,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    // b.txt should transfer fine
    assert_eq!(result.transferred_count(), 1);
    assert!(result.transferred.contains(&"b.txt".to_string()));

    // a.txt should produce an error (not a panic / hard fail)
    assert_eq!(result.error_count(), 1);
    let (failed_id, _err) = &result.errors[0];
    assert_eq!(failed_id, "a.txt");

    // The original a.txt must remain untouched
    let data = StorageExt::get_bytes(&dest, &"a.txt".to_string())
        .await
        .unwrap();
    assert_eq!(data, b"original");
}

#[tokio::test]
async fn test_conflict_fail_no_existing() {
    let source = source_with(&["a.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Fail,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 1);
    assert_eq!(result.error_count(), 0);
}

// ── Delete source (move semantics) ────────────────────────────────────────────

#[tokio::test]
async fn test_delete_source_after_transfer() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        delete_source: true,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 2);
    assert_eq!(result.deleted_count(), 2);
    assert_eq!(result.error_count(), 0);

    // Source must be empty
    assert!(!source.exists(&"a.txt".to_string()).await.unwrap());
    assert!(!source.exists(&"b.txt".to_string()).await.unwrap());

    // Destination must have everything
    assert!(dest.exists(&"a.txt".to_string()).await.unwrap());
    assert!(dest.exists(&"b.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_no_delete_source_by_default() {
    let source = source_with(&["a.txt"]).await;
    let dest = MemoryStorage::new();

    let result = migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    assert_eq!(result.transferred_count(), 1);
    assert_eq!(result.deleted_count(), 0);
    assert!(source.exists(&"a.txt".to_string()).await.unwrap());
}

#[tokio::test]
async fn test_delete_source_with_prefix() {
    let source = source_with(&["docs/a.txt", "docs/b.txt", "keep.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        prefix: Some("docs/".to_string()),
        delete_source: true,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 2);
    assert_eq!(result.deleted_count(), 2);

    // Only prefixed items removed from source
    assert!(!source.exists(&"docs/a.txt".to_string()).await.unwrap());
    assert!(!source.exists(&"docs/b.txt".to_string()).await.unwrap());
    assert!(
        source.exists(&"keep.txt".to_string()).await.unwrap(),
        "out-of-prefix item must not be deleted"
    );
}

// ── Data integrity ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_binary_data_integrity() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    let payload: Vec<u8> = (0u8..=255).collect();
    source
        .put_bytes("binary.bin".to_string(), &payload)
        .await
        .unwrap();

    migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    let received = StorageExt::get_bytes(&dest, &"binary.bin".to_string())
        .await
        .unwrap();
    assert_eq!(received, payload);
}

#[tokio::test]
async fn test_large_file_integrity() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    let payload: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
    source
        .put_bytes("large.bin".to_string(), &payload)
        .await
        .unwrap();

    let result = migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    assert!(result.is_complete());
    let received = StorageExt::get_bytes(&dest, &"large.bin".to_string())
        .await
        .unwrap();
    assert_eq!(received, payload);
}

#[tokio::test]
async fn test_unicode_key_and_content() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    let key = "données/résumé.txt".to_string();
    let content = "Héllo wörld 🌍";

    source
        .put_bytes(key.clone(), content.as_bytes())
        .await
        .unwrap();

    migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    let received = StorageExt::get_string(&dest, &key).await.unwrap();
    assert_eq!(received, content);
}

// ── Concurrency ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_concurrent_migration() {
    let source = MemoryStorage::new();
    let dest = MemoryStorage::new();

    for i in 0..20u32 {
        source
            .put_bytes(
                format!("file-{i:02}.txt"),
                format!("content-{i}").as_bytes(),
            )
            .await
            .unwrap();
    }

    let options = MigrateOptions {
        concurrency: 8,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    assert_eq!(result.transferred_count(), 20);
    assert!(result.is_complete());

    for i in 0..20u32 {
        let id = format!("file-{i:02}.txt");
        assert!(dest.exists(&id).await.unwrap(), "missing {id}");

        let data = StorageExt::get_string(&dest, &id).await.unwrap();
        assert_eq!(data, format!("content-{i}"));
    }
}

#[tokio::test]
async fn test_concurrency_one() {
    let source = source_with(&["a.txt", "b.txt", "c.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        concurrency: 1,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();
    assert_eq!(result.transferred_count(), 3);
    assert!(result.is_complete());
}

#[tokio::test]
async fn test_concurrency_zero_clamped_to_one() {
    let source = source_with(&["a.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        concurrency: 0, // should be clamped to 1
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();
    assert_eq!(result.transferred_count(), 1);
}

// ── StorageExt::migrate_to convenience method ─────────────────────────────────

#[tokio::test]
async fn test_migrate_to_extension_method() {
    let source = source_with(&["x.txt", "y.txt"]).await;
    let dest = MemoryStorage::new();

    let result = source
        .migrate_to(&dest, MigrateOptions::default())
        .await
        .unwrap();

    assert_eq!(result.transferred_count(), 2);
    assert!(result.is_complete());
}

#[tokio::test]
async fn test_migrate_to_with_options() {
    let source = source_with(&["keep/a.txt", "keep/b.txt", "drop/c.txt"]).await;
    let dest = MemoryStorage::new();

    let result = source
        .migrate_to(
            &dest,
            MigrateOptions {
                prefix: Some("keep/".to_string()),
                delete_source: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.transferred_count(), 2);
    assert_eq!(result.deleted_count(), 2);

    assert!(dest.exists(&"keep/a.txt".to_string()).await.unwrap());
    assert!(dest.exists(&"keep/b.txt".to_string()).await.unwrap());
    assert!(!dest.exists(&"drop/c.txt".to_string()).await.unwrap());
    assert!(source.exists(&"drop/c.txt".to_string()).await.unwrap());
}

// ── MigrationResult helpers ───────────────────────────────────────────────────

#[tokio::test]
async fn test_result_display_basic() {
    let source = source_with(&["a.txt", "b.txt"]).await;
    let dest = MemoryStorage::new();

    let result = migrate(&source, &dest, MigrateOptions::default())
        .await
        .unwrap();

    let s = result.to_string();
    assert!(s.contains("2 transferred"), "got: {s}");
    assert!(s.contains("0 skipped"), "got: {s}");
    assert!(s.contains("0 errors"), "got: {s}");
    assert!(!s.contains("deleted"), "should not mention deletions: {s}");
}

#[tokio::test]
async fn test_result_display_with_errors() {
    let source = source_with(&["a.txt", "b.txt", "c.txt"]).await;
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"old").await.unwrap();
    dest.put_bytes("b.txt".to_string(), b"old").await.unwrap();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Fail,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    let s = result.to_string();
    assert!(s.contains("1 transferred"), "got: {s}");
    assert!(s.contains("2 errors"), "got: {s}");
}

#[tokio::test]
async fn test_result_display_with_deletes() {
    let source = source_with(&["a.txt"]).await;
    let dest = MemoryStorage::new();

    let options = MigrateOptions {
        delete_source: true,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();
    let s = result.to_string();
    assert!(s.contains("1 deleted from source"), "got: {s}");
}

#[tokio::test]
async fn test_result_total_attempted() {
    let source = source_with(&["a.txt", "b.txt", "c.txt"]).await;
    let dest = MemoryStorage::new();
    dest.put_bytes("a.txt".to_string(), b"x").await.unwrap();

    let options = MigrateOptions {
        conflict: ConflictStrategy::Skip,
        ..Default::default()
    };

    let result = migrate(&source, &dest, options).await.unwrap();

    // 1 skipped + 2 transferred = 3 total attempted
    assert_eq!(result.total_attempted(), 3);
}
