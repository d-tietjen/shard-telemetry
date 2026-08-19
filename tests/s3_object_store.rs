use std::time::{SystemTime, UNIX_EPOCH};

use shard_telemetry::{S3ObjectStore, S3ObjectStoreConfig, TelemetryError, TelemetryObjectStore};

struct ExactCleanup {
    store: S3ObjectStore,
    keys: Vec<&'static str>,
}

impl Drop for ExactCleanup {
    fn drop(&mut self) {
        for key in &self.keys {
            let _ = self.store.delete(key);
        }
    }
}

#[test]
#[ignore = "requires SHARD_TELEMETRY_S3_TEST_BUCKET and disposable S3/MinIO credentials"]
fn real_s3_backend_preserves_immutable_conditional_and_exact_delete_contract() {
    let bucket = std::env::var("SHARD_TELEMETRY_S3_TEST_BUCKET")
        .expect("SHARD_TELEMETRY_S3_TEST_BUCKET must name a disposable test bucket");
    let base_prefix = std::env::var("SHARD_TELEMETRY_S3_TEST_PREFIX")
        .unwrap_or_else(|_| "shard-telemetry-contract-tests".into());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock follows Unix epoch")
        .as_nanos();
    let prefix = format!("{base_prefix}/{}-{nonce}", std::process::id());
    let endpoint = std::env::var("SHARD_TELEMETRY_S3_TEST_ENDPOINT").ok();
    let allow_http = endpoint
        .as_deref()
        .is_some_and(|endpoint| endpoint.starts_with("http://"));
    let store = S3ObjectStore::open(S3ObjectStoreConfig {
        bucket,
        prefix,
        region: std::env::var("SHARD_TELEMETRY_S3_TEST_REGION").ok(),
        endpoint,
        allow_http,
        virtual_hosted_style: false,
    })
    .expect("S3 test backend opens");
    let _cleanup = ExactCleanup {
        store: store.clone(),
        keys: vec!["immutable/payload", "catalog/CURRENT"],
    };

    let immutable = store
        .put_bytes_if_absent("immutable/payload", b"payload-one")
        .expect("immutable object is created");
    assert_eq!(immutable.bytes, 11);
    assert_eq!(
        store
            .put_bytes_if_absent("immutable/payload", b"payload-one")
            .expect("byte-identical immutable retry succeeds")
            .content_digest,
        immutable.content_digest
    );
    assert!(
        store
            .put_bytes_if_absent("immutable/payload", b"payload-two")
            .is_err()
    );
    assert_eq!(
        store.get("immutable/payload", 11).expect("bounded GET"),
        b"payload-one"
    );
    assert_eq!(
        store
            .get_range("immutable/payload", 2..9)
            .expect("bounded range GET"),
        b"yload-o"
    );
    assert_eq!(
        store
            .head("immutable/payload")
            .expect("HEAD succeeds")
            .expect("immutable object exists")
            .bytes,
        11
    );

    let first = store
        .compare_and_swap("catalog/CURRENT", None, b"generation-one")
        .expect("conditional create succeeds");
    assert!(matches!(
        store.compare_and_swap("catalog/CURRENT", None, b"stale"),
        Err(TelemetryError::StaleCatalog { .. })
    ));
    let second = store
        .compare_and_swap(
            "catalog/CURRENT",
            Some(&first.version_token),
            b"generation-two",
        )
        .expect("conditional update succeeds");
    assert_ne!(first.version_token, second.version_token);
    assert_eq!(
        store.get("catalog/CURRENT", 14).expect("updated pointer"),
        b"generation-two"
    );

    store
        .delete("immutable/payload")
        .expect("exact delete succeeds");
    store
        .delete("immutable/payload")
        .expect("exact delete is idempotent");
    assert!(
        store
            .head("immutable/payload")
            .expect("deleted HEAD")
            .is_none()
    );
}
