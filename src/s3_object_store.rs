use std::future::Future;
use std::io::Read;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attribute, AttributeValue, Attributes, DynObjectStore, Error as CloudError, ObjectStore,
    ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;

use crate::tier::hash_file;
use crate::{ObjectMetadata, TelemetryError, TelemetryObjectStore, TelemetryResult};

const BLAKE3_METADATA_KEY: &str = "shard-telemetry-blake3";
const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 4;

type CloudTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type CloudTask = Box<dyn FnOnce(Arc<DynObjectStore>) -> CloudTaskFuture + Send + 'static>;

/// Production S3 and S3-compatible object-store configuration.
///
/// Credentials are deliberately absent. The adapter uses the standard AWS
/// environment, workload identity, ECS, or instance-metadata credential chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectStoreConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional key prefix dedicated to one ShardTelemetry deployment.
    pub prefix: String,
    /// Optional AWS region override.
    pub region: Option<String>,
    /// Optional S3-compatible endpoint override.
    pub endpoint: Option<String>,
    /// Allows an explicitly configured plaintext endpoint.
    pub allow_http: bool,
    /// Uses virtual-hosted-style requests instead of path-style requests.
    pub virtual_hosted_style: bool,
}

impl S3ObjectStoreConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.bucket.trim().is_empty() {
            return Err(TelemetryError::InvalidConfiguration(
                "S3 object-store bucket cannot be empty".into(),
            ));
        }
        if !self.prefix.is_empty()
            && (self.prefix.starts_with('/')
                || self.prefix.ends_with('/')
                || self
                    .prefix
                    .split('/')
                    .any(|part| part.is_empty() || matches!(part, "." | "..")))
        {
            return Err(TelemetryError::InvalidConfiguration(
                "S3 object-store prefix must contain safe nonempty path segments".into(),
            ));
        }
        if self.allow_http
            && self
                .endpoint
                .as_deref()
                .is_none_or(|endpoint| !endpoint.starts_with("http://"))
        {
            return Err(TelemetryError::InvalidConfiguration(
                "S3 allow_http requires an explicit http:// endpoint".into(),
            ));
        }
        Ok(())
    }
}

/// Rust-native S3 adapter with conditional publication and streaming uploads.
///
/// The synchronous tier contract is bridged to one asynchronous runtime. Each
/// request is spawned independently, so stripe-local callers retain concurrent
/// network I/O without constructing a Tokio runtime per shard or request.
#[derive(Clone)]
pub struct S3ObjectStore {
    sender: UnboundedSender<CloudTask>,
    prefix: Arc<str>,
}

impl std::fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3ObjectStore {
    /// Builds an S3 client and starts its shared asynchronous I/O runtime.
    pub fn open(config: S3ObjectStoreConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&config.bucket)
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(config.virtual_hosted_style);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        let store = builder
            .build()
            .map_err(|error| cloud_error("configure", &config.bucket, error))?;
        let store: Arc<DynObjectStore> = Arc::new(store);
        let (sender, mut receiver) = unbounded_channel::<CloudTask>();
        std::thread::Builder::new()
            .name("shard-telemetry-s3".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .thread_name("shard-telemetry-s3-io")
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        eprintln!("shard-telemetry could not start S3 runtime: {error}");
                        return;
                    }
                };
                runtime.block_on(async move {
                    while let Some(task) = receiver.recv().await {
                        tokio::spawn(task(Arc::clone(&store)));
                    }
                });
            })
            .map_err(|error| TelemetryError::ObjectStore(format!("start S3 runtime: {error}")))?;
        Ok(Self {
            sender,
            prefix: Arc::from(config.prefix),
        })
    }

    fn object_path(&self, key: &str) -> TelemetryResult<ObjectPath> {
        crate::tier::validate_object_key(key)?;
        let key = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        ObjectPath::parse(key)
            .map_err(|error| TelemetryError::ObjectStore(format!("invalid S3 key: {error}")))
    }

    fn execute<T: Send + 'static>(
        &self,
        operation: impl FnOnce(
            Arc<DynObjectStore>,
        ) -> Pin<Box<dyn Future<Output = TelemetryResult<T>> + Send>>
        + Send
        + 'static,
    ) -> TelemetryResult<T> {
        let (response, receiver) = sync_channel(1);
        self.sender
            .send(Box::new(move |store| {
                Box::pin(async move {
                    let _ = response.send(operation(store).await);
                })
            }))
            .map_err(|_| TelemetryError::ObjectStore("S3 runtime is unavailable".into()))?;
        receiver
            .recv()
            .map_err(|_| TelemetryError::ObjectStore("S3 operation was abandoned".into()))?
    }
}

impl TelemetryObjectStore for S3ObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let key = key.to_owned();
        let bytes = Bytes::copy_from_slice(bytes);
        let digest = blake3::hash(&bytes).to_hex().to_string();
        self.execute(move |store| {
            Box::pin(async move {
                let options = PutOptions {
                    mode: PutMode::Create,
                    attributes: digest_attributes(&digest),
                    ..PutOptions::default()
                };
                match store
                    .put_opts(&path, PutPayload::from(bytes.clone()), options)
                    .await
                {
                    Ok(result) => Ok(metadata_from_put(
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                        result,
                        digest,
                    )),
                    Err(CloudError::AlreadyExists { .. }) => {
                        let existing = store
                            .get(&path)
                            .await
                            .map_err(|error| cloud_error("read immutable retry", &key, error))?;
                        if existing.meta.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                            || existing_digest(&existing.attributes).as_deref() != Some(&digest)
                        {
                            return Err(TelemetryError::ObjectStore(format!(
                                "immutable S3 object key {key} already contains different bytes"
                            )));
                        }
                        Ok(metadata_from_meta(&existing.meta, digest))
                    }
                    Err(error) => Err(cloud_error("create immutable", &key, error)),
                }
            })
        })
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        let expected = hash_file(source)?;
        let path = self.object_path(key)?;
        let key = key.to_owned();
        let source = source.to_path_buf();
        self.execute(move |store| {
            Box::pin(async move {
                match store.get(&path).await {
                    Ok(existing) => {
                        if existing.meta.size != expected.bytes
                            || existing_digest(&existing.attributes).as_deref()
                                != Some(&expected.content_digest)
                        {
                            return Err(TelemetryError::ObjectStore(format!(
                                "immutable S3 object key {key} already contains different bytes"
                            )));
                        }
                        return Ok(metadata_from_meta(&existing.meta, expected.content_digest));
                    }
                    Err(CloudError::NotFound { .. }) => {}
                    Err(error) => return Err(cloud_error("inspect immutable", &key, error)),
                }
                upload_file(
                    store.as_ref(),
                    &path,
                    &source,
                    expected.bytes,
                    &expected.content_digest,
                    &key,
                )
                .await
            })
        })
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        let path = self.object_path(key)?;
        let key = key.to_owned();
        self.execute(move |store| {
            Box::pin(async move {
                let result = store
                    .get(&path)
                    .await
                    .map_err(|error| cloud_error("read", &key, error))?;
                if result.meta.size > max_bytes {
                    return Err(TelemetryError::ObjectStore(format!(
                        "object {key} is {} bytes, exceeding read limit {max_bytes}",
                        result.meta.size
                    )));
                }
                result
                    .bytes()
                    .await
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| cloud_error("read body", &key, error))
            })
        })
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end {
            return Err(TelemetryError::ObjectStore(
                "object byte range starts after its end".into(),
            ));
        }
        let path = self.object_path(key)?;
        let key = key.to_owned();
        self.execute(move |store| {
            Box::pin(async move {
                store
                    .get_range(&path, range)
                    .await
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| cloud_error("read range", &key, error))
            })
        })
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        let path = self.object_path(key)?;
        let key = key.to_owned();
        self.execute(move |store| {
            Box::pin(async move {
                match store.head(&path).await {
                    Ok(metadata) => Ok(Some(metadata_from_meta(&metadata, String::new()))),
                    Err(CloudError::NotFound { .. }) => Ok(None),
                    Err(error) => Err(cloud_error("inspect", &key, error)),
                }
            })
        })
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        let path = self.object_path(key)?;
        let key = key.to_owned();
        self.execute(move |store| {
            Box::pin(async move {
                match store.delete(&path).await {
                    Ok(()) | Err(CloudError::NotFound { .. }) => Ok(()),
                    Err(error) => Err(cloud_error("delete", &key, error)),
                }
            })
        })
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let key = key.to_owned();
        let expected = expected_version.map(decode_version).transpose()?;
        let bytes = Bytes::copy_from_slice(bytes);
        let digest = blake3::hash(&bytes).to_hex().to_string();
        self.execute(move |store| {
            Box::pin(async move {
                let mode = expected.clone().map_or(PutMode::Create, PutMode::Update);
                let options = PutOptions {
                    mode,
                    attributes: digest_attributes(&digest),
                    ..PutOptions::default()
                };
                match store.put_opts(&path, bytes.clone().into(), options).await {
                    Ok(result) => Ok(metadata_from_put(
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                        result,
                        digest,
                    )),
                    Err(
                        error
                        @ (CloudError::AlreadyExists { .. } | CloudError::Precondition { .. }),
                    ) => {
                        let observed = match store.head(&path).await {
                            Ok(metadata) => Some(version_token(
                                metadata.e_tag.as_deref(),
                                metadata.version.as_deref(),
                            )),
                            Err(CloudError::NotFound { .. }) => None,
                            Err(head_error) => {
                                return Err(cloud_error("inspect stale", &key, head_error));
                            }
                        };
                        let _ = error;
                        Err(TelemetryError::StaleCatalog {
                            expected: expected_version_token(expected.as_ref()),
                            observed,
                        })
                    }
                    Err(error) => Err(cloud_error("conditional replace", &key, error)),
                }
            })
        })
    }
}

async fn upload_file(
    store: &DynObjectStore,
    path: &ObjectPath,
    source: &Path,
    expected_bytes: u64,
    digest: &str,
    key: &str,
) -> TelemetryResult<ObjectMetadata> {
    let options = object_store::PutMultipartOptions {
        attributes: digest_attributes(digest),
        ..Default::default()
    };
    let mut upload = store
        .put_multipart_opts(path, options)
        .await
        .map_err(|error| cloud_error("start multipart upload", key, error))?;
    let mut parts = JoinSet::new();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let source = source.to_path_buf();
    let reader_source = source.clone();
    let reader = tokio::task::spawn_blocking(move || read_file_chunks(reader_source, sender));
    while let Some(chunk) = receiver.recv().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(abort_upload(
                    upload.as_mut(),
                    TelemetryError::StorageIo(format!("read S3 multipart source: {error}")),
                    key,
                )
                .await);
            }
        };
        if parts.len() >= MULTIPART_CONCURRENCY
            && let Some(result) = parts.join_next().await
            && let Err(error) = flatten_part_result(result, key)
        {
            parts.shutdown().await;
            return Err(abort_upload(upload.as_mut(), error, key).await);
        }
        parts.spawn(upload.put_part(PutPayload::from(chunk)));
    }
    let reader_result = reader
        .await
        .map_err(|error| TelemetryError::StorageIo(format!("join S3 file reader: {error}")))
        .and_then(std::convert::identity);
    if let Err(error) = reader_result {
        parts.shutdown().await;
        return Err(abort_upload(upload.as_mut(), error, key).await);
    }
    while let Some(result) = parts.join_next().await {
        if let Err(error) = flatten_part_result(result, key) {
            parts.shutdown().await;
            return Err(abort_upload(upload.as_mut(), error, key).await);
        }
    }
    let result = match upload.complete().await {
        Ok(result) => result,
        Err(error) => {
            let error = cloud_error("complete multipart upload", key, error);
            return Err(abort_upload(upload.as_mut(), error, key).await);
        }
    };
    let observed = hash_file(&source)?;
    if observed.bytes != expected_bytes || observed.content_digest != digest {
        let _ = store.delete(path).await;
        return Err(TelemetryError::CorruptTier(
            "S3 multipart source changed while it was uploaded".into(),
        ));
    }
    Ok(metadata_from_put(expected_bytes, result, digest.to_owned()))
}

fn flatten_part_result(
    result: Result<object_store::Result<()>, tokio::task::JoinError>,
    key: &str,
) -> TelemetryResult<()> {
    result
        .map_err(|error| {
            TelemetryError::ObjectStore(format!("join S3 multipart task for {key}: {error}"))
        })?
        .map_err(|error| cloud_error("upload multipart part", key, error))
}

async fn abort_upload(
    upload: &mut dyn object_store::MultipartUpload,
    primary: TelemetryError,
    key: &str,
) -> TelemetryError {
    match upload.abort().await {
        Ok(()) => primary,
        Err(error) => TelemetryError::ObjectStore(format!(
            "{primary}; abort S3 multipart upload for {key}: {error}"
        )),
    }
}

fn read_file_chunks(
    source: PathBuf,
    sender: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), TelemetryError> {
    let mut file = std::fs::File::open(&source)
        .map_err(|error| TelemetryError::StorageIo(format!("open S3 source: {error}")))?;
    loop {
        let mut chunk = vec![0; MULTIPART_CHUNK_BYTES];
        let read = file
            .read(&mut chunk)
            .map_err(|error| TelemetryError::StorageIo(format!("read S3 source: {error}")))?;
        if read == 0 {
            return Ok(());
        }
        chunk.truncate(read);
        if sender.blocking_send(Ok(Bytes::from(chunk))).is_err() {
            return Ok(());
        }
    }
}

fn digest_attributes(digest: &str) -> Attributes {
    let mut attributes = Attributes::new();
    attributes.insert(
        Attribute::Metadata(BLAKE3_METADATA_KEY.into()),
        AttributeValue::from(digest.to_owned()),
    );
    attributes
}

fn existing_digest(attributes: &Attributes) -> Option<String> {
    attributes
        .get(&Attribute::Metadata(BLAKE3_METADATA_KEY.into()))
        .map(|value| value.as_ref().to_owned())
}

fn metadata_from_put(
    bytes: u64,
    result: object_store::PutResult,
    content_digest: String,
) -> ObjectMetadata {
    ObjectMetadata {
        bytes,
        version_token: version_token(result.e_tag.as_deref(), result.version.as_deref()),
        content_digest,
    }
}

fn metadata_from_meta(
    metadata: &object_store::ObjectMeta,
    content_digest: String,
) -> ObjectMetadata {
    ObjectMetadata {
        bytes: metadata.size,
        version_token: version_token(metadata.e_tag.as_deref(), metadata.version.as_deref()),
        content_digest,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CloudVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

fn version_token(e_tag: Option<&str>, version: Option<&str>) -> String {
    serde_json::to_string(&CloudVersion {
        e_tag: e_tag.map(str::to_owned),
        version: version.map(str::to_owned),
    })
    .expect("cloud version contains only strings")
}

fn decode_version(encoded: &str) -> TelemetryResult<UpdateVersion> {
    let version: CloudVersion = serde_json::from_str(encoded)
        .map_err(|_| TelemetryError::ObjectStore("S3 catalog version token is malformed".into()))?;
    Ok(UpdateVersion {
        e_tag: version.e_tag,
        version: version.version,
    })
}

fn expected_version_token(version: Option<&UpdateVersion>) -> Option<String> {
    version.map(|version| version_token(version.e_tag.as_deref(), version.version.as_deref()))
}

fn cloud_error(operation: &str, key: &str, error: CloudError) -> TelemetryError {
    TelemetryError::ObjectStore(format!("{operation} S3 object {key}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_rejects_ambiguous_or_plaintext_locations() {
        let base = S3ObjectStoreConfig {
            bucket: "bucket".into(),
            prefix: "deployment-a".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            allow_http: false,
            virtual_hosted_style: false,
        };
        assert!(base.validate().is_ok());
        assert!(
            S3ObjectStoreConfig {
                prefix: String::new(),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            S3ObjectStoreConfig {
                prefix: "/bad".into(),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            S3ObjectStoreConfig {
                endpoint: Some("https://example.com".into()),
                allow_http: true,
                ..base
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn cloud_version_round_trips_both_s3_preconditions() {
        let encoded = version_token(Some("etag"), Some("version"));
        let decoded = decode_version(&encoded).expect("version decodes");
        assert_eq!(decoded.e_tag.as_deref(), Some("etag"));
        assert_eq!(decoded.version.as_deref(), Some("version"));
    }
}
