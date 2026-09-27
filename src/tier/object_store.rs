use super::*;

/// Metadata returned for one object-store object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// Exact object length in bytes.
    pub bytes: u64,
    /// Opaque object-store version used for conditional publication.
    ///
    /// This is deliberately not treated as a content checksum. For example,
    /// an S3 multipart ETag is a version token but is not a BLAKE3 digest.
    pub version_token: String,
    /// Lowercase BLAKE3 digest calculated over the complete object contents.
    pub content_digest: String,
}

/// Minimal object-store contract needed by the ShardTelemetry tier.
///
/// Immutable data uses put-if-absent operations. Only the small `CURRENT`
/// pointer is mutable, and it is replaced with an object-version
/// compare-and-swap.
pub trait TelemetryObjectStore: Send + Sync {
    /// Creates an immutable object from bytes, or verifies an identical retry.
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata>;

    /// Creates an immutable object from a local file without buffering it all.
    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata>;

    /// Reads an entire object subject to a caller-provided allocation limit.
    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>>;

    /// Reads exactly one byte range from an object.
    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>>;

    /// Returns object metadata, or `None` when the key is absent.
    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>>;

    /// Deletes one exact object key.
    ///
    /// Deletion is idempotent: an already absent key is a successful outcome.
    /// Catalog ownership code never calls this with a discovered or listed key.
    fn delete(&self, key: &str) -> TelemetryResult<()>;

    /// Conditionally replaces a small mutable object.
    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata>;
}

#[derive(Debug, Default)]
struct ObjectStoreCounters {
    put_requests: AtomicU64,
    put_bytes: AtomicU64,
    get_requests: AtomicU64,
    get_bytes: AtomicU64,
    range_requests: AtomicU64,
    range_bytes: AtomicU64,
    head_requests: AtomicU64,
    compare_and_swaps: AtomicU64,
    delete_requests: AtomicU64,
    failures: AtomicU64,
}

/// Process-local object-tier operation counters shared by all stripe owners.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreStats {
    /// Immutable byte and file put attempts.
    pub put_requests: u64,
    /// Source bytes accepted by successful put operations.
    pub put_bytes: u64,
    /// Complete-object read attempts.
    pub get_requests: u64,
    /// Bytes returned by complete-object reads.
    pub get_bytes: u64,
    /// Byte-range read attempts.
    pub range_requests: u64,
    /// Bytes returned by byte-range reads.
    pub range_bytes: u64,
    /// Metadata lookup attempts.
    pub head_requests: u64,
    /// Conditional `CURRENT` publication attempts.
    pub compare_and_swaps: u64,
    /// Exact-key idempotent deletion attempts.
    pub delete_requests: u64,
    /// Failed object-store operations of any kind.
    pub failures: u64,
}

/// Cloneable type-erased object-store handle used by production stripe owners.
#[derive(Clone)]
pub struct SharedTelemetryObjectStore {
    inner: Arc<dyn TelemetryObjectStore>,
    counters: Arc<ObjectStoreCounters>,
}

impl SharedTelemetryObjectStore {
    /// Wraps an object-store adapter for use by independently owned stripes.
    #[must_use]
    pub fn new(store: Arc<dyn TelemetryObjectStore>) -> Self {
        Self {
            inner: store,
            counters: Arc::new(ObjectStoreCounters::default()),
        }
    }

    /// Returns operation and transfer counters shared by every clone.
    #[must_use]
    pub fn stats(&self) -> ObjectStoreStats {
        ObjectStoreStats {
            put_requests: self.counters.put_requests.load(Ordering::Relaxed),
            put_bytes: self.counters.put_bytes.load(Ordering::Relaxed),
            get_requests: self.counters.get_requests.load(Ordering::Relaxed),
            get_bytes: self.counters.get_bytes.load(Ordering::Relaxed),
            range_requests: self.counters.range_requests.load(Ordering::Relaxed),
            range_bytes: self.counters.range_bytes.load(Ordering::Relaxed),
            head_requests: self.counters.head_requests.load(Ordering::Relaxed),
            compare_and_swaps: self.counters.compare_and_swaps.load(Ordering::Relaxed),
            delete_requests: self.counters.delete_requests.load(Ordering::Relaxed),
            failures: self.counters.failures.load(Ordering::Relaxed),
        }
    }

    fn record_failure<T>(&self, result: &TelemetryResult<T>) {
        if result.is_err() {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for SharedTelemetryObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SharedTelemetryObjectStore(..)")
    }
}

impl From<LocalObjectStore> for SharedTelemetryObjectStore {
    fn from(store: LocalObjectStore) -> Self {
        Self::new(Arc::new(store))
    }
}

impl TelemetryObjectStore for SharedTelemetryObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_bytes_if_absent(key, bytes);
        self.record_failure(&result);
        if result.is_ok() {
            self.counters.put_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_file_if_absent(key, source);
        self.record_failure(&result);
        if let Ok(metadata) = &result {
            self.counters
                .put_bytes
                .fetch_add(metadata.bytes, Ordering::Relaxed);
        }
        result
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get(key, max_bytes);
        self.record_failure(&result);
        if let Ok(bytes) = &result {
            self.counters.get_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.counters.range_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_range(key, range);
        self.record_failure(&result);
        if let Ok(bytes) = &result {
            self.counters.range_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.counters.head_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.head(key);
        self.record_failure(&result);
        result
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        self.counters
            .delete_requests
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.delete(key);
        self.record_failure(&result);
        result
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.counters
            .compare_and_swaps
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.compare_and_swap(key, expected_version, bytes);
        self.record_failure(&result);
        result
    }
}

/// Filesystem implementation of [`TelemetryObjectStore`] used for local operation
/// and deterministic testing of S3-style immutable publication.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Opens or creates a local object-store root.
    pub fn open(root: impl AsRef<Path>) -> TelemetryResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .map_err(|error| storage_io("create local object-store root", error))?;
        Ok(Self { root })
    }

    /// Returns the backing filesystem root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn object_path(&self, key: &str) -> TelemetryResult<PathBuf> {
        validate_object_key(key)?;
        Ok(self.root.join(key))
    }

    fn update_lock(&self) -> TelemetryResult<File> {
        let path = self.root.join(".shard-telemetry-object-store.lock");
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|error| storage_io("open object-store update lock", error))?;
        FileExt::lock_exclusive(&lock)
            .map_err(|error| storage_io("lock object-store update lock", error))?;
        Ok(lock)
    }
}

impl TelemetryObjectStore for LocalObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let expected = metadata_for_bytes(bytes);
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(expected)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let source_metadata = source
            .metadata()
            .map_err(|error| storage_io("inspect immutable object source", error))?;
        if !source_metadata.is_file() || source_metadata.len() == 0 {
            return Err(TelemetryError::ObjectStore(
                "immutable object source must be a nonempty regular file".into(),
            ));
        }
        let lock = self.update_lock()?;
        let expected = hash_file(source)?;
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        let copied = copy_file_atomically(source, &path)?;
        unlock_file(&lock)?;
        if copied != expected {
            return Err(TelemetryError::CorruptTier(
                "object source changed while it was copied".into(),
            ));
        }
        Ok(copied)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        let path = self.object_path(key)?;
        let metadata = path
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?;
        if metadata.len() > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object {key} is {} bytes, exceeding read limit {max_bytes}",
                metadata.len()
            )));
        }
        fs::read(path).map_err(|error| object_io(key, "read", error))
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end {
            return Err(TelemetryError::ObjectStore(
                "object byte range starts after its end".into(),
            ));
        }
        let path = self.object_path(key)?;
        let mut file = File::open(path).map_err(|error| object_io(key, "open", error))?;
        let object_bytes = file
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?
            .len();
        if range.end > object_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object range {}..{} exceeds {key} length {object_bytes}",
                range.start, range.end
            )));
        }
        let bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("object byte range cannot fit in memory".into())
        })?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|error| object_io(key, "seek", error))?;
        let mut output = vec![0; bytes];
        file.read_exact(&mut output)
            .map_err(|error| object_io(key, "read range", error))?;
        Ok(output)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        let path = self.object_path(key)?;
        metadata_for_path_if_present(&path)
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let result = match fs::remove_file(&path) {
            Ok(()) => sync_parent(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(object_io(key, "delete", error)),
        };
        let unlock = unlock_file(&lock);
        result.and(unlock)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let observed = metadata_for_path_if_present(&path)?;
        if observed
            .as_ref()
            .map(|metadata| metadata.version_token.as_str())
            != expected_version
        {
            unlock_file(&lock)?;
            return Err(TelemetryError::StaleCatalog {
                expected: expected_version.map(str::to_owned),
                observed: observed.map(|metadata| metadata.version_token),
            });
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(metadata_for_bytes(bytes))
    }
}
