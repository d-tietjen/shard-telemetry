//! Cumulative admission for synchronous persisted-state recovery.
//!
//! Charges bound input and declared Rust state, not allocator overhead or RSS.
//! Filesystem calls can outlast the saved deadline; their actual errors are
//! retained before checking the clock. Completed successful synchronous handles
//! may be released on expiry; this is not a native/background-work owner API.

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::mem::size_of;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;
use serde::de::{DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};

#[derive(Clone, Copy)]
pub(crate) enum RecoveryFileAccess {
    Read,
    Temporary,
    Lock,
}

/// Positive, cumulative limits for one recovery attempt.
#[derive(Clone, Copy, Debug)]
pub struct TelemetryRecoveryLimits {
    /// Largest input path or directory-entry name accepted by this attempt.
    pub max_path_bytes: usize,
    /// Largest individual regular file, before allocation or decoding.
    pub max_file_bytes: usize,
    /// Sum of admitted file bytes, including one growth probe per file.
    pub max_read_bytes: usize,
    /// Directory iterator advances, including unrelated entries and final EOF.
    pub max_directory_entries: usize,
    /// JSON object records, including nested acknowledgement/envelope objects.
    pub max_records: usize,
    /// JSON values and map keys inspected before typed deserialization.
    pub max_json_values: usize,
    /// Decoded JSON string bytes, including keys and discarded fields.
    pub max_string_bytes: usize,
    /// Declared input, parser scratch and typed-state capacity charges.
    pub max_retained_bytes: usize,
    /// Cumulative admitted read, parse, validation and serialization work.
    pub max_work_bytes: usize,
}

/// Charges already spent; refusals never replenish this ledger.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TelemetryRecoveryUsage {
    /// Admitted file bytes and growth probes.
    pub read_bytes: usize,
    /// Admitted directory iterator advances, including final EOF.
    pub directory_entries: usize,
    /// Admitted JSON object records.
    pub records: usize,
    /// Admitted JSON values and keys.
    pub json_values: usize,
    /// Admitted decoded string bytes.
    pub string_bytes: usize,
    /// Declared state and transient capacity charges.
    pub retained_bytes: usize,
    /// Admitted work bytes.
    pub work_bytes: usize,
}

/// A genuine recovery refusal or physical/decoder error.
#[derive(Debug)]
pub enum TelemetryRecoveryError {
    /// A zero/overflowing limit, or an exhausted cumulative allowance.
    Limits,
    /// The original absolute deadline has elapsed.
    Expired,
    /// Persisted input is not a stable regular file or has invalid structure.
    InvalidInput,
    /// A fallible allocation failed.
    Allocation(std::collections::TryReserveError),
    /// The actual filesystem error, even if its call also exceeded the deadline.
    Io(io::Error),
    /// The actual JSON decoding error.
    Json(serde_json::Error),
    /// The actual existing catalog validation error.
    Catalog(crate::LokiApiError),
    /// Actual allocated/read bytes retained when their operation is refused.
    Buffer {
        /// The real buffer; Debug reports sizes without exposing its contents.
        buffer: TelemetryRecoveryBuffer,
        /// The actual refusal or physical error observed with that buffer.
        cause: Box<TelemetryRecoveryError>,
    },
}

/// Retains a real recovery allocation after capacity/deadline/I/O refusal.
pub struct TelemetryRecoveryBuffer(Vec<u8>);
impl TelemetryRecoveryBuffer {
    /// Actual allocation capacity, which can exceed a requested reservation.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }
    /// Number of initialized bytes retained, including an incomplete read.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Whether the retained buffer contains no initialized bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl fmt::Debug for TelemetryRecoveryBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryRecoveryBuffer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl fmt::Display for TelemetryRecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits => f.write_str("telemetry recovery allowance exhausted"),
            Self::Expired => f.write_str("telemetry recovery deadline expired"),
            Self::InvalidInput => f.write_str("telemetry recovery input is invalid or changed"),
            Self::Allocation(error) => write!(f, "telemetry recovery allocation failed: {error}"),
            Self::Io(error) => write!(f, "telemetry recovery I/O failed: {error}"),
            Self::Json(error) => write!(f, "telemetry recovery JSON failed: {error}"),
            Self::Catalog(error) => write!(f, "telemetry catalog recovery failed: {error}"),
            Self::Buffer { cause, .. } => write!(f, "telemetry recovery buffer retained: {cause}"),
        }
    }
}
impl std::error::Error for TelemetryRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Allocation(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Buffer { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

/// One non-clone cumulative recovery ledger and its original deadline.
#[derive(Debug)]
pub struct TelemetryRecoveryBudget {
    limits: TelemetryRecoveryLimits,
    usage: TelemetryRecoveryUsage,
    expiry: Instant,
    refused: bool,
    last_io_error: Option<io::ErrorKind>,
    expired_after_operation: bool,
}

impl TelemetryRecoveryBudget {
    /// Validates limits without opening files or starting work.
    pub fn new(
        limits: TelemetryRecoveryLimits,
        original_expiry: Instant,
    ) -> Result<Self, TelemetryRecoveryError> {
        if [
            limits.max_path_bytes,
            limits.max_file_bytes,
            limits.max_read_bytes,
            limits.max_directory_entries,
            limits.max_records,
            limits.max_json_values,
            limits.max_string_bytes,
            limits.max_retained_bytes,
            limits.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
        {
            return Err(TelemetryRecoveryError::Limits);
        }
        let mut budget = Self {
            limits,
            usage: TelemetryRecoveryUsage::default(),
            expiry: original_expiry,
            refused: false,
            last_io_error: None,
            expired_after_operation: false,
        };
        budget.check()?;
        Ok(budget)
    }

    /// The same saved deadline; a caller cannot refresh it.
    pub fn original_expiry(&self) -> Instant {
        self.expiry
    }

    /// Previously spent charges, including work from a refused attempt.
    pub fn usage(&self) -> TelemetryRecoveryUsage {
        self.usage
    }

    /// Actual most recent physical error kind, retained before clock checks.
    pub fn last_io_error(&self) -> Option<io::ErrorKind> {
        self.last_io_error
    }

    /// Whether a completed physical call was observed after the saved deadline.
    pub fn expired_after_operation(&self) -> bool {
        self.expired_after_operation
    }

    /// Refuses any later operation after expiry or an earlier refusal.
    pub fn check(&mut self) -> Result<(), TelemetryRecoveryError> {
        if Instant::now() >= self.expiry {
            self.refused = true;
            return Err(TelemetryRecoveryError::Expired);
        }
        if self.refused {
            return Err(TelemetryRecoveryError::Limits);
        }
        Ok(())
    }

    /// Spends all axes atomically before the associated operation/allocation.
    pub fn charge(&mut self, charge: TelemetryRecoveryUsage) -> Result<(), TelemetryRecoveryError> {
        self.check()?;
        macro_rules! add {
            ($field:ident, $limit:ident) => {
                match self.usage.$field.checked_add(charge.$field) {
                    Some(value) if value <= self.limits.$limit => value,
                    _ => {
                        self.refused = true;
                        return Err(TelemetryRecoveryError::Limits);
                    }
                }
            };
        }
        self.usage = TelemetryRecoveryUsage {
            read_bytes: add!(read_bytes, max_read_bytes),
            directory_entries: add!(directory_entries, max_directory_entries),
            records: add!(records, max_records),
            json_values: add!(json_values, max_json_values),
            string_bytes: add!(string_bytes, max_string_bytes),
            retained_bytes: add!(retained_bytes, max_retained_bytes),
            work_bytes: add!(work_bytes, max_work_bytes),
        };
        Ok(())
    }

    pub(crate) fn invalid<T>(&mut self) -> Result<T, TelemetryRecoveryError> {
        self.refused = true;
        Err(TelemetryRecoveryError::InvalidInput)
    }

    pub(crate) fn path_limit(&self) -> usize {
        self.limits.max_path_bytes
    }

    fn allocate_path(&mut self, length: usize) -> Result<PathBuf, TelemetryRecoveryError> {
        if length > self.path_limit() {
            return self.invalid();
        }
        self.charge(TelemetryRecoveryUsage {
            retained_bytes: length
                .checked_add(size_of::<PathBuf>())
                .ok_or(TelemetryRecoveryError::Limits)?,
            work_bytes: length,
            ..Default::default()
        })?;
        let mut path = PathBuf::new();
        if let Err(error) = path.try_reserve_exact(length) {
            self.refused = true;
            return Err(retain_buffer(
                path.into_os_string().into_encoded_bytes(),
                TelemetryRecoveryError::Allocation(error),
            ));
        }
        let extra = path.capacity().saturating_sub(length);
        if let Err(error) = self.charge(TelemetryRecoveryUsage {
            retained_bytes: extra,
            ..Default::default()
        }) {
            return Err(retain_buffer(
                path.into_os_string().into_encoded_bytes(),
                error,
            ));
        }
        Ok(path)
    }

    pub(crate) fn join_path(
        &mut self,
        parent: &Path,
        component: &std::ffi::OsStr,
    ) -> Result<PathBuf, TelemetryRecoveryError> {
        if !matches!(
            Path::new(component).components().next(),
            Some(std::path::Component::Normal(_))
        ) || Path::new(component).components().count() != 1
        {
            return self.invalid();
        }
        let length = parent
            .as_os_str()
            .len()
            .checked_add(component.len())
            .and_then(|n| n.checked_add(1))
            .ok_or(TelemetryRecoveryError::Limits)?;
        let mut path = self.allocate_path(length)?;
        path.push(parent);
        path.push(component);
        if let Err(error) = self.check() {
            return Err(retain_buffer(
                path.into_os_string().into_encoded_bytes(),
                error,
            ));
        }
        Ok(path)
    }

    pub(crate) fn suffixed_path(
        &mut self,
        original: &Path,
        suffix: &str,
    ) -> Result<PathBuf, TelemetryRecoveryError> {
        let length = original
            .as_os_str()
            .len()
            .checked_add(suffix.len())
            .ok_or(TelemetryRecoveryError::Limits)?;
        let mut path = self.allocate_path(length)?;
        path.push(original);
        path.as_mut_os_string().push(suffix);
        if let Err(error) = self.check() {
            return Err(retain_buffer(
                path.into_os_string().into_encoded_bytes(),
                error,
            ));
        }
        Ok(path)
    }

    pub(crate) fn catalog_result<T>(
        &mut self,
        result: Result<T, crate::LokiApiError>,
    ) -> Result<T, TelemetryRecoveryError> {
        match result {
            Ok(value) => {
                self.check()?;
                Ok(value)
            }
            Err(error) => {
                self.refused = true;
                Err(TelemetryRecoveryError::Catalog(error))
            }
        }
    }

    pub(crate) fn io<T>(
        &mut self,
        operation: impl FnOnce() -> io::Result<T>,
    ) -> Result<T, TelemetryRecoveryError> {
        self.charge(TelemetryRecoveryUsage {
            work_bytes: 1,
            ..Default::default()
        })?;
        let result = operation();
        self.completed_io(result)
    }

    pub(crate) fn completed_io<T>(
        &mut self,
        result: io::Result<T>,
    ) -> Result<T, TelemetryRecoveryError> {
        self.last_io_error = result.as_ref().err().map(io::Error::kind);
        self.expired_after_operation |= Instant::now() >= self.expiry;
        match result {
            Err(error) => {
                // Explicit read/write loops may retry EINTR only after another
                // admission and original-clock check. Other errors are terminal.
                self.refused |= error.kind() != io::ErrorKind::Interrupted;
                Err(TelemetryRecoveryError::Io(error))
            }
            Ok(value) => {
                self.check()?;
                Ok(value)
            }
        }
    }

    #[cfg(unix)]
    pub(crate) fn ensure_directory(&mut self, path: &Path) -> Result<File, TelemetryRecoveryError> {
        use rustix::fs::{Mode, OFlags, mkdirat, open, openat};
        use std::path::Component;
        self.check()?;
        if path.as_os_str().len() > self.path_limit() {
            return self.invalid();
        }
        let flags = OFlags::RDONLY
            | OFlags::DIRECTORY
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC;
        let mut parent = self.io(|| {
            open(
                if path.is_absolute() { "/" } else { "." },
                flags,
                Mode::empty(),
            )
            .map_err(io::Error::from)
        })?;
        for part in path.components() {
            let Component::Normal(name) = part else {
                if matches!(part, Component::RootDir | Component::CurDir) {
                    continue;
                }
                return self.invalid();
            };
            self.charge(TelemetryRecoveryUsage {
                work_bytes: name
                    .len()
                    .checked_add(1)
                    .ok_or(TelemetryRecoveryError::Limits)?,
                ..Default::default()
            })?;
            self.check()?;
            // NotFound is an observed absence, not a failed attempt: retain it
            // before the same original-clock check, then create relative to
            // this actual nofollow directory FD.
            let next = openat(&parent, name, flags, Mode::empty()).map_err(io::Error::from);
            match next {
                Err(ref error) if error.kind() == io::ErrorKind::NotFound => {
                    self.last_io_error = Some(error.kind());
                    self.expired_after_operation |= Instant::now() >= self.expiry;
                    self.check()?;
                    self.io(|| {
                        mkdirat(&parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR)
                            .map_err(io::Error::from)
                    })?;
                    parent = self.io(|| {
                        openat(&parent, name, flags, Mode::empty()).map_err(io::Error::from)
                    })?;
                }
                result => parent = self.completed_io(result)?,
            }
        }
        Ok(File::from(parent))
    }

    #[cfg(not(unix))]
    pub(crate) fn ensure_directory(&mut self, _: &Path) -> Result<File, TelemetryRecoveryError> {
        self.invalid()
    }

    /// Reads one regular file under the cumulative byte budget. A growing file
    /// cannot make a successful truncated prefix; one extra byte detects growth.
    pub fn read_file(&mut self, path: &Path) -> Result<Vec<u8>, TelemetryRecoveryError> {
        self.check()?;
        if path.as_os_str().len() > self.limits.max_path_bytes {
            self.refused = true;
            return Err(TelemetryRecoveryError::Limits);
        }
        let mut file = self.open_path(path, RecoveryFileAccess::Read)?;
        let metadata = self.io(|| file.metadata())?;
        let length = usize::try_from(metadata.len()).map_err(|_| TelemetryRecoveryError::Limits)?;
        if length > self.limits.max_file_bytes {
            self.refused = true;
            return Err(TelemetryRecoveryError::Limits);
        }
        if !metadata.is_file() {
            return self.invalid();
        }
        let probe = length
            .checked_add(1)
            .ok_or(TelemetryRecoveryError::Limits)?;
        self.charge(TelemetryRecoveryUsage {
            read_bytes: probe,
            retained_bytes: length,
            work_bytes: length
                .checked_add(probe)
                .ok_or(TelemetryRecoveryError::Limits)?,
            ..Default::default()
        })?;
        let mut bytes = self.allocate_buffer(length)?;
        bytes.resize(length, 0);
        let mut position = 0;
        while position < length {
            let request = (length - position).min(16 * 1024);
            if let Err(error) = self.charge(TelemetryRecoveryUsage {
                work_bytes: request
                    .checked_add(1)
                    .ok_or(TelemetryRecoveryError::Limits)?,
                ..Default::default()
            }) {
                return Err(retain_buffer(bytes, error));
            }
            let actual = self.io(|| file.read(&mut bytes[position..position + request]));
            match actual {
                Ok(0) => {
                    self.refused = true;
                    return Err(retain_buffer(
                        bytes,
                        TelemetryRecoveryError::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "recovery file shrank during read",
                        )),
                    ));
                }
                Ok(count) => position += count,
                Err(TelemetryRecoveryError::Io(ref error))
                    if error.kind() == io::ErrorKind::Interrupted
                        && !self.expired_after_operation =>
                {
                    continue;
                }
                Err(error) => return Err(retain_buffer(bytes, error)),
            }
        }
        loop {
            if let Err(error) = self.charge(TelemetryRecoveryUsage {
                work_bytes: 2,
                ..Default::default()
            }) {
                return Err(retain_buffer(bytes, error));
            }
            match self.io(|| file.read(&mut [0_u8; 1])) {
                Ok(0) => break,
                Ok(_) => {
                    self.refused = true;
                    return Err(retain_buffer(bytes, TelemetryRecoveryError::InvalidInput));
                }
                Err(TelemetryRecoveryError::Io(ref error))
                    if error.kind() == io::ErrorKind::Interrupted
                        && !self.expired_after_operation =>
                {
                    continue;
                }
                Err(error) => return Err(retain_buffer(bytes, error)),
            }
        }
        Ok(bytes)
    }

    // Walk every parent through O_NOFOLLOW directory FDs, then open the final
    // regular file O_NONBLOCK|O_NOFOLLOW. A FIFO/device/symlink cannot block the
    // pre-metadata open or redirect this reader outside its explicit path.
    #[cfg(unix)]
    pub(crate) fn open_path(
        &mut self,
        path: &Path,
        access: RecoveryFileAccess,
    ) -> Result<File, TelemetryRecoveryError> {
        use rustix::fs::{Mode, OFlags, open, openat};
        use std::path::Component;
        self.check()?;
        if path.as_os_str().len() > self.limits.max_path_bytes {
            return self.invalid();
        }
        let directory_flags = OFlags::RDONLY
            | OFlags::DIRECTORY
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC;
        let mut parent = self.io(|| {
            open(
                if path.is_absolute() { "/" } else { "." },
                directory_flags,
                Mode::empty(),
            )
            .map_err(io::Error::from)
        })?;
        let mut parts = path.components().peekable();
        while let Some(component) = parts.next() {
            let Component::Normal(name) = component else {
                if matches!(component, Component::RootDir | Component::CurDir) {
                    continue;
                }
                return self.invalid();
            };
            self.charge(TelemetryRecoveryUsage {
                work_bytes: name
                    .len()
                    .checked_add(1)
                    .ok_or(TelemetryRecoveryError::Limits)?,
                ..Default::default()
            })?;
            if parts.peek().is_none() {
                let flags = match access {
                    RecoveryFileAccess::Read => OFlags::RDONLY,
                    RecoveryFileAccess::Temporary => OFlags::WRONLY | OFlags::CREATE,
                    RecoveryFileAccess::Lock => OFlags::RDWR | OFlags::CREATE,
                } | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC;
                let fd = self.io(|| {
                    openat(&parent, name, flags, Mode::RUSR | Mode::WUSR).map_err(io::Error::from)
                })?;
                let file = File::from(fd);
                let metadata = self.io(|| file.metadata())?;
                if !metadata.is_file() {
                    return self.invalid();
                }
                if matches!(access, RecoveryFileAccess::Temporary) {
                    use std::os::unix::fs::MetadataExt;
                    if metadata.nlink() != 1 {
                        return self.invalid();
                    }
                    self.io(|| file.set_len(0))?;
                }
                return Ok(file);
            }
            parent = self.io(|| {
                openat(&parent, name, directory_flags, Mode::empty()).map_err(io::Error::from)
            })?;
        }
        self.invalid()
    }

    #[cfg(not(unix))]
    pub(crate) fn open_path(
        &mut self,
        _: &Path,
        _: RecoveryFileAccess,
    ) -> Result<File, TelemetryRecoveryError> {
        // Legacy constructors remain portable. Owned recovery requires an
        // actual nofollow/nonblocking file producer on the selected platform.
        self.invalid()
    }

    pub(crate) fn allocate_buffer(
        &mut self,
        length: usize,
    ) -> Result<Vec<u8>, TelemetryRecoveryError> {
        let mut bytes = Vec::new();
        // Callers prepay `length`. Capture real capacity/error before a clock
        // check; an excess or late allocation remains owned by the error.
        if let Err(error) = bytes.try_reserve_exact(length) {
            self.refused = true;
            return Err(retain_buffer(
                bytes,
                TelemetryRecoveryError::Allocation(error),
            ));
        }
        let extra = bytes.capacity().saturating_sub(length);
        if let Err(error) = self.charge(TelemetryRecoveryUsage {
            retained_bytes: extra,
            ..Default::default()
        }) {
            return Err(retain_buffer(bytes, error));
        }
        Ok(bytes)
    }

    pub(crate) fn write_file(
        &mut self,
        file: &mut File,
        bytes: &[u8],
    ) -> Result<(), TelemetryRecoveryError> {
        use io::Write;
        let mut position = 0;
        while position < bytes.len() {
            let request = (bytes.len() - position).min(16 * 1024);
            self.charge(TelemetryRecoveryUsage {
                work_bytes: request
                    .checked_add(1)
                    .ok_or(TelemetryRecoveryError::Limits)?,
                ..Default::default()
            })?;
            match self.io(|| file.write(&bytes[position..position + request])) {
                Ok(0) => {
                    self.refused = true;
                    return Err(TelemetryRecoveryError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "recovery write made no progress",
                    )));
                }
                Ok(count) => position += count,
                Err(TelemetryRecoveryError::Io(ref error))
                    if error.kind() == io::ErrorKind::Interrupted
                        && !self.expired_after_operation =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    // The first pass retains no JSON tree. Every typed-state slot is prepaid
    // at twice the largest actual Rust element size supplied by its owner;
    // this covers typed vector growth. Decoded strings are prepaid separately.
    pub(crate) fn decode_json<T: DeserializeOwned>(
        &mut self,
        bytes: &[u8],
        largest_element_bytes: usize,
    ) -> Result<T, TelemetryRecoveryError> {
        let slot = largest_element_bytes
            .checked_mul(2)
            .ok_or(TelemetryRecoveryError::Limits)?;
        self.charge(TelemetryRecoveryUsage {
            // serde's escaped-string scratch can grow geometrically.
            retained_bytes: bytes
                .len()
                .checked_mul(2)
                .ok_or(TelemetryRecoveryError::Limits)?,
            work_bytes: bytes
                .len()
                .checked_mul(2)
                .ok_or(TelemetryRecoveryError::Limits)?,
            ..Default::default()
        })?;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        let scanned = Census { budget: self, slot }
            .deserialize(&mut decoder)
            .and_then(|()| decoder.end());
        if let Err(error) = scanned {
            if self.refused {
                self.check()?;
            }
            self.refused = true;
            return Err(TelemetryRecoveryError::Json(error));
        }
        // The census owns an escaped-string scratch Vec. Retire it before the
        // typed decoder can allocate its own scratch on the same input.
        drop(decoder);
        self.check()?;
        let value = serde_json::from_slice(bytes).map_err(|error| {
            self.refused = true;
            TelemetryRecoveryError::Json(error)
        })?;
        self.check()?;
        Ok(value)
    }

    pub(crate) fn hash_json<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<blake3::Hash, TelemetryRecoveryError> {
        struct HashWriter<'a> {
            budget: &'a mut TelemetryRecoveryBudget,
            hash: blake3::Hasher,
        }
        impl io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.budget
                    .charge(TelemetryRecoveryUsage {
                        work_bytes: bytes.len(),
                        ..Default::default()
                    })
                    .map_err(io::Error::other)?;
                self.hash.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut writer = HashWriter {
            budget: self,
            hash: blake3::Hasher::new(),
        };
        serde_json::to_writer(&mut writer, value).map_err(TelemetryRecoveryError::Json)?;
        writer.budget.check()?;
        Ok(writer.hash.finalize())
    }

    pub(crate) fn encode_json<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<Vec<u8>, TelemetryRecoveryError> {
        struct Counter<'a> {
            budget: &'a mut TelemetryRecoveryBudget,
            bytes: usize,
        }
        impl io::Write for Counter<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.bytes = self
                    .bytes
                    .checked_add(bytes.len())
                    .ok_or_else(|| io::Error::other("JSON size overflow"))?;
                if self.bytes > self.budget.limits.max_file_bytes {
                    self.budget.refused = true;
                    return Err(io::Error::other("JSON exceeds recovery file allowance"));
                }
                self.budget
                    .charge(TelemetryRecoveryUsage {
                        work_bytes: bytes.len(),
                        ..Default::default()
                    })
                    .map_err(io::Error::other)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter {
            budget: self,
            bytes: 0,
        };
        serde_json::to_writer(&mut counter, value).map_err(TelemetryRecoveryError::Json)?;
        let length = counter.bytes;
        self.charge(TelemetryRecoveryUsage {
            retained_bytes: length,
            work_bytes: length,
            ..Default::default()
        })?;
        let mut bytes = self.allocate_buffer(length)?;
        // A fixed slice writer cannot grow the actual buffer past its charged
        // allocation. The independent first pass supplies its exact length.
        bytes.resize(length, 0);
        let mut remaining = bytes.as_mut_slice();
        if let Err(error) = serde_json::to_writer(&mut remaining, value) {
            self.refused = true;
            return Err(retain_buffer(bytes, TelemetryRecoveryError::Json(error)));
        }
        if !remaining.is_empty() {
            return self.invalid();
        }
        if let Err(error) = self.check() {
            return Err(retain_buffer(bytes, error));
        }
        Ok(bytes)
    }
}

fn retain_buffer(bytes: Vec<u8>, cause: TelemetryRecoveryError) -> TelemetryRecoveryError {
    TelemetryRecoveryError::Buffer {
        buffer: TelemetryRecoveryBuffer(bytes),
        cause: Box::new(cause),
    }
}

struct Census<'a> {
    budget: &'a mut TelemetryRecoveryBudget,
    slot: usize,
}
impl<'de> DeserializeSeed<'de> for Census<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.budget
            .charge(TelemetryRecoveryUsage {
                json_values: 1,
                retained_bytes: self.slot,
                work_bytes: 1,
                ..Default::default()
            })
            .map_err(serde::de::Error::custom)?;
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Census<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded persisted JSON")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        self.budget
            .charge(TelemetryRecoveryUsage {
                string_bytes: value.len(),
                // Typed storage and its index-owned copy are both charged.
                retained_bytes: value
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| E::custom("string overflow"))?,
                work_bytes: value.len(),
                ..Default::default()
            })
            .map_err(E::custom)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        while values
            .next_element_seed(Census {
                budget: self.budget,
                slot: self.slot,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        self.budget
            .charge(TelemetryRecoveryUsage {
                records: 1,
                ..Default::default()
            })
            .map_err(serde::de::Error::custom)?;
        while values
            .next_key_seed(Census {
                budget: self.budget,
                slot: self.slot,
            })?
            .is_some()
        {
            values.next_value_seed(Census {
                budget: self.budget,
                slot: self.slot,
            })?;
        }
        Ok(())
    }
}

pub(crate) fn largest_element<T>() -> usize {
    size_of::<T>().max(size_of::<String>())
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    pub(crate) struct Directory(pub(crate) PathBuf);
    impl Directory {
        pub(crate) fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "telemetry-recovery-{}-{stamp}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("unique fixture directory");
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).expect("fixture cleanup")
        }
    }
    pub(crate) fn limits() -> TelemetryRecoveryLimits {
        TelemetryRecoveryLimits {
            max_path_bytes: 4096,
            max_file_bytes: 16 * 1024,
            max_read_bytes: 128 * 1024,
            max_directory_entries: 32,
            max_records: 256,
            max_json_values: 4096,
            max_string_bytes: 128 * 1024,
            max_retained_bytes: 8 * 1024 * 1024,
            max_work_bytes: 8 * 1024 * 1024,
        }
    }
    pub(crate) fn budget() -> TelemetryRecoveryBudget {
        TelemetryRecoveryBudget::new(limits(), Instant::now() + Duration::from_secs(30))
            .expect("budget")
    }

    #[test]
    fn regular_file_sizes_and_cumulative_reads_are_admitted_before_reading() {
        let directory = Directory::new();
        let path = directory.0.join("actual.json");
        fs::write(&path, b"1234").expect("fixture");
        let mut policy = limits();
        policy.max_file_bytes = 3;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(matches!(
            guard.read_file(&path),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(guard.usage().read_bytes, 0);
        let framing = guard.usage();
        assert_eq!(
            framing,
            TelemetryRecoveryUsage {
                work_bytes: framing.work_bytes,
                ..Default::default()
            }
        );
        assert!(framing.work_bytes > 0);
        assert!(guard.check().is_err());

        policy.max_file_bytes = 4;
        policy.max_read_bytes = 5;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert_eq!(guard.read_file(&path).expect("exact file"), b"1234");
        let used = guard.usage();
        assert_eq!(used.read_bytes, 5);
        assert!(matches!(
            guard.read_file(&path),
            Err(TelemetryRecoveryError::Limits)
        ));
        // The same path/open/stat framing enters before the read-byte refusal.
        // No second payload read or allocation is admitted by that refusal.
        assert_eq!(
            guard.usage(),
            TelemetryRecoveryUsage {
                work_bytes: used.work_bytes + framing.work_bytes,
                ..used
            }
        );
        assert!(guard.check().is_err());
    }

    #[test]
    fn json_records_and_strings_refuse_before_typed_deserialization() {
        struct MustNotDecode;
        impl<'de> serde::Deserialize<'de> for MustNotDecode {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                panic!("typed allocation must not start after structural refusal")
            }
        }
        let mut policy = limits();
        policy.max_records = 1;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(matches!(
            guard.decode_json::<MustNotDecode>(br#"[{"a":1},{"b":2}]"#, 32),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(guard.usage().records, 1);
        policy.max_records = 10;
        policy.max_string_bytes = 2;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(
            guard
                .decode_json::<MustNotDecode>(br#"{"a":"long"}"#, 32)
                .is_err()
        );
    }

    #[test]
    fn expired_attempt_performs_no_new_io_and_late_actual_error_is_preserved() {
        let directory = Directory::new();
        let mut guard = budget();
        guard.expiry = Instant::now();
        let missing = directory.0.join("absent");
        assert!(matches!(
            guard.read_file(&missing),
            Err(TelemetryRecoveryError::Expired)
        ));
        assert_eq!(guard.last_io_error(), None);
        let mut guard = budget();
        guard.expiry = Instant::now() + Duration::from_millis(100);
        let actual = guard.io(|| {
            std::thread::sleep(Duration::from_millis(150));
            File::open(&missing)
        });
        assert!(
            matches!(actual, Err(TelemetryRecoveryError::Io(ref error)) if error.kind() == io::ErrorKind::NotFound)
        );
        assert_eq!(guard.last_io_error(), Some(io::ErrorKind::NotFound));
        assert!(guard.expired_after_operation());
    }

    #[test]
    fn nofollow_nonblocking_reader_refuses_symlinks_and_a_real_fifo() {
        use std::os::unix::fs::symlink;
        let directory = Directory::new();
        let actual = directory.0.join("actual");
        fs::write(&actual, b"actual data").expect("regular file");
        let linked = directory.0.join("linked");
        symlink(&actual, &linked).expect("file symlink");
        assert!(budget().read_file(&linked).is_err());
        let parent_link = directory.0.join("parent-link");
        symlink(&directory.0, &parent_link).expect("parent symlink");
        assert!(budget().read_file(&parent_link.join("actual")).is_err());
        let fifo = directory.0.join("fifo");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .expect("actual FIFO");
        assert!(matches!(
            budget().read_file(&fifo),
            Err(TelemetryRecoveryError::InvalidInput)
        ));
        assert_eq!(
            fs::read(actual).expect("regular source unchanged"),
            b"actual data"
        );
    }

    #[test]
    fn census_scratch_is_retired_before_escaped_string_typed_decode() {
        let mut input = String::from("\"");
        input.push_str(&"x".repeat(8190));
        input.push_str("\\n\"");
        let value: String = budget()
            .decode_json(input.as_bytes(), std::mem::size_of::<String>())
            .expect("bounded escaped string");
        assert_eq!(value.len(), 8191);
        assert!(value.ends_with('\n'));
    }
}
