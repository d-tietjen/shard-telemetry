//! Bounded catalog recovery; this does not open an engine or publish readiness.

use super::*;
use crate::recovery::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};

/// Genuine recovered catalogs and their exclusive directory lease.
///
/// This preparatory owner does not create an engine, append pool, sink worker,
/// write fence or serving permission. Full owned attachment additionally needs
/// bounded sink/object recovery and worker construction in their own producers.
#[derive(Debug)]
pub struct TelemetryRecoveredCatalogs {
    directory: PathBuf,
    _deletes: DeleteCatalog,
    _append_receipts: AppendReceiptCatalog,
    // Field declaration order is drop order: release the exclusive lease last.
    _lease: DataDirectoryLease,
}

impl TelemetryRecoveredCatalogs {
    /// Recovers actual delete/retry state using the same cumulative ledger and
    /// original expiry. Directory ownership lasts until this value is dropped.
    /// A refusal can follow a completed format/migration/pruning filesystem
    /// operation; it never claims those physical operations did not occur.
    ///
    /// Callers must keep directory names stable and coordinate every writer
    /// with this lease. Subsequent path-based reads, rename and removal are
    /// not atomic with the retained directory FD: concurrent pathname
    /// substitution can target a replacement tree. This preparatory API is
    /// not a namespace fence or qualification of full recovered-engine startup.
    pub fn recover(
        directory: PathBuf,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, TelemetryRecoveryError> {
        if !cfg!(any(target_os = "linux", target_os = "android")) {
            return budget.invalid();
        }
        budget.charge(TelemetryRecoveryUsage {
            retained_bytes: std::mem::size_of::<Self>()
                .checked_add(directory.capacity())
                .ok_or(TelemetryRecoveryError::Limits)?,
            work_bytes: directory.as_os_str().len(),
            ..Default::default()
        })?;
        let lease = DataDirectoryLease::acquire_with_recovery_budget(&directory, budget)?;
        let delete_path =
            budget.join_path(&directory, std::ffi::OsStr::new("delete-catalog-v1.json"))?;
        let deletes = DeleteCatalog::open_with_recovery_budget(delete_path, budget)?;
        let append_receipts = AppendReceiptCatalog::open_with_recovery_budget(&directory, budget)?;
        budget.check()?;
        Ok(Self {
            directory,
            _lease: lease,
            _deletes: deletes,
            _append_receipts: append_receipts,
        })
    }

    /// The exact directory whose real catalogs and lease this value retains.
    pub fn data_directory(&self) -> &Path {
        &self.directory
    }
}

impl AppendReceiptCatalog {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn open_with_recovery_budget(
        data_directory: &Path,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, TelemetryRecoveryError> {
        budget.charge(TelemetryRecoveryUsage {
            retained_bytes: std::mem::size_of::<Self>(),
            ..Default::default()
        })?;
        let directory = budget.join_path(
            data_directory,
            std::ffi::OsStr::new("native-append-receipts-v2"),
        )?;
        let directory_sync = budget.ensure_directory(&directory)?;
        let buffer_bytes = budget
            .path_limit()
            .checked_add(std::mem::size_of::<rustix::fs::RawDirEntry<'_>>())
            .and_then(|n| n.checked_add(std::mem::align_of::<usize>()))
            .ok_or(TelemetryRecoveryError::Limits)?;
        budget.charge(TelemetryRecoveryUsage {
            retained_bytes: buffer_bytes,
            ..Default::default()
        })?;
        let mut directory_buffer = budget.allocate_buffer(buffer_bytes)?;
        let mut entries =
            rustix::fs::RawDir::new(&directory_sync, directory_buffer.spare_capacity_mut());
        let mut state = AppendReceiptState::default();
        loop {
            // Include EOF and unrelated files: enumeration cannot bypass the
            // bound by adding names which the catalog later ignores.
            budget.charge(TelemetryRecoveryUsage {
                directory_entries: 1,
                // RawDir borrows this prepaid fixed buffer and returns a
                // borrowed CStr. It neither grows storage nor retries EINTR.
                work_bytes: if entries.is_buffer_empty() {
                    buffer_bytes
                } else {
                    1
                },
                ..Default::default()
            })?;
            budget.check()?;
            let actual = entries.next().transpose().map_err(std::io::Error::from);
            let entry = match budget.completed_io(actual) {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(TelemetryRecoveryError::Io(ref error))
                    if error.kind() == std::io::ErrorKind::Interrupted
                        && !budget.expired_after_operation() =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            use std::os::unix::ffi::OsStrExt;
            let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
            if matches!(name.to_str(), Some("." | "..")) {
                continue;
            }
            if name.len() > budget.path_limit() {
                return budget.invalid();
            }
            let path = budget.join_path(&directory, name)?;
            if path.as_os_str().len() > budget.path_limit() {
                return budget.invalid();
            }
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let bytes = budget.read_file(&path)?;
            let receipt: PersistedAppendReceipt = budget.decode_json(
                &bytes,
                std::mem::size_of::<(String, AppendReceiptStateEntry)>(),
            )?;
            if path
                .file_stem()
                .is_none_or(|stem| stem != receipt.request_id.as_str())
            {
                return budget.invalid();
            }
            Self::insert_bounded_recovered(&mut state, receipt, budget)?;
        }
        #[allow(
            clippy::drop_non_drop,
            reason = "end the actual RawDir descriptor and scratch-buffer borrows before releasing either owner"
        )]
        drop(entries);
        drop(directory_buffer);
        let legacy_path = budget.join_path(
            data_directory,
            std::ffi::OsStr::new("native-append-receipts-v1.json"),
        )?;
        budget.check()?;
        if budget.io(|| legacy_path.try_exists())? {
            let bytes = budget.read_file(&legacy_path)?;
            let persisted: PersistedAppendReceipts = budget.decode_json(
                &bytes,
                std::mem::size_of::<(String, AppendReceiptStateEntry)>(),
            )?;
            if persisted.version != APPEND_RECEIPTS_VERSION {
                return budget.invalid();
            }
            // A prior bounded/crashed migration may have published only part
            // of v2. Reconcile the retained v1 source even when v2 exists.
            // Never advertise that partial publication as complete recovery.
            for (index, receipt) in persisted.receipts.iter().enumerate() {
                budget.charge(TelemetryRecoveryUsage {
                    work_bytes: index
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(receipt.request_id.len()))
                        .ok_or(TelemetryRecoveryError::Limits)?,
                    ..Default::default()
                })?;
                if persisted.receipts[..index]
                    .iter()
                    .any(|prior| prior.request_id == receipt.request_id)
                {
                    return budget.invalid();
                }
            }
            let cutoff =
                unix_nanos_now().saturating_sub(duration_to_nanos(NATIVE_APPEND_RECEIPT_RETENTION));
            for receipt in persisted.receipts {
                if receipt.recorded_at_unix_nanos < cutoff {
                    continue;
                }
                budget.charge(TelemetryRecoveryUsage {
                    work_bytes: state
                        .receipts
                        .len()
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(receipt.request_id.len()))
                        .and_then(|n| n.checked_add(receipt.payload_digest.len()))
                        .and_then(|n| {
                            n.checked_add(
                                receipt
                                    .acknowledgement
                                    .partitions
                                    .len()
                                    .checked_mul(std::mem::size_of::<crate::NativePartitionAck>())?,
                            )
                        })
                        .ok_or(TelemetryRecoveryError::Limits)?,
                    ..Default::default()
                })?;
                if let Some(AppendReceiptStateEntry::Complete(existing)) =
                    state.receipts.get(&receipt.request_id)
                {
                    if existing != &receipt {
                        return budget.invalid();
                    }
                    continue;
                }
                Self::insert_bounded_recovered(&mut state, receipt, budget)?;
            }
        }
        budget.check()?;
        let catalog = Self {
            directory,
            directory_sync,
            state: Mutex::new(state),
            changed: Condvar::new(),
        };
        // There are no concurrent users before this owner is published.
        let mut state = catalog
            .state
            .lock()
            .map_err(|_| TelemetryRecoveryError::InvalidInput)?;
        let cutoff =
            unix_nanos_now().saturating_sub(duration_to_nanos(NATIVE_APPEND_RECEIPT_RETENTION));
        while state.completed_by_time.first().is_some_and(|(at, _)| {
            *at < cutoff || state.completed_by_time.len() > MAX_NATIVE_APPEND_RECEIPTS
        }) {
            let (_, id) = state
                .completed_by_time
                .first()
                .ok_or(TelemetryRecoveryError::InvalidInput)?;
            let path = catalog.bounded_receipt_path(id, budget)?;
            // An expired v1 receipt has not yet acquired a v2 file. Bound its
            // in-memory retirement without requiring a fabricated v2 record.
            budget.io(|| match fs::remove_file(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                result => result,
            })?;
            let (_, id) = state
                .completed_by_time
                .pop_first()
                .ok_or(TelemetryRecoveryError::InvalidInput)?;
            if !matches!(
                state.receipts.remove(&id),
                Some(AppendReceiptStateEntry::Complete(_))
            ) {
                return budget.invalid();
            }
        }
        for receipt in state.receipts.values() {
            let AppendReceiptStateEntry::Complete(receipt) = receipt else {
                return budget.invalid();
            };
            let path = catalog.bounded_receipt_path(&receipt.request_id, budget)?;
            if !budget.io(|| path.try_exists())? {
                let bytes = budget.encode_json(receipt)?;
                let temporary = budget.suffixed_path(&path, ".tmp")?;
                budget.check()?;
                let mut file =
                    budget.open_path(&temporary, crate::recovery::RecoveryFileAccess::Temporary)?;
                budget.write_file(&mut file, &bytes)?;
                budget.io(|| file.sync_data())?;
                budget.io(|| fs::rename(temporary, path))?;
                budget.io(|| catalog.directory_sync.sync_all())?;
            }
        }
        drop(state);
        budget.check()?;
        Ok(catalog)
    }

    fn bounded_receipt_path(
        &self,
        id: &str,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<PathBuf, TelemetryRecoveryError> {
        let base = budget.join_path(&self.directory, std::ffi::OsStr::new(id))?;
        budget.suffixed_path(&base, ".json")
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn open_with_recovery_budget(
        _: &Path,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, TelemetryRecoveryError> {
        budget.invalid()
    }

    fn insert_bounded_recovered(
        state: &mut AppendReceiptState,
        receipt: PersistedAppendReceipt,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<(), TelemetryRecoveryError> {
        let strings = receipt
            .request_id
            .len()
            .checked_mul(2)
            .ok_or(TelemetryRecoveryError::Limits)?;
        let comparisons = state
            .receipts
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_mul(receipt.request_id.len() + receipt.payload_digest.len()))
            .ok_or(TelemetryRecoveryError::Limits)?;
        budget.charge(TelemetryRecoveryUsage {
            retained_bytes: std::mem::size_of::<(String, AppendReceiptStateEntry)>()
                .checked_add(std::mem::size_of::<(u64, String)>())
                .and_then(|n| n.checked_add(strings))
                .ok_or(TelemetryRecoveryError::Limits)?,
            work_bytes: comparisons,
            ..Default::default()
        })?;
        if receipt.request_id.len() != 32
            || receipt.payload_digest.len() != 64
            || !receipt
                .request_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !receipt
                .payload_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return budget.invalid();
        }
        // This existing validator preserves duplicate refusal and receipt/index
        // insertion semantics. Its actual error is retained, including late errors.
        let actual = Self::insert_recovered(state, receipt);
        budget.catalog_result(actual)
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests {
    use super::*;
    use crate::recovery::tests::{Directory, budget, limits};

    fn receipt(id: u128) -> PersistedAppendReceipt {
        PersistedAppendReceipt {
            request_id: request_key(id),
            payload_digest: "a".repeat(64),
            recorded_at_unix_nanos: unix_nanos_now(),
            acknowledgement: crate::NativeTelemetryAppendAck {
                partitions: Vec::new(),
            },
        }
    }

    fn write_v2(directory: &Path, id: u128) -> Vec<u8> {
        let path = directory.join("native-append-receipts-v2");
        fs::create_dir_all(&path).expect("catalog directory");
        let bytes = serde_json::to_vec(&receipt(id)).expect("receipt bytes");
        fs::write(path.join(format!("{}.json", request_key(id))), &bytes).expect("actual receipt");
        bytes
    }

    #[test]
    fn directory_enumeration_includes_unrelated_files_and_is_finite() {
        let directory = Directory::new();
        let path = directory.0.join("native-append-receipts-v2");
        fs::create_dir(&path).expect("catalog directory");
        fs::write(path.join("unrelated-a"), b"not JSON").expect("file");
        fs::write(path.join("unrelated-b"), b"not JSON").expect("file");
        let mut policy = limits();
        policy.max_directory_entries = 1;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(matches!(
            AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut guard),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(guard.usage().directory_entries, 1);
        assert_eq!(guard.usage().read_bytes, 0);
        assert_eq!(fs::read_dir(path).expect("retained directory").count(), 2);
    }

    #[test]
    fn v2_recovery_spends_one_cumulative_read_budget_without_modifying_files() {
        let directory = Directory::new();
        let first = write_v2(&directory.0, 7);
        let second = write_v2(&directory.0, 8);
        assert_eq!(first.len(), second.len());
        let mut policy = limits();
        policy.max_read_bytes = first.len() + 1;
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(matches!(
            AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut guard),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(guard.usage().read_bytes, first.len() + 1);
        for (id, bytes) in [(7, first), (8, second)] {
            assert_eq!(
                fs::read(
                    directory
                        .0
                        .join("native-append-receipts-v2")
                        .join(format!("{}.json", request_key(id)))
                )
                .expect("retained bytes"),
                bytes
            );
        }
    }

    #[test]
    fn oversized_legacy_receipt_json_refuses_before_decode_or_migration() {
        let directory = Directory::new();
        let path = directory.0.join("native-append-receipts-v1.json");
        let bytes = vec![b' '; limits().max_file_bytes + 1];
        fs::write(&path, &bytes).expect("legacy file");
        let mut guard = budget();
        assert!(matches!(
            AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut guard),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(guard.usage().read_bytes, 0);
        assert_eq!(fs::read(path).expect("unchanged legacy"), bytes);
        assert_eq!(
            fs::read_dir(directory.0.join("native-append-receipts-v2"))
                .expect("directory")
                .count(),
            0
        );
    }

    #[test]
    fn bounded_migration_and_restart_preserve_retry_identity_and_order() {
        let directory = Directory::new();
        let legacy = PersistedAppendReceipts {
            version: APPEND_RECEIPTS_VERSION,
            receipts: vec![receipt(8), receipt(7)],
        };
        let original = serde_json::to_vec(&legacy).expect("legacy bytes");
        fs::write(
            directory.0.join("native-append-receipts-v1.json"),
            &original,
        )
        .expect("legacy file");
        let catalog = AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut budget())
            .expect("bounded migrate");
        for id in [7, 8] {
            assert!(matches!(
                catalog.reserve(id, &"a".repeat(64)).expect("same retry"),
                AppendReceiptReservation::Existing(_)
            ));
        }
        assert_eq!(
            catalog
                .state
                .lock()
                .expect("state")
                .receipts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![request_key(7), request_key(8)]
        );
        drop(catalog);
        let reopened = AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut budget())
            .expect("bounded restart");
        assert_eq!(reopened.state.lock().expect("state").receipts.len(), 2);
        drop(reopened);
        let legacy_api =
            AppendReceiptCatalog::open(&directory.0).expect("legacy constructor unchanged");
        assert!(matches!(
            legacy_api
                .reserve(7, &"a".repeat(64))
                .expect("legacy retry"),
            AppendReceiptReservation::Existing(_)
        ));
        assert_eq!(
            fs::read(directory.0.join("native-append-receipts-v1.json"))
                .expect("legacy source retained"),
            original
        );
    }

    #[test]
    fn recovered_catalog_owner_holds_the_actual_exclusive_directory_lease() {
        let directory = Directory::new();
        let recovered = TelemetryRecoveredCatalogs::recover(directory.0.clone(), &mut budget())
            .expect("real catalogs");
        assert_eq!(recovered.data_directory(), directory.0);
        assert!(DataDirectoryLease::acquire(&directory.0).is_err());
        drop(recovered);
        assert!(DataDirectoryLease::acquire(&directory.0).is_ok());
    }

    #[test]
    fn oversized_original_path_capacity_refuses_before_directory_mutation() {
        let directory = Directory::new();
        let mut supplied = PathBuf::with_capacity(8192);
        supplied.push(&directory.0);
        let mut policy = limits();
        policy.max_retained_bytes = 4096;
        assert!(supplied.capacity() > policy.max_retained_bytes);
        let mut guard =
            TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                .expect("guard");
        assert!(matches!(
            TelemetryRecoveredCatalogs::recover(supplied, &mut guard),
            Err(TelemetryRecoveryError::Limits)
        ));
        assert_eq!(
            fs::read_dir(&directory.0)
                .expect("fixture directory")
                .count(),
            0
        );
    }

    #[test]
    fn symlink_parent_is_refused_before_creating_an_absent_child() {
        use std::os::unix::fs::symlink;
        let directory = Directory::new();
        let real = directory.0.join("real");
        fs::create_dir(&real).expect("real directory");
        let linked = directory.0.join("link");
        symlink(&real, &linked).expect("parent symlink");
        assert!(TelemetryRecoveredCatalogs::recover(linked.join("absent"), &mut budget()).is_err());
        assert!(!real.join("absent").exists());
    }

    #[test]
    fn expired_v1_receipts_need_no_fabricated_v2_file() {
        let directory = Directory::new();
        let mut expired = receipt(6);
        expired.recorded_at_unix_nanos = 0;
        let legacy = PersistedAppendReceipts {
            version: APPEND_RECEIPTS_VERSION,
            receipts: vec![expired, receipt(7)],
        };
        fs::write(
            directory.0.join("native-append-receipts-v1.json"),
            serde_json::to_vec(&legacy).expect("legacy bytes"),
        )
        .expect("actual legacy file");
        let catalog = AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut budget())
            .expect("bounded migration");
        assert_eq!(
            catalog
                .state
                .lock()
                .expect("state")
                .receipts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![request_key(7)]
        );
        assert!(
            !directory
                .0
                .join("native-append-receipts-v2")
                .join(format!("{}.json", request_key(6)))
                .exists()
        );
        drop(catalog);
        assert!(
            AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut budget()).is_ok()
        );
    }

    #[test]
    fn partial_v1_publication_restarts_without_losing_remaining_retry_identities() {
        let legacy = PersistedAppendReceipts {
            version: APPEND_RECEIPTS_VERSION,
            receipts: vec![receipt(7), receipt(8)],
        };
        let bytes = serde_json::to_vec(&legacy).expect("legacy bytes");
        let measure = Directory::new();
        fs::write(measure.0.join("native-append-receipts-v1.json"), &bytes).expect("source");
        let mut full = budget();
        drop(
            AppendReceiptCatalog::open_with_recovery_budget(&measure.0, &mut full)
                .expect("measure genuine full recovery"),
        );
        let mut lower = 1;
        let mut upper = full.usage().work_bytes + 1024;
        let mut interrupted = None;
        // Find a real cumulative allowance which stops after exactly one
        // completed rename. No fake I/O, injected readiness or reset ledger.
        for _ in 0..32 {
            if lower > upper {
                break;
            }
            let allowance = lower + (upper - lower) / 2;
            let actual = Directory::new();
            fs::write(actual.0.join("native-append-receipts-v1.json"), &bytes).expect("source");
            let mut policy = limits();
            policy.max_work_bytes = allowance;
            let mut guard =
                TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(30))
                    .expect("guard");
            let result = AppendReceiptCatalog::open_with_recovery_budget(&actual.0, &mut guard);
            let published = fs::read_dir(actual.0.join("native-append-receipts-v2"))
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|entry| {
                            entry
                                .path()
                                .extension()
                                .is_some_and(|suffix| suffix == "json")
                        })
                        .count()
                })
                .unwrap_or(0);
            if published == 1 && result.is_err() {
                interrupted = Some(actual);
                break;
            }
            if published == 0 {
                lower = allowance + 1
            } else {
                upper = allowance.saturating_sub(1)
            }
        }
        let interrupted = interrupted.expect("a real bounded interruption after first publication");
        let first_path = interrupted
            .0
            .join("native-append-receipts-v2")
            .join(format!("{}.json", request_key(7)));
        let first = fs::read(&first_path).expect("first actual committed receipt");
        let resumed =
            AppendReceiptCatalog::open_with_recovery_budget(&interrupted.0, &mut budget())
                .expect("reconcile complete source after interruption");
        for id in [7, 8] {
            assert!(matches!(
                resumed
                    .reserve(id, &"a".repeat(64))
                    .expect("original retry"),
                AppendReceiptReservation::Existing(_)
            ));
        }
        assert_eq!(
            fs::read(first_path).expect("first receipt unchanged"),
            first
        );
        assert_eq!(
            fs::read(interrupted.0.join("native-append-receipts-v1.json"))
                .expect("legacy input unchanged"),
            bytes
        );
    }

    #[test]
    fn partial_migration_conflict_is_explicit_instead_of_selecting_a_subset() {
        let directory = Directory::new();
        write_v2(&directory.0, 7);
        let mut conflicting = receipt(7);
        conflicting.payload_digest = "b".repeat(64);
        let legacy = PersistedAppendReceipts {
            version: APPEND_RECEIPTS_VERSION,
            receipts: vec![conflicting, receipt(8)],
        };
        fs::write(
            directory.0.join("native-append-receipts-v1.json"),
            serde_json::to_vec(&legacy).expect("legacy bytes"),
        )
        .expect("source");
        assert!(matches!(
            AppendReceiptCatalog::open_with_recovery_budget(&directory.0, &mut budget()),
            Err(TelemetryRecoveryError::InvalidInput)
        ));
        assert!(
            !directory
                .0
                .join("native-append-receipts-v2")
                .join(format!("{}.json", request_key(8)))
                .exists()
        );
    }
}
