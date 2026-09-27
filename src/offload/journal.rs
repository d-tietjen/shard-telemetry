use super::*;

impl CheckpointJournal {
    pub(super) fn open(path: PathBuf, source_id: Arc<str>) -> Result<Self, OffloadError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(journal_io_error)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .map_err(journal_io_error)?;
        lock.try_lock_exclusive().map_err(|error| {
            OffloadError::new(format!(
                "upstream offload checkpoint is already owned by another worker: {error}"
            ))
        })?;
        let persisted = match fs::read(&path) {
            Ok(encoded) => {
                serde_json::from_slice::<PersistedCheckpoints>(&encoded).map_err(|error| {
                    OffloadError::new(format!(
                        "invalid upstream offload checkpoint journal: {error}"
                    ))
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => PersistedCheckpoints {
                version: OFFLOAD_CHECKPOINT_VERSION,
                source_id: Some(source_id.to_string()),
                retry_namespace: Some(source_retry_namespace(&source_id)),
                checkpoints: Vec::new(),
            },
            Err(error) => return Err(journal_io_error(error)),
        };
        let retry_namespace = match persisted.version {
            1 => RetryNamespace::LegacyV1,
            2 => {
                validate_persisted_source(persisted.source_id.as_deref(), &source_id)?;
                RetryNamespace::Source(Arc::clone(&source_id))
            }
            OFFLOAD_CHECKPOINT_VERSION => {
                validate_persisted_source(persisted.source_id.as_deref(), &source_id)?;
                match persisted.retry_namespace.as_deref() {
                    Some("legacy-v1") => RetryNamespace::LegacyV1,
                    Some(namespace) if namespace == source_retry_namespace(&source_id) => {
                        RetryNamespace::Source(Arc::clone(&source_id))
                    }
                    _ => {
                        return Err(OffloadError::new(
                            "upstream offload checkpoint journal retry namespace is invalid",
                        ));
                    }
                }
            }
            version => {
                return Err(OffloadError::new(format!(
                    "unsupported upstream offload checkpoint journal version {version}",
                )));
            }
        };
        let mut offsets = BTreeMap::new();
        for checkpoint in persisted.checkpoints {
            let topic_partition = TopicPartition::new(
                TopicId::new(checkpoint.topic_id),
                LogicalPartitionId::new(checkpoint.partition_id),
            );
            if offsets
                .insert(topic_partition, LogicalOffset::new(checkpoint.next_offset))
                .is_some()
            {
                return Err(OffloadError::new(
                    "upstream offload checkpoint journal contains duplicate partitions",
                ));
            }
        }
        Ok(Self {
            path,
            source_id,
            retry_namespace,
            offsets,
            _exclusive_lock: lock,
        })
    }

    pub(super) fn next(&self, partition: TopicPartition) -> Option<LogicalOffset> {
        self.offsets.get(&partition).copied()
    }

    pub(super) fn advance(
        &mut self,
        partition: TopicPartition,
        next_offset: LogicalOffset,
    ) -> Result<(), OffloadError> {
        let previous = self.offsets.insert(partition, next_offset);
        if previous.is_some_and(|previous| previous > next_offset) {
            return Err(OffloadError::new(
                "upstream offload checkpoint attempted to move backward",
            ));
        }
        self.persist()
    }

    pub(super) fn snapshot(&self) -> Vec<OffloadCheckpoint> {
        self.offsets
            .iter()
            .map(|(topic_partition, next_offset)| OffloadCheckpoint {
                topic_partition: *topic_partition,
                next_offset: *next_offset,
            })
            .collect()
    }

    pub(super) fn retry_namespace(&self) -> RetryNamespace {
        self.retry_namespace.clone()
    }

    pub(super) fn persist(&self) -> Result<(), OffloadError> {
        let encoded = serde_json::to_vec(&PersistedCheckpoints {
            version: OFFLOAD_CHECKPOINT_VERSION,
            source_id: Some(self.source_id.to_string()),
            retry_namespace: Some(match &self.retry_namespace {
                RetryNamespace::LegacyV1 => "legacy-v1".to_owned(),
                RetryNamespace::Source(source_id) => source_retry_namespace(source_id),
            }),
            checkpoints: self
                .offsets
                .iter()
                .map(|(topic_partition, next_offset)| PersistedCheckpoint {
                    topic_id: topic_partition.topic_id.get(),
                    partition_id: topic_partition.partition_id.get(),
                    next_offset: next_offset.get(),
                })
                .collect(),
        })
        .map_err(|error| {
            OffloadError::new(format!(
                "upstream offload checkpoint serialization failed: {error}"
            ))
        })?;
        let temporary = temporary_path(&self.path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(journal_io_error)?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(journal_io_error)?;
        fs::rename(&temporary, &self.path).map_err(journal_io_error)?;
        File::open(self.path.parent().unwrap_or_else(|| Path::new(".")))
            .and_then(|directory| directory.sync_all())
            .map_err(journal_io_error)
    }
}

pub(super) fn validate_persisted_source(
    persisted_source: Option<&str>,
    configured_source: &str,
) -> Result<(), OffloadError> {
    match persisted_source {
        Some(source) if source == configured_source => Ok(()),
        Some(_) => Err(OffloadError::new(
            "upstream offload checkpoint journal source ID does not match configuration",
        )),
        None => Err(OffloadError::new(
            "upstream offload checkpoint journal is missing source ID",
        )),
    }
}

pub(super) fn source_retry_namespace(source_id: &str) -> String {
    format!("source:{source_id}")
}
