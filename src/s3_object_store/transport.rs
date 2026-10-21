//! Explicit opt-in transport configuration; existing constructors keep their policy.

use std::time::{Duration, Instant};

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientConfigKey, RetryConfig};
use tokio::sync::mpsc::unbounded_channel;

use super::*;

impl Default for S3ObjectStoreTransportLimits {
    fn default() -> Self {
        Self {
            request_timeout_ms: 30_000,
            connect_timeout_ms: 5_000,
            retry_timeout_ms: 180_000,
            max_retries: 10,
        }
    }
}

pub(super) struct ValidatedTransportLimits(S3ObjectStoreTransportLimits);

impl ValidatedTransportLimits {
    pub(super) fn new(limits: S3ObjectStoreTransportLimits) -> TelemetryResult<Self> {
        let now = Instant::now();
        if limits.request_timeout_ms == 0
            || limits.connect_timeout_ms == 0
            || limits.retry_timeout_ms == 0
            || limits.connect_timeout_ms > limits.request_timeout_ms
            || limits.max_retries.checked_add(1).is_none()
            || [
                limits.request_timeout_ms,
                limits.connect_timeout_ms,
                limits.retry_timeout_ms,
            ]
            .iter()
            .any(|&ms| now.checked_add(Duration::from_millis(ms)).is_none())
        {
            return Err(TelemetryError::InvalidConfiguration(
                "S3 transport needs positive representable timeouts, connect <= request, and a finite attempt count".into(),
            ));
        }
        Ok(Self(limits))
    }

    pub(super) fn apply(self, builder: AmazonS3Builder) -> AmazonS3Builder {
        // Preserve every other from_env ClientOptions field. In object_store
        // 0.14.1, from_env has no retry/backoff key and retains RetryConfig::default.
        let limits = self.0;
        builder
            .with_config(
                AmazonS3ConfigKey::Client(ClientConfigKey::Timeout),
                format!("{}ms", limits.request_timeout_ms),
            )
            .with_config(
                AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout),
                format!("{}ms", limits.connect_timeout_ms),
            )
            .with_retry(RetryConfig {
                max_retries: limits.max_retries,
                retry_timeout: Duration::from_millis(limits.retry_timeout_ms),
                ..RetryConfig::default()
            })
    }
}

pub(super) fn apply_transport_limits(
    builder: AmazonS3Builder,
    limits: Option<ValidatedTransportLimits>,
) -> AmazonS3Builder {
    match limits {
        Some(limits) => limits.apply(builder),
        None => builder,
    }
}

impl S3ObjectStore {
    pub(super) fn open_configured(
        config: S3ObjectStoreConfig,
        configured_threads: Option<usize>,
        limits: Option<S3ObjectStoreTransportLimits>,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let worker_threads = configured_threads.unwrap_or(DEFAULT_RUNTIME_WORKER_THREADS);
        if !(1..=MAX_RUNTIME_WORKER_THREADS).contains(&worker_threads) {
            return Err(TelemetryError::InvalidConfiguration(
                "S3 runtime worker threads must be in 1..=64".into(),
            ));
        }
        let limits = limits.map(ValidatedTransportLimits::new).transpose()?;
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
        let builder = apply_transport_limits(builder, limits);
        Self::start_with_builder(config, worker_threads, builder)
    }

    // An ordinary private composition boundary also lets tests exercise the real
    // provider against an owned local HTTP endpoint without changing process env.
    pub(super) fn start_with_builder(
        config: S3ObjectStoreConfig,
        worker_threads: usize,
        builder: AmazonS3Builder,
    ) -> TelemetryResult<Self> {
        let store = builder
            .build()
            .map_err(|error| cloud_error("configure", &config.bucket, error))?;
        let store: Arc<DynObjectStore> = Arc::new(store);
        let (sender, mut receiver) = unbounded_channel::<CloudTask>();
        std::thread::Builder::new()
            .name("shard-telemetry-s3".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads)
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
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
