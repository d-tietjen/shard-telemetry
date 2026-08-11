use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::serve::ListenerExt;
use clap::Parser;
use shard_telemetry::{
    DurableTelemetryConfig, DurableTelemetryStore, LokiApiConfig, NativeServerConfig,
    ObjectTierConfig, OtlpIngestService, OtlpReceiverConfig, ProductionRuntime,
    PrometheusApiConfig, PrometheusService, S3ObjectStoreConfig, ServiceLifecycle,
    ShardTelemetryConfig, SignalConfig, SingleTenantConfig, StripeConfig, TempoApiConfig,
    TempoService, loki_router, loki_router_with_clickhouse, otlp_http_router, prometheus_router,
    serve_native, serve_otlp_grpc, single_tenant_loki_router, tempo_router,
};

#[derive(Debug, Parser)]
#[command(
    name = "shard-telemetry-server",
    about = "Standalone signal-native ShardTelemetry server"
)]
struct Arguments {
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:3100")]
    listen: SocketAddr,
    /// High-throughput native binary protocol listen address.
    #[arg(long, default_value = "127.0.0.1:3101")]
    native_listen: SocketAddr,
    /// OTLP/gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:4317")]
    otlp_grpc_listen: SocketAddr,
    /// OTLP/HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:4318")]
    otlp_http_listen: SocketAddr,
    /// Tenant used when X-Scope-OrgID is absent.
    #[arg(long, default_value = "fake")]
    default_tenant: String,
    /// File containing the bearer token required by Loki, OTLP, and native TCP.
    #[arg(long)]
    auth_token_file: Option<PathBuf>,
    /// Explicitly starts without production authentication and admission controls.
    #[arg(long, default_value_t = false)]
    insecure_development_mode: bool,
    /// Maximum number of entries returned by one query.
    #[arg(long, default_value_t = 5_000)]
    max_query_limit: usize,
    /// Durable local data directory.
    #[arg(long, default_value = "./shard-telemetry-data")]
    data_directory: PathBuf,
    /// Local object-store backend; mutually exclusive with S3.
    #[arg(long)]
    object_store_directory: Option<PathBuf>,
    /// S3 or S3-compatible bucket for the durable compressed tier.
    #[arg(long)]
    object_store_s3_bucket: Option<String>,
    /// Key prefix dedicated to this deployment inside the S3 bucket.
    #[arg(long, default_value = "shard-telemetry")]
    object_store_s3_prefix: String,
    /// Optional AWS region override; otherwise the standard AWS chain is used.
    #[arg(long)]
    object_store_s3_region: Option<String>,
    /// Optional S3-compatible endpoint.
    #[arg(long)]
    object_store_s3_endpoint: Option<String>,
    /// Explicitly permits a plaintext S3-compatible endpoint in development.
    #[arg(long, default_value_t = false)]
    object_store_s3_allow_http: bool,
    /// Uses virtual-hosted-style S3 requests.
    #[arg(long, default_value_t = false)]
    object_store_s3_virtual_hosted_style: bool,
    /// Seconds between ownership reclamation and compressed-tier checkpoints.
    #[arg(long, default_value_t = 60)]
    object_store_maintenance_interval_seconds: u64,
    /// Grace after root replacement before cross-process readers' exact keys are reclaimed.
    #[arg(long, default_value_t = 600)]
    object_store_reader_grace_seconds: u64,
    /// Maximum publication duration before a replacement owner may replay PENDING cleanup.
    #[arg(long, default_value_t = 1_800)]
    object_store_writer_lease_seconds: u64,
    /// Retain a duplicate raw-payload journal to accelerate hot-index recovery.
    #[arg(long, default_value_t = false)]
    recovery_journal: bool,
    /// Retain logs, traces, and metrics for this many seconds; zero is indefinite.
    #[arg(long, default_value_t = 0)]
    retention_seconds: u64,
    /// Seconds between batch-aligned physical retention passes.
    #[arg(long, default_value_t = 300)]
    retention_compaction_interval_seconds: u64,
    /// Number of physical owner stripes.
    #[arg(long, default_value_t = 16)]
    shards: u32,
    /// Stable tenant partitions spread over physical stripes.
    #[arg(long, default_value_t = 256)]
    tenant_partitions: u32,
    /// Microseconds shard-stream may collect adjacent appends before one sync.
    #[arg(long, default_value_t = 250)]
    append_linger_micros: u64,
    /// Maximum seconds a durable append waits for indexed read visibility.
    #[arg(long, default_value_t = 300)]
    indexed_ack_timeout_seconds: u64,
    /// Acknowledge native appends after the compressed WAL commit instead of
    /// waiting for the stripe index to become query-visible.
    #[arg(long, default_value_t = false)]
    native_durable_ack: bool,
    /// File containing the bearer token that enables the ClickHouse scan route.
    /// When omitted, the route is not registered.
    #[arg(long)]
    clickhouse_token_file: Option<PathBuf>,
    /// Maximum concurrent HTTP requests in single-tenant production mode.
    #[arg(long, default_value_t = 1_024)]
    max_http_in_flight: usize,
    /// Maximum concurrent ingest requests across HTTP and native TCP.
    #[arg(long, default_value_t = 256)]
    max_ingest_in_flight: usize,
    /// Maximum concurrent query requests across HTTP and native TCP.
    #[arg(long, default_value_t = 256)]
    max_query_in_flight: usize,
    /// Sustained ingest byte rate; zero disables byte-rate limiting.
    #[arg(long, default_value_t = 0)]
    ingest_bytes_per_second: u64,
    /// Ingest byte burst accepted by the token bucket.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    ingest_burst_bytes: u64,
    /// Maximum concurrent Loki tail subscribers.
    #[arg(long, default_value_t = 128)]
    max_tail_subscribers: usize,
    /// Maximum simultaneously connected native TCP clients.
    #[arg(long, default_value_t = 4_096)]
    max_native_connections: usize,
    /// Maximum seconds allowed for one Loki, Arrow, or native query.
    #[arg(long, default_value_t = 30)]
    query_timeout_seconds: u64,
    /// Maximum seconds a native connection may wait before authentication.
    #[arg(long, default_value_t = 5)]
    native_auth_timeout_seconds: u64,
    /// Maximum seconds allowed for an administrative flush.
    #[arg(long, default_value_t = 300)]
    flush_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    if arguments.max_query_limit == 0 {
        return Err("--max-query-limit must be nonzero".into());
    }
    if arguments.flush_timeout_seconds == 0 {
        return Err("--flush-timeout-seconds must be nonzero".into());
    }
    if arguments.query_timeout_seconds == 0 {
        return Err("--query-timeout-seconds must be nonzero".into());
    }
    if arguments.native_auth_timeout_seconds == 0 {
        return Err("--native-auth-timeout-seconds must be nonzero".into());
    }
    if arguments.retention_seconds > 0 && arguments.retention_compaction_interval_seconds == 0 {
        return Err(
            "--retention-compaction-interval-seconds must be nonzero when retention is enabled"
                .into(),
        );
    }
    if arguments.object_store_maintenance_interval_seconds == 0 {
        return Err("--object-store-maintenance-interval-seconds must be nonzero".into());
    }
    if arguments.object_store_writer_lease_seconds == 0 {
        return Err("--object-store-writer-lease-seconds must be nonzero".into());
    }
    if arguments.insecure_development_mode && arguments.auth_token_file.is_some() {
        return Err("--insecure-development-mode conflicts with --auth-token-file".into());
    }
    let lifecycle = Arc::new(ServiceLifecycle::new());
    let production = match arguments.auth_token_file.as_ref() {
        Some(path) => {
            let token = read_secret(path, "production authentication")?;
            Some(Arc::new(ProductionRuntime::new(
                SingleTenantConfig {
                    tenant: Arc::from(arguments.default_tenant.as_str()),
                    bearer_token: token,
                    max_http_in_flight: arguments.max_http_in_flight,
                    max_ingest_in_flight: arguments.max_ingest_in_flight,
                    max_query_in_flight: arguments.max_query_in_flight,
                    ingest_bytes_per_second: arguments.ingest_bytes_per_second,
                    ingest_burst_bytes: arguments.ingest_burst_bytes,
                    max_tail_subscribers: arguments.max_tail_subscribers,
                    max_native_connections: arguments.max_native_connections,
                    query_timeout: std::time::Duration::from_secs(arguments.query_timeout_seconds),
                    native_auth_timeout: std::time::Duration::from_secs(
                        arguments.native_auth_timeout_seconds,
                    ),
                },
                Arc::clone(&lifecycle),
            )?))
        }
        None if arguments.insecure_development_mode => None,
        None => {
            return Err(
                "--auth-token-file is required unless --insecure-development-mode is explicit"
                    .into(),
            );
        }
    };
    if arguments.object_store_directory.is_some() && arguments.object_store_s3_bucket.is_some() {
        return Err("--object-store-directory conflicts with --object-store-s3-bucket".into());
    }
    if arguments.object_store_s3_bucket.is_none()
        && (arguments.object_store_s3_region.is_some()
            || arguments.object_store_s3_endpoint.is_some()
            || arguments.object_store_s3_allow_http
            || arguments.object_store_s3_virtual_hosted_style)
    {
        return Err("S3 object-store options require --object-store-s3-bucket".into());
    }
    if production.is_some() && arguments.object_store_s3_allow_http {
        return Err("plaintext S3 endpoints are forbidden in production mode".into());
    }
    if production.is_some()
        && arguments.object_store_directory.is_none()
        && arguments.object_store_s3_bucket.is_none()
    {
        return Err(
            "a local or S3 object store is required in production mode for durable compressed checkpoints"
                .into(),
        );
    }
    let object_tier_configured =
        arguments.object_store_directory.is_some() || arguments.object_store_s3_bucket.is_some();
    if object_tier_configured
        && arguments.object_store_reader_grace_seconds <= arguments.query_timeout_seconds
    {
        return Err(
            "--object-store-reader-grace-seconds must exceed --query-timeout-seconds".into(),
        );
    }
    if object_tier_configured
        && arguments.object_store_writer_lease_seconds <= arguments.flush_timeout_seconds
    {
        return Err(
            "--object-store-writer-lease-seconds must exceed --flush-timeout-seconds".into(),
        );
    }
    let object_tier_config = ObjectTierConfig {
        retirement_grace: std::time::Duration::from_secs(
            arguments.object_store_reader_grace_seconds,
        ),
        transaction_lease: std::time::Duration::from_secs(
            arguments.object_store_writer_lease_seconds,
        ),
        ..ObjectTierConfig::default()
    };
    let s3_object_store = arguments
        .object_store_s3_bucket
        .map(|bucket| S3ObjectStoreConfig {
            bucket,
            prefix: arguments.object_store_s3_prefix,
            region: arguments.object_store_s3_region,
            endpoint: arguments.object_store_s3_endpoint,
            allow_http: arguments.object_store_s3_allow_http,
            virtual_hosted_style: arguments.object_store_s3_virtual_hosted_style,
        });
    let listener = tokio::net::TcpListener::bind(arguments.listen).await?;
    let native_listener = tokio::net::TcpListener::bind(arguments.native_listen).await?;
    let otlp_http_listener = tokio::net::TcpListener::bind(arguments.otlp_http_listen).await?;
    let store = Arc::new(DurableTelemetryStore::open_with_object_tier_config(
        DurableTelemetryConfig {
            data_directory: arguments.data_directory,
            object_store_directory: arguments.object_store_directory,
            s3_object_store,
            recovery_journal: arguments.recovery_journal,
            retention: (arguments.retention_seconds > 0)
                .then(|| std::time::Duration::from_secs(arguments.retention_seconds)),
            shard_count: arguments.shards,
            tenant_partitions: arguments.tenant_partitions,
            append_linger: std::time::Duration::from_micros(arguments.append_linger_micros),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: std::time::Duration::from_secs(
                arguments.indexed_ack_timeout_seconds,
            ),
        },
        object_tier_config,
    )?);
    let api_config = LokiApiConfig {
        default_tenant: Arc::from(arguments.default_tenant.as_str()),
        max_query_limit: arguments.max_query_limit,
    };
    let logical_partitions = u16::try_from(arguments.tenant_partitions)
        .ok()
        .and_then(std::num::NonZeroU16::new)
        .ok_or("--tenant-partitions must be in 1..=65535")?;
    let otlp_service = OtlpIngestService::new(
        Arc::clone(&store),
        OtlpReceiverConfig {
            tenant: Arc::from(arguments.default_tenant.as_str()),
            signals: ShardTelemetryConfig {
                logs: SignalConfig {
                    logical_partitions,
                    ..ShardTelemetryConfig::default().logs
                },
                traces: SignalConfig {
                    logical_partitions,
                    ..ShardTelemetryConfig::default().traces
                },
                metrics: SignalConfig {
                    logical_partitions,
                    ..ShardTelemetryConfig::default().metrics
                },
                ..ShardTelemetryConfig::default()
            },
            wait_for_index: true,
            ..OtlpReceiverConfig::default()
        },
    )?
    .with_production(production.clone());
    let otlp_app = otlp_http_router(otlp_service.clone());
    let prometheus_service = PrometheusService::new(
        Arc::clone(&store),
        PrometheusApiConfig {
            tenant: Arc::from(arguments.default_tenant.as_str()),
            logical_partitions,
            ..PrometheusApiConfig::default()
        },
    )?
    .with_production(production.clone());
    let tempo_service = TempoService::new(
        Arc::clone(&store),
        TempoApiConfig {
            tenant: Arc::from(arguments.default_tenant.as_str()),
            ..TempoApiConfig::default()
        },
    )?
    .with_production(production.clone());
    let clickhouse_token = arguments
        .clickhouse_token_file
        .as_ref()
        .map(|path| read_secret(path, "ClickHouse"))
        .transpose()?;
    let flush_timeout = std::time::Duration::from_secs(arguments.flush_timeout_seconds);
    let app = match production.as_ref() {
        Some(runtime) => single_tenant_loki_router(
            store.clone(),
            api_config,
            Arc::clone(runtime),
            clickhouse_token,
            flush_timeout,
        )?,
        None => match clickhouse_token {
            Some(token) => loki_router_with_clickhouse(store.clone(), api_config, token)?,
            None => loki_router(store.clone(), api_config),
        },
    }
    .merge(prometheus_router(prometheus_service))
    .merge(tempo_router(tempo_service));
    lifecycle.mark_ready();
    let (shutdown, _) = tokio::sync::broadcast::channel::<()>(1);
    let signal = shutdown.clone();
    let mut administrative_shutdown = lifecycle.subscribe_shutdown();
    let shutdown_lifecycle = Arc::clone(&lifecycle);
    let shutdown_store = Arc::clone(&store);
    tokio::spawn(async move {
        let administrative = tokio::select! {
            () = shutdown_signal() => false,
            result = administrative_shutdown.changed() => {
                if result.is_ok() && !*administrative_shutdown.borrow() {
                    return;
                }
                true
            }
        };
        if !administrative {
            shutdown_lifecycle.begin_draining();
            let result = tokio::task::spawn_blocking(move || {
                shard_telemetry::LokiStore::flush(shutdown_store.as_ref(), flush_timeout)
            })
            .await;
            match result {
                Ok(Ok(())) => shutdown_lifecycle.request_shutdown(),
                Ok(Err(error)) => {
                    shutdown_lifecycle.mark_failed(format!("shutdown flush failed: {error}"));
                    shutdown_lifecycle.request_shutdown();
                }
                Err(error) => {
                    shutdown_lifecycle.mark_failed(format!("shutdown flush task failed: {error}"));
                    shutdown_lifecycle.request_shutdown();
                }
            }
        }
        let _ = signal.send(());
    });
    let mut http_shutdown = shutdown.subscribe();
    let mut native_shutdown = shutdown.subscribe();
    let mut otlp_http_shutdown = shutdown.subscribe();
    let mut otlp_grpc_shutdown = shutdown.subscribe();
    if object_tier_configured {
        let maintenance_store = Arc::clone(&store);
        let maintenance_lifecycle = Arc::clone(&lifecycle);
        let mut maintenance_shutdown = shutdown.subscribe();
        let interval =
            std::time::Duration::from_secs(arguments.object_store_maintenance_interval_seconds);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = maintenance_shutdown.recv() => break,
                    _ = ticker.tick() => {
                        let store = Arc::clone(&maintenance_store);
                        let result = tokio::task::spawn_blocking(move || {
                            shard_telemetry::LokiStore::flush(store.as_ref(), flush_timeout)
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                maintenance_lifecycle.mark_failed(format!(
                                    "object-tier maintenance failed: {error}"
                                ));
                                break;
                            }
                            Err(error) => {
                                maintenance_lifecycle.mark_failed(format!(
                                    "object-tier maintenance task failed: {error}"
                                ));
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
    if arguments.retention_seconds > 0 {
        let retention_store = Arc::clone(&store);
        let retention_lifecycle = Arc::clone(&lifecycle);
        let mut retention_shutdown = shutdown.subscribe();
        let interval =
            std::time::Duration::from_secs(arguments.retention_compaction_interval_seconds);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = retention_shutdown.recv() => break,
                    _ = ticker.tick() => {
                        let store = Arc::clone(&retention_store);
                        match tokio::task::spawn_blocking(move || store.compact_retention()).await {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                retention_lifecycle.mark_failed(format!(
                                    "retention compaction failed: {error}"
                                ));
                                break;
                            }
                            Err(error) => {
                                retention_lifecycle.mark_failed(format!(
                                    "retention compaction task failed: {error}"
                                ));
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
    let listener = listener.tap_io(|stream| {
        let _ = stream.set_nodelay(true);
    });
    let http = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = http_shutdown.recv().await;
    });
    let native = serve_native(
        native_listener,
        Arc::clone(&store),
        NativeServerConfig {
            wait_for_index: !arguments.native_durable_ack,
            production,
            ..NativeServerConfig::default()
        },
        async move {
            let _ = native_shutdown.recv().await;
        },
    );
    let otlp_http_listener = otlp_http_listener.tap_io(|stream| {
        let _ = stream.set_nodelay(true);
    });
    let otlp_http = axum::serve(otlp_http_listener, otlp_app).with_graceful_shutdown(async move {
        let _ = otlp_http_shutdown.recv().await;
    });
    let otlp_grpc = async move {
        serve_otlp_grpc(arguments.otlp_grpc_listen, otlp_service, async move {
            let _ = otlp_grpc_shutdown.recv().await;
        })
        .await
        .map_err(std::io::Error::other)
    };
    tokio::try_join!(http, native, otlp_http, otlp_grpc)?;
    Ok(())
}

fn read_secret(
    path: &std::path::Path,
    purpose: &str,
) -> Result<Arc<str>, Box<dyn std::error::Error>> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(format!("{purpose} token path {} is not a file", path.display()).into());
    }
    if metadata.len() > 4_096 {
        return Err(format!("{purpose} token file {} exceeds 4096 bytes", path.display()).into());
    }
    let token = std::fs::read_to_string(path)?;
    let token = token.trim();
    if token.is_empty() {
        return Err(format!("{purpose} token file {} is empty", path.display()).into());
    }
    Ok(Arc::from(token))
}

async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
