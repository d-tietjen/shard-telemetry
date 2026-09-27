//! Prometheus API ownership: service configuration, query endpoints, Remote Write/Read, and responses.
//! `prometheus_api/` contains endpoint behavior; this root keeps the route surface.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::prometheus_protocol::v1 as prometheus_v1;
use crate::prometheus_xor::encode_xor_chunk;
use crate::{
    DurableTelemetryStore, ExponentialHistogramBuckets, HistogramCount, MetricKind, MetricValue,
    NumberValue, ProductionRuntime, PromqlEngine, PromqlLimits, PromqlValue, RemoteWriteDecoder,
    RemoteWriteStats, RemoteWriteVersion, ServiceState, TelemetryError, TelemetryResult,
    TelemetryRouter, TelemetryValue,
};

mod query;
use query::*;
mod remote_write;
use remote_write::*;
mod remote_read;
use remote_read::*;
mod response;
use response::*;
#[cfg(test)]
mod tests;

/// Single-tenant Prometheus compatibility limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusApiConfig {
    /// Authenticated tenant assigned to every series.
    pub tenant: Arc<str>,
    /// Maximum compressed and Snappy-decompressed request bytes.
    pub max_request_bytes: usize,
    /// Stable logical metric partitions.
    pub logical_partitions: NonZeroU16,
}

impl Default for PrometheusApiConfig {
    fn default() -> Self {
        Self {
            tenant: Arc::from("default"),
            max_request_bytes: 64 * 1024 * 1024,
            logical_partitions: NonZeroU16::new(256).expect("constant is nonzero"),
        }
    }
}

impl PrometheusApiConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.tenant.is_empty() {
            return Err(TelemetryError::InvalidConfiguration(
                "Prometheus tenant must not be empty".into(),
            ));
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > 64 * 1024 * 1024 {
            return Err(TelemetryError::InvalidConfiguration(
                "Prometheus request limit must be in 1..=64 MiB".into(),
            ));
        }
        Ok(())
    }
}

/// Shared Prometheus protocol service backed by signal-native metric stripes.
#[derive(Clone)]
pub struct PrometheusService {
    store: Arc<DurableTelemetryStore>,
    config: PrometheusApiConfig,
    router: TelemetryRouter,
    production: Option<Arc<ProductionRuntime>>,
}

impl std::fmt::Debug for PrometheusService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrometheusService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PrometheusService {
    /// Creates a Prometheus API service.
    pub fn new(
        store: Arc<DurableTelemetryStore>,
        config: PrometheusApiConfig,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        Ok(Self {
            store,
            router: TelemetryRouter::new(config.logical_partitions),
            config,
            production: None,
        })
    }

    /// Attaches shared fail-closed production admission.
    #[must_use]
    pub fn with_production(mut self, production: Option<Arc<ProductionRuntime>>) -> Self {
        self.production = production;
        self
    }

    #[allow(clippy::result_large_err)]
    fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Response> {
        if headers
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|tenant| tenant != self.config.tenant.as_ref())
        {
            return Err(write_error(
                StatusCode::FORBIDDEN,
                "tenant header does not match the configured tenant",
            ));
        }
        let Some(runtime) = &self.production else {
            return Ok(None);
        };
        let credential = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !credential.is_some_and(|value| runtime.authenticates(value)) {
            if credential.is_none() {
                runtime.record_authentication_failure();
            }
            return Err(write_error(
                StatusCode::UNAUTHORIZED,
                "valid production bearer token is required",
            ));
        }
        runtime.try_http().map(Some).ok_or_else(|| {
            write_error(
                StatusCode::TOO_MANY_REQUESTS,
                "HTTP concurrency limit exceeded",
            )
        })
    }
}

/// Builds Prometheus Remote Write and query compatibility routes.
pub fn prometheus_router(service: PrometheusService) -> Router {
    let max_request_bytes = service.config.max_request_bytes;
    Router::new()
        .route("/api/v1/write", post(remote_write))
        .route("/api/v1/read", post(remote_read))
        .route("/api/v1/query", get(query_get).post(query_post))
        .route(
            "/api/v1/query_range",
            get(query_range_get).post(query_range_post),
        )
        .route("/api/v1/series", get(series_get).post(series_post))
        .route("/api/v1/labels", get(labels_get).post(labels_post))
        .route(
            "/api/v1/label/{name}/values",
            get(label_values_get).post(label_values_post),
        )
        .route("/api/v1/metadata", get(metadata_get))
        .route(
            "/api/v1/query_exemplars",
            get(exemplars_get).post(exemplars_post),
        )
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(service)
}

#[derive(Debug, Clone, Deserialize)]
struct InstantQueryParameters {
    query: String,
    time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RangeQueryParameters {
    query: String,
    start: String,
    end: String,
    step: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct DiscoveryParameters {
    #[serde(default, rename = "match[]")]
    selectors: Vec<String>,
    start: Option<String>,
    end: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MetadataParameters {
    metric: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExemplarParameters {
    query: String,
    start: String,
    end: String,
}
