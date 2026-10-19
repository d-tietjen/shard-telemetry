//! Status shares authenticated tenant, query admission, physical ownership and response controls.

use super::*;
use crate::{decode_native_provider_status_request, encode_native_provider_status_response};

pub(super) async fn dispatch_provider_status(
    header: NativeFrameHeader,
    payload: Vec<u8>,
    store: Arc<DurableTelemetryStore>,
    config: &NativeServerConfig,
) -> NativeFrame {
    let Some(runtime) = config.production.as_ref() else {
        return error_frame(
            header,
            NativeStatus::Unavailable,
            "native status requires production authentication and query controls",
        );
    };
    let request = match decode_native_provider_status_request(&payload) {
        Ok(request) => request,
        Err(error) => return error_frame(header, NativeStatus::BadRequest, &error.to_string()),
    };
    if request.tenant != runtime.tenant() {
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "native status tenant does not match the authenticated tenant",
        );
    }
    let query_permit = match runtime.try_query() {
        Some(permit) => {
            runtime.record_query();
            permit
        }
        None if matches!(
            runtime.lifecycle().state(),
            ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
        ) =>
        {
            return error_frame(
                header,
                NativeStatus::Unavailable,
                "native status service is unavailable",
            );
        }
        None => {
            return error_frame(
                header,
                NativeStatus::TooManyRequests,
                "native query concurrency limit exceeded",
            );
        }
    };
    let query_timeout = runtime.query_timeout();
    let worker = tokio::task::spawn_blocking(move || {
        // The permit remains owned by physical checkpoint reads after an async timeout.
        let result = store
            .local_provider_status_with_budget(&request, query_timeout)
            .and_then(|response| {
                encode_native_provider_status_response(&response)
                    .map_err(|error| LokiApiError::internal(error.to_string()))
            });
        drop(query_permit);
        result
    });
    let result = match tokio::time::timeout(query_timeout, worker).await {
        Ok(result) => result,
        Err(_) => {
            return error_frame(
                header,
                NativeStatus::Timeout,
                "native status deadline exceeded",
            );
        }
    };
    match result {
        Ok(Ok(encoded)) => query_ok_frame(header, encoded, config),
        Ok(Err(error)) => store_error_frame(header, error),
        Err(error) => error_frame(
            header,
            NativeStatus::Internal,
            &format!("native status worker failed: {error}"),
        ),
    }
}
