use crate::{
    protocol::{Error, Request, MAX_BODY, MAX_TIMEOUT},
    registry::Registry,
};
use axum::{
    body::to_bytes,
    extract::State,
    http::Method,
    response::{IntoResponse, Response},
    Json, Router,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

pub struct Service {
    pub registry: RwLock<Option<Arc<Registry>>>,
    pub shutting_down: AtomicBool,
    pub invalid_artifacts: AtomicBool,
    pub active: Arc<Semaphore>,
    pub admitted: Arc<Semaphore>,
}
impl Service {
    pub fn new() -> Arc<Self> {
        let active = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(2);
        Arc::new(Self {
            registry: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            invalid_artifacts: AtomicBool::new(false),
            active: Arc::new(Semaphore::new(active)),
            admitted: Arc::new(Semaphore::new(active + 8)),
        })
    }
    pub fn reason(&self) -> Option<&'static str> {
        if self.shutting_down.load(Ordering::Acquire) {
            Some("shutting_down")
        } else if self.invalid_artifacts.load(Ordering::Acquire) {
            Some("invalid_artifacts")
        } else if self.registry.read().unwrap().is_none() {
            Some("loading_models")
        } else {
            None
        }
    }
}
struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
pub fn router(service: Arc<Service>) -> Router {
    Router::new().fallback(dispatch).with_state(service)
}
async fn dispatch(State(service): State<Arc<Service>>, req: axum::extract::Request) -> Response {
    let disconnected = req
        .extensions()
        .get::<axum::extract::ConnectInfo<crate::connection::Disconnect>>()
        .cloned();
    let result = if let Some(axum::extract::ConnectInfo(crate::connection::Disconnect(
        mut closed,
    ))) = disconnected
    {
        tokio::select! {
            result = handle(service, req) => result,
            _ = async { if !*closed.borrow() { let _ = closed.changed().await; } } => Err(Error::new("deadline_exceeded")),
        }
    } else {
        handle(service, req).await
    };
    match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
async fn handle(service: Arc<Service>, req: axum::extract::Request) -> Result<Response, Error> {
    let receipt = Instant::now();
    let path = req.uri().path();
    let expected = match path {
        "/health/live" | "/health/ready" | "/v1/models" => Method::GET,
        "/v1/embeddings" => Method::POST,
        _ => return Err(Error::new("not_found")),
    };
    if req.method() != expected {
        return Err(Error::new("method_not_allowed"));
    }
    if path == "/health/live" {
        return Ok(Json(serde_json::json!({"status":"live"})).into_response());
    }
    if path == "/health/ready" {
        if let Some(reason) = service.reason() {
            return Ok((
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    serde_json::json!({"status":"not_ready","protocol_version":1,"reason":reason}),
                ),
            )
                .into_response());
        }
        return Ok(Json(serde_json::json!({"status":"ready","protocol_version":1,"manifest_sha256":service.registry.read().unwrap().as_ref().unwrap().manifest_sha256})).into_response());
    }
    if service.reason().is_some() {
        return Err(Error::new("not_ready"));
    }
    let registry = service.registry.read().unwrap().as_ref().unwrap().clone();
    if path == "/v1/models" {
        return Ok(Json(registry.discovery()).into_response());
    }
    if req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/json")
        })
        != Some(true)
    {
        return Err(Error::new("unsupported_media_type"));
    }
    // Admission also bounds bodies being read; slow senders cannot consume unbounded memory.
    let admission = service
        .admitted
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::new("overloaded"))?;
    let bytes = tokio::time::timeout(
        Duration::from_millis(MAX_TIMEOUT),
        to_bytes(req.into_body(), MAX_BODY),
    )
    .await
    .map_err(|_| Error::new("deadline_exceeded"))?
    .map_err(|error| {
        use std::error::Error as _;
        if error
            .source()
            .is_some_and(|e| e.is::<http_body_util::LengthLimitError>())
        {
            Error::new("body_too_large")
        } else {
            Error::new("invalid_request")
        }
    })?;
    let request: Request =
        serde_json::from_slice(&bytes).map_err(|_| Error::new("invalid_request"))?;
    if request.timeout_ms == 0 || request.timeout_ms > MAX_TIMEOUT {
        return Err(Error::new("invalid_request"));
    }
    let deadline = receipt + Duration::from_millis(request.timeout_ms);
    let model = registry
        .models
        .get(&request.model)
        .ok_or_else(|| Error::new("model_not_found"))?
        .clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel = Cancel(cancelled.clone());
    let work = async {
        // Tokio's semaphore queues waiters FIFO; dropping an expired waiter removes it.
        let permit = service
            .active
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("not_ready"))?;
        if service.shutting_down.load(Ordering::Acquire) {
            return Err(Error::new("not_ready"));
        }
        tokio::task::spawn_blocking(move || {
            // Both permits remain with a running kernel even if its HTTP future is dropped.
            let (_permit, _admission) = (permit, admission);
            model.infer(&request, deadline, &cancelled)
        })
        .await
        .map_err(|_| Error::new("inference_failed"))?
    };
    let output = tokio::time::timeout_at(deadline.into(), work)
        .await
        .map_err(|_| Error::new("deadline_exceeded"))??;
    Ok(Json(output).into_response())
}
