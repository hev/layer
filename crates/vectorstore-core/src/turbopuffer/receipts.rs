//! Payload-free, optional receipts at the physical HTTP read boundary.
//! These are evidence, never billing counters or query-budget reservations.
use super::TurbopufferError;
use serde::Serialize;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadKind {
    Query,
    Fetch,
    FetchMany,
    Siblings,
    Vector,
    Scan,
    Metadata,
}
#[derive(Clone, Debug, Serialize)]
pub struct ReadReceipt {
    pub sequence: u64,
    pub namespace: String,
    pub kind: ReadKind,
    pub query_units: u32,
    pub phase: &'static str,
    pub outcome: &'static str,
    pub status: Option<u16>,
    pub billing_present: bool,
    pub billing_valid: bool,
    pub queried_bytes: Option<u64>,
    pub returned_bytes: Option<u64>,
    pub written_bytes: Option<u64>,
    pub elapsed_ms: u64,
}
pub type ReadReceiptObserver = Arc<dyn Fn(ReadReceipt) + Send + Sync>;
tokio::task_local! { static OBSERVER: ReadReceiptObserver; }
static SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// The innermost evidence scope owns each physical event once and restores
/// its parent on exit/drop. This never changes the separate billing observer.
pub fn scope<F: std::future::Future>(
    observer: ReadReceiptObserver,
    work: F,
) -> impl std::future::Future<Output = F::Output> {
    OBSERVER.scope(observer, Box::pin(work))
}
pub(super) struct Pending {
    observer: Option<ReadReceiptObserver>,
    event: ReadReceipt,
    started: Instant,
}
impl Pending {
    pub(super) fn new(namespace: &str, kind: ReadKind) -> Self {
        Self::with_units(namespace, kind, 1)
    }
    pub(super) fn with_units(namespace: &str, kind: ReadKind, units: u32) -> Self {
        let observer = OBSERVER.try_with(Clone::clone).ok();
        let event = ReadReceipt {
            sequence: if observer.is_some() {
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            } else {
                0
            },
            namespace: namespace.into(),
            kind,
            query_units: units,
            phase: "send_attempt",
            outcome: "unknown",
            status: None,
            billing_present: false,
            billing_valid: false,
            queried_bytes: None,
            returned_bytes: None,
            written_bytes: None,
            elapsed_ms: 0,
        };
        if let Some(observer) = &observer {
            observer(event.clone());
        }
        Self {
            observer,
            event,
            started: Instant::now(),
        }
    }
    pub(super) async fn send(
        &mut self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, TurbopufferError> {
        match request.send().await {
            Ok(response) => {
                self.event.status = Some(response.status().as_u16());
                Ok(response)
            }
            Err(error) => {
                self.event.outcome = "transport_error";
                Err(TurbopufferError::Other(error.to_string()))
            }
        }
    }
    pub(super) fn finished_bytes(&mut self, status: u16, body: &[u8]) {
        self.event.status = Some(status);
        self.event.outcome = if (200..300).contains(&status) {
            "success"
        } else {
            "http_error"
        };
        if let Ok(body) = serde_json::from_slice::<Value>(body) {
            self.billing(&body);
        } else {
            self.event.outcome = "non_json_response";
        }
    }
    pub(super) fn finished_json_success(&mut self) {
        self.event.outcome = "success";
    }
    pub(super) fn billing(&mut self, body: &Value) {
        let Some(billing) = body.get("billing") else {
            return;
        };
        self.event.billing_present = true;
        self.event.billing_valid = billing.is_object();
        fn bytes(billing: &Value, name: &str, valid: &mut bool) -> Option<u64> {
            let a = billing.get(format!("billable_logical_bytes_{name}"));
            let b = billing.get(format!("billable_bytes_{name}"));
            let a_num = a.and_then(Value::as_u64);
            let b_num = b.and_then(Value::as_u64);
            if (a.is_some() && a_num.is_none())
                || (b.is_some() && b_num.is_none())
                || (a_num.is_some() && b_num.is_some() && a_num != b_num)
            {
                *valid = false;
                return None;
            }
            a_num.or(b_num)
        }
        self.event.queried_bytes = bytes(billing, "queried", &mut self.event.billing_valid);
        self.event.returned_bytes = bytes(billing, "returned", &mut self.event.billing_valid);
        self.event.written_bytes = bytes(billing, "written", &mut self.event.billing_valid);
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            self.event.phase = "outcome";
            self.event.elapsed_ms = self
                .started
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            observer(self.event.clone());
        }
    }
}
/// Called only after authorization and query admission, immediately before
/// polling reqwest. A send attempt is not proof the provider accepted work.
pub(super) async fn read_json(
    namespace: &str,
    kind: ReadKind,
    request: reqwest::RequestBuilder,
) -> Result<Value, TurbopufferError> {
    read_json_with_units(namespace, kind, request, 1).await
}
pub(super) async fn read_json_with_units(
    namespace: &str,
    kind: ReadKind,
    request: reqwest::RequestBuilder,
    units: u32,
) -> Result<Value, TurbopufferError> {
    let mut pending = Pending::with_units(namespace, kind, units);
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            pending.event.outcome = "transport_error";
            return Err(TurbopufferError::Other(error.to_string()));
        }
    };
    pending.event.status = Some(response.status().as_u16());
    if !response.status().is_success() {
        pending.event.outcome = "http_error";
        let error = TurbopufferError::from_response(response).await;
        if let TurbopufferError::Response(response) = &error {
            if let Ok(body) = serde_json::from_slice::<Value>(&response.body) {
                pending.billing(&body);
            }
        }
        return Err(error);
    }
    let body = match response.json::<Value>().await {
        Ok(body) => body,
        Err(error) => {
            pending.event.outcome = "decode_error";
            return Err(TurbopufferError::Other(error.to_string()));
        }
    };
    pending.billing(&body);
    pending.event.outcome = "success";
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    #[tokio::test]
    async fn receipt_scope_does_not_embed_large_request_future() {
        let payload = [9_u8; 128 * 1024];
        let work = async move {
            tokio::task::yield_now().await;
            assert_eq!(std::hint::black_box(payload)[0], 9);
        };
        assert!(std::mem::size_of_val(&work) >= 128 * 1024);
        let scoped = scope(Arc::new(|_| {}), work);
        assert!(std::mem::size_of_val(&scoped) < 4096);
        scoped.await;
    }
    #[tokio::test]
    async fn nested_receipts_restore_and_cancellation_is_unknown() {
        let outer = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(Mutex::new(Vec::new()));
        let observer = |events: Arc<Mutex<Vec<ReadReceipt>>>| -> ReadReceiptObserver {
            Arc::new(move |e| events.lock().unwrap().push(e))
        };
        scope(observer(outer.clone()), async {
            {
                let work = scope(observer(inner.clone()), async {
                    let _pending = Pending::new("demo", ReadKind::Fetch);
                    std::future::pending::<()>().await;
                });
                tokio::pin!(work);
                tokio::select! { _ = &mut work => unreachable!(), _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {} }
            }
            let events = inner.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].phase, "send_attempt");
            assert_eq!(events[1].phase, "outcome");
            assert_eq!(events[1].outcome, "unknown");
            assert_eq!(events[0].sequence, events[1].sequence);
            assert!(outer.lock().unwrap().is_empty());
            drop(events);
            let _pending = Pending::new("demo", ReadKind::Metadata);
        }).await;
        assert_eq!(outer.lock().unwrap().len(), 2);
        let _outside = Pending::new("demo", ReadKind::Fetch);
        assert_eq!(outer.lock().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn actual_http_receipts_include_errors_and_keep_payloads_out() {
        use axum::{routing::get, Json, Router};
        use std::sync::atomic::AtomicU64;
        let calls = Arc::new(AtomicU64::new(0));
        let count = calls.clone();
        let app = Router::new().route("/ok", get(move || {
            let count = count.clone(); async move { count.fetch_add(1, Ordering::Relaxed);
                Json(serde_json::json!({"rows":[{"text":"private-payload"}],"billing":{"billable_bytes_queried":123,"billable_bytes_returned":7}})) }
        })).route("/missing", get(|| async { Json(serde_json::json!({"schema":{}})) }))
        .route("/error", get(|| async { (axum::http::StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({"billing":{"billable_bytes_queried":12},"error":"private-error"}))) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        let copy = events.clone();
        let observer: ReadReceiptObserver = Arc::new(move |e| copy.lock().unwrap().push(e));
        scope(observer, async {
            let http = reqwest::Client::new();
            let body = read_json_with_units(
                "demo",
                ReadKind::Fetch,
                http.get(format!("{endpoint}/ok")),
                2,
            )
            .await
            .unwrap();
            assert_eq!(body["rows"][0]["text"], "private-payload");
            let missing = read_json(
                "demo",
                ReadKind::Metadata,
                http.get(format!("{endpoint}/missing")),
            )
            .await
            .unwrap();
            assert!(missing.get("billing").is_none());
            assert!(read_json(
                "demo",
                ReadKind::Fetch,
                http.get(format!("{endpoint}/error"))
            )
            .await
            .unwrap_err()
            .is_rate_limited());
        })
        .await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let events = events.lock().unwrap().clone();
        assert_eq!(events.len(), 6);
        assert_eq!(events[0].query_units, 2);
        assert_eq!(events[1].query_units, 2);
        assert_eq!(events[1].outcome, "success");
        assert_eq!(events[1].queried_bytes, Some(123));
        assert_eq!(events[3].queried_bytes, None);
        assert!(!events[3].billing_present);
        assert_eq!(events[5].status, Some(429));
        assert_eq!(events[5].queried_bytes, Some(12));
        let serialized = serde_json::to_string(&events).unwrap();
        assert!(!serialized.contains("private-payload"));
        assert!(!serialized.contains("private-error"));
        server.abort();
        let _ = server.await;
    }
    #[test]
    fn missing_malformed_and_conflicting_billing_never_becomes_zero() {
        let mut p = Pending::new("demo", ReadKind::Metadata);
        p.billing(&serde_json::json!({"schema":{"private":"never copied"}}));
        assert!(!p.event.billing_present);
        assert_eq!(p.event.queried_bytes, None);
        p.billing(&serde_json::json!({"billing":{"billable_bytes_queried":123,"billable_logical_bytes_queried":124}}));
        assert!(!p.event.billing_valid);
        assert_eq!(p.event.queried_bytes, None);
        let encoded = serde_json::to_string(&p.event).unwrap();
        assert!(!encoded.contains("private"));
    }
}
