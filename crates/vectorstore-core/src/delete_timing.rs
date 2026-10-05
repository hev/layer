//! Request-local namespace DELETE timings. No names or request data are recorded.
//! Parent phases include children; absent phases are unmeasured, not zero.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Function,
    FunctionList,
    FunctionLock,
    FunctionObserve,
    Receipts,
    Guard,
    Intent,
    IntentPut,
    IntentQueue,
    Upstream,
    UpstreamHeaders,
    UpstreamBody,
    Invalidate,
    Notify,
}
impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::FunctionList => "function_list",
            Self::FunctionLock => "function_lock",
            Self::FunctionObserve => "function_observe",
            Self::Receipts => "receipts",
            Self::Guard => "guard",
            Self::Intent => "intent",
            Self::IntentPut => "intent_put",
            Self::IntentQueue => "intent_queue",
            Self::Upstream => "upstream",
            Self::UpstreamHeaders => "upstream_headers",
            Self::UpstreamBody => "upstream_body",
            Self::Invalidate => "invalidate",
            Self::Notify => "notify",
        }
    }
}
#[derive(Clone, Default)]
pub struct Timings(Arc<Mutex<BTreeMap<Phase, Duration>>>);
tokio::task_local! { static CURRENT: Timings; }
impl Timings {
    pub async fn scope<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        CURRENT.scope(self.clone(), work).await
    }
    pub fn header(&self, total: Duration) -> String {
        let mut header = format!("server;dur={:.3}", total.as_secs_f64() * 1000.0);
        for (phase, duration) in self.0.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            header.push_str(&format!(
                ", {};dur={:.3}",
                phase.label(),
                duration.as_secs_f64() * 1000.0
            ));
        }
        header
    }
}
/// RAII also records early errors and cancelled work. Inactive outside DELETE.
#[must_use]
pub struct Timer(Option<(Timings, Phase, Instant)>);
pub fn start(phase: Phase) -> Timer {
    Timer(
        CURRENT
            .try_with(|t| (t.clone(), phase, Instant::now()))
            .ok(),
    )
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((timings, phase, started)) = self.0.take() {
            *timings
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(phase)
                .or_default() += started.elapsed();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn independent_delays_and_error_are_attributed_without_leaking_context() {
        let timings = Timings::default();
        let started = Instant::now();
        let result: Result<(), ()> = timings
            .scope(async {
                {
                    let _t = start(Phase::Function);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                {
                    let _t = start(Phase::IntentPut);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
                let _t = start(Phase::Upstream);
                tokio::time::sleep(Duration::from_millis(60)).await;
                Err(())
            })
            .await;
        assert!(result.is_err());
        let total = started.elapsed();
        {
            let phases = timings.0.lock().unwrap();
            assert!(phases[&Phase::Function] >= Duration::from_millis(20));
            assert!(phases[&Phase::IntentPut] >= Duration::from_millis(40));
            assert!(phases[&Phase::Upstream] >= Duration::from_millis(60));
            assert!(total >= phases.values().sum::<Duration>());
        }
        let header = timings.header(total);
        assert!(!header.contains("upstream_body"));
        assert!(header.split(", ").all(|v| v
            .split_once(";dur=")
            .unwrap()
            .1
            .parse::<f64>()
            .is_ok()));
        let isolated = Timings::default();
        isolated
            .scope(async {
                tokio::spawn(async {
                    let _t = start(Phase::Notify);
                })
                .await
                .unwrap();
            })
            .await;
        assert!(!isolated.header(total).contains("notify"));
    }
}
