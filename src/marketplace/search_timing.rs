//! Opt-in test-binary diagnostics, not telemetry or protocol/authority state.
//! Correlate one view, its exact head RPCs and their wire stages. Only closed
//! fields: no query text, extension/peer identities, payloads or error strings.
use serde::Serialize;
use std::{
    future::Future,
    io::Write,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use uuid::Uuid;
pub(super) mod cold_setup;

const MAX_EVENTS: usize = 512;
const MAX_EVENT_BYTES: usize = 2048;
static ENABLED: OnceLock<bool> = OnceLock::new();
static EVENTS: Budget = Budget(AtomicUsize::new(0));

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Context {
    pub query_id: Uuid,
    pub view_id: Uuid,
}
tokio::task_local! { static CONTEXT: Context; }
tokio::task_local! { static INTERACTIVE_WAIT: (u64, bool); }
pub(super) fn context() -> Option<Context> {
    CONTEXT.try_with(|c| *c).ok()
}
pub(super) fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("ELIXIR_MARKETPLACE_SEARCH_TIMING").is_some_and(|v| v == "1")
    })
}
pub(super) fn view(query_id: Uuid) -> Option<Context> {
    enabled().then(|| Context {
        query_id,
        view_id: Uuid::new_v4(),
    })
}
pub(super) async fn scope<T>(context: Option<Context>, future: impl Future<Output = T>) -> T {
    match context {
        Some(c) => CONTEXT.scope(c, future).await,
        None => future.await,
    }
}
pub(super) async fn interactive_wait<T>(
    wait_us: u64,
    partial_settle: bool,
    future: impl Future<Output = T>,
) -> T {
    INTERACTIVE_WAIT
        .scope((wait_us, partial_settle), future)
        .await
}
pub(super) fn interactive_wait_observation() -> Option<(u64, bool)> {
    INTERACTIVE_WAIT.try_with(|wait| *wait).ok()
}
pub(super) fn micros(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RpcKind {
    LocalSearch,
    CatalogHeads,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Status {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub(super) enum Event {
    SearchQueryTiming {
        query_id: Uuid,
        candidates: usize,
        initial_page_ms: u128,
        admission_ms: u128,
        continuation_ms: u128,
        first_admitted_ms: Option<u128>,
        total_ms: u128,
    },
    SearchViewTiming {
        context: Context,
        interactive_wait: Option<(u64, bool)>,
        supports_us: u64,
        total_us: u64,
        cards: usize,
        phase: super::search::SearchPhase,
    },
    SearchCurrentBatchTiming {
        context: Option<Context>,
        rows: usize,
        requests: usize,
        rpc_ids: Vec<u64>,
        metadata_rank_ms: u128,
        heads_ms: u128,
        after_ms: u128,
        total_ms: u128,
    },
    SearchHeadWaitTiming {
        context: Option<Context>,
        request_id: u64,
        total_us: u64,
    },
    SearchWorkerRpcTiming {
        request_id: u64,
        operation: RpcKind,
        request_bytes: usize,
        response_bytes: Option<usize>,
        write_us: Option<u64>,
        header_us: Option<u64>,
        body_us: Option<u64>,
        decode_us: Option<u64>,
        total_us: u64,
        status: Status,
    },
    SearchTimingTruncated,
}
struct Budget(AtomicUsize);
impl Budget {
    fn claim(&self) -> Option<bool> {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                if n <= MAX_EVENTS { Some(n + 1) } else { None }
            })
            .ok()
            .map(|n| n < MAX_EVENTS)
    }
}
pub(super) fn emit(event: Event) {
    if !enabled() {
        return;
    }
    let event = match EVENTS.claim() {
        Some(true) => event,
        Some(false) => Event::SearchTimingTruncated,
        None => return,
    };
    write_event(&mut std::io::stderr().lock(), &event);
}
fn write_event(output: &mut impl Write, event: &impl Serialize) {
    if let Ok(encoded) = serde_json::to_string(event) {
        if encoded.len() <= MAX_EVENT_BYTES {
            // A closed diagnostic stream must not unwind an owned RPC or
            // turn cancellation cleanup into a second panic.
            let _ = writeln!(output, "{encoded}");
        }
    }
}

pub(super) struct Rpc {
    request_id: u64,
    kind: RpcKind,
    request_bytes: usize,
    response_bytes: Option<usize>,
    started: Instant,
    checkpoint: Instant,
    write_us: Option<u64>,
    header_us: Option<u64>,
    body_us: Option<u64>,
    decode_us: Option<u64>,
    status: Status,
}
impl Rpc {
    pub fn begin(request_id: u64, kind: RpcKind, request_bytes: usize) -> Option<Self> {
        enabled().then(|| Self::new(request_id, kind, request_bytes))
    }
    fn new(request_id: u64, kind: RpcKind, request_bytes: usize) -> Self {
        let started = Instant::now();
        Self {
            request_id,
            kind,
            request_bytes,
            response_bytes: None,
            started,
            checkpoint: started,
            write_us: None,
            header_us: None,
            body_us: None,
            decode_us: None,
            status: Status::Cancelled,
        }
    }
    fn elapsed_stage(&mut self) -> u64 {
        let now = Instant::now();
        let elapsed = now
            .duration_since(self.checkpoint)
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        self.checkpoint = now;
        elapsed
    }
    pub fn written(&mut self) {
        self.write_us = Some(self.elapsed_stage());
    }
    pub fn header(&mut self, bytes: usize) {
        self.header_us = Some(self.elapsed_stage());
        self.response_bytes = Some(bytes);
    }
    pub fn body(&mut self) {
        self.body_us = Some(self.elapsed_stage());
    }
    pub fn decoded(&mut self) {
        self.decode_us = Some(self.elapsed_stage());
    }
    pub fn finished(&mut self, succeeded: bool) {
        self.status = if succeeded {
            Status::Succeeded
        } else {
            Status::Failed
        };
    }
    fn event(&self) -> Event {
        Event::SearchWorkerRpcTiming {
            request_id: self.request_id,
            operation: self.kind,
            request_bytes: self.request_bytes,
            response_bytes: self.response_bytes,
            write_us: self.write_us,
            header_us: self.header_us,
            body_us: self.body_us,
            decode_us: self.decode_us,
            total_us: micros(self.started),
            status: self.status,
        }
    }
}
impl Drop for Rpc {
    fn drop(&mut self) {
        emit(self.event());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_closed_diagnostic_stream_never_unwinds_owned_work() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        write_event(&mut Closed, &Event::SearchTimingTruncated);
        let mut bytes = vec![];
        write_event(&mut bytes, &Event::SearchTimingTruncated);
        assert_eq!(bytes, b"{\"stage\":\"search_timing_truncated\"}\n");
    }
    #[test]
    fn budget_has_one_truncation_marker_and_no_unbounded_history_or_counter_growth() {
        let budget = Budget(AtomicUsize::new(0));
        for _ in 0..MAX_EVENTS {
            assert_eq!(budget.claim(), Some(true));
        }
        assert_eq!(budget.claim(), Some(false));
        for _ in 0..1000 {
            assert_eq!(budget.claim(), None);
        }
        assert_eq!(budget.0.load(Ordering::Relaxed), MAX_EVENTS + 1);
    }
    #[tokio::test]
    async fn context_is_scoped_and_does_not_leak_to_another_task_or_replaced_view() {
        let a = Context {
            query_id: Uuid::new_v4(),
            view_id: Uuid::new_v4(),
        };
        let b = Context {
            query_id: Uuid::new_v4(),
            view_id: Uuid::new_v4(),
        };
        assert_eq!(context(), None);
        scope(Some(a), async {
            assert_eq!(context(), Some(a));
            assert_eq!(tokio::spawn(async { context() }).await.unwrap(), None);
            scope(Some(b), async {
                assert_eq!(context(), Some(b));
            })
            .await;
            assert_eq!(context(), Some(a));
        })
        .await;
        assert_eq!(context(), None);
    }
    #[test]
    fn rpc_fields_keep_partial_failure_and_cancellation_distinct_without_payloads() {
        let mut rpc = Rpc::new(7, RpcKind::CatalogHeads, 1024);
        assert_eq!(rpc.status, Status::Cancelled);
        rpc.written();
        rpc.header(2048);
        rpc.body();
        rpc.decoded();
        rpc.finished(true);
        let event = serde_json::to_value(rpc.event()).unwrap();
        assert_eq!(event["request_id"], 7);
        assert_eq!(event["status"], "succeeded");
        assert_eq!(event["response_bytes"], 2048);
        for key in ["write_us", "header_us", "body_us", "decode_us", "total_us"] {
            assert!(event[key].is_u64());
        }
        assert!(serde_json::to_vec(&event).unwrap().len() < MAX_EVENT_BYTES);
        rpc.finished(false);
        assert_eq!(rpc.status, Status::Failed);
        let failed = Rpc::new(8, RpcKind::LocalSearch, 64);
        let event = serde_json::to_value(failed.event()).unwrap();
        assert_eq!(event["status"], "cancelled");
        assert!(event["response_bytes"].is_null());
        for forbidden in [
            "query",
            "extension_id",
            "peer",
            "payload",
            "error",
            "token",
            "account",
        ] {
            assert!(event.get(forbidden).is_none());
        }
    }
}
