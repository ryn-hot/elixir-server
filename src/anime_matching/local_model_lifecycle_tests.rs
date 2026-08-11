#![cfg(unix)]

use super::*;
use std::{fs, os::unix::fs::PermissionsExt};

const FAKE_WORKER: &[u8] = br#"#!/usr/bin/env python3
import http.server
import os
import signal
import sys
import time

LIFECYCLE_PATH = __file__ + ".lifecycle"
MODE_PATH = __file__ + ".mode"
RELEASE_PATH = __file__ + ".release"
SPAWN_COUNT_PATH = __file__ + ".spawn-count"


def next_spawn_number():
    try:
        with open(SPAWN_COUNT_PATH, "r", encoding="utf-8") as source:
            current = int(source.read().strip())
    except (FileNotFoundError, ValueError):
        current = 0
    current += 1
    with open(SPAWN_COUNT_PATH, "w", encoding="utf-8") as destination:
        destination.write(str(current))
        destination.flush()
        os.fsync(destination.fileno())
    return current


SPAWN_NUMBER = next_spawn_number()
with open(MODE_PATH, "r", encoding="utf-8") as source:
    MODE = source.read().strip()
chat_calls = 0
health_calls = 0


def record(event):
    with open(LIFECYCLE_PATH, "a", encoding="utf-8") as lifecycle:
        lifecycle.write(f"{event}:{os.getpid()}\n")
        lifecycle.flush()
        os.fsync(lifecycle.fileno())


def stop(_signal, _frame):
    record(f"stopped:{SPAWN_NUMBER}")
    os._exit(0)


VALID = b'{"choices":[{"message":{"content":"{\\"m\\":[[0,[0],[0]]]}"}}],"usage":{"prompt_tokens":32,"completion_tokens":14},"timings":{"prompt_ms":10.0,"predicted_ms":10.0}}'
WRONG = b'{"choices":[{"message":{"content":"{\\"m\\":[]}"}}],"usage":{"prompt_tokens":32,"completion_tokens":4},"timings":{"prompt_ms":10.0,"predicted_ms":10.0}}'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        global health_calls
        if self.path == "/health":
            health_calls += 1
            if MODE == "health_stall" and health_calls > 1:
                record(f"health-stall:{SPAWN_NUMBER}:{health_calls}")
                while True:
                    time.sleep(60)
            self.reply(200, b'{}')
        else:
            self.reply(404, b'{}')

    def do_POST(self):
        global chat_calls
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        if self.path == "/v1/chat/completions/input_tokens":
            self.reply(
                200,
                b'{"object":"response.input_tokens","input_tokens":32}',
            )
            return
        if self.path != "/v1/chat/completions":
            self.reply(404, b'{}')
            return

        chat_calls += 1
        record(f"chat:{SPAWN_NUMBER}:{chat_calls}")
        if MODE == "stall":
            while True:
                time.sleep(60)
        if MODE == "wrong_wait":
            while not os.path.exists(RELEASE_PATH):
                time.sleep(0.01)
            self.reply(200, WRONG)
            return
        if MODE == "wrong" or (MODE == "restart" and SPAWN_NUMBER > 1):
            self.reply(200, WRONG)
            return
        self.reply(200, VALID)

    def log_message(self, _format, *_arguments):
        pass


arguments = sys.argv
host = arguments[arguments.index("--host") + 1]
port = int(arguments[arguments.index("--port") + 1])
signal.signal(signal.SIGTERM, stop)
server = http.server.ThreadingHTTPServer((host, port), Handler)
server.daemon_threads = True
record(f"started:{SPAWN_NUMBER}")
server.serve_forever()
"#;

struct FakeRuntime {
    _directory: tempfile::TempDir,
    worker_path: PathBuf,
    model_path: PathBuf,
    lifecycle_path: PathBuf,
    release_path: PathBuf,
    spawn_count_path: PathBuf,
}

impl FakeRuntime {
    fn new(mode: &str) -> Self {
        let directory = tempfile::tempdir().expect("temporary fake-worker directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        fs::write(&worker_path, FAKE_WORKER).expect("write fake worker");
        fs::set_permissions(&worker_path, fs::Permissions::from_mode(0o755))
            .expect("make fake worker executable");
        fs::write(&model_path, b"fixture model").expect("write fake model");
        fs::write(worker_path.with_extension("mode"), mode).expect("write fake-worker mode");
        Self {
            lifecycle_path: worker_path.with_extension("lifecycle"),
            release_path: worker_path.with_extension("release"),
            spawn_count_path: worker_path.with_extension("spawn-count"),
            worker_path,
            model_path,
            _directory: directory,
        }
    }

    fn profile(&self) -> LocalModelRuntimeProfile {
        LocalModelRuntimeProfile {
            bundle_version: "2026.08.lifecycle-test".to_string(),
            model_id: "qwen3-4b-instruct-2507".to_string(),
            model_revision: "fixture-model-r1".to_string(),
            worker_revision: "fixture-worker-r1".to_string(),
            backend: "cpu".to_string(),
            profile_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            protocol_version: LLAMA_SERVER_PROTOCOL_VERSION,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
            worker_path: self.worker_path.clone(),
            model_path: self.model_path.clone(),
            context_tokens: V1_CONTEXT_TOKENS,
            max_output_tokens: 256,
            threads: 2,
            batch_threads: 2,
            gpu_layers: 0,
            kv_cache_type: "f16".to_string(),
            peak_rss_bytes: 512 * 1024 * 1024,
            idle_unload_seconds: 300,
            sampling: LocalModelSamplingProfile::default(),
        }
    }

    fn events(&self) -> Vec<String> {
        fs::read_to_string(&self.lifecycle_path)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn spawn_count(&self) -> u64 {
        fs::read_to_string(&self.spawn_count_path)
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }

    fn release_waiting_response(&self) {
        fs::write(&self.release_path, b"release").expect("release fake-worker response");
    }
}

struct RevocableAdmission {
    revoked: AtomicBool,
}

impl LocalModelAdmission for RevocableAdmission {
    fn admit(
        &self,
        phase: LocalModelAdmissionPhase,
        _profile: &LocalModelRuntimeProfile,
    ) -> Result<()> {
        if self.revoked.load(Ordering::Acquire)
            && matches!(
                phase,
                LocalModelAdmissionPhase::Inference
                    | LocalModelAdmissionPhase::ActivationInference
                    | LocalModelAdmissionPhase::ProbeInference
            )
        {
            bail!("fixture admission revoked")
        }
        Ok(())
    }
}

async fn activate_fake(runtime: &FakeRuntime) -> LocalModelEngine {
    let engine = LocalModelEngine::allow_all_for_probe().expect("probe engine");
    engine
        .activate_profile_for_probe(runtime.profile())
        .await
        .expect("activate fake profile");
    engine
}

async fn wait_for_event(runtime: &FakeRuntime, prefix: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let events = runtime.events();
        if events.iter().any(|event| event.starts_with(prefix)) {
            return events;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {prefix:?}; events={events:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_background_settled(engine: &LocalModelEngine) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let requested = engine
                .inner
                .background_prime_requested
                .load(Ordering::Acquire);
            let completed = engine
                .inner
                .background_prime_completed
                .load(Ordering::Acquire);
            if !engine.inner.background_warm_active.load(Ordering::Acquire)
                && completed == requested
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background prime did not settle");
}

fn started_pid(events: &[String], spawn_number: u64) -> i32 {
    events
        .iter()
        .find_map(|event| {
            let mut fields = event.split(':');
            (fields.next() == Some("started")
                && fields.next()?.parse::<u64>().ok()? == spawn_number)
                .then(|| fields.next()?.parse::<i32>().ok())?
        })
        .unwrap_or_else(|| panic!("missing PID for spawn {spawn_number}: {events:?}"))
}

fn assert_process_reaped(process_id: i32) {
    assert_eq!(
        unsafe { libc::kill(process_id, 0) },
        -1,
        "fake worker {process_id} remains alive"
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH),
        "fake worker {process_id} was not fully reaped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_and_sequential_prime_run_one_completion_and_publish_ready() {
    let runtime = FakeRuntime::new("valid");
    let engine = activate_fake(&runtime).await;

    let (first, second, third) = tokio::join!(engine.prime(), engine.prime(), engine.prime());
    first.expect("first prime");
    second.expect("coalesced concurrent prime");
    third.expect("coalesced concurrent prime");
    engine.prime().await.expect("idempotent sequential prime");

    let snapshot = engine.snapshot().await;
    assert_eq!(snapshot.state, LocalModelWorkerState::Ready);
    assert!(snapshot.process_id.is_some());
    let events = runtime.events();
    assert_eq!(runtime.spawn_count(), 1, "unexpected respawn: {events:?}");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("chat:"))
            .count(),
        1,
        "idempotent prime ran more than one completion: {events:?}"
    );

    engine.shutdown().await;
}

#[tokio::test]
async fn primed_health_stall_is_bounded_and_reaped() {
    let runtime = FakeRuntime::new("health_stall");
    let engine = activate_fake(&runtime).await;
    engine.prime().await.expect("initial valid prime");
    let events = runtime.events();
    let process_id = started_pid(&events, 1);
    tokio::time::pause();

    let error = engine
        .prime()
        .await
        .expect_err("stalled health response must hit the readiness deadline");
    assert!(
        error.to_string().contains("readiness deadline exceeded"),
        "unexpected stalled-health failure: {error:#}"
    );
    let failed = engine.snapshot().await;
    assert_eq!(failed.state, LocalModelWorkerState::Unavailable);
    assert!(failed.process_id.is_none());
    assert_process_reaped(process_id);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_prime_is_killed_and_reaped_without_ever_publishing_ready() {
    let runtime = FakeRuntime::new("wrong_wait");
    let engine = activate_fake(&runtime).await;
    let prime_engine = engine.clone();
    let prime_task = tokio::spawn(async move { prime_engine.prime().await });

    let events = wait_for_event(&runtime, "chat:1:1:").await;
    let process_id = started_pid(&events, 1);
    let starting = engine.snapshot().await;
    assert_eq!(starting.state, LocalModelWorkerState::Starting);
    assert_eq!(starting.process_id, u32::try_from(process_id).ok());
    runtime.release_waiting_response();

    let error = tokio::time::timeout(Duration::from_secs(3), prime_task)
        .await
        .expect("wrong prime did not terminate")
        .expect("wrong-prime task panicked")
        .expect_err("wrong prime must fail");
    assert!(error.to_string().contains("wrong mapping"), "{error:#}");
    let failed = engine.snapshot().await;
    assert_eq!(failed.state, LocalModelWorkerState::Unavailable);
    assert!(failed.process_id.is_none());
    assert_process_reaped(process_id);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_stalled_prime_reaps_worker_and_never_publishes_ready() {
    let runtime = FakeRuntime::new("stall");
    let engine = activate_fake(&runtime).await;
    let prime_engine = engine.clone();
    let prime_task = tokio::spawn(async move { prime_engine.prime().await });

    let events = wait_for_event(&runtime, "chat:1:1:").await;
    let process_id = started_pid(&events, 1);
    assert_eq!(
        engine.snapshot().await.state,
        LocalModelWorkerState::Starting
    );
    tokio::time::timeout(Duration::from_secs(3), engine.shutdown())
        .await
        .expect("shutdown did not cancel stalled prime");
    let prime_result = tokio::time::timeout(Duration::from_secs(3), prime_task)
        .await
        .expect("stalled prime task did not exit")
        .expect("stalled-prime task panicked");
    assert!(prime_result.is_err());

    let stopped = engine.snapshot().await;
    assert_eq!(stopped.state, LocalModelWorkerState::Inactive);
    assert!(stopped.process_id.is_none());
    assert_process_reaped(process_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_revocation_during_stalled_prime_reaps_worker_and_never_publishes_ready() {
    let runtime = FakeRuntime::new("stall");
    let admission = Arc::new(RevocableAdmission {
        revoked: AtomicBool::new(false),
    });
    let engine = LocalModelEngine::new_for_probe(admission.clone()).expect("probe engine");
    engine
        .activate_profile_for_probe(runtime.profile())
        .await
        .expect("activate fake profile");
    let prime_engine = engine.clone();
    let prime_task = tokio::spawn(async move { prime_engine.prime().await });

    let events = wait_for_event(&runtime, "chat:1:1:").await;
    let process_id = started_pid(&events, 1);
    assert_eq!(
        engine.snapshot().await.state,
        LocalModelWorkerState::Starting
    );
    admission.revoked.store(true, Ordering::Release);
    let error = tokio::time::timeout(Duration::from_secs(3), prime_task)
        .await
        .expect("admission-revoked prime did not terminate")
        .expect("admission-revoked prime task panicked")
        .expect_err("revoked prime must fail");
    assert!(error.to_string().contains("admission"), "{error:#}");

    let rejected = engine.snapshot().await;
    assert_eq!(rejected.state, LocalModelWorkerState::Inactive);
    assert!(rejected.process_id.is_none());
    assert_process_reaped(process_id);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_replacement_prime_does_not_bypass_the_single_restart_budget() {
    let runtime = FakeRuntime::new("restart");
    let engine = activate_fake(&runtime).await;
    engine.prime().await.expect("initial valid prime");
    let original_process_id = engine
        .crash_active_worker_for_certification()
        .await
        .expect("crash initial worker");

    engine
        .match_candidates(prime_request().expect("fixed prime request"))
        .await
        .expect_err("crash must use deterministic fallback");
    wait_for_event(&runtime, "chat:2:1:").await;
    wait_for_background_settled(&engine).await;
    assert_eq!(
        runtime.spawn_count(),
        2,
        "one replacement was not attempted"
    );

    for _ in 0..3 {
        engine
            .match_candidates(prime_request().expect("fixed prime request"))
            .await
            .expect_err("unavailable model must use deterministic fallback");
        wait_for_background_settled(&engine).await;
    }

    let spawn_count = runtime.spawn_count();
    engine.shutdown().await;
    assert_eq!(
        spawn_count, 2,
        "requests bypassed the exhausted restart budget and spawned {spawn_count} workers"
    );
    assert_process_reaped(i32::try_from(original_process_id).expect("worker PID fits i32"));
}
