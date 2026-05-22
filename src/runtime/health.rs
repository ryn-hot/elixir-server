use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const RUNTIME_DEGRADED_COOLDOWN_SECONDS: i64 = 120;
const RUNTIME_RECOVERY_WINDOW_SECONDS: i64 = 180;
const RUNTIME_RECOVERY_WARMUP_SECONDS: i64 = 12;
const RUNTIME_RECOVERY_STAGGER_SECONDS: i64 = 6;
const INSTANCE_QUARANTINE_SECONDS: i64 = 300;
const RUNTIME_DEPENDENCY_DEFER_SECONDS: i64 = 45;
const RUNTIME_AUTO_RESET_FAILURE_THRESHOLD: u32 = 3;
const RUNTIME_AUTO_RESET_COOLDOWN_SECONDS: i64 = 600;
const RUNTIME_AUTO_RESET_WINDOW_SECONDS: i64 = 3600;
const RUNTIME_AUTO_RESET_MAX_ATTEMPTS_PER_WINDOW: u32 = 1;
const RUNTIME_HEALTH_POLL_INTERVAL_SECONDS: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerRuntimeHealthState {
    Healthy,
    Recovering,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerInstanceQuarantine {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub extension_name: String,
    pub instance_name: String,
    pub reason: String,
    pub until: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DockerRuntimeHealthSnapshot {
    pub state: DockerRuntimeHealthState,
    pub code: Option<String>,
    pub reason: Option<String>,
    pub until: Option<DateTime<Utc>>,
    pub host_warning: Option<String>,
    pub quarantined_instances: Vec<DockerInstanceQuarantine>,
    pub last_failure_code: Option<String>,
    pub last_failure_reason: Option<String>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_reset_attempt_at: Option<DateTime<Utc>>,
    pub auto_reset_attempts_in_window: u32,
    pub reboot_recommended: bool,
    pub dependency_actions_deferred_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DockerRuntimeSubsystemImpact {
    pub id: &'static str,
    pub label: &'static str,
    pub status: &'static str,
    pub detail: String,
}

pub struct DockerRuntimeSupervisor {
    inner: Mutex<DockerRuntimeSupervisorState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerAutoResetDecision {
    NotNeeded,
    AttemptNow,
    Cooldown,
    BudgetExceeded,
    RebootRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDockerRuntimeHealthState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded_until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_restart_after: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub quarantined_instances: Vec<DockerInstanceQuarantine>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub consecutive_engine_failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reset_attempt_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_reset_window_started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub auto_reset_attempts_in_window: u32,
    #[serde(default)]
    pub reboot_recommended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_actions_deferred_until: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct DockerRuntimeSupervisorState {
    degraded_until: Option<DateTime<Utc>>,
    recovery_until: Option<DateTime<Utc>>,
    next_restart_after: Option<DateTime<Utc>>,
    code: Option<String>,
    reason: Option<String>,
    host_warning: Option<String>,
    quarantined_instances: HashMap<Uuid, DockerInstanceQuarantine>,
    last_failure_code: Option<String>,
    last_failure_reason: Option<String>,
    last_failure_at: Option<DateTime<Utc>>,
    consecutive_engine_failures: u32,
    last_reset_attempt_at: Option<DateTime<Utc>>,
    auto_reset_window_started_at: Option<DateTime<Utc>>,
    auto_reset_attempts_in_window: u32,
    reboot_recommended: bool,
    dependency_actions_deferred_until: Option<DateTime<Utc>>,
}

impl DockerRuntimeSupervisor {
    pub fn new(host_warning: Option<String>) -> Self {
        Self {
            inner: Mutex::new(DockerRuntimeSupervisorState {
                degraded_until: None,
                recovery_until: None,
                next_restart_after: None,
                code: None,
                reason: None,
                host_warning,
                quarantined_instances: HashMap::new(),
                last_failure_code: None,
                last_failure_reason: None,
                last_failure_at: None,
                consecutive_engine_failures: 0,
                last_reset_attempt_at: None,
                auto_reset_window_started_at: None,
                auto_reset_attempts_in_window: 0,
                reboot_recommended: false,
                dependency_actions_deferred_until: None,
            }),
        }
    }

    pub fn restore(&self, persisted: PersistedDockerRuntimeHealthState) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.degraded_until = persisted.degraded_until;
        state.recovery_until = persisted.recovery_until;
        state.next_restart_after = persisted.next_restart_after;
        state.code = persisted.code;
        state.reason = persisted.reason;
        state.quarantined_instances = persisted
            .quarantined_instances
            .into_iter()
            .map(|item| (item.instance_id, item))
            .collect();
        state.last_failure_code = persisted.last_failure_code;
        state.last_failure_reason = persisted.last_failure_reason;
        state.last_failure_at = persisted.last_failure_at;
        state.consecutive_engine_failures = persisted.consecutive_engine_failures;
        state.last_reset_attempt_at = persisted.last_reset_attempt_at;
        state.auto_reset_window_started_at = persisted.auto_reset_window_started_at;
        state.auto_reset_attempts_in_window = persisted.auto_reset_attempts_in_window;
        state.reboot_recommended = persisted.reboot_recommended;
        state.dependency_actions_deferred_until = persisted.dependency_actions_deferred_until;
        state.prune_expired();
    }

    pub fn persisted_state(&self) -> PersistedDockerRuntimeHealthState {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        PersistedDockerRuntimeHealthState {
            degraded_until: state.degraded_until,
            recovery_until: state.recovery_until,
            next_restart_after: state.next_restart_after,
            code: state.code.clone(),
            reason: state.reason.clone(),
            quarantined_instances: state.quarantined_instances.values().cloned().collect(),
            last_failure_code: state.last_failure_code.clone(),
            last_failure_reason: state.last_failure_reason.clone(),
            last_failure_at: state.last_failure_at,
            consecutive_engine_failures: state.consecutive_engine_failures,
            last_reset_attempt_at: state.last_reset_attempt_at,
            auto_reset_window_started_at: state.auto_reset_window_started_at,
            auto_reset_attempts_in_window: state.auto_reset_attempts_in_window,
            reboot_recommended: state.reboot_recommended,
            dependency_actions_deferred_until: state.dependency_actions_deferred_until,
        }
    }

    pub fn snapshot(&self) -> DockerRuntimeHealthSnapshot {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        state.snapshot()
    }

    pub fn record_engine_failure(&self, code: impl Into<String>, reason: impl Into<String>) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        let code = code.into();
        let reason = reason.into();
        state.prune_expired();
        state.degraded_until =
            Some(now + ChronoDuration::seconds(RUNTIME_DEGRADED_COOLDOWN_SECONDS));
        state.recovery_until = None;
        state.next_restart_after = None;
        state.dependency_actions_deferred_until =
            Some(now + ChronoDuration::seconds(RUNTIME_DEPENDENCY_DEFER_SECONDS));
        state.code = Some(code.clone());
        state.reason = Some(reason.clone());
        state.last_failure_code = Some(code);
        state.last_failure_reason = Some(reason);
        state.last_failure_at = Some(now);
        state.consecutive_engine_failures = state.consecutive_engine_failures.saturating_add(1);
    }

    pub fn record_engine_ready(&self, started_by_elixir: bool) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        let was_degraded = state
            .degraded_until
            .map(|value| value > now)
            .unwrap_or(false);
        state.degraded_until = None;
        state.consecutive_engine_failures = 0;
        state.reboot_recommended = false;

        if was_degraded || started_by_elixir {
            state.recovery_until =
                Some(now + ChronoDuration::seconds(RUNTIME_RECOVERY_WINDOW_SECONDS));
            state.next_restart_after =
                Some(now + ChronoDuration::seconds(RUNTIME_RECOVERY_WARMUP_SECONDS));
            state.dependency_actions_deferred_until =
                Some(now + ChronoDuration::seconds(RUNTIME_DEPENDENCY_DEFER_SECONDS));
            state.code = Some("docker_runtime_recovering".to_string());
            state.reason = Some(
                "Docker recovered and Elixir is restoring extension runtimes gradually."
                    .to_string(),
            );
        } else if state
            .recovery_until
            .map(|value| value <= now)
            .unwrap_or(false)
        {
            state.recovery_until = None;
            state.next_restart_after = None;
            state.code = None;
            state.reason = None;
        }
    }

    pub fn auto_reset_decision(&self) -> DockerAutoResetDecision {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        if state.reboot_recommended {
            return DockerAutoResetDecision::RebootRequired;
        }
        if state.consecutive_engine_failures < RUNTIME_AUTO_RESET_FAILURE_THRESHOLD {
            return DockerAutoResetDecision::NotNeeded;
        }
        if state
            .last_reset_attempt_at
            .map(|value| value + ChronoDuration::seconds(RUNTIME_AUTO_RESET_COOLDOWN_SECONDS) > now)
            .unwrap_or(false)
        {
            return DockerAutoResetDecision::Cooldown;
        }
        if state.auto_reset_attempts_in_window >= RUNTIME_AUTO_RESET_MAX_ATTEMPTS_PER_WINDOW {
            return DockerAutoResetDecision::BudgetExceeded;
        }
        DockerAutoResetDecision::AttemptNow
    }

    pub fn note_manual_reset_attempt(&self) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        state.last_reset_attempt_at = Some(Utc::now());
    }

    pub fn note_auto_reset_attempt(&self) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        state.last_reset_attempt_at = Some(now);
        if state.auto_reset_window_started_at.is_none() {
            state.auto_reset_window_started_at = Some(now);
        }
        state.auto_reset_attempts_in_window = state.auto_reset_attempts_in_window.saturating_add(1);
    }

    pub fn mark_reboot_recommended(&self, reason: impl Into<String>) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        let reason = reason.into();
        state.reboot_recommended = true;
        state.degraded_until = None;
        state.recovery_until = None;
        state.next_restart_after = None;
        state.dependency_actions_deferred_until = None;
        state.code = Some("docker_runtime_reboot_recommended".to_string());
        state.reason = Some(reason.clone());
        state.last_failure_at = Some(now);
        state.last_failure_code = Some("docker_runtime_reboot_recommended".to_string());
        state.last_failure_reason = Some(reason);
    }

    pub fn should_defer_dependency_actions(&self) -> Option<(DateTime<Utc>, String)> {
        let snapshot = self.snapshot();
        let until = snapshot.dependency_actions_deferred_until?;
        let reason = snapshot.reason.unwrap_or_else(|| {
            "Docker recovered recently. Elixir is waiting for core runtimes before restoring dependent bindings."
                .to_string()
        });
        Some((until, reason))
    }

    pub fn record_recovery_progress(&self, ready: usize, total: usize) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        if !state
            .recovery_until
            .map(|value| value > now)
            .unwrap_or(false)
        {
            return;
        }
        if total == 0 || ready >= total {
            state.dependency_actions_deferred_until = None;
            state.reason = Some(
                "Docker recovered and Elixir verified that core extension runtimes are reachable again."
                    .to_string(),
            );
            return;
        }

        state.dependency_actions_deferred_until =
            Some(now + ChronoDuration::seconds(RUNTIME_DEPENDENCY_DEFER_SECONDS));
        state.reason = Some(format!(
            "Docker recovered. Elixir is waiting for {ready}/{total} core extension runtime(s) before restoring bindings and downstream automation."
        ));
    }

    pub fn is_circuit_open(&self) -> bool {
        matches!(self.snapshot().state, DockerRuntimeHealthState::Degraded)
    }

    pub fn circuit_open_until(&self) -> Option<(DateTime<Utc>, String)> {
        let snapshot = self.snapshot();
        if snapshot.state != DockerRuntimeHealthState::Degraded {
            return None;
        }
        let until = snapshot.until?;
        let reason = snapshot
            .reason
            .unwrap_or_else(|| "Docker runtime is degraded".to_string());
        Some((until, reason))
    }

    pub fn restart_delay(&self) -> Option<Duration> {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        let next_restart_after = state.next_restart_after?;
        if next_restart_after <= now {
            return None;
        }
        let remaining = next_restart_after - now;
        remaining.to_std().ok()
    }

    pub fn note_restart_started(&self) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        if state
            .recovery_until
            .map(|value| value > now)
            .unwrap_or(false)
        {
            state.next_restart_after =
                Some(now + ChronoDuration::seconds(RUNTIME_RECOVERY_STAGGER_SECONDS));
        }
    }

    pub fn quarantine_instance(
        &self,
        instance_id: Uuid,
        extension_id: impl Into<String>,
        extension_name: impl Into<String>,
        instance_name: impl Into<String>,
        reason: impl Into<String>,
    ) -> DockerInstanceQuarantine {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        let now = Utc::now();
        state.prune_expired();
        let quarantine = DockerInstanceQuarantine {
            instance_id,
            extension_id: extension_id.into(),
            extension_name: extension_name.into(),
            instance_name: instance_name.into(),
            reason: reason.into(),
            until: now + ChronoDuration::seconds(INSTANCE_QUARANTINE_SECONDS),
        };
        state
            .quarantined_instances
            .insert(instance_id, quarantine.clone());
        quarantine
    }

    pub fn quarantined_instance(&self, instance_id: Uuid) -> Option<DockerInstanceQuarantine> {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        state.quarantined_instances.get(&instance_id).cloned()
    }

    pub fn clear_instance_quarantine(&self, instance_id: Uuid) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        state.quarantined_instances.remove(&instance_id);
    }

    pub fn clear_all_quarantines(&self) {
        let mut state = self.inner.lock().expect("runtime supervisor lock");
        state.prune_expired();
        state.quarantined_instances.clear();
    }
}

impl DockerRuntimeSupervisorState {
    fn prune_expired(&mut self) {
        let now = Utc::now();
        self.quarantined_instances
            .retain(|_, item| item.until > now);
        if self
            .degraded_until
            .map(|value| value <= now)
            .unwrap_or(false)
        {
            self.degraded_until = None;
        }
        if self
            .recovery_until
            .map(|value| value <= now)
            .unwrap_or(false)
        {
            self.recovery_until = None;
            self.next_restart_after = None;
            if !self.reboot_recommended {
                self.code = None;
                self.reason = None;
            }
        }
        if self
            .dependency_actions_deferred_until
            .map(|value| value <= now)
            .unwrap_or(false)
        {
            self.dependency_actions_deferred_until = None;
        }
        if self
            .auto_reset_window_started_at
            .map(|value| value + ChronoDuration::seconds(RUNTIME_AUTO_RESET_WINDOW_SECONDS) <= now)
            .unwrap_or(false)
        {
            self.auto_reset_window_started_at = None;
            self.auto_reset_attempts_in_window = 0;
        }
    }

    fn snapshot(&self) -> DockerRuntimeHealthSnapshot {
        let now = Utc::now();
        let state = if self.reboot_recommended
            || self
                .degraded_until
                .map(|value| value > now)
                .unwrap_or(false)
        {
            DockerRuntimeHealthState::Degraded
        } else if self
            .recovery_until
            .map(|value| value > now)
            .unwrap_or(false)
        {
            DockerRuntimeHealthState::Recovering
        } else {
            DockerRuntimeHealthState::Healthy
        };

        let until = match state {
            DockerRuntimeHealthState::Healthy => None,
            DockerRuntimeHealthState::Recovering => self.recovery_until,
            DockerRuntimeHealthState::Degraded => self.degraded_until,
        };

        DockerRuntimeHealthSnapshot {
            state,
            code: self.code.clone(),
            reason: self.reason.clone(),
            until,
            host_warning: self.host_warning.clone(),
            quarantined_instances: self.quarantined_instances.values().cloned().collect(),
            last_failure_code: self.last_failure_code.clone(),
            last_failure_reason: self.last_failure_reason.clone(),
            last_failure_at: self.last_failure_at,
            last_reset_attempt_at: self.last_reset_attempt_at,
            auto_reset_attempts_in_window: self.auto_reset_attempts_in_window,
            reboot_recommended: self.reboot_recommended,
            dependency_actions_deferred_until: self.dependency_actions_deferred_until,
        }
    }
}

pub fn runtime_health_poll_interval() -> Duration {
    Duration::from_secs(RUNTIME_HEALTH_POLL_INTERVAL_SECONDS)
}

pub fn docker_auto_reset_max_attempts_per_window() -> u32 {
    RUNTIME_AUTO_RESET_MAX_ATTEMPTS_PER_WINDOW
}

pub fn docker_auto_reset_window_seconds() -> i64 {
    RUNTIME_AUTO_RESET_WINDOW_SECONDS
}

pub fn docker_auto_reset_cooldown_seconds() -> i64 {
    RUNTIME_AUTO_RESET_COOLDOWN_SECONDS
}

pub fn docker_runtime_affected_subsystems(
    snapshot: &DockerRuntimeHealthSnapshot,
) -> Vec<DockerRuntimeSubsystemImpact> {
    const SUBSYSTEMS: [(&str, &str); 5] = [
        ("extensions", "Extensions"),
        ("qbittorrent", "qBittorrent"),
        ("nzbget", "NZBGet"),
        ("arr_stack", "Arr stack"),
        (
            "protected_downloader_networking",
            "Protected downloader networking",
        ),
    ];

    SUBSYSTEMS
        .into_iter()
        .map(|(id, label)| {
            let (status, detail) = docker_runtime_subsystem_state(snapshot, id);
            DockerRuntimeSubsystemImpact {
                id,
                label,
                status,
                detail,
            }
        })
        .collect()
}

fn docker_runtime_subsystem_state(
    snapshot: &DockerRuntimeHealthSnapshot,
    subsystem_id: &str,
) -> (&'static str, String) {
    if snapshot.reboot_recommended {
        return (
            "blocked",
            "Docker recovery needs a host reboot before Elixir resumes this runtime-backed subsystem."
                .to_string(),
        );
    }

    match snapshot.state {
        DockerRuntimeHealthState::Degraded => (
            "blocked",
            match subsystem_id {
                "extensions" => {
                    "Container-backed extension runtimes are paused while Docker is degraded."
                        .to_string()
                }
                "qbittorrent" => {
                    "qBittorrent runtime operations fail closed while Docker is degraded."
                        .to_string()
                }
                "nzbget" => "NZBGet runtime operations fail closed while Docker is degraded."
                    .to_string(),
                "arr_stack" => {
                    "Arr connector and binding work is paused so downstream state is not rewritten while Docker is degraded."
                        .to_string()
                }
                "protected_downloader_networking" => {
                    "Protected downloader gateway and rehome operations are paused while Docker is degraded."
                        .to_string()
                }
                _ => "Docker-backed runtime operations are paused while Docker is degraded."
                    .to_string(),
            },
        ),
        DockerRuntimeHealthState::Recovering => {
            let status = if snapshot.dependency_actions_deferred_until.is_some() {
                "deferred"
            } else {
                "recovering"
            };
            (
                status,
                match subsystem_id {
                    "arr_stack" => {
                        "Arr connector and binding work waits until core provider runtimes are reachable again."
                            .to_string()
                    }
                    "protected_downloader_networking" => {
                        "Protected downloader networking resumes after Docker recovery warmup completes."
                            .to_string()
                    }
                    _ => {
                        "Runtime operations are staged while Docker recovery and provider readiness checks complete."
                            .to_string()
                    }
                },
            )
        }
        DockerRuntimeHealthState::Healthy => {
            if subsystem_id == "extensions" && !snapshot.quarantined_instances.is_empty() {
                return (
                    "attention",
                    format!(
                        "{} extension instance(s) remain quarantined while Docker stabilizes.",
                        snapshot.quarantined_instances.len()
                    ),
                );
            }
            if let Some(host_warning) = snapshot.host_warning.as_deref() {
                if !host_warning.trim().is_empty() {
                    return ("attention", host_warning.to_string());
                }
            }
            (
                "ok",
                "Runtime operations are available for this subsystem.".to_string(),
            )
        }
    }
}

pub fn detect_docker_desktop_filesharing_warning() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")?;
        let path = PathBuf::from(home)
            .join("Library")
            .join("Group Containers")
            .join("group.com.docker")
            .join("settings.json");
        let raw = std::fs::read_to_string(path).ok()?;
        let json = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
        docker_desktop_filesharing_warning_from_settings(&json)
    }

    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

fn docker_desktop_filesharing_warning_from_settings(json: &serde_json::Value) -> Option<String> {
    let use_grpc_fuse = json.get("useGrpcfuse").and_then(serde_json::Value::as_bool);
    let use_virtiofs = json
        .get("useVirtualizationFrameworkVirtioFS")
        .and_then(serde_json::Value::as_bool);
    if use_virtiofs == Some(true) {
        return None;
    }
    if use_grpc_fuse == Some(true) || matches!(use_virtiofs, Some(false)) {
        return Some(
            "Docker Desktop is using gRPC FUSE or has VirtioFS disabled. VirtioFS is recommended for Elixir-managed downloads on macOS."
                .to_string(),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn supervisor_enters_degraded_and_recovering_states() {
        let supervisor = DockerRuntimeSupervisor::new(None);
        supervisor.record_engine_failure("docker_runtime_unhealthy", "daemon missing");
        assert_eq!(
            supervisor.snapshot().state,
            DockerRuntimeHealthState::Degraded
        );

        supervisor.record_engine_ready(true);
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, DockerRuntimeHealthState::Recovering);
        assert_eq!(snapshot.code.as_deref(), Some("docker_runtime_recovering"));
    }

    #[test]
    fn supervisor_tracks_and_clears_quarantine() {
        let supervisor = DockerRuntimeSupervisor::new(None);
        let instance_id = Uuid::new_v4();
        supervisor.quarantine_instance(
            instance_id,
            "elixir.modules.sonarr",
            "Sonarr",
            "default",
            "container would not stop",
        );
        assert!(supervisor.quarantined_instance(instance_id).is_some());
        supervisor.clear_instance_quarantine(instance_id);
        assert!(supervisor.quarantined_instance(instance_id).is_none());
    }

    #[test]
    fn supervisor_enforces_auto_reset_budget() {
        let supervisor = DockerRuntimeSupervisor::new(None);
        for _ in 0..RUNTIME_AUTO_RESET_FAILURE_THRESHOLD {
            supervisor.record_engine_failure("docker_runtime_unavailable", "daemon missing");
        }
        assert_eq!(
            supervisor.auto_reset_decision(),
            DockerAutoResetDecision::AttemptNow
        );

        supervisor.note_auto_reset_attempt();
        assert_eq!(
            supervisor.auto_reset_decision(),
            DockerAutoResetDecision::Cooldown
        );

        let mut persisted = supervisor.persisted_state();
        persisted.last_reset_attempt_at =
            Some(Utc::now() - ChronoDuration::seconds(RUNTIME_AUTO_RESET_COOLDOWN_SECONDS + 5));
        supervisor.restore(persisted);
        assert_eq!(
            supervisor.auto_reset_decision(),
            DockerAutoResetDecision::BudgetExceeded
        );
    }

    #[test]
    fn supervisor_restores_persisted_state() {
        let supervisor = DockerRuntimeSupervisor::new(None);
        let instance_id = Uuid::new_v4();
        let persisted = PersistedDockerRuntimeHealthState {
            degraded_until: Some(Utc::now() + ChronoDuration::seconds(30)),
            recovery_until: None,
            next_restart_after: None,
            code: Some("docker_runtime_unavailable".to_string()),
            reason: Some("daemon missing".to_string()),
            quarantined_instances: vec![DockerInstanceQuarantine {
                instance_id,
                extension_id: "elixir.modules.sonarr".to_string(),
                extension_name: "Sonarr".to_string(),
                instance_name: "default".to_string(),
                reason: "kill stuck".to_string(),
                until: Utc::now() + ChronoDuration::seconds(60),
            }],
            last_failure_code: Some("docker_runtime_unavailable".to_string()),
            last_failure_reason: Some("daemon missing".to_string()),
            last_failure_at: Some(Utc::now()),
            consecutive_engine_failures: 3,
            last_reset_attempt_at: Some(Utc::now()),
            auto_reset_window_started_at: Some(Utc::now()),
            auto_reset_attempts_in_window: 1,
            reboot_recommended: true,
            dependency_actions_deferred_until: Some(
                Utc::now() + ChronoDuration::seconds(RUNTIME_DEPENDENCY_DEFER_SECONDS),
            ),
        };

        supervisor.restore(persisted);
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, DockerRuntimeHealthState::Degraded);
        assert!(snapshot.reboot_recommended);
        assert_eq!(snapshot.auto_reset_attempts_in_window, 1);
        assert_eq!(snapshot.quarantined_instances.len(), 1);
    }

    #[test]
    fn supervisor_recovery_progress_can_release_dependency_defer() {
        let supervisor = DockerRuntimeSupervisor::new(None);
        supervisor.record_engine_failure("docker_runtime_unavailable", "daemon missing");
        supervisor.record_engine_ready(true);
        assert!(supervisor.should_defer_dependency_actions().is_some());

        supervisor.record_recovery_progress(2, 2);
        assert!(supervisor.should_defer_dependency_actions().is_none());
    }

    #[test]
    fn docker_desktop_warning_is_suppressed_when_virtiofs_is_enabled() {
        let settings = json!({
            "useGrpcfuse": true,
            "useVirtualizationFrameworkVirtioFS": true
        });
        assert!(docker_desktop_filesharing_warning_from_settings(&settings).is_none());
    }

    #[test]
    fn docker_desktop_warning_is_reported_when_virtiofs_is_disabled() {
        let settings = json!({
            "useGrpcfuse": false,
            "useVirtualizationFrameworkVirtioFS": false
        });
        assert!(docker_desktop_filesharing_warning_from_settings(&settings).is_some());
    }
}
