use std::{sync::Arc, time::Duration};

use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::cache::CacheOperation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitAdmission {
    Allowed,
    Probe,
    Open,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CircuitKey {
    provider_id: Uuid,
    operation: CacheOperation,
}

#[derive(Debug)]
enum CircuitMode {
    Closed,
    Open { until: tokio::time::Instant },
    HalfOpen,
}

#[derive(Debug)]
struct CircuitState {
    failures: u32,
    mode: CircuitMode,
}

impl Default for CircuitState {
    fn default() -> Self {
        Self {
            failures: 0,
            mode: CircuitMode::Closed,
        }
    }
}

#[derive(Clone)]
pub struct ProviderCircuitBreakers {
    entries: Arc<DashMap<CircuitKey, Arc<Mutex<CircuitState>>>>,
    failure_threshold: u32,
    open_duration: Duration,
    max_entries: usize,
}

impl Default for ProviderCircuitBreakers {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30), 4_096)
    }
}

impl ProviderCircuitBreakers {
    pub fn new(failure_threshold: u32, open_duration: Duration, max_entries: usize) -> Self {
        assert!(failure_threshold > 0 && max_entries > 0);
        Self {
            entries: Arc::new(DashMap::new()),
            failure_threshold,
            open_duration,
            max_entries,
        }
    }

    pub async fn admit(&self, provider_id: Uuid, operation: CacheOperation) -> CircuitAdmission {
        let state = self.state(provider_id, operation);
        let mut state = state.lock().await;
        match state.mode {
            CircuitMode::Closed => CircuitAdmission::Allowed,
            CircuitMode::Open { until } if tokio::time::Instant::now() < until => {
                CircuitAdmission::Open
            }
            CircuitMode::Open { .. } => {
                state.mode = CircuitMode::HalfOpen;
                CircuitAdmission::Probe
            }
            CircuitMode::HalfOpen => CircuitAdmission::Open,
        }
    }

    pub async fn record_success(&self, provider_id: Uuid, operation: CacheOperation) {
        let state = self.state(provider_id, operation);
        let mut state = state.lock().await;
        state.failures = 0;
        state.mode = CircuitMode::Closed;
    }

    pub async fn record_failure(&self, provider_id: Uuid, operation: CacheOperation) {
        let state = self.state(provider_id, operation);
        let mut state = state.lock().await;
        state.failures = state.failures.saturating_add(1);
        if state.failures >= self.failure_threshold || matches!(state.mode, CircuitMode::HalfOpen) {
            state.mode = CircuitMode::Open {
                until: tokio::time::Instant::now() + self.open_duration,
            };
        }
    }

    fn state(&self, provider_id: Uuid, operation: CacheOperation) -> Arc<Mutex<CircuitState>> {
        let key = CircuitKey {
            provider_id,
            operation,
        };
        if let Some(state) = self.entries.get(&key) {
            return state.clone();
        }
        if self.entries.len() >= self.max_entries {
            if let Some(candidate) = self.entries.iter().next().map(|entry| *entry.key()) {
                self.entries.remove(&candidate);
            }
        }
        match self.entries.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let state = Arc::new(Mutex::new(CircuitState::default()));
                entry.insert(state.clone());
                state
            }
        }
    }
}
