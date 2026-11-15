//! Marks the legacy Arr stack's own code: the server's Sonarr, Radarr and
//! Prowlarr drivers, the code that reads their API keys from `config.xml`,
//! the server's direct calls to their APIs, its web-page rules for them and
//! its Sonarr library source. Stage 3 deletes all of it. A test turns recording
//! on to prove a marketplace stack never runs any of it; otherwise each mark is
//! one relaxed atomic load. See docs/architecture/arr-stack-marketplace-extension.md.
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

static RECORDING: AtomicBool = AtomicBool::new(false);
static RAN: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// Called where a piece of the legacy Arr stack's code starts.
#[inline]
pub fn mark(name: &'static str) {
    if RECORDING.load(Ordering::Relaxed) {
        if let Ok(mut ran) = RAN.lock() {
            if !ran.contains(&name) {
                ran.push(name);
            }
        }
    }
}

/// Clears the record and starts recording.
pub fn start_recording() {
    if let Ok(mut ran) = RAN.lock() {
        ran.clear();
    }
    RECORDING.store(true, Ordering::Relaxed);
}

/// Stops recording and returns what ran since `start_recording`.
pub fn stop_recording() -> Vec<&'static str> {
    RECORDING.store(false, Ordering::Relaxed);
    RAN.lock().map(|ran| ran.clone()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[test]
    fn records_only_while_recording() {
        // Other tests may mark legacy code concurrently; check only our names.
        super::mark("legacy_arr test: before");
        super::start_recording();
        super::mark("legacy_arr test: during");
        super::mark("legacy_arr test: during");
        let ran = super::stop_recording();
        super::mark("legacy_arr test: after");
        assert_eq!(
            ran.iter()
                .filter(|name| **name == "legacy_arr test: during")
                .count(),
            1
        );
        assert!(!ran.contains(&"legacy_arr test: before"));
        assert!(!super::stop_recording().contains(&"legacy_arr test: after"));
    }
}
