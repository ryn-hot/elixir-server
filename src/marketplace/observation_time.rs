//! Local health evidence must age even if a monotonic clock pauses on sleep.
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy)]
pub(super) struct ObservationTime {
    monotonic: Instant,
    wall: SystemTime,
}

impl ObservationTime {
    pub(super) fn now() -> Self {
        Self::capture(Instant::now())
    }

    pub(super) fn capture(monotonic: Instant) -> Self {
        Self {
            monotonic,
            wall: SystemTime::now(),
        }
    }

    pub(super) fn is_current(self, maximum: Duration) -> bool {
        self.is_current_at(Instant::now(), SystemTime::now(), maximum)
    }

    fn is_current_at(self, monotonic: Instant, wall: SystemTime, maximum: Duration) -> bool {
        self.checked_age_at(monotonic, wall)
            .is_some_and(|elapsed| elapsed <= maximum)
    }

    pub(super) fn checked_age_at(self, monotonic: Instant, wall: SystemTime) -> Option<Duration> {
        Some(
            monotonic
                .checked_duration_since(self.monotonic)?
                .max(wall.duration_since(self.wall).ok()?),
        )
    }

    pub(super) fn monotonic_deadline(self, maximum: Duration) -> Option<Instant> {
        self.monotonic.checked_add(maximum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_clocks_must_be_within_the_existing_age_window() {
        let started = ObservationTime::now();
        let max = Duration::from_secs(3);
        let expired = max + Duration::from_millis(1);
        assert!(started.is_current_at(started.monotonic + max, started.wall + max, max));
        assert!(!started.is_current_at(started.monotonic, started.wall + expired, max));
        assert!(!started.is_current_at(started.monotonic + expired, started.wall, max));
    }

    #[test]
    fn either_clock_rollback_denies_a_current_observation() {
        let started = ObservationTime::now();
        let max = Duration::from_secs(600);
        let back = Duration::from_millis(1);
        assert!(!started.is_current_at(started.monotonic, started.wall - back, max));
        assert!(!started.is_current_at(started.monotonic - back, started.wall, max));
    }
}
