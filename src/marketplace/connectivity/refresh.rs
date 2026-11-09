//! One bounded refresh, not a background task or a public-worker constructor.
//! The probe owns only immutable expectations, leaving the controller available
//! for live tunnel checks and lease renewal. No credits or queues are created.
use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{connectivity::WarpState, network_probe::WireguardIdentity};
use tokio::time::{Instant, sleep_until, timeout_at};

use super::{ConnectivityObservation, ProbeBinding, ProbePolicy, observe_fixed_worker};
use crate::{
    marketplace::{
        packet::PacketGuardController, runtime::inspect_fixed_worker, storage::FixedWorkerSpec,
    },
    runtime::{RuntimeManager, model::ContainerHandle},
};

const REFRESH_DEADLINE: Duration = Duration::from_secs(45);
const MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

#[cfg(unix)]
struct ProviderLease<'a> {
    runtime: &'a dyn RuntimeManager,
    guard: &'a mut PacketGuardController,
    previous: &'a crate::marketplace::packet::ProviderConnectivity,
}
#[cfg(unix)]
impl CheckedLease for ProviderLease<'_> {
    async fn renew(&mut self) -> Result<()> {
        self.guard
            .renew_provider_lease(self.runtime, self.previous)
            .await
    }
}

/// The same bounded DNS/TLS exchange and checked scheduling loop, with actual
/// provider/TUN evidence rather than a fabricated WireGuard identity. Owning
/// `previous` means cancellation cannot restore the caller's old health slot.
#[cfg(unix)]
pub(in crate::marketplace) async fn refresh_provider(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    profile: &FixedWorkerSpec,
    guard: &mut PacketGuardController,
    previous: crate::marketplace::packet::ProviderConnectivity,
) -> Result<crate::marketplace::packet::ProviderConnectivity> {
    if !previous.is_recent_for(guard) {
        bail!("provider refresh requires current protected DNS/TLS health");
    }
    let policy = ProbePolicy {
        family: previous.family(),
        require_warp: true,
    };
    let binding = ProbeBinding::capture(guard, profile)?;
    let deadline = Instant::now() + REFRESH_DEADLINE;
    timeout_at(deadline, inspect_fixed_worker(runtime, handle, profile))
        .await
        .context("provider refresh worker inspection deadline")??;
    let interval =
        Duration::from_millis(u64::from(guard.profile().policy().lease_seconds) * 1000 / 3)
            .min(MAX_HEARTBEAT_INTERVAL);
    let sample = {
        let mut lease = ProviderLease {
            runtime,
            guard,
            previous: &previous,
        };
        drive(
            observe_fixed_worker(runtime, handle, profile, binding, policy),
            &mut lease,
            interval,
            deadline,
        )
        .await?
    };
    timeout_at(
        deadline,
        guard.accept_provider_worker_connectivity(runtime, sample),
    )
    .await
    .context("provider refresh final inspection deadline")?
}

// Private seam exercises the same scheduling loop with deterministic clocks;
// production implementations can only renew using the owned packet controller.
trait CheckedLease: Send {
    fn renew(&mut self) -> impl Future<Output = Result<()>> + Send;
}

struct LiveLease<'a> {
    runtime: &'a dyn RuntimeManager,
    guard: &'a mut PacketGuardController,
    identity: &'a WireguardIdentity,
    previous: &'a ConnectivityObservation,
}
impl CheckedLease for LiveLease<'_> {
    async fn renew(&mut self) -> Result<()> {
        renew_once(self.runtime, self.guard, self.identity, self.previous).await
    }
}

pub(super) async fn renew_once(
    runtime: &dyn RuntimeManager,
    guard: &mut PacketGuardController,
    identity: &WireguardIdentity,
    previous: &ConnectivityObservation,
) -> Result<()> {
    if !previous.is_recent_for(guard) {
        bail!("independent connectivity evidence expired during refresh");
    }
    let tunnel = guard.observe_wireguard(runtime, identity).await?;
    if !previous.is_recent_for(guard) {
        bail!("independent connectivity evidence expired during tunnel inspection");
    }
    guard.renew_observed_lease(runtime, &tunnel).await
}

async fn drive<T>(
    probe: impl Future<Output = Result<T>> + Send,
    lease: &mut impl CheckedLease,
    heartbeat_interval: Duration,
    deadline: Instant,
) -> Result<T> {
    // The initial check occurs before polling the network probe. Keep one timer
    // and one probe. Rearm after completion so slow checks cannot accumulate
    // missed ticks or cause a burst of catch-up renewals.
    timeout_at(deadline, lease.renew())
        .await
        .context("connectivity refresh initial lease deadline")??;
    let mut next_check = Instant::now() + heartbeat_interval;
    tokio::pin!(probe);
    loop {
        tokio::select! {
            biased;
            _ = sleep_until(deadline) => bail!("connectivity refresh deadline"),
            result = &mut probe => return result,
            _ = sleep_until(next_check) => {
                timeout_at(deadline, lease.renew())
                    .await
                    .context("connectivity refresh lease deadline")??;
                next_check = Instant::now() + heartbeat_interval;
            }
        }
    }
}

pub(super) async fn refresh(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    profile: &FixedWorkerSpec,
    guard: &mut PacketGuardController,
    identity: &WireguardIdentity,
    policy: ProbePolicy,
    previous: Option<ConnectivityObservation>,
) -> Result<ConnectivityObservation> {
    let previous = previous.context("periodic refresh requires successful bootstrap evidence")?;
    if !previous.is_recent_for(guard)
        || previous.report.family != policy.family
        || (policy.require_warp && previous.report.warp == WarpState::Off)
    {
        bail!("periodic refresh evidence or provider policy differs");
    }
    let deadline = Instant::now() + REFRESH_DEADLINE;
    let binding = ProbeBinding::capture(guard, profile)?;
    // Verify the actual worker before the first renewal, not only inside the
    // concurrently polled network job. A desired namespace string is not proof.
    timeout_at(deadline, inspect_fixed_worker(runtime, handle, profile))
        .await
        .context("connectivity refresh worker inspection deadline")??;
    let heartbeat_interval =
        Duration::from_millis(u64::from(guard.profile().policy().lease_seconds) * 1000 / 3)
            .min(MAX_HEARTBEAT_INTERVAL);
    let probe = observe_fixed_worker(runtime, handle, profile, binding, policy);
    let sample = {
        let mut lease = LiveLease {
            runtime,
            guard,
            identity,
            previous: &previous,
        };
        drive(probe, &mut lease, heartbeat_interval, deadline).await?
    };
    // A successful reply cannot mask a concurrent change to the actor, tunnel,
    // epoch or namespace. Read the actual kernel again before returning it.
    timeout_at(deadline, async {
        let tunnel = guard.observe_wireguard(runtime, identity).await?;
        if !sample.is_recent_for(guard) {
            bail!("connectivity refresh no longer matches its owned packet controller");
        }
        guard.renew_observed_lease(runtime, &tunnel).await
    })
    .await
    .context("connectivity refresh final inspection deadline")??;
    Ok(sample)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn refresh_future_can_be_owned_by_the_server_supervisor_task() {
        fn require_send<T: Send>(_: T) {}
        let check = |runtime: &dyn RuntimeManager,
                     handle: &ContainerHandle,
                     profile: &FixedWorkerSpec,
                     guard: &mut PacketGuardController,
                     identity: &WireguardIdentity,
                     policy: ProbePolicy| {
            require_send(refresh(
                runtime, handle, profile, guard, identity, policy, None,
            ));
        };
        let _ = check;
    }

    #[derive(Default)]
    struct FixtureLease {
        calls: Vec<Instant>,
        fail_at: Option<usize>,
        delay_at: Option<(usize, Duration)>,
    }
    impl CheckedLease for FixtureLease {
        async fn renew(&mut self) -> Result<()> {
            self.calls.push(Instant::now());
            if self.fail_at == Some(self.calls.len()) {
                bail!("fixture tunnel health failed");
            }
            if let Some((call, delay)) = self.delay_at
                && call == self.calls.len()
            {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slow_probe_keeps_checking_and_renewing_without_borrowing_the_controller() -> Result<()>
    {
        let start = Instant::now();
        let mut lease = FixtureLease::default();
        let probe = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(42)
        };
        assert_eq!(
            drive(
                probe,
                &mut lease,
                MAX_HEARTBEAT_INTERVAL,
                start + REFRESH_DEADLINE
            )
            .await?,
            42
        );
        assert_eq!(
            lease
                .calls
                .iter()
                .map(|time| (*time - start).as_secs())
                .collect::<Vec<_>>(),
            [0, 3, 6, 9]
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn failed_tunnel_cancels_the_pending_probe_and_never_renews_again() {
        struct Cancellation(Arc<AtomicBool>);
        impl Drop for Cancellation {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = Cancellation(cancelled.clone());
        let probe = async move {
            let _cancellation = cancellation;
            std::future::pending::<Result<()>>().await
        };
        let mut lease = FixtureLease {
            fail_at: Some(2),
            ..Default::default()
        };
        let error = drive(
            probe,
            &mut lease,
            MAX_HEARTBEAT_INTERVAL,
            Instant::now() + REFRESH_DEADLINE,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("tunnel health failed"));
        assert_eq!(lease.calls.len(), 2);
        assert!(cancelled.load(Ordering::SeqCst));
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(lease.calls.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_probe_has_priority_over_a_simultaneously_due_heartbeat() {
        let start = Instant::now();
        let mut lease = FixtureLease::default();
        let probe = async {
            tokio::time::sleep(MAX_HEARTBEAT_INTERVAL).await;
            bail!("fixture DNS/TLS failure")
        };
        let error = drive::<()>(
            probe,
            &mut lease,
            MAX_HEARTBEAT_INTERVAL,
            start + REFRESH_DEADLINE,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("DNS/TLS failure"));
        assert_eq!(lease.calls.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_probe_and_stalled_lease_share_one_absolute_deadline() {
        for stall_at in [None, Some(1), Some(2)] {
            let start = Instant::now();
            let mut lease = FixtureLease {
                delay_at: stall_at.map(|call| (call, Duration::from_secs(90))),
                ..Default::default()
            };
            let error = drive(
                std::future::pending::<Result<()>>(),
                &mut lease,
                MAX_HEARTBEAT_INTERVAL,
                start + REFRESH_DEADLINE,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("deadline"));
            assert_eq!(Instant::now() - start, REFRESH_DEADLINE);
            assert_eq!(lease.calls.len(), stall_at.unwrap_or(15));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn missed_heartbeats_are_skipped_not_accumulated_into_catch_up_work() -> Result<()> {
        let start = Instant::now();
        let mut lease = FixtureLease {
            delay_at: Some((2, Duration::from_secs(8))),
            ..Default::default()
        };
        let probe = async {
            tokio::time::sleep(Duration::from_secs(13)).await;
            Ok(())
        };
        drive(
            probe,
            &mut lease,
            MAX_HEARTBEAT_INTERVAL,
            start + REFRESH_DEADLINE,
        )
        .await?;
        assert_eq!(
            lease
                .calls
                .iter()
                .map(|time| (*time - start).as_secs())
                .collect::<Vec<_>>(),
            [0, 3]
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn failed_initial_tunnel_check_does_not_poll_the_network_probe() {
        let polled = Arc::new(AtomicBool::new(false));
        let seen = polled.clone();
        let probe = async move {
            seen.store(true, Ordering::SeqCst);
            Ok(())
        };
        let mut lease = FixtureLease {
            fail_at: Some(1),
            ..Default::default()
        };
        assert!(
            drive(
                probe,
                &mut lease,
                MAX_HEARTBEAT_INTERVAL,
                Instant::now() + REFRESH_DEADLINE
            )
            .await
            .is_err()
        );
        assert!(!polled.load(Ordering::SeqCst));
    }
}
