//! Deterministic scheduling only, not evidence of native VPN protection.
use super::super::tests::fixture;
use super::*;

fn checking(sender: &watch::Sender<Snapshot>, seconds: u64) {
    sender.send_replace(Snapshot {
        state: ProtectionState::Checking,
        until: Some(Instant::now() + Duration::from_secs(seconds)),
    });
}

#[tokio::test(start_paused = true)]
async fn healthy_checking_defers_only_before_native_launch() -> Result<()> {
    let f = fixture()?;
    checking(&f.protection, 60);
    let error = Work::Background.admit_start(&f.owner).unwrap_err();
    assert!(verification_deferred(&error));
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    for before_launch in [false, true] {
        let (mut completion, _reply) = oneshot::channel::<Result<()>>();
        let error = if before_launch {
            Work::Background
                .ready_before_launch(&f.owner, &mut completion)
                .await
        } else {
            Work::Background.ready(&f.owner, &mut completion).await
        }
        .unwrap_err();
        assert_eq!(verification_deferred(&error), before_launch);
    }
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(15)),
    });
    Work::Background.admit_start(&f.owner)?;
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn checking_cannot_disguise_revocation_pressure_staleness_or_owner_failure() -> Result<()> {
    for change in 0..13 {
        let f = fixture()?;
        checking(&f.protection, 60);
        match change {
            0 => f.owner.failed.store(true, Ordering::SeqCst),
            1 => f
                .settings
                .send_modify(|s| s.accepted_disclosure_version = None),
            2 => f.settings.send_modify(|s| s.metering = Metering::Unknown),
            3 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().resource_pressure = true),
            4 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().playback_pressure = true),
            5 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().background_allowance = false),
            6 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().available_memory = None),
            7 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().disk_headroom = false),
            8 => {
                f.facts.send_replace(None);
            }
            9 => tokio::time::advance(Duration::from_secs(31)).await,
            10 => drop(f.settings),
            11 => drop(f.facts),
            12 => drop(f.protection),
            _ => unreachable!(),
        }
        let error = Work::Background.admit_start(&f.owner).unwrap_err();
        assert!(!verification_deferred(&error), "change {change}");
        assert_eq!(f.owner.slots.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn only_background_slot_contention_is_a_prelaunch_deferral() -> Result<()> {
    let f = fixture()?;
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    for work in [Work::Background, Work::Foreground, Work::Restrictions] {
        let error = try_slot(&f.owner, work, "source/verification slot is busy").unwrap_err();
        assert_eq!(
            verification_deferred(&error),
            matches!(work, Work::Background | Work::Restrictions)
        );
        assert_eq!(error.to_string(), "source/verification slot is busy");
    }
    drop(permit);
    drop(try_slot(&f.owner, Work::Background, "unused")?);
    f.owner.slots.close();
    let error = try_slot(
        &f.owner,
        Work::Background,
        "source/verification slot is busy",
    )
    .unwrap_err();
    assert!(!verification_deferred(&error));
    assert!(!verification_deferred(&anyhow::anyhow!(
        "background source preflight is not admitted"
    )));
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn local_restrictions_need_no_network_disclosure_or_metering_but_keep_background_priority()
-> Result<()> {
    let f = fixture()?;
    f.settings.send_modify(|s| {
        s.accepted_disclosure_version = None;
        s.metering = Metering::Unknown;
    });
    Work::Restrictions.admit_start(&f.owner)?;
    assert!(Work::Background.admit_start(&f.owner).is_err());
    checking(&f.protection, 60);
    Work::Restrictions.admit_start(&f.owner)?;
    assert!(Work::Restrictions.admit(&f.owner, true).is_err());
    assert_eq!(f.owner.slots.available_permits(), 1);
    for change in 0..9 {
        let f = fixture()?;
        match change {
            0 => f.owner.failed.store(true, Ordering::SeqCst),
            1 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().resource_pressure = true),
            2 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().playback_pressure = true),
            3 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().background_allowance = false),
            4 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().available_memory = None),
            5 => f
                .facts
                .send_modify(|v| v.as_mut().unwrap().disk_headroom = false),
            6 => {
                f.facts.send_replace(None);
            }
            7 => {
                tokio::time::advance(Duration::from_secs(31)).await;
            }
            8 => {
                drop(f.protection);
            }
            _ => unreachable!(),
        }
        assert!(
            Work::Restrictions.admit_start(&f.owner).is_err(),
            "change {change}"
        );
        assert_eq!(f.owner.slots.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn local_restrictions_wait_for_the_same_ready_event_before_launch_or_input() -> Result<()> {
    for before_launch in [false, true] {
        let f = fixture()?;
        f.settings.send_modify(|s| {
            s.accepted_disclosure_version = None;
            s.metering = Metering::Unknown;
        });
        checking(&f.protection, 60);
        Work::Restrictions.admit_start(&f.owner)?;
        assert!(Work::Restrictions.admit(&f.owner, true).is_err());
        let permit = try_slot(&f.owner, Work::Restrictions, "restriction slot busy")?;
        let (mut completion, _response) = oneshot::channel::<Result<()>>();
        let mut wait = Box::pin(ready_for_phase(
            Work::Restrictions,
            &f.owner,
            &mut completion,
            before_launch,
        ));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        assert_eq!(f.owner.slots.available_permits(), 0);
        f.protection.send_replace(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(15)),
        });
        wait.await?;
        Work::Restrictions.admit(&f.owner, true)?;
        assert!(!f.owner.failed.load(Ordering::SeqCst));
        drop(permit);
        assert_eq!(f.owner.slots.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn local_restriction_waits_cannot_renew_the_existing_twelve_second_bound() -> Result<()> {
    for before_launch in [false, true] {
        let f = fixture()?;
        checking(&f.protection, 60);
        let (mut completion, _response) = oneshot::channel::<Result<()>>();
        let mut wait = Box::pin(ready_for_phase(
            Work::Restrictions,
            &f.owner,
            &mut completion,
            before_launch,
        ));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(3)).await;
            checking(&f.protection, 60);
            assert!(futures_util::poll!(wait.as_mut()).is_pending());
        }
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            wait.await
                .unwrap_err()
                .to_string()
                .contains("readiness deadline")
        );
        assert!(Work::Restrictions.admit(&f.owner, true).is_err());
        assert!(!f.owner.failed.load(Ordering::SeqCst));
        assert_eq!(f.owner.slots.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn local_restriction_waits_keep_cancellation_background_pressure_and_owner_fences()
-> Result<()> {
    for before_launch in [false, true] {
        for change in 0..14 {
            let f = fixture()?;
            checking(&f.protection, 60);
            let (mut completion, response) = oneshot::channel::<Result<()>>();
            let mut wait = Box::pin(ready_for_phase(
                Work::Restrictions,
                &f.owner,
                &mut completion,
                before_launch,
            ));
            assert!(futures_util::poll!(wait.as_mut()).is_pending());
            match change {
                0 => drop(response),
                1 => f
                    .protection
                    .send_modify(|s| s.state = ProtectionState::Failed),
                2 => f
                    .protection
                    .send_modify(|s| s.state = ProtectionState::Stopped),
                3 => f
                    .protection
                    .send_modify(|s| s.state = ProtectionState::Expired),
                4 => f
                    .facts
                    .send_modify(|v| v.as_mut().unwrap().resource_pressure = true),
                5 => f
                    .facts
                    .send_modify(|v| v.as_mut().unwrap().playback_pressure = true),
                6 => f
                    .facts
                    .send_modify(|v| v.as_mut().unwrap().background_allowance = false),
                7 => f
                    .facts
                    .send_modify(|v| v.as_mut().unwrap().available_memory = None),
                8 => f
                    .facts
                    .send_modify(|v| v.as_mut().unwrap().disk_headroom = false),
                9 => drop(f.facts),
                10 => drop(f.settings),
                11 => drop(f.protection),
                12 => {
                    f.facts.send_replace(None);
                }
                13 => f.facts.send_modify(|v| {
                    v.as_mut().unwrap().observed = Instant::now() - Duration::from_secs(31)
                }),
                _ => unreachable!(),
            }
            assert!(
                wait.await.is_err(),
                "before_launch={before_launch} change={change}"
            );
            assert!(!f.owner.failed.load(Ordering::SeqCst));
            assert_eq!(f.owner.slots.available_permits(), 1);
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn prelaunch_wait_survives_a_health_check_longer_than_a_native_renewal() -> Result<()> {
    let f = fixture()?;
    checking(&f.protection, 72);
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let mut wait = Box::pin(Work::Foreground.ready_before_launch(&f.owner, &mut completion));
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(20)).await;
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
    assert_eq!(f.owner.slots.available_permits(), 0);
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(15)),
    });
    wait.await?;
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    drop(permit);
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn prelaunch_health_observations_cannot_renew_the_original_72_second_budget() -> Result<()> {
    let f = fixture()?;
    checking(&f.protection, 72);
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let mut wait = Box::pin(Work::Foreground.ready_before_launch(&f.owner, &mut completion));
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
    for _ in 0..7 {
        tokio::time::advance(Duration::from_secs(10)).await;
        checking(&f.protection, 72);
        f.facts
            .send_modify(|facts| facts.as_mut().unwrap().observed = Instant::now());
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
    }
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        wait.await
            .unwrap_err()
            .to_string()
            .contains("readiness deadline")
    );
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

async fn ready_for_phase<T>(
    work: Work,
    owner: &Owner,
    completion: &mut oneshot::Sender<Result<T>>,
    before_launch: bool,
) -> Result<()> {
    if before_launch {
        work.ready_before_launch(owner, completion).await
    } else {
        work.ready(owner, completion).await
    }
}

#[tokio::test(start_paused = true)]
async fn foreground_waits_on_the_same_slot_but_background_defers_immediately() -> Result<()> {
    let f = fixture()?;
    checking(&f.protection, 60);
    Work::Foreground.admit_start(&f.owner)?;
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let mut wait = Box::pin(Work::Foreground.ready(&f.owner, &mut completion));
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
    assert_eq!(f.owner.slots.available_permits(), 0);
    let (mut background, _reply) = oneshot::channel::<Result<()>>();
    assert!(
        Work::Background
            .ready(&f.owner, &mut background)
            .await
            .is_err()
    );
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(15)),
    });
    wait.await?;
    assert_eq!(f.owner.slots.available_permits(), 0);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    drop(permit);
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn checking_observations_cannot_extend_the_fixed_readiness_deadline() -> Result<()> {
    let f = fixture()?;
    checking(&f.protection, 60);
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let mut wait = Box::pin(Work::Foreground.ready(&f.owner, &mut completion));
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(3)).await;
        checking(&f.protection, 60);
        f.facts
            .send_modify(|facts| facts.as_mut().unwrap().observed = Instant::now());
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
    }
    tokio::time::advance(Duration::from_secs(3)).await;
    let error = wait.await.unwrap_err();
    assert!(error.to_string().contains("readiness deadline"));
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn earlier_protection_expiry_ends_the_wait_without_granting_readiness() -> Result<()> {
    for before_launch in [false, true] {
        let f = fixture()?;
        checking(&f.protection, 1);
        let (mut completion, _response) = oneshot::channel::<Result<()>>();
        let mut wait = Box::pin(ready_for_phase(
            Work::Foreground,
            &f.owner,
            &mut completion,
            before_launch,
        ));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(wait.await.is_err());
        assert!(Work::Foreground.admit_start(&f.owner).is_err());
        assert!(!f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn checking_wait_rechecks_cancellation_consent_resources_and_owner_loss() -> Result<()> {
    for before_launch in [false, true] {
        for change in 0..9 {
            let f = fixture()?;
            checking(&f.protection, 60);
            let (mut completion, response) = oneshot::channel::<Result<()>>();
            let mut wait = Box::pin(ready_for_phase(
                Work::Foreground,
                &f.owner,
                &mut completion,
                before_launch,
            ));
            assert!(futures_util::poll!(wait.as_mut()).is_pending());
            match change {
                0 => drop(response),
                1 => f
                    .protection
                    .send_modify(|snapshot| snapshot.state = ProtectionState::Failed),
                2 => f
                    .protection
                    .send_modify(|snapshot| snapshot.state = ProtectionState::Stopped),
                3 => f
                    .protection
                    .send_modify(|snapshot| snapshot.state = ProtectionState::Expired),
                4 => f.settings.send_modify(|settings| {
                    settings.mode = elixir_marketplace::policy::ContributionMode::Auto;
                    settings.accepted_disclosure_version = None;
                }),
                5 => f.facts.send_modify(|facts| {
                    facts.as_mut().unwrap().foreground_resource_pressure = true
                }),
                6 => f
                    .facts
                    .send_modify(|facts| facts.as_mut().unwrap().available_memory = None),
                7 => drop(f.protection),
                8 => drop(f.facts),
                _ => unreachable!(),
            }
            assert!(wait.await.is_err(), "change {change}");
            assert!(!f.owner.failed.load(Ordering::SeqCst));
        }
    }
    Ok(())
}

#[tokio::test]
async fn optional_background_pressure_does_not_veto_foreground_but_severe_pressure_does()
-> Result<()> {
    let f = fixture()?;
    f.settings.send_modify(|settings| {
        settings.mode = elixir_marketplace::policy::ContributionMode::Auto;
    });
    Work::Background.admit(&f.owner, true)?;
    Work::Foreground.admit(&f.owner, true)?;
    f.facts
        .send_modify(|facts| facts.as_mut().unwrap().resource_pressure = true);
    assert!(Work::Background.admit(&f.owner, true).is_err());
    Work::Foreground.admit(&f.owner, true)?;
    assert!(Client(f.owner.clone()).catalog_preparation_ready()?);
    f.facts.send_modify(|facts| {
        facts.as_mut().unwrap().foreground_resource_pressure = true;
    });
    assert!(Work::Background.admit(&f.owner, true).is_err());
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    assert!(!Client(f.owner.clone()).catalog_preparation_ready()?);
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}
