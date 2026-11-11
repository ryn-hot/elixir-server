//! One opt-in empty-catalog participant per physical site. Reuses the same
//! VPN/guard/network and exact-identity retirement as the two-rig fixture.
use super::*;

const CATALOG_LEAD_SECONDS: u64 = 180;
const MIN_CATALOG_WARMUP: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Catalog,
    Consumer,
}

impl Role {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "catalog" => Ok(Self::Catalog),
            "consumer" => Ok(Self::Consumer),
            _ => anyhow::bail!("independent pilot role must be catalog or consumer"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Consumer => "consumer",
        }
    }

    fn check_reply(self, reply: WorkerControlOutcome) -> Result<()> {
        let WorkerControlOutcome::NetworkReply {
            response,
            control_bytes,
            ..
        } = reply
        else {
            anyhow::bail!("independent pilot did not receive an authenticated peer reply");
        };
        ensure!(
            control_bytes > 0,
            "independent pilot received no peer bytes"
        );
        match (self, response.response) {
            (
                Self::Catalog,
                PeerOutcome::Rejected {
                    code: RejectionCode::Unauthorized,
                },
            ) => Ok(()),
            (Self::Consumer, PeerOutcome::Search { page })
                if page.results.is_empty()
                    && page.next_cursor.is_none()
                    && !page.network_complete
                    && page.metadata_only =>
            {
                Ok(())
            }
            _ => anyhow::bail!("independent pilot received the wrong response for its role"),
        }
    }
}

struct Schedule {
    role: Role,
    topic: Digest,
    receipt: Digest,
    start_at: u64,
    network_at: u64,
}

struct Timing {
    peer_ready: Duration,
    catalog_warmup: Duration,
}

impl Schedule {
    fn parse(role: &str, nonce: &str, start_at: &str, current: u64) -> Result<Self> {
        let role = Role::parse(role)?;
        let nonce: Digest = nonce
            .parse()
            .context("independent pilot nonce must be a digest")?;
        ensure!(
            nonce.as_bytes().iter().any(|&byte| byte != 0),
            "zero pilot nonce"
        );
        let start_at: u64 = start_at
            .parse()
            .context("independent pilot start must be UTC epoch seconds")?;
        let network_at = match role {
            Role::Catalog => start_at
                .checked_sub(CATALOG_LEAD_SECONDS)
                .context("pilot catalog start underflows")?,
            Role::Consumer => start_at,
        };
        ensure!(
            (90..=300).contains(&network_at.saturating_sub(current)),
            "launch between 90 and 300 seconds before this role's network start"
        );
        let topic = Digest::of(
            [
                b"elixir-marketplace/offsite-steady-catalog/v2\0".as_slice(),
                nonce.as_bytes(),
                &start_at.to_be_bytes(),
            ]
            .concat(),
        );
        let receipt = Digest::of(
            [
                b"elixir-marketplace/offsite-receipt/v2\0".as_slice(),
                topic.as_bytes(),
                &start_at.to_be_bytes(),
            ]
            .concat(),
        );
        Ok(Self {
            role,
            topic,
            receipt,
            start_at,
            network_at,
        })
    }

    async fn wait_for_start(at: u64) -> Result<()> {
        let current = now()?;
        ensure!(current <= at, "missed the coordinated pilot start");
        tokio::time::sleep(Duration::from_secs(at - current)).await;
        ensure!(now()?.abs_diff(at) <= 5, "clock drift at coordinated start");
        Ok(())
    }

    async fn run(&self, rig: &mut Rig, guard_image: &str, worker_image: &str) -> Result<Timing> {
        rig.open(guard_image, worker_image)
            .await
            .context("independent protected startup")?;
        let catalog = self.role == Role::Catalog;
        if catalog {
            let database = rig.database.clone();
            rig.worker()?.reconcile_native_catalog(&database).await?;
        }
        ready(rig.worker()?).await?;
        Self::wait_for_start(self.network_at).await?;
        ready(rig.worker()?).await?;
        let began = tokio::time::Instant::now();
        let status = rig
            .worker()?
            .start_network(config(self.topic, catalog)?)
            .await?;
        ensure!(
            status.active
                && status.consumer
                && status.catalog == catalog
                && !status.routing
                && !status.adaptive,
            "independent pilot worker roles differ"
        );
        let (began, catalog_warmup) = if catalog {
            let warm = tokio::time::Instant::now();
            Self::wait_for_start(self.start_at).await?;
            let elapsed = warm.elapsed();
            ensure!(
                elapsed >= MIN_CATALOG_WARMUP,
                "catalog startup did not leave enough warmup time"
            );
            ready(rig.worker()?).await?;
            (tokio::time::Instant::now(), elapsed)
        } else {
            (began, Duration::ZERO)
        };
        let connected = timeout(READINESS_WINDOW.saturating_sub(began.elapsed()), async {
            loop {
                let status = rig.worker()?.network_status().await?;
                status.within_limits(&NetworkLimits::default())?;
                ensure!(
                    status.active
                        && status.consumer
                        && status.catalog == catalog
                        && !status.routing
                        && !status.adaptive,
                    "independent pilot lost its assigned role"
                );
                ensure!(
                    status.peers.len() <= 1,
                    "more than one peer joined the isolated pilot"
                );
                if let Some(peer) = status.peers.into_iter().next() {
                    return Ok::<_, anyhow::Error>(peer);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .context("independent pilot did not connect within the readiness window")??;
        let elapsed = began.elapsed();
        ensure!(
            elapsed < READINESS_WINDOW,
            "independent pilot missed its readiness window"
        );
        ready(rig.worker()?).await?;
        let response = rig.worker()?.request_peer(connected, search()).await?;
        self.role.check_reply(response.control_response)?;
        // Give the remote peer time to finish its opposite-direction check.
        // This does not retry discovery or extend the readiness window.
        tokio::time::sleep(Duration::from_secs(20)).await;
        ensure!(
            !rig.worker()?.stop_network().await?.active,
            "independent pilot role did not stop"
        );
        Ok(Timing {
            peer_ready: elapsed,
            catalog_warmup,
        })
    }
}

#[tokio::test]
#[ignore = "one free VPN identity; coordinate one independent operator/site with the opposite role; empty isolated test topic only"]
async fn independent_site_empty_catalog_pilot() -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_WAN_LIVE_TEST").as_deref() == Ok("1"),
        "explicit consent for one independent free-provider participant required"
    );
    let schedule = Schedule::parse(
        &std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_ROLE")?,
        &std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_NONCE")?,
        &std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_START_UTC")?,
        now()?,
    )?;
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker_image).await?;
    for (label, value) in [
        ("onion_transport", "2"),
        ("onion_fixture", "0"),
        ("wan_query_budget", "1"),
    ] {
        ensure!(
            labels
                .get(&format!("elixir.marketplace.worker.{label}"))
                .map(String::as_str)
                == Some(value),
            "independent pilot needs the ordinary bounded onion worker profile"
        );
    }
    let mut rig = Rig::new(runtime, &provider_image).await?;
    eprintln!(
        "{}",
        serde_json::json!({"stage":"independent_pilot_local_recovery",
        "deployment":rig.deployment,"recovery_root":rig.root.path()})
    );
    let outcome = std::panic::AssertUnwindSafe(schedule.run(&mut rig, &guard_image, &worker_image))
        .catch_unwind()
        .await;
    let retirement = rig.close().await;
    // Never emit a success receipt if operation or exact-identity cleanup fail.
    retirement.context("independent pilot same-identity retirement")?;
    let timing = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    eprintln!(
        "{}",
        serde_json::json!({"stage":"independent_empty_catalog_pilot_receipt",
        "schema_version":2,"run":schedule.receipt,"role":schedule.role.name(),
        "worker_image":worker_image,"guard_image":guard_image,"provider_image":provider_image,
        "peer_ready_ms":timing.peer_ready.as_millis(),
        "catalog_warmup_ms":timing.catalog_warmup.as_millis(),"authenticated_role_reply":true,
        "identity_revoked":true,"test_only_empty_catalog":true})
    );
    Ok(())
}

#[test]
fn independent_pilot_requires_bounded_time_and_separates_topic_from_receipt() -> Result<()> {
    let nonce = Digest::of(b"independent pilot unit test only").to_string();
    let a = Schedule::parse("catalog", &nonce, "1270", 1000)?;
    let b = Schedule::parse("consumer", &nonce, "1270", 1000)?;
    assert_eq!(a.network_at, 1090);
    assert_eq!(b.network_at, b.start_at);
    assert_eq!(a.start_at, b.start_at);
    assert_eq!(a.topic, b.topic);
    assert_eq!(a.receipt, b.receipt);
    assert_ne!(a.receipt, a.topic);
    assert_ne!(
        a.topic,
        Schedule::parse("catalog", &nonce, "1271", 1000)?.topic
    );
    assert_ne!(
        a.receipt,
        Schedule::parse("catalog", &nonce, "1271", 1000)?.receipt
    );
    for (role, nonce, time) in [
        ("routing", nonce.as_str(), "1270"),
        ("catalog", "not-a-digest", "1270"),
        ("catalog", nonce.as_str(), "1269"),
        ("catalog", nonce.as_str(), "1481"),
        ("catalog", nonce.as_str(), "1"),
        ("consumer", nonce.as_str(), "1089"),
        ("consumer", nonce.as_str(), "1301"),
    ] {
        assert!(Schedule::parse(role, nonce, time, 1000).is_err());
    }
    Ok(())
}
