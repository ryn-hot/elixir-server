//! Explicitly approved three-identity WAN qualification. Real authenticated
//! installation, installed archive sharing, public DHT/onion/Noise, and native
//! quarantine. TEST adoption is confined to the random test namespace.
use super::*;
use crate::marketplace::{
    quarantine::{QuarantinedSource, SourceMatchedQuarantine, SourceQuarantineJob},
    worker::PARTICIPATION_DISCLOSURE_VERSION,
};
use elixir_marketplace::{
    admission::CandidateDescriptor, quarantine::QuarantinePurpose, worker_network::PeerKey,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
mod catalog;

struct Participant {
    rig: Rig,
    state: Option<AppState>,
    app: Option<Router>,
    token: Option<String>,
    shutdown: CancellationToken,
    startup: Option<JoinHandle<Result<()>>>,
    installed_startup: Option<JoinHandle<Result<()>>>,
}
impl Participant {
    async fn new(runtime: Arc<dyn RuntimeManager>, image: &str) -> Result<Self> {
        Ok(Self {
            rig: Rig::new_layout(runtime, image, true).await?,
            state: None,
            app: None,
            token: None,
            shutdown: CancellationToken::new(),
            startup: None,
            installed_startup: None,
        })
    }
    fn state(&self) -> Result<&AppState> {
        self.state.as_ref().context("participant state missing")
    }
    fn app(&self) -> Result<&Router> {
        self.app.as_ref().context("participant router missing")
    }
    fn token(&self) -> Result<&str> {
        self.token.as_deref().context("participant bearer missing")
    }
    fn client(&self) -> Result<ParticipationClient> {
        self.state()?.marketplace.client()
    }

    async fn start(
        &mut self,
        guard: &str,
        worker: &str,
        topic: Digest,
        seed: bool,
        allocation: &Allocation,
    ) -> Result<()> {
        let root = self.rig.root.path().canonicalize()?;
        let url = format!("sqlite://{}", root.join("wan.sqlite").display());
        let (mut state, _, _, token) =
            crate::http::handlers::marketplace::installation_http_fixture(
                self.rig.database.clone(),
                &root,
                &url,
            )
            .await?;
        write_profile(&root, worker, allocation)?;
        let mut settings = state.settings.as_ref().clone();
        settings.marketplace.startup_file = Some(write_network_profile_with_network(
            &self.rig,
            guard,
            worker,
            // Startup may configure only consumer roles. Actual saved owner
            // disclosure/metering and live host policy decide contribution.
            config(topic, false)?,
        )?);
        settings.marketplace.installed_startup_file = Some(root.join("installed-startup.json"));
        settings.marketplace.installation_management = true;
        state.settings = Arc::new(settings);
        self.rig.secrets = state.secrets.clone();
        self.app = Some(crate::http::router(state.clone()));
        self.token = Some(token);
        self.state = Some(state);
        // Parse the closed trusted profile before a settings mutation or any
        // enrollment. Invalid static roles must not become an HTTP readback
        // failure after the preference transaction has already committed.
        crate::marketplace::startup::customer_provider_configuration(self.state()?).await?;
        let preference = Preferences {
            contribution: if seed {
                ContributionMode::On
            } else {
                ContributionMode::Off
            },
            metering: if seed {
                ConnectionMetering::Unmetered
            } else {
                ConnectionMetering::Metered
            },
            contribution_disclosure_version: seed.then_some(PARTICIPATION_DISCLOSURE_VERSION),
            ..Default::default()
        };
        let saved = http(self.app()?, self.token()?, "PATCH", "/api/v1/marketplace/settings",
            Some(json!({"expected_revision":1,"mutation_id":Uuid::new_v4(),"preferences":preference}))).await?;
        ensure!(
            saved["settings"]["revision"] == 2
                && saved["settings"]["preferences"] == json!(preference),
            "owner contribution choice was not saved"
        );
        self.rig.application = Some(self.state()?.marketplace.clone());
        self.rig.enrollment_started = true;
        self.startup = Some(
            crate::marketplace::startup::spawn_configured(self.state()?, self.shutdown.clone())
                .context("original configured startup missing")?,
        );
        // Match main: networking does not start retained-extension restoration,
        // installed sharing or automatic updates. The separate startup owns
        // those services. Join its original task before racing an installation
        // against the empty-database restoration barrier.
        self.installed_startup = Some(
            crate::marketplace::installed_startup::spawn_configured(
                self.state()?,
                self.shutdown.clone(),
            )
            .context("original installed startup missing")?,
        );
        let installed = self.installed_startup.as_mut().unwrap().await;
        self.installed_startup = None;
        installed.context("original installed startup join")??;
        let until = tokio::time::Instant::now() + Duration::from_secs(600);
        loop {
            if self.state()?.marketplace.installation_review_available() {
                break;
            }
            ensure!(
                !self.startup.as_ref().unwrap().is_finished(),
                "original participant startup terminated"
            );
            ensure!(
                tokio::time::Instant::now() < until,
                "original participant startup deadline"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        source_ready(&self.client()?).await?;
        self.roles(seed, 2).await?;
        eprintln!(
            "{}",
            json!({"stage":"holder_participant_ready","seeder":seed})
        );
        Ok(())
    }
    async fn roles(&self, seed: bool, revision: i64) -> Result<()> {
        let client = self.client()?;
        // A recovered contribution owner earns ten healthy minutes. Consumers
        // need not wait for this; do not weaken the real policy for a WAN test.
        let until = tokio::time::Instant::now() + Duration::from_secs(if seed { 900 } else { 180 });
        let mut report = tokio::time::Instant::now();
        loop {
            let state = client.snapshot();
            if state.phase == ParticipationPhase::Running
                && state.preferences_revision == Some(revision)
                && state
                    .applied_roles
                    .is_some_and(|r| r.consumer && r.catalog == seed && !r.routing)
            {
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "actual contribution roles did not apply: {state:?}"
            );
            if tokio::time::Instant::now() >= report {
                eprintln!(
                    "{}",
                    json!({"stage":"holder_contribution_wait","seeder":seed,
                    "phase":state.phase,"blockers":state.blockers,
                    "recovery_seconds":state.recovery_seconds})
                );
                report = tokio::time::Instant::now() + Duration::from_secs(15);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    async fn stop(&mut self) -> Result<()> {
        self.shutdown.cancel();
        let startup = match self.startup.take() {
            Some(task) => task
                .await
                .context("original participant startup join")
                .and_then(|r| r),
            None => Ok(()),
        };
        let installed = match self.installed_startup.take() {
            Some(task) => task
                .await
                .context("original installed startup join")
                .and_then(|result| result),
            None => Ok(()),
        };
        let application = match &self.state {
            Some(state) => state.marketplace.shutdown().await,
            None => Ok(()),
        };
        if let Err(error) = &startup {
            eprintln!("Original holder startup retirement failed: {error:#}");
        }
        if let Err(error) = &installed {
            eprintln!("Original holder installed startup retirement failed: {error:#}");
        }
        application.and(startup).and(installed)
    }
    async fn close(&mut self) -> Result<()> {
        let stopped = self.stop().await;
        // Always attempt SAME-identity recovery/revocation, even after a failed
        // operation or stop. Never replace an uncertain enrollment with a new one.
        let retired = self.rig.close().await;
        if let Err(error) = &retired {
            eprintln!("Original holder identity retirement failed: {error:#}");
        }
        self.rig.database.pool.close().await;
        self.app = None;
        self.state = None;
        self.token = None;
        if stopped.is_err() || retired.is_err() {
            self.rig.root.preserve();
        }
        match (stopped, retired) {
            (Ok(()), result) => result,
            (Err(first), Ok(())) => Err(first),
            (Err(first), Err(cleanup)) => {
                Err(first.context(format!("same-identity cleanup also failed: {cleanup:#}")))
            }
        }
    }
    async fn seeding(&self, instance: Uuid, source: &SourceFixture, enable: bool) -> Result<()> {
        let extension = &source.publication.release.payload.extension_id;
        let current = http(
            self.app()?,
            self.token()?,
            "GET",
            &publisher_view_path(extension, instance),
            None,
        )
        .await?;
        let action = if enable {
            json!({"type":"enable","publication":source.publication,
            "accepted_reference":Digest::of(source.publication.to_bytes()?),"accept_unqualified_seeding":true})
        } else {
            json!({"type":"disable"})
        };
        let saved = http(self.app()?, self.token()?, "POST", "/api/v1/marketplace/publisher-seeding",
            Some(json!({"schema_version":1,"instance_id":instance,"extension_id":extension,
                "expected_revision":current["revision"],"mutation_id":Uuid::new_v4(),"action":action}))).await?;
        ensure!(
            saved["enabled"] == enable,
            "exact publisher-seeding choice did not apply"
        );
        if enable {
            // A saved consent row is not a running contributor. Wait for the
            // actual protected owner's nonempty installed-sharing lease before
            // publishing a test offer; the download still proves exact bytes.
            let until = tokio::time::Instant::now() + Duration::from_secs(60);
            while !self.client()?.snapshot().package_sharing_active {
                ensure!(
                    tokio::time::Instant::now() < until,
                    "original installed archive never acquired its sharing lease"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            eprintln!("{}", json!({"stage":"holder_installed_sharing_ready"}));
        }
        Ok(())
    }
}

fn publisher_view_path(
    extension: &elixir_marketplace::identity::ExtensionId,
    instance: Uuid,
) -> String {
    // ExtensionId's closed alphabet contains ':' and '/'. Encode those path
    // separators before Axum decodes the single identity parameter.
    let encoded = extension.as_str().replace(':', "%3A").replace('/', "%2F");
    format!("/api/v1/marketplace/publisher-seeding/{encoded}/{instance}")
}

#[tokio::test]
async fn publisher_view_path_roundtrips_the_scoped_extension_identity() -> Result<()> {
    let extension: elixir_marketplace::identity::ExtensionId =
        format!("elx:{}/fixture", "a".repeat(52)).parse()?;
    let instance = Uuid::new_v4();
    let app =
        Router::new().route(
            "/api/v1/marketplace/publisher-seeding/:extension/:instance",
            axum::routing::get(
                |axum::extract::Path((extension, instance)): axum::extract::Path<(
                    String,
                    Uuid,
                )>| async move {
                    axum::Json(json!({"extension":extension,"instance":instance}))
                },
            ),
        );
    let reply = http(
        &app,
        "test-only",
        "GET",
        &publisher_view_path(&extension, instance),
        None,
    )
    .await?;
    ensure!(
        reply == json!({"extension":extension,"instance":instance}),
        "publisher path changed scoped identity"
    );
    Ok(())
}

fn consent() -> Result<()> {
    ensure!(
        [
            "ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST",
            "ELIXIR_MARKETPLACE_WAN_LIVE_TEST",
            "ELIXIR_MARKETPLACE_PUBLIC_SOURCE_LIVE_TEST",
            "ELIXIR_MARKETPLACE_PUBLIC_INSTALL_LIVE_TEST",
            "ELIXIR_MARKETPLACE_THREE_PARTICIPANT_LIVE_TEST",
        ]
        .iter()
        .all(|name| std::env::var(name).as_deref() == Ok("1")),
        "explicit approved three-identity synthetic-only test consent required"
    );
    Ok(())
}

async fn discover(actors: &[Participant]) -> Result<(PeerKey, PeerKey)> {
    let until = tokio::time::Instant::now() + Duration::from_secs(600);
    let mut refresh = tokio::time::Instant::now();
    loop {
        let mut peers = Vec::new();
        for actor in actors {
            let status = actor.client()?.network_status().await?;
            status.within_limits(&NetworkLimits::default())?;
            peers.push(status.peers);
        }
        if peers.iter().all(|p| p.len() == 2) {
            // Own keys are inferred only from the three actually authenticated
            // peer sets; no private known-peer fixture/bootstrap is injected.
            let a = peers[1]
                .iter()
                .filter(|p| peers[2].contains(p))
                .collect::<Vec<_>>();
            let b = peers[0]
                .iter()
                .filter(|p| peers[2].contains(p))
                .collect::<Vec<_>>();
            ensure!(
                a.len() == 1 && b.len() == 1 && a[0] != b[0],
                "independent peer sets differ"
            );
            return Ok((a[0].clone(), b[0].clone()));
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "original three-peer DHT discovery deadline"
        );
        if tokio::time::Instant::now() >= refresh {
            for actor in actors {
                let _ = actor.client()?.refresh_network().await;
            }
            refresh = tokio::time::Instant::now() + Duration::from_secs(10);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn received_descriptor(
    client: &ParticipationClient,
    peer: PeerKey,
    source: &SourceFixture,
) -> Result<CandidateDescriptor> {
    let response = client
        .request_peer(
            peer,
            PeerOperation::Search {
                query: SearchQuery {
                    text: source.publication.release.payload.extension_id.to_string(),
                    tags: vec![],
                    limit: 1,
                },
                cursor: None,
            },
        )
        .await?;
    let WorkerControlOutcome::NetworkReply {
        response,
        control_bytes,
        ..
    } = response.control_response
    else {
        bail!("holder did not return an authenticated network reply");
    };
    ensure!(control_bytes > 0, "holder network reply was not accounted");
    let PeerOutcome::Search { page } = response.response else {
        bail!("holder did not serve authenticated search");
    };
    ensure!(
        page.results.len() == 1 && page.metadata_only && !page.network_complete,
        "holder search page differs"
    );
    let descriptor = page.results.into_iter().next().unwrap().descriptor;
    ensure!(
        Digest::of(elixir_marketplace::canonical::to_vec(
            &descriptor.release.payload
        )?) == source.matched.release_id,
        "holder advertised a different release"
    );
    Ok(descriptor)
}

#[test]
fn three_participant_test_requires_explicit_consent_before_any_enrollment() {
    // A normal unit run must never enroll, create native actors or read fixture
    // secrets. The actual ignored test calls this gate before image/layout I/O.
    if std::env::var("ELIXIR_MARKETPLACE_THREE_PARTICIPANT_LIVE_TEST").as_deref() != Ok("1") {
        assert!(consent().is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit approved three temporary VPN identities, genuine installed sharing and public DHT/onion/Noise; TEST adoption, tiny-source between-slot failover only"]
async fn two_installed_holders_deliver_to_a_fresh_consumer_and_recover_after_holder_loss()
-> Result<()> {
    consent()?; // MUST precede fixture access, layout creation and enrollment.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn,elixir_server::marketplace::lifecycle=debug")
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    let (descriptor, source) = super::super::cold_page::holder_fixture().await?;
    let provider = image()?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker).await?;
    ensure!(
        [
            ("elixir.marketplace.worker.blob_sharing", "2"),
            ("elixir.marketplace.worker.onion_transport", "2"),
            ("elixir.marketplace.worker.wan_query_budget", "1")
        ]
        .iter()
        .all(|(k, v)| labels.get(*k).map(String::as_str) == Some(*v))
            && !labels.keys().any(|key| key.contains("private_peer_fixture")
                || key.contains("source_batch_bridge_fixture")),
        "ordinary immutable sharing/onion worker required before enrollment"
    );
    let topic = Digest::of(Uuid::new_v4().as_bytes());
    let mut allocation = crate::marketplace::application::fixture_allocation();
    allocation.source.cpu_millicores = 200;
    allocation.source.wall_seconds = 600;
    allocation.validate()?;
    let mut actors = Vec::new();
    let mut job: Option<SourceQuarantineJob> = None;
    let mut quarantined: Option<QuarantinedSource> = None;
    let mut matched: Option<SourceMatchedQuarantine> = None;
    // All construction is inside the joined outcome so a later participant's
    // setup failure cannot skip cleanup of earlier namespaces/identities.
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        for n in 0..3 {
            actors.push(Participant::new(runtime.clone(), &provider).await?);
            eprintln!("{}", json!({"stage":"holder_participant_layout","participant":n + 1,
                "deployment":actors[n].rig.deployment,"recovery_root":actors[n].rig.root.path()}));
            actors[n].start(&guard, &worker, topic, n < 2, &allocation).await?;
            if n < 2 {
                let actor = &actors[n];
                let instance = install_with_updates(actor.app()?, actor.state()?, actor.token()?,
                    &source, None, true, false).await?;
                actor.seeding(instance, &source, true).await?;
                let reply = actor.client()?.request(PeerOperation::Offer { descriptor: Box::new(descriptor.clone()) }).await?;
                ensure!(matches!(reply.response, PeerOutcome::Offered { release_id } if release_id == source.matched.release_id),
                    "test catalog rejected the exact approved descriptor");
            }
        }
        ensure!(actors.iter().map(|a| &a.rig.deployment).collect::<std::collections::BTreeSet<_>>().len() == 3,
            "independent owned deployments required");
        let consumer = actors[2].client()?;
        let installed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_grants")
            .fetch_one(&actors[2].rig.database.pool).await?;
        ensure!(installed == 0, "fresh consumer unexpectedly has installed sources");
        let (a, b) = discover(&actors).await?;
        let received = received_descriptor(&consumer, a.clone(), &source).await?;
        let second = received_descriptor(&consumer, b.clone(), &source).await?;
        ensure!(elixir_marketplace::canonical::to_vec(&received.release)?
            == elixir_marketplace::canonical::to_vec(&second.release)?, "holders disagree on signed release");
        // These are REAL consumer-side native checks of peer data and live
        // public source, not host-created positive observations.
        let metadata = consumer.verify_metadata(&received).await?;
        let admitted_source = consumer.preflight_fresh_metadata(&metadata, 0).await?;
        ensure!(admitted_source.release_id() == source.matched.release_id, "initial public source differs");
        consumer.open_source_quarantine_retained(&mut job, &metadata, None, &allocation.source, None).await?;
        let original_volume = job.as_ref().context("original quarantine missing")?.plan_workspace().release_id;
        let original_deadline = job.as_ref().unwrap().expires();
        // A pristine third root plus ordinary network-only receiver prevents
        // either a local installed archive or fixture sender from passing.
        job.as_mut().unwrap().receive_peer_owned(&consumer, a.clone(), QuarantinePurpose::Package).await?;
        eprintln!("{}", json!({"stage":"real_holder_package_received","bytes":source.package.len(),"holder":1}));
        actors[0].stop().await?;
        ensure!(actors[0].client().is_err(), "stopped holder retained an active client");
        let failed = job.as_mut().unwrap().receive_peer_owned(&consumer, a, QuarantinePurpose::Proof).await;
        ensure!(failed.is_err(), "offline holder unexpectedly delivered proof");
        // receive_peer_owned joins the original transfer's cancellation before
        // returning. Do not replace the job or refund/recreate its counters.
        ensure!(job.as_ref().unwrap().expires() == original_deadline
            && job.as_ref().unwrap().plan_workspace().release_id == original_volume,
            "holder loss replaced or renewed the original job");
        job.as_mut().unwrap().receive_peer_owned(&consumer, b.clone(), QuarantinePurpose::Proof).await?;
        quarantined = Some(SourceQuarantineJob::finish_retained(&mut job).await?);
        matched = Some(QuarantinedSource::match_retained(&mut quarantined).await?);
        // Match current public source after the interrupted network operation,
        // too; a long peer wait cannot turn an expired initial fact into trust.
        let current = consumer.verify_metadata(&received).await?;
        let public_source = consumer.preflight_fresh_metadata(&current, 0).await?;
        matched.as_mut().unwrap().refresh_public_source(public_source)?;
        matched.as_ref().unwrap().require_public_source()?;
        ensure!(matched.as_ref().unwrap().summary().release_id == source.matched.release_id
            && matched.as_ref().unwrap().summary().commit == source.matched.commit,
            "actual peer bytes did not match exact package/Git/public source");
        actors[2].roles(false, 2).await?;
        // Withdraw the last holder through its normal durable owner setting.
        let off = http(actors[1].app()?, actors[1].token()?, "PATCH", "/api/v1/marketplace/settings",
            Some(json!({"expected_revision":2,"mutation_id":Uuid::new_v4(),"preferences":Preferences {
                contribution:ContributionMode::Off,metering:ConnectionMetering::Metered,..Default::default()
            }}))).await?;
        ensure!(off["settings"]["revision"] == 3, "holder opt-out was not saved");
        actors[1].roles(false, 3).await?;
        let denied = consumer.request_peer(b, search()).await;
        let refused = match denied {
            Err(_) => true,
            Ok(reply) => matches!(reply.control_response, WorkerControlOutcome::NetworkReply {response, ..}
                if matches!(response.response, PeerOutcome::Rejected {code:RejectionCode::Unauthorized})),
        };
        ensure!(refused, "opted-out holder continued serving metadata");
        eprintln!("{}", json!({"stage":"protected_holder_failover_verified","holders":2,"consumer_only":true,
            "package_from_first_holder":true,"proof_from_second_after_first_stopped":true,
            "same_quarantine_deadline":true,"source_git_license_equal":true,"last_holder_opt_out_applied":true,
            "simulated_paid_support_only":true,"mid_body_interruption_qualified":false}));
        Ok::<(), anyhow::Error>(())
    })).catch_unwind().await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("Original protected-holder operation failed: {error:#}");
    }
    let mut cleanup_errors = Vec::new();
    if let Some(value) = matched.as_mut() {
        if let Err(e) = value.discard_retained().await {
            cleanup_errors.push(format!("matched quarantine: {e:#}"));
        } else {
            matched = None;
        }
    }
    if let Some(value) = quarantined.as_mut() {
        if let Err(e) = value.discard_retained().await {
            cleanup_errors.push(format!("quarantine: {e:#}"));
        } else {
            quarantined = None;
        }
    }
    if let Some(value) = job.as_mut() {
        if let Err(e) = value.discard_retained().await {
            cleanup_errors.push(format!("source job: {e:#}"));
        } else {
            job = None;
        }
    }
    for (n, actor) in actors.iter_mut().enumerate() {
        if let Err(e) = actor.close().await {
            cleanup_errors.push(format!("participant {}: {e:#}", n + 1));
        }
    }
    for error in &cleanup_errors {
        eprintln!("Protected-holder cleanup failure: {error}");
    }
    if !cleanup_errors.is_empty() || !outcome.as_ref().is_ok_and(|v| v.is_ok()) {
        for actor in &actors {
            actor.rig.root.preserve();
        }
    }
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => {
            return Err(error.context(format!("cleanup failures: {}", cleanup_errors.len())));
        }
        Ok(Ok(())) => ensure!(
            cleanup_errors.is_empty(),
            "same-identity cleanup failures: {}",
            cleanup_errors.join("; ")
        ),
    }
    eprintln!(
        "{}",
        json!({"stage":"protected_holder_test_complete","same_identities_revoked":3,
        "production_enabled":false,"excludes":"mid-body/large-package failover, real billing, separate physical sites, playback and scale/soak"})
    );
    Ok(())
}
