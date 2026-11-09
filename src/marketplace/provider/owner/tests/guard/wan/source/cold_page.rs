//! Real protected public source + actual search service. Test-only issuer
//! credentials stay in this consumer-only fixture, never public serving.
//! Measures backend search, not desktop rendering, catalog download or billing.
use super::*;
use crate::marketplace::{
    search::{SearchPhase, SearchRequest, SearchService},
    worker::SearchSourceObservation,
};
use elixir_marketplace::{
    admission::CandidateDescriptor,
    canonical,
    evidence::IssuerPolicy,
    identity::ExtensionId,
    records::{MediaTag, RuntimeProfile, SignatureCache},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

const COMMIT: &str = "09d9c040aaf015b37b6bf13037ce18f0fb6abbba";
const WARM_SAMPLES: usize = 32;
mod desktop;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    schema_version: u32,
    fixture_only: bool,
    simulated_paid_support: bool,
    commit: String,
    descriptors: Vec<CandidateDescriptor>,
}
struct Input {
    descriptors: Vec<CandidateDescriptor>,
    matched: Vec<SourceMatchSummary>,
}
// Reuse the exact approved snapshots and independent equality checks. Only one
// tiny source is offered by the separately consented three-participant test.
pub(super) async fn holder_fixture() -> Result<(CandidateDescriptor, SourceFixture)> {
    let input = input().await?;
    let descriptor = input
        .descriptors
        .into_iter()
        .next()
        .context("approved descriptor missing")?;
    let matched = input
        .matched
        .into_iter()
        .next()
        .context("approved source equality missing")?;
    let directory = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_SOURCE_BATCH_FIXTURE")?)
        .canonicalize()?
        .join("releases/cold-page-01");
    let publication = PreparedPublication::parse(&bounded(
        &directory.join("publication.json"),
        MAX_PUBLICATION_BYTES,
    )?)?;
    ensure!(
        publication.release_id()? == matched.release_id,
        "approved holder fixture changed"
    );
    let package = bounded(&directory.join("source.elx"), 16 * 1024)?;
    let proof = bounded(&directory.join("source-proof.elxgit"), 16 * 1024)?;
    Ok((
        descriptor,
        SourceFixture {
            publication,
            matched,
            package,
            proof,
        },
    ))
}
async fn input() -> Result<Input> {
    let root = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_SOURCE_BATCH_FIXTURE")?);
    ensure!(
        root.is_absolute(),
        "absolute approved source fixture required"
    );
    let root = root.canonicalize()?;
    let output = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new("node")
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join(".."))
            .args([
                "crates/elixir_marketplace/worker/test/source-batch-claims.mjs",
                "--isolated-test-only",
                "--claims",
            ])
            .arg(&root)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success() && output.stdout.len() <= 512 * 1024 && output.stderr.len() <= 1024,
        "bounded isolated test credential input failed"
    );
    let claims: Claims = canonical::decode(&output.stdout, 512 * 1024)?;
    ensure!(
        claims.schema_version == 1
            && claims.fixture_only
            && claims.simulated_paid_support
            && claims.commit == COMMIT
            && claims.descriptors.len() == 20,
        "exact isolated twenty-source input required"
    );
    let issuer = PublicKey::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[3; 32])
            .verifying_key()
            .to_bytes(),
    )?;
    let policy = IssuerPolicy::new([issuer])?;
    let mut cache = SignatureCache::new(1024)?;
    let mut ids = BTreeSet::new();
    let mut matched = Vec::new();
    for (n, descriptor) in claims.descriptors.iter().enumerate() {
        descriptor.verify_metadata(now()?, &policy, &mut cache)?;
        let directory = root
            .join("releases")
            .join(format!("cold-page-{:02}", n + 1));
        let publication = PreparedPublication::parse(&bounded(
            &directory.join("publication.json"),
            MAX_PUBLICATION_BYTES,
        )?)?;
        let summary = publication.verify(now()?)?;
        let release = &publication.release.payload;
        ensure!(
            canonical::to_vec(&publication.publisher)? == canonical::to_vec(&descriptor.publisher)?
                && canonical::to_vec(&publication.release)?
                    == canonical::to_vec(&descriptor.release)?
                && canonical::to_vec(&publication.head)? == canonical::to_vec(&descriptor.head)?,
            "test claims changed the SDK publication"
        );
        ensure!(
            release.version == "1.0.0"
                && release.source.commit == COMMIT
                && release.source.subdirectory == format!("bench/cold-page-{:02}", n + 1)
                && release.media_tags == [MediaTag::Live]
                && release.runtime_profile == RuntimeProfile::SourceJs
                && release.artifacts.is_empty()
                && publication.head.payload.sequence == 0
                && descriptor.gate_evidence.len() == 5
                && descriptor.revocations.is_empty()
                && ids.insert(release.extension_id.clone()),
            "reviewed distinct synthetic release differs"
        );
        ensure!(
            release.source.locations.len() == 1
                && release.source.locations[0].kind == RepositoryKind::Github
                && release.source.locations[0].repository_identity == REPOSITORY
                && release.source.locations[0].url.as_deref() == Some(URL),
            "only the approved repository may be fetched"
        );
        let equality = SourceMatchDescriptor {
            schema_version: 1,
            workspace: QuarantineWorkspace {
                schema_version: 1,
                release_id: summary.release_id,
                package: release.package.clone(),
                proof: release.source.proof.clone(),
                artifact: None,
                timeout_seconds: 30,
            },
            release: release.clone(),
        }
        .verify(
            Cursor::new(bounded(&directory.join("source.elx"), 16 * 1024)?),
            Cursor::new(bounded(&directory.join("source-proof.elxgit"), 16 * 1024)?),
        )?;
        ensure!(
            equality.files == 3 && equality.commit == COMMIT && equality.license.path == "LICENSE",
            "exact three-file package/Git/license equality required before enrollment"
        );
        matched.push(equality);
    }
    Ok(Input {
        descriptors: claims.descriptors,
        matched,
    })
}

#[tokio::test]
#[ignore = "explicit private approved20 fixture; offline signed package/Git equality only, no enrollment or WAN"]
async fn approved_twenty_sources_match_their_signed_packages_before_enrollment() -> Result<()> {
    ensure!(
        input().await?.matched.len() == 20,
        "twenty independently checked snapshots required"
    );
    Ok(())
}

async fn query(
    service: &SearchService,
    client: &ParticipationClient,
    name: &str,
    query: SearchQuery,
    expected: &BTreeMap<ExtensionId, Digest>,
) -> Result<u64> {
    let began = Instant::now();
    let mut next_diagnostic = Duration::from_secs(15);
    eprintln!(
        "{}",
        serde_json::json!({"stage":"protected_public_backend_query_started", "query":name})
    );
    let id = service.start(
        client.clone(),
        SearchRequest {
            query,
            cursor: None,
            prepare_catalog: false,
        },
    )?;
    let view = loop {
        let view = service.view(id).await?; // Also renews this actual query's interest.
        if view.phase != SearchPhase::Checking {
            break view;
        }
        if began.elapsed() >= next_diagnostic {
            eprintln!(
                "{}",
                serde_json::json!({
                    "stage":"protected_public_backend_query_wait", "query":name,
                    "elapsed_ms":began.elapsed().as_millis(), "phase":view.phase,
                    "rows":view.results.len(), "checked":view.checked,
                    "omitted":view.omitted, "pending":view.pending,
                    "readiness":client.fixture_preparation_diagnostics(),
                })
            );
            next_diagnostic = began.elapsed() + Duration::from_secs(15);
        }
        ensure!(
            began.elapsed() < Duration::from_secs(180),
            "bounded public-source search did not finish: query={name} phase={:?} rows={} checked={} omitted={} pending={}",
            view.phase,
            view.results.len(),
            view.checked,
            view.omitted,
            view.pending
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let elapsed = u64::try_from(began.elapsed().as_millis())?;
    ensure!(
        view.phase == SearchPhase::Ready
            && view.omitted == 0
            && view.results.len() == expected.len()
            && !view.network_complete
            && view.ranking_partial
            && view.metadata_only,
        "protected search did not return the independently verified expected page: phase={:?} rows={} checked={} omitted={}",
        view.phase,
        view.results.len(),
        view.checked,
        view.omitted
    );
    for card in &view.results {
        ensure!(
            expected.get(&card.extension_id) == Some(&card.release_id)
                && card.support.discovery_qualified
                && card.support.verified_supporters == 5
                && !card.support.is_global_total
                && card.metadata_only,
            "test search card lost native/source/adoption binding"
        );
    }
    let actual: BTreeMap<_, _> = view
        .results
        .iter()
        .map(|card| (card.extension_id.clone(), card.release_id))
        .collect();
    ensure!(
        &actual == expected,
        "protected search omitted or duplicated an expected identity"
    );
    // Join this SAME query before the next sample, never race detached enrichment.
    service.request_stop();
    let retiring = Instant::now();
    service.join().await?;
    eprintln!(
        "{}",
        serde_json::json!({"stage":"protected_public_backend_query", "query":name,
        "elapsed_ms":elapsed, "rows":view.results.len(), "checked":view.checked,
        "retirement_ms":retiring.elapsed().as_millis(), "simulated_support_only":true})
    );
    Ok(elapsed)
}
fn browse() -> SearchQuery {
    SearchQuery {
        text: String::new(),
        tags: vec![],
        limit: 20,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "one approved free provider + actual protected20 public-source cold/warm backend search; isolated simulated support, consumer-only, no public package/credential serving or real billing"]
async fn actual_protected_twenty_source_page_and_warm_queries() -> Result<()> {
    for consent in [
        "ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST",
        "ELIXIR_MARKETPLACE_WAN_LIVE_TEST",
        "ELIXIR_MARKETPLACE_PUBLIC_SOURCE_LIVE_TEST",
        "ELIXIR_MARKETPLACE_PUBLIC_COLD_PAGE_LIVE_TEST",
    ] {
        ensure!(
            std::env::var(consent).as_deref() == Ok("1"),
            "explicit approved provider/source benchmark consent required"
        );
    }
    let input = input().await?; // No enrollment before full SDK/package/Git equality.
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker_image).await?;
    ensure!(
        [
            "elixir.marketplace.worker.metadata_batch",
            "elixir.marketplace.worker.source_batch",
            "elixir.marketplace.worker.wan_query_budget"
        ]
        .iter()
        .all(|key| labels.get(*key).map(String::as_str) == Some("1"))
            && !labels.contains_key("elixir.marketplace.worker.source_batch_bridge_fixture"),
        "real production-path batch image required"
    );
    let service = SearchService::default();
    // Use the existing installed-application layout/pool, not the single-
    // connection low-level provider fixture. Two runtime threads model a
    // modest server rather than the default single-threaded unit-test runtime.
    let mut rig = Rig::new_layout(runtime, &provider_image, true).await?;
    let mut sources: Vec<SearchSourceObservation> = Vec::new();
    let mut fresh = None;
    let outcome = async {
        let actor = crate::marketplace::grant_store::tests::owner(&rig.database).await?;
        let saved = preferences::save(&rig.database, &actor.fixture_principal(), &Change { expected_revision: 0,
            mutation_id: uuid::Uuid::new_v4(), preferences: Preferences { contribution: ContributionMode::Off,
                metering: ConnectionMetering::Metered, ..Default::default() } }, chrono::Utc::now()).await?;
        ensure!(saved.revision == 1 && !saved.preferences.paid_support_consent
            && saved.preferences.contribution_disclosure_version.is_none(), "saved isolated Off preferences differ");
        rig.open(&guard_image, &worker_image).await.context("public cold-page actual protected startup")?;
        eprintln!("{}", serde_json::json!({"stage":"protected_public_backend_worker_open"}));
        let playback = Arc::new(PlaybackJobManager::new(rig.database.pool.clone(), None));
        let worker = rig.worker.take().context("original protected worker missing")?;
        let network = config(Digest::of(uuid::Uuid::new_v4().as_bytes()), false)?;
        rig.participating = Some(ParticipatingWorker::start(worker, network, rig.database.clone(), playback).await?);
        let client = rig.participating.as_ref().context("original participating owner missing")?.client();
        source_ready(&client).await?;
        eprintln!("{}", serde_json::json!({"stage":"protected_public_backend_participation_ready"}));
        let roles = client.snapshot().applied_roles.context("actual roles missing")?;
        ensure!(roles.consumer && !roles.catalog && !roles.routing, "isolated synthetic claims must never acquire public serving roles");
        // These are PRIVATE own-worker inputs, not request_peer/public offers.
        // No host native-metadata/source receipt is prewarmed before cold search.
        for (descriptor, matched) in input.descriptors.iter().zip(&input.matched) {
            let reply = client.request(PeerOperation::Offer { descriptor: Box::new(descriptor.clone()) }).await?;
            ensure!(matches!(reply.response, PeerOutcome::Offered { release_id } if release_id == matched.release_id),
                "isolated native catalog did not retain exact SDK descriptor");
        }
        eprintln!("{}", serde_json::json!({"stage":"protected_public_backend_private_catalog_loaded", "descriptors":20}));
        let expected: BTreeMap<_, _> = input.descriptors.iter().zip(&input.matched)
            .map(|(d, matched)| (d.release.payload.extension_id.clone(), matched.release_id)).collect();
        let cold = query(&service, &client, "cold_browse", browse(), &expected).await?;
        let mut warm = Vec::with_capacity(WARM_SAMPLES);
        for _ in 0..WARM_SAMPLES { warm.push(query(&service, &client, "warm_browse", browse(), &expected).await?); }
        warm.sort_unstable();
        let p95 = warm[(WARM_SAMPLES * 95).div_ceil(100) - 1];
        let mut tagged = browse(); tagged.tags = vec![MediaTag::Live];
        let filter = query(&service, &client, "live_filter", tagged, &expected).await?;
        let first = &input.descriptors[0];
        let one = BTreeMap::from([(first.release.payload.extension_id.clone(), input.matched[0].release_id)]);
        let mut text = browse(); text.text = first.release.payload.display_name.clone();
        let text_ms = query(&service, &client, "matching_text", text, &one).await?;
        let exact = SearchQuery { text: first.release.payload.extension_id.to_string(), tags: vec![], limit: 1 };
        let exact_ms = query(&service, &client, "exact_id", exact, &one).await?;
        // Include a real no-match path; never conceal discovery latency behind
        // only successful local matches. This may expose an outstanding UX gate.
        let mut absent = browse(); absent.tags = vec![MediaTag::Movies];
        let miss = query(&service, &client, "empty_filter", absent, &BTreeMap::new()).await?;
        for (descriptor, matched) in input.descriptors.iter().zip(&input.matched) {
            let metadata = client.verify_metadata(descriptor).await?;
            let source = client.preflight_search_metadata(&metadata, 0).await?;
            ensure!(source.release_id() == matched.release_id && source.valid_for() > Duration::from_secs(3600),
                "actual public-source search cache lost its exact release or discovery-only TTL");
            sources.push(source);
        }
        let metadata = client.verify_metadata(first).await?;
        let separate = client.preflight_fresh_metadata(&metadata, 0).await?;
        ensure!(separate.release_id() == input.matched[0].release_id
            && separate.license_sha256() == input.matched[0].license.sha256
            && separate.valid_for() <= Duration::from_secs(300), "fresh installation-side source fact differs");
        fresh = Some(separate);
        eprintln!("{}", serde_json::json!({"stage":"actual_protected_public_backend_summary", "sources":20,
            "cold_browse_ms":cold, "status_poll_ms":100, "warm_browse_samples":WARM_SAMPLES, "warm_browse_p95_ms":p95,
            "host_debug_assertions":cfg!(debug_assertions), "runtime_threads":2, "database_pool_limit":6,
            "live_filter_ms":filter, "matching_text_ms":text_ms, "exact_id_ms":exact_ms, "empty_filter_ms":miss,
            "warm_backend_200ms_budget_met":p95 <= 200 && filter <= 200 && text_ms <= 200 && exact_ms <= 200 && miss <= 200,
            "real_paid_credentials":0, "simulated_support_only":true, "consumer_only":true,
            "excludes":"network bootstrap/catalog delivery time, desktop HTTP/rendering, real billing, peer package transfer, installation/update, loaded playback and catalog-scale/soak proof"}));
        Ok::<(), anyhow::Error>(())
    }.await;
    if let Err(error) = &outcome {
        eprintln!("Public cold-page original operation failed before retirement: {error:#}");
    }
    service.request_stop();
    let search_retirement = service.join().await;
    if let Err(error) = &search_retirement {
        eprintln!("Public cold-page original search retirement failed: {error:#}");
    }
    let cleanup = rig.close().await;
    if let Err(error) = &cleanup {
        eprintln!("Public cold-page same-identity cleanup failed: {error:#}");
    }
    cleanup.context("public cold-page original identity revocation and joined cleanup")?;
    search_retirement?;
    ensure!(
        sources.iter().all(|source| source.valid_for().is_zero())
            && fresh
                .as_ref()
                .is_none_or(|source| source.valid_for().is_zero()),
        "retired public source facts remained usable"
    );
    outcome
}
