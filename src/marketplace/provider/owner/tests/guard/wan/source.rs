//! Approved public test source through the REAL protected owner factory.
//! No manufactured protection, host facts, paid eligibility or install grant.
use super::*;
use crate::{
    marketplace::{
        preferences::{self, Change, ConnectionMetering, Preferences},
        worker::{ParticipationClient, ParticipationPhase, PublicSourceObservation},
    },
    playback::PlaybackJobManager,
};
use elixir_marketplace::{
    policy::ContributionMode,
    publication::{MAX_PUBLICATION_BYTES, PreparedPublication},
    quarantine::QuarantineWorkspace,
    records::RepositoryKind,
    source_job::{SourceMatchDescriptor, SourceMatchSummary},
};
use std::{
    fs::OpenOptions,
    io::{Cursor, Read},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
mod cold_page;
mod installation;

const REPOSITORY: &str = "github.com/ryn-hot/elixir-marketplace-test-extension";
const URL: &str = "https://github.com/ryn-hot/elixir-marketplace-test-extension";
const SNAPSHOTS: [(&str, &str, u64); 2] = [
    ("1.0.0", "8a3dcef4e9f3efbe88f569437c9f1fa28d6435fd", 0),
    ("1.1.0", "842277ac588189eccf1c78deac2de665b4942024", 1),
];
struct SourceFixture {
    publication: PreparedPublication,
    matched: SourceMatchSummary,
    package: Vec<u8>,
    proof: Vec<u8>,
}
fn bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.nlink() == 1 && meta.len() > 0 && meta.len() <= maximum as u64,
        "bounded regular public fixture file required"
    );
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == meta.len() && bytes.len() <= maximum,
        "public fixture file changed"
    );
    Ok(bytes)
}
fn fixtures(root: &Path) -> Result<Vec<SourceFixture>> {
    ensure!(
        root.is_absolute() && root.is_dir(),
        "explicit absolute public fixture root required"
    );
    let mut fixtures = Vec::new();
    for (index, (version, commit, sequence)) in SNAPSHOTS.into_iter().enumerate() {
        let directory = root.join(format!("release-v{}", index + 1));
        let publication = PreparedPublication::parse(&bounded(
            &directory.join("publication.json"),
            MAX_PUBLICATION_BYTES,
        )?)?;
        let summary = publication.verify(now()?)?;
        let release = &publication.release.payload;
        ensure!(
            release.version == version
                && release.source.commit == commit
                && publication.head.payload.sequence == sequence,
            "exact reviewed test snapshot required"
        );
        ensure!(
            release.source.locations.len() == 1
                && release.source.locations[0].kind == RepositoryKind::Github
                && release.source.locations[0].repository_identity == REPOSITORY
                && release.source.locations[0].url.as_deref() == Some(URL),
            "only the owner-approved public test repository may be used"
        );
        let descriptor = SourceMatchDescriptor {
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
        };
        // Independent package/Git/manifest/license equality BEFORE enrollment.
        // This host assertion is not a native process or install capability.
        let package = bounded(&directory.join("source.elx"), 16 * 1024)?;
        let proof = bounded(&directory.join("source-proof.elxgit"), 16 * 1024)?;
        let matched = descriptor.verify(Cursor::new(&package), Cursor::new(&proof))?;
        ensure!(
            matched.files == 4 && matched.commit == commit && matched.license.path == "LICENSE",
            "reviewed four-file source equality differs"
        );
        fixtures.push(SourceFixture {
            publication,
            matched,
            package,
            proof,
        });
    }
    ensure!(
        fixtures[0].publication.publisher.to_bytes()?
            == fixtures[1].publication.publisher.to_bytes()?,
        "test publisher changed"
    );
    Ok(fixtures)
}
fn configured_fixtures() -> Result<Vec<SourceFixture>> {
    fixtures(Path::new(&std::env::var(
        "ELIXIR_MARKETPLACE_PUBLIC_SOURCE_FIXTURE",
    )?))
}

#[test]
#[ignore = "explicit local approved source-fixture root; no provider, containers or public networking"]
fn approved_public_source_snapshots_have_exact_signed_package_git_and_license_equality()
-> Result<()> {
    let loaded = configured_fixtures()?;
    ensure!(loaded.len() == 2, "two reviewed source versions required");
    Ok(())
}

async fn source_ready(client: &ParticipationClient) -> Result<()> {
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match client.snapshot().phase {
            ParticipationPhase::Running if client.catalog_preparation_ready()? => return Ok(()),
            ParticipationPhase::Starting
            | ParticipationPhase::Running
            | ParticipationPhase::Applying
            | ParticipationPhase::WaitingForProtection => {}
            _ => anyhow::bail!("actual protected public-source owner unavailable"),
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "actual public-source readiness deferred beyond the bounded wait"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[ignore = "one approved free provider + actual protected published-source verification; no paid proofs, peer seeding or installation"]
async fn approved_github_source_is_verified_by_the_actual_protected_participation_owner()
-> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_WAN_LIVE_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_PUBLIC_SOURCE_LIVE_TEST").as_deref() == Ok("1"),
        "explicit approved-provider/public-source test consent required"
    );
    let loaded = configured_fixtures()?; // Validate all files before any registration.
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut rig = Rig::new(runtime, &provider_image).await?;
    let mut sources: Vec<PublicSourceObservation> = Vec::new();
    let outcome = async {
        let actor = crate::marketplace::grant_store::tests::owner(&rig.database).await?;
        let saved = preferences::save(&rig.database, &actor.fixture_principal(), &Change {
            expected_revision: 0,
            mutation_id: uuid::Uuid::new_v4(),
            preferences: Preferences {
                contribution: ContributionMode::Off,
                metering: ConnectionMetering::Metered,
                ..Default::default()
            },
        }, chrono::Utc::now()).await?;
        ensure!(saved.revision == 1 && !saved.preferences.paid_support_consent
            && saved.preferences.contribution_disclosure_version.is_none(), "explicit saved Off fixture differs");
        rig.open(&guard_image, &worker_image).await.context("public-source actual protected startup")?;
        let playback = Arc::new(PlaybackJobManager::new(rig.database.pool.clone(), None));
        let worker = rig.worker.take().context("protected source worker missing")?;
        rig.participating = Some(ParticipatingWorker::start(worker,
            config(Digest::of(uuid::Uuid::new_v4().as_bytes()), false)?, rig.database.clone(), playback).await?);
        let client = rig.participating.as_ref().unwrap().client();
        source_ready(&client).await?;
        let roles = client.snapshot().applied_roles.context("actual applied source roles missing")?;
        ensure!(roles.consumer && !roles.catalog && !roles.routing, "saved Off source test silently became infrastructure");
        for fixture in &loaded {
            source_ready(&client).await?;
            let metadata = client.verify_publication(&fixture.publication).await.context("actual native publication verification")?;
            source_ready(&client).await?;
            let began = tokio::time::Instant::now();
            let source = client.preflight_fresh_publication(&metadata, 0).await.context("actual protected fresh public source fetch")?;
            ensure!(source.release_id() == fixture.matched.release_id
                && source.license_sha256() == fixture.matched.license.sha256
                && source.valid_for() > Duration::from_secs(2), "public source differs from independently verified package/Git/license");
            eprintln!("{}", serde_json::json!({"stage":"actual_protected_public_source", "version":fixture.publication.release.payload.version,
                "release_id":source.release_id(), "elapsed_ms":began.elapsed().as_millis(), "paid_credentials":0, "discovery_qualified":false}));
            sources.push(source);
            let mut changed = fixture.publication.clone();
            changed.release.payload.description.push_str(" unsigned change");
            source_ready(&client).await?;
            let rejection = client.verify_publication(&changed).await.err().context("modified public publication authenticated")?;
            ensure!(rejection.chain().any(|cause| cause.to_string().contains("direct reference was not authenticated")),
                "tamper check failed at admission instead of actual native authentication: {rejection:#}");
            ensure!(client.snapshot().phase == ParticipationPhase::Running, "ordinary signature rejection poisoned protected owner");
        }
        Ok::<(), anyhow::Error>(())
    }.await;
    if let Err(error) = &outcome {
        eprintln!("Public-source operation failed before retirement: {error:#}");
    }
    let cleanup = rig.close().await;
    if let Err(error) = &cleanup {
        eprintln!("Public-source same-identity retirement failed: {error:#}");
    }
    cleanup.context("public-source original identity revocation and joined cleanup")?;
    ensure!(
        sources.iter().all(|source| source.valid_for().is_zero()),
        "retired protected source facts remained usable"
    );
    outcome?;
    eprintln!(
        "{}",
        serde_json::json!({"actual_protected_public_sources":sources.len(), "saved_off":true, "metered":true,
        "consumer_only":true, "real_preferences_host_metrics_native_binding":true, "same_identity_revoked":true,
        "excludes":"paid eligibility, catalog seeding, app HTTP/UI, package peer transfer, installation, updates, playback and scale"})
    );
    Ok(())
}
