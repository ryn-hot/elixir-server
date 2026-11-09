//! Native free-provider composition under the existing startup/retry owner.
//! No alternate health factory, direct fallback or per-attempt registration.
use super::*;
use crate::marketplace::provider::{Owner, Profile as ProviderProfile, TRANSPORT_BOOTSTRAP_BYTES};
use elixir_marketplace::{packet_guard::PROVIDER_TUN_INTERFACE, warp_provider::Scope};

pub(super) async fn prepare(
    orchestrator: Arc<crate::orchestrator::OrchestratorService>,
    database: Database,
    manager: &SecretsManager,
    profile: Profile,
    paths: RuntimePaths,
    playback: Arc<crate::playback::PlaybackJobManager>,
    startup: StartOwner,
    consent: Option<&super::super::provider_setup::Binding>,
) -> Result<()> {
    let Provider::NativeWarp {
        image, enrollment, ..
    } = &profile.provider
    else {
        anyhow::bail!("native provider selection missing");
    };
    ensure!(
        preferences::load_internal(&database).await?.revision > 0,
        "marketplace owner setup is required before provider startup"
    );
    ensure!(
        available_transfer(&database, now()?, &profile.limits).await?
            >= BOOTSTRAP_PROBE_BYTES + INITIAL_WORKER_BYTES + TRANSPORT_BOOTSTRAP_BYTES,
        "marketplace provider startup allowance is unavailable"
    );
    ProtectedWorker::await_startup_host(&profile.limits, &playback).await?;
    orchestrator.ensure_runtime_ready().await?;
    ProtectedWorker::await_startup_host(&profile.limits, &playback).await?;
    let runtime = orchestrator.runtime_manager();
    let directory = PathBuf::from(&paths.data_root)
        .canonicalize()?
        .join("marketplace");
    let leases = StartupLeases::acquire_inner(&directory, true)?;
    let attempt = startup.begin(&leases)?;
    let StartupLeases {
        root,
        packet,
        worker,
        ..
    } = leases;
    let deployment = marketplace_deployment(&paths);
    let scope = Scope {
        deployment: ProviderProfile::deployment_scope(&deployment),
        enrollment: *enrollment,
    };
    require_provider_consent(consent).await?;
    let mut owner = Owner::launch_consented(
        runtime.clone(),
        worker,
        deployment.clone(),
        image.clone(),
        scope,
        Arc::new(manager.clone()),
    )
    .wait()
    .await?;
    let connected = async {
        require_provider_consent(consent).await?;
        owner.connect().await?;
        // Wait for observable link presence, not an arbitrary stabilization
        // sleep. This is not health: fresh native transport + DNS/TLS follow.
        let until = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            require_provider_consent(consent).await?;
            if owner.inspect().await? {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "native provider link startup deadline"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        owner.transport().await
    }
    .await;
    let transport = match connected {
        Ok(transport) => transport,
        Err(error) => {
            owner
                .close(false)
                .await
                .context("failed provider startup cleanup remains fenced")?;
            return Err(error);
        }
    };
    let policy = PacketPolicy {
        schema_version: 1,
        epoch: Digest::of(uuid::Uuid::new_v4().as_bytes()),
        tunnel_interface: PROVIDER_TUN_INTERFACE.into(),
        tunnel_addresses: transport.tunnel_addresses,
        endpoints: vec![transport.endpoint],
        lease_seconds: 15,
        upload_bytes_per_second: profile.limits.upload_bytes_per_second,
        download_bytes_per_second: profile.limits.download_bytes_per_second,
    };
    let debit = match reserve_transfer(
        &database,
        now()?,
        TRANSPORT_BOOTSTRAP_BYTES,
        &profile.limits,
    )
    .await
    {
        Ok(debit) => debit,
        Err(error) => {
            owner
                .close(false)
                .await
                .context("provider debit failure cleanup remains fenced")?;
            return Err(error);
        }
    };
    let mut guard = owner
        .guard(
            profile.guard_image.clone(),
            policy,
            profile.limits.clone(),
            debit,
        )
        .wait()
        .await?;
    let bootstrap = async {
        require_provider_consent(consent).await?;
        ProtectedWorker::admit_startup_resources(
            runtime.as_ref(),
            &mut guard,
            &profile.limits,
            &playback,
        )
        .await?;
        let debit =
            reserve_transfer(&database, now()?, BOOTSTRAP_PROBE_BYTES, &profile.limits).await?;
        let health = guard
            .bootstrap_provider_connectivity(runtime.as_ref(), debit, ProbeFamily::Ipv4)
            .await?;
        require_provider_consent(consent).await?;
        let debit = reserve_worker_startup_credit(&database, now()?, &profile.limits).await?;
        Ok::<_, anyhow::Error>((health, debit))
    }
    .await;
    let (health, debit) = match bootstrap {
        Ok(result) => result,
        Err(error) => {
            guard
                .stop(runtime.as_ref())
                .await
                .context("provider bootstrap cleanup remains fenced")?;
            return Err(error);
        }
    };
    let worker = ProtectedWorker::start_provider(
        runtime,
        ProtectedWorkerConfig {
            controller_directory: directory.join("worker"),
            deployment,
            image: profile.worker_image,
            issuer_keys: profile.issuer_keys,
            probe_policy: ProbePolicy {
                family: ProbeFamily::Ipv4,
                require_warp: true,
            },
        },
        database.clone(),
        guard,
        health,
        debit,
    )
    .wait()
    .await?;
    if let Err(error) = require_provider_consent(consent).await {
        worker
            .stop()
            .await
            .context("withdrawn provider consent cleanup remains fenced")?;
        return Err(error);
    }
    let owner = ParticipatingWorker::start(worker, profile.network, database, playback).await?;
    if let Err(error) = require_provider_consent(consent).await {
        owner
            .stop()
            .await
            .context("withdrawn provider consent participation cleanup remains fenced")?;
        return Err(error);
    }
    attempt.attach(owner)?;
    drop(packet);
    drop(root);
    Ok(())
}
