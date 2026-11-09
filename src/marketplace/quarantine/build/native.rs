//! Actual offline Docker/source-owner composition. Public-source liveness is
//! an explicit TEST observation; this does not qualify public discovery,
//! container execution, real paid credentials, playback or production launch.
use super::*;
use crate::runtime::docker::DockerRuntimeManager;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::FutureExt;

mod retained;

// Synthetic SDK replies only. The actual NativeInvocation/image owner and
// revocation are mandatory; this does not stand in for installed DB grants.
struct ContainerBroker {
    source: SourceMatchDescriptor,
    native: Option<Arc<super::super::execution::NativeInvocation>>,
    calls: u32,
    retired: bool,
}
#[async_trait::async_trait]
impl super::super::execution::Broker for ContainerBroker {
    fn native_started(
        &mut self,
        invocation: super::super::execution::NativeInvocation,
    ) -> Result<()> {
        if self.native.is_some() {
            bail!("compiled fixture received a second native owner");
        }
        invocation.require_invoke(&self.source)?;
        self.native = Some(Arc::new(invocation));
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        if let Some(native) = &self.native {
            native.require_invoke(&self.source)?;
        }
        Ok(())
    }
    async fn request(
        &mut self,
        raw: &elixir_marketplace::source_execution::RuntimeJson,
        _: usize,
    ) -> Result<elixir_marketplace::source_execution::SourceSdkReply> {
        use elixir_marketplace::source_execution::{RuntimeJson, SourceSdkReply, SourceSdkRequest};
        let native = self
            .native
            .as_ref()
            .context("SDK effect preceded actual native binding")?;
        native.require_invoke(&self.source)?;
        // Like the actual installed broker: require the immutable process to
        // be running immediately before an effect, not after RESULT_ACK has
        // deliberately let the guardian exit. Scope/image/lease checks remain
        // live throughout transport and the actual exit is joined separately.
        native.current().await?;
        self.calls += 1;
        let request = SourceSdkRequest::parse(raw)?;
        let value = match (self.calls, request) {
            (1, SourceSdkRequest::StateSet { key, value })
                if key == "fixture" && value == serde_json::json!(true) =>
            {
                serde_json::json!(true)
            }
            (2, SourceSdkRequest::StateGet { key }) if key == "fixture" => serde_json::json!(true),
            (3, SourceSdkRequest::Secret { key }) if key == "token" => {
                serde_json::json!("synthetic-sdk-value")
            }
            (4, SourceSdkRequest::Fetch { url, .. }) if url == "https://fixture.invalid/" => {
                serde_json::json!({"body":"synthetic body","headers":[],"status":200})
            }
            _ => bail!("compiled fixture requested an unexpected synthetic SDK effect"),
        };
        Ok(SourceSdkReply::Value(RuntimeJson::from_value(value)?))
    }
    async fn retire_effects(&mut self) -> Result<()> {
        if let Some(native) = self.native.take() {
            if native.check().is_ok() {
                bail!("compiled native scope was not revoked before retiring effects");
            }
            self.retired = true;
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "explicit private native compiled-build ownership test; no public network"]
async fn actual_owned_source_staging_build_comparison_and_deadline_cleanup() -> Result<()> {
    native_variants(
        &["match", "wrong-output", "busy-build", "drop-before-build"],
        false,
        false,
    )
    .await
}

#[tokio::test]
#[ignore = "explicit offline original-owner OCI graph test; no public network or activation"]
async fn actual_owned_reproduction_precedes_native_container_graph_admission() -> Result<()> {
    native_variants(
        &["container-image", "container-image-invalid"],
        false,
        false,
    )
    .await
}

#[tokio::test]
#[ignore = "explicit offline original-owner OCI filesystem test; no public network or activation"]
async fn actual_owned_reproduction_precedes_native_container_filesystem_admission() -> Result<()> {
    native_variants(
        &["container-image", "container-image-filesystem-invalid"],
        true,
        false,
    )
    .await
}

#[tokio::test]
#[ignore = "explicit offline original-owner image export/staging test; no daemon import or activation"]
async fn actual_owned_reproduction_filesystem_and_native_export_seal_and_cleanup() -> Result<()> {
    native_variants(&["container-image"], true, true).await
}

async fn native_variants(variants: &[&str], filesystem: bool, export: bool) -> Result<()> {
    native_variants_with_import(variants, filesystem, export, false).await
}
#[tokio::test]
#[ignore = "explicit offline original-source-owner image import/retirement; no publisher execution"]
async fn actual_owned_reproduction_export_daemon_import_inspection_and_retirement() -> Result<()> {
    native_variants_with_import(&["container-image"], true, true, true).await
}
async fn native_variants_with_import(
    variants: &[&str],
    filesystem: bool,
    export: bool,
    import: bool,
) -> Result<()> {
    native_variants_with_handoff(variants, filesystem, export, import, false).await
}
#[tokio::test]
#[ignore = "explicit original-source image reference handoff across another native phase; no publisher execution"]
async fn actual_owned_import_reference_handoff_native_phase_and_disposal() -> Result<()> {
    native_variants_with_handoff(&["container-image"], true, true, true, true).await
}
async fn native_variants_with_handoff(
    variants: &[&str],
    filesystem: bool,
    export: bool,
    import: bool,
    adopt: bool,
) -> Result<()> {
    native_variants_with_runtime(variants, filesystem, export, import, adopt, false).await
}
#[tokio::test]
#[ignore = "explicit original-source rebuilt/imported image plus independent guardian and SDK execution; no activation or public network"]
async fn actual_owned_rebuilt_imported_container_invocation_sdk_and_cleanup() -> Result<()> {
    native_variants_with_runtime(&["container-image-runtime"], true, true, true, true, true).await
}
#[tokio::test]
#[ignore = "explicit actual original-source/image plus account/grant/instance SQL/AES SDK; synthetic metadata/source observations, no activation"]
async fn actual_owned_compiled_invocation_uses_real_grants_and_stops_on_revocation() -> Result<()> {
    native_variants_with_runtime(&["container-image-grants"], true, true, true, true, true).await
}
#[tokio::test]
#[ignore = "explicit actual build/archive/disposal/new-job rematch/reimport/execution; no installed activation or public network"]
async fn actual_owned_retained_build_record_rechecks_archived_bytes_under_a_new_job() -> Result<()>
{
    native_variants_with_runtime(&["container-image-retained"], true, true, true, true, true).await
}
#[tokio::test]
#[ignore = "actual reproduced/imported code plus installed controller, crypto, SQL/AES SDK, cancellation and revocation; ACTIVE enrollment synthetic"]
async fn actual_owned_installed_compiled_calls_recheck_crypto_grants_and_cleanup() -> Result<()> {
    native_variants_with_runtime(&["container-image-installed"], true, true, true, true, true).await
}
#[tokio::test]
#[ignore = "actual compiled archive/native probe/state copy/candidate SDK/readiness/activation/cold reopen, plus failed-readiness cleanup; source/protection observations synthetic"]
async fn actual_compiled_activation_uses_the_original_probe_candidate_and_commit() -> Result<()> {
    native_variants_with_runtime(
        &[
            "container-image-activation",
            "container-image-activation-failure",
        ],
        true,
        true,
        true,
        true,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "actual native v1 activation -> v2 nonempty-state/credential update -> failed v3 cleanup and offline v2; source/protection observations synthetic"]
async fn actual_compiled_updates_copy_nonempty_state_and_preserve_the_active_generation_on_failure()
-> Result<()> {
    native_variants_with_runtime(
        &[
            "container-image-activation-update-v1",
            "container-image-activation-update-v2",
            "container-image-activation-update-v3-failure",
        ],
        true,
        true,
        true,
        true,
        true,
    )
    .await
}
async fn native_variants_with_runtime(
    variants: &[&str],
    filesystem: bool,
    export: bool,
    import: bool,
    adopt: bool,
    execute: bool,
) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let toolchain = std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?;
    if !image.starts_with("sha256:")
        || !image_is_pinned(&image)
        || !toolchain.starts_with("sha256:")
        || !image_is_pinned(&toolchain)
    {
        bail!("explicit immutable local fixture images required");
    }
    let mut manager = DockerRuntimeManager::new(None);
    if import {
        let profile = crate::runtime::image_import::Profile::from_operator_configuration(
            std::path::PathBuf::from(
                std::env::var_os("ELIXIR_TEST_IMAGE_IMPORT_BINARY")
                    .context("explicit host import companion missing")?,
            )
            .canonicalize()?,
            std::env::var("ELIXIR_TEST_IMAGE_IMPORT_BINARY_SHA256")?.parse()?,
            std::path::PathBuf::from(
                std::env::var_os("ELIXIR_TEST_IMAGE_IMPORT_SOCKET")
                    .context("explicit local daemon socket missing")?,
            )
            .canonicalize()?,
        )?;
        manager = manager.with_marketplace_import_profile(profile);
    }
    if execute {
        let profile = crate::runtime::container_guardian::Profile::from_operator_configuration(
            std::path::PathBuf::from(
                std::env::var_os("ELIXIR_TEST_CONTAINER_GUARDIAN_BINARY")
                    .context("explicit guardian binary missing")?,
            )
            .canonicalize()?,
            std::env::var("ELIXIR_TEST_CONTAINER_GUARDIAN_SHA256")?.parse()?,
            elixir_marketplace::reproduction::BuildPlatform::LinuxAmd64,
        )?;
        manager = manager.with_marketplace_container_guardian(profile);
    }
    let runtime = Arc::new(manager);
    let updating = variants
        .iter()
        .any(|v| v.starts_with("container-image-activation-update-"));
    if updating
        && variants
            .iter()
            .any(|v| !v.starts_with("container-image-activation-update-"))
    {
        bail!("compiled update fixture requires one continuous update sequence");
    }
    let mut shared_directory = if updating {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let shared_deployment =
        updating.then(|| format!("compiled-fixture-{}", uuid::Uuid::new_v4().simple()));
    let mut update_checkpoint: Option<
        crate::marketplace::grant_store::installation::activation::CompiledUpdateFixtureState,
    > = None;
    for &variant in variants {
        let directory = if updating {
            None
        } else {
            Some(tempfile::tempdir()?)
        };
        let root = directory
            .as_ref()
            .or(shared_directory.as_ref())
            .context("fixture root missing")?
            .path()
            .canonicalize()?;
        let lane = root.join("lane");
        let deployment = shared_deployment
            .clone()
            .unwrap_or_else(|| format!("compiled-fixture-{}", uuid::Uuid::new_v4().simple()));
        let volume = format!("elixir-marketplace-source-{deployment}");
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
            ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
        ]);
        // Do not duplicate the whole multi-phase state on each async wrapper's
        // stack. In particular the filesystem-negative path has more wrappers
        // than the direct runtime case; both retain the same cleanup owner.
        let operation=std::panic::AssertUnwindSafe(Box::pin(async {
            if (variant=="container-image-installed" || variant.starts_with("container-image-activation")) && (!updating||update_checkpoint.is_none()) {
                Box::pin(crate::marketplace::grant_store::installation::runtime::initialize_compiled_fixture(
                    runtime.clone(),&root,&lane,&image,&deployment)).await?;
            }
            let script=Path::new(env!("CARGO_MANIFEST_DIR")).join("../extensions/sdk/marketplace/test/compiled-build-fixture.mjs");
            let fixture=timeout(Duration::from_secs(30),tokio::process::Command::new("node").arg(script)
                .args([toolchain.as_str(),if variant=="drop-before-build"{"match"}else{variant}]).kill_on_drop(true).output()).await??;
            if !fixture.status.success() || fixture.stdout.len()>2*1024*1024 {bail!("offline compiled fixture failed");}
            let value:serde_json::Value=canonical::parse(&fixture.stdout,2*1024*1024)?;
            let grant_fixture=(variant=="container-image-grants").then(||value.clone());
            let retained_fixture=(matches!(variant,"container-image-retained"|"container-image-installed")||variant.starts_with("container-image-activation")).then(||value.clone());
            let artifact=STANDARD.decode(value["artifact_base64"].as_str().context("candidate fixture missing")?)?;
            let (release,package,proof)=crate::marketplace::quarantine::tests::decode_fixture(value)?;
            let plan=SourceQuarantinePlan::for_release(&release,Some(0),&SourceJobBudget{memory_bytes:256*MIB,cpu_millicores:200,
                disk_bytes:512*MIB,input_bytes:64*MIB,output_bytes:64*MIB,wall_seconds:if variant=="busy-build"{20}else{120}})?;
            let mut job=SourceQuarantineJob::open(runtime.clone(),&lane,&deployment,&image,&plan).await?;
            let original_expires=job.expires;
            for (purpose,body) in [(QuarantinePurpose::Package,&package),(QuarantinePurpose::Proof,&proof),(QuarantinePurpose::Artifact,&artifact)] {
                job.receive(runtime.as_ref(),purpose,&mut body.as_slice()).await?;
            }
            let ready=job.finish(runtime.as_ref()).await?;
            let source=ready.match_source(runtime.as_ref()).await?;
            let (public,_health)=crate::marketplace::worker::PublicSourceObservation::fixture_for_source(
                source.summary().release_id,source.summary().license.sha256);
            let mut owner=Some(source.bind_public_source(public)?);
            if SourceMatchedQuarantine::begin_fresh_build(&mut owner,&profile::ApprovedToolchains::default()).is_ok() || owner.is_none() {
                bail!("unapproved compiler must not consume its original source owner");
            }
            let approved=profile::ApprovedToolchains::from_operator_digests([toolchain.parse()?])?;
            let mut build=SourceMatchedQuarantine::begin_fresh_build(&mut owner,&approved)?;
            if owner.is_some() {bail!("build did not take exclusive source ownership");}
            if variant=="drop-before-build" {
                drop(build);
                timeout(Duration::from_secs(15),async {
                    loop {
                        if ControllerLease::acquire(&lane).is_ok(){return Ok::<_,anyhow::Error>(());}
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }).await??;
                if runtime.private_file_volume_owned(&volume,&labels).await? {bail!("dropped build released its lane before disposing source bytes");}
                return Ok(());
            }
            let built=build.run().await;
            if variant=="match" || variant.starts_with("container-image") {
                built?;
                let (mut source,evidence)=build.finish()?;
                let mut adopted_image=None;
                let mut runtime_view=None;
                if build.package.is_some() || build.proof.is_some() || build.staging.is_some() {
                    bail!("build retained source file descriptors after retiring staging allocation");
                }
                if source.expires()!=original_expires || evidence.release!=release.release_id()
                    || evidence.artifact.sha256!=release.payload().artifacts[0].blob.sha256 {
                    bail!("native build changed its original release/deadline");
                }
                if filesystem {
                    // Both fixtures have a valid exact image graph; the
                    // negative must fail native layer semantics, not parsing.
                    let expected_image=super::super::container_image::descriptor(&source)?.verify(std::io::Cursor::new(&artifact))?;
                    if variant=="container-image" && expected_image.config.to_string()!="sha256:452c1ee955f698cad26d3427503f00371c288cf0db5d42fe6c433aad59c94ad8" {
                        bail!("original image fixture changed unexpectedly");
                    }
                    let observed=source.check_container_filesystem(&evidence).await;
                    if variant=="container-image-filesystem-invalid" {
                        let error=observed.err().context("reproduced missing-working-directory image was admitted")?;
                        if !format!("{error:#}").contains("native container filesystem rejected") {
                            return Err(error).context("invalid filesystem did not reach native rejection");
                        }
                    } else {
                        let observed=observed?;
                        observed.require_source(&source)?;
                        let summary=observed.summary();
                        if canonical::to_vec(&summary.image)?!=canonical::to_vec(&expected_image)?
                            || summary.image.artifact.sha256!=release.payload().artifacts[0].blob.sha256
                            || summary.entrypoint!="/app/provider" || summary.working_directory!="/app"
                            || summary.final_entries!=3 || summary.reserved_unpacked_bytes>512*MIB {
                            bail!("native image filesystem differs from reproduced artifact");
                        }
                        if export {
                            let mut original=Some(source);
                            let mut exporting=SourceMatchedQuarantine::begin_image_export(&mut original,&evidence,&observed)?;
                            if original.is_some() {bail!("image export did not take exclusive original source ownership");}
                            exporting.run().await?;
                            let (returned,mut sealed)=exporting.finish()?;
                            sealed.require_source(&returned)?;
                            if returned.expires()!=original_expires || (!execute && sealed.summary().archive.size!=688_128)
                                || sealed.summary().filesystem.image.config!=expected_image.config {
                                bail!("native staged image export differs from original authored fixture/deadline");
                            }
                            if import {
                                let mut original=Some(returned);let mut export=Some(sealed);
                                let mut importing=SourceMatchedQuarantine::begin_image_import(&mut original,&mut export)?;
                                if original.is_some() || export.is_some(){bail!("image importer did not take its actual original source/export");}
                                importing.run().await?;
                                importing.verified()?.require_source(importing.source()?)?;
                                if importing.source()?.expires()!=original_expires || importing.verified()?.config()!=expected_image.config {
                                    bail!("native import changed its original source/image/deadline");
                                }
                                if adopt {
                                    let (mut returned,image)=importing.adopt().await?;
                                    image.require_source(&returned)?;
                                    if Fence::load(&returned.job_lease())?.is_some() || (!execute && image.image().config.to_string()!="sha256:452c1ee955f698cad26d3427503f00371c288cf0db5d42fe6c433aad59c94ad8") {bail!("image handoff did not release only its old actor fence");}
                                    // Actual independently inspected native measurement,
                                    // NOT publisher execution. The passive image must
                                    // remain owned across this distinct root operation.
                                    returned.source.job.measure_host(original_expires).await?.report()?;
                                    if let Some(fixture)=retained_fixture.as_ref() {
                                        if updating {
                                            Box::pin(crate::marketplace::grant_store::installation::activation::run_compiled_activation_fixture(
                                                returned,&image,&evidence,fixture,&root,&deployment,Some(&mut update_checkpoint))).await?;
                                        } else {
                                            Box::pin(retained::run(returned,&image,&evidence,fixture,&root)).await?;
                                        }
                                        if variant.starts_with("container-image-activation") {
                                            println!("offline native build {variant}: actual probe/candidate/owner activation or failed-readiness cleanup; source/protection observations synthetic");
                                        } else {
                                            println!("offline native build {variant}: actual archive/new-job rematch/reimport/execution; no installed activation claim");
                                        }
                                        return Ok(());
                                    } else if let Some(fixture)=grant_fixture.as_ref() {
                                        returned=crate::marketplace::grant_store::invocation::native_tests::run_compiled(
                                            returned,&image,fixture,&root,&package,&proof,&deployment).await?;
                                    } else if execute {
                                        let view=image.runtime_view(&returned)?;
                                        view.check()?;
                                        let mut broker=ContainerBroker { source: returned.execution_descriptor(), native: None, calls: 0, retired: false };
                                        let limits=elixir_marketplace::source_execution::SourceExecutionLimits {
                                            profile: Default::default(), heap_bytes:32*MIB, timeout_millis:10_000,
                                            max_file_bytes:8*MIB,json_bytes:MIB,pending_jobs:128,
                                            sdk:Some(elixir_marketplace::source_execution::SourceSdkLimits {calls:8,request_bytes:MIB,response_bytes:MIB}),
                                        };
                                        let started=Instant::now();
                                        let invoked=returned.invoke_container(&image, limits,
                                            &elixir_marketplace::source_execution::RuntimeJson::parse(b"null")?,&mut broker).await;
                                        println!("compiled fixture invocation: elapsed_ms={} sdk_calls={} scope_retired={}",started.elapsed().as_millis(),broker.calls,broker.retired);
                                        let result=invoked.context("original-source compiled fixture invocation")?;
                                        if result.as_str()!="true" || broker.calls!=4 || !broker.retired {bail!("original-source compiled SDK invocation did not complete and revoke its scope");}
                                        image.require_source(&returned)?;
                                        view.check()?;
                                        runtime_view=Some(view);
                                    }
                                    image.require_source(&returned)?;adopted_image=Some(image);source=returned;
                                } else {
                                    importing.cancel().await?;
                                    if importing.verified().is_ok(){bail!("retired image import retained usable evidence");}
                                    source=importing.take_cancelled_source()?;
                                    super::super::image_import::require_control_absent(&source.job_lease())?;
                                }
                            } else {
                                // Export qualification alone never imports.
                                sealed.discard().await?;source=returned;
                            }
                            if lane.join("image-export/image.blob").exists() {bail!("retired image export file remains");}
                        }
                    }
                    if source.expires()!=original_expires {bail!("filesystem check renewed its original deadline");}
                } else if variant.starts_with("container-image") {
                    let graph=source.check_container_image(&evidence).await;
                    if variant=="container-image-invalid" {
                        let error=graph.err().context("reproduced malformed image was admitted")?;
                        if !format!("{error:#}").contains("native container image graph rejected") {return Err(error).context("invalid OCI did not reach the actual native graph rejection");}
                    } else {
                        let graph=graph?;
                        if graph.summary().artifact.sha256!=release.payload().artifacts[0].blob.sha256
                            || graph.summary().command.entrypoint!=["/app/provider"] || graph.summary().layers.len()!=1 {
                            bail!("native OCI graph differs from its reproduced source artifact");
                        }
                    }
                    if source.expires()!=original_expires {bail!("container graph check renewed its original deadline");}
                }
                source.discard_retained().await?;
                if let Some(view)=runtime_view {
                    if view.check().is_ok(){bail!("disposed source/image left a usable compiled SDK image view");}
                }
                if let Some(image)=adopted_image {
                    if image.require_source(&source).is_ok(){bail!("disposed image reference retained usable runtime evidence");}
                    super::super::image_reference::require_absent(&source.job_lease())?;
                    super::super::image_import::require_control_absent(&source.job_lease())?;
                }
            } else {
                if built.is_ok() || build.finish().is_ok() {bail!("invalid/unfinished reproduction returned verified output");}
                build.cancel().await?;
                if build.source()?.expires()!=original_expires {bail!("failed build renewed its deadline");}
                build.take_cancelled_source()?.discard_retained().await?;
            }
            if fs_exists(&lane.join("build-inputs/releases/.pending")) {bail!("build staging retained payload files after joined cleanup");}
            println!("offline native build {variant}: exact original source owner, package/proof-only staging, no activation claim");
            Ok::<_,anyhow::Error>(())
        })).catch_unwind().await.unwrap_or_else(|_| Err(anyhow::anyhow!("native build fixture panicked; original cleanup remains required")));
        // Exact source-role recovery only. Keep any uncertain fence for audit;
        // never force-reset it or infer process retirement from a missing name.
        let cleanup = Box::pin(async {
            if lane.exists() {
                // An operation error can drop its build owner and start that
                // SAME retained cleanup task. Wait for its actual OS lock;
                // do not race it or interpret a live owner as cleanup failure.
                timeout(Duration::from_secs(30), async {
                    loop {
                        match ControllerLease::acquire_for_recovery(&lane) {
                            Ok(lease) => {
                                SourceQuarantineJob::recover_leased(runtime.as_ref(), &lease)
                                    .await?;
                                return Ok::<_, anyhow::Error>(());
                            }
                            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                        }
                    }
                })
                .await
                .context("original fixture cleanup owner still holds the lane")??;
            }
            let mut roles = vec![(deployment.clone(), volume.clone(), labels.clone())];
            if matches!(
                variant,
                "container-image-retained" | "container-image-installed"
            ) || variant.starts_with("container-image-activation")
            {
                let cold = format!("{deployment}-cold");
                let cold_labels = HashMap::from([
                    (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                    (ELIXIR_DEPLOYMENT_ID_LABEL.into(), cold.clone()),
                    ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
                ]);
                roles.push((
                    cold.clone(),
                    format!("elixir-marketplace-source-{cold}"),
                    cold_labels,
                ));
                let installed = format!("{deployment}-installed");
                let installed_labels = HashMap::from([
                    (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                    (ELIXIR_DEPLOYMENT_ID_LABEL.into(), installed.clone()),
                    ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
                ]);
                roles.push((
                    installed.clone(),
                    format!("elixir-marketplace-source-{installed}"),
                    installed_labels,
                ));
            }
            for (deployment, volume, labels) in roles {
                SourceQuarantineJob::remove_stopped_writer(runtime.as_ref(), &deployment).await?;
                if runtime.private_file_volume_owned(&volume, &labels).await? {
                    runtime.remove_private_file_volume(&volume, &labels).await?;
                }
                if runtime
                    .get_container_handle(&writer_name(&deployment))
                    .await?
                    .is_some()
                {
                    bail!("fixture writer remains");
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if let Err(error) = cleanup {
            let retained = directory
                .or_else(|| shared_directory.take())
                .context("fixture root missing")?
                .keep();
            bail!(
                "offline build {variant} cleanup unconfirmed: {error:#}; retained at {}; operation: {}",
                retained.display(),
                operation.err().map(|e| e.to_string()).unwrap_or_default()
            );
        }
        if let Err(error) = operation {
            let retained = directory
                .or_else(|| shared_directory.take())
                .context("fixture root missing")?
                .keep();
            return Err(error).with_context(|| {
                format!(
                    "offline build {variant} failed; joined cleanup, fixtures retained at {}",
                    retained.display()
                )
            });
        }
    }
    Ok(())
}
fn fs_exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

#[test]
fn multi_phase_fixture_wrappers_keep_bounded_futures_without_a_larger_thread_stack() {
    fn bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    // No future is created/polled and no native work or grant is fabricated.
    let direct = bytes(|| native_variants_with_runtime(&[], true, true, true, true, true));
    let nested = bytes(|| native_variants(&[], true, false));
    assert!(
        direct <= 128 * 1024,
        "direct fixture future too large: {direct}"
    );
    assert!(
        nested <= 128 * 1024,
        "nested fixture future too large: {nested}"
    );
}
