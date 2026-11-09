//! Actual offline Docker/source-owner composition. Public-source liveness is
//! an explicit TEST observation; this does not qualify public discovery,
//! container execution, real paid credentials, playback or production launch.
use super::*;
use crate::runtime::docker::DockerRuntimeManager;
use base64::{Engine as _, engine::general_purpose::STANDARD};

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
    native_variants_with_import(variants,filesystem,export,false).await
}
#[tokio::test]
#[ignore = "explicit offline original-source-owner image import/retirement; no publisher execution"]
async fn actual_owned_reproduction_export_daemon_import_inspection_and_retirement()->Result<()> {
    native_variants_with_import(&["container-image"],true,true,true).await
}
async fn native_variants_with_import(variants: &[&str], filesystem: bool, export: bool, import:bool) -> Result<()> {
    native_variants_with_handoff(variants,filesystem,export,import,false).await
}
#[tokio::test]
#[ignore="explicit original-source image reference handoff across another native phase; no publisher execution"]
async fn actual_owned_import_reference_handoff_native_phase_and_disposal()->Result<()> {
    native_variants_with_handoff(&["container-image"],true,true,true,true).await
}
async fn native_variants_with_handoff(variants:&[&str],filesystem:bool,export:bool,import:bool,adopt:bool)->Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let toolchain = std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?;
    if !image.starts_with("sha256:")
        || !image_is_pinned(&image)
        || !toolchain.starts_with("sha256:")
        || !image_is_pinned(&toolchain)
    {
        bail!("explicit immutable local fixture images required");
    }
    let mut manager=DockerRuntimeManager::new(None);
    if import {
        let profile=crate::runtime::image_import::Profile::from_operator_configuration(
            std::path::PathBuf::from(std::env::var_os("ELIXIR_TEST_IMAGE_IMPORT_BINARY").context("explicit host import companion missing")?).canonicalize()?,
            std::env::var("ELIXIR_TEST_IMAGE_IMPORT_BINARY_SHA256")?.parse()?,
            std::path::PathBuf::from(std::env::var_os("ELIXIR_TEST_IMAGE_IMPORT_SOCKET").context("explicit local daemon socket missing")?).canonicalize()?)?;
        manager=manager.with_marketplace_import_profile(profile);
    }
    let runtime = Arc::new(manager);
    for &variant in variants {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let lane = root.join("lane");
        let deployment = format!("compiled-fixture-{}", uuid::Uuid::new_v4().simple());
        let volume = format!("elixir-marketplace-source-{deployment}");
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
            ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
        ]);
        let operation=async {
            let script=Path::new(env!("CARGO_MANIFEST_DIR")).join("../extensions/sdk/marketplace/test/compiled-build-fixture.mjs");
            let fixture=timeout(Duration::from_secs(30),tokio::process::Command::new("node").arg(script)
                .args([toolchain.as_str(),if variant=="drop-before-build"{"match"}else{variant}]).kill_on_drop(true).output()).await??;
            if !fixture.status.success() || fixture.stdout.len()>2*1024*1024 {bail!("offline compiled fixture failed");}
            let value:serde_json::Value=canonical::parse(&fixture.stdout,2*1024*1024)?;
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
                if build.package.is_some() || build.proof.is_some() {
                    bail!("build retained source file descriptors after retiring staging allocation");
                }
                if source.expires()!=original_expires || evidence.release!=release.release_id()
                    || evidence.artifact.sha256!=release.payload().artifacts[0].blob.sha256 {
                    bail!("native build changed its original release/deadline");
                }
                if filesystem {
                    // Both fixtures have a valid exact image graph; the
                    // negative must fail native layer semantics, not parsing.
                    super::super::container_image::descriptor(&source)?.verify(std::io::Cursor::new(&artifact))?;
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
                        if summary.image.artifact.sha256!=release.payload().artifacts[0].blob.sha256
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
                            if returned.expires()!=original_expires || sealed.summary().archive.size!=688_128
                                || sealed.summary().filesystem.image.config.to_string()!="sha256:452c1ee955f698cad26d3427503f00371c288cf0db5d42fe6c433aad59c94ad8" {
                                bail!("native staged image export differs from original authored fixture/deadline");
                            }
                            if import {
                                let mut original=Some(returned);let mut export=Some(sealed);
                                let mut importing=SourceMatchedQuarantine::begin_image_import(&mut original,&mut export)?;
                                if original.is_some() || export.is_some(){bail!("image importer did not take its actual original source/export");}
                                importing.run().await?;
                                importing.verified()?.require_source(importing.source()?)?;
                                if importing.source()?.expires()!=original_expires || importing.verified()?.config().to_string()!="sha256:452c1ee955f698cad26d3427503f00371c288cf0db5d42fe6c433aad59c94ad8" {
                                    bail!("native import changed its original source/image/deadline");
                                }
                                if adopt {
                                    let (mut returned,image)=importing.adopt().await?;
                                    image.require_source(&returned)?;
                                    if Fence::load(&returned.job_lease())?.is_some() || image.image().config.to_string()!="sha256:452c1ee955f698cad26d3427503f00371c288cf0db5d42fe6c433aad59c94ad8" {bail!("image handoff did not release only its old actor fence");}
                                    // Actual independently inspected native measurement,
                                    // NOT publisher execution. The passive image must
                                    // remain owned across this distinct root operation.
                                    returned.source.job.measure_host(original_expires).await?.report()?;
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
        }.await;
        // Exact source-role recovery only. Keep any uncertain fence for audit;
        // never force-reset it or infer process retirement from a missing name.
        let cleanup = async {
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
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = cleanup {
            let retained = directory.keep();
            bail!(
                "offline build {variant} cleanup unconfirmed: {error:#}; retained at {}; operation: {}",
                retained.display(),
                operation.err().map(|e| e.to_string()).unwrap_or_default()
            );
        }
        if let Err(error) = operation {
            let retained = directory.keep();
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
