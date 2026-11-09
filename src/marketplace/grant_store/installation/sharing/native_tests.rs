//! Real SDK archives + SQLite bindings/index + parent file reads + packaged
//! private decoder/control bridge + Noise. Enrollment/admission watches are
//! synthetic: NOT VPN, packet credits, public repositories or native activation.
use super::super::{
    standing::tests::local_http::configured,
    tests::{Fixture, budget, signed_fixture},
};
use super::tests::metadata;
use super::*;
use crate::marketplace::{
    archive::{ArchiveBudget, SharingReader},
    quarantine::peer_tests::{check_profile, docker, remove_peer},
    worker::PeerPipeFixture,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use elixir_marketplace::{
    worker_control::WorkerControlOperation as Op, worker_sharing::SharingState,
};
use futures_util::FutureExt;
use std::{os::unix::fs::PermissionsExt, panic::AssertUnwindSafe, time::Duration};
use tokio::time::{Instant, sleep};

struct Restore(std::path::PathBuf);
impl Drop for Restore {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
    }
}

#[tokio::test]
#[ignore = "explicit pinned on-demand worker, offline packaged Noise/SQL/archive composition; synthetic enrollment, NOT protected/public activation"]
async fn packaged_worker_serves_multiple_active_archives_outside_its_advertised_window()
-> Result<()> {
    packaged_sharing(false).await
}
#[tokio::test]
#[ignore = "explicit pinned on-demand worker, owner-authorized publisher seeding with no paid receipt; synthetic enrollment, NOT public/VPN qualification"]
async fn packaged_worker_seeds_explicit_unqualified_releases_without_paid_metadata() -> Result<()> {
    packaged_sharing(true).await
}
async fn packaged_sharing(publisher: bool) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    anyhow::ensure!(
        crate::marketplace::runtime::image_is_pinned(&image),
        "immutable image required"
    );
    let first = Fixture::new().await?;
    let second = Fixture::with_database_and_actor(
        tempfile::tempdir()?,
        first.db.clone(),
        Some(first.actor.clone()),
        "second-provider",
    )
    .await?;
    let (first_owner, _) = configured(&first, "First").await?;
    let (second_owner, _) = configured(&second, "Second").await?;
    if publisher {
        super::publisher::tests::enable(&first, first_owner).await?;
        super::publisher::tests::enable(&second, second_owner).await?;
        (first.meta_owner)(false);
        (second.meta_owner)(false);
    }
    let blobs = [
        first.descriptor.release.payload.package.clone(),
        first.descriptor.release.payload.source.proof.clone(),
        second.descriptor.release.payload.package.clone(),
        second.descriptor.release.payload.source.proof.clone(),
    ];
    let data = tempfile::tempdir()?;
    let path = data.path().canonicalize()?;
    std::fs::write(path.join("descriptors.json"), canonical::to_vec(&blobs)?)?;
    std::fs::set_permissions(
        path.join("descriptors.json"),
        std::fs::Permissions::from_mode(0o444),
    )?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555))?;
    let _restore = Restore(path.clone());
    let value = signed_fixture().await?;
    let issuer = value["issuer_key"].as_str().context("fixture issuer")?;
    let name = format!("elixir-marketplace-sharing-{}", Uuid::new_v4().simple());
    let mut pipe: Option<PeerPipeFixture> = None;
    let result=AssertUnwindSafe(async {
        pipe=Some(PeerPipeFixture::start(&image,&path,&name,issuer,"sharing").await?);
        check_profile(&name).await?;
        let client=&pipe.as_ref().unwrap().client;
        let generation=Digest::of(Uuid::new_v4().as_bytes());
        let mut renewed=Instant::now();
        // Only one discovery hint: all other installed slots still need to
        // work immediately, not after a rotation makes them visible.
        let inventory=SharingInventory{generation,blobs:vec![blobs[0].clone()]};
        client.sharing_control(Op::SharingSet{inventory:inventory.clone(),lease_ms:15_000,resolve_installed:true}).await?;
        let mut source:Option<SharingSource>=None;
        let mut root=None;
        let mut reader:Option<(Digest,SharingReader)>=None;
        let mut owners=vec![];
        let mut requests=std::collections::BTreeSet::new();let mut unknown=0;
        let deadline=Instant::now()+Duration::from_secs(180);
        loop {
            anyhow::ensure!(Instant::now()<deadline,"packaged sharing deadline");
            if renewed.elapsed()>=Duration::from_secs(3){
                client.sharing_control(Op::SharingSet{inventory:inventory.clone(),lease_ms:15_000,resolve_installed:true}).await?;
                renewed=Instant::now();
            }
            match client.sharing_control(Op::SharingPoll).await? {
                SharingState::Resolve{generation:epoch,request,sha256}=>{
                    assert_eq!(epoch,generation);reader=None;
                    let targets=hints::lookup(&first.db,sha256).await?;
                    if targets.is_empty(){
                        unknown+=1;assert_eq!(sha256,Digest::from_bytes([255;32]));
                        client.sharing_control(Op::SharingAuthorize{generation,request,blob:None}).await?;continue;
                    }
                    let target=&targets[0];let (f,owner)=if target.extension==first.descriptor.release.payload.extension_id {(&first,first_owner)}else{(&second,second_owner)};
                    assert_eq!(target.instance,owner);requests.insert(sha256);
                    let checked=if publisher {
                        let consent=super::publisher::load(&f.db,f.actor.home,&target.extension,owner).await?.context("explicit publisher choice missing")?;
                        let (publication,hold)=crate::marketplace::worker::PublicationObservation::fixture_for_verified_publication(super::publisher::tests::publication(f))?;
                        owners.push(Box::new(hold) as Box<dyn Fn(bool)>);
                        SharingSource::capture_publisher(&f.db,f.actor.home,owner,consent,publication,budget()).await?
                    } else {
                        let (observation,hold)=metadata(f).await?;owners.push(hold);
                        SharingSource::capture(&f.db,f.actor.home,owner,observation,budget()).await?
                    };
                    checked.descriptor(&target.blob)?;
                    root=Some(f.root.path().canonicalize()?.join("archive"));source=Some(checked);
                    client.sharing_control(Op::SharingAuthorize{generation,request,blob:Some(target.blob.clone())}).await?;
                }
                SharingState::Read{generation:epoch,request,blob,offset,maximum}=>{
                    assert_eq!(epoch,generation);assert!(maximum<=64*1024);
                    let source=source.as_ref().context("original source missing")?;
                    source.current(&first.db).await?;
                    if reader.as_ref().is_none_or(|(id,_)|*id!=request){
                        assert_eq!(offset,0);let path=root.clone().context("original archive missing")?;let expected=source.descriptor(&blob)?;let blob=blob.clone();
                        let file=tokio::task::spawn_blocking(move||SharingReader::open(&path,ArchiveBudget{bytes:128*1024*1024,releases:4},&expected,&blob)).await??;
                        reader=Some((request,file));
                    }
                    let (_,mut file)=reader.take().context("original reader missing")?;
                    let (file,bytes)=tokio::task::spawn_blocking(move||{let bytes=file.read(offset,maximum);(file,bytes)}).await?;
                    reader=Some((request,file));let bytes=bytes?;
                    source.current(&first.db).await?;
                    let state=client.sharing_control(Op::SharingChunk{generation,request,offset,data:Some(URL_SAFE_NO_PAD.encode(bytes))}).await?;
                    assert!(matches!(state,SharingState::Accepted{request:actual,offset:at} if actual==request && at==offset));
                }
                SharingState::Ready{serving:false,..}=>{
                    reader=None;
                    let status=docker(&["exec","--user=10004:10004",&name,"/usr/local/bin/node","-e",
                        "process.stdout.write(require('node:fs').readFileSync('/state/sharing-result.json'))"]).await?;
                    let status:serde_json::Value=serde_json::from_str(&status)?;
                    anyhow::ensure!(status["phase"]!="failed","packaged receiver failed: {status}");
                    if status["phase"]=="complete"{
                        assert_eq!(status["unknown_rejected"],true);assert_eq!(status["receipts"].as_array().unwrap().len(),4);
                        assert_eq!(requests.len(),4);assert_eq!(unknown,1);break;
                    }
                }
                SharingState::Ready{..}=>{},
                _=>bail!("unexpected sharing status"),
            }
            sleep(Duration::from_millis(20)).await;
        }
        client.sharing_control(Op::SharingSet{inventory:SharingInventory{generation:Digest::of(b"retire exact test sharing epoch"),blobs:vec![]},lease_ms:1,resolve_installed:false}).await?;
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    let joined = match pipe.take() {
        Some(pipe) => pipe.finish().await,
        None => Ok(()),
    };
    let removed = remove_peer(&name).await;
    match result {
        Ok(result) => {
            result?;
            joined?;
            removed?;
        }
        Err(error) => {
            let _ = joined;
            let _ = removed;
            std::panic::resume_unwind(error);
        }
    }
    Ok(())
}
