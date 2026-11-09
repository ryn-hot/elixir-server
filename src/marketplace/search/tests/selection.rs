//! Real local signatures with synthetic source/protection/current-state facts.
//! No public peer, native process, install or transfer-authority qualification.
use super::*;

async fn result() -> Result<(SearchService, Arc<Fake>, Uuid, ExtensionId, Digest)> {
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    let view = ready(&service, id).await?;
    let card = view
        .results
        .first()
        .context("verified fixture card missing")?;
    Ok((
        service,
        backend,
        id,
        card.extension_id.clone(),
        card.release_id,
    ))
}
fn peer(index: usize) -> PeerKey {
    PeerKey::try_from(format!("{index:064x}")).unwrap()
}

#[tokio::test]
async fn only_one_exact_admitted_selection_is_retained_and_catalog_hints_are_not_download_authority()
-> Result<()> {
    let (service, backend, id, extension, release) = result().await?;
    *backend.catalog_hints.lock().unwrap() = vec![peer(2), peer(1), peer(2)];
    let selected = service.selected(id, &extension, release, |_| true).await?;
    assert_eq!(selected.value.source.release_id(), release);
    assert_eq!(
        selected
            .value
            .metadata
            .descriptor()
            .release
            .payload
            .extension_id,
        extension
    );
    assert_eq!(selected.peers, vec![peer(2), peer(1)]);
    assert!(
        service
            .selected(id, &extension, release, |_| true)
            .await
            .is_err()
    );
    // Replacing search cannot pin a second backend/session through retained
    // selections. The existing selected fact remains bounded until dropped.
    let next = service.start_with(backend, request("example"))?;
    ready(&service, next).await?;
    assert!(
        service
            .selected(next, &extension, release, |_| true)
            .await
            .is_err()
    );
    drop(selected);
    drop(
        service
            .selected(next, &extension, release, |_| true)
            .await?,
    );
    Ok(())
}

#[tokio::test]
async fn raw_ids_other_queries_other_owners_and_unadmitted_releases_cannot_select_a_card()
-> Result<()> {
    let (service, _, id, extension, release) = result().await?;
    let other = descriptor(1, 1)?.release.payload.extension_id;
    assert!(
        service
            .selected(Uuid::new_v4(), &extension, release, |_| true)
            .await
            .is_err()
    );
    assert!(
        service
            .selected(id, &other, release, |_| true)
            .await
            .is_err()
    );
    assert!(
        service
            .selected(id, &extension, Digest::of(b"unadmitted"), |_| true)
            .await
            .is_err()
    );
    assert!(
        service
            .selected(id, &extension, release, |_| false)
            .await
            .is_err()
    );
    // Every rejection releases the single retained-selection slot.
    drop(service.selected(id, &extension, release, |_| true).await?);
    service.request_stop();
    assert!(
        service
            .selected(id, &extension, release, |_| true)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn replacement_owner_or_result_removal_during_an_await_cannot_return_old_facts() -> Result<()>
{
    for failure in ["query", "owner", "removed", "protection", "revocation"] {
        let (service, backend, id, extension, release) = result().await?;
        backend.hold_catalog_hints.store(true, Ordering::SeqCst);
        let owner = AtomicBool::new(true);
        let mut select =
            Box::pin(service.selected(id, &extension, release, |_| owner.load(Ordering::SeqCst)));
        tokio::select! {
            value = &mut select => { value?; bail!("selection did not pause at the hint boundary"); },
            _ = backend.hints_started.notified() => {}
        }
        match failure {
            "query" => {
                service.start_with(backend.clone(), request("replacement"))?;
            }
            "owner" => {
                owner.store(false, Ordering::SeqCst);
            }
            "removed" => {
                service
                    .inner
                    .slot
                    .lock()
                    .unwrap()
                    .query
                    .as_mut()
                    .unwrap()
                    .values
                    .clear();
            }
            "protection" => {
                for owner in backend.owners.lock().unwrap().iter() {
                    owner(false);
                }
            }
            "revocation" => {
                backend.current.store(false, Ordering::SeqCst);
            }
            _ => unreachable!(),
        }
        backend.hints_resume.notify_one();
        assert!(select.await.is_err(), "{failure}");
        assert_eq!(service.selections.available_permits(), 1);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn expiry_or_timeout_after_lookup_never_hands_off_an_old_receipt() -> Result<()> {
    let (service, backend, id, extension, release) = result().await?;
    tokio::time::pause();
    backend.hold_catalog_hints.store(true, Ordering::SeqCst);
    let mut select = Box::pin(service.selected(id, &extension, release, |_| true));
    tokio::select! {
        value = &mut select => { value?; bail!("selection did not pause"); },
        _ = backend.hints_started.notified() => {}
    }
    tokio::time::advance(Duration::from_secs(301)).await;
    backend.hints_resume.notify_one();
    assert!(select.await.is_err());
    assert_eq!(service.selections.available_permits(), 1);
    assert!(
        service
            .selected(id, &extension, release, |_| true)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_selection_releases_both_bounded_slots_without_fetching_any_body() -> Result<()> {
    let (service, backend, id, extension, release) = result().await?;
    backend.hold_catalog_hints.store(true, Ordering::SeqCst);
    let mut select = Box::pin(service.selected(id, &extension, release, |_| true));
    tokio::select! {
        value = &mut select => { value?; bail!("selection did not pause"); },
        _ = backend.hints_started.notified() => {}
    }
    drop(select);
    assert_eq!(service.selections.available_permits(), 1);
    assert_eq!(service.views.available_permits(), 1);
    drop(service.selected(id, &extension, release, |_| true).await?);
    Ok(())
}

#[tokio::test]
async fn oversized_catalog_hint_sets_are_rejected_instead_of_increasing_the_peer_budget()
-> Result<()> {
    let (service, backend, id, extension, release) = result().await?;
    *backend.catalog_hints.lock().unwrap() = (1..=7).map(peer).collect();
    assert!(
        service
            .selected(id, &extension, release, |_| true)
            .await
            .is_err()
    );
    backend.catalog_hints.lock().unwrap().clear();
    drop(service.selected(id, &extension, release, |_| true).await?);
    Ok(())
}
