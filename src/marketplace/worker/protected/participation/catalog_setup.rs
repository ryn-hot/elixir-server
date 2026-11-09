//! One explicitly requested page in the existing cooperative request slot.
//! No serving promotion, new credit, peer address input, job queue or actor.
use super::*;

pub(super) struct Page {
    pub peer: PeerKey,
    pub started: bool,
    pub reconnect_until: Option<Instant>,
    pub admission: super::super::source_preflight::Client,
    pub reply: oneshot::Sender<Result<Option<CatalogJobState>>>,
}
enum Decision {
    Wait,
    Start,
    Finish,
}
enum Reachability {
    Ready,
    Refresh,
    Wait,
}
fn reachable(present: bool, until: Option<Instant>) -> Result<Reachability> {
    if present {
        return Ok(Reachability::Ready);
    }
    match until {
        None => Ok(Reachability::Refresh),
        Some(until) if Instant::now() < until => Ok(Reachability::Wait),
        Some(_) => bail!("catalog peer did not reconnect"),
    }
}
fn decision(state: &CatalogJobState, peer: &PeerKey, started: bool) -> Result<Decision> {
    if !started {
        return Ok(if state.phase == CatalogJobPhase::Running {
            Decision::Wait
        } else {
            Decision::Start
        });
    }
    if state.kind != Some(CatalogJobKind::Population)
        || state.peer.as_ref() != Some(peer)
        || state.phase == CatalogJobPhase::Idle
    {
        bail!("catalog setup page ownership changed");
    }
    Ok(if state.phase == CatalogJobPhase::Running {
        Decision::Wait
    } else {
        Decision::Finish
    })
}
pub(super) async fn step(network: &mut Live, mut page: Page) -> Option<Request> {
    // The worker still owns/joins any already-started page after this waiter
    // disappears. Dropping a status waiter never aborts an uncertain commit.
    if page.reply.is_closed() {
        return None;
    }
    let result = async {
        if !network.applied.is_some_and(|roles| roles.consumer)
            || !network.worker.catalog_jobs_supported()
            || !network.worker.catalog_sync_supported()
        {
            bail!("protected catalog synchronization is unavailable");
        }
        let Some(state) = network
            .worker
            .catalog_job(WorkerControlOperation::CatalogJobStatus)
            .await?
        else {
            return Ok((false, None));
        };
        match decision(&state, &page.peer, page.started)? {
            Decision::Wait => Ok((true, None)),
            Decision::Finish => {
                network.catalog_progress.observed(&state);
                Ok((false, Some(state)))
            }
            Decision::Start => {
                // Recheck the actual protected helper owner at dispatch, not
                // just a caller's old "ready" observation. A busy mirror yields
                // this request so its security update can use the same slot.
                if !page.admission.catalog_preparation_ready()?
                    || !network.security_mirror.current(&network.database).await?
                {
                    return Ok((false, None));
                }
                if page.reply.is_closed() {
                    return Ok((false, None));
                }
                let present = network
                    .worker
                    .network_status()
                    .await?
                    .peers
                    .contains(&page.peer);
                match reachable(present, page.reconnect_until)? {
                    Reachability::Ready => {}
                    Reachability::Wait => return Ok((true, None)),
                    Reachability::Refresh => {
                        // A previous page's native checks can outlast the
                        // transport's idle timeout. Refresh the SAME bounded
                        // topic manager once, then cooperatively wait for this
                        // exact Noise identity. Never transfer its durable
                        // cursor to another peer or accept an address here.
                        page.reconnect_until =
                            Some(Instant::now() + ParticipationClient::DISCOVERY_WAIT);
                        network.worker.refresh_network().await?;
                        return Ok((true, None));
                    }
                }
                let Some(state) = network
                    .worker
                    .catalog_job(WorkerControlOperation::CatalogSync {
                        peer: page.peer.clone(),
                        max_control_bytes: CATALOG_CONTROL_BUDGET,
                    })
                    .await?
                else {
                    return Ok((false, None));
                };
                page.started = true;
                network.catalog_progress.started(&state);
                match decision(&state, &page.peer, true)? {
                    Decision::Wait => Ok((true, None)),
                    Decision::Finish => {
                        network.catalog_progress.observed(&state);
                        Ok((false, Some(state)))
                    }
                    Decision::Start => unreachable!(),
                }
            }
        }
    }
    .await;
    match result {
        Ok((true, _)) => Some(Request::CatalogSetup(page)),
        Ok((false, state)) => {
            let _ = page.reply.send(Ok(state));
            None
        }
        Err(error) => {
            let _ = page.reply.send(Err(error));
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state(phase: CatalogJobPhase) -> CatalogJobState {
        CatalogJobState {
            phase,
            kind: Some(CatalogJobKind::Population),
            peer: Some(PeerKey::try_from("1".repeat(64)).unwrap()),
            admitted: 0,
            unchanged: 0,
            rejected: 0,
            indexed: 0,
            removed: 0,
        }
    }
    #[tokio::test(start_paused = true)]
    async fn a_lost_peer_gets_one_bounded_refresh_not_an_unbounded_retry_loop() -> Result<()> {
        assert!(matches!(reachable(true, None)?, Reachability::Ready));
        assert!(matches!(reachable(false, None)?, Reachability::Refresh));
        let until = Some(Instant::now() + ParticipationClient::DISCOVERY_WAIT);
        for _ in 0..ParticipationClient::DISCOVERY_WAIT.as_secs() - 1 {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(matches!(reachable(false, until)?, Reachability::Wait));
            assert!(matches!(reachable(true, until)?, Reachability::Ready));
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(reachable(false, until).is_err());
        Ok(())
    }
    #[test]
    fn only_the_started_same_peer_page_can_finish_the_original_waiter() -> Result<()> {
        let peer = PeerKey::try_from("1".repeat(64))?;
        assert!(matches!(
            decision(&state(CatalogJobPhase::Running), &peer, false)?,
            Decision::Wait
        ));
        assert!(matches!(
            decision(&state(CatalogJobPhase::Complete), &peer, false)?,
            Decision::Start
        ));
        assert!(matches!(
            decision(&state(CatalogJobPhase::Running), &peer, true)?,
            Decision::Wait
        ));
        for phase in [
            CatalogJobPhase::Progress,
            CatalogJobPhase::Complete,
            CatalogJobPhase::Limited,
            CatalogJobPhase::Failed,
            CatalogJobPhase::Cancelled,
        ] {
            assert!(matches!(
                decision(&state(phase), &peer, true)?,
                Decision::Finish
            ));
        }
        let mut foreign = state(CatalogJobPhase::Complete);
        foreign.peer = Some(PeerKey::try_from("2".repeat(64))?);
        assert!(decision(&foreign, &peer, true).is_err());
        foreign = state(CatalogJobPhase::Complete);
        foreign.kind = Some(CatalogJobKind::Maintenance);
        assert!(decision(&foreign, &peer, true).is_err());
        assert!(decision(&state(CatalogJobPhase::Idle), &peer, true).is_err());
        Ok(())
    }
}
