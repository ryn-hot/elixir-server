//! A bounded background pass AFTER normal fresh first-page browsing.
//! Progress is data, not a global coverage, source/install or serving capability.
//! Uses the existing search task/interest and helper slots; retains no row history.
use super::*;
use elixir_marketplace::peer::PeerResponse;

const MAX_ROWS: usize = 10_000;
const MAX_PAGES: usize = 512;
const MAX_BYTES: usize = 128 * 1024 * 1024;
pub(super) const LIFETIME: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    #[default]
    Checking,
    Discovering,
    Waiting,
    Complete,
    Limited,
    Unavailable,
}

#[derive(Clone, Serialize)]
pub struct Progress {
    pub schema_version: u32,
    pub phase: Phase,
    pub scope: &'static str,
    pub examined: usize,
    pub available: usize,
    pub omitted: usize,
    pub pages: usize,
    pub received: usize,
    pub local_pass_complete: bool,
    pub network_complete: bool,
    pub metadata_only: bool,
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            schema_version: 1,
            phase: Phase::Checking,
            scope: "current_local_catalog",
            examined: 0,
            available: 0,
            omitted: 0,
            pages: 0,
            received: 0,
            local_pass_complete: false,
            network_complete: false,
            metadata_only: true,
        }
    }
}
fn update(inner: &Arc<Inner>, interest: &Interest, edit: impl FnOnce(&mut Progress)) -> Result<()> {
    let mut slot = inner
        .slot
        .lock()
        .map_err(|_| anyhow::anyhow!("catalog preparation owner unavailable"))?;
    let progress = slot
        .query
        .as_mut()
        .filter(|q| q.id == interest.id)
        .and_then(|q| q.preparation.as_mut())
        .context("catalog preparation was replaced")?;
    edit(progress);
    Ok(())
}
fn limited(inner: &Arc<Inner>, interest: &Interest) -> Result<()> {
    update(inner, interest, |p| p.phase = Phase::Limited)?;
    bail!("catalog preparation reached its bounded pass limit")
}
pub(super) fn failed(inner: &Arc<Inner>, interest: &Interest) {
    let _ = update(inner, interest, |p| {
        if !matches!(p.phase, Phase::Limited | Phase::Complete) {
            p.phase = Phase::Unavailable;
        }
    });
}
async fn ready(
    inner: &Arc<Inner>,
    interest: &Interest,
    backend: &dyn Backend,
    until: Instant,
) -> Result<()> {
    loop {
        if Instant::now() >= until {
            return limited(inner, interest);
        }
        if !interest.active() {
            bail!("catalog preparation is no longer requested");
        }
        if backend.preparation_ready()? {
            update(inner, interest, |p| p.phase = Phase::Checking)?;
            return Ok(());
        }
        update(inner, interest, |p| p.phase = Phase::Waiting)?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Clone, Copy)]
pub(super) enum BrowseScope {
    EarlyLocal,
    Ordinary,
}

fn begin_recheck(query: &mut Query) {
    query.phase = SearchPhase::Checking;
    query.total = 0;
    query.checked = 0;
    query.omitted = 0;
    query.next_cursor = None;
}

/// One local-only browse when the first recovery data lands. It runs in the
/// original query task and uses the normal admission and view fences. If this
/// yields nothing, the existing final browse remains available once.
pub(super) struct EarlyBrowse<'a> {
    inner: &'a Arc<Inner>,
    interest: &'a Interest,
    request: &'a SearchRequest,
    backend: &'a Arc<dyn Backend>,
    until: Instant,
    tried: bool,
    published: bool,
}
impl<'a> EarlyBrowse<'a> {
    pub(super) fn new(
        inner: &'a Arc<Inner>,
        interest: &'a Interest,
        request: &'a SearchRequest,
        backend: &'a Arc<dyn Backend>,
        until: Instant,
    ) -> Self {
        Self {
            inner,
            interest,
            request,
            backend,
            until,
            tried: false,
            published: false,
        }
    }
    async fn recover(&mut self) {
        if std::mem::replace(&mut self.tried, true)
            || Instant::now() >= self.until
            || !self.interest.active()
        {
            return;
        }
        let started = self.inner.slot.lock().ok().is_some_and(|mut slot| {
            let Some(query) = slot.query.as_mut().filter(|query| {
                query.id == self.interest.id
                    && query.phase == SearchPhase::Ready
                    && query.values.is_empty()
            }) else {
                return false;
            };
            begin_recheck(query);
            true
        });
        if !started {
            return;
        }
        self.published = self.check(BrowseScope::EarlyLocal).await;
    }
    pub(super) async fn complete(&self) -> bool {
        let active = self.interest.active();
        let retry = self.inner.slot.lock().ok().is_some_and(|mut slot| {
            let Some(query) = slot.query.as_mut().filter(|q| q.id == self.interest.id) else {
                return false;
            };
            let Some(progress) = query.preparation.as_mut() else {
                return false;
            };
            // Never expose Ready + Complete + empty before a scheduled final
            // recheck. The desktop would treat that combination as terminal.
            progress.phase = Phase::Complete;
            progress.local_pass_complete = true;
            if !active || !query.values.is_empty() {
                return false;
            }
            begin_recheck(query);
            true
        });
        if retry {
            self.check(BrowseScope::Ordinary).await
        } else {
            self.published
        }
    }
    async fn check(&self, scope: BrowseScope) -> bool {
        let result = match self.backend.reset_browse(scope).await {
            Ok(()) => {
                process(
                    self.inner,
                    self.interest,
                    self.request,
                    self.backend.clone(),
                )
                .await
            }
            Err(error) => Err(error),
        }
        .inspect_err(|error| {
            crate::marketplace::lifecycle::trace_failure("catalog_browse_refresh", error)
        });
        let early = matches!(scope, BrowseScope::EarlyLocal);
        if let Ok(mut slot) = self.inner.slot.lock()
            && let Some(query) = slot.query.as_mut().filter(|q| q.id == self.interest.id)
        {
            // An unsuccessful early browse is not terminal while preparation
            // can still find data. Don't publish its omissions or cursor as
            // a finished empty result; discard partial observations on error.
            if early && (result.is_err() || query.values.is_empty()) {
                query.values.clear();
                begin_recheck(query);
            }
            query.phase = if early || result.is_ok() {
                SearchPhase::Ready
            } else {
                SearchPhase::Unavailable
            };
            return result.is_ok() && !query.values.is_empty();
        }
        false
    }
}

pub(super) async fn prepare(
    inner: &Arc<Inner>,
    interest: &Interest,
    backend: &dyn Backend,
    until: Instant,
    early: &mut EarlyBrowse<'_>,
) -> Result<()> {
    ready(inner, interest, backend, until).await?;
    update(inner, interest, |p| p.phase = Phase::Discovering)?;
    backend.populate(interest, until, early).await?;
    let (mut after, mut rows, mut pages, mut bytes) = (None, 0usize, 0usize, 0usize);
    loop {
        ready(inner, interest, backend, until).await?;
        if pages == MAX_PAGES || rows == MAX_ROWS {
            return limited(inner, interest);
        }
        let page = loop {
            ready(inner, interest, backend, until).await?;
            match backend.inventory(after.clone(), interest).await {
                Err(error) if ParticipationClient::queue_busy(&error) => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                other => break other?,
            }
        };
        let size = canonical::to_vec(&page)?.len();
        if size > MAX_CONTROL_FRAME_BYTES {
            bail!("catalog inventory frame bound");
        }
        if bytes.checked_add(size).is_none_or(|n| n > MAX_BYTES) {
            return limited(inner, interest);
        }
        PeerResponse {
            schema_version: 1,
            request_id: Some(1),
            response: PeerOutcome::Inventory {
                page: Box::new(page.clone()),
            },
        }
        .validate()?;
        if page
            .entries
            .first()
            .is_some_and(|entry| after.as_ref().is_some_and(|id| entry.extension_id <= *id))
        {
            bail!("catalog inventory did not advance");
        }
        if page.entries.len() > MAX_ROWS - rows {
            return limited(inner, interest);
        }
        bytes += size;
        pages += 1;
        update(inner, interest, |p| p.pages = pages)?;
        let descriptors: Vec<_> = page
            .entries
            .iter()
            .filter_map(|entry| entry.descriptor.clone())
            .collect();
        loop {
            ready(inner, interest, backend, until).await?;
            let result = backend.warm_metadata(&descriptors, interest).await;
            if !interest.active() {
                bail!("catalog preparation was replaced");
            }
            if result.is_err() && !backend.preparation_ready()? {
                continue;
            }
            result?;
            break;
        }
        let next = page.next_after;
        for entry in page.entries {
            let descriptor = entry
                .descriptor
                .context("unrequested catalog hint omission")?;
            loop {
                ready(inner, interest, backend, until).await?;
                let result = backend.warm(descriptor.clone(), interest).await;
                if !interest.active() {
                    bail!("catalog preparation was replaced");
                }
                // Resource/helper/protection deferral is not a negative listing
                // verdict. Retry the SAME bounded row after its owner recovers.
                if result
                    .as_ref()
                    .err()
                    .is_some_and(ParticipationClient::queue_busy)
                {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                if result.is_err() && !backend.preparation_ready()? {
                    continue;
                }
                rows += 1;
                update(inner, interest, |p| {
                    p.examined = rows;
                    if result.is_ok() {
                        p.available += 1;
                    } else {
                        p.omitted += 1;
                    }
                })?;
                if result.is_ok() {
                    early.recover().await;
                }
                tokio::task::yield_now().await;
                break;
            }
        }
        if next.is_none() {
            // The search owner publishes completion together with any
            // first-page recheck, under its one query lock.
            return Ok(());
        }
        after = next;
    }
}

/// Explicit setup only. Reuses the existing protected consumer and worker's
/// durable sync cursors/work/packet budgets; never enables catalog/routing roles.
pub(super) async fn populate(
    client: &ParticipationClient,
    interest: &Interest,
    deadline: Instant,
    early: &mut EarlyBrowse<'_>,
) -> Result<()> {
    use elixir_marketplace::worker_control::CatalogJobPhase;
    let inner = interest
        .inner
        .upgrade()
        .context("catalog preparation owner stopped")?;
    let peers =
        remote::while_interested(interest, remote::discover(client, interest, None)).await?;
    let mut pages = 0;
    for peer in peers.into_iter().take(3) {
        loop {
            if !interest.active() {
                bail!("catalog population is no longer requested");
            }
            if Instant::now() >= deadline {
                return limited(&inner, interest);
            }
            if pages == MAX_PAGES {
                return Ok(());
            } // partial network view, never global completion
            if !client.catalog_preparation_ready()? {
                update(&inner, interest, |p| p.phase = Phase::Waiting)?;
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            update(&inner, interest, |p| p.phase = Phase::Discovering)?;
            let state =
                match remote::while_interested(interest, client.prepare_catalog_page(peer.clone()))
                    .await
                {
                    Ok(Some(state)) => state,
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                    Err(_) => break, // another admitted source may still work; no direct fallback/retry storm
                };
            pages += 1;
            let count = state.admitted + state.unchanged + state.rejected;
            update(&inner, interest, |p| p.received += count)?;
            if state.admitted > 0 {
                early.recover().await;
            }
            if state.phase != CatalogJobPhase::Progress {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
