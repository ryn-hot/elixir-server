//! Pagination/queue scheduling only. These raw fixture rows are deliberately
//! unverified and cannot establish source, discovery, install or native authority.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Failure {
    Queue,
    PeerText,
    UnknownTransport,
}
struct Pressure {
    catalog: Mock,
    remaining: AtomicUsize,
    permanent: bool,
    failure: Failure,
    calls: AtomicUsize,
    started: tokio::sync::Notify,
}
impl Pressure {
    fn new(remote: bool, pages: Vec<Vec<SearchResult>>) -> Self {
        let mut catalog = Mock::new();
        if remote {
            catalog.catalog(1, pages);
        } else {
            catalog.pages.insert("local".into(), pages);
        }
        Self {
            catalog,
            remaining: 0.into(),
            permanent: false,
            failure: Failure::Queue,
            calls: 0.into(),
            started: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait]
impl Transport for Pressure {
    async fn peers(&self, interest: &Interest) -> Result<Vec<PeerKey>> {
        self.catalog.peers(interest).await
    }
    async fn fetch(
        &self,
        location: &Location,
        query: &SearchQuery,
        cursor: Option<String>,
        interest: &Interest,
    ) -> Result<SearchPage> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let remaining = self.remaining.load(Ordering::SeqCst);
        if self.permanent || remaining > 0 {
            if remaining > 0 {
                self.remaining.fetch_sub(1, Ordering::SeqCst);
            }
            self.started.notify_one();
            return match self.failure {
                Failure::Queue => Err(ParticipationClient::fixture_queue_busy()),
                Failure::PeerText => bail!("marketplace request slot is unavailable"),
                Failure::UnknownTransport => bail!("original transport outcome is unknown"),
            };
        }
        self.catalog.fetch(location, query, cursor, interest).await
    }
}

#[tokio::test(start_paused = true)]
async fn known_local_pressure_preserves_the_exact_local_or_peer_continuation() -> Result<()> {
    for remote in [false, true] {
        let (_inner, interest, mut request) = context(1);
        let transport = Pressure::new(remote, vec![vec![candidate(1)], vec![candidate(2)]]);
        let mut session = if remote {
            Session::for_update(DiscoveryProgress::default())
        } else {
            Session::default()
        };
        let first = session.read(&transport, &request, &interest).await?;
        assert_eq!(first.results.len(), 1);
        request.cursor = first.next_cursor;
        assert!(request.cursor.is_some());
        transport.remaining.store(1, Ordering::SeqCst);
        let second = session.read(&transport, &request, &interest).await?;
        assert_eq!(
            second.results[0].descriptor.to_bytes()?,
            candidate(2).descriptor.to_bytes()?
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 3);
        let calls = transport.catalog.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            2,
            "known-unsent waits are not real source requests"
        );
        assert_eq!(calls[1].1, Some(format!("{}_1", calls[0].0)));
        assert_eq!(session.page_requests, 1);
        assert_eq!(session.requests, 2);
        assert_eq!(session.sources.len(), 1);
        assert_eq!(session.sources[0].pages, 2);
        assert!(!session.sources[0].rejected);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn permanent_queue_pressure_expires_without_false_empty_success_or_peer_rejection()
-> Result<()> {
    let (inner, interest, request) = context(1);
    // Keep the actual original interest live past this operation's fixed queue
    // wait, as setup may do. Polling must not renew that capacity deadline.
    inner
        .slot
        .lock()
        .unwrap()
        .query
        .as_mut()
        .unwrap()
        .preparation_until = Some(Instant::now() + Duration::from_secs(3600));
    let mut transport = Pressure::new(true, vec![vec![candidate(1)]]);
    transport.permanent = true;
    let mut session = Session::for_update(DiscoveryProgress::default());
    let start = Instant::now();
    let error = session
        .read(&transport, &request, &interest)
        .await
        .err()
        .context("queue must expire")?;
    assert!(ParticipationClient::queue_busy(&error));
    assert_eq!(start.elapsed(), Duration::from_secs(30));
    assert_eq!(transport.calls.load(Ordering::SeqCst), 120);
    assert!(transport.catalog.calls.lock().unwrap().is_empty());
    assert_eq!(session.sources.len(), 1);
    assert!(!session.sources[0].rejected && !session.sources[0].done);
    assert_eq!(session.page_requests, 0);
    assert_eq!(session.requests, 0);
    assert_eq!(session.sources[0].pages, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn replacement_ends_the_same_page_wait_before_another_fetch() -> Result<()> {
    let (inner, interest, request) = context(1);
    let mut transport = Pressure::new(true, vec![vec![candidate(1)]]);
    transport.permanent = true;
    let transport = Arc::new(transport);
    let querying = transport.clone();
    let task = tokio::spawn(async move {
        let mut session = Session::for_update(DiscoveryProgress::default());
        let result = session.read(querying.as_ref(), &request, &interest).await;
        (session, result)
    });
    transport.started.notified().await;
    inner.slot.lock().unwrap().query = None;
    tokio::time::advance(Duration::from_millis(250)).await;
    let (session, result) = task.await?;
    assert!(result.is_err());
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    assert!(!session.sources[0].rejected);
    assert_eq!(session.requests, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn peer_text_or_unknown_transport_cannot_replay_a_page_request() -> Result<()> {
    for failure in [Failure::PeerText, Failure::UnknownTransport] {
        let (_inner, interest, request) = context(1);
        let mut transport = Pressure::new(true, vec![vec![candidate(1)]]);
        transport.failure = failure;
        transport.remaining.store(1, Ordering::SeqCst);
        let mut session = Session::for_update(DiscoveryProgress::default());
        let page = session.read(&transport, &request, &interest).await?;
        assert!(page.results.is_empty());
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert!(transport.catalog.calls.lock().unwrap().is_empty());
        assert!(session.sources[0].rejected);
        assert_eq!(session.requests, 1);
    }
    Ok(())
}
