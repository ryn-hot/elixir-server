//! Actual offline source-backed Live client -> SAME application execution owner.
//! No HTTP endpoint/readiness flags or replacement controller are constructed.
use super::*;
use crate::live::{
    config::LiveConfig,
    contract::{
        CatalogPageRequest, MetaRequest, ProviderOperation, ProviderRequestContext, ResolveRequest,
        StreamProtocol,
    },
    diagnostics::LiveRedactor,
    provider::LiveProviderClient,
};
use tokio_util::sync::CancellationToken;

// Six fixed observations in this private native fixture, not a benchmark gate
// or runtime telemetry. Preserve the one original future/result; never print
// arguments, IDs, credentials, response bodies or the error chain.
async fn observed<T, E>(
    operation: &'static str,
    call: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, E> {
    let started = std::time::Instant::now();
    let result = call.await;
    eprintln!(
        "native source Live {operation}: elapsed_ms={} success={}",
        started.elapsed().as_millis(),
        result.is_ok()
    );
    result
}

pub(super) async fn qualify(
    database: &Database,
    state: &crate::state::AppState,
    home: Uuid,
    user: Uuid,
    instance: Uuid,
) -> Result<()> {
    let mut limits = LiveConfig::default().providers;
    // Functional cold-native qualification only, not production latency proof.
    limits.source_request_timeout_seconds = 60;
    limits.source_hard_timeout_seconds = 120;
    let client = LiveProviderClient::new_with_marketplace(
        database.clone(),
        state.marketplace.clone(),
        limits,
        Arc::new(LiveRedactor::default()),
    )?;
    let providers = client.directory().discover().await?;
    assert_eq!(
        providers.len(),
        1,
        "cancelled B must not register a provider"
    );
    let snapshot = &providers[0];
    assert_eq!(snapshot.instance_id, instance);
    assert!(
        snapshot.operation_url(ProviderOperation::Health).is_err(),
        "source transport must have no HTTP URL fallback"
    );
    let descriptor = state
        .marketplace
        .source_provider(database, snapshot.provider_id)
        .await?
        .context("registered source provider")?;
    assert_eq!(descriptor.home, home);
    let before = sdk_rows(database).await?;
    let cancellation = CancellationToken::new();
    let context = ProviderRequestContext {
        locale: "en-US".into(),
        timezone: "UTC".into(),
        now: chrono::Utc::now(),
    };
    assert!(
        client
            .catalogs(snapshot, Uuid::new_v4(), &context, &cancellation)
            .await
            .is_err(),
        "foreign user cannot execute source code"
    );
    assert_eq!(sdk_rows(database).await?, before);
    let health = observed("health_initial", client.health(snapshot, &cancellation)).await?;
    assert_eq!(
        health.contract_versions.into_iter().collect::<Vec<_>>(),
        [1]
    );
    let catalogs = observed(
        "catalogs",
        client.catalogs(snapshot, user, &context, &cancellation),
    )
    .await?;
    assert_eq!(catalogs.catalogs.len(), 1);
    assert_eq!(catalogs.catalogs[0].id, "events");
    let page = observed(
        "catalog",
        client.catalog(
            snapshot,
            user,
            &context,
            &CatalogPageRequest {
                catalog_id: "events".into(),
                cursor: None,
                limit: 10,
                filters: Default::default(),
            },
            &cancellation,
        ),
    )
    .await?;
    assert_eq!(page.items.len(), 1);
    assert!(page.diagnostics.is_empty());
    let item_id = page.items[0].id.clone();
    let meta = observed(
        "meta",
        client.meta(
            snapshot,
            user,
            &context,
            &MetaRequest {
                item_id: item_id.clone(),
            },
            &cancellation,
        ),
    )
    .await?;
    assert_eq!(meta.streams.len(), 1);
    let resolved = observed(
        "resolve",
        client.resolve(
            snapshot,
            user,
            &context,
            &ResolveRequest {
                item_id,
                stream_id: meta.streams[0].id.clone(),
            },
            &cancellation,
        ),
    )
    .await?;
    assert_eq!(resolved.descriptor.protocol, StreamProtocol::Hls);
    assert_eq!(
        sdk_rows(database).await?,
        before,
        "read-only provider operations must not rerun migration"
    );

    // A normal control writer takes parent locks and advances choice counters.
    // Changing back must not resurrect the old provider revision.
    let store = crate::extensions::store::ExtensionStore::new(&database.pool);
    store.set_instance_enabled(instance, false).await?;
    store.set_instance_enabled(instance, true).await?;
    let mut stale = state.marketplace.begin_source_provider_call(
        &descriptor,
        RuntimeJson::from_value(json!({}))?,
        state.marketplace.source_provider_turn().await?,
    )?;
    assert!(
        stale.finish().await.is_err(),
        "stale metadata must be rejected inside original binding before code/effects"
    );
    stale.cancel().await?;
    drop(stale);
    assert_eq!(sdk_rows(database).await?, before);
    assert!(client.directory().verify(snapshot).await.is_err());
    let fresh = client.directory().get(snapshot.provider_id).await?;
    assert_ne!(fresh.revision, snapshot.revision);
    observed(
        "health_after_revision_change",
        client.health(&fresh, &cancellation),
    )
    .await?;
    assert_eq!(sdk_rows(database).await?, before);
    let (health_state,receipts):(String,i64)=sqlx::query_as("SELECT p.health_state,(SELECT COUNT(*) FROM provider_readiness r WHERE r.provider_id=p.provider_id) FROM providers p WHERE p.provider_id=$1")
        .bind(snapshot.provider_id.to_string()).fetch_one(&database.pool).await?;
    assert_eq!(health_state, "unknown");
    assert_eq!(
        receipts, 0,
        "routing metadata must not manufacture DriverReady"
    );
    Ok(())
}
