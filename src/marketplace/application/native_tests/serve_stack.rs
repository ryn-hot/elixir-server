//! The Arr Stack module's own code in the long-lived executor: it reaches a
//! stand-in Sonarr only through Elixir's member path (`http://sonarr.stack`)
//! with the member's API key, and answers Elixir's media-manager interface.
use super::*;
use crate::marketplace::{
    grant_store::installation::serve::ServeHost,
    quarantine::execution::ServeOutcome,
    stack::{StackMemberEndpoint, StackMembers},
};
use axum::{
    Json, Router,
    extract::Query,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use std::{collections::HashMap, sync::Arc};

const KEY: &str = "stand-in-sonarr-key";

struct StandIn(String);
#[async_trait::async_trait]
impl StackMembers for StandIn {
    async fn member(&self, _stack: &str, member: &str) -> Result<Option<StackMemberEndpoint>> {
        Ok((member == "sonarr").then(|| StackMemberEndpoint {
            base_url: self.0.clone(),
            api_key: Some(KEY.to_owned()),
        }))
    }
}

fn keyed(headers: &HeaderMap) -> Result<(), StatusCode> {
    (headers.get("x-api-key").and_then(|v| v.to_str().ok()) == Some(KEY))
        .then_some(())
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn sonarr() -> Result<String> {
    let app = Router::new()
        .route(
            "/api/v3/series/42",
            get(|headers: HeaderMap| async move {
                keyed(&headers)?;
                Ok::<_, StatusCode>(Json(serde_json::json!({"id":42,"statistics":{"episodeFileCount":2,"episodeCount":10}})))
            }),
        )
        .route(
            "/api/v3/queue",
            get(|headers: HeaderMap, Query(q): Query<HashMap<String, String>>| async move {
                keyed(&headers)?;
                assert_eq!(q.get("pageSize").map(String::as_str), Some("250"));
                Ok::<_, StatusCode>(Json(serde_json::json!({"records":[
                    {"seriesId":42,"episodeId":100,"downloadId":"abc","downloadClient":"qBittorrent","trackedDownloadState":"downloading","trackedDownloadStatus":"ok","status":"downloading"},
                    {"seriesId":7,"episodeId":200,"downloadId":"other"}]})))
            }),
        )
        .route(
            "/api/v3/episode",
            get(|headers: HeaderMap| async move {
                keyed(&headers)?;
                Ok::<_, StatusCode>(Json(serde_json::json!([{"id":100,"seasonNumber":1,"episodeNumber":3,"title":"Stand-in"}])))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{address}/"))
}

pub(super) async fn qualify(
    client: &ParticipationClient,
    database: &crate::db::Database,
    secrets: &crate::secrets::SecretsManager,
    home: Uuid,
    extension: &elixir_marketplace::identity::ExtensionId,
    instance: Uuid,
) -> Result<()> {
    let configuration = client.installed_configuration(allocation().source, allocation().retention)?;
    let host = ServeHost::new(
        database.clone(),
        secrets.clone(),
        configuration,
        Some(Arc::new(StandIn(sonarr().await?))),
    );
    let status = || {
        RuntimeJson::from_value(serde_json::json!({
            "schema_version": 1, "capability": "media.manager", "member": "sonarr",
            "operation": "acquisition_status", "request": {"kind": "series", "itemId": "42"}
        }))
    };
    let mut elapsed = Vec::new();
    for _ in 0..5 {
        let started = std::time::Instant::now();
        let ServeOutcome::Result(value) = host
            .call(home, extension, instance, allocation().installations, status()?)
            .await?
        else {
            bail!("the Arr Stack module failed acquisition_status");
        };
        elapsed.push(started.elapsed().as_millis());
        let reply: serde_json::Value = serde_json::from_str(value.as_str())?;
        assert_eq!(reply["status"], 200, "{reply}");
        assert_eq!(reply["body"]["item"]["fileCount"], 2);
        assert_eq!(reply["body"]["queue"].as_array().map(Vec::len), Some(1), "only this series' entry");
        assert_eq!(reply["body"]["queue"][0]["downloader"], "qBittorrent");
        assert_eq!(reply["body"]["episodes"]["100"]["title"], "Stand-in");
    }
    // A member it does not have is refused by Elixir, not reached.
    let arguments = RuntimeJson::from_value(serde_json::json!({
        "schema_version": 1, "capability": "media.manager", "member": "radarr",
        "operation": "acquisition_status", "request": {"kind": "movie", "itemId": "5"}
    }))?;
    let ServeOutcome::Result(value) = host
        .call(home, extension, instance, allocation().installations, arguments)
        .await?
    else {
        bail!("an unavailable member must be a reported error, not a failed call");
    };
    let reply: serde_json::Value = serde_json::from_str(value.as_str())?;
    assert_ne!(reply["status"], 200, "{reply}");
    eprintln!("Arr Stack module in the long-lived executor: acquisition_status elapsed_ms={elapsed:?} (first includes start)");
    host.shutdown().await?;
    Ok(())
}
