mod provider_types;
mod providers;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::{
    config::MetadataConfig,
    db::models::MediaType,
    extensions::{ExternalIds, MediaIdentity},
};

pub use provider_types::DiscoveryResult;
pub use provider_types::MetadataResult;

const TVDB_TOKEN_TTL_SECONDS: u64 = 60 * 60 * 12;

#[derive(Clone)]
struct TvdbToken {
    token: String,
    expires_at: Instant,
}

#[derive(Clone)]
pub struct MetadataService {
    client: Client,
    pub config: MetadataConfig,
    tvdb_token: Arc<Mutex<Option<TvdbToken>>>,
}

impl MetadataService {
    pub fn new(config: MetadataConfig) -> Result<Self> {
        let client = Client::builder()
            .user_agent("ElixirMediaServer/0.1 (+https://example.com)")
            .timeout(std::time::Duration::from_secs(
                config.request_timeout_seconds,
            ))
            .build()?;

        Ok(Self {
            client,
            config,
            tvdb_token: Arc::new(Mutex::new(None)),
        })
    }

    pub fn ttl_seconds(&self) -> u64 {
        self.config.ttl_seconds
    }

    pub async fn fetch_metadata(&self, identity: &MediaIdentity) -> Result<Option<MetadataResult>> {
        match identity.r#type {
            MediaType::Anime => {
                if identity.external_ids.anilist.is_none() {
                    return Ok(None);
                }
                if self.config.enable_anilist {
                    if let Some(meta) = providers::anilist::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
                if self.config.enable_aniapi {
                    if let Some(meta) = providers::aniapi::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
                if self.config.enable_consumet {
                    if let Some(meta) = providers::consumet::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
            }
            _ => {
                if identity.external_ids.imdb.is_none() {
                    return Ok(None);
                }
                if self.config.enable_cinemeta {
                    if let Some(meta) = providers::cinemeta::fetch(
                        &self.client,
                        identity,
                        &self.config.cinemeta_base_url,
                    )
                    .await?
                    {
                        return Ok(Some(meta));
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn discovery_search(
        &self,
        query: &str,
        r#type: Option<MediaType>,
    ) -> Result<Vec<DiscoveryResult>> {
        let requested_type = r#type.unwrap_or(MediaType::Movie);
        let media_kind = match requested_type {
            MediaType::Movie => "movie",
            MediaType::Series => "series",
            MediaType::Anime => "series",
        };

        // Anime-specific search via Anilist
        if requested_type == MediaType::Anime && self.config.enable_anilist {
            if let Ok(items) = providers::anilist::search(&self.client, query).await {
                if !items.is_empty() {
                    let mapped = items
                        .into_iter()
                        .map(|media| {
                            let title = media
                                .title
                                .as_ref()
                                .and_then(|t| {
                                    t.get("romaji")
                                        .or_else(|| t.get("english"))
                                        .or_else(|| t.get("native"))
                                })
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or(query)
                                .to_string();
                            let year = media
                                .startDate
                                .as_ref()
                                .and_then(|d| d.get("year"))
                                .and_then(serde_json::Value::as_i64)
                                .map(|v| v as i32);
                            DiscoveryResult {
                                title,
                                r#type: MediaType::Anime,
                                year,
                                external_ids: Some(ExternalIds {
                                    anilist: Some(media.id.to_string()),
                                    ..Default::default()
                                }),
                                description: media.description.clone(),
                            }
                        })
                        .collect();
                    return Ok(mapped);
                }
            }
        }

        if matches!(requested_type, MediaType::Movie | MediaType::Series) {
            if let Ok(results) = self.tvdb_discovery_search(query, requested_type).await {
                if !results.is_empty() {
                    return Ok(results);
                }
            }
        }

        // Try Cinemeta first.
        if self.config.enable_cinemeta {
            if let Ok(results) = providers::cinemeta::search(
                &self.client,
                query,
                media_kind,
                &self.config.cinemeta_base_url,
            )
            .await
            {
                if !results.is_empty() {
                    let mapped = results
                        .into_iter()
                        .map(|meta| {
                            let title = meta
                                .rest
                                .get("name")
                                .or_else(|| meta.rest.get("title"))
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_else(|| query)
                                .to_string();
                            let year = meta.year.or_else(|| {
                                meta.rest
                                    .get("year")
                                    .and_then(serde_json::Value::as_i64)
                                    .map(|v| v as i32)
                            });
                            DiscoveryResult {
                                title,
                                r#type: requested_type,
                                year,
                                external_ids: Some(ExternalIds {
                                    imdb: meta.imdb_id.clone(),
                                    ..Default::default()
                                }),
                                description: meta.description.clone(),
                            }
                        })
                        .collect();
                    return Ok(mapped);
                }
            }
        }

        Ok(Vec::new())
    }

    fn tvdb_enabled(&self) -> bool {
        self.config.enable_tvdb
            && !self.config.tvdb_base_url.trim().is_empty()
            && self
                .config
                .tvdb_api_key
                .as_deref()
                .map(str::trim)
                .map(|value| !value.is_empty())
                .unwrap_or(false)
    }

    async fn tvdb_discovery_search(
        &self,
        query: &str,
        media_type: MediaType,
    ) -> Result<Vec<DiscoveryResult>> {
        if !self.tvdb_enabled() || query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let tvdb_type = match media_type {
            MediaType::Movie => "movie",
            MediaType::Series => "series",
            MediaType::Anime => return Ok(Vec::new()),
        };
        let payload: Option<Value> = self
            .tvdb_get_json(
                "/search",
                &[
                    ("query", query.trim().to_string()),
                    ("type", tvdb_type.to_string()),
                ],
            )
            .await?;
        let items = payload
            .as_ref()
            .and_then(|value| value.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(providers::tvdb::map_search_results(
            &items, query, media_type,
        ))
    }

    async fn tvdb_get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Option<T>> {
        let Some(token) = self.tvdb_token().await? else {
            return Ok(None);
        };
        let base_url = self.config.tvdb_base_url.trim().trim_end_matches('/');
        if base_url.is_empty() {
            return Ok(None);
        }
        let url = format!("{base_url}{path}");
        let mut response = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(query)
            .send()
            .await?;

        if response.status() == StatusCode::UNAUTHORIZED {
            self.clear_tvdb_token().await;
            if let Some(token) = self.tvdb_token().await? {
                response = self
                    .client
                    .get(&url)
                    .bearer_auth(&token)
                    .query(query)
                    .send()
                    .await?;
            }
        }

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            anyhow::bail!("tvdb returned {}", response.status());
        }
        Ok(Some(response.json::<T>().await?))
    }

    async fn tvdb_token(&self) -> Result<Option<String>> {
        if !self.tvdb_enabled() {
            return Ok(None);
        }
        {
            let guard = self.tvdb_token.lock().await;
            if let Some(token) = guard.as_ref() {
                if Instant::now() < token.expires_at {
                    return Ok(Some(token.token.clone()));
                }
            }
        }

        let base_url = self.config.tvdb_base_url.trim().trim_end_matches('/');
        if base_url.is_empty() {
            return Ok(None);
        }
        let api_key = self
            .config
            .tvdb_api_key
            .as_deref()
            .map(str::trim)
            .unwrap_or_default();
        if api_key.is_empty() {
            return Ok(None);
        }

        let response = self
            .client
            .post(format!("{base_url}/login"))
            .json(&serde_json::json!({ "apikey": api_key }))
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("tvdb login failed {}", response.status());
        }
        let payload: TvdbLoginResponse = response
            .json()
            .await
            .context("tvdb login response parse failed")?;
        let token = payload
            .data
            .and_then(|data| data.token)
            .ok_or_else(|| anyhow::anyhow!("tvdb login response missing token"))?;
        let token_value = token.clone();
        let token = TvdbToken {
            token,
            expires_at: Instant::now() + Duration::from_secs(TVDB_TOKEN_TTL_SECONDS),
        };
        let mut guard = self.tvdb_token.lock().await;
        *guard = Some(token);
        Ok(Some(token_value))
    }

    async fn clear_tvdb_token(&self) {
        let mut guard = self.tvdb_token.lock().await;
        *guard = None;
    }
}

#[derive(Debug, Deserialize)]
struct TvdbLoginResponse {
    data: Option<TvdbLoginData>,
}

#[derive(Debug, Deserialize)]
struct TvdbLoginData {
    token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::Result;
    use axum::{
        Json, Router,
        extract::{Path, Query, State},
        routing::{get, post},
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[derive(Clone)]
    struct TvdbStubState {
        search_calls: Arc<AtomicUsize>,
    }

    async fn tvdb_login() -> Json<Value> {
        Json(serde_json::json!({
            "data": { "token": "stub-token" }
        }))
    }

    async fn tvdb_search(
        State(state): State<TvdbStubState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<Value> {
        state.search_calls.fetch_add(1, Ordering::Relaxed);
        let title = params
            .get("query")
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string());
        let tv_type = params
            .get("type")
            .map(|value| value.as_str())
            .unwrap_or("movie");
        Json(serde_json::json!({
            "status": "success",
            "data": [
                {
                    "id": "550",
                    "type": tv_type,
                    "name": title,
                    "overview": "Test overview",
                    "year": "1999",
                    "remote_ids": [
                        { "sourceName": "IMDB", "id": "tt0137523" }
                    ]
                }
            ]
        }))
    }

    async fn start_tvdb_stub() -> Result<(String, Arc<AtomicUsize>, oneshot::Sender<()>)> {
        let search_calls = Arc::new(AtomicUsize::new(0));
        let state = TvdbStubState {
            search_calls: search_calls.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/v4/login", post(tvdb_login))
            .route("/v4/search", get(tvdb_search))
            .with_state(state);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok((format!("http://{address}/v4"), search_calls, shutdown_tx))
    }

    #[derive(Clone)]
    struct DiscoveryStubState {
        tvdb_search_calls: Arc<AtomicUsize>,
        cinemeta_search_calls: Arc<AtomicUsize>,
    }

    async fn tvdb_search_empty(
        State(state): State<DiscoveryStubState>,
        _query: Query<HashMap<String, String>>,
    ) -> Json<Value> {
        state.tvdb_search_calls.fetch_add(1, Ordering::Relaxed);
        Json(serde_json::json!({
            "status": "success",
            "data": []
        }))
    }

    async fn cinemeta_search(
        State(state): State<DiscoveryStubState>,
        Path((kind, entry)): Path<(String, String)>,
    ) -> Json<Value> {
        state.cinemeta_search_calls.fetch_add(1, Ordering::Relaxed);
        let query = entry
            .strip_prefix("search=")
            .unwrap_or(entry.as_str())
            .strip_suffix(".json")
            .unwrap_or(entry.as_str())
            .to_string();
        let (imdb_id, year) = if kind.eq_ignore_ascii_case("movie") {
            ("tt0137523", 1999)
        } else {
            ("tt0903747", 2008)
        };
        Json(serde_json::json!({
            "metas": [
                {
                    "imdb_id": imdb_id,
                    "name": query,
                    "year": year,
                    "description": format!("{} fallback result", kind)
                }
            ]
        }))
    }

    async fn start_discovery_stub() -> Result<(
        String,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        oneshot::Sender<()>,
    )> {
        let tvdb_search_calls = Arc::new(AtomicUsize::new(0));
        let cinemeta_search_calls = Arc::new(AtomicUsize::new(0));
        let state = DiscoveryStubState {
            tvdb_search_calls: tvdb_search_calls.clone(),
            cinemeta_search_calls: cinemeta_search_calls.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/v4/login", post(tvdb_login))
            .route("/v4/search", get(tvdb_search_empty))
            .route("/meta/:kind/:entry", get(cinemeta_search))
            .with_state(state);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok((
            format!("http://{address}"),
            tvdb_search_calls,
            cinemeta_search_calls,
            shutdown_tx,
        ))
    }

    #[tokio::test]
    async fn discovery_search_uses_tvdb_for_movies_when_configured() -> Result<()> {
        let (base_url, search_calls, shutdown_tx) = start_tvdb_stub().await?;
        let mut config = MetadataConfig::default();
        config.enable_tvdb = true;
        config.tvdb_base_url = base_url;
        config.tvdb_api_key = Some("stub-key".to_string());
        config.enable_cinemeta = false;
        config.enable_anilist = false;
        config.enable_aniapi = false;
        config.enable_consumet = false;

        let service = MetadataService::new(config)?;
        let results = service
            .discovery_search("Fight Club", Some(MediaType::Movie))
            .await?;

        assert_eq!(search_calls.load(Ordering::Relaxed), 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Fight Club");
        let ids = results[0].external_ids.as_ref().expect("external ids");
        assert_eq!(ids.tvdb_movie.as_deref(), Some("550"));
        assert_eq!(ids.imdb.as_deref(), Some("tt0137523"));

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn discovery_search_falls_back_to_cinemeta_when_tvdb_empty_for_movie_and_series()
    -> Result<()> {
        let (base_url, tvdb_calls, cinemeta_calls, shutdown_tx) = start_discovery_stub().await?;
        let mut config = MetadataConfig::default();
        config.enable_tvdb = true;
        config.tvdb_base_url = format!("{base_url}/v4");
        config.tvdb_api_key = Some("stub-key".to_string());
        config.enable_cinemeta = true;
        config.cinemeta_base_url = base_url;
        config.enable_anilist = false;
        config.enable_aniapi = false;
        config.enable_consumet = false;

        let service = MetadataService::new(config)?;

        let movie_results = service
            .discovery_search("Fight Club", Some(MediaType::Movie))
            .await?;
        assert_eq!(movie_results.len(), 1);
        assert_eq!(movie_results[0].title, "Fight Club");
        assert_eq!(movie_results[0].r#type, MediaType::Movie);
        let movie_ids = movie_results[0].external_ids.as_ref().expect("movie ids");
        assert_eq!(movie_ids.imdb.as_deref(), Some("tt0137523"));

        let series_results = service
            .discovery_search("Breaking Bad", Some(MediaType::Series))
            .await?;
        assert_eq!(series_results.len(), 1);
        assert_eq!(series_results[0].title, "Breaking Bad");
        assert_eq!(series_results[0].r#type, MediaType::Series);
        let series_ids = series_results[0].external_ids.as_ref().expect("series ids");
        assert_eq!(series_ids.imdb.as_deref(), Some("tt0903747"));

        assert_eq!(tvdb_calls.load(Ordering::Relaxed), 2);
        assert_eq!(cinemeta_calls.load(Ordering::Relaxed), 2);

        let _ = shutdown_tx.send(());
        Ok(())
    }
}
