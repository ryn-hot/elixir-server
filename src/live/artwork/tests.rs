use std::{
    collections::HashMap,
    io::Cursor,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result as AnyResult;
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderValue, Response, StatusCode, header::CONTENT_TYPE},
    routing::get,
};
use image::{DynamicImage, ImageFormat, Rgb, RgbImage};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    db::Database,
    live::{
        catalog::{LiveArtworkKind, LivePublicKeyScope},
        contract::SensitiveString,
        provider::tests::{NativeFixture, seed_provider, test_database},
        upstream::{DnsResolver, UpstreamErrorCode},
    },
};

use super::*;

struct LoopbackResolver;

#[async_trait]
impl DnsResolver for LoopbackResolver {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        cancellation: &CancellationToken,
    ) -> std::result::Result<Vec<IpAddr>, crate::live::upstream::UpstreamError> {
        if cancellation.is_cancelled() {
            return Err(UpstreamErrorCode::Cancelled.into());
        }
        Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
    }
}

#[derive(Clone)]
struct ImageFixtureState {
    png: Arc<Vec<u8>>,
    large_png: Arc<Vec<u8>>,
    counts: Arc<Mutex<HashMap<String, usize>>>,
}

struct ImageFixture {
    state: ImageFixtureState,
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl ImageFixture {
    async fn start() -> AnyResult<Self> {
        let state = ImageFixtureState {
            png: Arc::new(png(8, 6)?),
            large_png: Arc::new(png(64, 64)?),
            counts: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = Router::new()
            .fallback(get(image_origin))
            .with_state(state.clone());
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let port = listener.local_addr()?.port();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { state, port, task })
    }

    fn count(&self, path: &str) -> usize {
        self.state
            .counts
            .lock()
            .ok()
            .and_then(|counts| counts.get(path).copied())
            .unwrap_or(0)
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn image_origin(
    State(state): State<ImageFixtureState>,
    OriginalUri(uri): OriginalUri,
) -> Response<Body> {
    let path = uri.path().to_string();
    if let Ok(mut counts) = state.counts.lock() {
        *counts.entry(path.clone()).or_default() += 1;
    }
    match path.as_str() {
        "/redirect" => Response::builder()
            .status(StatusCode::FOUND)
            .header("location", "/image.png")
            .body(Body::empty())
            .unwrap(),
        "/image.png" => image_response(StatusCode::OK, "image/png", state.png.as_slice()),
        "/bad-mime" => image_response(StatusCode::OK, "text/html", state.png.as_slice()),
        "/mismatch.jpg" => image_response(StatusCode::OK, "image/jpeg", state.png.as_slice()),
        "/malformed.png" => image_response(StatusCode::OK, "image/png", b"not-an-image"),
        "/large.png" => image_response(StatusCode::OK, "image/png", state.large_png.as_slice()),
        "/oversized.png" => image_response(StatusCode::OK, "image/png", &vec![0u8; 2048]),
        "/slow.png" => {
            tokio::time::sleep(Duration::from_millis(150)).await;
            image_response(StatusCode::OK, "image/png", state.png.as_slice())
        }
        _ => image_response(StatusCode::NOT_FOUND, "text/plain", b"missing"),
    }
}

fn image_response(status: StatusCode, content_type: &str, bytes: &[u8]) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes.to_vec()));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
    response
}

fn png(width: u32, height: u32) -> AnyResult<Vec<u8>> {
    let image = RgbImage::from_pixel(width, height, Rgb([23, 101, 207]));
    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageRgb8(image).write_to(&mut output, ImageFormat::Png)?;
    Ok(output.into_inner())
}

async fn seed_owner(database: &Database) -> AnyResult<(Uuid, Uuid, Uuid)> {
    let user_id = Uuid::new_v4();
    let home_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(format!("{user_id}@example.invalid"))
        .bind("test-hash")
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, 'Artwork Home')")
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO home_members (id, home_id, user_id, role, status)
         VALUES ($1, $2, $3, 'owner', 'active')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO profiles (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, $3, 'account', 'Owner', TRUE)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .execute(&database.pool)
    .await?;
    Ok((user_id, home_id, profile_id))
}

fn fetch_request(
    provider_id: Uuid,
    home_id: Uuid,
    profile_id: Uuid,
    revision: i64,
    port: u16,
    path: &str,
    item: &str,
) -> ArtworkFetchRequest {
    ArtworkFetchRequest {
        provider_id,
        item_id: item.to_string(),
        kind: LiveArtworkKind::Poster,
        source: SensitiveString::new(format!("http://artwork.live.test:{port}{path}")),
        scope: LivePublicKeyScope {
            home_id,
            profile_id,
            authorization_revision: revision,
        },
    }
}

async fn setup(
    limits: LiveArtworkLimits,
) -> AnyResult<(
    Database,
    NativeFixture,
    ImageFixture,
    LiveArtworkService,
    Uuid,
    Uuid,
    Uuid,
)> {
    let database = test_database().await?;
    let provider_fixture = NativeFixture::start().await?;
    let (_, provider_id) =
        seed_provider(&database, provider_fixture.port(), serde_json::json!({})).await?;
    let (_, home_id, profile_id) = seed_owner(&database).await?;
    let images = ImageFixture::start().await?;
    let service = LiveArtworkService::new_for_test(
        database.pool.clone(),
        Arc::new(LoopbackResolver),
        limits,
    )?;
    Ok((
        database,
        provider_fixture,
        images,
        service,
        provider_id,
        home_id,
        profile_id,
    ))
}

#[tokio::test]
async fn s14_artwork_fetch_redirect_decode_and_cache_without_manual_rules() -> AnyResult<()> {
    let (_database, provider_fixture, images, service, provider, home, profile) =
        setup(LiveArtworkLimits::default()).await?;
    let cancellation = CancellationToken::new();
    let first = service
        .fetch(
            fetch_request(
                provider,
                home,
                profile,
                1,
                images.port,
                "/redirect",
                "event-1",
            ),
            &cancellation,
        )
        .await?;
    assert_eq!((first.width, first.height), (8, 6));
    assert_eq!(first.content_type, "image/png");
    assert!(!first.cache_hit);
    assert_eq!(images.count("/redirect"), 1);
    assert_eq!(images.count("/image.png"), 1);

    let cached = service
        .fetch(
            fetch_request(
                provider,
                home,
                profile,
                1,
                images.port,
                "/redirect",
                "event-1",
            ),
            &cancellation,
        )
        .await?;
    assert!(cached.cache_hit);
    assert_eq!(cached.etag, first.etag);
    assert_eq!(images.count("/redirect"), 1);

    let other_profile = service
        .fetch(
            fetch_request(
                provider,
                home,
                Uuid::new_v4(),
                1,
                images.port,
                "/redirect",
                "event-1",
            ),
            &cancellation,
        )
        .await?;
    assert!(!other_profile.cache_hit);
    assert_eq!(images.count("/redirect"), 2);

    assert_eq!(service.evict_provider(home, provider).await?, 2);
    images.stop().await;
    provider_fixture.stop().await
}

#[tokio::test]
async fn s14_artwork_rejects_mime_mismatch_malformed_dimensions_and_encoded_oversize()
-> AnyResult<()> {
    let mut limits = LiveArtworkLimits::default();
    limits.max_width = 32;
    limits.max_height = 32;
    limits.max_pixels = 1024;
    limits.max_encoded_bytes = 1024;
    let (_database, provider_fixture, images, service, provider, home, profile) =
        setup(limits).await?;
    let cancellation = CancellationToken::new();
    for (path, code) in [
        ("/bad-mime", LiveArtworkErrorCode::MediaTypeRejected),
        ("/mismatch.jpg", LiveArtworkErrorCode::MediaTypeRejected),
        ("/malformed.png", LiveArtworkErrorCode::ImageRejected),
        ("/large.png", LiveArtworkErrorCode::ImageRejected),
        ("/oversized.png", LiveArtworkErrorCode::ImageTooLarge),
    ] {
        let error = service
            .fetch(
                fetch_request(provider, home, profile, 1, images.port, path, path),
                &cancellation,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), code, "{path}");
        assert!(!format!("{error:?} {error}").contains("artwork.live.test"));
    }
    images.stop().await;
    provider_fixture.stop().await
}

#[tokio::test]
async fn s14_artwork_singleflight_cancels_last_waiter_and_recovers() -> AnyResult<()> {
    let (_database, provider_fixture, images, service, provider, home, profile) =
        setup(LiveArtworkLimits::default()).await?;
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let image_port = images.port;
    let first = {
        let service = service.clone();
        let cancellation = first_cancel.clone();
        tokio::spawn(async move {
            service
                .fetch(
                    fetch_request(
                        provider,
                        home,
                        profile,
                        1,
                        image_port,
                        "/slow.png",
                        "cancelled",
                    ),
                    &cancellation,
                )
                .await
        })
    };
    let second = {
        let service = service.clone();
        let cancellation = second_cancel.clone();
        tokio::spawn(async move {
            service
                .fetch(
                    fetch_request(
                        provider,
                        home,
                        profile,
                        1,
                        image_port,
                        "/slow.png",
                        "cancelled",
                    ),
                    &cancellation,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    first_cancel.cancel();
    assert_eq!(
        first.await?.unwrap_err().code(),
        LiveArtworkErrorCode::Cancelled
    );
    assert_eq!(second.await??.width, 8);
    assert_eq!(images.count("/slow.png"), 1);

    let third_cancel = CancellationToken::new();
    let fourth_cancel = CancellationToken::new();
    let third = {
        let service = service.clone();
        let cancellation = third_cancel.clone();
        tokio::spawn(async move {
            service
                .fetch(
                    fetch_request(
                        provider,
                        home,
                        profile,
                        1,
                        image_port,
                        "/slow.png",
                        "all-cancel",
                    ),
                    &cancellation,
                )
                .await
        })
    };
    let fourth = {
        let service = service.clone();
        let cancellation = fourth_cancel.clone();
        tokio::spawn(async move {
            service
                .fetch(
                    fetch_request(
                        provider,
                        home,
                        profile,
                        1,
                        image_port,
                        "/slow.png",
                        "all-cancel",
                    ),
                    &cancellation,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    third_cancel.cancel();
    fourth_cancel.cancel();
    assert_eq!(
        third.await?.unwrap_err().code(),
        LiveArtworkErrorCode::Cancelled
    );
    assert_eq!(
        fourth.await?.unwrap_err().code(),
        LiveArtworkErrorCode::Cancelled
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    let recovered = service
        .fetch(
            fetch_request(
                provider,
                home,
                profile,
                1,
                image_port,
                "/slow.png",
                "all-cancel",
            ),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(recovered.width, 8);
    images.stop().await;
    provider_fixture.stop().await
}

#[tokio::test]
async fn s14_artwork_expiry_is_batched_and_bounded() -> AnyResult<()> {
    let mut limits = LiveArtworkLimits::default();
    limits.cache_ttl = Duration::from_millis(30);
    limits.cache_max_entries = 2;
    limits.cache_max_bytes = 1024 * 1024;
    let (_database, provider_fixture, images, service, provider, home, profile) =
        setup(limits).await?;
    for item in ["one", "two"] {
        service
            .fetch(
                fetch_request(provider, home, profile, 1, images.port, "/image.png", item),
                &CancellationToken::new(),
            )
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(service.expire_batch(1).await?, 1);
    assert_eq!(service.expire_batch(10).await?, 1);
    assert_eq!(service.expire_batch(10).await?, 0);
    assert_eq!(
        service.expire_batch(0).await.unwrap_err().code(),
        LiveArtworkErrorCode::InvalidRequest
    );
    images.stop().await;
    provider_fixture.stop().await
}
