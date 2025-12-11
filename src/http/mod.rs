pub mod auth;
pub mod error;
pub mod handlers;

use axum::{
    Router,
    routing::{get, post},
};

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health::healthcheck))
        .route("/api/v1/health", get(handlers::health::healthcheck))
        .route("/metrics", get(handlers::health::metrics))
        .route("/api/v1/settings", get(handlers::settings::settings))
        .route("/api/v1/auth/login", post(handlers::auth::login))
        .route("/api/v1/library/items", get(handlers::library::list_items))
        .route("/api/v1/library/items/:id", get(handlers::library::detail))
        .route("/api/v1/library/scan", post(handlers::library::scan))
        .route("/api/v1/play", post(handlers::playback::play))
        .route("/stream/direct/:id", get(handlers::playback::stream_direct))
        .route(
            "/sessions/:id/master.m3u8",
            get(handlers::playback::master_playlist),
        )
        .route(
            "/sessions/:id/:segment",
            get(handlers::playback::serve_segment),
        )
        .route(
            "/api/v1/sessions/:id/seek",
            post(handlers::playback::seek_transcode),
        )
        .route(
            "/api/v1/sessions/:id",
            get(handlers::playback::session_detail),
        )
        .route(
            "/api/v1/sessions/:id/end",
            post(handlers::playback::end_session),
        )
        .route(
            "/api/v1/servers/register",
            post(handlers::control::register),
        )
        .route("/api/v1/me/servers", get(handlers::control::list))
        .route(
            "/api/v1/servers/register/health",
            get(handlers::control::health),
        )
        .route(
            "/api/v1/servers/register/schema",
            get(handlers::control::schema),
        )
        .route("/api/v1/discovery/search", get(handlers::discovery::search))
        .route("/api/v1/profile/playback", get(handlers::profile::profile))
        .with_state(state)
}

#[cfg(test)]
mod tests;
