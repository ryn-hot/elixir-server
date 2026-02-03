pub mod auth;
pub mod error;
pub mod handlers;

use axum::{
    http::{header, Method},
    Router,
    routing::{delete, get, patch, post},
};
use tower_http::cors::{Any, CorsLayer};

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health::healthcheck))
        .route("/api/v1/health", get(handlers::health::healthcheck))
        .route("/metrics", get(handlers::health::metrics))
        .route("/api/v1/settings", get(handlers::settings::settings))
        .route(
            "/api/v1/extensions/catalog",
            get(handlers::extensions::catalog),
        )
        .route(
            "/api/v1/extensions/registries/refresh",
            post(handlers::extensions::refresh_catalog),
        )
        .route(
            "/api/v1/extensions/:id",
            get(handlers::extensions::get_extension),
        )
        .route(
            "/api/v1/extensions/install",
            post(handlers::extensions::install_extension),
        )
        .route(
            "/api/v1/extensions/:id/enable",
            post(handlers::extensions::enable_extension),
        )
        .route(
            "/api/v1/extensions/:id/disable",
            post(handlers::extensions::disable_extension),
        )
        .route(
            "/api/v1/extensions/:id/uninstall",
            post(handlers::extensions::uninstall_extension),
        )
        .route(
            "/api/v1/extensions/:id/instances",
            post(handlers::extensions::create_instance),
        )
        .route(
            "/api/v1/extensions/instances",
            get(handlers::extensions::list_instances),
        )
        .route(
            "/api/v1/extensions/instances/:id",
            patch(handlers::extensions::update_instance),
        )
        .route(
            "/api/v1/extensions/instances/:id",
            delete(handlers::extensions::delete_instance),
        )
        .route(
            "/api/v1/extensions/instances/:id/rollback",
            post(handlers::extensions::rollback_instance),
        )
        .route(
            "/api/v1/extensions/blueprints/apply",
            post(handlers::extensions::apply_blueprint),
        )
        .route(
            "/api/v1/extensions/plan/:id/confirm",
            post(handlers::extensions::confirm_plan),
        )
        .route(
            "/api/v1/extensions/plan/:id/cancel",
            post(handlers::extensions::cancel_plan),
        )
        .route(
            "/api/v1/extensions/reconcile/now",
            post(handlers::extensions::reconcile_now),
        )
        .route(
            "/api/v1/extensions/reconcile/latest",
            get(handlers::extensions::reconcile_latest),
        )
        .route(
            "/api/v1/extensions/runs",
            get(handlers::extensions::list_runs),
        )
        .route(
            "/api/v1/extensions/desired-blueprints",
            get(handlers::extensions::list_desired_blueprints),
        )
        .route(
            "/api/v1/extensions/desired-blueprints",
            delete(handlers::extensions::clear_desired_blueprints),
        )
        .route(
            "/api/v1/extensions/runs/:id",
            get(handlers::extensions::run_detail),
        )
        .route(
            "/api/v1/extensions/graph",
            get(handlers::extensions::graph),
        )
        .route(
            "/api/v1/extensions/providers/:id/health",
            get(handlers::extensions::provider_health),
        )
        .route(
            "/api/v1/extensions/runtime/:id/logs",
            get(handlers::extensions::runtime_logs),
        )
        .route(
            "/api/v1/extensions/secrets",
            get(handlers::extensions::list_secrets),
        )
        .route(
            "/api/v1/extensions/secrets",
            post(handlers::extensions::create_secret),
        )
        .route(
            "/api/v1/extensions/secrets/:id",
            get(handlers::extensions::get_secret),
        )
        .route(
            "/api/v1/extensions/secrets/:id",
            patch(handlers::extensions::update_secret),
        )
        .route(
            "/api/v1/extensions/secrets/:id",
            delete(handlers::extensions::delete_secret),
        )
        .route(
            "/api/v1/extensions/secrets/:id/rotate",
            post(handlers::extensions::rotate_secret),
        )
        .route("/api/v1/auth/login", post(handlers::auth::login))
        .route("/api/v1/auth/signup", post(handlers::auth::signup))
        .route(
            "/api/v1/auth/reset/start",
            post(handlers::auth::start_password_reset),
        )
        .route(
            "/api/v1/auth/reset/complete",
            post(handlers::auth::complete_password_reset),
        )
        .route("/api/v1/library/items", get(handlers::library::list_items))
        .route("/api/v1/library/items/:id", get(handlers::library::detail))
        .route(
            "/api/v1/library/series/:id/seasons",
            get(handlers::library::list_seasons),
        )
        .route(
            "/api/v1/library/seasons/:id",
            get(handlers::library::season_detail),
        )
        .route(
            "/api/v1/library/seasons/:id/episodes",
            get(handlers::library::list_episodes),
        )
        .route("/api/v1/library/scan", post(handlers::library::scan))
        .route("/api/v1/artwork/:id", get(handlers::artwork::get_artwork))
        .route(
            "/api/v1/library/review/queue",
            get(handlers::review::list_queue),
        )
        .route(
            "/api/v1/library/review/queue/:id",
            get(handlers::review::queue_detail),
        )
        .route(
            "/api/v1/library/review/queue/:id/apply",
            post(handlers::review::apply_review),
        )
        .route(
            "/api/v1/library/overrides",
            post(handlers::review::set_override),
        )
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
            "/api/v1/sessions/:id/resume",
            get(handlers::playback::resume_session),
        )
        .route(
            "/api/v1/sessions/:id/poll",
            get(handlers::playback::poll_session),
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
        .route(
            "/api/v1/discovery/suggest",
            get(handlers::discovery::suggest),
        )
        .route("/api/v1/profile/playback", get(handlers::profile::profile))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::OPTIONS])
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
        )
}

#[cfg(test)]
mod tests;
