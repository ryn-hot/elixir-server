//! Standalone live-streaming domain boundaries.

use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post, put},
};

use crate::state::AppState;

pub mod admin;
pub mod artwork;
pub mod catalog;
pub mod config;
pub mod contract;
pub mod crypto;
pub mod diagnostics;
pub mod egress;
pub mod lease;
pub mod metrics;
pub mod planner;
pub mod provider;
pub mod relay;
pub mod remux;
pub mod service;
pub mod session;
pub mod types;
pub mod upstream;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/live/providers",
            get(crate::http::handlers::live::catalog::providers),
        )
        .route(
            "/api/v1/live/catalogs",
            get(crate::http::handlers::live::catalog::catalogs),
        )
        .route(
            "/api/v1/live/catalogs/:provider_id/:catalog_id/items",
            get(crate::http::handlers::live::catalog::catalog_items),
        )
        .route(
            "/api/v1/live/items/:provider_id/:item_key",
            get(crate::http::handlers::live::catalog::item),
        )
        .route(
            "/api/v1/live/artwork/:artwork_id",
            get(crate::http::handlers::live::artwork::get),
        )
        .route(
            "/api/v1/live/admin/providers/:provider_id/destination-rules",
            get(crate::http::handlers::live::admin::list_destination_rules)
                .post(crate::http::handlers::live::admin::create_destination_rule)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/providers",
            get(crate::http::handlers::live::admin::list_providers),
        )
        .route(
            "/api/v1/live/admin/providers/:provider_id/destination-rules/:rule_id",
            put(crate::http::handlers::live::admin::update_destination_rule)
                .delete(crate::http::handlers::live::admin::delete_destination_rule)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/providers/:provider_id/grants/:profile_id",
            put(crate::http::handlers::live::admin::set_provider_grant)
                .delete(crate::http::handlers::live::admin::revoke_provider_grant)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/providers/:provider_id/disable",
            post(crate::http::handlers::live::admin::disable_provider)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/sessions",
            get(crate::http::handlers::live::admin::list_sessions),
        )
        .route(
            "/api/v1/live/admin/sessions/:session_id/terminate",
            post(crate::http::handlers::live::admin::terminate_session)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/sessions/:session_id/diagnostics",
            get(crate::http::handlers::live::diagnostics::session_bundle),
        )
        .route(
            "/api/v1/live/admin/egress",
            get(crate::http::handlers::live::admin::egress_status)
                .put(crate::http::handlers::live::admin::update_egress_policy)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/keys",
            get(crate::http::handlers::live::admin::key_state),
        )
        .route(
            "/api/v1/live/admin/keys/envelope/rotate",
            post(crate::http::handlers::live::admin::rotate_envelope_key)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/keys/token-hash/rotate",
            post(crate::http::handlers::live::admin::rotate_token_hash_key)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/admin/keys/audit/rotate",
            post(crate::http::handlers::live::admin::rotate_audit_key)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/api/v1/live/sessions",
            post(crate::http::handlers::live::sessions::create),
        )
        .route(
            "/api/v1/live/sessions/:session_id",
            get(crate::http::handlers::live::sessions::get)
                .delete(crate::http::handlers::live::sessions::end),
        )
        .route(
            "/api/v1/live/sessions/:session_id/heartbeat",
            post(crate::http::handlers::live::sessions::heartbeat),
        )
        .route(
            "/api/v1/live/sessions/:session_id/refresh",
            post(crate::http::handlers::live::sessions::refresh),
        )
        .route(
            "/api/v1/live/sessions/:session_id/failover",
            post(crate::http::handlers::live::sessions::failover),
        )
        .route(
            "/api/v1/live/sessions/:session_id/delivery/hls/manifest.m3u8",
            get(crate::http::handlers::live::delivery::hls_manifest),
        )
        .route(
            "/api/v1/live/sessions/:session_id/delivery/hls/resources/:resource_id",
            get(crate::http::handlers::live::delivery::hls_resource),
        )
        .route(
            "/api/v1/live/sessions/:session_id/delivery/stream",
            get(crate::http::handlers::live::delivery::progressive_stream),
        )
}
