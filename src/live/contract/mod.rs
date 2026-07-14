//! Frozen `live.catalog_provider/v1` boundary.

mod model;
mod validation;
mod wire;

pub use model::*;
pub(crate) use validation::parse_provider_failure;
pub use validation::{
    parse_catalog_page_response, parse_catalogs_response, parse_health_response,
    parse_meta_response, parse_refresh_response, parse_resolve_response, validate_provider_config,
};

pub const LIVE_PROVIDER_CONTRACT_VERSION: u32 = 1;
pub const LIVE_PROVIDER_PROTOCOL: &str = "live_provider_v1";
pub const MAX_PROVIDER_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

#[cfg(test)]
mod tests;
