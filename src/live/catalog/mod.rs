//! Live catalog aggregation, cache, visibility, and grant boundary.

mod cache;
mod circuit;
mod coalesce;
mod grants;
mod public_keys;
mod service;

pub use cache::{
    CacheFreshness, CacheKey, CacheOperation, CacheRequest, CatalogCacheEntry,
    CatalogCacheRepository, CatalogCacheValue, VisibilityPartition,
};
pub use grants::{
    AuditedGrantMutation, GrantMutation, LiveProviderAccess, LiveProviderGrant,
    LiveProviderGrantError, LiveProviderGrantRepository, VisibilityDecision,
};
pub use public_keys::{
    LiveArtworkKind, LivePublicKeyCodec, LivePublicKeyError, LivePublicKeyScope, OpenedArtworkKey,
};
pub use service::{
    AggregatedCatalogs, CatalogServiceError, LiveCatalogAccessContext, LiveCatalogService,
    ProviderCatalog, ProviderCatalogPage, ProviderItemMetadata, ProviderScopedError,
    VisibleLiveProvider, VisibleProviderReadiness,
};

#[cfg(test)]
mod tests;
