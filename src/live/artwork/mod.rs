//! Validated, profile-isolated Live artwork proxy/cache.

mod service;

pub use service::{
    ArtworkFetchRequest, LiveArtwork, LiveArtworkError, LiveArtworkErrorCode, LiveArtworkLimits,
    LiveArtworkService,
};

#[cfg(test)]
mod tests;
