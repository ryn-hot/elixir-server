//! Strict HLS parsing, rewriting, and session-scoped resource ownership.

mod resource;
mod rewrite;

pub use resource::{
    HlsByteRange, HlsManifestScope, HlsResourceDescriptor, HlsResourceId, HlsResourceKind,
    HlsResourceLimits, HlsResourceMap,
};
pub use rewrite::{
    HlsManifestKind, HlsRewriteConfig, HlsRewriteError, HlsRewriteResult, HlsRewriter,
};

#[cfg(test)]
mod tests;
