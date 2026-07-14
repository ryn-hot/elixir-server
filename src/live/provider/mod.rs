//! Revision-bound Live provider integration boundary.

mod client;
mod directory;

pub use client::{LiveProviderClient, ProviderClientBuildError, ProviderInvocationError};
pub use directory::{
    LiveProviderDirectory, LiveProviderSnapshot, ProviderDirectoryError,
    ProviderDirectoryErrorCode, ProviderRevision,
};

#[cfg(test)]
pub(crate) mod tests;
