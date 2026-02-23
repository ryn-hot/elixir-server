mod downloader_torrent;
mod driver_trait;
mod indexer_registry;
mod media_manager_movies;
mod media_manager_tv;
mod patches;
mod registry;

pub use downloader_torrent::DownloaderTorrentDriver;
pub use driver_trait::{ApplyResult, ApplyStatus, CapabilityDriver, DriverCtx, StateSnapshot};
pub use indexer_registry::IndexerRegistryDriver;
pub use media_manager_movies::MediaManagerMoviesDriver;
pub use media_manager_tv::MediaManagerTvDriver;
pub use patches::{
    DownloaderSpec, DriverPatch, IndexerCredentialField, IndexerRegistryPatch,
    MediaManagerMoviesPatch, MediaManagerTvPatch,
};
pub use registry::DriverRegistry;
