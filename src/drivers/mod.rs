mod downloader_nzb;
mod downloader_torrent;
mod driver_trait;
mod indexer_registry;
mod media_manager_movies;
mod media_manager_support;
mod media_manager_tv;
mod patches;
mod quality_policy;
mod registry;

pub use downloader_nzb::DownloaderNzbDriver;
pub(crate) use downloader_nzb::render_nzbget_config_patch;
pub(crate) use downloader_nzb::render_nzbget_config_text_updates;
pub(crate) use downloader_nzb::{
    NzbgetPauseSnapshot, pause_nzbget_for_rehome, resume_nzbget_after_rehome,
};
pub use downloader_torrent::DownloaderTorrentDriver;
pub(crate) use downloader_torrent::bootstrap_qbittorrent_session_cookie;
pub(crate) use downloader_torrent::{
    QbittorrentPauseSnapshot, pause_qbittorrent_for_rehome, resume_qbittorrent_after_rehome,
};
pub use driver_trait::{
    ActivitySnapshot, AddMediaOptions, AddMediaRequest, AddMediaResult, ApplyResult, ApplyStatus,
    CapabilityDriver, DriftEvaluation, DriftField, DriftStatus, DriverCtx, FieldSemantics,
    PatchApplyPolicy, PatchSemantics, PatchSideEffect, StateSnapshot,
};
pub use indexer_registry::IndexerRegistryDriver;
pub use media_manager_movies::MediaManagerMoviesDriver;
pub use media_manager_tv::MediaManagerTvDriver;
#[cfg(test)]
pub use patches::AppSpec;
pub use patches::{
    DownloaderNzbPatch, DownloaderSpec, DownloaderTorrentPatch, DriverPatch,
    IndexerCredentialField, IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
pub(crate) use quality_policy::{
    build_radarr_quality_policy_plan, build_sonarr_quality_policy_plan,
    is_elixir_managed_radarr_quality_profile, is_elixir_managed_sonarr_quality_profile,
};
pub use registry::DriverRegistry;
