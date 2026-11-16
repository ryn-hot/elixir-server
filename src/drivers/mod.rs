mod downloader_nzb;
mod downloader_torrent;
mod driver_trait;
mod patches;
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
#[cfg(test)]
pub use patches::AppSpec;
pub use patches::{
    DownloaderNzbPatch, DownloaderSpec, DownloaderTorrentPatch, DriverPatch,
    IndexerCredentialField, IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
pub use registry::DriverRegistry;
