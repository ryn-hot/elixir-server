//! The legacy Arr stack: Sonarr, Radarr, Prowlarr, Bazarr, Byparr and
//! FlareSolverr as bundled modules, their connectors and the Arr stack
//! blueprint, installed by Elixir before the marketplace Arr Stack. The server
//! no longer contains their code (Stage 3 of
//! docs/architecture/arr-stack-marketplace-extension.md). An installation left
//! from that time is kept exactly as it is: Elixir never starts, sets up,
//! changes or calls it again, and only recognizes it to say so. It can be
//! uninstalled as usual.

/// Shown wherever a legacy Arr installation would have been used.
pub const UNSUPPORTED: &str = "This Sonarr, Radarr or Prowlarr was installed by an older version of \
     Elixir and is no longer managed. Install the Arr Stack extension from the marketplace; this \
     installation can be uninstalled.";

const MODULES: &[&str] = &[
    "elixir.modules.sonarr",
    "elixir.modules.radarr",
    "elixir.modules.prowlarr",
    "elixir.modules.bazarr",
    "elixir.modules.byparr",
    "elixir.modules.flaresolverr",
];

/// The legacy blueprint's own connectors that are not named after an Arr app.
const DOWNLOADER_CONNECTORS: &[&str] = &[
    "elixir.connectors.qbittorrent_defaults",
    "elixir.connectors.nzbget_defaults",
];

/// Whether an installed extension belongs to the legacy Arr stack.
pub fn is_legacy_arr_extension(extension_id: &str) -> bool {
    let id = extension_id.trim();
    if MODULES.contains(&id)
        || DOWNLOADER_CONNECTORS.contains(&id)
        || id == "elixir.blueprints.arr_stack"
    {
        return true;
    }
    id.strip_prefix("elixir.connectors.").is_some_and(|name| {
        ["sonarr_", "radarr_", "prowlarr_", "bazarr_"]
            .iter()
            .any(|app| name.starts_with(app))
    })
}

#[cfg(test)]
mod tests {
    use super::is_legacy_arr_extension;

    #[test]
    fn recognizes_only_the_legacy_arr_stack() {
        for legacy in [
            "elixir.modules.sonarr",
            "elixir.modules.flaresolverr",
            "elixir.blueprints.arr_stack",
            "elixir.connectors.prowlarr_sonarr_app",
            "elixir.connectors.radarr_qbittorrent",
            "elixir.connectors.qbittorrent_defaults",
        ] {
            assert!(is_legacy_arr_extension(legacy), "{legacy}");
        }
        for current in [
            "elixir.modules.qbittorrent",
            "elixir.modules.nzbget",
            "stack.0123abcd.sonarr",
            "stack.0123abcd.prowlarr-sonarr",
            "elx:publisher/arr-stack",
            "elixir.sources.prism",
        ] {
            assert!(!is_legacy_arr_extension(current), "{current}");
        }
    }
}
