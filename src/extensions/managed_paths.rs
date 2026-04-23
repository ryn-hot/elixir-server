pub const DOWNLOADS_ROOT: &str = "/downloads";
pub const DOWNLOADS_TV_DIR: &str = "/downloads/tv";
pub const DOWNLOADS_ANIME_DIR: &str = "/downloads/anime";
pub const DOWNLOADS_MOVIES_DIR: &str = "/downloads/movies";

pub const QBITTORRENT_INCOMPLETE_DIR: &str = "/runtime/incomplete";

pub const NZBGET_MAIN_DIR: &str = "/config";
pub const NZBGET_INCOMPLETE_DIR: &str = "/runtime/incomplete";
pub const NZBGET_NZB_DIR: &str = "/runtime/nzb";
pub const NZBGET_QUEUE_DIR: &str = "/runtime/queue";
pub const NZBGET_TEMP_DIR: &str = "/runtime/tmp";
pub const NZBGET_SCRIPT_DIR: &str = "/config/scripts";
pub const NZBGET_LOG_FILE: &str = "/config/nzbget.log";
pub const NZBGET_WEB_DIR: &str = "/app/nzbget/webui";
pub const NZBGET_CONFIG_TEMPLATE: &str = "/app/nzbget/webui/nzbget.conf.template";
pub const NZBGET_LOCK_FILE: &str = "/config/nzbget.lock";

pub const NZBGET_REQUIRED_MANAGED_PATHS: [(&str, &str); 11] = [
    ("MainDir", NZBGET_MAIN_DIR),
    ("DestDir", DOWNLOADS_ROOT),
    ("InterDir", NZBGET_INCOMPLETE_DIR),
    ("NzbDir", NZBGET_NZB_DIR),
    ("QueueDir", NZBGET_QUEUE_DIR),
    ("TempDir", NZBGET_TEMP_DIR),
    ("ScriptDir", NZBGET_SCRIPT_DIR),
    ("LogFile", NZBGET_LOG_FILE),
    ("WebDir", NZBGET_WEB_DIR),
    ("ConfigTemplate", NZBGET_CONFIG_TEMPLATE),
    ("LockFile", NZBGET_LOCK_FILE),
];
