pub fn filter_auto_managed_runtime_missing(
    extension_id: &str,
    missing: Vec<String>,
) -> Vec<String> {
    if is_qbittorrent_extension_id(extension_id) {
        return filter_secret_suffixes(missing, &["qbittorrent_username", "qbittorrent_password"]);
    }
    if is_nzbget_extension_id(extension_id) {
        return filter_secret_suffixes(missing, &["nzbget_username", "nzbget_password"]);
    }
    missing
}

pub fn is_qbittorrent_extension_id(extension_id: &str) -> bool {
    extension_id.to_ascii_lowercase().contains("qbittorrent")
}

pub fn is_nzbget_extension_id(extension_id: &str) -> bool {
    extension_id.to_ascii_lowercase().contains("nzbget")
}

fn filter_secret_suffixes(missing: Vec<String>, suffixes: &[&str]) -> Vec<String> {
    missing
        .into_iter()
        .filter(|value| {
            !suffixes
                .iter()
                .any(|suffix| value.ends_with(&format!(":{suffix}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::filter_auto_managed_runtime_missing;

    #[test]
    fn qbittorrent_runtime_credentials_are_filtered_from_missing_secrets() {
        let filtered = filter_auto_managed_runtime_missing(
            "elixir.modules.qbittorrent",
            vec![
                "instance:abc:qbittorrent_username".to_string(),
                "instance:abc:qbittorrent_password".to_string(),
                "global:wireguard_config".to_string(),
            ],
        );

        assert_eq!(filtered, vec!["global:wireguard_config".to_string()]);
    }

    #[test]
    fn nzbget_runtime_credentials_are_filtered_from_missing_secrets() {
        let filtered = filter_auto_managed_runtime_missing(
            "elixir.modules.nzbget",
            vec![
                "instance:def:nzbget_username".to_string(),
                "instance:def:nzbget_password".to_string(),
                "global:wireguard_config".to_string(),
            ],
        );

        assert_eq!(filtered, vec!["global:wireguard_config".to_string()]);
    }
}
