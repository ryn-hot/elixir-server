use uuid::Uuid;

pub fn build_aliases(
    extension_id: &str,
    instance_name: &str,
    instance_id: Uuid,
    service_name: Option<String>,
) -> (Vec<String>, String) {
    let slug = slugify(extension_id);
    let primary_alias = format!("svc-{}-{}", slug, instance_name);
    let mut aliases = vec![instance_alias(instance_id), primary_alias.clone()];
    if let Some(service_name) = service_name {
        if !aliases.contains(&service_name) {
            aliases.push(service_name);
        }
    }
    (aliases, primary_alias)
}

pub fn container_name(instance_id: Uuid) -> String {
    format!("elx-{}", short_instance_id(instance_id))
}

/// The one network alias of an instance that no other Elixir server on the
/// same Docker host shares: it carries the same short ID as the container
/// name, which Docker keeps unique. The other aliases (`svc-<extension>-
/// <instance>`, a manifest's `service_name` such as `elx-qbittorrent`) are
/// the same on every server.
pub fn instance_alias(instance_id: Uuid) -> String {
    format!("svc-{}", short_instance_id(instance_id))
}

fn slugify(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

fn short_instance_id(instance_id: Uuid) -> String {
    let raw = instance_id.simple().to_string();
    raw.chars().take(6).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instance_alias_is_its_first_alias_and_shares_the_container_short_id() {
        let instance_id = Uuid::parse_str("db14bab5-9946-4fd4-a6f1-d85c8365cc16").unwrap();
        assert_eq!(instance_alias(instance_id), "svc-db14ba");
        assert_eq!(container_name(instance_id), "elx-db14ba");
        let (aliases, primary) = build_aliases(
            "elixir.modules.qbittorrent",
            "default",
            instance_id,
            Some("elx-qbittorrent".to_string()),
        );
        assert_eq!(
            aliases,
            [
                "svc-db14ba",
                "svc-elixir-modules-qbittorrent-default",
                "elx-qbittorrent"
            ]
        );
        assert_eq!(primary, "svc-elixir-modules-qbittorrent-default");
    }
}
