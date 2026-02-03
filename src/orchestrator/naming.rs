use uuid::Uuid;

pub fn build_aliases(
    extension_id: &str,
    instance_name: &str,
    instance_id: Uuid,
    service_name: Option<String>,
) -> (Vec<String>, String) {
    let short_id = short_instance_id(instance_id);
    let slug = slugify(extension_id);
    let primary_alias = format!("svc-{}-{}", slug, instance_name);
    let mut aliases = vec![format!("svc-{}", short_id), primary_alias.clone()];
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
