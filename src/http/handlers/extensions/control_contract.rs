use std::collections::HashMap;

use crate::db::models::SecretScope;
use crate::extensions::manifest::{ManifestControlOwnedSetting, ManifestControlSurface};
use crate::extensions::store::NewSecret;

use super::*;

#[async_trait::async_trait]
pub(super) trait ExtensionControlProvider: Send + Sync {
    async fn load_live_snapshot(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<ExtensionControlLiveSnapshot> {
        let _ = (state, store, context);
        Ok(ExtensionControlLiveSnapshot::default())
    }

    async fn build_sections(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let _ = (state, store, context);
        Ok(Vec::new())
    }

    fn build_actions(&self, _context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        Vec::new()
    }

    async fn update_settings(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let _ = (state, store, context, values);
        anyhow::bail!("this extension does not expose editable settings yet")
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        let _ = (state, store, context, params);
        anyhow::bail!("unsupported control action '{action_id}'")
    }
}

pub(super) struct UnsupportedControlProvider;
pub(super) struct GenericManifestControlProvider;

#[async_trait::async_trait]
impl ExtensionControlProvider for UnsupportedControlProvider {}

#[async_trait::async_trait]
impl ExtensionControlProvider for GenericManifestControlProvider {
    async fn build_sections(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let Some(control_surface) = load_manifest_control_surface(context)? else {
            return Ok(Vec::new());
        };

        let mut sections =
            build_owned_setting_sections(state, store, context, &control_surface).await?;
        sections.extend(build_native_only_sections(&control_surface));
        if let Some(section) = build_runtime_bridge_gap_section(&control_surface) {
            sections.push(section);
        }
        Ok(sections)
    }

    async fn update_settings(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let Some(control_surface) = load_manifest_control_surface(context)? else {
            anyhow::bail!("this extension does not expose a generic control contract");
        };
        if values.is_empty() {
            return Ok(());
        }

        let owned_settings = control_surface
            .owned_settings
            .iter()
            .map(|setting| (setting.id.as_str(), setting))
            .collect::<HashMap<_, _>>();

        for field_id in values.keys() {
            if !owned_settings.contains_key(field_id.as_str()) {
                anyhow::bail!("unsupported control setting '{field_id}'");
            }
        }

        for (field_id, raw_value) in values {
            let setting = owned_settings
                .get(field_id.as_str())
                .copied()
                .ok_or_else(|| anyhow::anyhow!("unsupported control setting '{field_id}'"))?;
            write_owned_setting_value(state, store, context, setting, raw_value).await?;
        }

        Ok(())
    }
}

pub(super) fn control_policy_observed(description: &str) -> ExtensionControlPolicy {
    build_control_policy("observed", "Observed", description)
}

pub(super) fn control_policy_seeded(description: &str) -> ExtensionControlPolicy {
    build_control_policy("seeded", "Seeded", description)
}

pub(super) fn control_policy_managed(description: &str) -> ExtensionControlPolicy {
    build_control_policy("managed", "Managed", description)
}

pub(super) fn control_notice(
    severity: &str,
    code: &str,
    title: &str,
    message: impl Into<String>,
) -> ExtensionControlNotice {
    ExtensionControlNotice {
        severity: severity.to_string(),
        code: code.to_string(),
        title: title.to_string(),
        message: message.into(),
        action: None,
    }
}

pub(super) fn repair_managed_invariants_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "repair_managed_invariants".to_string(),
        label: "Repair in Elixir".to_string(),
        description: "Re-apply the Elixir-managed invariants this extension depends on."
            .to_string(),
        kind: "primary".to_string(),
        params: None,
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

pub(super) fn section_has_managed_drift(section: &ExtensionControlSection) -> bool {
    section
        .notices
        .iter()
        .any(|notice| notice.code.starts_with("managed_"))
}

fn build_control_policy(mode: &str, label: &str, description: &str) -> ExtensionControlPolicy {
    ExtensionControlPolicy {
        mode: mode.to_string(),
        label: label.to_string(),
        description: description.to_string(),
    }
}

fn load_manifest_control_surface(
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ManifestControlSurface>> {
    Ok(context
        .manifest
        .control_surface
        .clone()
        .filter(|surface| surface.adapter.trim().eq_ignore_ascii_case("generic_v1")))
}

async fn build_owned_setting_sections(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    control_surface: &ManifestControlSurface,
) -> anyhow::Result<Vec<ExtensionControlSection>> {
    let mut seeded_fields = Vec::new();
    let mut managed_fields = Vec::new();
    let mut seeded_requires_instance = false;
    let mut managed_requires_instance = false;

    for setting in &control_surface.owned_settings {
        let field = read_owned_setting_field(state, store, context, setting).await?;
        let requires_instance =
            field.readonly && uses_instance_scope(setting) && context.selected_instance.is_none();
        if setting.ownership_mode().eq_ignore_ascii_case("seeded") {
            seeded_requires_instance |= requires_instance;
            seeded_fields.push(field);
        } else {
            managed_requires_instance |= requires_instance;
            managed_fields.push(field);
        }
    }

    let mut sections = Vec::new();
    if !seeded_fields.is_empty() {
        let mut notices = Vec::new();
        if seeded_requires_instance {
            notices.push(control_notice(
                "warning",
                "instance_required",
                "Enable a default instance first",
                "This extension stores some seeded defaults per instance. Create or enable a default instance before editing them here.",
            ));
        }
        sections.push(ExtensionControlSection {
            id: "ownedSettingsSeeded".to_string(),
            title: "Seeded settings".to_string(),
            description:
                "Elixir can seed these defaults for the extension, but downstream overrides are allowed and can become the new live value."
                    .to_string(),
            policy: Some(control_policy_seeded(
                "These are extension-defined seeded defaults. Elixir writes them intentionally, but does not treat downstream changes as drift by default.",
            )),
            notices,
            fields: seeded_fields,
            entities: Vec::new(),
            actions: Vec::new(),
        });
    }
    if !managed_fields.is_empty() {
        let mut notices = Vec::new();
        if managed_requires_instance {
            notices.push(control_notice(
                "warning",
                "instance_required",
                "Enable a default instance first",
                "This extension stores some managed settings per instance. Create or enable a default instance before editing them here.",
            ));
        }
        sections.push(ExtensionControlSection {
            id: "ownedSettingsManaged".to_string(),
            title: "Managed settings".to_string(),
            description:
                "These extension-defined settings are explicitly owned by Elixir. Downstream tools should not silently redefine them."
                    .to_string(),
            policy: Some(control_policy_managed(
                "These settings are declared as managed by the extension. Elixir owns their meaning and should not silently adopt downstream changes as the new source of truth.",
            )),
            notices,
            fields: managed_fields,
            entities: Vec::new(),
            actions: Vec::new(),
        });
    }

    Ok(sections)
}

async fn read_owned_setting_field(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
) -> anyhow::Result<ExtensionControlField> {
    let (value, readonly) = read_owned_setting_value(state, store, context, setting).await?;
    Ok(ExtensionControlField {
        id: setting.id.clone(),
        label: setting.label.clone(),
        description: setting
            .description
            .clone()
            .unwrap_or_else(|| default_owned_setting_description(setting)),
        field_type: setting.field_type.clone(),
        value,
        required: setting.required,
        readonly,
        secret: setting.secret,
        options: setting
            .options
            .iter()
            .map(|option| ExtensionControlOption {
                value: option.value.clone(),
                label: option.label.clone(),
            })
            .collect(),
        validation: None,
    })
}

async fn read_owned_setting_value(
    _state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
) -> anyhow::Result<(serde_json::Value, bool)> {
    let storage_type = setting.storage.r#type.trim().to_ascii_lowercase();
    match storage_type.as_str() {
        "extension_setting" => {
            let key = extension_control_setting_key(
                &context.extension.extension_id,
                &setting.storage.key,
            );
            let value = store
                .get_extension_setting(&key)
                .await?
                .unwrap_or(serde_json::Value::Null);
            Ok((value, false))
        }
        "instance_setting" => {
            let Some(instance) = context.selected_instance.as_ref() else {
                return Ok((serde_json::Value::Null, true));
            };
            let value = instance
                .config_json
                .as_ref()
                .and_then(|config| config.get(&setting.storage.key))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok((value, false))
        }
        "global_secret" => {
            let key = extension_control_setting_key(
                &context.extension.extension_id,
                &setting.storage.key,
            );
            let present = store
                .get_secret(SecretScope::Global, None, &key)
                .await?
                .is_some();
            Ok((secret_field_value(present), false))
        }
        "instance_secret" => {
            let Some(instance) = context.selected_instance.as_ref() else {
                return Ok((serde_json::Value::Null, true));
            };
            let present = store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance.instance_id),
                    &setting.storage.key,
                )
                .await?
                .is_some();
            Ok((secret_field_value(present), false))
        }
        other => anyhow::bail!("unsupported control storage type '{other}'"),
    }
}

async fn write_owned_setting_value(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
    raw_value: &serde_json::Value,
) -> anyhow::Result<()> {
    let normalized = normalize_owned_setting_value(setting, raw_value)?;
    let storage_type = setting.storage.r#type.trim().to_ascii_lowercase();
    match storage_type.as_str() {
        "extension_setting" => {
            let key = extension_control_setting_key(
                &context.extension.extension_id,
                &setting.storage.key,
            );
            if normalized.is_null() {
                store.delete_extension_setting(&key).await?;
            } else {
                store.upsert_extension_setting(&key, &normalized).await?;
            }
        }
        "instance_setting" => {
            let instance = context.selected_instance.as_ref().ok_or_else(|| {
                anyhow::anyhow!("no active instance is available for this extension yet")
            })?;
            let updated = merge_instance_control_setting(
                instance.config_json.as_ref(),
                &setting.storage.key,
                normalized,
            )?;
            store
                .update_instance_config(instance.instance_id, Some(&updated))
                .await?;
        }
        "global_secret" => {
            let text = normalize_secret_value_for_storage(&normalized)?;
            let key = extension_control_setting_key(
                &context.extension.extension_id,
                &setting.storage.key,
            );
            upsert_generic_secret(state, store, SecretScope::Global, None, &key, &text).await?;
        }
        "instance_secret" => {
            let instance = context.selected_instance.as_ref().ok_or_else(|| {
                anyhow::anyhow!("no active instance is available for this extension yet")
            })?;
            let text = normalize_secret_value_for_storage(&normalized)?;
            upsert_generic_secret(
                state,
                store,
                SecretScope::Instance,
                Some(instance.instance_id),
                &setting.storage.key,
                &text,
            )
            .await?;
        }
        other => anyhow::bail!("unsupported control storage type '{other}'"),
    }
    Ok(())
}

fn normalize_owned_setting_value(
    setting: &ManifestControlOwnedSetting,
    raw_value: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let field_type = setting.field_type.trim().to_ascii_lowercase();
    match field_type.as_str() {
        "toggle" => raw_value
            .as_bool()
            .map(serde_json::Value::Bool)
            .ok_or_else(|| anyhow::anyhow!("{} must be a boolean", setting.label)),
        "number" => {
            if raw_value.is_number() {
                Ok(raw_value.clone())
            } else if let Some(text) = raw_value.as_str() {
                let parsed = text
                    .trim()
                    .parse::<f64>()
                    .with_context(|| format!("{} must be a number", setting.label))?;
                Ok(serde_json::json!(parsed))
            } else {
                anyhow::bail!("{} must be a number", setting.label)
            }
        }
        "select" => {
            if !setting
                .options
                .iter()
                .any(|option| option.value == *raw_value)
            {
                anyhow::bail!("{} must be one of the declared options", setting.label);
            }
            Ok(raw_value.clone())
        }
        "text" | "password" => {
            let text = raw_value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("{} must be text", setting.label))?;
            if setting.required && text.trim().is_empty() {
                anyhow::bail!("{} is required", setting.label);
            }
            Ok(serde_json::Value::String(text.to_string()))
        }
        other => anyhow::bail!("unsupported field type '{other}'"),
    }
}

fn normalize_secret_value_for_storage(value: &serde_json::Value) -> anyhow::Result<String> {
    let text = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("secret values must be text"))?
        .trim()
        .to_string();
    if text.is_empty() {
        anyhow::bail!("secret value is required");
    }
    Ok(text)
}

async fn upsert_generic_secret(
    state: &AppState,
    store: &ExtensionStore<'_>,
    scope: SecretScope,
    scope_id: Option<Uuid>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let encrypted = state.secrets.encrypt(value)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope,
            scope_id,
            key: key.to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await?;
    Ok(())
}

fn merge_instance_control_setting(
    existing: Option<&serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let mut object = existing
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if value.is_null() {
        object.remove(key);
    } else {
        object.insert(key.to_string(), value);
    }
    Ok(serde_json::Value::Object(object))
}

fn build_native_only_sections(
    control_surface: &ManifestControlSurface,
) -> Vec<ExtensionControlSection> {
    if control_surface.native_only.is_empty() {
        return Vec::new();
    }

    vec![ExtensionControlSection {
        id: "nativeOnly".to_string(),
        title: "Native-only areas".to_string(),
        description:
            "This extension declares parts of its product surface that remain native-only. Elixir exposes that boundary instead of pretending to manage it."
                .to_string(),
        policy: None,
        notices: control_surface
            .native_only
            .iter()
            .map(|area| {
                control_notice(
                    "info",
                    "native_only",
                    &area.title,
                    area.description.clone().unwrap_or_else(|| {
                        "This area is intentionally managed only in the extension's native interface."
                            .to_string()
                    }),
                )
            })
            .collect(),
        fields: Vec::new(),
        entities: Vec::new(),
        actions: Vec::new(),
    }]
}

fn build_runtime_bridge_gap_section(
    control_surface: &ManifestControlSurface,
) -> Option<ExtensionControlSection> {
    let mut notices = Vec::new();
    if !control_surface.observed_state.is_empty() {
        notices.push(control_notice(
            "info",
            "runtime_bridge_required",
            "Observed state requires a runtime bridge",
            "This extension declares observed live state, but generic_v1 does not yet have a runtime bridge for fetching those live values automatically.",
        ));
    }
    if !control_surface.entities.is_empty() || !control_surface.actions.is_empty() {
        notices.push(control_notice(
            "info",
            "runtime_bridge_required",
            "Entities and actions require a runtime bridge",
            "This extension declares entities or actions, but generic_v1 does not yet have a runtime bridge for executing extension-defined live operations automatically.",
        ));
    }
    if notices.is_empty() {
        return None;
    }
    Some(ExtensionControlSection {
        id: "runtimeBridge".to_string(),
        title: "Runtime bridge".to_string(),
        description:
            "The platform control contract is active here, but live observed state and extension-defined runtime actions still require a future generic runtime bridge."
                .to_string(),
        policy: Some(control_policy_observed(
            "The platform owns the meaning of observed state, but the extension still needs a runtime bridge before Elixir can fetch or execute those declarations live.",
        )),
        notices,
        fields: Vec::new(),
        entities: Vec::new(),
        actions: Vec::new(),
    })
}

fn default_owned_setting_description(setting: &ManifestControlOwnedSetting) -> String {
    if setting.ownership_mode().eq_ignore_ascii_case("seeded") {
        "Elixir seeds this extension-defined default but does not silently treat downstream overrides as drift."
            .to_string()
    } else {
        "Elixir manages this extension-defined setting intentionally and does not silently adopt downstream edits as the new source of truth."
            .to_string()
    }
}

fn secret_field_value(present: bool) -> serde_json::Value {
    if present {
        serde_json::Value::String("saved".to_string())
    } else {
        serde_json::Value::Null
    }
}

fn uses_instance_scope(setting: &ManifestControlOwnedSetting) -> bool {
    matches!(
        setting.storage.r#type.trim().to_ascii_lowercase().as_str(),
        "instance_setting" | "instance_secret"
    )
}

fn extension_control_setting_key(extension_id: &str, key: &str) -> String {
    format!("control_surface:{extension_id}:{key}")
}
