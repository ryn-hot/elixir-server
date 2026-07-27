use std::collections::{HashMap, HashSet};

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

        let account_setting_ids = live_account_setting_ids(context, &control_surface);
        let mut sections = Vec::new();
        if let Some(section) = build_live_account_section(
            state,
            store,
            context,
            &control_surface,
            &account_setting_ids,
        )
        .await?
        {
            sections.push(section);
        }
        sections.extend(
            build_owned_setting_sections(
                state,
                store,
                context,
                &control_surface,
                &account_setting_ids,
            )
            .await?,
        );
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

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        _params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        if action_id != "disconnect_live_account" {
            anyhow::bail!("unsupported control action '{action_id}'");
        }
        let Some(control_surface) = load_manifest_control_surface(context)? else {
            anyhow::bail!("this extension does not expose a generic control contract");
        };
        let setting_ids = live_account_setting_ids(context, &control_surface);
        if setting_ids.is_empty() {
            anyhow::bail!("this extension does not declare a Live account");
        }
        for setting in control_surface
            .owned_settings
            .iter()
            .filter(|setting| setting_ids.contains(setting.id.as_str()))
        {
            clear_owned_setting_value(store, context, setting).await?;
        }
        trigger_extensions_reconcile(state, "Live provider account disconnected");
        Ok("Account disconnected.".to_string())
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
    excluded_setting_ids: &HashSet<String>,
) -> anyhow::Result<Vec<ExtensionControlSection>> {
    let mut seeded_fields = Vec::new();
    let mut seeded_advanced_fields = Vec::new();
    let mut managed_fields = Vec::new();
    let mut managed_advanced_fields = Vec::new();
    let mut seeded_requires_instance = false;
    let mut seeded_advanced_requires_instance = false;
    let mut managed_requires_instance = false;
    let mut managed_advanced_requires_instance = false;

    for setting in &control_surface.owned_settings {
        if excluded_setting_ids.contains(&setting.id) {
            continue;
        }
        let field = read_owned_setting_field(state, store, context, setting).await?;
        let requires_instance =
            field.readonly && uses_instance_scope(setting) && context.selected_instance.is_none();
        if setting.ownership_mode().eq_ignore_ascii_case("seeded") {
            if setting.advanced {
                seeded_advanced_requires_instance |= requires_instance;
                seeded_advanced_fields.push(field);
            } else {
                seeded_requires_instance |= requires_instance;
                seeded_fields.push(field);
            }
        } else if setting.advanced {
            managed_advanced_requires_instance |= requires_instance;
            managed_advanced_fields.push(field);
        } else {
            managed_requires_instance |= requires_instance;
            managed_fields.push(field);
        }
    }

    let mut sections = Vec::new();
    if !seeded_fields.is_empty() {
        sections.push(seed_settings_section(
            "ownedSettingsSeeded",
            "Seeded settings",
            seeded_requires_instance,
            seeded_fields,
        ));
    }
    if !seeded_advanced_fields.is_empty() {
        sections.push(seed_settings_section(
            "ownedSettingsSeededAdvanced",
            "Advanced settings",
            seeded_advanced_requires_instance,
            seeded_advanced_fields,
        ));
    }
    if !managed_fields.is_empty() {
        sections.push(managed_settings_section(
            "ownedSettingsManaged",
            "Managed settings",
            managed_requires_instance,
            managed_fields,
        ));
    }
    if !managed_advanced_fields.is_empty() {
        sections.push(managed_settings_section(
            "ownedSettingsManagedAdvanced",
            "Advanced managed settings",
            managed_advanced_requires_instance,
            managed_advanced_fields,
        ));
    }

    Ok(sections)
}

fn live_account_setting_ids(
    context: &ExtensionControlContext,
    control_surface: &ManifestControlSurface,
) -> HashSet<String> {
    let required_storage_keys = context
        .manifest
        .provides
        .iter()
        .filter(|provide| {
            provide.capability == crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY
        })
        .filter_map(|provide| provide.scope.as_ref())
        .filter(|scope| scope.requires_account)
        .flat_map(|scope| scope.required_fields.iter().cloned())
        .collect::<HashSet<_>>();
    control_surface
        .owned_settings
        .iter()
        .filter(|setting| required_storage_keys.contains(&setting.storage.key))
        .map(|setting| setting.id.clone())
        .collect()
}

async fn build_live_account_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    control_surface: &ManifestControlSurface,
    setting_ids: &HashSet<String>,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    if setting_ids.is_empty() {
        return Ok(None);
    }

    let mut fields = Vec::new();
    let mut configured = context.selected_instance.is_some();
    for setting in control_surface
        .owned_settings
        .iter()
        .filter(|setting| setting_ids.contains(setting.id.as_str()))
    {
        let field = read_owned_setting_field(state, store, context, setting).await?;
        configured &= owned_setting_field_is_present(&field);
        fields.push(field);
    }

    let mut notices = Vec::new();
    if !configured {
        notices.push(control_notice(
            "warning",
            "live_account_required",
            "Connect account",
            "Enter the required provider account details to load this Live service.",
        ));
    }

    let mut actions = Vec::new();
    if let (Some(account_setup), Some(instance)) = (
        control_surface.account_setup.as_ref(),
        context.selected_instance.as_ref(),
    ) {
        actions.push(ExtensionControlAction {
            id: "start_live_account_setup".to_string(),
            label: if configured {
                "Reconnect account".to_string()
            } else {
                "Connect account".to_string()
            },
            description: "Open the provider's account setup page and return the configured account to this extension instance."
                .to_string(),
            kind: "primary".to_string(),
            params: Some(serde_json::json!({
                "accountSetup": account_setup.mode.clone(),
                "instanceId": instance.instance_id,
            })),
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        });
    }
    if configured {
        actions.push(ExtensionControlAction {
            id: "disconnect_live_account".to_string(),
            label: "Disconnect account".to_string(),
            description: "Remove this instance's provider account details from Elixir.".to_string(),
            kind: "danger".to_string(),
            params: None,
            confirm_text: Some(
                "Disconnect this provider account? This does not cancel the upstream subscription."
                    .to_string(),
            ),
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        });
    }

    Ok(Some(ExtensionControlSection {
        id: "liveAccount".to_string(),
        title: "Account".to_string(),
        description: if configured {
            "This extension instance has the account details required by its Live provider."
                .to_string()
        } else {
            "Connect the account used by this Live provider. Elixir stores it only for this extension instance."
                .to_string()
        },
        policy: Some(control_policy_managed(
            "Elixir stores these extension-declared account fields for this instance.",
        )),
        notices,
        fields,
        entities: Vec::new(),
        actions,
    }))
}

fn owned_setting_field_is_present(field: &ExtensionControlField) -> bool {
    match &field.value {
        serde_json::Value::Null => false,
        serde_json::Value::String(value) => !value.trim().is_empty(),
        _ => true,
    }
}

fn seed_settings_section(
    id: &str,
    title: &str,
    requires_instance: bool,
    fields: Vec<ExtensionControlField>,
) -> ExtensionControlSection {
    let mut notices = Vec::new();
    if requires_instance {
        notices.push(control_notice(
            "warning",
            "instance_required",
            "Enable a default instance first",
            "This extension stores some seeded defaults per instance. Create or enable a default instance before editing them here.",
        ));
    }
    ExtensionControlSection {
        id: id.to_string(),
        title: title.to_string(),
        description:
            "Elixir can seed these defaults for the extension, but downstream overrides are allowed and can become the new live value."
                .to_string(),
        policy: Some(control_policy_seeded(
            "These are extension-defined seeded defaults. Elixir writes them intentionally, but does not treat downstream changes as drift by default.",
        )),
        notices,
        fields,
        entities: Vec::new(),
        actions: Vec::new(),
    }
}

fn managed_settings_section(
    id: &str,
    title: &str,
    requires_instance: bool,
    fields: Vec<ExtensionControlField>,
) -> ExtensionControlSection {
    let mut notices = Vec::new();
    if requires_instance {
        notices.push(control_notice(
            "warning",
            "instance_required",
            "Enable a default instance first",
            "This extension stores some managed settings per instance. Create or enable a default instance before editing them here.",
        ));
    }
    ExtensionControlSection {
        id: id.to_string(),
        title: title.to_string(),
        description:
            "These extension-defined settings are explicitly owned by Elixir. Downstream tools should not silently redefine them."
                .to_string(),
        policy: Some(control_policy_managed(
            "These settings are declared as managed by the extension. Elixir owns their meaning and should not silently adopt downstream changes as the new source of truth.",
        )),
        notices,
        fields,
        entities: Vec::new(),
        actions: Vec::new(),
    }
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
                .or_else(|| setting.default.clone())
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
                .or_else(|| setting.default.clone())
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

pub(super) async fn write_owned_setting_value(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
    raw_value: &serde_json::Value,
) -> anyhow::Result<()> {
    if clear_optional_secret_value_if_requested(store, context, setting, raw_value).await? {
        return Ok(());
    }

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

async fn clear_optional_secret_value_if_requested(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
    raw_value: &serde_json::Value,
) -> anyhow::Result<bool> {
    let clear_requested = setting.secret
        && !setting.required
        && (raw_value.is_null()
            || raw_value
                .as_str()
                .is_some_and(|value| value.trim().is_empty()));
    if !clear_requested {
        return Ok(false);
    }

    clear_owned_setting_value(store, context, setting).await?;
    Ok(true)
}

async fn clear_owned_setting_value(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    setting: &ManifestControlOwnedSetting,
) -> anyhow::Result<()> {
    match setting.storage.r#type.trim().to_ascii_lowercase().as_str() {
        "instance_setting" => {
            let instance = context
                .selected_instance
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no extension instance is available"))?;
            let current = store
                .get_instance(instance.instance_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("extension instance not found"))?;
            let updated = merge_instance_control_setting(
                current.config_json.as_ref(),
                &setting.storage.key,
                serde_json::Value::Null,
            )?;
            store
                .update_instance_config(instance.instance_id, Some(&updated))
                .await?;
        }
        "instance_secret" => {
            let instance = context
                .selected_instance
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no extension instance is available"))?;
            for secret in store
                .list_secrets(
                    Some(SecretScope::Instance),
                    Some(instance.instance_id),
                    Some(&setting.storage.key),
                )
                .await?
            {
                store.delete_secret(secret.secret_id).await?;
            }
        }
        "global_secret" => {
            let key = extension_control_setting_key(
                &context.extension.extension_id,
                &setting.storage.key,
            );
            for secret in store
                .list_secrets(Some(SecretScope::Global), None, Some(&key))
                .await?
            {
                store.delete_secret(secret.secret_id).await?;
            }
        }
        other => anyhow::bail!("unsupported Live account storage type '{other}'"),
    }
    Ok(())
}

pub(super) fn normalize_owned_setting_value(
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

#[cfg(test)]
mod account_tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel},
        },
        extensions::store::{NewExtension, NewExtensionInstance, NewSecret},
    };

    #[tokio::test]
    async fn disconnect_clears_only_declared_instance_account_fields() -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let manifest: ExtensionManifest = serde_json::from_value(serde_json::json!({
            "id": "fixture.live.account",
            "version": "1.0.0",
            "kind": "module",
            "name": "Fixture Live Account",
            "provides": [{
                "capability": "live.catalog_provider",
                "slot": "default",
                "scope": {
                    "requires_account": true,
                    "required_fields": ["username", "password"]
                }
            }],
            "control_surface": {
                "adapter": "generic_v1",
                "owned_settings": [
                    {
                        "id": "username",
                        "label": "Username",
                        "type": "text",
                        "required": true,
                        "ownership": "managed",
                        "storage": {"type": "instance_setting", "key": "username"}
                    },
                    {
                        "id": "password",
                        "label": "Password",
                        "type": "password",
                        "required": true,
                        "secret": true,
                        "ownership": "managed",
                        "storage": {"type": "instance_secret", "key": "password"}
                    }
                ]
            }
        }))?;
        let manifest_json = serde_json::to_value(&manifest)?;
        store
            .upsert_extension(&NewExtension {
                extension_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json,
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: manifest.id.clone(),
                instance_name: "default".to_string(),
                config_json: Some(serde_json::json!({
                    "username": "viewer",
                    "region": "us"
                })),
                enabled: true,
            })
            .await?;
        for key in ["password", "unrelated_secret"] {
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(instance_id),
                    key: key.to_string(),
                    value_encrypted: format!("encrypted-{key}"),
                    rotatable: false,
                })
                .await?;
        }
        let extension = store.get_extension(&manifest.id).await?.expect("extension");
        let instance = store.get_instance(instance_id).await?.expect("instance");
        let context = ExtensionControlContext {
            extension,
            manifest: manifest.clone(),
            summary: ExtensionStatusSummaryItem {
                extension_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                kind: ExtensionKind::Module,
                trust_level: ExtensionTrustLevel::Community,
                enabled: true,
                severity: "ready".to_string(),
                status_code: "ready".to_string(),
                label: "Ready".to_string(),
                description: "Ready".to_string(),
                primary_action: "open".to_string(),
                primary_action_label: "Open".to_string(),
                auto_update: None,
                optional_addons: Vec::new(),
            },
            instances: vec![instance.clone()],
            selected_instance: Some(instance),
            providers: Vec::new(),
            selected_provider: None,
            control_binding: ExtensionControlBinding::GenericManifest,
        };
        let settings = &manifest
            .control_surface
            .as_ref()
            .expect("control surface")
            .owned_settings;
        for setting in settings {
            clear_owned_setting_value(&store, &context, setting).await?;
        }

        let updated = store
            .get_instance(instance_id)
            .await?
            .expect("updated instance");
        assert_eq!(
            updated.config_json,
            Some(serde_json::json!({"region": "us"}))
        );
        assert!(
            store
                .get_secret(SecretScope::Instance, Some(instance_id), "password")
                .await?
                .is_none()
        );
        assert!(
            store
                .get_secret(SecretScope::Instance, Some(instance_id), "unrelated_secret")
                .await?
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn blank_optional_instance_secret_clears_the_saved_value() -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let manifest: ExtensionManifest = serde_json::from_value(serde_json::json!({
            "id": "fixture.live.optional-secret",
            "version": "1.0.0",
            "kind": "module",
            "name": "Fixture Optional Secret",
            "provides": [{
                "capability": "live.catalog_provider",
                "slot": "default",
                "scope": {"requires_account": false}
            }],
            "control_surface": {
                "adapter": "generic_v1",
                "owned_settings": [{
                    "id": "premiumManifestUrl",
                    "label": "Premium manifest URL",
                    "type": "text",
                    "required": false,
                    "secret": true,
                    "ownership": "managed",
                    "storage": {
                        "type": "instance_secret",
                        "key": "premium_manifest_url"
                    }
                }]
            }
        }))?;
        store
            .upsert_extension(&NewExtension {
                extension_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: serde_json::to_value(&manifest)?,
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: manifest.id.clone(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        for key in ["premium_manifest_url", "unrelated_secret"] {
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(instance_id),
                    key: key.to_string(),
                    value_encrypted: format!("encrypted-{key}"),
                    rotatable: false,
                })
                .await?;
        }

        let extension = store.get_extension(&manifest.id).await?.expect("extension");
        let instance = store.get_instance(instance_id).await?.expect("instance");
        let context = ExtensionControlContext {
            extension,
            manifest: manifest.clone(),
            summary: ExtensionStatusSummaryItem {
                extension_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                kind: ExtensionKind::Module,
                trust_level: ExtensionTrustLevel::Community,
                enabled: true,
                severity: "ready".to_string(),
                status_code: "ready".to_string(),
                label: "Ready".to_string(),
                description: "Ready".to_string(),
                primary_action: "open".to_string(),
                primary_action_label: "Open".to_string(),
                auto_update: None,
                optional_addons: Vec::new(),
            },
            instances: vec![instance.clone()],
            selected_instance: Some(instance),
            providers: Vec::new(),
            selected_provider: None,
            control_binding: ExtensionControlBinding::GenericManifest,
        };
        let setting = &manifest
            .control_surface
            .as_ref()
            .expect("control surface")
            .owned_settings[0];

        assert!(
            clear_optional_secret_value_if_requested(
                &store,
                &context,
                setting,
                &serde_json::json!("  "),
            )
            .await?
        );
        assert!(
            store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance_id),
                    "premium_manifest_url",
                )
                .await?
                .is_none()
        );
        assert!(
            store
                .get_secret(SecretScope::Instance, Some(instance_id), "unrelated_secret",)
                .await?
                .is_some()
        );
        assert!(
            !clear_optional_secret_value_if_requested(
                &store,
                &context,
                setting,
                &serde_json::json!("https://premium.example/manifest.json"),
            )
            .await?
        );
        Ok(())
    }
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
