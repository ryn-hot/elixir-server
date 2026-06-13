use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::control_contract::{
    ExtensionControlProvider, GenericManifestControlProvider, UnsupportedControlProvider,
    control_notice, control_policy_managed, control_policy_observed, control_policy_seeded,
};
use super::*;
use crate::debrid::{
    DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY, DebridAccount, DebridServiceKind,
    MAX_DEBRID_CONCURRENT_DOWNLOADS, MIN_DEBRID_CONCURRENT_DOWNLOADS,
    active_debrid_service_from_config, debrid_concurrent_downloads_from_config,
    debrid_secret_exists_for_instance, reconcile_debrid_provider_for_instance,
    test_debrid_service_account, validate_debrid_concurrent_downloads,
};
use crate::drivers::render_nzbget_config_text_updates;
use crate::extensions::cloudstream_registry::{
    CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY, CloudStreamRegistryClient,
    CloudStreamRegistryFetchConfig, CloudStreamRegistryStoreInput,
    apply_cloudstream_source_replacement_recommendation, persist_cloudstream_registry_snapshot,
    seed_cloudstream_recommended_source_pack_for_instance,
};
use crate::extensions::managed_paths::{
    DOWNLOADS_ROOT, NZBGET_CONFIG_TEMPLATE, NZBGET_INCOMPLETE_DIR, NZBGET_LOCK_FILE,
    NZBGET_LOG_FILE, NZBGET_MAIN_DIR, NZBGET_NZB_DIR, NZBGET_QUEUE_DIR, NZBGET_SCRIPT_DIR,
    NZBGET_TEMP_DIR, NZBGET_WEB_DIR,
};
use crate::extensions::nuvio_registry::{
    NuvioRegistryClient, NuvioRegistryFetchConfig, NuvioRegistryStoreInput,
    PRISM_RECOMMENDED_REGISTRY_KEY, persist_nuvio_registry_snapshot,
    seed_prism_recommended_source_pack_for_instance,
};
use crate::extensions::source_artifacts::install_source_module_artifact;
use crate::extensions::store::{
    ExtensionSourceModule, ExtensionSourceModuleVersion, ExtensionSourceRegistry,
};

// First-party providers plug into the same platform-owned control contract as
// the generic manifest path. Their custom logic lives here, but the
// ownership/notice/action primitives are defined by the generic contract layer.
struct ArrManagerControlAdapter {
    implementation: &'static str,
}

#[async_trait::async_trait]
impl ExtensionControlProvider for ArrManagerControlAdapter {
    async fn load_live_snapshot(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<ExtensionControlLiveSnapshot> {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };
        let Some(provider) = context.selected_provider.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };
        let endpoint_json = provider
            .endpoint_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
        let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
        let base_url =
            super::resolve_control_provider_transport_base_url(instance.instance_id, &endpoint)
                .await?;

        match self.implementation {
            "sonarr" => {
                super::load_sonarr_control_snapshot(state, store, instance, &base_url).await
            }
            "radarr" => {
                super::load_radarr_control_snapshot(state, store, instance, &base_url).await
            }
            _ => Ok(ExtensionControlLiveSnapshot::default()),
        }
    }

    async fn build_sections(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let mut sections = Vec::new();
        if let Some(section) =
            super::build_extension_control_settings_section(state, store, context).await?
        {
            sections.push(section);
        }
        if let Some(section) =
            super::build_extension_control_download_client_preference_section(state, store, context)
                .await?
        {
            sections.push(section);
        }
        if let Some(section) =
            super::build_extension_control_managed_items_section(store, context).await?
        {
            sections.push(section);
        }
        Ok(sections)
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        let mut actions = Vec::new();
        if context.selected_provider.is_some() {
            actions.push(build_test_connection_action());
        }
        if context.selected_instance.is_some()
            && matches!(
                context.summary.status_code.as_str(),
                "connection_issue" | "degraded_runtime"
            )
        {
            actions.push(build_repair_connection_issue_action(self.implementation));
        }
        actions
    }

    async fn update_settings(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let Some(instance) = context.selected_instance.as_ref() else {
            anyhow::bail!("no active instance is available for this extension yet");
        };
        if let Some(field_id) = values.keys().find(|field_id| {
            !matches!(
                field_id.as_str(),
                "monitorOnAdd" | "searchOnAdd" | "downloadClientPreference"
            )
        }) {
            anyhow::bail!("unsupported control setting '{field_id}'");
        }

        let mut default_values = HashMap::new();
        for field_id in ["monitorOnAdd", "searchOnAdd"] {
            if let Some(value) = values.get(field_id) {
                default_values.insert(field_id.to_string(), value.clone());
            }
        }
        if !default_values.is_empty() {
            super::save_manager_control_defaults(store, instance.instance_id, &default_values)
                .await?;
        }
        if let Some(value) = values.get("downloadClientPreference") {
            super::update_arr_download_client_preference(state, store, context, value).await?;
        }

        Ok(())
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        match action_id {
            "test_connection" => {
                let snapshot = self.load_live_snapshot(state, store, context).await?;
                Ok(test_connection_message(
                    self.implementation,
                    context,
                    &snapshot,
                ))
            }
            "repair_connection_issue" => {
                repair_arr_connection_issue(self, state, store, context, self.implementation).await
            }
            "repair_managed_invariants" => {
                super::run_extension_control_targeted_managed_repair(state, store, context).await
            }
            "search_missing" | "refresh_manager" => {
                let (base_url, api_key) =
                    super::resolve_extension_control_arr_connection(state, store, context).await?;
                super::execute_extension_control_manager_command(
                    self.implementation,
                    &base_url,
                    &api_key,
                    action_id,
                    None,
                )
                .await
            }
            "search_item" | "refresh_item" | "remove_item" => {
                let provider = context.selected_provider.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("no active provider is available for this extension yet")
                })?;
                let intent =
                    super::resolve_extension_control_intent(store, provider.provider_id, params)
                        .await?;
                let manager_item_id = intent
                    .manager_item_id
                    .as_deref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("manager item id is not available for this item")
                    })?
                    .parse::<i64>()
                    .context("parsing manager item id")?;
                let (base_url, api_key) =
                    super::resolve_extension_control_arr_connection(state, store, context).await?;
                let message = super::execute_extension_control_manager_command(
                    self.implementation,
                    &base_url,
                    &api_key,
                    action_id,
                    Some(manager_item_id),
                )
                .await?;
                if action_id == "remove_item" {
                    store
                        .deactivate_managed_ingest_intent(intent.intent_id)
                        .await?;
                }
                Ok(message)
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

struct DebridControlAdapter;

#[async_trait::async_trait]
impl ExtensionControlProvider for DebridControlAdapter {
    async fn build_sections(
        &self,
        _state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(vec![ExtensionControlSection {
                id: "debridAccounts".to_string(),
                title: "Debrid accounts".to_string(),
                description: "Create a default instance before adding debrid service accounts."
                    .to_string(),
                policy: Some(control_policy_managed(
                    "Debrid account credentials are encrypted instance secrets owned by Elixir.",
                )),
                notices: vec![control_notice(
                    "warning",
                    "instance_required",
                    "Create an instance",
                    "The Debrid module stores service accounts on its default instance.",
                )],
                fields: Vec::new(),
                entities: Vec::new(),
                actions: Vec::new(),
            }]);
        };

        Ok(vec![build_debrid_accounts_section(store, instance).await?])
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        if context.selected_instance.is_some() {
            vec![build_test_connection_action()]
        } else {
            Vec::new()
        }
    }

    async fn update_settings(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let instance = context
            .selected_instance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no active debrid instance is available"))?;
        let mut should_reconcile = false;
        let mut active_validation: Option<(String, String)> = None;
        let active_before = active_debrid_service_from_config(instance.config_json.as_ref())?;

        for (field_id, value) in values {
            if let Some(service) = parse_debrid_token_field_id(field_id)? {
                let token = value.as_str().map(str::trim).unwrap_or_default();
                if token.is_empty() {
                    continue;
                }
                write_debrid_service_token(state, store, instance.instance_id, service, token)
                    .await?;
                mark_debrid_service_validation(
                    store,
                    instance.instance_id,
                    service,
                    "untested",
                    "API token saved. Test the account to validate it.",
                )
                .await?;
                if active_before == service {
                    should_reconcile = true;
                }
                continue;
            }
        }

        for (field_id, value) in values {
            if field_id == "activeService" {
                let service = parse_debrid_service_value(value)?;
                ensure_debrid_service_configured(store, instance.instance_id, service).await?;
                let (validation_state, message) = validate_debrid_service_for_activation(
                    state,
                    store,
                    instance.instance_id,
                    service,
                )
                .await?;
                let mut config = load_instance_config_object(store, instance.instance_id).await?;
                config.insert(
                    "activeService".to_string(),
                    json!(service.implementation_id()),
                );
                config.insert(
                    "lastActiveServiceValidation".to_string(),
                    json!({
                        "state": validation_state,
                        "message": message,
                    }),
                );
                store
                    .update_instance_config(instance.instance_id, Some(&Value::Object(config)))
                    .await?;
                active_validation = Some((validation_state, message));
                should_reconcile = true;
                continue;
            }

            if field_id == DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY {
                let cap = parse_debrid_concurrent_downloads_setting(value)?;
                let mut config = load_instance_config_object(store, instance.instance_id).await?;
                config.insert(
                    DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY.to_string(),
                    json!(cap),
                );
                store
                    .update_instance_config(instance.instance_id, Some(&Value::Object(config)))
                    .await?;
                should_reconcile = true;
                continue;
            }

            if parse_debrid_token_field_id(field_id)?.is_some() {
                continue;
            }

            anyhow::bail!("unsupported debrid control setting '{field_id}'");
        }

        if should_reconcile {
            let provider_id =
                reconcile_debrid_provider_for_instance(&state.db_pool, store, instance.instance_id)
                    .await?;
            if let Some((validation_state, message)) = active_validation {
                let health = if validation_state == "healthy" {
                    ProviderHealthState::Healthy
                } else {
                    ProviderHealthState::Unknown
                };
                let readiness = if validation_state == "healthy" {
                    ProviderReadinessPhase::DriverReady
                } else {
                    ProviderReadinessPhase::Unknown
                };
                store.update_provider_health(provider_id, health).await?;
                store
                    .upsert_provider_readiness(provider_id, readiness, Some(&message))
                    .await?;
            }
            super::trigger_extensions_reconcile(state, "debrid active service update");
        }
        Ok(())
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        let instance = context
            .selected_instance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no active debrid instance is available"))?;
        match action_id {
            "test_connection" | "test_debrid_service" => {
                let service = debrid_service_from_action_or_active(params, instance)?;
                run_debrid_service_test(state, store, instance.instance_id, service).await
            }
            "set_active_debrid_service" => {
                let service = debrid_service_from_action(params)?;
                ensure_debrid_service_configured(store, instance.instance_id, service).await?;
                let (validation_state, message) = validate_debrid_service_for_activation(
                    state,
                    store,
                    instance.instance_id,
                    service,
                )
                .await?;
                let mut config = load_instance_config_object(store, instance.instance_id).await?;
                config.insert(
                    "activeService".to_string(),
                    json!(service.implementation_id()),
                );
                config.insert(
                    "lastActiveServiceValidation".to_string(),
                    json!({
                        "state": validation_state,
                        "message": message,
                    }),
                );
                store
                    .update_instance_config(instance.instance_id, Some(&Value::Object(config)))
                    .await?;
                let provider_id = reconcile_debrid_provider_for_instance(
                    &state.db_pool,
                    store,
                    instance.instance_id,
                )
                .await?;
                let health = if validation_state == "healthy" {
                    ProviderHealthState::Healthy
                } else {
                    ProviderHealthState::Unknown
                };
                let readiness = if validation_state == "healthy" {
                    ProviderReadinessPhase::DriverReady
                } else {
                    ProviderReadinessPhase::Unknown
                };
                store.update_provider_health(provider_id, health).await?;
                store
                    .upsert_provider_readiness(provider_id, readiness, Some(&message))
                    .await?;
                super::trigger_extensions_reconcile(state, "debrid active service switch");
                Ok(message)
            }
            "remove_debrid_service_account" => {
                let service = debrid_service_from_action(params)?;
                remove_debrid_service_token(store, instance.instance_id, service).await?;
                mark_debrid_service_validation(
                    store,
                    instance.instance_id,
                    service,
                    "not_configured",
                    "Account removed.",
                )
                .await?;
                let active = active_debrid_service_from_config(instance.config_json.as_ref())?;
                if active == service {
                    reconcile_debrid_provider_for_instance(
                        &state.db_pool,
                        store,
                        instance.instance_id,
                    )
                    .await?;
                }
                Ok(format!("{} account removed.", service.display_name()))
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

async fn validate_debrid_service_for_activation(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> anyhow::Result<(String, String)> {
    match test_debrid_service_account(state, store, instance_id, service).await {
        Ok(account) => {
            let message = format!(
                "{} account '{}' is reachable.",
                service.display_name(),
                debrid_account_display_id(&account)
            );
            mark_debrid_service_validation(store, instance_id, service, "healthy", &message)
                .await?;
            Ok(("healthy".to_string(), message))
        }
        Err(err) if debrid_adapter_pending(&err) => {
            let message = format!(
                "{} account token is saved. Native validation for this service is not implemented yet.",
                service.display_name()
            );
            mark_debrid_service_validation(
                store,
                instance_id,
                service,
                "adapter_pending",
                &message,
            )
            .await?;
            Ok(("adapter_pending".to_string(), message))
        }
        Err(err) => {
            let message = format!(
                "{} account validation failed: {err}",
                service.display_name()
            );
            mark_debrid_service_validation(store, instance_id, service, "unhealthy", &message)
                .await?;
            Err(anyhow::anyhow!(message))
        }
    }
}

async fn build_debrid_accounts_section(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
) -> anyhow::Result<ExtensionControlSection> {
    let active_service = active_debrid_service_from_config(instance.config_json.as_ref())?;
    let concurrent_downloads =
        debrid_concurrent_downloads_from_config(instance.config_json.as_ref());
    let validation = debrid_validation_map(instance.config_json.as_ref());
    let mut fields = vec![
        ExtensionControlField {
            id: "activeService".to_string(),
            label: "Active service".to_string(),
            description: "New acquisition.debrid.default submissions use this debrid service."
                .to_string(),
            field_type: "select".to_string(),
            value: json!(active_service.implementation_id()),
            required: true,
            readonly: false,
            secret: false,
            options: DebridServiceKind::ALL
                .into_iter()
                .map(|service| ExtensionControlOption {
                    value: json!(service.implementation_id()),
                    label: service.display_name().to_string(),
                })
                .collect(),
            validation: None,
        },
        ExtensionControlField {
            id: DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY.to_string(),
            label: "Concurrent Debrid downloads".to_string(),
            description: format!(
                "Default is 1. Check your active Debrid service and account plan before changing this. Values above provider limits can cause rate limits or failed downloads. Allowed range: {MIN_DEBRID_CONCURRENT_DOWNLOADS}-{MAX_DEBRID_CONCURRENT_DOWNLOADS}."
            ),
            field_type: "number".to_string(),
            value: json!(concurrent_downloads),
            required: true,
            readonly: false,
            secret: false,
            options: Vec::new(),
            validation: None,
        },
    ];
    let mut entities = Vec::new();

    for service in DebridServiceKind::ALL {
        let token_present =
            debrid_secret_exists_for_instance(store, instance.instance_id, service).await?;
        let token_field_id = debrid_token_field_id(service);
        fields.push(ExtensionControlField {
            id: token_field_id,
            label: format!("{} API token", service.display_name()),
            description: format!(
                "Encrypted {} token used only by the built-in Debrid provider.",
                service.display_name()
            ),
            field_type: "password".to_string(),
            value: debrid_secret_field_value(token_present),
            required: false,
            readonly: false,
            secret: true,
            options: Vec::new(),
            validation: None,
        });

        entities.push(build_debrid_account_entity(
            instance.instance_id,
            service,
            token_present,
            active_service == service,
            validation.get(service.implementation_id()),
        ));
    }

    Ok(ExtensionControlSection {
        id: "debridAccounts".to_string(),
        title: "Debrid accounts".to_string(),
        description:
            "Store debrid account tokens and choose the one Elixir uses for direct HTTPS debrid downloads."
                .to_string(),
        policy: Some(control_policy_managed(
            "Debrid credentials are encrypted instance secrets. Source extensions never receive them.",
        )),
        notices: Vec::new(),
        fields,
        entities,
        actions: Vec::new(),
    })
}

fn build_debrid_account_entity(
    instance_id: Uuid,
    service: DebridServiceKind,
    token_present: bool,
    active: bool,
    validation: Option<&Value>,
) -> ExtensionControlEntity {
    let validation_state = validation
        .and_then(|value| value.get("state"))
        .and_then(Value::as_str)
        .unwrap_or(if token_present {
            "untested"
        } else {
            "not_configured"
        });
    let validation_message = validation
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(if token_present {
            "API token is saved."
        } else {
            "No API token is saved."
        });
    let last_tested_at = validation
        .and_then(|value| value.get("lastTestedAt"))
        .and_then(Value::as_str);
    let mut details = vec![
        if active {
            "Active for new debrid submissions".to_string()
        } else {
            "Inactive".to_string()
        },
        if token_present {
            "Account token saved".to_string()
        } else {
            "Account token missing".to_string()
        },
        format!("Validation: {validation_state}"),
        validation_message.to_string(),
    ];
    if let Some(last_tested_at) = last_tested_at {
        details.push(format!("Last tested: {last_tested_at}"));
    }

    let mut actions = vec![debrid_service_action(
        "test_debrid_service",
        if token_present {
            "Test connection"
        } else {
            "Add account"
        },
        &format!("Validate the {} account token.", service.display_name()),
        if token_present {
            "secondary"
        } else {
            "primary"
        },
        service,
        Some(instance_id),
        !token_present,
        None,
    )];
    if !active {
        actions.push(debrid_service_action(
            "set_active_debrid_service",
            "Set active",
            &format!("Use {} for new debrid submissions.", service.display_name()),
            "primary",
            service,
            Some(instance_id),
            !token_present,
            None,
        ));
    }
    if token_present {
        actions.push(debrid_service_action(
            "remove_debrid_service_account",
            "Remove account",
            &format!("Remove the saved {} token.", service.display_name()),
            "danger",
            service,
            None,
            false,
            Some(format!(
                "Remove the saved {} token from this Debrid instance?",
                service.display_name()
            )),
        ));
    }
    actions.push(debrid_service_docs_action(service));

    ExtensionControlEntity {
        id: format!("debridAccount.{}", service.implementation_id()),
        title: service.display_name().to_string(),
        subtitle: Some(if active {
            "Active".to_string()
        } else if token_present {
            "Configured".to_string()
        } else {
            "Not configured".to_string()
        }),
        details,
        actions,
    }
}

fn debrid_account_display_id(account: &DebridAccount) -> String {
    account
        .username
        .as_deref()
        .or(account.account_id.as_deref())
        .unwrap_or("unknown")
        .to_string()
}

fn debrid_service_action(
    id: &str,
    label: &str,
    description: &str,
    kind: &str,
    service: DebridServiceKind,
    instance_id: Option<Uuid>,
    prompt_for_token: bool,
    confirm_text: Option<String>,
) -> ExtensionControlAction {
    ExtensionControlAction {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        kind: kind.to_string(),
        params: Some(json!({ "service": service.implementation_id() })),
        confirm_text,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: if prompt_for_token {
            vec!["API token".to_string()]
        } else {
            Vec::new()
        },
        secret_keys: if prompt_for_token {
            vec![service.secret_key().to_string()]
        } else {
            Vec::new()
        },
        secret_scope_instance_id: if prompt_for_token { instance_id } else { None },
    }
}

fn debrid_service_docs_action(service: DebridServiceKind) -> ExtensionControlAction {
    ExtensionControlAction {
        id: "open_debrid_service_docs".to_string(),
        label: "API docs".to_string(),
        description: format!("Open {} API documentation.", service.display_name()),
        kind: "secondary".to_string(),
        params: Some(json!({ "service": service.implementation_id() })),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: Some(service.docs_url().to_string()),
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

async fn run_debrid_service_test(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> anyhow::Result<String> {
    ensure_debrid_service_configured(store, instance_id, service).await?;
    match test_debrid_service_account(state, store, instance_id, service).await {
        Ok(account) => {
            let message = format!(
                "{} account '{}' is reachable.",
                service.display_name(),
                debrid_account_display_id(&account)
            );
            mark_debrid_service_validation(store, instance_id, service, "healthy", &message)
                .await?;
            if let Some(provider) = active_debrid_provider_for_instance(store, instance_id).await? {
                if provider.implementation.as_deref() == Some(service.implementation_id()) {
                    store
                        .update_provider_health(provider.provider_id, ProviderHealthState::Healthy)
                        .await?;
                    store
                        .upsert_provider_readiness(
                            provider.provider_id,
                            ProviderReadinessPhase::DriverReady,
                            Some(&message),
                        )
                        .await?;
                }
            }
            Ok(message)
        }
        Err(err) if debrid_adapter_pending(&err) => {
            let message = format!(
                "{} account token is saved. Native validation for this service is not implemented yet.",
                service.display_name()
            );
            mark_debrid_service_validation(
                store,
                instance_id,
                service,
                "adapter_pending",
                &message,
            )
            .await?;
            if let Some(provider) = active_debrid_provider_for_instance(store, instance_id).await? {
                if provider.implementation.as_deref() == Some(service.implementation_id()) {
                    store
                        .update_provider_health(provider.provider_id, ProviderHealthState::Unknown)
                        .await?;
                    store
                        .upsert_provider_readiness(
                            provider.provider_id,
                            ProviderReadinessPhase::Unknown,
                            Some(&message),
                        )
                        .await?;
                }
            }
            Ok(message)
        }
        Err(err) => {
            let message = format!(
                "{} account validation failed: {err}",
                service.display_name()
            );
            mark_debrid_service_validation(store, instance_id, service, "unhealthy", &message)
                .await?;
            if let Some(provider) = active_debrid_provider_for_instance(store, instance_id).await? {
                if provider.implementation.as_deref() == Some(service.implementation_id()) {
                    store
                        .update_provider_health(
                            provider.provider_id,
                            ProviderHealthState::Unhealthy,
                        )
                        .await?;
                    store
                        .upsert_provider_readiness(
                            provider.provider_id,
                            ProviderReadinessPhase::Unknown,
                            Some(&message),
                        )
                        .await?;
                }
            }
            Err(anyhow::anyhow!(message))
        }
    }
}

fn debrid_adapter_pending(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("native adapter is not implemented yet")
}

async fn ensure_debrid_service_configured(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> anyhow::Result<()> {
    if debrid_secret_exists_for_instance(store, instance_id, service).await? {
        Ok(())
    } else {
        anyhow::bail!(
            "Add a {} API token before selecting it as the active debrid service.",
            service.display_name()
        )
    }
}

async fn write_debrid_service_token(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
    token: &str,
) -> anyhow::Result<()> {
    let encrypted = state.secrets.encrypt(token)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: service.secret_key().to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await?;
    Ok(())
}

async fn remove_debrid_service_token(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> anyhow::Result<()> {
    for key in service.secret_keys_for_read() {
        if let Some(secret) = store
            .get_secret(SecretScope::Instance, Some(instance_id), key)
            .await?
        {
            store.delete_secret(secret.secret_id).await?;
        }
    }
    Ok(())
}

async fn active_debrid_provider_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<Option<Provider>> {
    Ok(store
        .list_providers(Some(instance_id))
        .await?
        .into_iter()
        .find(|provider| provider.capability == "debrid.resolver" && provider.slot_id == "default"))
}

async fn mark_debrid_service_validation(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
    state: &str,
    message: &str,
) -> anyhow::Result<()> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("debrid instance '{instance_id}' no longer exists"))?;
    let mut config = instance
        .config_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut validations = config
        .get("serviceValidation")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    validations.insert(
        service.implementation_id().to_string(),
        json!({
            "state": state,
            "message": message,
            "lastTestedAt": Utc::now().to_rfc3339(),
        }),
    );
    config.insert("serviceValidation".to_string(), Value::Object(validations));
    store
        .update_instance_config(instance_id, Some(&Value::Object(config)))
        .await?;
    Ok(())
}

async fn load_instance_config_object(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<serde_json::Map<String, Value>> {
    Ok(store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("debrid instance '{instance_id}' no longer exists"))?
        .config_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default())
}

fn debrid_validation_map(config: Option<&Value>) -> serde_json::Map<String, Value> {
    config
        .and_then(|value| value.get("serviceValidation"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn parse_debrid_service_value(value: &Value) -> anyhow::Result<DebridServiceKind> {
    let raw = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("debrid service must be text"))?;
    DebridServiceKind::from_implementation_id(raw)
}

fn parse_debrid_concurrent_downloads_setting(value: &Value) -> anyhow::Result<i64> {
    let raw = value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| {
            value
                .as_f64()
                .filter(|number| number.fract() == 0.0)
                .map(|number| number as i64)
        })
        .or_else(|| {
            value
                .as_str()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .and_then(|text| text.parse::<i64>().ok())
        })
        .ok_or_else(|| anyhow::anyhow!("Debrid concurrent downloads must be a whole number"))?;
    validate_debrid_concurrent_downloads(raw)
}

fn debrid_service_from_action(
    params: &HashMap<String, Value>,
) -> anyhow::Result<DebridServiceKind> {
    let raw = params
        .get("service")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("service is required"))?;
    DebridServiceKind::from_implementation_id(raw)
}

fn debrid_service_from_action_or_active(
    params: &HashMap<String, Value>,
    instance: &ExtensionInstance,
) -> anyhow::Result<DebridServiceKind> {
    match params.get("service").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => DebridServiceKind::from_implementation_id(value),
        _ => active_debrid_service_from_config(instance.config_json.as_ref()),
    }
}

fn parse_debrid_token_field_id(field_id: &str) -> anyhow::Result<Option<DebridServiceKind>> {
    let Some(raw) = field_id.strip_prefix("token.") else {
        return Ok(None);
    };
    Ok(Some(DebridServiceKind::from_implementation_id(raw)?))
}

fn debrid_token_field_id(service: DebridServiceKind) -> String {
    format!("token.{}", service.implementation_id())
}

fn debrid_secret_field_value(present: bool) -> Value {
    if present { json!("saved") } else { Value::Null }
}

struct ProwlarrControlAdapter;

#[async_trait::async_trait]
impl ExtensionControlProvider for ProwlarrControlAdapter {
    async fn load_live_snapshot(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<ExtensionControlLiveSnapshot> {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };
        let Some(provider) = context.selected_provider.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };
        let endpoint_json = provider
            .endpoint_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
        let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
        let base_url =
            super::resolve_control_provider_transport_base_url(instance.instance_id, &endpoint)
                .await?;
        super::load_prowlarr_control_snapshot(state, store, instance, &base_url).await
    }

    async fn build_sections(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let mut sections = Vec::new();
        if let Some(section) =
            super::build_extension_control_prowlarr_indexers_section(state, store, context).await?
        {
            sections.push(section);
        }
        if let Some(section) =
            super::build_extension_control_prowlarr_connector_section(state, store, context).await?
        {
            sections.push(section);
        }
        Ok(sections)
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        if context.selected_provider.is_some() {
            vec![build_test_connection_action()]
        } else {
            Vec::new()
        }
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        match action_id {
            "test_connection" => {
                let snapshot = self.load_live_snapshot(state, store, context).await?;
                Ok(test_connection_message("prowlarr", context, &snapshot))
            }
            "activate_connector" => {
                let target_extension_id = params
                    .get("extensionId")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("extensionId is required"))?;

                let title = params
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(target_extension_id);

                if let Some(existing) = store.get_extension(target_extension_id).await? {
                    if !existing.enabled {
                        store
                            .set_extension_enabled(target_extension_id, true)
                            .await?;
                    }
                } else {
                    let entry = super::load_cached_registry_entry_by_extension_id(
                        state,
                        target_extension_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("connector is not available in the registry cache")
                    })?;
                    super::install_extension_internal(
                        state,
                        &InstallRequest {
                            download_url: Some(entry.download_url),
                            package_path: None,
                        },
                    )
                    .await?;
                }

                let config = ReconcileConfig::from_settings(&state.settings);
                state.orchestrator.reconcile_once(&config).await?;
                Ok(format!("{title} is now managed by Elixir."))
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

struct DownloaderControlAdapter;

impl DownloaderControlAdapter {
    async fn build_queue_section(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Option<ExtensionControlSection>> {
        let Some(provider) = context.selected_provider.as_ref() else {
            return Ok(None);
        };
        let implementation = provider
            .implementation
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();

        let section = match implementation.as_str() {
            "qbittorrent" => match build_qbittorrent_queue_section(state, store, context).await {
                Ok(section) => section,
                Err(err) => {
                    tracing::warn!("qbittorrent control queue unavailable: {err}");
                    None
                }
            },
            "nzbget" => match build_nzbget_queue_section(state, store, context).await {
                Ok(section) => section,
                Err(err) => {
                    log_nzbget_control_availability("nzbget control queue unavailable", &err);
                    None
                }
            },
            _ => None,
        };

        Ok(section)
    }
}

#[async_trait::async_trait]
impl ExtensionControlProvider for DownloaderControlAdapter {
    async fn load_live_snapshot(
        &self,
        state: &AppState,
        _store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<ExtensionControlLiveSnapshot> {
        let Some(provider) = context.selected_provider.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(ExtensionControlLiveSnapshot::default());
        };

        let snapshot = state
            .orchestrator
            .read_provider_state(provider, instance)
            .await?;
        Ok(ExtensionControlLiveSnapshot {
            version: None,
            metrics: build_downloader_live_metrics(snapshot.activity.as_ref()),
        })
    }

    async fn build_sections(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let mut sections = Vec::new();
        if let Some(section) =
            super::build_extension_control_settings_section(state, store, context).await?
        {
            sections.push(section);
        }
        if downloader_implementation(context) == "nzbget" {
            match build_nzbget_servers_section(state, store, context).await {
                Ok(Some(section)) => sections.push(section),
                Ok(None) => {}
                Err(err) => {
                    log_nzbget_control_availability("nzbget control servers unavailable", &err);
                }
            }
        }
        if let Some(section) = self.build_queue_section(state, store, context).await? {
            sections.push(section);
        }
        Ok(sections)
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        if context.selected_provider.is_some() {
            vec![build_test_connection_action()]
        } else {
            Vec::new()
        }
    }

    async fn update_settings(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        _context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let Some(profile) = values
            .get("downloaderProfile")
            .and_then(serde_json::Value::as_str)
            .map(|value| value.trim().to_ascii_lowercase())
        else {
            anyhow::bail!("downloaderProfile is required for downloader defaults");
        };

        match profile.as_str() {
            "balanced" => {
                if state.settings.extensions.downloader_profile
                    == DownloaderPerformanceProfile::Balanced
                {
                    store
                        .delete_extension_setting(super::DOWNLOADER_PROFILE_SETTING_KEY)
                        .await?;
                } else {
                    store
                        .upsert_extension_setting(
                            super::DOWNLOADER_PROFILE_SETTING_KEY,
                            &serde_json::Value::String(profile),
                        )
                        .await?;
                }
            }
            "aggressive" => {
                if state.settings.extensions.downloader_profile
                    == DownloaderPerformanceProfile::Aggressive
                {
                    store
                        .delete_extension_setting(super::DOWNLOADER_PROFILE_SETTING_KEY)
                        .await?;
                } else {
                    store
                        .upsert_extension_setting(
                            super::DOWNLOADER_PROFILE_SETTING_KEY,
                            &serde_json::Value::String(profile),
                        )
                        .await?;
                }
            }
            _ => anyhow::bail!("downloaderProfile must be balanced or aggressive"),
        }

        state
            .orchestrator
            .apply_builtin_downloader_profiles_now()
            .await?;

        Ok(())
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        let implementation = downloader_implementation(context);
        match (implementation.as_str(), action_id) {
            ("qbittorrent", "test_connection") | ("nzbget", "test_connection") => {
                let snapshot = self.load_live_snapshot(state, store, context).await?;
                Ok(test_connection_message(
                    implementation.as_str(),
                    context,
                    &snapshot,
                ))
            }
            ("qbittorrent", "repair_managed_invariants")
            | ("nzbget", "repair_managed_invariants") => {
                super::run_extension_control_managed_repair(state).await
            }
            ("qbittorrent", "pause_all") => {
                qbittorrent_run_global_action(state, store, context, "pause_all").await
            }
            ("qbittorrent", "resume_all") => {
                qbittorrent_run_global_action(state, store, context, "resume_all").await
            }
            ("qbittorrent", "pause_item")
            | ("qbittorrent", "resume_item")
            | ("qbittorrent", "recheck_item")
            | ("qbittorrent", "remove_item") => {
                let item_id = control_action_item_id(params)?;
                qbittorrent_run_item_action(state, store, context, action_id, &item_id).await
            }
            ("nzbget", "pause_all") => {
                nzbget_run_global_action(state, store, context, "pause_all").await
            }
            ("nzbget", "resume_all") => {
                nzbget_run_global_action(state, store, context, "resume_all").await
            }
            ("nzbget", "add_server") => nzbget_add_server(state, store, context, params).await,
            ("nzbget", "edit_server") => nzbget_edit_server(state, store, context, params).await,
            ("nzbget", "test_server") => {
                nzbget_test_server_action(state, store, context, params).await
            }
            ("nzbget", "remove_server") => {
                nzbget_remove_server(state, store, context, params).await
            }
            ("nzbget", "pause_item") | ("nzbget", "resume_item") | ("nzbget", "remove_item") => {
                let item_id = control_action_item_id(params)?;
                nzbget_run_item_action(state, store, context, action_id, &item_id).await
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

struct CloudStreamControlAdapter;

#[async_trait::async_trait]
impl ExtensionControlProvider for CloudStreamControlAdapter {
    async fn build_sections(
        &self,
        _state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(vec![ExtensionControlSection {
                id: "cloudstreamSetup".to_string(),
                title: "CloudStream Compat".to_string(),
                description:
                    "Create the default instance to activate the recommended CloudStream source pack."
                        .to_string(),
                policy: Some(control_policy_seeded(
                    "Elixir owns the CloudStream Compat source registry and routes all candidates through Extension Suite.",
                )),
                notices: vec![control_notice(
                    "info",
                    "cloudstream_instance_missing",
                    "Default instance required",
                    "CloudStream Compat needs one enabled instance before sources can be managed.",
                )],
                fields: Vec::new(),
                entities: Vec::new(),
                actions: Vec::new(),
            }]);
        };

        let registries = store
            .list_source_registries(Some(instance.instance_id))
            .await?;
        let modules = store
            .list_source_modules(Some(instance.instance_id), None)
            .await?;
        let registry_by_id = registries
            .iter()
            .map(|registry| (registry.registry_id, registry))
            .collect::<BTreeMap<_, _>>();

        let mut sections = Vec::new();
        sections.push(build_cloudstream_recommended_section(
            context,
            instance,
            &registries,
            &modules,
        ));
        if let Some(section) =
            build_cloudstream_problems_section(store, instance, &modules, &registry_by_id).await?
        {
            sections.push(section);
        }
        sections.push(build_cloudstream_policy_section(instance));
        sections.push(build_cloudstream_installed_sources_section(
            &modules,
            &registry_by_id,
        ));
        sections.push(build_cloudstream_available_sources_section(
            &modules,
            &registry_by_id,
        ));
        sections.push(build_cloudstream_custom_repositories_section(&registries));
        sections.push(build_cloudstream_version_pins_section(store, &modules).await?);
        if let Some(section) = build_cloudstream_diagnostics_section(store, &modules).await? {
            sections.push(section);
        }
        Ok(sections)
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        if context.selected_instance.is_none() {
            return Vec::new();
        }
        vec![
            cloudstream_refresh_recommended_pack_action(),
            cloudstream_add_custom_repo_action(),
        ]
    }

    async fn update_settings(
        &self,
        _state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let instance = cloudstream_selected_instance(context)?;
        let allowed = [
            "curatedExecutableUpdates",
            "curatedBrokenModuleReplacement",
            "customRepoExecutableAutoUpdate",
        ];
        for key in values.keys() {
            if !allowed.iter().any(|allowed| allowed == key) {
                anyhow::bail!("unsupported CloudStream source policy setting '{key}'");
            }
        }
        let mut config = instance
            .config_json
            .clone()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        let mut policy = config
            .get("sourcePackPolicy")
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        for key in allowed {
            if let Some(value) = values.get(key).and_then(serde_json::Value::as_bool) {
                policy.insert(key.to_string(), serde_json::Value::Bool(value));
            }
        }
        config.insert(
            "sourcePackPolicy".to_string(),
            serde_json::Value::Object(policy),
        );
        store
            .update_instance_config(
                instance.instance_id,
                Some(&serde_json::Value::Object(config)),
            )
            .await?;
        Ok(())
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        let instance = cloudstream_selected_instance(context)?;
        match action_id {
            "refresh_recommended_pack" => {
                let summary = seed_cloudstream_recommended_source_pack_for_instance(
                    store,
                    instance.instance_id,
                    None,
                )
                .await?;
                Ok(format!(
                    "Recommended CloudStream source pack refreshed: {} module(s), {} version(s), {} disabled.",
                    summary.modules, summary.versions, summary.disabled_modules
                ))
            }
            "add_custom_repo" => cloudstream_add_custom_repo(store, instance, params).await,
            "refresh_custom_repo" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                cloudstream_refresh_registry(store, instance, registry_id).await
            }
            "trust_custom_repo" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    cloudstream_find_registry(store, instance.instance_id, registry_id).await?;
                if registry.registry_type == "elixir_curated_cloudstream_pack" {
                    anyhow::bail!("curated source packs are already trusted by package policy");
                }
                store
                    .set_source_registry_trust(registry_id, "maintainer_known", true)
                    .await?;
                Ok(format!(
                    "Trusted '{}'. Modules remain disabled until explicitly enabled.",
                    registry.display_name
                ))
            }
            "enable_registry" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    cloudstream_find_registry(store, instance.instance_id, registry_id).await?;
                store
                    .set_source_registry_enabled_state(registry_id, true, true)
                    .await?;
                if registry.trust_class != "custom" || registry.trusted_for_executable_updates {
                    store
                        .set_source_modules_enabled_for_registry(
                            registry_id,
                            true,
                            "available",
                            None,
                        )
                        .await?;
                }
                Ok(format!(
                    "Enabled source registry '{}'.",
                    registry.display_name
                ))
            }
            "disable_registry" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    cloudstream_find_registry(store, instance.instance_id, registry_id).await?;
                store
                    .set_source_registry_enabled_state(registry_id, false, false)
                    .await?;
                store
                    .set_source_modules_enabled_for_registry(
                        registry_id,
                        false,
                        "disabled",
                        Some("source registry disabled"),
                    )
                    .await?;
                Ok(format!(
                    "Disabled source registry '{}'.",
                    registry.display_name
                ))
            }
            "enable_source_module" | "install_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    cloudstream_find_module(store, instance.instance_id, source_module_id).await?;
                cloudstream_validate_module_activation(store, instance.instance_id, &module)
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                if action_id == "install_source_module" {
                    if let Some(version) =
                        cloudstream_preferred_module_version(&module, &versions, None)
                    {
                        if let Some(version_record) = versions
                            .iter()
                            .find(|candidate| candidate.version == version)
                        {
                            if version_record.artifact_url.is_some() {
                                install_source_module_artifact(
                                    store,
                                    &state.settings.extensions.storage_root,
                                    &module,
                                    version_record,
                                )
                                .await?;
                            }
                        }
                        store
                            .set_source_module_active_version(
                                source_module_id,
                                Some(version.as_str()),
                                module.active_version.as_deref(),
                            )
                            .await?;
                        cloudstream_mark_active_version(store, &versions, &version).await?;
                    }
                }
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Enabled CloudStream source '{}'.",
                    module.display_name
                ))
            }
            "disable_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    cloudstream_find_module(store, instance.instance_id, source_module_id).await?;
                store
                    .set_source_module_enabled_state(
                        source_module_id,
                        false,
                        "disabled",
                        Some("source module disabled by user"),
                    )
                    .await?;
                Ok(format!(
                    "Disabled CloudStream source '{}'.",
                    module.display_name
                ))
            }
            "pin_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let requested_version = cloudstream_param_string(params, "version")?;
                let module =
                    cloudstream_find_module(store, instance.instance_id, source_module_id).await?;
                cloudstream_validate_module_activation(store, instance.instance_id, &module)
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                let version = cloudstream_preferred_module_version(
                    &module,
                    &versions,
                    Some(requested_version.as_str()),
                )
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "version '{}' is not available for '{}'",
                        requested_version,
                        module.display_name
                    )
                })?;
                if let Some(version_record) = versions
                    .iter()
                    .find(|candidate| candidate.version == version)
                {
                    if version_record.artifact_url.is_some() {
                        install_source_module_artifact(
                            store,
                            &state.settings.extensions.storage_root,
                            &module,
                            version_record,
                        )
                        .await?;
                    }
                }
                store
                    .set_source_module_pinned_version(source_module_id, Some(&version))
                    .await?;
                store
                    .set_source_module_active_version(
                        source_module_id,
                        Some(&version),
                        module.active_version.as_deref(),
                    )
                    .await?;
                cloudstream_mark_active_version(store, &versions, &version).await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Pinned '{}' to version {}.",
                    module.display_name, version
                ))
            }
            "clear_source_module_pin" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    cloudstream_find_module(store, instance.instance_id, source_module_id).await?;
                store
                    .set_source_module_pinned_version(source_module_id, None)
                    .await?;
                Ok(format!(
                    "Cleared version pin for '{}'.",
                    module.display_name
                ))
            }
            "rollback_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    cloudstream_find_module(store, instance.instance_id, source_module_id).await?;
                let rollback_version = module
                    .rollback_version
                    .as_deref()
                    .map(str::to_string)
                    .ok_or_else(|| {
                        anyhow::anyhow!("'{}' has no rollback version", module.display_name)
                    })?;
                store
                    .set_source_module_pinned_version(source_module_id, None)
                    .await?;
                store
                    .set_source_module_active_version(
                        source_module_id,
                        Some(&rollback_version),
                        module.active_version.as_deref(),
                    )
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                cloudstream_mark_active_version(store, &versions, &rollback_version).await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Rolled '{}' back to version {}.",
                    module.display_name, rollback_version
                ))
            }
            "apply_source_replacement" => {
                let recommendation_key = cloudstream_param_string(params, "recommendationKey")?;
                let applied = apply_cloudstream_source_replacement_recommendation(
                    store,
                    instance.instance_id,
                    &recommendation_key,
                )
                .await?;
                if applied {
                    Ok("Applied CloudStream source replacement recommendation.".to_string())
                } else {
                    Ok("Replacement recommendation was no longer active.".to_string())
                }
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

struct PrismControlAdapter;

#[async_trait::async_trait]
impl ExtensionControlProvider for PrismControlAdapter {
    async fn build_sections(
        &self,
        _state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
    ) -> anyhow::Result<Vec<ExtensionControlSection>> {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(vec![ExtensionControlSection {
                id: "nuvioSetup".to_string(),
                title: "Prism".to_string(),
                description: "Create the default instance to activate Prism source modules."
                    .to_string(),
                policy: Some(control_policy_seeded(
                    "Elixir owns the Prism source registry and routes all candidates through Extension Suite.",
                )),
                notices: vec![control_notice(
                    "info",
                    "nuvio_instance_missing",
                    "Default instance required",
                    "Prism needs one enabled instance before sources can be managed.",
                )],
                fields: Vec::new(),
                entities: Vec::new(),
                actions: Vec::new(),
            }]);
        };

        let registries = store
            .list_source_registries(Some(instance.instance_id))
            .await?;
        let modules = store
            .list_source_modules(Some(instance.instance_id), None)
            .await?
            .into_iter()
            .filter(|module| module.ecosystem == "nuvio" || module.ecosystem == "stremio")
            .collect::<Vec<_>>();
        let registry_by_id = registries
            .iter()
            .map(|registry| (registry.registry_id, registry))
            .collect::<BTreeMap<_, _>>();

        let mut sections = vec![build_prism_recommended_section(
            context,
            instance,
            &registries,
            &modules,
        )];
        if let Some(section) =
            build_prism_problems_section(store, instance, &modules, &registry_by_id).await?
        {
            sections.push(section);
        }
        sections.push(build_prism_policy_section(instance));
        sections.push(build_nuvio_installed_sources_section(
            &modules,
            &registry_by_id,
        ));
        sections.push(build_nuvio_available_sources_section(
            &modules,
            &registry_by_id,
        ));
        sections.push(build_nuvio_repositories_section(&registries));
        sections.push(build_nuvio_version_pins_section(store, &modules).await?);
        if let Some(section) = build_cloudstream_diagnostics_section(store, &modules).await? {
            sections.push(section);
        }
        Ok(sections)
    }

    fn build_actions(&self, context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
        if context.selected_instance.is_none() {
            return Vec::new();
        }
        vec![
            prism_refresh_recommended_pack_action(),
            nuvio_add_custom_repo_action(),
        ]
    }

    async fn update_settings(
        &self,
        _state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        values: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        let instance = nuvio_selected_instance(context)?;
        let allowed = [
            "recommendedPackAutoEnable",
            "recommendedPackExecutableUpdates",
            "customRepoMetadataRefresh",
            "customRepoExecutableTrustRequired",
            "rollbackPinAlwaysAvailable",
        ];
        for key in values.keys() {
            if !allowed.iter().any(|allowed| allowed == key) {
                anyhow::bail!("unsupported Prism source policy setting '{key}'");
            }
        }
        if values
            .get("customRepoExecutableTrustRequired")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|value| !value)
        {
            anyhow::bail!("custom Prism repository executable trust cannot be disabled");
        }
        if values
            .get("rollbackPinAlwaysAvailable")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|value| !value)
        {
            anyhow::bail!("Prism rollback and pin controls cannot be disabled");
        }
        let mut config = instance
            .config_json
            .clone()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        let mut policy = config
            .get("sourcePackPolicy")
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        for key in allowed {
            if let Some(value) = values.get(key).and_then(serde_json::Value::as_bool) {
                policy.insert(key.to_string(), serde_json::Value::Bool(value));
            }
        }
        config.insert(
            "sourcePackPolicy".to_string(),
            serde_json::Value::Object(policy),
        );
        store
            .update_instance_config(
                instance.instance_id,
                Some(&serde_json::Value::Object(config)),
            )
            .await?;
        Ok(())
    }

    async fn execute_action(
        &self,
        state: &AppState,
        store: &ExtensionStore<'_>,
        context: &ExtensionControlContext,
        action_id: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<String> {
        let instance = nuvio_selected_instance(context)?;
        match action_id {
            "refresh_recommended_pack" => {
                let summary = seed_prism_recommended_source_pack_for_instance(
                    store,
                    instance.instance_id,
                    None,
                    Some(&state.settings.extensions.storage_root),
                )
                .await?;
                Ok(format!(
                    "Recommended Prism source pack refreshed: {} module(s), {} version(s), {} disabled.",
                    summary.modules, summary.versions, summary.disabled_modules
                ))
            }
            "add_custom_repo" => nuvio_add_custom_repo(store, instance, params).await,
            "refresh_custom_repo" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                nuvio_refresh_registry(store, instance, registry_id).await
            }
            "trust_custom_repo" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                if registry.registry_type == "elixir_curated_nuvio_pack" {
                    anyhow::bail!(
                        "curated Prism source packs are already trusted by package policy"
                    );
                }
                store
                    .set_source_registry_trust(registry_id, "maintainer_known", true)
                    .await?;
                Ok(format!(
                    "Trusted '{}'. Modules remain disabled until explicitly installed.",
                    registry.display_name
                ))
            }
            "enable_registry" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                store
                    .set_source_registry_enabled_state(registry_id, true, true)
                    .await?;
                if registry.registry_type == "elixir_curated_nuvio_pack"
                    || registry.trusted_for_executable_updates
                {
                    store
                        .set_source_modules_enabled_for_registry(
                            registry_id,
                            true,
                            "available",
                            None,
                        )
                        .await?;
                }
                Ok(format!(
                    "Enabled source registry '{}'.",
                    registry.display_name
                ))
            }
            "disable_registry" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                store
                    .set_source_registry_enabled_state(registry_id, false, false)
                    .await?;
                store
                    .set_source_modules_enabled_for_registry(
                        registry_id,
                        false,
                        "disabled",
                        Some("source registry disabled"),
                    )
                    .await?;
                Ok(format!(
                    "Disabled source registry '{}'.",
                    registry.display_name
                ))
            }
            "install_source_module" | "enable_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                nuvio_validate_module_activation(store, instance.instance_id, &module).await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                let version = cloudstream_preferred_module_version(&module, &versions, None)
                    .ok_or_else(|| {
                        anyhow::anyhow!("'{}' has no available version", module.display_name)
                    })?;
                if action_id == "install_source_module" || !module.installed {
                    let version_record = versions
                        .iter()
                        .find(|candidate| candidate.version == version)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "version '{}' is not available for '{}'",
                                version,
                                module.display_name
                            )
                        })?;
                    install_source_module_artifact(
                        store,
                        &state.settings.extensions.storage_root,
                        &module,
                        version_record,
                    )
                    .await?;
                }
                store
                    .set_source_module_active_version(
                        source_module_id,
                        Some(&version),
                        module.active_version.as_deref(),
                    )
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                cloudstream_mark_active_version(store, &versions, &version).await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Enabled Prism source '{}' at version {}.",
                    module.display_name, version
                ))
            }
            "smoke_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                nuvio_validate_module_activation(store, instance.instance_id, &module).await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                let version = cloudstream_preferred_module_version(&module, &versions, None)
                    .ok_or_else(|| {
                        anyhow::anyhow!("'{}' has no available version", module.display_name)
                    })?;
                let version_record = versions
                    .iter()
                    .find(|candidate| candidate.version == version)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "version '{}' is not available for '{}'",
                            version,
                            module.display_name
                        )
                    })?;
                install_source_module_artifact(
                    store,
                    &state.settings.extensions.storage_root,
                    &module,
                    version_record,
                )
                .await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Prism source '{}' passed artifact health check at version {}.",
                    module.display_name, version
                ))
            }
            "disable_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                store
                    .set_source_module_enabled_state(
                        source_module_id,
                        false,
                        "disabled",
                        Some("source module disabled by user"),
                    )
                    .await?;
                Ok(format!("Disabled Prism source '{}'.", module.display_name))
            }
            "pin_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let requested_version = cloudstream_param_string(params, "version")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                nuvio_validate_module_activation(store, instance.instance_id, &module).await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                let version = cloudstream_preferred_module_version(
                    &module,
                    &versions,
                    Some(requested_version.as_str()),
                )
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "version '{}' is not available for '{}'",
                        requested_version,
                        module.display_name
                    )
                })?;
                let version_record = versions
                    .iter()
                    .find(|candidate| candidate.version == version)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "version '{}' is not available for '{}'",
                            version,
                            module.display_name
                        )
                    })?;
                install_source_module_artifact(
                    store,
                    &state.settings.extensions.storage_root,
                    &module,
                    version_record,
                )
                .await?;
                store
                    .set_source_module_pinned_version(source_module_id, Some(&version))
                    .await?;
                store
                    .set_source_module_active_version(
                        source_module_id,
                        Some(&version),
                        module.active_version.as_deref(),
                    )
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                cloudstream_mark_active_version(store, &versions, &version).await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Pinned '{}' to version {}.",
                    module.display_name, version
                ))
            }
            "clear_source_module_pin" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                store
                    .set_source_module_pinned_version(source_module_id, None)
                    .await?;
                Ok(format!(
                    "Cleared version pin for '{}'.",
                    module.display_name
                ))
            }
            "rollback_source_module" => {
                let source_module_id = cloudstream_param_uuid(params, "sourceModuleId")?;
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                let rollback_version = module
                    .rollback_version
                    .as_deref()
                    .map(str::to_string)
                    .ok_or_else(|| {
                        anyhow::anyhow!("'{}' has no rollback version", module.display_name)
                    })?;
                store
                    .set_source_module_pinned_version(source_module_id, None)
                    .await?;
                store
                    .set_source_module_active_version(
                        source_module_id,
                        Some(&rollback_version),
                        module.active_version.as_deref(),
                    )
                    .await?;
                let versions = store.list_source_module_versions(source_module_id).await?;
                cloudstream_mark_active_version(store, &versions, &rollback_version).await?;
                store
                    .set_source_module_enabled_state(source_module_id, true, "available", None)
                    .await?;
                Ok(format!(
                    "Rolled '{}' back to version {}.",
                    module.display_name, rollback_version
                ))
            }
            "apply_source_replacement" => {
                let recommendation_key = cloudstream_param_string(params, "recommendationKey")?;
                let applied = apply_prism_source_replacement_recommendation(
                    store,
                    instance.instance_id,
                    &recommendation_key,
                )
                .await?;
                if applied {
                    Ok("Applied Prism source replacement recommendation.".to_string())
                } else {
                    Ok("Replacement recommendation was no longer active.".to_string())
                }
            }
            _ => anyhow::bail!("unsupported control action '{action_id}'"),
        }
    }
}

fn build_prism_recommended_section(
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    registries: &[ExtensionSourceRegistry],
    modules: &[ExtensionSourceModule],
) -> ExtensionControlSection {
    let recommended = registries
        .iter()
        .find(|registry| registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY);
    let enabled_modules = modules
        .iter()
        .filter(|module| module.enabled && module.health_state != "disabled")
        .count();
    let healthyish_modules = modules
        .iter()
        .filter(|module| matches!(module.health_state.as_str(), "available" | "healthy"))
        .count();
    let problem_modules = modules
        .iter()
        .filter(|module| {
            matches!(
                module.health_state.as_str(),
                "degraded" | "broken" | "unsupported" | "account_required"
            ) || module.last_error.is_some()
        })
        .count();

    let mut notices = Vec::new();
    if recommended.is_none() {
        notices.push(ExtensionControlNotice {
            action: Some(prism_refresh_recommended_pack_action()),
            ..control_notice(
                "warning",
                "prism_recommended_pack_missing",
                "Recommended pack missing",
                "Refresh the recommended Prism source pack before using Extension Suite stream acquisition.",
            )
        });
    }
    if let Some(registry) = recommended {
        if !registry.enabled {
            notices.push(ExtensionControlNotice {
                action: Some(cloudstream_registry_action(
                    "enable_registry",
                    "Enable recommended pack",
                    "Enable the recommended Prism source pack and its safe modules.",
                    "primary",
                    registry.registry_id,
                    None,
                )),
                ..control_notice(
                    "warning",
                    "prism_recommended_pack_disabled",
                    "Recommended pack disabled",
                    "The recommended Prism source pack is installed but disabled.",
                )
            });
        }
        if registry.last_fetch_status == "failed" {
            notices.push(ExtensionControlNotice {
                action: Some(prism_refresh_recommended_pack_action()),
                ..control_notice(
                    "warning",
                    "prism_recommended_pack_refresh_failed",
                    "Recommended pack refresh failed",
                    registry
                        .last_fetch_error
                        .as_deref()
                        .unwrap_or("Elixir could not refresh the recommended Prism source pack."),
                )
            });
        }
    }
    if problem_modules > 0 {
        notices.push(control_notice(
            "warning",
            "prism_source_problems",
            "Some sources need attention",
            format!(
                "{} Prism source module(s) are degraded, broken, unsupported, account-required, or reporting an error.",
                problem_modules
            ),
        ));
    }

    let mut entities = Vec::new();
    if let Some(registry) = recommended {
        entities.push(ExtensionControlEntity {
            id: registry.registry_id.to_string(),
            title: registry.display_name.clone(),
            subtitle: Some(if registry.enabled {
                "Ready".to_string()
            } else {
                "Disabled".to_string()
            }),
            details: vec![
                format!("Instance: {}", instance.instance_name),
                format!("Implementation: {}", context.summary.label),
                format!("Enabled modules: {}", enabled_modules),
                format!("Healthy or available modules: {}", healthyish_modules),
                format!("Problem modules: {}", problem_modules),
                format!(
                    "Last refresh: {}",
                    registry
                        .last_fetched_at
                        .map(|value| value.format("%Y-%m-%d %H:%M UTC").to_string())
                        .unwrap_or_else(|| "never".to_string())
                ),
            ],
            actions: vec![prism_refresh_recommended_pack_action()],
        });
    }

    ExtensionControlSection {
        id: "prismRecommended".to_string(),
        title: "Recommended".to_string(),
        description:
            "Recommended Prism sources are active through Extension Suite. Custom repositories and source pins are managed below."
                .to_string(),
        policy: Some(control_policy_seeded(
            "Elixir seeds the recommended pack and invokes Prism only as a stream candidate provider. Download routing and import verification stay inside Elixir.",
        )),
        notices,
        fields: Vec::new(),
        entities,
        actions: vec![prism_refresh_recommended_pack_action()],
    }
}

async fn build_prism_problems_section(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let mut entities = Vec::new();
    for module in modules.iter().filter(|module| {
        matches!(
            module.health_state.as_str(),
            "degraded" | "broken" | "unsupported" | "account_required"
        ) || module.last_error.is_some()
            || module.replacement_recommendation_key.is_some()
    }) {
        let mut actions =
            nuvio_module_actions(module, registry_by_id.get(&module.registry_id).copied());
        let recommendations = store
            .list_source_replacement_recommendations(Some(module.source_module_id), true)
            .await?;
        for recommendation in recommendations {
            actions.insert(
                0,
                cloudstream_apply_replacement_action(&recommendation.recommendation_key),
            );
        }
        entities.push(ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions,
        });
    }
    if entities.is_empty() {
        return Ok(None);
    }
    Ok(Some(ExtensionControlSection {
        id: "prismNeedsAttention".to_string(),
        title: "Needs attention".to_string(),
        description: format!(
            "{} source module(s) for '{}' need action.",
            entities.len(),
            instance.instance_name
        ),
        policy: Some(control_policy_observed(
            "Elixir reports source health and replacement recommendations. It only changes module state when you choose an action here.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }))
}

#[derive(Debug, Clone, Copy)]
struct PrismSourcePolicy {
    recommended_pack_auto_enable: bool,
    recommended_pack_executable_updates: bool,
    custom_repo_metadata_refresh: bool,
    custom_repo_executable_trust_required: bool,
    rollback_pin_always_available: bool,
}

fn prism_source_policy(instance: &ExtensionInstance) -> PrismSourcePolicy {
    let policy = instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("sourcePackPolicy"))
        .and_then(serde_json::Value::as_object);
    PrismSourcePolicy {
        recommended_pack_auto_enable: policy
            .and_then(|policy| policy.get("recommendedPackAutoEnable"))
            .and_then(serde_json::Value::as_bool)
            .or_else(|| {
                policy
                    .and_then(|policy| policy.get("curatedExecutableUpdates"))
                    .and_then(serde_json::Value::as_bool)
            })
            .unwrap_or(true),
        recommended_pack_executable_updates: policy
            .and_then(|policy| policy.get("recommendedPackExecutableUpdates"))
            .and_then(serde_json::Value::as_bool)
            .or_else(|| {
                policy
                    .and_then(|policy| policy.get("curatedBrokenModuleReplacement"))
                    .and_then(serde_json::Value::as_bool)
            })
            .unwrap_or(true),
        custom_repo_metadata_refresh: policy
            .and_then(|policy| policy.get("customRepoMetadataRefresh"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        custom_repo_executable_trust_required: policy
            .and_then(|policy| policy.get("customRepoExecutableTrustRequired"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        rollback_pin_always_available: policy
            .and_then(|policy| policy.get("rollbackPinAlwaysAvailable"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
    }
}

fn build_prism_policy_section(instance: &ExtensionInstance) -> ExtensionControlSection {
    let policy = prism_source_policy(instance);
    ExtensionControlSection {
        id: "prismSourcePolicy".to_string(),
        title: "Source policy".to_string(),
        description:
            "Recommended Prism sources can receive maintainer updates. Custom executable installs stay explicit unless trusted."
                .to_string(),
        policy: Some(control_policy_seeded(
            "These settings are sent to Prism with Extension Suite requests and never grant downloader credentials or library mutation rights.",
        )),
        notices: Vec::new(),
        fields: vec![
            prism_policy_field(
                "recommendedPackAutoEnable",
                "Recommended pack auto-enable",
                "Keep the maintainer-recommended Prism pack enabled by default.",
                policy.recommended_pack_auto_enable,
                false,
            ),
            prism_policy_field(
                "recommendedPackExecutableUpdates",
                "Recommended executable updates",
                "Allow executable updates only when the recommended artifact is hash-pinned or signed.",
                policy.recommended_pack_executable_updates,
                false,
            ),
            prism_policy_field(
                "customRepoMetadataRefresh",
                "Custom repo metadata refresh",
                "Allow user-added repositories to refresh metadata without installing executable code.",
                policy.custom_repo_metadata_refresh,
                false,
            ),
            prism_policy_field(
                "customRepoExecutableTrustRequired",
                "Custom executable trust required",
                "Custom repositories must be explicitly trusted before executable source installs or updates.",
                policy.custom_repo_executable_trust_required,
                true,
            ),
            prism_policy_field(
                "rollbackPinAlwaysAvailable",
                "Rollback and pin available",
                "Pin and rollback controls remain available for every installed source module.",
                policy.rollback_pin_always_available,
                true,
            ),
        ],
        entities: Vec::new(),
        actions: Vec::new(),
    }
}

fn prism_policy_field(
    id: &str,
    label: &str,
    description: &str,
    value: bool,
    readonly: bool,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "toggle".to_string(),
        value: serde_json::Value::Bool(value),
        required: false,
        readonly,
        secret: false,
        options: Vec::new(),
        validation: None,
    }
}

fn build_nuvio_installed_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> ExtensionControlSection {
    let entities = modules
        .iter()
        .filter(|module| module.enabled || module.installed)
        .map(|module| ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions: nuvio_module_actions(module, registry_by_id.get(&module.registry_id).copied()),
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "nuvioInstalledSources".to_string(),
        title: "Installed sources".to_string(),
        description: "Source modules currently enabled or installed for Prism.".to_string(),
        policy: Some(control_policy_seeded(
            "Elixir decides which module descriptors are sent to Prism. The source runtime cannot mutate this list.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn build_nuvio_available_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> ExtensionControlSection {
    let entities = modules
        .iter()
        .filter(|module| !module.enabled && !module.installed)
        .map(|module| ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions: nuvio_module_actions(module, registry_by_id.get(&module.registry_id).copied()),
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "nuvioAvailableSources".to_string(),
        title: "Available sources".to_string(),
        description:
            "Discovered Prism source modules that are not active. Repository trust and module install are explicit."
                .to_string(),
        policy: Some(control_policy_observed(
            "Elixir inventories Nuvio manifests but does not download or execute source code as a background side effect.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn build_nuvio_repositories_section(
    registries: &[ExtensionSourceRegistry],
) -> ExtensionControlSection {
    let entities = registries
        .iter()
        .filter(|registry| registry.registry_key != PRISM_RECOMMENDED_REGISTRY_KEY)
        .map(|registry| {
            let mut actions = vec![cloudstream_registry_action(
                "refresh_custom_repo",
                "Refresh",
                "Fetch the repository metadata again and update discovered source modules.",
                "secondary",
                registry.registry_id,
                None,
            )];
            if registry.enabled {
                actions.push(cloudstream_registry_action(
                    "disable_registry",
                    "Disable",
                    "Disable this repository and its source modules.",
                    "danger",
                    registry.registry_id,
                    Some("Disable this Nuvio repository and its modules?"),
                ));
            } else {
                actions.push(cloudstream_registry_action(
                    "enable_registry",
                    "Enable",
                    "Enable this repository. Modules still require their own explicit install.",
                    "primary",
                    registry.registry_id,
                    None,
                ));
            }
            if registry.trust_class == "custom" || !registry.trusted_for_executable_updates {
                actions.push(cloudstream_registry_action(
                    "trust_custom_repo",
                    "Trust repo",
                    "Mark this repository as maintainer-known for explicit source module installs.",
                    "secondary",
                    registry.registry_id,
                    Some("Trust this custom Prism repository for executable source installs? Only do this for maintainers you trust."),
                ));
            }
            ExtensionControlEntity {
                id: registry.registry_id.to_string(),
                title: registry.display_name.clone(),
                subtitle: Some(format!(
                    "{} • {}",
                    if registry.enabled { "Enabled" } else { "Disabled" },
                    registry.trust_class.replace('_', " ")
                )),
                details: vec![
                    format!("Registry key: {}", registry.registry_key),
                    format!("Type: {}", registry.registry_type),
                    format!(
                        "Executable install trust: {}",
                        if registry.trusted_for_executable_updates {
                            "trusted"
                        } else {
                            "blocked"
                        }
                    ),
                    format!("URL: {}", registry.url.as_deref().unwrap_or("none")),
                    format!("Last fetch: {}", registry.last_fetch_status),
                    registry
                        .last_fetch_error
                        .as_ref()
                        .map(|error| format!("Last error: {error}"))
                        .unwrap_or_else(|| "Last error: none".to_string()),
                ],
                actions,
            }
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "nuvioRepositories".to_string(),
        title: "Repositories".to_string(),
        description: "Prism source repositories. Adding a repository inventories metadata only."
            .to_string(),
        policy: Some(control_policy_observed(
            "Repository metadata is user-managed. Elixir validates and stores source descriptors but only installs executable code after explicit action.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![nuvio_add_custom_repo_action()],
    }
}

async fn build_nuvio_version_pins_section(
    store: &ExtensionStore<'_>,
    modules: &[ExtensionSourceModule],
) -> anyhow::Result<ExtensionControlSection> {
    build_cloudstream_version_pins_section(store, modules)
        .await
        .map(|mut section| {
            section.id = "nuvioVersionPins".to_string();
            section.title = "Version pins".to_string();
            section.description =
                "Pin or roll back Nuvio source module versions when a source update breaks."
                    .to_string();
            section
        })
}

fn nuvio_module_actions(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
) -> Vec<ExtensionControlAction> {
    let mut actions = Vec::new();
    let registry_allows_activation = registry
        .map(|registry| {
            registry.enabled
                && (registry.registry_type == "elixir_curated_nuvio_pack"
                    || registry.trusted_for_executable_updates)
        })
        .unwrap_or(false);
    if module.enabled {
        actions.push(cloudstream_source_module_action(
            "disable_source_module",
            "Disable",
            "Disable this Prism source module.",
            "danger",
            module.source_module_id,
            Some("Disable this Prism source module?"),
        ));
    } else if !module.unsupported && !module.account_required && registry_allows_activation {
        actions.push(cloudstream_source_module_action(
            if module.installed {
                "enable_source_module"
            } else {
                "install_source_module"
            },
            if module.installed {
                "Enable"
            } else {
                "Install"
            },
            "Install and enable this source module for Extension Suite searches.",
            "primary",
            module.source_module_id,
            None,
        ));
    }
    if module.installed || module.enabled {
        actions.push(cloudstream_source_module_action(
            "smoke_source_module",
            "Check health",
            "Fetch, hash-check, and statically validate this Prism source artifact.",
            "secondary",
            module.source_module_id,
            None,
        ));
    }
    if module.rollback_version.is_some() {
        actions.push(cloudstream_source_module_action(
            "rollback_source_module",
            "Rollback",
            "Return this source module to its previous active version.",
            "secondary",
            module.source_module_id,
            Some("Roll this Prism source module back to its previous active version?"),
        ));
    }
    actions.extend(cloudstream_version_actions(module, &[]));
    actions
}

fn nuvio_add_custom_repo_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "add_custom_repo".to_string(),
        label: "Add repository".to_string(),
        description: "Add a Prism-compatible manifest.json source repository.".to_string(),
        kind: "secondary".to_string(),
        params: Some(json!({
            "promptTitle": "Add Prism repository",
            "submitLabel": "Add repository",
            "promptFields": [
                {
                    "id": "registryUrl",
                    "label": "Repository URL",
                    "description": "Prism-compatible manifest.json URL.",
                    "fieldType": "text",
                    "required": true,
                    "value": ""
                },
                {
                    "id": "displayName",
                    "label": "Display name",
                    "description": "Optional local name for this repository.",
                    "fieldType": "text",
                    "required": false,
                    "value": ""
                },
                {
                    "id": "trustedForExecutableUpdates",
                    "label": "Trust source installs",
                    "description": "Only enable for source repositories maintained by someone you trust.",
                    "fieldType": "toggle",
                    "required": false,
                    "value": false
                }
            ]
        })),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

fn prism_refresh_recommended_pack_action() -> ExtensionControlAction {
    cloudstream_simple_action(
        "refresh_recommended_pack",
        "Refresh recommended",
        "Refresh the bundled recommended Prism source pack.",
        "primary",
        None,
        None,
    )
}

async fn nuvio_add_custom_repo(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let url = cloudstream_param_string(params, "registryUrl")?;
    let display_name = params
        .get("displayName")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let trusted = params
        .get("trustedForExecutableUpdates")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let registry_key = format!(
        "nuvio.custom.{}",
        cloudstream_stable_text_id(&format!("nuvio_manifest_json:{url}"))
    );
    let registry_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "elixir:nuvio:custom-registry:{}:{}",
            instance.instance_id, registry_key
        )
        .as_bytes(),
    );
    let client = NuvioRegistryClient::new(NuvioRegistryFetchConfig::default())?;
    let snapshot = client.fetch_registry("nuvio_manifest_json", &url).await?;
    let summary = persist_nuvio_registry_snapshot(
        store,
        &NuvioRegistryStoreInput {
            registry_id,
            instance_id: instance.instance_id,
            registry_key: registry_key.clone(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: if trusted {
                "maintainer_known".to_string()
            } else {
                "custom".to_string()
            },
            display_name,
            url: Some(url),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: trusted,
        },
        &snapshot,
    )
    .await?;
    Ok(format!(
        "Added Nuvio repository '{}': {} module(s), {} version(s), {} disabled.",
        registry_key, summary.modules, summary.versions, summary.disabled_modules
    ))
}

async fn nuvio_refresh_registry(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    registry_id: Uuid,
) -> anyhow::Result<String> {
    let registry = nuvio_find_registry(store, instance.instance_id, registry_id).await?;
    let url = registry
        .url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Nuvio registry has no URL"))?;
    let client = NuvioRegistryClient::new(NuvioRegistryFetchConfig::default())?;
    let snapshot = client
        .fetch_registry(&registry.registry_type, url)
        .await
        .with_context(|| format!("refreshing Nuvio registry '{}'", registry.display_name))?;
    let summary = persist_nuvio_registry_snapshot(
        store,
        &NuvioRegistryStoreInput {
            registry_id,
            instance_id: instance.instance_id,
            registry_key: registry.registry_key.clone(),
            registry_type: registry.registry_type.clone(),
            trust_class: registry.trust_class.clone(),
            display_name: Some(registry.display_name.clone()),
            url: registry.url.clone(),
            enabled: registry.enabled,
            auto_refresh: registry.auto_refresh,
            trusted_for_executable_updates: registry.trusted_for_executable_updates,
        },
        &snapshot,
    )
    .await?;
    Ok(format!(
        "Refreshed '{}': {} module(s), {} version(s), {} disabled.",
        registry.display_name, summary.modules, summary.versions, summary.disabled_modules
    ))
}

async fn nuvio_validate_module_activation(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    module: &ExtensionSourceModule,
) -> anyhow::Result<()> {
    if module.unsupported {
        anyhow::bail!(
            "'{}' is unsupported: {}",
            module.display_name,
            module
                .unsupported_reason
                .as_deref()
                .unwrap_or("unsupported by Prism")
        );
    }
    if module.account_required {
        anyhow::bail!(
            "'{}' requires an account before activation",
            module.display_name
        );
    }
    let registry = nuvio_find_registry(store, instance_id, module.registry_id).await?;
    if !registry.enabled {
        anyhow::bail!(
            "source registry '{}' is disabled; enable the registry first",
            registry.display_name
        );
    }
    if registry.registry_type != "elixir_curated_nuvio_pack"
        && !registry.trusted_for_executable_updates
    {
        anyhow::bail!(
            "source registry '{}' must be explicitly trusted before installing executable modules",
            registry.display_name
        );
    }
    Ok(())
}

async fn apply_prism_source_replacement_recommendation(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    recommendation_key: &str,
) -> anyhow::Result<bool> {
    let recommendation_key = recommendation_key.trim();
    if recommendation_key.is_empty() {
        anyhow::bail!("recommendation key must not be empty");
    }
    let modules = store.list_source_modules(Some(instance_id), None).await?;
    let module_ids = modules
        .iter()
        .filter(|module| module.ecosystem == "nuvio" || module.ecosystem == "stremio")
        .map(|module| module.source_module_id)
        .collect::<HashSet<_>>();
    let replacement_by_id = modules
        .iter()
        .map(|module| (module.source_module_id, module))
        .collect::<HashMap<_, _>>();
    let recommendations = store
        .list_source_replacement_recommendations(None, true)
        .await?;
    let Some(recommendation) = recommendations.into_iter().find(|recommendation| {
        recommendation.recommendation_key == recommendation_key
            && module_ids.contains(&recommendation.source_module_id)
    }) else {
        return Ok(false);
    };
    match recommendation.action.as_str() {
        "replace" => {
            let Some(replacement_source_module_id) = recommendation.replacement_source_module_id
            else {
                anyhow::bail!(
                    "replace recommendation '{recommendation_key}' has no replacement module"
                );
            };
            let Some(replacement) = replacement_by_id.get(&replacement_source_module_id) else {
                anyhow::bail!(
                    "replace recommendation '{recommendation_key}' references missing replacement module"
                );
            };
            if replacement.unsupported {
                anyhow::bail!(
                    "replace recommendation '{}' references unsupported replacement module '{}'",
                    recommendation_key,
                    replacement.display_name
                );
            }
            store
                .set_source_module_enabled_state(
                    recommendation.source_module_id,
                    false,
                    "disabled",
                    recommendation.reason.as_deref(),
                )
                .await?;
            store
                .set_source_module_enabled_state(
                    replacement_source_module_id,
                    true,
                    "available",
                    None,
                )
                .await?;
        }
        "disable" => {
            store
                .set_source_module_enabled_state(
                    recommendation.source_module_id,
                    false,
                    "disabled",
                    recommendation.reason.as_deref(),
                )
                .await?;
        }
        "pin" => {
            if let Some(version) = recommendation.recommended_version.as_deref() {
                store
                    .set_source_module_active_version(
                        recommendation.source_module_id,
                        Some(version),
                        None,
                    )
                    .await?;
            }
        }
        "none" => {}
        other => anyhow::bail!("unsupported source replacement action '{other}'"),
    }
    store
        .mark_source_replacement_recommendation_applied(recommendation.recommendation_id)
        .await?;
    Ok(true)
}

async fn nuvio_find_registry(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    registry_id: Uuid,
) -> anyhow::Result<ExtensionSourceRegistry> {
    store
        .list_source_registries(Some(instance_id))
        .await?
        .into_iter()
        .find(|registry| registry.registry_id == registry_id)
        .ok_or_else(|| anyhow::anyhow!("Nuvio registry '{registry_id}' was not found"))
}

async fn nuvio_find_module(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    source_module_id: Uuid,
) -> anyhow::Result<ExtensionSourceModule> {
    store
        .list_source_modules(Some(instance_id), None)
        .await?
        .into_iter()
        .find(|module| {
            module.source_module_id == source_module_id
                && (module.ecosystem == "nuvio" || module.ecosystem == "stremio")
        })
        .ok_or_else(|| anyhow::anyhow!("Nuvio source module '{source_module_id}' was not found"))
}

fn nuvio_selected_instance(
    context: &ExtensionControlContext,
) -> anyhow::Result<&ExtensionInstance> {
    context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active Prism instance is available yet"))
}

fn build_cloudstream_recommended_section(
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    registries: &[ExtensionSourceRegistry],
    modules: &[ExtensionSourceModule],
) -> ExtensionControlSection {
    let recommended = registries
        .iter()
        .find(|registry| registry.registry_key == CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY);
    let enabled_modules = modules
        .iter()
        .filter(|module| module.enabled && module.health_state != "disabled")
        .count();
    let healthyish_modules = modules
        .iter()
        .filter(|module| matches!(module.health_state.as_str(), "available" | "healthy"))
        .count();
    let problem_modules = modules
        .iter()
        .filter(|module| {
            matches!(
                module.health_state.as_str(),
                "degraded" | "broken" | "unsupported" | "account_required"
            ) || module.last_error.is_some()
        })
        .count();

    let mut notices = Vec::new();
    if recommended.is_none() {
        notices.push(ExtensionControlNotice {
            action: Some(cloudstream_refresh_recommended_pack_action()),
            ..control_notice(
                "warning",
                "cloudstream_recommended_pack_missing",
                "Recommended pack missing",
                "Refresh the recommended CloudStream source pack before using Extension Suite stream acquisition.",
            )
        });
    }
    if let Some(registry) = recommended {
        if !registry.enabled {
            notices.push(ExtensionControlNotice {
                action: Some(cloudstream_registry_action(
                    "enable_registry",
                    "Enable recommended pack",
                    "Enable the recommended source pack and its safe modules.",
                    "primary",
                    registry.registry_id,
                    None,
                )),
                ..control_notice(
                    "warning",
                    "cloudstream_recommended_pack_disabled",
                    "Recommended pack disabled",
                    "The recommended CloudStream pack is installed but disabled.",
                )
            });
        }
        if registry.last_fetch_status == "failed" {
            notices.push(ExtensionControlNotice {
                action: Some(cloudstream_refresh_recommended_pack_action()),
                ..control_notice(
                    "warning",
                    "cloudstream_recommended_pack_refresh_failed",
                    "Recommended pack refresh failed",
                    registry.last_fetch_error.as_deref().unwrap_or(
                        "Elixir could not refresh the recommended CloudStream source pack.",
                    ),
                )
            });
        }
    }
    if problem_modules > 0 {
        notices.push(control_notice(
            "warning",
            "cloudstream_source_problems",
            "Some sources need attention",
            format!(
                "{} CloudStream source module(s) are degraded, broken, unsupported, account-required, or reporting an error.",
                problem_modules
            ),
        ));
    }

    let mut entities = Vec::new();
    if let Some(registry) = recommended {
        entities.push(ExtensionControlEntity {
            id: registry.registry_id.to_string(),
            title: registry.display_name.clone(),
            subtitle: Some(if registry.enabled {
                "Ready".to_string()
            } else {
                "Disabled".to_string()
            }),
            details: vec![
                format!("Instance: {}", instance.instance_name),
                format!("Implementation: {}", context.summary.label),
                format!("Enabled modules: {}", enabled_modules),
                format!("Healthy or available modules: {}", healthyish_modules),
                format!("Problem modules: {}", problem_modules),
                format!(
                    "Last refresh: {}",
                    registry
                        .last_fetched_at
                        .map(|value| value.format("%Y-%m-%d %H:%M UTC").to_string())
                        .unwrap_or_else(|| "never".to_string())
                ),
            ],
            actions: vec![cloudstream_refresh_recommended_pack_action()],
        });
    }

    ExtensionControlSection {
        id: "cloudstreamRecommended".to_string(),
        title: "CloudStream Compat".to_string(),
        description:
            "Recommended CloudStream sources are active through Extension Suite. Custom repositories and source pins are managed below."
                .to_string(),
        policy: Some(control_policy_seeded(
            "Elixir seeds the recommended pack and invokes CloudStream Compat only as a source provider. Download routing stays inside Elixir.",
        )),
        notices,
        fields: Vec::new(),
        entities,
        actions: vec![cloudstream_refresh_recommended_pack_action()],
    }
}

async fn build_cloudstream_problems_section(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let mut entities = Vec::new();
    for module in modules.iter().filter(|module| {
        matches!(
            module.health_state.as_str(),
            "degraded" | "broken" | "unsupported" | "account_required"
        ) || module.last_error.is_some()
            || module.replacement_recommendation_key.is_some()
    }) {
        let mut actions =
            cloudstream_module_actions(module, registry_by_id.get(&module.registry_id).copied());
        let recommendations = store
            .list_source_replacement_recommendations(Some(module.source_module_id), true)
            .await?;
        for recommendation in recommendations {
            actions.insert(
                0,
                cloudstream_apply_replacement_action(&recommendation.recommendation_key),
            );
        }
        entities.push(ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions,
        });
    }
    if entities.is_empty() {
        return Ok(None);
    }
    Ok(Some(ExtensionControlSection {
        id: "cloudstreamProblems".to_string(),
        title: "Problems".to_string(),
        description: format!(
            "{} source module(s) for '{}' need action.",
            entities.len(),
            instance.instance_name
        ),
        policy: Some(control_policy_observed(
            "Elixir reports source health and replacement recommendations. It only changes module state when you choose an action here.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }))
}

fn build_cloudstream_policy_section(instance: &ExtensionInstance) -> ExtensionControlSection {
    let policy = cloudstream_source_policy(instance);
    ExtensionControlSection {
        id: "cloudstreamSourcePolicy".to_string(),
        title: "Source update policy".to_string(),
        description:
            "Recommended sources can receive maintainer updates. Custom repository executable updates stay off unless explicitly enabled."
                .to_string(),
        policy: Some(control_policy_seeded(
            "These settings are sent to CloudStream Compat with each Extension Suite request and do not grant downloader or library access.",
        )),
        notices: Vec::new(),
        fields: vec![
            cloudstream_policy_field(
                "curatedExecutableUpdates",
                "Recommended executable updates",
                "Allow maintainer-approved recommended source updates.",
                policy.curated_executable_updates,
            ),
            cloudstream_policy_field(
                "curatedBrokenModuleReplacement",
                "Recommended broken-source replacement",
                "Allow curated replacement recommendations to disable broken recommended sources and enable their replacement.",
                policy.curated_broken_module_replacement,
            ),
            cloudstream_policy_field(
                "customRepoExecutableAutoUpdate",
                "Custom repo auto updates",
                "Allow trusted custom repositories to apply executable update recommendations. Leave off unless you trust the repository maintainer.",
                policy.custom_repo_executable_auto_update,
            ),
        ],
        entities: Vec::new(),
        actions: Vec::new(),
    }
}

fn build_cloudstream_installed_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> ExtensionControlSection {
    let entities = modules
        .iter()
        .filter(|module| module.enabled || module.installed)
        .map(|module| ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions: cloudstream_module_actions(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "cloudstreamInstalledSources".to_string(),
        title: "Installed sources".to_string(),
        description: "Source modules currently enabled or installed for CloudStream Compat."
            .to_string(),
        policy: Some(control_policy_seeded(
            "Elixir decides which module descriptors are sent to CloudStream Compat. The source runtime cannot mutate this list.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn build_cloudstream_available_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> ExtensionControlSection {
    let entities = modules
        .iter()
        .filter(|module| !module.enabled && !module.installed)
        .map(|module| ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(cloudstream_module_subtitle(module)),
            details: cloudstream_module_details(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
            actions: cloudstream_module_actions(
                module,
                registry_by_id.get(&module.registry_id).copied(),
            ),
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "cloudstreamAvailableSources".to_string(),
        title: "Available sources".to_string(),
        description:
            "Discovered source modules that are not currently active. Custom repository modules require explicit trust before activation."
                .to_string(),
        policy: Some(control_policy_observed(
            "Elixir inventories these sources from explicit source packs or repositories. It does not download third-party code as a background side effect.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn build_cloudstream_custom_repositories_section(
    registries: &[ExtensionSourceRegistry],
) -> ExtensionControlSection {
    let entities = registries
        .iter()
        .filter(|registry| registry.registry_key != CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY)
        .map(|registry| {
            let mut actions = vec![cloudstream_registry_action(
                "refresh_custom_repo",
                "Refresh",
                "Fetch the repository metadata again and update discovered source modules.",
                "secondary",
                registry.registry_id,
                None,
            )];
            if registry.enabled {
                actions.push(cloudstream_registry_action(
                    "disable_registry",
                    "Disable",
                    "Disable this repository and its source modules.",
                    "danger",
                    registry.registry_id,
                    Some("Disable this CloudStream repository and its modules?"),
                ));
            } else {
                actions.push(cloudstream_registry_action(
                    "enable_registry",
                    "Enable",
                    "Enable this repository. Modules still require their own activation when the repository is custom and untrusted.",
                    "primary",
                    registry.registry_id,
                    None,
                ));
            }
            if registry.trust_class == "custom" || !registry.trusted_for_executable_updates {
                actions.push(cloudstream_registry_action(
                    "trust_custom_repo",
                    "Trust repo",
                    "Mark this repository as maintainer-known for explicit executable updates.",
                    "secondary",
                    registry.registry_id,
                    Some("Trust this custom CloudStream repository for executable updates? Only do this for maintainers you trust."),
                ));
            }
            ExtensionControlEntity {
                id: registry.registry_id.to_string(),
                title: registry.display_name.clone(),
                subtitle: Some(format!(
                    "{} • {}",
                    if registry.enabled { "Enabled" } else { "Disabled" },
                    registry.trust_class.replace('_', " ")
                )),
                details: vec![
                    format!("Registry key: {}", registry.registry_key),
                    format!("Type: {}", registry.registry_type),
                    format!(
                        "Executable update trust: {}",
                        if registry.trusted_for_executable_updates {
                            "trusted"
                        } else {
                            "blocked"
                        }
                    ),
                    format!(
                        "URL: {}",
                        registry.url.as_deref().unwrap_or("bundled source pack")
                    ),
                    format!("Last fetch: {}", registry.last_fetch_status),
                    registry
                        .last_fetch_error
                        .as_ref()
                        .map(|error| format!("Last error: {error}"))
                        .unwrap_or_else(|| "Last error: none".to_string()),
                ],
                actions,
            }
        })
        .collect::<Vec<_>>();
    ExtensionControlSection {
        id: "cloudstreamCustomRepositories".to_string(),
        title: "Custom repositories".to_string(),
        description:
            "Power-user source repositories. Adding a repository inventories metadata; trust and module activation remain explicit."
                .to_string(),
        policy: Some(control_policy_observed(
            "Custom repositories are user-managed. Elixir validates and stores their metadata but does not silently install executable plugin code.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![cloudstream_add_custom_repo_action()],
    }
}

async fn build_cloudstream_version_pins_section(
    store: &ExtensionStore<'_>,
    modules: &[ExtensionSourceModule],
) -> anyhow::Result<ExtensionControlSection> {
    let mut entities = Vec::new();
    for module in modules {
        let versions = store
            .list_source_module_versions(module.source_module_id)
            .await?;
        if versions.is_empty()
            && module.active_version.is_none()
            && module.rollback_version.is_none()
            && module.pinned_version.is_none()
        {
            continue;
        }
        entities.push(ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(format!(
                "Active: {}{}",
                module.active_version.as_deref().unwrap_or("none"),
                module
                    .pinned_version
                    .as_deref()
                    .map(|version| format!(" • pinned {version}"))
                    .unwrap_or_default()
            )),
            details: vec![
                format!(
                    "Available versions: {}",
                    versions
                        .iter()
                        .map(|version| version.version.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                format!(
                    "Rollback version: {}",
                    module.rollback_version.as_deref().unwrap_or("none")
                ),
            ],
            actions: cloudstream_version_actions(module, &versions),
        });
    }
    Ok(ExtensionControlSection {
        id: "cloudstreamVersionPins".to_string(),
        title: "Version pins".to_string(),
        description: "Pin or roll back source module versions when a source update breaks."
            .to_string(),
        policy: Some(control_policy_seeded(
            "Pins are Elixir-owned source policy. They affect provider invocation only; they do not mutate downloader or library state.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    })
}

async fn build_cloudstream_diagnostics_section(
    store: &ExtensionStore<'_>,
    modules: &[ExtensionSourceModule],
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let mut entities = Vec::new();
    for module in modules.iter().take(25) {
        let events = store
            .list_source_health_events(module.source_module_id, 3)
            .await?;
        if events.is_empty() && module.last_error.is_none() {
            continue;
        }
        let mut details = Vec::new();
        if let Some(error) = module.last_error.as_deref() {
            details.push(format!("Last error: {error}"));
        }
        for event in events {
            details.push(format!(
                "{} {}: {}",
                event.observed_at.format("%Y-%m-%d %H:%M UTC"),
                event.state,
                event.reason.as_deref().unwrap_or(event.event_type.as_str())
            ));
        }
        entities.push(ExtensionControlEntity {
            id: module.source_module_id.to_string(),
            title: module.display_name.clone(),
            subtitle: Some(format!("Health: {}", module.health_state)),
            details,
            actions: Vec::new(),
        });
    }
    if entities.is_empty() {
        return Ok(None);
    }
    Ok(Some(ExtensionControlSection {
        id: "cloudstreamDiagnostics".to_string(),
        title: "Diagnostics".to_string(),
        description: "Recent source health events and runtime errors.".to_string(),
        policy: Some(control_policy_observed(
            "Diagnostics are observed state from provider searches and materialization. They are not shown unless there is something useful to inspect.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }))
}

#[derive(Debug, Clone, Copy)]
struct CloudStreamSourcePolicy {
    curated_executable_updates: bool,
    curated_broken_module_replacement: bool,
    custom_repo_executable_auto_update: bool,
}

fn cloudstream_source_policy(instance: &ExtensionInstance) -> CloudStreamSourcePolicy {
    let policy = instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("sourcePackPolicy"))
        .and_then(serde_json::Value::as_object);
    CloudStreamSourcePolicy {
        curated_executable_updates: policy
            .and_then(|policy| policy.get("curatedExecutableUpdates"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        curated_broken_module_replacement: policy
            .and_then(|policy| policy.get("curatedBrokenModuleReplacement"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        custom_repo_executable_auto_update: policy
            .and_then(|policy| policy.get("customRepoExecutableAutoUpdate"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    }
}

fn cloudstream_policy_field(
    id: &str,
    label: &str,
    description: &str,
    value: bool,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "toggle".to_string(),
        value: serde_json::Value::Bool(value),
        required: false,
        readonly: false,
        secret: false,
        options: Vec::new(),
        validation: None,
    }
}

fn cloudstream_module_subtitle(module: &ExtensionSourceModule) -> String {
    format!(
        "{} • {}{}{}",
        if module.enabled {
            "Enabled"
        } else {
            "Disabled"
        },
        module.health_state.replace('_', " "),
        module
            .active_version
            .as_deref()
            .map(|version| format!(" • v{version}"))
            .unwrap_or_default(),
        module
            .pinned_version
            .as_deref()
            .map(|version| format!(" • pinned {version}"))
            .unwrap_or_default()
    )
}

fn cloudstream_module_details(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
) -> Vec<String> {
    let mut details = Vec::new();
    if let Some(registry) = registry {
        details.push(format!("Registry: {}", registry.display_name));
        details.push(format!("Trust: {}", registry.trust_class.replace('_', " ")));
    }
    if let Some(package) = module.plugin_package.as_deref() {
        details.push(format!("Plugin package: {package}"));
    }
    if let Some(media_types) = module.media_types_json.as_ref() {
        details.push(format!(
            "Media types: {}",
            cloudstream_json_list(media_types)
        ));
    }
    if let Some(domains) = module.source_domains_json.as_ref() {
        details.push(format!("Domains: {}", cloudstream_json_list(domains)));
    }
    if module.account_required {
        details.push("Account required.".to_string());
    }
    if module.unsupported {
        details.push(format!(
            "Unsupported: {}",
            module
                .unsupported_reason
                .as_deref()
                .unwrap_or("unsupported by CloudStream Compat")
        ));
    }
    if let Some(error) = module.last_error.as_deref() {
        details.push(format!("Last error: {error}"));
    }
    details
}

fn cloudstream_json_list(value: &serde_json::Value) -> String {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "none".to_string())
}

fn cloudstream_module_actions(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
) -> Vec<ExtensionControlAction> {
    let mut actions = Vec::new();
    let registry_allows_activation = registry
        .map(|registry| {
            registry.enabled
                && (registry.trust_class != "custom" || registry.trusted_for_executable_updates)
        })
        .unwrap_or(false);
    if module.enabled {
        actions.push(cloudstream_source_module_action(
            "disable_source_module",
            "Disable",
            "Disable this CloudStream source module.",
            "danger",
            module.source_module_id,
            Some("Disable this CloudStream source module?"),
        ));
    } else if !module.unsupported && !module.account_required && registry_allows_activation {
        actions.push(cloudstream_source_module_action(
            if module.installed {
                "enable_source_module"
            } else {
                "install_source_module"
            },
            if module.installed {
                "Enable"
            } else {
                "Install"
            },
            "Enable this source module for Extension Suite searches.",
            "primary",
            module.source_module_id,
            None,
        ));
    }
    if module.rollback_version.is_some() {
        actions.push(cloudstream_source_module_action(
            "rollback_source_module",
            "Rollback",
            "Return this source module to its previous active version.",
            "secondary",
            module.source_module_id,
            Some("Roll this CloudStream source module back to its previous active version?"),
        ));
    }
    actions.extend(cloudstream_version_actions(module, &[]));
    actions
}

fn cloudstream_version_actions(
    module: &ExtensionSourceModule,
    versions: &[ExtensionSourceModuleVersion],
) -> Vec<ExtensionControlAction> {
    let mut actions = Vec::new();
    if !versions.is_empty() {
        actions.push(ExtensionControlAction {
            id: "pin_source_module".to_string(),
            label: "Pin version".to_string(),
            description: "Pin this source module to a known-good version.".to_string(),
            kind: "secondary".to_string(),
            params: Some(json!({
                "sourceModuleId": module.source_module_id.to_string(),
                "promptTitle": format!("Pin {}", module.display_name),
                "submitLabel": "Pin version",
                "promptFields": [{
                    "id": "version",
                    "label": "Version",
                    "description": "Choose the exact source module version to keep active.",
                    "fieldType": "select",
                    "required": true,
                    "value": module
                        .pinned_version
                        .as_deref()
                        .or(module.active_version.as_deref())
                        .unwrap_or_else(|| versions.last().map(|version| version.version.as_str()).unwrap_or("")),
                    "options": versions
                        .iter()
                        .map(|version| json!({
                            "value": version.version,
                            "label": version.version,
                        }))
                        .collect::<Vec<_>>()
                }]
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
    if module.pinned_version.is_some() {
        actions.push(cloudstream_source_module_action(
            "clear_source_module_pin",
            "Clear pin",
            "Allow this source module to follow normal update policy again.",
            "secondary",
            module.source_module_id,
            None,
        ));
    }
    actions
}

fn cloudstream_refresh_recommended_pack_action() -> ExtensionControlAction {
    cloudstream_simple_action(
        "refresh_recommended_pack",
        "Refresh recommended",
        "Refresh the bundled recommended CloudStream source pack.",
        "primary",
        None,
        None,
    )
}

fn cloudstream_add_custom_repo_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "add_custom_repo".to_string(),
        label: "Add repository".to_string(),
        description: "Add a CloudStream repo.json or plugins.json source repository.".to_string(),
        kind: "secondary".to_string(),
        params: Some(json!({
            "promptTitle": "Add CloudStream repository",
            "submitLabel": "Add repository",
            "promptFields": [
                {
                    "id": "registryUrl",
                    "label": "Repository URL",
                    "description": "CloudStream repo.json or plugins.json URL.",
                    "fieldType": "text",
                    "required": true,
                    "value": ""
                },
                {
                    "id": "registryType",
                    "label": "Registry type",
                    "description": "Use repo.json when the URL points at a CloudStream repository descriptor. Use plugins.json for a direct plugin list.",
                    "fieldType": "select",
                    "required": true,
                    "value": "cloudstream_repo_json",
                    "options": [
                        { "value": "cloudstream_repo_json", "label": "CloudStream repo.json" },
                        { "value": "cloudstream_plugins_json", "label": "CloudStream plugins.json" }
                    ]
                },
                {
                    "id": "displayName",
                    "label": "Display name",
                    "description": "Optional local name for this repository.",
                    "fieldType": "text",
                    "required": false,
                    "value": ""
                },
                {
                    "id": "trustedForExecutableUpdates",
                    "label": "Trust executable updates",
                    "description": "Only enable for repositories maintained by someone you trust.",
                    "fieldType": "toggle",
                    "required": false,
                    "value": false
                }
            ]
        })),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

fn cloudstream_registry_action(
    id: &str,
    label: &str,
    description: &str,
    kind: &str,
    registry_id: Uuid,
    confirm_text: Option<&str>,
) -> ExtensionControlAction {
    cloudstream_simple_action(
        id,
        label,
        description,
        kind,
        Some(json!({ "registryId": registry_id.to_string() })),
        confirm_text,
    )
}

fn cloudstream_source_module_action(
    id: &str,
    label: &str,
    description: &str,
    kind: &str,
    source_module_id: Uuid,
    confirm_text: Option<&str>,
) -> ExtensionControlAction {
    cloudstream_simple_action(
        id,
        label,
        description,
        kind,
        Some(json!({ "sourceModuleId": source_module_id.to_string() })),
        confirm_text,
    )
}

fn cloudstream_apply_replacement_action(recommendation_key: &str) -> ExtensionControlAction {
    cloudstream_simple_action(
        "apply_source_replacement",
        "Apply replacement",
        "Apply the active replacement recommendation for this source module.",
        "primary",
        Some(json!({ "recommendationKey": recommendation_key })),
        None,
    )
}

fn cloudstream_simple_action(
    id: &str,
    label: &str,
    description: &str,
    kind: &str,
    params: Option<serde_json::Value>,
    confirm_text: Option<&str>,
) -> ExtensionControlAction {
    ExtensionControlAction {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        kind: kind.to_string(),
        params,
        confirm_text: confirm_text.map(str::to_string),
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

async fn cloudstream_add_custom_repo(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let url = cloudstream_param_string(params, "registryUrl")?;
    let registry_type = cloudstream_param_string(params, "registryType")
        .unwrap_or_else(|_| "cloudstream_repo_json".to_string());
    if !matches!(
        registry_type.as_str(),
        "cloudstream_repo_json" | "cloudstream_plugins_json"
    ) {
        anyhow::bail!("registryType must be cloudstream_repo_json or cloudstream_plugins_json");
    }
    let display_name = params
        .get("displayName")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let trusted = params
        .get("trustedForExecutableUpdates")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let registry_key = format!(
        "cloudstream.custom.{}",
        cloudstream_stable_text_id(&format!("{registry_type}:{url}"))
    );
    let registry_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "elixir:cloudstream:custom-registry:{}:{}",
            instance.instance_id, registry_key
        )
        .as_bytes(),
    );
    let client = CloudStreamRegistryClient::new(CloudStreamRegistryFetchConfig::default())?;
    let snapshot = client.fetch_registry(&registry_type, &url).await?;
    let summary = persist_cloudstream_registry_snapshot(
        store,
        &CloudStreamRegistryStoreInput {
            registry_id,
            instance_id: instance.instance_id,
            registry_key: registry_key.clone(),
            registry_type,
            trust_class: if trusted {
                "maintainer_known".to_string()
            } else {
                "custom".to_string()
            },
            display_name,
            url: Some(url),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: trusted,
        },
        &snapshot,
    )
    .await?;
    Ok(format!(
        "Added CloudStream repository '{}': {} module(s), {} version(s), {} disabled.",
        registry_key, summary.modules, summary.versions, summary.disabled_modules
    ))
}

async fn cloudstream_refresh_registry(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    registry_id: Uuid,
) -> anyhow::Result<String> {
    let registry = cloudstream_find_registry(store, instance.instance_id, registry_id).await?;
    if registry.registry_type == "elixir_curated_cloudstream_pack" {
        let summary = seed_cloudstream_recommended_source_pack_for_instance(
            store,
            instance.instance_id,
            None,
        )
        .await?;
        return Ok(format!(
            "Recommended CloudStream source pack refreshed: {} module(s), {} version(s).",
            summary.modules, summary.versions
        ));
    }
    let url = registry
        .url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("custom CloudStream registry has no URL"))?;
    let client = CloudStreamRegistryClient::new(CloudStreamRegistryFetchConfig::default())?;
    let snapshot = client
        .fetch_registry(&registry.registry_type, url)
        .await
        .with_context(|| {
            format!(
                "refreshing CloudStream registry '{}'",
                registry.display_name
            )
        })?;
    let summary = persist_cloudstream_registry_snapshot(
        store,
        &CloudStreamRegistryStoreInput {
            registry_id,
            instance_id: instance.instance_id,
            registry_key: registry.registry_key.clone(),
            registry_type: registry.registry_type.clone(),
            trust_class: registry.trust_class.clone(),
            display_name: Some(registry.display_name.clone()),
            url: registry.url.clone(),
            enabled: registry.enabled,
            auto_refresh: registry.auto_refresh,
            trusted_for_executable_updates: registry.trusted_for_executable_updates,
        },
        &snapshot,
    )
    .await?;
    Ok(format!(
        "Refreshed '{}': {} module(s), {} version(s), {} disabled.",
        registry.display_name, summary.modules, summary.versions, summary.disabled_modules
    ))
}

async fn cloudstream_validate_module_activation(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    module: &ExtensionSourceModule,
) -> anyhow::Result<()> {
    if module.unsupported {
        anyhow::bail!(
            "'{}' is unsupported: {}",
            module.display_name,
            module
                .unsupported_reason
                .as_deref()
                .unwrap_or("unsupported by CloudStream Compat")
        );
    }
    if module.account_required {
        anyhow::bail!(
            "'{}' requires an account before activation",
            module.display_name
        );
    }
    let registry = cloudstream_find_registry(store, instance_id, module.registry_id).await?;
    if !registry.enabled {
        anyhow::bail!(
            "source registry '{}' is disabled; enable the registry first",
            registry.display_name
        );
    }
    if registry.trust_class == "custom" && !registry.trusted_for_executable_updates {
        anyhow::bail!(
            "source registry '{}' must be explicitly trusted before activating modules",
            registry.display_name
        );
    }
    Ok(())
}

async fn cloudstream_mark_active_version(
    store: &ExtensionStore<'_>,
    versions: &[ExtensionSourceModuleVersion],
    active_version: &str,
) -> anyhow::Result<()> {
    for version in versions {
        let install_state = if version.version == active_version {
            "active"
        } else if version.install_state == "active" {
            "installed"
        } else {
            version.install_state.as_str()
        };
        store
            .set_source_module_version_state(
                version.version_id,
                install_state,
                &version.smoke_status,
                version.smoke_error.as_deref(),
            )
            .await?;
    }
    Ok(())
}

fn cloudstream_preferred_module_version(
    module: &ExtensionSourceModule,
    versions: &[ExtensionSourceModuleVersion],
    requested: Option<&str>,
) -> Option<String> {
    if let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) {
        return versions
            .iter()
            .find(|version| version.version == requested)
            .map(|version| version.version.clone());
    }
    module
        .pinned_version
        .clone()
        .or_else(|| module.active_version.clone())
        .or_else(|| {
            versions
                .iter()
                .max_by(|left, right| {
                    cloudstream_version_key(&left.version)
                        .cmp(&cloudstream_version_key(&right.version))
                        .then_with(|| left.version.cmp(&right.version))
                })
                .map(|version| version.version.clone())
        })
}

fn cloudstream_version_key(version: &str) -> Vec<u64> {
    version
        .split(|ch: char| !ch.is_ascii_digit())
        .filter_map(|part| part.parse::<u64>().ok())
        .collect()
}

async fn cloudstream_find_registry(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    registry_id: Uuid,
) -> anyhow::Result<ExtensionSourceRegistry> {
    store
        .list_source_registries(Some(instance_id))
        .await?
        .into_iter()
        .find(|registry| registry.registry_id == registry_id)
        .ok_or_else(|| anyhow::anyhow!("CloudStream registry '{registry_id}' was not found"))
}

async fn cloudstream_find_module(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    source_module_id: Uuid,
) -> anyhow::Result<ExtensionSourceModule> {
    store
        .list_source_modules(Some(instance_id), None)
        .await?
        .into_iter()
        .find(|module| module.source_module_id == source_module_id)
        .ok_or_else(|| {
            anyhow::anyhow!("CloudStream source module '{source_module_id}' was not found")
        })
}

fn cloudstream_selected_instance(
    context: &ExtensionControlContext,
) -> anyhow::Result<&ExtensionInstance> {
    context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active CloudStream Compat instance is available yet"))
}

fn cloudstream_param_uuid(
    params: &HashMap<String, serde_json::Value>,
    key: &str,
) -> anyhow::Result<Uuid> {
    let raw = cloudstream_param_string(params, key)?;
    Uuid::parse_str(&raw).with_context(|| format!("parsing {key}"))
}

fn cloudstream_param_string(
    params: &HashMap<String, serde_json::Value>,
    key: &str,
) -> anyhow::Result<String> {
    params
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("{key} is required"))
}

fn cloudstream_stable_text_id(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source".to_string()
    } else {
        output
    }
}

pub(super) async fn load_live_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    resolve_adapter(context)
        .load_live_snapshot(state, store, context)
        .await
}

pub(super) async fn build_sections(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Vec<ExtensionControlSection>> {
    let mut sections = resolve_adapter(context)
        .build_sections(state, store, context)
        .await?;
    if let Some(section) = build_backup_section(state, context).await? {
        sections.push(section);
    }
    Ok(sections)
}

pub(super) fn build_actions(context: &ExtensionControlContext) -> Vec<ExtensionControlAction> {
    let mut actions = Vec::new();
    if context.instances.is_empty() && context.extension.kind != ExtensionKind::Blueprint {
        actions.push(build_create_default_instance_action());
    }
    actions.extend(resolve_adapter(context).build_actions(context));
    actions
}

pub(super) async fn update_settings(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    values: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<()> {
    resolve_adapter(context)
        .update_settings(state, store, context, values)
        .await
}

pub(super) async fn execute_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    action_id: &str,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    if action_id == "create_default_instance" {
        let created =
            super::create_default_extension_instance(state, store, &context.manifest).await?;
        return Ok(if created {
            "Default instance created.".to_string()
        } else {
            "Default instance is already available.".to_string()
        });
    }
    if action_id == "create_backup" {
        return run_create_backup_action(state, context).await;
    }
    if action_id == "restore_backup" {
        return run_restore_backup_action(state, context, params).await;
    }
    resolve_adapter(context)
        .execute_action(state, store, context, action_id, params)
        .await
}

fn resolve_adapter(context: &ExtensionControlContext) -> Box<dyn ExtensionControlProvider> {
    match context.control_binding {
        ExtensionControlBinding::Sonarr => Box::new(ArrManagerControlAdapter {
            implementation: "sonarr",
        }),
        ExtensionControlBinding::Radarr => Box::new(ArrManagerControlAdapter {
            implementation: "radarr",
        }),
        ExtensionControlBinding::Prowlarr => Box::new(ProwlarrControlAdapter),
        ExtensionControlBinding::Qbittorrent | ExtensionControlBinding::Nzbget => {
            Box::new(DownloaderControlAdapter)
        }
        ExtensionControlBinding::RealDebrid => Box::new(DebridControlAdapter),
        ExtensionControlBinding::CloudStream => Box::new(CloudStreamControlAdapter),
        ExtensionControlBinding::Prism => Box::new(PrismControlAdapter),
        ExtensionControlBinding::GenericManifest => Box::new(GenericManifestControlProvider),
        ExtensionControlBinding::Unsupported => Box::new(UnsupportedControlProvider),
    }
}

fn build_test_connection_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "test_connection".to_string(),
        label: "Test connection".to_string(),
        description: "Check that Elixir can reach this service and read its status.".to_string(),
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

fn build_create_default_instance_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "create_default_instance".to_string(),
        label: "Create default instance".to_string(),
        description: "Create the default runtime instance Elixir uses to manage this extension."
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

fn build_repair_connection_issue_action(implementation: &str) -> ExtensionControlAction {
    let label = match implementation {
        "sonarr" => "Repair Sonarr",
        "radarr" => "Repair Radarr",
        _ => "Repair connection",
    };
    ExtensionControlAction {
        id: "repair_connection_issue".to_string(),
        label: label.to_string(),
        description:
            "Recreate this runtime, wait for it to come back, then re-apply Elixir-managed wiring."
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

async fn build_backup_section(
    state: &AppState,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let Some(policy) = context.manifest.backup.as_ref() else {
        return Ok(None);
    };
    let Some(instance) = context.selected_instance.as_ref() else {
        return Ok(None);
    };
    let snapshots = state
        .orchestrator
        .list_extension_backups(
            &state.settings.extensions.storage_root,
            &context.extension.extension_id,
            instance.instance_id,
        )
        .await?;
    let entities = snapshots
        .iter()
        .map(|snapshot| ExtensionControlEntity {
            id: snapshot.snapshot_id.to_string(),
            title: snapshot.label.clone(),
            subtitle: Some(format!(
                "{} • {}",
                snapshot.created_at.format("%Y-%m-%d %H:%M:%S UTC"),
                snapshot.reason.replace('_', " ")
            )),
            details: vec![format!(
                "Includes: {}",
                snapshot
                    .items
                    .iter()
                    .map(|item| item.label.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            )],
            actions: vec![build_restore_backup_action(snapshot.snapshot_id)],
        })
        .collect::<Vec<_>>();
    let mut notices = Vec::new();
    if snapshots.is_empty() {
        notices.push(control_notice(
            "info",
            "no_backups",
            "No backups yet",
            "Create a backup before using restore or performing risky repairs.",
        ));
    }
    Ok(Some(ExtensionControlSection {
        id: "backups".to_string(),
        title: "Backups".to_string(),
        description: format!(
            "Elixir stores exact snapshots of the extension paths this manifest opted into backup. Retention: {} snapshot(s).",
            policy.retention
        ),
        policy: Some(control_policy_managed(
            "These snapshots are extension-defined recovery points. Elixir can create and restore them without reconfiguring unrelated services.",
        )),
        notices,
        fields: Vec::new(),
        entities,
        actions: vec![build_create_backup_action()],
    }))
}

fn build_create_backup_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "create_backup".to_string(),
        label: "Create backup".to_string(),
        description: "Capture a recovery snapshot of this extension's backed-up state.".to_string(),
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

fn build_restore_backup_action(snapshot_id: Uuid) -> ExtensionControlAction {
    ExtensionControlAction {
        id: "restore_backup".to_string(),
        label: "Restore".to_string(),
        description:
            "Replace this extension's backed-up state with the selected snapshot and recreate its runtime."
                .to_string(),
        kind: "secondary".to_string(),
        params: Some(json!({
            "snapshotId": snapshot_id.to_string()
        })),
        confirm_text: Some(
            "Restore this backup? Elixir will first create a recovery point, then replace the extension's backed-up state and recreate its runtime.".to_string(),
        ),
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

async fn run_create_backup_action(
    state: &AppState,
    context: &ExtensionControlContext,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let snapshot = state
        .orchestrator
        .create_extension_backup(
            &state.settings.extensions.storage_root,
            &context.extension.extension_id,
            instance,
            &context.manifest,
            None,
            "manual",
        )
        .await?;
    Ok(format!("Created backup '{}'.", snapshot.label))
}

async fn run_restore_backup_action(
    state: &AppState,
    context: &ExtensionControlContext,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let snapshot_id = parse_snapshot_id(params)?;
    let outcome = state
        .orchestrator
        .restore_extension_backup(
            &state.settings.extensions.storage_root,
            &context.extension.extension_id,
            instance,
            &context.manifest,
            snapshot_id,
        )
        .await?;
    Ok(match outcome.recovery_point {
        Some(recovery_point) => format!(
            "Restored backup '{}'. Elixir also created recovery point '{}'.",
            outcome.restored_snapshot.label, recovery_point.label
        ),
        None => format!("Restored backup '{}'.", outcome.restored_snapshot.label),
    })
}

fn parse_snapshot_id(params: &HashMap<String, serde_json::Value>) -> anyhow::Result<Uuid> {
    let snapshot_id = params
        .get("snapshotId")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("backup restore action is missing snapshotId"))?;
    Uuid::parse_str(snapshot_id).context("parsing backup snapshot id")
}

const ARR_CONNECTION_REPAIR_TIMEOUT: Duration = Duration::from_secs(75);
const ARR_CONNECTION_REPAIR_REPAIR_TIMEOUT: Duration = Duration::from_secs(45);
const ARR_CONNECTION_REPAIR_POLL_INTERVAL: Duration = Duration::from_secs(3);
const ARR_CONNECTION_REPAIR_LOG_WINDOW_MINUTES: i64 = 10;
const ARR_CONNECTION_REPAIR_LOG_LINES: usize = 12;
const ARR_CONNECTION_REPAIR_LOG_CHARS: usize = 1600;

async fn repair_arr_connection_issue(
    adapter: &ArrManagerControlAdapter,
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    implementation: &str,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let service_label = arr_service_label(implementation);

    state
        .orchestrator
        .recreate_instance_runtime(&context.extension.extension_id, instance, &context.manifest)
        .await
        .with_context(|| format!("recreating {service_label} runtime"))?;

    let recovered_snapshot = match wait_for_arr_live_snapshot(
        adapter,
        state,
        store,
        context,
        ARR_CONNECTION_REPAIR_TIMEOUT,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            anyhow::bail!(
                "{}",
                arr_runtime_repair_failure_message(state, instance.instance_id, service_label, err)
                    .await
            );
        }
    };

    let repair_outcome = super::run_extension_control_managed_repair(state).await;
    let final_snapshot = match wait_for_arr_live_snapshot(
        adapter,
        state,
        store,
        context,
        ARR_CONNECTION_REPAIR_REPAIR_TIMEOUT,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            if repair_outcome.is_err() {
                recovered_snapshot
            } else {
                anyhow::bail!(
                    "{}",
                    arr_runtime_repair_failure_message(
                        state,
                        instance.instance_id,
                        service_label,
                        err
                    )
                    .await
                );
            }
        }
    };

    let connection_message = test_connection_message(implementation, context, &final_snapshot);
    match repair_outcome {
        Ok(repair_message) => Ok(format!(
            "{service_label} runtime recreated. {connection_message} {repair_message}"
        )),
        Err(err) => Ok(format!(
            "{service_label} runtime recreated. {connection_message} Explicit repair reported a follow-up issue: {err}"
        )),
    }
}

async fn wait_for_arr_live_snapshot(
    adapter: &ArrManagerControlAdapter,
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    timeout: Duration,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_error = match adapter.load_live_snapshot(state, store, context).await {
        Ok(snapshot) => return Ok(snapshot),
        Err(err) => err,
    };

    while std::time::Instant::now() < deadline {
        tokio::time::sleep(ARR_CONNECTION_REPAIR_POLL_INTERVAL).await;
        match adapter.load_live_snapshot(state, store, context).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(err) => last_error = err,
        }
    }

    Err(last_error)
}

async fn arr_runtime_repair_failure_message(
    state: &AppState,
    instance_id: uuid::Uuid,
    service_label: &str,
    wait_err: anyhow::Error,
) -> String {
    let since =
        chrono::Utc::now() - chrono::Duration::minutes(ARR_CONNECTION_REPAIR_LOG_WINDOW_MINUTES);
    let logs = state
        .orchestrator
        .instance_runtime_logs(instance_id, Some(since))
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let excerpt = summarize_runtime_log_excerpt(&logs);

    if excerpt.is_empty() {
        format!(
            "{service_label} did not become reachable after recreate within {} seconds. {}",
            ARR_CONNECTION_REPAIR_TIMEOUT.as_secs(),
            wait_err
        )
    } else {
        format!(
            "{service_label} did not become reachable after recreate within {} seconds. {} Latest startup logs:\n{}",
            ARR_CONNECTION_REPAIR_TIMEOUT.as_secs(),
            wait_err,
            excerpt
        )
    }
}

fn summarize_runtime_log_excerpt(logs: &str) -> String {
    let mut lines = logs
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return String::new();
    }
    if lines.len() > ARR_CONNECTION_REPAIR_LOG_LINES {
        lines = lines.split_off(lines.len() - ARR_CONNECTION_REPAIR_LOG_LINES);
    }
    let mut excerpt = lines.join("\n");
    if excerpt.len() > ARR_CONNECTION_REPAIR_LOG_CHARS {
        let keep = ARR_CONNECTION_REPAIR_LOG_CHARS.saturating_sub(1);
        excerpt = excerpt
            .chars()
            .rev()
            .take(keep)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>();
        excerpt.insert(0, '…');
    }
    excerpt
}

fn arr_service_label(implementation: &str) -> &'static str {
    match implementation {
        "sonarr" => "Sonarr",
        "radarr" => "Radarr",
        _ => "Service",
    }
}

fn test_connection_message(
    implementation: &str,
    context: &ExtensionControlContext,
    snapshot: &ExtensionControlLiveSnapshot,
) -> String {
    let label = match implementation {
        "sonarr" => "Sonarr",
        "radarr" => "Radarr",
        "prowlarr" => "Prowlarr",
        "qbittorrent" => "qBittorrent",
        "nzbget" => "NZBGet",
        _ => context
            .selected_instance
            .as_ref()
            .map(|instance| instance.instance_name.as_str())
            .unwrap_or("Service"),
    };
    match snapshot.version.as_deref() {
        Some(version) if !version.trim().is_empty() => {
            format!("{label} is reachable. Version {version}.")
        }
        _ => format!("{label} is reachable."),
    }
}

#[derive(Debug, Deserialize)]
struct QbittorrentControlTorrent {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    dlspeed: Option<u64>,
    #[serde(default)]
    upspeed: Option<u64>,
    #[serde(default)]
    total_size: Option<u64>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    amount_left: Option<u64>,
    #[serde(default)]
    eta: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct NzbgetControlGroup {
    #[serde(rename = "NZBID")]
    nzb_id: i64,
    #[serde(rename = "NZBName", default)]
    nzb_name: Option<String>,
    #[serde(rename = "NZBFilename", default)]
    nzb_filename: Option<String>,
    #[serde(rename = "Category", default)]
    category: Option<String>,
    #[serde(rename = "Status", default)]
    status: Option<String>,
    #[serde(rename = "Priority", default)]
    priority: Option<i64>,
    #[serde(rename = "FileSizeLo", default)]
    file_size_lo: Option<u64>,
    #[serde(rename = "FileSizeHi", default)]
    file_size_hi: Option<u64>,
    #[serde(rename = "RemainingSizeLo", default)]
    remaining_size_lo: Option<u64>,
    #[serde(rename = "RemainingSizeHi", default)]
    remaining_size_hi: Option<u64>,
    #[serde(rename = "DownloadedSizeLo", default)]
    downloaded_size_lo: Option<u64>,
    #[serde(rename = "DownloadedSizeHi", default)]
    downloaded_size_hi: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct NzbgetControlConfigItem {
    #[serde(alias = "Name")]
    name: String,
    #[serde(alias = "Value")]
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct NzbgetControlConfigUpdate {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

impl NzbgetControlConfigUpdate {
    fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct NzbgetServerEntry {
    slot: u32,
    active: bool,
    name: String,
    level: i64,
    host: String,
    encryption: bool,
    port: Option<u16>,
    username: String,
    password: String,
    connections: Option<u64>,
    cert_verification: String,
}

const NZBGET_SERVER_INVENTORY_KEY: &str = "server_inventory";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
struct PersistedNzbgetServerEntry {
    slot: u32,
    active: bool,
    name: String,
    level: i64,
    host: String,
    encryption: bool,
    port: Option<u16>,
    connections: Option<u64>,
    cert_verification: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NzbgetProviderInventorySummary {
    pub configured_count: usize,
    pub active_count: usize,
}

fn nzbget_inventory_has_configured_servers(inventory: &BTreeMap<u32, NzbgetServerEntry>) -> bool {
    inventory.values().any(nzbget_server_is_configured)
}

fn persisted_nzbget_server_inventory(
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
) -> Vec<PersistedNzbgetServerEntry> {
    inventory
        .values()
        .filter(|server| nzbget_server_is_configured(server))
        .map(|server| PersistedNzbgetServerEntry {
            slot: server.slot,
            active: server.active,
            name: server.name.clone(),
            level: server.level,
            host: server.host.clone(),
            encryption: server.encryption,
            port: server.port,
            connections: server.connections,
            cert_verification: nzbget_server_cert_verification(server),
        })
        .collect()
}

fn persisted_nzbget_server_inventory_to_live(
    persisted: Vec<PersistedNzbgetServerEntry>,
) -> BTreeMap<u32, NzbgetServerEntry> {
    persisted
        .into_iter()
        .map(|server| {
            (
                server.slot,
                NzbgetServerEntry {
                    slot: server.slot,
                    active: server.active,
                    name: server.name,
                    level: server.level,
                    host: server.host,
                    encryption: server.encryption,
                    port: server.port,
                    username: String::new(),
                    password: String::new(),
                    connections: server.connections,
                    cert_verification: normalize_nzbget_cert_verification(
                        &server.cert_verification,
                    ),
                },
            )
        })
        .collect()
}

fn load_persisted_nzbget_server_inventory_state_from_config(
    config_json: Option<&serde_json::Value>,
) -> anyhow::Result<Option<BTreeMap<u32, NzbgetServerEntry>>> {
    let Some(config_json) = config_json else {
        return Ok(None);
    };
    let Some(value) = config_json.get(NZBGET_SERVER_INVENTORY_KEY) else {
        return Ok(None);
    };
    let persisted: Vec<PersistedNzbgetServerEntry> = serde_json::from_value(value.clone())
        .context("parsing persisted nzbget server inventory")?;
    Ok(Some(persisted_nzbget_server_inventory_to_live(persisted)))
}

async fn load_persisted_nzbget_server_inventory_state(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<Option<BTreeMap<u32, NzbgetServerEntry>>> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("NZBGet instance {instance_id} was not found"))?;
    load_persisted_nzbget_server_inventory_state_from_config(instance.config_json.as_ref())
}

async fn persist_nzbget_server_inventory(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
) -> anyhow::Result<()> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("NZBGet instance {instance_id} was not found"))?;
    let mut config = match instance.config_json {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => anyhow::bail!("nzbget instance config must be a JSON object"),
        None => serde_json::Map::new(),
    };

    let persisted = persisted_nzbget_server_inventory(inventory);
    let value = serde_json::to_value(&persisted).context("serializing nzbget server inventory")?;
    if config.get(NZBGET_SERVER_INVENTORY_KEY) == Some(&value) {
        return Ok(());
    }
    config.insert(NZBGET_SERVER_INVENTORY_KEY.to_string(), value);

    store
        .update_instance_config(instance_id, Some(&serde_json::Value::Object(config)))
        .await
}

fn upsert_nzbget_server_inventory_entry(
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
    server: NzbgetServerEntry,
) -> BTreeMap<u32, NzbgetServerEntry> {
    let mut updated = inventory.clone();
    updated.insert(server.slot, server);
    updated
}

fn remove_nzbget_server_inventory_entry(
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
    slot: u32,
) -> BTreeMap<u32, NzbgetServerEntry> {
    let mut updated = inventory.clone();
    updated.remove(&slot);
    updated
}

fn downloader_implementation(context: &ExtensionControlContext) -> String {
    context
        .selected_provider
        .as_ref()
        .and_then(|provider| provider.implementation.as_deref())
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| context.extension.extension_id.to_ascii_lowercase())
}

fn build_downloader_live_metrics(
    activity: Option<&crate::drivers::ActivitySnapshot>,
) -> Vec<ExtensionControlMetric> {
    let Some(activity) = activity else {
        return Vec::new();
    };
    let mut metrics = Vec::new();
    if let Some(status) = activity
        .status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        metrics.push(control_metric("status", "Status", status.to_string()));
    }
    if let Some(rate) = activity.download_rate_bps.filter(|value| *value > 0) {
        metrics.push(control_metric(
            "downloadRate",
            "Download rate",
            format_rate_bps(rate),
        ));
    }
    if let Some(rate) = activity.upload_rate_bps.filter(|value| *value > 0) {
        metrics.push(control_metric(
            "uploadRate",
            "Upload rate",
            format_rate_bps(rate),
        ));
    }
    if let Some(count) = activity.active_items {
        metrics.push(control_metric(
            "activeItems",
            "Active items",
            count.to_string(),
        ));
    }
    if let Some(count) = activity.queued_items {
        metrics.push(control_metric(
            "queuedItems",
            "Queued items",
            count.to_string(),
        ));
    }
    if let Some(count) = activity.error_items {
        metrics.push(control_metric("errorItems", "Issues", count.to_string()));
    }
    if let Some(count) = activity.post_process_items {
        metrics.push(control_metric(
            "postProcessItems",
            "Post-processing",
            count.to_string(),
        ));
    }
    metrics
}

fn control_metric(id: &str, label: &str, value: String) -> ExtensionControlMetric {
    ExtensionControlMetric {
        id: id.to_string(),
        label: label.to_string(),
        value,
    }
}

pub(super) async fn load_nzbget_provider_inventory_summary(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<NzbgetProviderInventorySummary> {
    let inventory = load_nzbget_server_inventory_for_instance(state, store, instance_id).await?;
    Ok(NzbgetProviderInventorySummary {
        configured_count: inventory
            .values()
            .filter(|server| nzbget_server_is_configured(server))
            .count(),
        active_count: inventory
            .values()
            .filter(|server| nzbget_server_is_configured(server) && server.active)
            .count(),
    })
}

async fn build_nzbget_servers_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    if downloader_implementation(context) != "nzbget" {
        return Ok(None);
    }
    let Some(instance) = context.selected_instance.as_ref() else {
        return Ok(None);
    };

    let inventory = load_nzbget_server_inventory(state, store, context).await?;
    let mut servers = inventory
        .into_values()
        .filter(nzbget_server_is_configured)
        .collect::<Vec<_>>();
    servers.sort_by(|left, right| {
        left.level
            .cmp(&right.level)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| {
                nzbget_server_title(left)
                    .to_ascii_lowercase()
                    .cmp(&nzbget_server_title(right).to_ascii_lowercase())
            })
    });

    let mut entities = Vec::with_capacity(servers.len());
    for server in &servers {
        let username = nzbget_resolve_username(state, store, instance.instance_id, server).await?;
        entities.push(build_nzbget_server_entity(server, &username));
    }

    Ok(Some(ExtensionControlSection {
        id: "servers".to_string(),
        title: "Servers".to_string(),
        description:
            "Configure one or more Usenet providers here. Elixir stores credentials as instance secrets, writes the NZBGet config, and validates each server with NZBGet's real connection test."
                .to_string(),
        policy: None,
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![nzbget_add_server_action()],
    }))
}

fn build_nzbget_server_entity(
    server: &NzbgetServerEntry,
    username: &str,
) -> ExtensionControlEntity {
    let title = nzbget_server_title(server);
    let mut subtitle_parts = vec![format!("Priority {}", server.level)];
    subtitle_parts.push(if server.active {
        "Active".to_string()
    } else {
        "Disabled".to_string()
    });
    let subtitle = Some(subtitle_parts.join(" · "));

    let mut details = vec![format!(
        "Server: {}:{}",
        server.host.trim(),
        nzbget_server_port(server)
    )];
    details.push(format!(
        "TLS: {}",
        if server.encryption { "On" } else { "Off" }
    ));
    details.push(format!(
        "Connections: {}",
        nzbget_server_connections(server)
    ));
    details.push(format!(
        "Certificate check: {}",
        nzbget_server_cert_verification(server)
    ));
    if !username.trim().is_empty() {
        details.push(format!("Username: {}", username.trim()));
    }

    ExtensionControlEntity {
        id: format!("server-{}", server.slot),
        title,
        subtitle,
        details,
        actions: vec![
            nzbget_edit_server_action(server, username),
            control_entity_action(
                "test_server",
                "Test",
                "Run NZBGet's real provider connection test for this server.",
                "secondary",
                json!({ "slot": server.slot }),
                None,
            ),
            control_entity_action(
                "remove_server",
                "Remove",
                "Remove this Usenet provider from NZBGet.",
                "danger",
                json!({ "slot": server.slot }),
                Some(format!(
                    "Remove {} from NZBGet?",
                    nzbget_server_title(server)
                )),
            ),
        ],
    }
}

fn nzbget_add_server_action() -> ExtensionControlAction {
    ExtensionControlAction {
        id: "add_server".to_string(),
        label: "Add provider".to_string(),
        description: "Add a Usenet provider to NZBGet.".to_string(),
        kind: "info".to_string(),
        params: Some(json!({
            "promptTitle": "Add NZBGet provider",
            "promptFields": nzbget_server_prompt_fields(None, "", true)
        })),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

fn nzbget_edit_server_action(server: &NzbgetServerEntry, username: &str) -> ExtensionControlAction {
    ExtensionControlAction {
        id: "edit_server".to_string(),
        label: "Edit".to_string(),
        description: "Edit this NZBGet provider.".to_string(),
        kind: "secondary".to_string(),
        params: Some(json!({
            "slot": server.slot,
            "promptTitle": "Edit NZBGet provider",
            "promptFields": nzbget_server_prompt_fields(Some(server), username, false)
        })),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

fn nzbget_server_prompt_fields(
    server: Option<&NzbgetServerEntry>,
    username: &str,
    require_credentials: bool,
) -> Vec<serde_json::Value> {
    let name = server.map(|value| value.name.clone()).unwrap_or_default();
    let host = server.map(|value| value.host.clone()).unwrap_or_default();
    let port = server.map(nzbget_server_port).unwrap_or(563_u16);
    let encryption = server.map(|value| value.encryption).unwrap_or(true);
    let connections = server.map(nzbget_server_connections).unwrap_or(20_u64);
    let priority = server.map(|value| value.level).unwrap_or(0_i64);
    let cert_verification = server
        .map(nzbget_server_cert_verification)
        .unwrap_or_else(|| "strict".to_string());
    let active = server.map(|value| value.active).unwrap_or(true);

    vec![
        nzbget_prompt_text_field(
            "name",
            "Label",
            "Optional friendly label for this provider.",
            name,
            false,
            false,
        ),
        nzbget_prompt_text_field(
            "host",
            "Host",
            "Provider host name, for example news.example.com.",
            host,
            true,
            false,
        ),
        nzbget_prompt_number_field(
            "port",
            "Port",
            "Provider port. TLS providers usually use 563.",
            serde_json::Value::from(port),
            true,
        ),
        nzbget_prompt_text_field(
            "username",
            "Username",
            "Provider login username.",
            username.to_string(),
            require_credentials,
            false,
        ),
        nzbget_prompt_text_field(
            "password",
            "Password",
            if require_credentials {
                "Provider login password."
            } else {
                "Leave blank to keep the current password."
            },
            String::new(),
            require_credentials,
            true,
        ),
        nzbget_prompt_toggle_field(
            "encryption",
            "Use TLS",
            "Enable encrypted provider connections.",
            encryption,
        ),
        nzbget_prompt_number_field(
            "connections",
            "Connections",
            "Number of parallel connections Elixir should configure for this provider.",
            serde_json::Value::from(connections),
            true,
        ),
        nzbget_prompt_number_field(
            "priority",
            "Priority",
            "Lower priority numbers are shown first in Elixir and written into NZBGet.",
            serde_json::Value::from(priority),
            true,
        ),
        nzbget_prompt_select_field(
            "certVerification",
            "Certificate check",
            "How strictly NZBGet should verify the provider certificate.",
            &cert_verification,
            &[
                ("strict", "Strict"),
                ("minimal", "Minimal"),
                ("none", "None"),
            ],
            true,
        ),
        nzbget_prompt_toggle_field(
            "active",
            "Active",
            "Disable this provider without deleting it from NZBGet.",
            active,
        ),
    ]
}

fn nzbget_prompt_text_field(
    id: &str,
    label: &str,
    description: &str,
    value: String,
    required: bool,
    secret: bool,
) -> serde_json::Value {
    json!({
        "id": id,
        "label": label,
        "description": description,
        "fieldType": if secret { "password" } else { "text" },
        "value": value,
        "required": required,
        "readonly": false,
        "secret": secret,
        "options": [],
    })
}

fn nzbget_prompt_number_field(
    id: &str,
    label: &str,
    description: &str,
    value: serde_json::Value,
    required: bool,
) -> serde_json::Value {
    json!({
        "id": id,
        "label": label,
        "description": description,
        "fieldType": "number",
        "value": value,
        "required": required,
        "readonly": false,
        "secret": false,
        "options": [],
    })
}

fn nzbget_prompt_toggle_field(
    id: &str,
    label: &str,
    description: &str,
    value: bool,
) -> serde_json::Value {
    json!({
        "id": id,
        "label": label,
        "description": description,
        "fieldType": "toggle",
        "value": value,
        "required": false,
        "readonly": false,
        "secret": false,
        "options": [],
    })
}

fn nzbget_prompt_select_field(
    id: &str,
    label: &str,
    description: &str,
    value: &str,
    options: &[(&str, &str)],
    required: bool,
) -> serde_json::Value {
    json!({
        "id": id,
        "label": label,
        "description": description,
        "fieldType": "select",
        "value": value,
        "required": required,
        "readonly": false,
        "secret": false,
        "options": options.iter().map(|(option_value, option_label)| {
            json!({
                "value": option_value,
                "label": option_label
            })
        }).collect::<Vec<_>>(),
    })
}

async fn load_nzbget_server_inventory(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<BTreeMap<u32, NzbgetServerEntry>> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    load_nzbget_server_inventory_for_instance(state, store, instance.instance_id).await
}

async fn load_nzbget_server_inventory_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<BTreeMap<u32, NzbgetServerEntry>> {
    let persisted_inventory =
        match load_persisted_nzbget_server_inventory_state(store, instance_id).await? {
            Some(inventory) => Some(
                sanitize_persisted_nzbget_server_inventory(state, store, instance_id, inventory)
                    .await?,
            ),
            None => None,
        };
    if let Some(inventory) = persisted_inventory.as_ref() {
        if let Err(err) = persist_nzbget_server_inventory(store, instance_id, inventory).await {
            tracing::warn!("persisting sanitized nzbget server inventory failed: {err}");
        }
    }

    let live_inventory =
        match load_live_nzbget_server_inventory_for_instance(state, store, instance_id).await {
            Ok(inventory) => inventory,
            Err(err) => {
                if let Some(inventory) = persisted_inventory {
                    log_nzbget_control_availability(
                        "loading live nzbget server inventory failed; using persisted inventory",
                        &err,
                    );
                    return Ok(inventory);
                }
                return Err(err);
            }
        };
    let live_inventory = sanitize_live_nzbget_server_inventory(
        state,
        store,
        instance_id,
        persisted_inventory.as_ref(),
        live_inventory,
    )
    .await?;
    if nzbget_inventory_has_configured_servers(&live_inventory) {
        if persisted_inventory.is_some() {
            if let Err(err) =
                persist_nzbget_server_inventory(store, instance_id, &live_inventory).await
            {
                tracing::warn!("persisting nzbget server inventory failed: {err}");
            }
        }
        return Ok(live_inventory);
    }

    let persisted_inventory = persisted_inventory.unwrap_or_default();
    if !nzbget_inventory_has_configured_servers(&persisted_inventory) {
        return Ok(live_inventory);
    }

    restore_nzbget_server_inventory(state, store, instance_id, &persisted_inventory).await?;
    match load_live_nzbget_server_inventory_for_instance(state, store, instance_id).await {
        Ok(restored_inventory) => Ok(restored_inventory),
        Err(err) => {
            log_nzbget_control_availability(
                "reloading restored nzbget server inventory failed",
                &err,
            );
            Ok(persisted_inventory)
        }
    }
}

async fn sanitize_persisted_nzbget_server_inventory(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    inventory: BTreeMap<u32, NzbgetServerEntry>,
) -> anyhow::Result<BTreeMap<u32, NzbgetServerEntry>> {
    let mut sanitized = BTreeMap::new();
    for (slot, server) in inventory {
        if !nzbget_server_is_configured(&server) {
            continue;
        }
        let username = nzbget_load_server_secret(state, store, instance_id, slot, "username")
            .await?
            .unwrap_or_default();
        let password = nzbget_load_server_secret(state, store, instance_id, slot, "password")
            .await?
            .unwrap_or_default();
        if username.trim().is_empty() || password.trim().is_empty() {
            tracing::warn!(
                "dropping persisted nzbget server inventory for slot {slot} because credentials are missing"
            );
            continue;
        }
        sanitized.insert(slot, server);
    }
    Ok(sanitized)
}

async fn sanitize_live_nzbget_server_inventory(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    persisted_inventory: Option<&BTreeMap<u32, NzbgetServerEntry>>,
    inventory: BTreeMap<u32, NzbgetServerEntry>,
) -> anyhow::Result<BTreeMap<u32, NzbgetServerEntry>> {
    let mut sanitized = BTreeMap::new();
    for (slot, server) in inventory {
        if !nzbget_server_is_configured(&server) {
            continue;
        }
        if persisted_inventory
            .map(|inventory| inventory.contains_key(&slot))
            .unwrap_or(false)
        {
            sanitized.insert(slot, server);
            continue;
        }
        let username = nzbget_load_server_secret(state, store, instance_id, slot, "username")
            .await?
            .unwrap_or_default();
        let password = nzbget_load_server_secret(state, store, instance_id, slot, "password")
            .await?
            .unwrap_or_default();
        if username.trim().is_empty() || password.trim().is_empty() {
            if nzbget_server_is_upstream_sample(&server) {
                tracing::debug!("ignoring stock nzbget sample server inventory for slot {slot}");
            } else {
                tracing::warn!(
                    "ignoring live nzbget server inventory for slot {slot} because Elixir does not own it and credentials are missing"
                );
            }
            continue;
        }
        sanitized.insert(slot, server);
    }
    Ok(sanitized)
}

async fn load_live_nzbget_server_inventory_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<BTreeMap<u32, NzbgetServerEntry>> {
    let config_value =
        nzbget_rpc_for_instance(state, store, instance_id, "config", json!([])).await?;
    let config_items: Vec<NzbgetControlConfigItem> =
        serde_json::from_value(config_value).context("parsing nzbget config")?;
    Ok(parse_nzbget_server_inventory(&config_items))
}

fn parse_nzbget_server_inventory(
    config_items: &[NzbgetControlConfigItem],
) -> BTreeMap<u32, NzbgetServerEntry> {
    let mut inventory = BTreeMap::new();
    for item in config_items {
        let Some((slot, field)) = parse_nzbget_server_option(&item.name) else {
            continue;
        };
        let server = inventory.entry(slot).or_insert_with(|| NzbgetServerEntry {
            slot,
            cert_verification: "strict".to_string(),
            ..NzbgetServerEntry::default()
        });
        match field {
            "Active" => server.active = parse_nzbget_bool(&item.value),
            "Name" => server.name = item.value.clone(),
            "Level" => server.level = item.value.trim().parse::<i64>().unwrap_or(0),
            "Host" => server.host = item.value.clone(),
            "Encryption" => server.encryption = parse_nzbget_bool(&item.value),
            "Port" => server.port = item.value.trim().parse::<u16>().ok(),
            "Username" => server.username = item.value.clone(),
            "Password" => server.password = item.value.clone(),
            "Connections" => server.connections = item.value.trim().parse::<u64>().ok(),
            "CertVerification" => {
                server.cert_verification = normalize_nzbget_cert_verification(&item.value)
            }
            _ => {}
        }
    }
    inventory
}

fn parse_nzbget_server_option(name: &str) -> Option<(u32, &str)> {
    let suffix = name.strip_prefix("Server")?;
    let (slot, field) = suffix.split_once('.')?;
    let slot = slot.parse::<u32>().ok()?;
    Some((slot, field))
}

fn parse_nzbget_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "yes" | "true" | "1" | "on"
    )
}

fn normalize_nzbget_cert_verification(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => "none".to_string(),
        "minimal" => "minimal".to_string(),
        _ => "strict".to_string(),
    }
}

fn nzbget_server_is_configured(server: &NzbgetServerEntry) -> bool {
    !server.host.trim().is_empty()
}

fn nzbget_server_is_upstream_sample(server: &NzbgetServerEntry) -> bool {
    server.host.trim().eq_ignore_ascii_case("my.newsserver.com")
        && server.username.trim().eq_ignore_ascii_case("user")
        && server.password.trim() == "pass"
        && server.encryption
        && nzbget_server_port(server) == 563
        && nzbget_server_connections(server) == 8
        && nzbget_server_cert_verification(server) == "strict"
}

fn nzbget_server_title(server: &NzbgetServerEntry) -> String {
    if !server.name.trim().is_empty() {
        server.name.trim().to_string()
    } else if !server.host.trim().is_empty() {
        server.host.trim().to_string()
    } else {
        format!("Server {}", server.slot)
    }
}

fn nzbget_server_port(server: &NzbgetServerEntry) -> u16 {
    server
        .port
        .unwrap_or(if server.encryption { 563_u16 } else { 119_u16 })
}

fn nzbget_server_connections(server: &NzbgetServerEntry) -> u64 {
    server.connections.unwrap_or(8_u64)
}

fn nzbget_server_cert_verification(server: &NzbgetServerEntry) -> String {
    normalize_nzbget_cert_verification(&server.cert_verification)
}

async fn nzbget_resolve_username(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    server: &NzbgetServerEntry,
) -> anyhow::Result<String> {
    if !server.username.trim().is_empty() {
        return Ok(server.username.trim().to_string());
    }
    Ok(
        nzbget_load_server_secret(state, store, instance_id, server.slot, "username")
            .await?
            .unwrap_or_default(),
    )
}

async fn nzbget_resolve_credentials(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    server: &NzbgetServerEntry,
) -> anyhow::Result<(String, String)> {
    let username = if !server.username.trim().is_empty() {
        server.username.trim().to_string()
    } else {
        nzbget_load_server_secret(state, store, instance_id, server.slot, "username")
            .await?
            .unwrap_or_default()
    };
    let password = if !server.password.trim().is_empty() {
        server.password.clone()
    } else {
        nzbget_load_server_secret(state, store, instance_id, server.slot, "password")
            .await?
            .unwrap_or_default()
    };
    Ok((username, password))
}

async fn nzbget_load_server_secret(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    slot: u32,
    key: &str,
) -> anyhow::Result<Option<String>> {
    let Some(secret) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            &nzbget_server_secret_key(slot, key),
        )
        .await?
    else {
        return Ok(None);
    };
    let decrypted = state.secrets.decrypt(&secret.value_encrypted)?;
    let trimmed = decrypted.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn nzbget_server_secret_key(slot: u32, key: &str) -> String {
    format!("nzbget.server.{slot}.{key}")
}

async fn nzbget_upsert_server_secret(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    slot: u32,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let encrypted = state.secrets.encrypt(value)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: nzbget_server_secret_key(slot, key),
            value_encrypted: encrypted,
            rotatable: true,
        })
        .await
}

async fn nzbget_delete_server_secret(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    slot: u32,
    key: &str,
) -> anyhow::Result<()> {
    if let Some(secret) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            &nzbget_server_secret_key(slot, key),
        )
        .await?
    {
        store.delete_secret(secret.secret_id).await?;
    }
    Ok(())
}

async fn restore_nzbget_server_inventory(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
) -> anyhow::Result<()> {
    let mut updates = Vec::new();
    for server in inventory
        .values()
        .filter(|server| nzbget_server_is_configured(server))
    {
        let (username, password) =
            nzbget_resolve_credentials(state, store, instance_id, server).await?;
        updates.extend(nzbget_server_config_updates(server, &username, &password));
    }
    nzbget_save_config_for_instance(state, store, instance_id, updates).await
}

fn nzbget_managed_config_updates_for_config(
    _config_json: Option<&serde_json::Value>,
) -> Vec<NzbgetControlConfigUpdate> {
    vec![
        NzbgetControlConfigUpdate::new("MainDir", NZBGET_MAIN_DIR),
        NzbgetControlConfigUpdate::new("DestDir", DOWNLOADS_ROOT),
        NzbgetControlConfigUpdate::new("InterDir", NZBGET_INCOMPLETE_DIR),
        NzbgetControlConfigUpdate::new("NzbDir", NZBGET_NZB_DIR),
        NzbgetControlConfigUpdate::new("QueueDir", NZBGET_QUEUE_DIR),
        NzbgetControlConfigUpdate::new("TempDir", NZBGET_TEMP_DIR),
        NzbgetControlConfigUpdate::new("ScriptDir", NZBGET_SCRIPT_DIR),
        NzbgetControlConfigUpdate::new("LogFile", NZBGET_LOG_FILE),
        NzbgetControlConfigUpdate::new("WebDir", NZBGET_WEB_DIR),
        NzbgetControlConfigUpdate::new("ConfigTemplate", NZBGET_CONFIG_TEMPLATE),
        NzbgetControlConfigUpdate::new("LockFile", NZBGET_LOCK_FILE),
    ]
}

fn dedupe_nzbget_config_updates(
    updates: Vec<NzbgetControlConfigUpdate>,
) -> Vec<NzbgetControlConfigUpdate> {
    let mut last_assignment = HashMap::new();
    for (index, update) in updates.iter().enumerate() {
        last_assignment.insert(update.name.clone(), index);
    }

    let mut deduped = Vec::with_capacity(updates.len());
    for (index, update) in updates.into_iter().enumerate() {
        if last_assignment.get(&update.name).copied() == Some(index) {
            deduped.push(update);
        }
    }
    deduped
}

fn nzbget_server_config_updates(
    server: &NzbgetServerEntry,
    username: &str,
    password: &str,
) -> Vec<NzbgetControlConfigUpdate> {
    vec![
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Active", server.slot),
            if server.active { "yes" } else { "no" },
        ),
        NzbgetControlConfigUpdate::new(format!("Server{}.Name", server.slot), server.name.clone()),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Level", server.slot),
            server.level.to_string(),
        ),
        NzbgetControlConfigUpdate::new(format!("Server{}.Host", server.slot), server.host.clone()),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Encryption", server.slot),
            if server.encryption { "yes" } else { "no" },
        ),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Port", server.slot),
            nzbget_server_port(server).to_string(),
        ),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Username", server.slot),
            username.trim().to_string(),
        ),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Password", server.slot),
            password.trim().to_string(),
        ),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.Connections", server.slot),
            nzbget_server_connections(server).to_string(),
        ),
        NzbgetControlConfigUpdate::new(
            format!("Server{}.CertVerification", server.slot),
            nzbget_server_cert_verification(server),
        ),
    ]
}

fn nzbget_clear_server_config_updates(slot: u32) -> Vec<NzbgetControlConfigUpdate> {
    vec![
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Active"), "no"),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Name"), ""),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Level"), "0"),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Host"), ""),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Encryption"), "no"),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Port"), "119"),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Username"), ""),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Password"), ""),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.Connections"), "8"),
        NzbgetControlConfigUpdate::new(format!("Server{slot}.CertVerification"), "strict"),
    ]
}

fn nzbget_action_slot(params: &HashMap<String, serde_json::Value>) -> anyhow::Result<u32> {
    match params.get("slot") {
        Some(serde_json::Value::String(value)) => {
            value.trim().parse::<u32>().context("parsing NZBGet slot")
        }
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .and_then(|slot| u32::try_from(slot).ok())
            .ok_or_else(|| anyhow::anyhow!("slot must be a positive integer")),
        Some(_) => anyhow::bail!("slot must be a string or number"),
        None => anyhow::bail!("slot is required"),
    }
}

fn nzbget_param_text(params: &HashMap<String, serde_json::Value>, key: &str) -> Option<String> {
    params.get(key).and_then(|value| match value {
        serde_json::Value::String(text) => Some(text.trim().to_string()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    })
}

fn nzbget_param_required_text(
    params: &HashMap<String, serde_json::Value>,
    key: &str,
) -> anyhow::Result<String> {
    nzbget_param_text(params, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("{key} is required"))
}

fn nzbget_param_bool(
    params: &HashMap<String, serde_json::Value>,
    key: &str,
    default: bool,
) -> anyhow::Result<bool> {
    match params.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(*value),
        Some(serde_json::Value::String(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(true),
                "false" | "0" | "no" | "off" => Ok(false),
                _ => anyhow::bail!("{key} must be true or false"),
            }
        }
        Some(_) => anyhow::bail!("{key} must be true or false"),
        None => Ok(default),
    }
}

fn nzbget_param_u16(params: &HashMap<String, serde_json::Value>, key: &str) -> anyhow::Result<u16> {
    match params.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .and_then(|number| u16::try_from(number).ok())
            .ok_or_else(|| anyhow::anyhow!("{key} must be a valid port")),
        Some(serde_json::Value::String(value)) => value
            .trim()
            .parse::<u16>()
            .with_context(|| format!("parsing {key}")),
        Some(_) => anyhow::bail!("{key} must be a valid port"),
        None => anyhow::bail!("{key} is required"),
    }
}

fn nzbget_param_u64(params: &HashMap<String, serde_json::Value>, key: &str) -> anyhow::Result<u64> {
    match params.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("{key} must be a positive integer")),
        Some(serde_json::Value::String(value)) => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("parsing {key}")),
        Some(_) => anyhow::bail!("{key} must be a positive integer"),
        None => anyhow::bail!("{key} is required"),
    }
}

fn nzbget_param_i64(params: &HashMap<String, serde_json::Value>, key: &str) -> anyhow::Result<i64> {
    match params.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("{key} must be an integer")),
        Some(serde_json::Value::String(value)) => value
            .trim()
            .parse::<i64>()
            .with_context(|| format!("parsing {key}")),
        Some(_) => anyhow::bail!("{key} must be an integer"),
        None => anyhow::bail!("{key} is required"),
    }
}

fn nzbget_param_cert_verification(
    params: &HashMap<String, serde_json::Value>,
    key: &str,
) -> anyhow::Result<String> {
    let value = nzbget_param_required_text(params, key)?.to_ascii_lowercase();
    match value.as_str() {
        "strict" | "minimal" | "none" => Ok(value),
        _ => anyhow::bail!("{key} must be strict, minimal, or none"),
    }
}

fn nzbget_allocate_server_slot(
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
) -> anyhow::Result<u32> {
    for slot in 1..=64_u32 {
        match inventory.get(&slot) {
            Some(server) if nzbget_server_is_configured(server) => continue,
            _ => return Ok(slot),
        }
    }
    anyhow::bail!("No free NZBGet server slots are available")
}

async fn nzbget_add_server(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let inventory = load_nzbget_server_inventory(state, store, context).await?;
    let slot = nzbget_allocate_server_slot(&inventory)?;
    let (message, _) = nzbget_save_server(
        state,
        store,
        instance.instance_id,
        &inventory,
        slot,
        None,
        params,
    )
    .await?;
    Ok(message)
}

async fn nzbget_edit_server(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let slot = nzbget_action_slot(params)?;
    let inventory = load_nzbget_server_inventory(state, store, context).await?;
    let existing = inventory
        .get(&slot)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("NZBGet provider slot {slot} was not found"))?;
    let (message, _) = nzbget_save_server(
        state,
        store,
        instance.instance_id,
        &inventory,
        slot,
        Some(&existing),
        params,
    )
    .await?;
    Ok(message)
}

async fn nzbget_test_server_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let slot = nzbget_action_slot(params)?;
    let inventory = load_nzbget_server_inventory(state, store, context).await?;
    let server = inventory
        .get(&slot)
        .ok_or_else(|| anyhow::anyhow!("NZBGet provider slot {slot} was not found"))?;
    let result =
        match nzbget_test_server_connection_with_retry(state, store, instance.instance_id, server)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return Ok(format!(
                    "Connection test unavailable: {}",
                    classify_nzbget_validation_transport_error(&err)
                ));
            }
        };
    Ok(match result {
        Some(message) => format!("Connection test failed: {message}"),
        None => format!("{} validated successfully.", nzbget_server_title(server)),
    })
}

async fn nzbget_remove_server(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let slot = nzbget_action_slot(params)?;
    let inventory = load_nzbget_server_inventory(state, store, context).await?;
    let existing = inventory
        .get(&slot)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("NZBGet provider slot {slot} was not found"))?;
    let updated_inventory = remove_nzbget_server_inventory_entry(&inventory, slot);

    nzbget_save_config_for_instance(
        state,
        store,
        instance.instance_id,
        nzbget_clear_server_config_updates(slot),
    )
    .await?;
    nzbget_delete_server_secret(store, instance.instance_id, slot, "username").await?;
    nzbget_delete_server_secret(store, instance.instance_id, slot, "password").await?;
    persist_nzbget_server_inventory(store, instance.instance_id, &updated_inventory).await?;

    Ok(format!(
        "Removed {} from NZBGet.",
        nzbget_server_title(&existing)
    ))
}

async fn nzbget_save_server(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    inventory: &BTreeMap<u32, NzbgetServerEntry>,
    slot: u32,
    existing: Option<&NzbgetServerEntry>,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<(String, NzbgetServerEntry)> {
    let host = nzbget_param_required_text(params, "host")?;
    let port = nzbget_param_u16(params, "port")?;
    if port == 0 {
        anyhow::bail!("port must be between 1 and 65535");
    }
    let connections = nzbget_param_u64(params, "connections")?;
    if !(1..=200).contains(&connections) {
        anyhow::bail!("connection limit invalid: enter a value between 1 and 200");
    }
    let level = nzbget_param_i64(params, "priority")?;
    let encryption = nzbget_param_bool(params, "encryption", true)?;
    let active = nzbget_param_bool(params, "active", true)?;
    let cert_verification = nzbget_param_cert_verification(params, "certVerification")?;
    let name = nzbget_param_text(params, "name").unwrap_or_default();

    let (existing_username, existing_password) = match existing {
        Some(server) => nzbget_resolve_credentials(state, store, instance_id, server).await?,
        None => (String::new(), String::new()),
    };
    let username = nzbget_param_text(params, "username")
        .filter(|value| !value.is_empty())
        .unwrap_or(existing_username);
    let password = nzbget_param_text(params, "password")
        .filter(|value| !value.is_empty())
        .unwrap_or(existing_password);
    if username.trim().is_empty() || password.trim().is_empty() {
        anyhow::bail!("auth failed: username and password are required");
    }

    let server = NzbgetServerEntry {
        slot,
        active,
        name,
        level,
        host,
        encryption,
        port: Some(port),
        username: username.clone(),
        password: password.clone(),
        connections: Some(connections),
        cert_verification: cert_verification.clone(),
    };

    nzbget_save_config_for_instance(
        state,
        store,
        instance_id,
        nzbget_server_config_updates(&server, &username, &password),
    )
    .await?;

    nzbget_upsert_server_secret(state, store, instance_id, slot, "username", &username).await?;
    nzbget_upsert_server_secret(state, store, instance_id, slot, "password", &password).await?;
    let updated_inventory = upsert_nzbget_server_inventory_entry(inventory, server.clone());
    persist_nzbget_server_inventory(store, instance_id, &updated_inventory).await?;

    let title = nzbget_server_title(&server);
    let message =
        match nzbget_test_server_connection_with_retry(state, store, instance_id, &server).await {
            Ok(Some(result)) => format!("Saved {title}, but validation failed: {result}"),
            Ok(None) => format!("Saved and validated {title}."),
            Err(err) => format!(
                "Saved {title}, but validation is still pending: {}",
                classify_nzbget_validation_transport_error(&err)
            ),
        };
    Ok((message, server))
}

async fn nzbget_save_config_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    updates: Vec<NzbgetControlConfigUpdate>,
) -> anyhow::Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("NZBGet instance {instance_id} was not found"))?;
    let updates = dedupe_nzbget_config_updates(
        nzbget_managed_config_updates_for_config(instance.config_json.as_ref())
            .into_iter()
            .chain(updates.into_iter())
            .collect(),
    );
    if nzbget_uses_named_config_storage(instance.config_json.as_ref()) {
        let current_text = state
            .orchestrator
            .read_instance_container_text_file(instance_id, "/config/nzbget.conf")
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("nzbget config file is missing for instance {instance_id}")
            })?;
        let raw_updates = updates
            .iter()
            .map(|update| (update.name.clone(), update.value.clone()))
            .collect::<Vec<_>>();
        if let Some(rendered) = render_nzbget_config_text_updates(&current_text, &raw_updates) {
            state
                .orchestrator
                .replace_instance_container_text_file_and_restart(
                    instance_id,
                    "/config/nzbget.conf",
                    &rendered,
                )
                .await?;
        }
        return Ok(());
    }
    let result =
        nzbget_rpc_for_instance(state, store, instance_id, "saveconfig", json!([updates])).await?;
    if !nzbget_rpc_success(&result) {
        anyhow::bail!("nzbget saveconfig returned unexpected payload: {result}");
    }
    let reload = nzbget_rpc_for_instance(state, store, instance_id, "reload", json!([])).await?;
    if !nzbget_rpc_success(&reload) {
        anyhow::bail!("nzbget reload returned unexpected payload: {reload}");
    }
    Ok(())
}

fn nzbget_uses_named_config_storage(instance_config: Option<&serde_json::Value>) -> bool {
    instance_config
        .and_then(|value| value.get("runtime"))
        .and_then(|value| value.get("config_storage"))
        .and_then(|value| value.get("source_kind"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("named_volume"))
}

async fn nzbget_test_server_connection(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    server: &NzbgetServerEntry,
) -> anyhow::Result<Option<String>> {
    let (username, password) =
        nzbget_resolve_credentials(state, store, instance_id, server).await?;
    if username.trim().is_empty() || password.trim().is_empty() {
        return Ok(Some(
            "Auth failed. Username or password is missing.".to_string(),
        ));
    }
    let cert_level = match nzbget_server_cert_verification(server).as_str() {
        "none" => 0,
        "minimal" => 1,
        _ => 2,
    };
    let result = nzbget_rpc_for_instance(
        state,
        store,
        instance_id,
        "testserver",
        json!([
            server.host,
            nzbget_server_port(server),
            username,
            password,
            server.encryption,
            "",
            30,
            cert_level
        ]),
    )
    .await?;

    Ok(match result {
        serde_json::Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(classify_nzbget_validation_message(trimmed))
            }
        }
        serde_json::Value::Null => None,
        serde_json::Value::Bool(true) => None,
        serde_json::Value::Bool(false) => {
            Some("DNS/host unreachable. NZBGet reported a generic connection failure.".to_string())
        }
        other => Some(format!(
            "NZBGet returned an unexpected validation result: {other}"
        )),
    })
}

async fn nzbget_test_server_connection_with_retry(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    server: &NzbgetServerEntry,
) -> anyhow::Result<Option<String>> {
    const VALIDATION_ATTEMPTS: usize = 4;
    const VALIDATION_RETRY_DELAY_MS: u64 = 350;

    let mut last_error = None;
    for attempt in 0..VALIDATION_ATTEMPTS {
        match nzbget_test_server_connection(state, store, instance_id, server).await {
            Ok(result) => return Ok(result),
            Err(err)
                if attempt + 1 < VALIDATION_ATTEMPTS && nzbget_validation_error_retryable(&err) =>
            {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(VALIDATION_RETRY_DELAY_MS)).await;
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("NZBGet validation failed unexpectedly")))
}

fn nzbget_validation_error_retryable(err: &anyhow::Error) -> bool {
    nzbget_transport_error_retryable_detail(&err.to_string())
}

fn classify_nzbget_validation_message(raw: &str) -> String {
    let trimmed = raw.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("auth")
        || lower.contains("username")
        || lower.contains("password")
        || lower.contains("authorization")
    {
        return format!("Auth failed. {trimmed}");
    }
    if lower.contains("tls")
        || lower.contains("ssl")
        || lower.contains("certificate")
        || lower.contains("handshake")
        || lower.contains("cipher")
    {
        return format!("TLS mismatch. {trimmed}");
    }
    if lower.contains("resolve")
        || lower.contains("host")
            && (lower.contains("unreachable")
                || lower.contains("unknown")
                || lower.contains("not found"))
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("refused")
        || lower.contains("no route")
        || lower.contains("network is unreachable")
    {
        return format!("DNS/host unreachable. {trimmed}");
    }
    if lower.contains("connection")
        && (lower.contains("invalid") || lower.contains("limit") || lower.contains("too many"))
    {
        return format!("Connection limit invalid. {trimmed}");
    }
    trimmed.to_string()
}

fn classify_nzbget_validation_transport_error(err: &anyhow::Error) -> String {
    let detail = err.to_string();
    let lower = detail.to_ascii_lowercase();
    if nzbget_transport_error_retryable_detail(&detail) {
        return "NZBGet did not come back quickly enough to validate the provider. Refresh in a moment to confirm the service is live.".to_string();
    }
    if lower.contains("provider endpoint is missing") {
        return "NZBGet does not have a reachable control endpoint yet.".to_string();
    }
    format!("Validation is temporarily unavailable. {detail}")
}

fn nzbget_transport_error_retryable_detail(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("sending downloader post jsonrpc")
        || lower.contains("tcp connect failed")
        || lower.contains("connection refused")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporarily unavailable")
}

fn log_nzbget_control_availability(message: &str, err: &anyhow::Error) {
    if nzbget_transport_error_retryable_detail(&err.to_string()) {
        tracing::debug!("{message}: {err}");
    } else {
        tracing::warn!("{message}: {err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn arr_context(status_code: &str) -> ExtensionControlContext {
        let instance_id = uuid::Uuid::new_v4();
        ExtensionControlContext {
            extension: crate::db::models::Extension {
                extension_id: "elixir.modules.sonarr".to_string(),
                name: "Sonarr".to_string(),
                version: "1.0.0".to_string(),
                kind: crate::db::models::ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: crate::db::models::ExtensionTrustLevel::Community,
                manifest_json: json!({}),
                package_hash: None,
                installed_at: Utc::now(),
                enabled: true,
            },
            manifest: crate::extensions::manifest::ExtensionManifest {
                id: "elixir.modules.sonarr".to_string(),
                version: "1.0.0".to_string(),
                kind: crate::db::models::ExtensionKind::Module,
                name: "Sonarr".to_string(),
                description: None,
                publisher: None,
                trust: None,
                permissions: Vec::new(),
                provides: Vec::new(),
                requires: Default::default(),
                conflicts: Vec::new(),
                runtime: None,
                backup: None,
                targets: Vec::new(),
                actions: Vec::new(),
                connectors: Vec::new(),
                optional_addons: Vec::new(),
                wants: Vec::new(),
                preferences: None,
                bindings: Vec::new(),
                execution: None,
                policies: None,
                networking: None,
                control_surface: None,
                owner_release: None,
            },
            summary: ExtensionStatusSummaryItem {
                extension_id: "elixir.modules.sonarr".to_string(),
                name: "Sonarr".to_string(),
                version: "1.0.0".to_string(),
                kind: crate::db::models::ExtensionKind::Module,
                trust_level: crate::db::models::ExtensionTrustLevel::Community,
                enabled: true,
                severity: if status_code == "ready" {
                    "ready".to_string()
                } else {
                    "attention".to_string()
                },
                status_code: status_code.to_string(),
                label: status_code.to_string(),
                description: status_code.to_string(),
                primary_action: "fix".to_string(),
                primary_action_label: "Fix".to_string(),
                auto_update: None,
                optional_addons: Vec::new(),
            },
            instances: vec![crate::db::models::ExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.sonarr".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                runtime_version: None,
                rollback_version: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                enabled: true,
            }],
            selected_instance: Some(crate::db::models::ExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.sonarr".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                runtime_version: None,
                rollback_version: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                enabled: true,
            }),
            providers: vec![crate::db::models::Provider {
                provider_id: uuid::Uuid::new_v4(),
                instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: crate::db::models::SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: crate::db::models::ProviderHealthState::Unhealthy,
                last_healthcheck_at: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            }],
            selected_provider: Some(crate::db::models::Provider {
                provider_id: uuid::Uuid::new_v4(),
                instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: crate::db::models::SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: crate::db::models::ProviderHealthState::Unhealthy,
                last_healthcheck_at: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            }),
            control_binding: ExtensionControlBinding::Sonarr,
        }
    }

    #[test]
    fn nzbget_server_is_upstream_sample_matches_stock_defaults() {
        let server = NzbgetServerEntry {
            slot: 1,
            active: true,
            name: String::new(),
            level: 0,
            host: "my.newsserver.com".to_string(),
            encryption: true,
            port: Some(563),
            username: "user".to_string(),
            password: "pass".to_string(),
            connections: Some(8),
            cert_verification: "strict".to_string(),
        };

        assert!(nzbget_server_is_upstream_sample(&server));
    }

    #[test]
    fn nzbget_server_is_upstream_sample_rejects_real_provider_shape() {
        let server = NzbgetServerEntry {
            slot: 1,
            active: true,
            name: "Newshosting".to_string(),
            level: 0,
            host: "news.newshosting.com".to_string(),
            encryption: true,
            port: Some(563),
            username: "reader".to_string(),
            password: "provider-secret".to_string(),
            connections: Some(20),
            cert_verification: "strict".to_string(),
        };

        assert!(!nzbget_server_is_upstream_sample(&server));
    }

    #[test]
    fn nzbget_add_provider_action_uses_setup_info_kind() {
        let action = nzbget_add_server_action();

        assert_eq!(action.id, "add_server");
        assert_eq!(action.label, "Add provider");
        assert_eq!(action.kind, "info");
    }

    #[test]
    fn arr_manager_actions_include_runtime_repair_when_connection_is_broken() {
        let context = arr_context("connection_issue");
        let actions = ArrManagerControlAdapter {
            implementation: "sonarr",
        }
        .build_actions(&context);
        let action_ids = actions
            .into_iter()
            .map(|action| action.id)
            .collect::<Vec<_>>();
        assert_eq!(
            action_ids,
            vec![
                "test_connection".to_string(),
                "repair_connection_issue".to_string()
            ]
        );
    }

    #[test]
    fn arr_manager_actions_skip_runtime_repair_when_service_is_ready() {
        let context = arr_context("ready");
        let actions = ArrManagerControlAdapter {
            implementation: "sonarr",
        }
        .build_actions(&context);
        let action_ids = actions
            .into_iter()
            .map(|action| action.id)
            .collect::<Vec<_>>();
        assert_eq!(action_ids, vec!["test_connection".to_string()]);
    }

    #[test]
    fn nzbget_managed_config_updates_use_runtime_paths_when_runtime_volume_exists() {
        let config_json = json!({
            "runtime": {
                "volumes": [
                    { "container_path": "/config" },
                    { "container_path": "/runtime" }
                ]
            }
        });
        let updates = nzbget_managed_config_updates_for_config(Some(&config_json));
        let as_map = updates
            .into_iter()
            .map(|update| (update.name, update.value))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            as_map.get("InterDir").map(String::as_str),
            Some("/runtime/incomplete")
        );
        assert_eq!(
            as_map.get("NzbDir").map(String::as_str),
            Some("/runtime/nzb")
        );
        assert_eq!(
            as_map.get("QueueDir").map(String::as_str),
            Some("/runtime/queue")
        );
        assert_eq!(
            as_map.get("TempDir").map(String::as_str),
            Some("/runtime/tmp")
        );
    }

    #[test]
    fn dedupe_nzbget_config_updates_keeps_last_assignment() {
        let updates = vec![
            NzbgetControlConfigUpdate::new("Server1.Connections", "32"),
            NzbgetControlConfigUpdate::new("DestDir", "/downloads"),
            NzbgetControlConfigUpdate::new("Server1.Connections", "8"),
        ];
        let deduped = dedupe_nzbget_config_updates(updates);
        let as_map = deduped
            .into_iter()
            .map(|update| (update.name, update.value))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            as_map.get("Server1.Connections").map(String::as_str),
            Some("8")
        );
        assert_eq!(
            as_map.get("DestDir").map(String::as_str),
            Some("/downloads")
        );
    }

    #[test]
    fn restore_backup_action_includes_snapshot_id_param() {
        let snapshot_id = uuid::Uuid::new_v4();
        let action = build_restore_backup_action(snapshot_id);
        assert_eq!(action.id, "restore_backup");
        assert_eq!(
            action
                .params
                .as_ref()
                .and_then(|value| value.get("snapshotId"))
                .and_then(|value| value.as_str())
                .map(str::to_string),
            Some(snapshot_id.to_string())
        );
    }
}

async fn build_qbittorrent_queue_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let value = request_downloader_json(
        state,
        store,
        context,
        ReqwestMethod::GET,
        "api/v2/torrents/info",
        None,
    )
    .await?;
    let mut torrents: Vec<QbittorrentControlTorrent> =
        serde_json::from_value(value).context("parsing qbittorrent queue")?;
    torrents.retain(|torrent| !torrent.hash.trim().is_empty());
    torrents.sort_by(|left, right| {
        qbittorrent_queue_rank(left)
            .cmp(&qbittorrent_queue_rank(right))
            .then_with(|| {
                left.name
                    .to_ascii_lowercase()
                    .cmp(&right.name.to_ascii_lowercase())
            })
    });
    torrents.truncate(12);

    let entities = torrents
        .iter()
        .map(build_qbittorrent_queue_entity)
        .collect::<Vec<_>>();

    Ok(Some(ExtensionControlSection {
        id: "queue".to_string(),
        title: "Queue".to_string(),
        description:
            "Live torrent activity from qBittorrent. Pause, resume, recheck, or remove items without leaving Elixir."
                .to_string(),
        policy: Some(super::control_policy_observed(
            "Queue state is live-read from qBittorrent. Elixir reflects it but does not treat it as managed configuration.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![
            ExtensionControlAction {
                id: "pause_all".to_string(),
                label: "Pause all".to_string(),
                description: "Pause all torrents in qBittorrent.".to_string(),
                kind: "secondary".to_string(),
                params: None,
                confirm_text: None,
                navigate_extension_id: None,
                navigate_view: None,
                open_url: None,
                required_fields: Vec::new(),
                secret_keys: Vec::new(),
                secret_scope_instance_id: None,
            },
            ExtensionControlAction {
                id: "resume_all".to_string(),
                label: "Resume all".to_string(),
                description: "Resume all torrents in qBittorrent.".to_string(),
                kind: "secondary".to_string(),
                params: None,
                confirm_text: None,
                navigate_extension_id: None,
                navigate_view: None,
                open_url: None,
                required_fields: Vec::new(),
                secret_keys: Vec::new(),
                secret_scope_instance_id: None,
            },
        ],
    }))
}

fn build_qbittorrent_queue_entity(torrent: &QbittorrentControlTorrent) -> ExtensionControlEntity {
    let title = torrent_title(&torrent.name, "Torrent");
    let state = torrent
        .state
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let mut subtitle_parts = vec![humanize_queue_state(&state)];
    if let Some(progress) = torrent.progress {
        subtitle_parts.push(format_percent(progress));
    }
    let subtitle = Some(subtitle_parts.join(" · "));

    let mut details = Vec::new();
    if let Some(category) = torrent
        .category
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        details.push(format!("Category: {category}"));
    }
    let total_size = torrent.total_size.or(torrent.size);
    if let Some(total_size) = total_size.filter(|value| *value > 0) {
        details.push(format!("Size: {}", format_bytes(total_size)));
    }
    if let Some(remaining) = torrent.amount_left.filter(|value| *value > 0) {
        details.push(format!("Remaining: {}", format_bytes(remaining)));
    }
    let mut rate_parts = Vec::new();
    if let Some(rate) = torrent.dlspeed.filter(|value| *value > 0) {
        rate_parts.push(format!("Down {}", format_rate_bps(rate)));
    }
    if let Some(rate) = torrent.upspeed.filter(|value| *value > 0) {
        rate_parts.push(format!("Up {}", format_rate_bps(rate)));
    }
    if !rate_parts.is_empty() {
        details.push(rate_parts.join(" · "));
    }
    if let Some(eta) = torrent.eta.filter(|value| *value > 0) {
        details.push(format!("ETA: {}", format_eta_seconds(eta)));
    }

    let mut actions = Vec::new();
    if qbittorrent_can_resume(&state) {
        actions.push(control_entity_action(
            "resume_item",
            "Resume",
            "Resume this torrent in qBittorrent.",
            "secondary",
            json!({ "itemId": torrent.hash }),
            None,
        ));
    } else {
        actions.push(control_entity_action(
            "pause_item",
            "Pause",
            "Pause this torrent in qBittorrent.",
            "secondary",
            json!({ "itemId": torrent.hash }),
            None,
        ));
    }
    actions.push(control_entity_action(
        "recheck_item",
        "Recheck",
        "Recheck this torrent in qBittorrent.",
        "secondary",
        json!({ "itemId": torrent.hash }),
        None,
    ));
    actions.push(control_entity_action(
        "remove_item",
        "Remove",
        "Remove this torrent from qBittorrent while leaving downloaded files in place.",
        "danger",
        json!({ "itemId": torrent.hash }),
        Some(format!("Remove {} from qBittorrent?", title)),
    ));

    ExtensionControlEntity {
        id: torrent.hash.clone(),
        title,
        subtitle,
        details,
        actions,
    }
}

async fn build_nzbget_queue_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let groups_value = nzbget_rpc(state, store, context, "listgroups", json!([0])).await?;
    let mut groups: Vec<NzbgetControlGroup> =
        serde_json::from_value(groups_value).context("parsing nzbget groups")?;
    groups.retain(|group| group.nzb_id > 0);
    groups.sort_by(|left, right| {
        nzbget_queue_rank(left)
            .cmp(&nzbget_queue_rank(right))
            .then_with(|| {
                nzbget_group_title(left)
                    .to_ascii_lowercase()
                    .cmp(&nzbget_group_title(right).to_ascii_lowercase())
            })
    });
    groups.truncate(12);

    let entities = groups
        .iter()
        .map(build_nzbget_queue_entity)
        .collect::<Vec<_>>();

    Ok(Some(ExtensionControlSection {
        id: "queue".to_string(),
        title: "Queue".to_string(),
        description:
            "Live Usenet queue activity from NZBGet. Pause, resume, or remove jobs without leaving Elixir."
                .to_string(),
        policy: Some(super::control_policy_observed(
            "Queue state is live-read from NZBGet. Elixir reflects it but does not treat it as managed configuration.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![
            ExtensionControlAction {
                id: "pause_all".to_string(),
                label: "Pause downloads".to_string(),
                description: "Pause NZBGet downloads.".to_string(),
                kind: "secondary".to_string(),
                params: None,
                confirm_text: None,
                navigate_extension_id: None,
                navigate_view: None,
                open_url: None,
                required_fields: Vec::new(),
                secret_keys: Vec::new(),
                secret_scope_instance_id: None,
            },
            ExtensionControlAction {
                id: "resume_all".to_string(),
                label: "Resume downloads".to_string(),
                description: "Resume NZBGet downloads.".to_string(),
                kind: "secondary".to_string(),
                params: None,
                confirm_text: None,
                navigate_extension_id: None,
                navigate_view: None,
                open_url: None,
                required_fields: Vec::new(),
                secret_keys: Vec::new(),
                secret_scope_instance_id: None,
            },
        ],
    }))
}

fn build_nzbget_queue_entity(group: &NzbgetControlGroup) -> ExtensionControlEntity {
    let title = nzbget_group_title(group);
    let state = group
        .status
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let subtitle = Some({
        let mut parts = vec![humanize_queue_state(&state)];
        if let Some(priority) = group.priority {
            parts.push(format!("Priority {priority}"));
        }
        parts.join(" · ")
    });

    let mut details = Vec::new();
    if let Some(category) = group
        .category
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        details.push(format!("Category: {category}"));
    }
    if let Some(size) =
        combine_size_parts(group.file_size_hi, group.file_size_lo).filter(|v| *v > 0)
    {
        details.push(format!("Size: {}", format_bytes(size)));
    }
    if let Some(remaining) =
        combine_size_parts(group.remaining_size_hi, group.remaining_size_lo).filter(|v| *v > 0)
    {
        details.push(format!("Remaining: {}", format_bytes(remaining)));
    }
    if let Some(downloaded) =
        combine_size_parts(group.downloaded_size_hi, group.downloaded_size_lo).filter(|v| *v > 0)
    {
        details.push(format!("Downloaded: {}", format_bytes(downloaded)));
    }

    let mut actions = Vec::new();
    if nzbget_can_resume(&state) {
        actions.push(control_entity_action(
            "resume_item",
            "Resume",
            "Resume this NZB in NZBGet.",
            "secondary",
            json!({ "itemId": group.nzb_id.to_string() }),
            None,
        ));
    } else {
        actions.push(control_entity_action(
            "pause_item",
            "Pause",
            "Pause this NZB in NZBGet.",
            "secondary",
            json!({ "itemId": group.nzb_id.to_string() }),
            None,
        ));
    }
    actions.push(control_entity_action(
        "remove_item",
        "Remove",
        "Remove this NZB from the NZBGet queue.",
        "danger",
        json!({ "itemId": group.nzb_id.to_string() }),
        Some(format!("Remove {} from NZBGet?", title)),
    ));

    ExtensionControlEntity {
        id: group.nzb_id.to_string(),
        title,
        subtitle,
        details,
        actions,
    }
}

fn control_entity_action(
    id: &str,
    label: &str,
    description: &str,
    kind: &str,
    params: serde_json::Value,
    confirm_text: Option<String>,
) -> ExtensionControlAction {
    ExtensionControlAction {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        kind: kind.to_string(),
        params: Some(params),
        confirm_text,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: None,
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    }
}

fn qbittorrent_queue_rank(torrent: &QbittorrentControlTorrent) -> u8 {
    let state = torrent
        .state
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if qbittorrent_is_error_state(&state) {
        3
    } else if qbittorrent_is_active_state(&state) {
        0
    } else if state.contains("queued") {
        1
    } else if qbittorrent_can_resume(&state) {
        2
    } else {
        4
    }
}

fn qbittorrent_is_active_state(state: &str) -> bool {
    matches!(
        state,
        "uploading"
            | "stalledup"
            | "checkingup"
            | "forcedup"
            | "allocating"
            | "downloading"
            | "metadl"
            | "stalleddl"
            | "forceddl"
            | "checkingdl"
            | "checkingresume"
            | "moving"
    )
}

fn qbittorrent_is_error_state(state: &str) -> bool {
    state == "error" || state == "missingfiles"
}

fn qbittorrent_can_resume(state: &str) -> bool {
    state.contains("paused") || state.contains("queued")
}

fn nzbget_queue_rank(group: &NzbgetControlGroup) -> u8 {
    let state = group
        .status
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if state.contains("failure") || state.contains("warning") {
        3
    } else if matches!(
        state.as_str(),
        "downloading"
            | "fetching"
            | "checking"
            | "repairing"
            | "extracting"
            | "moving"
            | "running"
            | "processing"
    ) {
        0
    } else if state == "queued" {
        1
    } else if nzbget_can_resume(&state) {
        2
    } else {
        4
    }
}

fn nzbget_can_resume(state: &str) -> bool {
    state == "paused"
}

fn torrent_title(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn nzbget_group_title(group: &NzbgetControlGroup) -> String {
    group
        .nzb_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            group
                .nzb_filename
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .map(str::to_string)
        .unwrap_or_else(|| format!("NZB {}", group.nzb_id))
}

fn humanize_queue_state(state: &str) -> String {
    if state.trim().is_empty() {
        return "Unknown".to_string();
    }
    state
        .replace("dl", " download")
        .replace("up", " upload")
        .split(|ch: char| ch == '_' || ch == '-')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!(
                    "{}{}",
                    first.to_uppercase().collect::<String>(),
                    chars.as_str().to_ascii_lowercase()
                ),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_percent(progress: f64) -> String {
    let percent = (progress * 100.0).clamp(0.0, 100.0);
    format!("{percent:.0}%")
}

fn format_rate_bps(rate: u64) -> String {
    format!("{}/s", format_bytes(rate))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else if value >= 100.0 {
        format!("{value:.0} {}", UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
    }
}

fn format_eta_seconds(seconds: i64) -> String {
    if seconds <= 0 {
        return "Soon".to_string();
    }
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m")
    } else {
        format!("{seconds}s")
    }
}

fn combine_size_parts(hi: Option<u64>, lo: Option<u64>) -> Option<u64> {
    match (hi, lo) {
        (Some(hi), Some(lo)) => Some((hi << 32) | lo),
        (Some(hi), None) => Some(hi << 32),
        (None, Some(lo)) => Some(lo),
        (None, None) => None,
    }
}

fn control_action_item_id(params: &HashMap<String, serde_json::Value>) -> anyhow::Result<String> {
    params
        .get("itemId")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("itemId is required"))
}

async fn request_downloader_builder(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    method: ReqwestMethod,
    path: &str,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    request_downloader_builder_for_instance(state, store, instance.instance_id, method, path).await
}

async fn request_downloader_builder_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    method: ReqwestMethod,
    path: &str,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let target = super::resolve_extension_ui_proxy_target(state, store, instance_id).await?;
    let client = super::build_extension_ui_proxy_client()?;
    let upstream_url = super::build_extension_ui_proxy_url(&target.base_url, path, None)?;
    super::build_extension_ui_upstream_request(
        &client,
        &target,
        method,
        upstream_url,
        &AxumHeaderMap::new(),
    )
    .await
}

async fn request_downloader_json(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    method: ReqwestMethod,
    path: &str,
    body: Option<serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let mut request =
        request_downloader_builder(state, store, context, method.clone(), path).await?;
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("sending downloader {} {path}", method.as_str()))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading downloader {} {path} response", method.as_str()))?;
    if !status.is_success() {
        let detail = describe_response_body(&bytes);
        anyhow::bail!(
            "downloader {} {path} failed ({}): {detail}",
            method.as_str(),
            status
        );
    }
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing downloader {} {path} response", method.as_str()))
}

pub(super) async fn load_qbittorrent_preferences(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<serde_json::Value> {
    request_downloader_json(
        state,
        store,
        context,
        ReqwestMethod::GET,
        "/api/v2/app/preferences",
        None,
    )
    .await
}

pub(super) async fn load_qbittorrent_categories(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<serde_json::Value> {
    request_downloader_json(
        state,
        store,
        context,
        ReqwestMethod::GET,
        "/api/v2/torrents/categories",
        None,
    )
    .await
}

async fn request_downloader_json_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    method: ReqwestMethod,
    path: &str,
    body: Option<serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    super::request_instance_service_json(state, store, instance_id, method, path, body).await
}

pub(super) async fn load_nzbget_live_config_map(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<BTreeMap<String, String>> {
    let value = nzbget_rpc(state, store, context, "config", json!([])).await?;
    parse_nzbget_control_config_map(value)
}

fn parse_nzbget_control_config_map(
    value: serde_json::Value,
) -> anyhow::Result<BTreeMap<String, String>> {
    let items: Vec<NzbgetControlConfigItem> =
        serde_json::from_value(value).context("parsing nzbget config items")?;
    Ok(items
        .into_iter()
        .map(|item| (item.name, item.value))
        .collect::<BTreeMap<_, _>>())
}

async fn request_downloader_form(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    path: &str,
    fields: &HashMap<String, String>,
) -> anyhow::Result<()> {
    let request =
        request_downloader_builder(state, store, context, ReqwestMethod::POST, path).await?;
    let response = request
        .form(fields)
        .send()
        .await
        .with_context(|| format!("sending downloader POST {path}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading downloader POST {path} response"))?;
    if !status.is_success() {
        let detail = describe_response_body(&bytes);
        anyhow::bail!("downloader POST {path} failed ({status}): {detail}");
    }
    Ok(())
}

async fn request_downloader_empty(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    method: ReqwestMethod,
    path: &str,
) -> anyhow::Result<()> {
    let request = request_downloader_builder(state, store, context, method.clone(), path).await?;
    let response = request
        .send()
        .await
        .with_context(|| format!("sending downloader {} {path}", method.as_str()))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading downloader {} {path} response", method.as_str()))?;
    if !status.is_success() {
        let detail = describe_response_body(&bytes);
        anyhow::bail!(
            "downloader {} {path} failed ({}): {detail}",
            method.as_str(),
            status
        );
    }
    Ok(())
}

fn describe_response_body(body: &[u8]) -> String {
    match std::str::from_utf8(body) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => "<empty response>".to_string(),
    }
}

async fn qbittorrent_run_global_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    action_id: &str,
) -> anyhow::Result<String> {
    match action_id {
        "pause_all" => {
            let mut fields = HashMap::new();
            fields.insert("hashes".to_string(), "all".to_string());
            request_downloader_form(state, store, context, "api/v2/torrents/pause", &fields)
                .await?;
            Ok("Paused all qBittorrent torrents.".to_string())
        }
        "resume_all" => {
            let mut fields = HashMap::new();
            fields.insert("hashes".to_string(), "all".to_string());
            request_downloader_form(state, store, context, "api/v2/torrents/resume", &fields)
                .await?;
            Ok("Resumed all qBittorrent torrents.".to_string())
        }
        _ => anyhow::bail!("unsupported qBittorrent action '{action_id}'"),
    }
}

async fn qbittorrent_run_item_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    action_id: &str,
    hash: &str,
) -> anyhow::Result<String> {
    let mut fields = HashMap::new();
    fields.insert("hashes".to_string(), hash.to_string());
    match action_id {
        "pause_item" => {
            request_downloader_form(state, store, context, "api/v2/torrents/pause", &fields)
                .await?;
            Ok("Paused torrent in qBittorrent.".to_string())
        }
        "resume_item" => {
            request_downloader_form(state, store, context, "api/v2/torrents/resume", &fields)
                .await?;
            Ok("Resumed torrent in qBittorrent.".to_string())
        }
        "recheck_item" => {
            request_downloader_form(state, store, context, "api/v2/torrents/recheck", &fields)
                .await?;
            Ok("Requested a qBittorrent recheck.".to_string())
        }
        "remove_item" => {
            fields.insert("deleteFiles".to_string(), "false".to_string());
            request_downloader_form(state, store, context, "api/v2/torrents/delete", &fields)
                .await?;
            Ok("Removed torrent from qBittorrent.".to_string())
        }
        _ => anyhow::bail!("unsupported qBittorrent item action '{action_id}'"),
    }
}

async fn nzbget_rpc(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let payload = request_downloader_json(
        state,
        store,
        context,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": method,
            "params": params,
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        anyhow::bail!("nzbget {method} returned error: {error}");
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("nzbget {method} response missing result"))
}

async fn nzbget_rpc_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let payload = request_downloader_json_for_instance(
        state,
        store,
        instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": method,
            "params": params,
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        anyhow::bail!("nzbget {method} returned error: {error}");
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("nzbget {method} response missing result"))
}

async fn nzbget_run_global_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    action_id: &str,
) -> anyhow::Result<String> {
    match action_id {
        "pause_all" => {
            let result = nzbget_rpc(
                state,
                store,
                context,
                "pausedownload",
                serde_json::Value::Array(Vec::new()),
            )
            .await?;
            if !nzbget_rpc_success(&result) {
                anyhow::bail!("nzbget pausedownload did not report success");
            }
            Ok("Paused NZBGet downloads.".to_string())
        }
        "resume_all" => {
            let result = nzbget_rpc(
                state,
                store,
                context,
                "resumedownload",
                serde_json::Value::Array(Vec::new()),
            )
            .await?;
            if !nzbget_rpc_success(&result) {
                anyhow::bail!("nzbget resumedownload did not report success");
            }
            Ok("Resumed NZBGet downloads.".to_string())
        }
        _ => anyhow::bail!("unsupported NZBGet action '{action_id}'"),
    }
}

async fn nzbget_run_item_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    action_id: &str,
    item_id: &str,
) -> anyhow::Result<String> {
    let group_id = item_id.parse::<i64>().context("parsing NZBGet group id")?;
    let command = match action_id {
        "pause_item" => "GroupPause",
        "resume_item" => "GroupResume",
        "remove_item" => "GroupDelete",
        _ => anyhow::bail!("unsupported NZBGet item action '{action_id}'"),
    };
    let result = nzbget_rpc(
        state,
        store,
        context,
        "editqueue",
        json!([command, "", [group_id]]),
    )
    .await?;
    if !nzbget_rpc_success(&result) {
        anyhow::bail!("nzbget editqueue {command} did not report success");
    }
    let message = match action_id {
        "pause_item" => "Paused NZB in NZBGet.",
        "resume_item" => "Resumed NZB in NZBGet.",
        "remove_item" => "Removed NZB from NZBGet.",
        _ => unreachable!(),
    };
    Ok(message.to_string())
}

fn nzbget_rpc_success(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(ok) => *ok,
        serde_json::Value::Number(number) => number.as_u64() == Some(1),
        serde_json::Value::Null => true,
        serde_json::Value::String(text) => {
            matches!(
                text.trim().to_ascii_lowercase().as_str(),
                "true" | "ok" | "1"
            )
        }
        _ => false,
    }
}
