use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::control_contract::{
    ExtensionControlProvider, GenericManifestControlProvider, UnsupportedControlProvider,
    control_notice, control_policy_managed, control_policy_observed, control_policy_seeded,
};
use super::*;
use crate::acquisition::stream_preflight::{
    FAILURE_ACCOUNT_REQUIRED, FAILURE_CAPTCHA_OR_BROWSER_REQUIRED, FAILURE_DRM_OR_LICENSE_REQUIRED,
    FAILURE_HOSTER_RESOLVER_MISSING, FAILURE_INVALID_CANDIDATE_SHAPE,
    FAILURE_MATERIALIZATION_PREFLIGHT_FAILED, FAILURE_NETWORK_BLOCKED,
    FAILURE_SOURCE_RETURNED_NON_MEDIA_RESPONSE, FAILURE_UNSAFE_URL, StreamCandidatePreflightReport,
    preflight_stream_candidate,
};
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
use crate::extensions::manifest::ManifestRuntimeSecurity;
use crate::extensions::nuvio_registry::{
    NuvioRegistryClient, NuvioRegistryFetchConfig, NuvioRegistryStoreInput, PRISM_EXTENSION_ID,
    PRISM_RECOMMENDED_REGISTRY_KEY, persist_nuvio_registry_snapshot,
    record_prism_source_registry_tombstone, restore_prism_recommended_source_pack_for_instance,
};
use crate::extensions::prism_policy::{
    PRISM_EGRESS_POLICY_VERSION, PRISM_SANDBOX_PROFILE_VERSION, prism_certification_policy_version,
};
use crate::extensions::source_artifacts::{
    install_source_module_artifact, remove_source_module_artifacts,
    uninstall_source_module_artifacts,
};
use crate::extensions::store::{
    ExtensionSourceCertificationJob, ExtensionSourceModule, ExtensionSourceModuleCertification,
    ExtensionSourceModuleVersion, ExtensionSourceRegistry, NewExtensionSourceCertificationJob,
    NewExtensionSourceHealthEvent, NewExtensionSourceModuleCertification,
};
use crate::runtime::model::ContainerRuntimeState;

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

        let repaired_orphans = store
            .delete_orphan_source_modules_for_instance(instance.instance_id)
            .await?;
        if repaired_orphans > 0 {
            tracing::info!(
                instance_id = %instance.instance_id,
                repaired_orphans,
                "removed orphaned source modules before rendering CloudStream controls"
            );
        }

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
                    .set_source_registry_trust(registry_id, registry.trust_class.as_str(), true)
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

const PRISM_PROVIDER_SCHEMA_VERSION: u32 = 1;
const PRISM_PROVIDER_SEARCH_PATH: &str = "search";
const PRISM_RUNTIME_SMOKE_TIMEOUT: Duration = Duration::from_secs(40);
const PRISM_RUNTIME_SMOKE_PROVIDER_TIMEOUT_MS: u64 = 30_000;
const PRISM_CERTIFICATION_MAX_PREFLIGHT_CANDIDATES: usize = 5;
const PRISM_AUTO_CERTIFICATION_DEFAULT_MAX_MODULES: usize = 50;
const PRISM_MARKETPLACE_POLICY_CONFIG_KEY: &str = "prismMarketplacePolicy";

#[async_trait::async_trait]
impl ExtensionControlProvider for PrismControlAdapter {
    async fn build_sections(
        &self,
        state: &AppState,
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

        let repaired_orphans = store
            .delete_orphan_source_modules_for_instance(instance.instance_id)
            .await?;
        if repaired_orphans > 0 {
            tracing::info!(
                instance_id = %instance.instance_id,
                repaired_orphans,
                "removed orphaned source modules before rendering Prism controls"
            );
        }

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
        let certifications = store
            .list_latest_source_module_certifications(instance.instance_id)
            .await?;
        let certification_by_module = certifications
            .iter()
            .map(|certification| (certification.source_module_id, certification))
            .collect::<BTreeMap<_, _>>();
        let certification_jobs = store
            .list_latest_source_certification_jobs(instance.instance_id)
            .await?;
        let job_by_module = certification_jobs
            .iter()
            .filter_map(|job| job.source_module_id.map(|module_id| (module_id, job)))
            .collect::<BTreeMap<_, _>>();
        let marketplace_policy = prism_marketplace_policy(instance);

        let mut sections = vec![build_prism_recommended_section(
            context,
            instance,
            &registries,
            &modules,
            &certification_by_module,
            &job_by_module,
        )];
        sections.push(build_prism_runtime_isolation_section(state, context, instance).await?);
        sections.push(build_nuvio_ready_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        ));
        if let Some(section) = build_prism_attention_sources_section(
            store,
            instance,
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        )
        .await?
        {
            sections.push(section);
        }
        sections.push(build_nuvio_disabled_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        ));
        sections.push(build_nuvio_available_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        ));
        sections.push(build_nuvio_repositories_section(
            &registries,
            &modules,
            &job_by_module,
        ));
        sections.push(build_nuvio_version_pins_section(store, &modules, &registry_by_id).await?);
        sections.push(build_prism_policy_section(instance, &marketplace_policy));
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
            prism_certify_enabled_sources_action(),
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
        let source_pack_allowed = [
            "recommendedPackAutoEnable",
            "recommendedPackExecutableUpdates",
            "customRepoMetadataRefresh",
            "customRepoExecutableTrustRequired",
            "rollbackPinAlwaysAvailable",
        ];
        let marketplace_allowed = [
            "preferredLanguageTags",
            "unknownLanguageBehavior",
            "autoCertifyTrustedRepositories",
            "autoCertifyCustomRepositories",
            "retainFailedArtifacts",
            "maxAutoCertifyModulesPerRepo",
            "maxConcurrentCertificationJobs",
        ];
        for key in values.keys() {
            if !source_pack_allowed.iter().any(|allowed| allowed == key)
                && !marketplace_allowed.iter().any(|allowed| allowed == key)
            {
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
        for key in source_pack_allowed {
            if let Some(value) = values.get(key).and_then(serde_json::Value::as_bool) {
                policy.insert(key.to_string(), serde_json::Value::Bool(value));
            }
        }
        config.insert(
            "sourcePackPolicy".to_string(),
            serde_json::Value::Object(policy),
        );
        let mut marketplace_policy = config
            .get(PRISM_MARKETPLACE_POLICY_CONFIG_KEY)
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(value) = values.get("preferredLanguageTags") {
            let languages = prism_parse_preferred_language_setting(value)?;
            marketplace_policy.insert("preferredLanguageTags".to_string(), json!(languages));
        }
        if let Some(value) = values
            .get("unknownLanguageBehavior")
            .and_then(serde_json::Value::as_str)
        {
            let value = value.trim();
            if !matches!(value, "certify" | "skip") {
                anyhow::bail!("unknownLanguageBehavior must be certify or skip");
            }
            marketplace_policy.insert(
                "unknownLanguageBehavior".to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        if let Some(value) = values
            .get("autoCertifyCustomRepositories")
            .and_then(serde_json::Value::as_str)
        {
            let value = value.trim();
            if !matches!(value, "after_trust" | "never") {
                anyhow::bail!("autoCertifyCustomRepositories must be after_trust or never");
            }
            marketplace_policy.insert(
                "autoCertifyCustomRepositories".to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        for key in ["autoCertifyTrustedRepositories", "retainFailedArtifacts"] {
            if let Some(value) = values.get(key).and_then(serde_json::Value::as_bool) {
                marketplace_policy.insert(key.to_string(), serde_json::Value::Bool(value));
            }
        }
        for key in [
            "maxAutoCertifyModulesPerRepo",
            "maxConcurrentCertificationJobs",
        ] {
            if let Some(value) = values.get(key).and_then(serde_json::Value::as_u64) {
                if value == 0 || value > 500 {
                    anyhow::bail!("{key} must be between 1 and 500");
                }
                marketplace_policy.insert(key.to_string(), json!(value));
            }
        }
        config.insert(
            PRISM_MARKETPLACE_POLICY_CONFIG_KEY.to_string(),
            serde_json::Value::Object(marketplace_policy),
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
                let summary = restore_prism_recommended_source_pack_for_instance(
                    store,
                    instance.instance_id,
                    None,
                    Some(&state.settings.extensions.storage_root),
                )
                .await?;
                let mut message = format!(
                    "Recommended Prism source pack refreshed: {} module(s), {} version(s), {} disabled.",
                    summary.modules, summary.versions, summary.disabled_modules
                );
                if let Some(registry) = store
                    .list_source_registries(Some(instance.instance_id))
                    .await?
                    .into_iter()
                    .find(|registry| registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY)
                {
                    let certification_summary = prism_enqueue_repository_certification_jobs(
                        store,
                        instance,
                        registry.registry_id,
                        "refresh_recommended_pack",
                        "recommended_repository_refresh",
                        false,
                    )
                    .await?;
                    if certification_summary.processed > 0 {
                        prism_spawn_certification_worker(
                            state.clone(),
                            context.clone(),
                            instance.instance_id,
                        );
                    }
                    message.push(' ');
                    message.push_str(&certification_summary.message(&registry.display_name));
                }
                Ok(message)
            }
            "add_custom_repo" => {
                let (registry_id, message) = nuvio_add_custom_repo(store, instance, params).await?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                let isolation_block = prism_custom_repository_isolation_block(
                    state,
                    context,
                    instance,
                    &registry,
                    registry.trusted_for_executable_updates,
                )
                .await?;
                if let Some(message_suffix) = isolation_block {
                    return Ok(format!("{message} {message_suffix}"));
                }
                let certification_summary = prism_enqueue_repository_certification_jobs(
                    store,
                    instance,
                    registry_id,
                    "add_custom_repo",
                    "repository_added",
                    registry.trusted_for_executable_updates,
                )
                .await?;
                if certification_summary.processed > 0 {
                    prism_spawn_certification_worker(
                        state.clone(),
                        context.clone(),
                        instance.instance_id,
                    );
                }
                Ok(format!(
                    "{message} {}",
                    certification_summary.message(&registry.display_name)
                ))
            }
            "certify_enabled_sources" | "certifyEnabledSources" => {
                let modules = store
                    .list_source_modules(Some(instance.instance_id), None)
                    .await?
                    .into_iter()
                    .filter(|module| {
                        (module.ecosystem == "nuvio" || module.ecosystem == "stremio")
                            && (module.enabled || module.installed)
                    })
                    .collect::<Vec<_>>();
                let mut certified = 0usize;
                let mut degraded = 0usize;
                let mut blocked = 0usize;
                for module in modules {
                    if module.unsupported || module.account_required {
                        continue;
                    }
                    let outcome =
                        smoke_prism_source_module_runtime(store, context, instance, &module)
                            .await?;
                    if prism_certification_is_eligible(&outcome.status) {
                        if outcome.status == "degraded" {
                            degraded += 1;
                        } else {
                            certified += 1;
                        }
                        store
                            .set_source_module_enabled_state(
                                module.source_module_id,
                                module.enabled,
                                &outcome.health_state,
                                outcome.failure_class.as_deref(),
                            )
                            .await?;
                    } else {
                        blocked += 1;
                        store
                            .set_source_module_enabled_state(
                                module.source_module_id,
                                false,
                                &outcome.health_state,
                                Some(&outcome.reason),
                            )
                            .await?;
                    }
                }
                Ok(format!(
                    "Prism certification finished: {certified} certified, {degraded} degraded, {blocked} blocked."
                ))
            }
            "cancel_certification" | "cancelCertification" => {
                let registry_id = params
                    .get("registryId")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| {
                        Uuid::parse_str(value.trim()).with_context(|| "parsing registryId")
                    })
                    .transpose()?;
                let source_module_id = params
                    .get("sourceModuleId")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| {
                        Uuid::parse_str(value.trim()).with_context(|| "parsing sourceModuleId")
                    })
                    .transpose()?;
                let cancelled = store
                    .cancel_source_certification_jobs(
                        instance.instance_id,
                        registry_id,
                        source_module_id,
                        "cancelled by user",
                    )
                    .await?;
                Ok(format!("Cancelled {cancelled} Prism certification job(s)."))
            }
            "get_certification_report" | "getCertificationReport" => {
                let report =
                    prism_certification_report(store, instance.instance_id, params).await?;
                Ok(report.to_string())
            }
            "set_certification_policy" | "setCertificationPolicy" => {
                let allowed = [
                    "recommendedPackAutoEnable",
                    "recommendedPackExecutableUpdates",
                    "customRepoMetadataRefresh",
                    "customRepoExecutableTrustRequired",
                    "rollbackPinAlwaysAvailable",
                    "preferredLanguageTags",
                    "unknownLanguageBehavior",
                    "autoCertifyTrustedRepositories",
                    "autoCertifyCustomRepositories",
                    "retainFailedArtifacts",
                    "maxAutoCertifyModulesPerRepo",
                    "maxConcurrentCertificationJobs",
                ];
                let values = params
                    .iter()
                    .filter_map(|(key, value)| {
                        allowed
                            .iter()
                            .any(|allowed| allowed == key)
                            .then(|| (key.clone(), value.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                if values.is_empty() {
                    return Ok("No Prism certification policy changes supplied.".to_string());
                }
                self.update_settings(state, store, context, &values).await?;
                Ok("Prism certification policy updated.".to_string())
            }
            "refresh_custom_repo" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let (registry_id, message) =
                    nuvio_refresh_registry(store, instance, registry_id).await?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                prism_ensure_custom_repository_isolation(
                    state,
                    context,
                    instance,
                    &registry,
                    registry.trusted_for_executable_updates,
                )
                .await?;
                let certification_summary = prism_enqueue_repository_certification_jobs(
                    store,
                    instance,
                    registry_id,
                    "refresh_custom_repo",
                    "repository_refreshed",
                    false,
                )
                .await?;
                if certification_summary.processed > 0 {
                    prism_spawn_certification_worker(
                        state.clone(),
                        context.clone(),
                        instance.instance_id,
                    );
                }
                Ok(format!(
                    "{message} {}",
                    certification_summary.message(&registry.display_name)
                ))
            }
            "certify_repository" | "certifyRepository" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                prism_ensure_custom_repository_isolation(state, context, instance, &registry, true)
                    .await?;
                let summary = prism_enqueue_repository_certification_jobs(
                    store,
                    instance,
                    registry_id,
                    "certify_repository",
                    "manual_repository_certification",
                    true,
                )
                .await?;
                if summary.processed > 0 {
                    prism_spawn_certification_worker(
                        state.clone(),
                        context.clone(),
                        instance.instance_id,
                    );
                }
                Ok(summary.message(&registry.display_name))
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
                    .set_source_registry_trust(registry_id, registry.trust_class.as_str(), true)
                    .await?;
                let trusted_registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                let isolation_block = prism_custom_repository_isolation_block(
                    state,
                    context,
                    instance,
                    &trusted_registry,
                    true,
                )
                .await?;
                if let Some(message_suffix) = isolation_block {
                    return Ok(format!(
                        "Trusted '{}'. Certification was not started. {}",
                        trusted_registry.display_name, message_suffix
                    ));
                }
                let summary = prism_enqueue_repository_certification_jobs(
                    store,
                    instance,
                    registry_id,
                    "trust_custom_repo",
                    "repository_trusted",
                    true,
                )
                .await?;
                if summary.processed > 0 {
                    prism_spawn_certification_worker(
                        state.clone(),
                        context.clone(),
                        instance.instance_id,
                    );
                }
                Ok(format!(
                    "Trusted '{}'. Certification started where eligible. Runnable modules will be enabled automatically; failures stay disabled with diagnostics. {}",
                    registry.display_name,
                    summary.message(&registry.display_name)
                ))
            }
            "enable_registry" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                store
                    .set_source_registry_enabled_state(registry_id, true, true)
                    .await?;
                Ok(format!(
                    "Enabled source registry '{}'. Scrapers remain disabled until certified and explicitly enabled.",
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
            "remove_registry" | "removeRepository" => {
                let registry_id = cloudstream_param_uuid(params, "registryId")?;
                let registry =
                    nuvio_find_registry(store, instance.instance_id, registry_id).await?;
                record_prism_source_registry_tombstone(store, &registry, "removed_by_user").await?;
                let modules = store.list_source_modules(None, Some(registry_id)).await?;
                let cancelled = store
                    .cancel_source_certification_jobs(
                        instance.instance_id,
                        Some(registry_id),
                        None,
                        "repository removed",
                    )
                    .await?;
                let mut removed_artifacts = 0usize;
                for module in &modules {
                    removed_artifacts += uninstall_source_module_artifacts(
                        store,
                        module,
                        "source repository removed",
                    )
                    .await?;
                }
                let deleted = store.delete_source_registry(registry_id).await?;
                if deleted == 0 {
                    anyhow::bail!(
                        "Prism source preset '{}' was already removed",
                        registry.display_name
                    );
                }
                Ok(format!(
                    "Removed Prism source preset '{}': {} scraper(s), {} artifact(s), {} queued/running certification job(s) cancelled.",
                    registry.display_name,
                    modules.len(),
                    removed_artifacts,
                    cancelled
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
                let module =
                    nuvio_find_module(store, instance.instance_id, source_module_id).await?;
                let outcome =
                    smoke_prism_source_module_runtime(store, context, instance, &module).await?;
                if prism_certification_is_eligible(&outcome.status) {
                    store
                        .set_source_module_enabled_state(
                            source_module_id,
                            true,
                            &outcome.health_state,
                            outcome.failure_class.as_deref(),
                        )
                        .await?;
                    Ok(format!(
                        "Enabled Prism source '{}' at version {} after {} certification: {}",
                        module.display_name, version, outcome.status, outcome.reason
                    ))
                } else {
                    store
                        .set_source_module_enabled_state(
                            source_module_id,
                            false,
                            &outcome.health_state,
                            Some(&outcome.reason),
                        )
                        .await?;
                    Ok(format!(
                        "Prism source '{}' was not enabled because certification finished as {}: {}",
                        module.display_name, outcome.status, outcome.reason
                    ))
                }
            }
            "smoke_source_module" | "certify_source_module" | "certifySourceModule" => {
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
                let outcome =
                    smoke_prism_source_module_runtime(store, context, instance, &module).await?;
                store
                    .set_source_module_enabled_state(
                        source_module_id,
                        module.enabled && prism_certification_is_eligible(&outcome.status),
                        &outcome.health_state,
                        outcome
                            .failure_class
                            .as_deref()
                            .or(Some(outcome.reason.as_str())),
                    )
                    .await?;
                Ok(format!(
                    "Prism source '{}' certification finished as {} at version {}: {}",
                    module.display_name, outcome.status, version, outcome.reason
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
                let removed_artifacts = uninstall_source_module_artifacts(
                    store,
                    &module,
                    "source module disabled by user",
                )
                .await?;
                Ok(format!(
                    "Disabled Prism source '{}' and moved it back to the scraper marketplace. Removed {} artifact(s); certification history was preserved.",
                    module.display_name, removed_artifacts
                ))
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

fn nuvio_registry_is_preset(registry: &ExtensionSourceRegistry) -> bool {
    registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY
        || registry.registry_type == "elixir_curated_nuvio_pack"
        || registry.trust_class != "custom"
}

fn build_prism_recommended_section(
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    registries: &[ExtensionSourceRegistry],
    modules: &[ExtensionSourceModule],
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlSection {
    let recommended = registries
        .iter()
        .find(|registry| registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY);
    let preset_registries = registries
        .iter()
        .filter(|registry| nuvio_registry_is_preset(registry))
        .collect::<Vec<_>>();
    let preset_registry_ids = preset_registries
        .iter()
        .map(|registry| registry.registry_id)
        .collect::<HashSet<_>>();
    let preset_modules = modules
        .iter()
        .filter(|module| preset_registry_ids.contains(&module.registry_id))
        .collect::<Vec<_>>();
    let problem_modules = preset_modules
        .iter()
        .filter(|module| {
            let certification = certification_by_module
                .get(&module.source_module_id)
                .copied();
            let job = job_by_module.get(&module.source_module_id).copied();
            nuvio_module_needs_attention(module, certification, job)
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
                "{} scraper(s) from maintainer presets need attention.",
                problem_modules
            ),
        ));
    }

    let entities = preset_registries
        .iter()
        .map(|registry| {
            let registry_modules = modules
                .iter()
                .filter(|module| module.registry_id == registry.registry_id)
                .collect::<Vec<_>>();
            let registry_jobs = registry_modules
                .iter()
                .filter_map(|module| job_by_module.get(&module.source_module_id).copied())
                .collect::<Vec<_>>();
            let queued_or_running = registry_jobs
                .iter()
                .filter(|job| matches!(job.status.as_str(), "queued" | "running"))
                .count();
            let ready_modules = registry_modules
                .iter()
                .filter(|module| {
                    nuvio_module_can_run_now(
                        module,
                        certification_by_module
                            .get(&module.source_module_id)
                            .copied(),
                    )
                })
                .count();
            let attention_modules = registry_modules
                .iter()
                .filter(|module| {
                    let certification = certification_by_module
                        .get(&module.source_module_id)
                        .copied();
                    let job = job_by_module.get(&module.source_module_id).copied();
                    nuvio_module_needs_attention(module, certification, job)
                })
                .count();
            let enabled_modules = registry_modules
                .iter()
                .filter(|module| module.enabled && module.health_state != "disabled")
                .count();
            let installed_disabled_modules = registry_modules
                .iter()
                .filter(|module| module.installed && !module.enabled)
                .count();
            let available_modules = registry_modules
                .iter()
                .filter(|module| !module.installed && !module.enabled)
                .count();
            let certified_modules = registry_modules
                .iter()
                .filter(|module| {
                    certification_by_module
                        .get(&module.source_module_id)
                        .is_some_and(|certification| {
                            certification.status == "certified"
                                && !nuvio_certification_is_expired(certification)
                        })
                })
                .count();

            let mut actions = Vec::new();
            if registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY {
                actions.push(prism_refresh_recommended_pack_action());
            } else {
                actions.push(cloudstream_registry_action(
                    "refresh_custom_repo",
                    "Refresh",
                    "Fetch this maintainer source metadata again and update discovered scrapers.",
                    "secondary",
                    registry.registry_id,
                    None,
                ));
            }
            if registry.enabled {
                actions.push(cloudstream_registry_action(
                    "certify_repository",
                    "Certify repo",
                    "Install, certify, and enable runnable scrapers from this source.",
                    "secondary",
                    registry.registry_id,
                    Some("Certify scrapers from this Prism source now?"),
                ));
            }
            if queued_or_running > 0 {
                actions.push(cloudstream_registry_action(
                    "cancel_certification",
                    "Cancel certification",
                    "Cancel queued Prism certification jobs for this source.",
                    "secondary",
                    registry.registry_id,
                    Some("Cancel queued Prism certification jobs for this source?"),
                ));
            }
            if registry.enabled {
                actions.push(cloudstream_registry_action(
                    "disable_registry",
                    "Disable preset",
                    "Disable this preset and its source modules without deleting certification history.",
                    "danger",
                    registry.registry_id,
                    Some("Disable this Prism source preset and its modules?"),
                ));
            } else {
                actions.push(cloudstream_registry_action(
                    "enable_registry",
                    "Enable preset",
                    "Enable this preset. Scrapers still require their own explicit install when they are not already active.",
                    "primary",
                    registry.registry_id,
                    None,
                ));
            }
            actions.push(nuvio_remove_registry_action(registry));

            ExtensionControlEntity {
                id: registry.registry_id.to_string(),
                title: registry.display_name.clone(),
                subtitle: Some(format!(
                    "{} • {} • {} scrapers",
                    if registry.enabled { "Enabled" } else { "Disabled" },
                    registry.trust_class.replace('_', " "),
                    registry_modules.len()
                )),
                details: vec![
                    format!("Instance: {}", instance.instance_name),
                    format!("Implementation: {}", context.summary.label),
                    format!("Registry key: {}", registry.registry_key),
                    format!("Type: {}", registry.registry_type),
                    format!("URL: {}", registry.url.as_deref().unwrap_or("bundled")),
                    format!("Discovered scrapers: {}", registry_modules.len()),
                    format!("Ready to search: {}", ready_modules),
                    format!("Enabled scrapers: {}", enabled_modules),
                    format!("Need attention: {}", attention_modules),
                    format!("Installed but disabled: {}", installed_disabled_modules),
                    format!("Available, not installed: {}", available_modules),
                    format!("Currently certified: {}", certified_modules),
                    format!("Certification queued or running: {queued_or_running}"),
                    format!(
                        "Last refresh: {}",
                        registry
                            .last_fetched_at
                            .map(|value| value.format("%Y-%m-%d %H:%M UTC").to_string())
                            .unwrap_or_else(|| "never".to_string())
                    ),
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
        id: "prismRecommended".to_string(),
        title: "Maintainer presets".to_string(),
        description: "Bundled and maintainer-known scraper sources.".to_string(),
        policy: Some(control_policy_seeded(
            "Preset updates can add or revise scraper descriptors, but each preset can be wiped independently.",
        )),
        notices,
        fields: Vec::new(),
        entities,
        actions: vec![prism_refresh_recommended_pack_action()],
    }
}

async fn build_prism_attention_sources_section(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let mut entities = nuvio_coalesced_module_entities(
        modules,
        registry_by_id,
        certification_by_module,
        job_by_module,
        |module| {
            let certification = certification_by_module
                .get(&module.source_module_id)
                .copied();
            let job = job_by_module.get(&module.source_module_id).copied();
            nuvio_module_needs_attention(module, certification, job)
        },
    );
    let mut recommendation_actions = BTreeMap::<Uuid, Vec<ExtensionControlAction>>::new();
    for module in modules {
        let recommendations = store
            .list_source_replacement_recommendations(Some(module.source_module_id), true)
            .await?;
        if !recommendations.is_empty() {
            recommendation_actions.insert(
                module.source_module_id,
                recommendations
                    .into_iter()
                    .map(|recommendation| {
                        cloudstream_apply_replacement_action(&recommendation.recommendation_key)
                    })
                    .collect(),
            );
        }
    }
    for entity in &mut entities {
        if let Ok(source_module_id) = Uuid::parse_str(&entity.id)
            && let Some(mut actions) = recommendation_actions.remove(&source_module_id)
        {
            actions.append(&mut entity.actions);
            entity.actions = actions;
        }
    }
    if entities.is_empty() {
        return Ok(None);
    }
    Ok(Some(ExtensionControlSection {
        id: "prismNeedsAttention".to_string(),
        title: "Needs attention".to_string(),
        description: format!(
            "{} enabled scraper(s) for '{}' cannot be used until fixed.",
            entities.len(),
            instance.instance_name
        ),
        policy: Some(control_policy_observed(
            "Prism only sends runnable, currently certified scrapers to Extension Suite searches.",
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

#[derive(Debug, Clone)]
struct PrismMarketplacePolicy {
    preferred_language_tags: Vec<String>,
    unknown_language_behavior: String,
    auto_certify_trusted_repositories: bool,
    auto_certify_custom_repositories: String,
    retain_failed_artifacts: bool,
    max_auto_certify_modules_per_repo: usize,
    max_concurrent_certification_jobs: usize,
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

fn prism_marketplace_policy(instance: &ExtensionInstance) -> PrismMarketplacePolicy {
    let policy = instance
        .config_json
        .as_ref()
        .and_then(|config| config.get(PRISM_MARKETPLACE_POLICY_CONFIG_KEY))
        .and_then(serde_json::Value::as_object);
    let preferred_language_tags = policy
        .and_then(|policy| policy.get("preferredLanguageTags"))
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(normalize_prism_language_tag)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec!["en".to_string(), "ja".to_string()]);
    let unknown_language_behavior = policy
        .and_then(|policy| policy.get("unknownLanguageBehavior"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| matches!(*value, "certify" | "skip"))
        .unwrap_or("certify")
        .to_string();
    let auto_certify_custom_repositories = policy
        .and_then(|policy| policy.get("autoCertifyCustomRepositories"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| matches!(*value, "after_trust" | "never"))
        .unwrap_or("after_trust")
        .to_string();
    PrismMarketplacePolicy {
        preferred_language_tags,
        unknown_language_behavior,
        auto_certify_trusted_repositories: policy
            .and_then(|policy| policy.get("autoCertifyTrustedRepositories"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        auto_certify_custom_repositories,
        retain_failed_artifacts: policy
            .and_then(|policy| policy.get("retainFailedArtifacts"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        max_auto_certify_modules_per_repo: policy
            .and_then(|policy| policy.get("maxAutoCertifyModulesPerRepo"))
            .and_then(serde_json::Value::as_u64)
            .map(|value| value.clamp(1, 500) as usize)
            .unwrap_or(PRISM_AUTO_CERTIFICATION_DEFAULT_MAX_MODULES),
        max_concurrent_certification_jobs: policy
            .and_then(|policy| policy.get("maxConcurrentCertificationJobs"))
            .and_then(serde_json::Value::as_u64)
            .map(|value| value.clamp(1, 16) as usize)
            .unwrap_or(2),
    }
}

#[derive(Debug)]
struct PrismRuntimeIsolationSummary {
    health: &'static str,
    title: &'static str,
    subtitle: String,
    details: Vec<String>,
    missing: Vec<String>,
}

async fn build_prism_runtime_isolation_section(
    state: &AppState,
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
) -> anyhow::Result<ExtensionControlSection> {
    let security = context
        .manifest
        .runtime
        .as_ref()
        .map(|runtime| &runtime.security);
    let runtime_state = state
        .orchestrator
        .describe_instance_runtime_state(instance.instance_id)
        .await;
    let (summary, notices) = match (security, runtime_state) {
        (Some(security), Ok(runtime_state)) => {
            let summary = prism_runtime_isolation_summary(security, runtime_state.as_ref());
            let mut notices = Vec::new();
            if summary.health == "unknown" {
                notices.push(control_notice(
                    "warning",
                    "prism_runtime_isolation_unknown",
                    "Runtime isolation unknown",
                    "Elixir could not inspect the Prism container yet. Custom source execution should wait until the runtime reports enforced isolation.",
                ));
            } else if summary.health != "healthy" {
                notices.push(control_notice(
                    "warning",
                    "prism_runtime_reduced_isolation",
                    "Reduced isolation",
                    "Prism is usable, but one or more configured container isolation controls are not visible in Docker inspect output.",
                ));
            }
            (summary, notices)
        }
        (Some(security), Err(err)) => {
            let mut summary = prism_runtime_isolation_summary(security, None);
            summary.details.push(format!(
                "Runtime inspect error: {}",
                prism_truncate_diagnostic(&err.to_string(), 300)
            ));
            let notices = vec![control_notice(
                "warning",
                "prism_runtime_isolation_inspect_failed",
                "Runtime isolation unknown",
                "Elixir could not inspect the Prism container. The runtime may still be starting or Docker may be unavailable.",
            )];
            (summary, notices)
        }
        (None, _) => {
            let summary = PrismRuntimeIsolationSummary {
                health: "unknown",
                title: "Runtime isolation unknown",
                subtitle: "No container runtime manifest is loaded for Prism.".to_string(),
                details: vec!["Prism manifest has no runtime.security policy.".to_string()],
                missing: vec!["runtime.security".to_string()],
            };
            let notices = vec![control_notice(
                "warning",
                "prism_runtime_security_manifest_missing",
                "Runtime policy missing",
                "Prism has no runtime.security manifest policy loaded.",
            )];
            (summary, notices)
        }
    };

    let mut details = summary.details;
    if !summary.missing.is_empty() {
        details.push(format!("Missing controls: {}", summary.missing.join(", ")));
    }

    Ok(ExtensionControlSection {
        id: "prismRuntimeIsolation".to_string(),
        title: "Runtime isolation".to_string(),
        description:
            "Actual Prism container isolation state from Docker inspect and the Prism manifest policy."
                .to_string(),
        policy: Some(control_policy_managed(
            "Elixir owns the Prism runtime sandbox and keeps public scraping/download routing unchanged.",
        )),
        notices,
        fields: Vec::new(),
        entities: vec![ExtensionControlEntity {
            id: "runtime-isolation".to_string(),
            title: summary.title.to_string(),
            subtitle: Some(summary.subtitle),
            details,
            actions: Vec::new(),
        }],
        actions: Vec::new(),
    })
}

fn prism_runtime_isolation_summary(
    security: &ManifestRuntimeSecurity,
    runtime_state: Option<&ContainerRuntimeState>,
) -> PrismRuntimeIsolationSummary {
    let Some(runtime_state) = runtime_state else {
        return PrismRuntimeIsolationSummary {
            health: "unknown",
            title: "Runtime isolation unknown",
            subtitle: "Docker inspect data is not available for the Prism container.".to_string(),
            details: vec![
                "Prism should run as a warm long-lived container before custom source execution."
                    .to_string(),
                format!(
                    "Expected sandbox profile: {}",
                    PRISM_SANDBOX_PROFILE_VERSION
                ),
                format!("Expected egress policy: {}", PRISM_EGRESS_POLICY_VERSION),
            ],
            missing: vec!["runtime_state".to_string()],
        };
    };

    let mut details = Vec::new();
    let mut missing = Vec::new();
    details.push(format!(
        "Sandbox profile: {}",
        runtime_state
            .labels
            .get("elixir.runtime.security.profile")
            .map(String::as_str)
            .unwrap_or("unreported")
    ));
    details.push(format!(
        "Sandbox policy version: {}",
        PRISM_SANDBOX_PROFILE_VERSION
    ));
    details.push(format!(
        "Egress policy version: {}",
        PRISM_EGRESS_POLICY_VERSION
    ));

    if security.run_as_non_root || security.user.is_some() {
        let actual_user = runtime_state.security.user.as_deref().unwrap_or_default();
        let enforced = if let Some(expected_user) = security.user.as_deref() {
            actual_user == expected_user
        } else {
            !actual_user.is_empty()
                && actual_user != "0"
                && !actual_user.eq_ignore_ascii_case("root")
        };
        if enforced {
            details.push(format!("Non-root user: enforced ({actual_user})"));
        } else {
            missing.push("non-root user".to_string());
            details.push(format!(
                "Non-root user: missing (actual: {})",
                if actual_user.is_empty() {
                    "image default"
                } else {
                    actual_user
                }
            ));
        }
    }

    if security.read_only_rootfs {
        if runtime_state.security.read_only_rootfs {
            details.push("Read-only root filesystem: enforced".to_string());
        } else {
            missing.push("read-only root filesystem".to_string());
            details.push("Read-only root filesystem: missing".to_string());
        }
    }

    if security.no_new_privileges {
        if runtime_state.security.no_new_privileges {
            details.push("No-new-privileges: enforced".to_string());
        } else {
            missing.push("no-new-privileges".to_string());
            details.push("No-new-privileges: missing".to_string());
        }
    }

    if !security.drop_capabilities.is_empty() {
        let wants_all = security
            .drop_capabilities
            .iter()
            .any(|capability| capability.eq_ignore_ascii_case("ALL"));
        let has_all = runtime_state
            .security
            .cap_drop
            .iter()
            .any(|capability| capability.eq_ignore_ascii_case("ALL"));
        if !wants_all || has_all {
            details.push(format!(
                "Dropped capabilities: enforced ({})",
                runtime_state.security.cap_drop.join(", ")
            ));
        } else {
            missing.push("drop all capabilities".to_string());
            details.push(format!(
                "Dropped capabilities: missing ALL (actual: {})",
                runtime_state.security.cap_drop.join(", ")
            ));
        }
    }

    if let Some(memory_limit_mb) = security.memory_limit_mb {
        let desired_bytes = (memory_limit_mb as i64) * 1024 * 1024;
        match runtime_state.security.memory_limit_bytes {
            Some(actual) if actual > 0 && actual <= desired_bytes => {
                details.push(format!(
                    "Memory limit: enforced ({} MiB)",
                    actual / 1024 / 1024
                ));
            }
            Some(actual) => {
                missing.push("memory limit".to_string());
                details.push(format!(
                    "Memory limit: reduced (expected <= {memory_limit_mb} MiB, actual {} MiB)",
                    actual / 1024 / 1024
                ));
            }
            None => {
                missing.push("memory limit".to_string());
                details.push(format!(
                    "Memory limit: missing (expected {memory_limit_mb} MiB)"
                ));
            }
        }
    }

    if let Some(pids_limit) = security.pids_limit {
        match runtime_state.security.pids_limit {
            Some(actual) if actual > 0 && actual <= pids_limit as i64 => {
                details.push(format!("PID limit: enforced ({actual})"));
            }
            Some(actual) => {
                missing.push("PID limit".to_string());
                details.push(format!(
                    "PID limit: reduced (expected <= {pids_limit}, actual {actual})"
                ));
            }
            None => {
                missing.push("PID limit".to_string());
                details.push(format!("PID limit: missing (expected {pids_limit})"));
            }
        }
    }

    if let Some(cpu_quota) = security.cpu_quota.as_deref() {
        if runtime_state.security.nano_cpus.is_some() {
            details.push(format!(
                "CPU quota: enforced ({})",
                runtime_state.security.nano_cpus.unwrap_or_default()
            ));
        } else {
            missing.push("CPU quota".to_string());
            details.push(format!("CPU quota: missing (expected {cpu_quota})"));
        }
    }

    for tmpfs in &security.tmpfs {
        let found = runtime_state
            .security
            .tmpfs
            .iter()
            .any(|mount| mount.path == tmpfs.path);
        if found {
            details.push(format!("Tmpfs {}: enforced", tmpfs.path));
        } else {
            missing.push(format!("tmpfs {}", tmpfs.path));
            details.push(format!("Tmpfs {}: missing", tmpfs.path));
        }
    }

    if let Some(seccomp_profile) = security.seccomp_profile.as_deref() {
        if runtime_state.security.seccomp_profile.as_deref() == Some(seccomp_profile) {
            details.push(format!("Seccomp profile: enforced ({seccomp_profile})"));
        } else {
            missing.push("seccomp profile".to_string());
            details.push(format!(
                "Seccomp profile: missing (expected {seccomp_profile}, actual {})",
                runtime_state
                    .security
                    .seccomp_profile
                    .as_deref()
                    .unwrap_or("unreported")
            ));
        }
    } else {
        details.push("Seccomp profile: not requested by default compatibility profile".to_string());
    }

    if let Some(apparmor_profile) = security.apparmor_profile.as_deref() {
        if runtime_state.security.apparmor_profile.as_deref() == Some(apparmor_profile) {
            details.push(format!("AppArmor profile: enforced ({apparmor_profile})"));
        } else {
            missing.push("AppArmor profile".to_string());
            details.push(format!(
                "AppArmor profile: missing (expected {apparmor_profile}, actual {})",
                runtime_state
                    .security
                    .apparmor_profile
                    .as_deref()
                    .unwrap_or("unreported")
            ));
        }
    } else {
        details
            .push("AppArmor profile: not requested by default compatibility profile".to_string());
    }

    if security.prohibit_docker_socket {
        if runtime_state
            .mounts
            .iter()
            .any(prism_mount_targets_docker_socket)
        {
            missing.push("Docker socket absent".to_string());
            details.push("Docker socket: mounted, runtime is unsafe".to_string());
        } else {
            details.push("Docker socket: absent".to_string());
        }
    }

    let writable_source_mounts = runtime_state
        .mounts
        .iter()
        .filter(|mount| {
            matches!(
                mount.destination.as_str(),
                "/app/source-modules" | "/app/stremio-source-modules"
            ) && !mount.read_only
        })
        .count();
    if writable_source_mounts == 0 {
        details.push("Source module mounts: read-only or absent".to_string());
    } else {
        missing.push("read-only source module mounts".to_string());
        details.push(format!(
            "Source module mounts: {writable_source_mounts} writable mount(s)"
        ));
    }

    let (health, title, subtitle) = if missing.is_empty() {
        (
            "healthy",
            "Runtime isolation enforced",
            "Prism sandbox controls are visible in Docker inspect.".to_string(),
        )
    } else {
        (
            "degraded",
            "Runtime isolation reduced",
            format!(
                "{} configured control(s) are missing or unreported.",
                missing.len()
            ),
        )
    };

    PrismRuntimeIsolationSummary {
        health,
        title,
        subtitle,
        details,
        missing,
    }
}

fn prism_mount_targets_docker_socket(mount: &crate::runtime::model::ContainerRuntimeMount) -> bool {
    mount.destination == "/var/run/docker.sock"
        || mount
            .source
            .as_deref()
            .is_some_and(|source| source.ends_with("/docker.sock"))
}

async fn prism_ensure_custom_repository_isolation(
    state: &AppState,
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    registry: &ExtensionSourceRegistry,
    would_execute_custom_code: bool,
) -> anyhow::Result<()> {
    if let Some(message) = prism_custom_repository_isolation_block(
        state,
        context,
        instance,
        registry,
        would_execute_custom_code,
    )
    .await?
    {
        anyhow::bail!("{message}");
    }
    Ok(())
}

async fn prism_custom_repository_isolation_block(
    state: &AppState,
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    registry: &ExtensionSourceRegistry,
    would_execute_custom_code: bool,
) -> anyhow::Result<Option<String>> {
    if !would_execute_custom_code || !prism_registry_requires_custom_isolation(registry) {
        return Ok(None);
    }
    let Some(security) = context
        .manifest
        .runtime
        .as_ref()
        .map(|runtime| &runtime.security)
    else {
        return Ok(Some(
            "Prism runtime isolation policy is unavailable, so custom repository certification is disabled.".to_string(),
        ));
    };
    let mut runtime_state = state
        .orchestrator
        .describe_instance_runtime_state(instance.instance_id)
        .await
        .with_context(|| {
            format!(
                "checking Prism runtime isolation before certifying custom repository '{}'",
                registry.display_name
            )
        })?;
    let mut summary = prism_runtime_isolation_summary(security, runtime_state.as_ref());
    if summary.health != "healthy" {
        if let Err(err) = state
            .orchestrator
            .ensure_instance_runtime_running(
                &context.extension.extension_id,
                instance,
                &context.manifest,
            )
            .await
        {
            return Ok(Some(format!(
                "Prism runtime isolation could not be repaired before certifying custom repository '{}': {err}",
                registry.display_name
            )));
        }
        runtime_state = state
            .orchestrator
            .describe_instance_runtime_state(instance.instance_id)
            .await
            .with_context(|| {
                format!(
                    "checking Prism runtime isolation after repairing runtime for custom repository '{}'",
                    registry.display_name
                )
            })?;
        summary = prism_runtime_isolation_summary(security, runtime_state.as_ref());
    }
    if summary.health == "healthy" {
        return Ok(None);
    }
    let reason = if summary.missing.is_empty() {
        summary.subtitle
    } else {
        format!("Missing controls: {}", summary.missing.join(", "))
    };
    Ok(Some(format!(
        "Prism runtime isolation is not enforced ({reason}). Custom repository certification is disabled until Prism reports enforced isolation."
    )))
}

fn prism_registry_requires_custom_isolation(registry: &ExtensionSourceRegistry) -> bool {
    registry.trust_class == "custom" && registry.registry_type != "elixir_curated_nuvio_pack"
}

fn build_prism_policy_section(
    instance: &ExtensionInstance,
    marketplace_policy: &PrismMarketplacePolicy,
) -> ExtensionControlSection {
    let policy = prism_source_policy(instance);
    ExtensionControlSection {
        id: "prismSourcePolicy".to_string(),
        title: "Advanced policy".to_string(),
        description: "Preset updates, repository trust, certification, and rollback controls."
            .to_string(),
        policy: Some(control_policy_seeded(
            "These settings do not grant downloader credentials or library mutation rights.",
        )),
        notices: Vec::new(),
        fields: vec![
            prism_text_policy_field(
                "preferredLanguageTags",
                "Preferred languages",
                "Comma-separated language tags used when auto-certifying repository scrapers.",
                &marketplace_policy.preferred_language_tags.join(", "),
                false,
            ),
            prism_select_policy_field(
                "unknownLanguageBehavior",
                "Unknown language",
                "Choose whether scrapers without language metadata are still certified.",
                &marketplace_policy.unknown_language_behavior,
                &[("certify", "Certify"), ("skip", "Skip")],
                false,
            ),
            prism_policy_field(
                "autoCertifyTrustedRepositories",
                "Auto-certify trusted repositories",
                "Install and certify eligible scrapers after trusted repository add or refresh.",
                marketplace_policy.auto_certify_trusted_repositories,
                false,
            ),
            prism_select_policy_field(
                "autoCertifyCustomRepositories",
                "Custom repository auto-cert",
                "Choose when custom repositories can install executable scraper code for certification.",
                &marketplace_policy.auto_certify_custom_repositories,
                &[("after_trust", "After trust"), ("never", "Never")],
                false,
            ),
            prism_policy_field(
                "retainFailedArtifacts",
                "Retain failed artifacts",
                "Keep downloaded scraper artifacts after certification failure for debugging.",
                marketplace_policy.retain_failed_artifacts,
                false,
            ),
            prism_number_policy_field(
                "maxAutoCertifyModulesPerRepo",
                "Max repo certification batch",
                "Maximum number of repository scrapers certified from one add or refresh action.",
                marketplace_policy.max_auto_certify_modules_per_repo,
                false,
            ),
            prism_number_policy_field(
                "maxConcurrentCertificationJobs",
                "Certification concurrency",
                "Maximum certification workers reserved for Prism repository batches.",
                marketplace_policy.max_concurrent_certification_jobs,
                false,
            ),
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

fn prism_text_policy_field(
    id: &str,
    label: &str,
    description: &str,
    value: &str,
    readonly: bool,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "text".to_string(),
        value: serde_json::Value::String(value.to_string()),
        required: false,
        readonly,
        secret: false,
        options: Vec::new(),
        validation: None,
    }
}

fn prism_number_policy_field(
    id: &str,
    label: &str,
    description: &str,
    value: usize,
    readonly: bool,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "number".to_string(),
        value: json!(value),
        required: false,
        readonly,
        secret: false,
        options: Vec::new(),
        validation: None,
    }
}

fn prism_select_policy_field(
    id: &str,
    label: &str,
    description: &str,
    value: &str,
    options: &[(&str, &str)],
    readonly: bool,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "select".to_string(),
        value: serde_json::Value::String(value.to_string()),
        required: false,
        readonly,
        secret: false,
        options: options
            .iter()
            .map(|(value, label)| ExtensionControlOption {
                value: json!(value),
                label: label.to_string(),
            })
            .collect(),
        validation: None,
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

#[derive(Debug, Clone)]
struct PrismLanguageEligibility {
    certifiable: bool,
    state: String,
    summary: String,
    normalized_tags: Vec<String>,
}

fn prism_module_language_eligibility(
    module: &ExtensionSourceModule,
    policy: &PrismMarketplacePolicy,
) -> PrismLanguageEligibility {
    let normalized_tags = prism_module_language_tags(module);
    if normalized_tags.is_empty() {
        if policy.unknown_language_behavior == "skip" {
            return PrismLanguageEligibility {
                certifiable: false,
                state: "skipped_unknown_language".to_string(),
                summary: "Skipped because this scraper does not declare supported languages."
                    .to_string(),
                normalized_tags,
            };
        }
        return PrismLanguageEligibility {
            certifiable: true,
            state: "unknown_language".to_string(),
            summary:
                "No language metadata declared; certifying because unknown-language scrapers are allowed."
                    .to_string(),
            normalized_tags,
        };
    }
    let preferred = policy
        .preferred_language_tags
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let matches_preferred = normalized_tags
        .iter()
        .any(|tag| tag == "multi" || tag == "all" || preferred.contains(tag));
    if matches_preferred {
        PrismLanguageEligibility {
            certifiable: true,
            state: "preferred_language".to_string(),
            summary: format!(
                "Language tags match preferred languages: {}.",
                normalized_tags.join(", ")
            ),
            normalized_tags,
        }
    } else {
        PrismLanguageEligibility {
            certifiable: false,
            state: "skipped_language".to_string(),
            summary: format!(
                "Skipped because languages {} do not match preferred languages {}.",
                normalized_tags.join(", "),
                policy.preferred_language_tags.join(", ")
            ),
            normalized_tags,
        }
    }
}

fn prism_module_language_tags(module: &ExtensionSourceModule) -> Vec<String> {
    let mut tags = Vec::new();
    if let Some(value) = module.language_tags_json.as_ref() {
        collect_prism_language_tags(value, &mut tags);
    }
    if let Some(metadata) = module.metadata_json.as_ref() {
        for pointer in [
            "/language",
            "/languages",
            "/languageTags",
            "/nuvio/language",
            "/nuvio/languages",
            "/nuvio/languageTags",
            "/nuvio/lang",
        ] {
            if let Some(value) = metadata.pointer(pointer) {
                collect_prism_language_tags(value, &mut tags);
            }
        }
    }
    let mut seen = HashSet::new();
    tags.into_iter()
        .filter_map(|tag| normalize_prism_language_tag(&tag))
        .filter(|tag| seen.insert(tag.clone()))
        .collect()
}

fn collect_prism_language_tags(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(value) => {
            for part in value.split([',', ';', '|', '/']) {
                let part = part.trim();
                if !part.is_empty() {
                    out.push(part.to_string());
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_prism_language_tags(value, out);
            }
        }
        Value::Object(map) => {
            for key in ["code", "tag", "language", "name"] {
                if let Some(value) = map.get(key) {
                    collect_prism_language_tags(value, out);
                }
            }
        }
        _ => {}
    }
}

fn normalize_prism_language_tag(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let lower = value
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-')
        .to_ascii_lowercase()
        .replace('_', "-");
    let primary = lower.split('-').next().unwrap_or(lower.as_str());
    match primary {
        "en" | "eng" | "english" => Some("en".to_string()),
        "ja" | "jp" | "jpn" | "japanese" | "nihongo" => Some("ja".to_string()),
        "hi" | "hin" | "hindi" => Some("hi".to_string()),
        "ta" | "tam" | "tamil" => Some("ta".to_string()),
        "te" | "tel" | "telugu" => Some("te".to_string()),
        "ml" | "mal" | "malayalam" => Some("ml".to_string()),
        "ko" | "kor" | "korean" => Some("ko".to_string()),
        "zh" | "chi" | "zho" | "chinese" => Some("zh".to_string()),
        "es" | "spa" | "spanish" => Some("es".to_string()),
        "fr" | "fra" | "fre" | "french" => Some("fr".to_string()),
        "de" | "deu" | "ger" | "german" => Some("de".to_string()),
        "multi" | "multilingual" | "dual" | "all" => Some("multi".to_string()),
        other if other.len() == 2 || other.len() == 3 => Some(other.to_string()),
        _ => None,
    }
}

fn prism_parse_preferred_language_setting(value: &Value) -> anyhow::Result<Vec<String>> {
    let mut raw = Vec::new();
    collect_prism_language_tags(value, &mut raw);
    let mut seen = HashSet::new();
    let languages = raw
        .into_iter()
        .filter_map(|value| normalize_prism_language_tag(&value))
        .filter(|value| seen.insert(value.clone()))
        .collect::<Vec<_>>();
    if languages.is_empty() {
        anyhow::bail!("preferredLanguageTags must include at least one language tag");
    }
    Ok(languages)
}

fn prism_language_eligibility_json(eligibility: &PrismLanguageEligibility) -> String {
    json!({
        "state": eligibility.state,
        "summary": eligibility.summary,
        "normalizedTags": eligibility.normalized_tags,
    })
    .to_string()
}

fn build_nuvio_ready_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlSection {
    let entities = nuvio_coalesced_module_entities(
        modules,
        registry_by_id,
        certification_by_module,
        job_by_module,
        |module| {
            nuvio_module_can_run_now(
                module,
                certification_by_module
                    .get(&module.source_module_id)
                    .copied(),
            )
        },
    );
    ExtensionControlSection {
        id: "nuvioReadySources".to_string(),
        title: "Ready to search".to_string(),
        description: "Scrapers Prism can run right now during stream searches.".to_string(),
        policy: Some(control_policy_seeded(
            "Only installed, enabled, currently certified scrapers are sent to Prism at search time.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn build_nuvio_disabled_sources_section(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlSection {
    let entities = nuvio_coalesced_module_entities(
        modules,
        registry_by_id,
        certification_by_module,
        job_by_module,
        |module| module.installed && !module.enabled,
    );
    ExtensionControlSection {
        id: "nuvioDisabledSources".to_string(),
        title: "Installed but disabled".to_string(),
        description: "Local scraper artifacts that are not included in Prism searches.".to_string(),
        policy: Some(control_policy_observed(
            "Disabled scrapers stay visible for inspection, but Prism will not query them.",
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
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlSection {
    let active_group_keys = nuvio_active_group_keys(modules);
    let entities = nuvio_coalesced_module_entities(
        modules,
        registry_by_id,
        certification_by_module,
        job_by_module,
        |module| {
            let registry = registry_by_id.get(&module.registry_id).copied();
            nuvio_registry_allows_module_catalog(registry)
                && !module.enabled
                && !module.installed
                && !active_group_keys.contains(&nuvio_module_coalesce_key(module))
        },
    );
    ExtensionControlSection {
        id: "nuvioAvailableSources".to_string(),
        title: "Scraper marketplace".to_string(),
        description: "Discovered Nuvio-compatible scrapers that are not enabled yet.".to_string(),
        policy: Some(control_policy_observed(
            "Repository manifests are inventoried before scraper code is installed.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }
}

fn nuvio_registry_allows_module_catalog(registry: Option<&ExtensionSourceRegistry>) -> bool {
    registry
        .map(|registry| {
            !(registry.trust_class == "custom" && !registry.trusted_for_executable_updates)
        })
        .unwrap_or(false)
}

fn nuvio_module_can_run_now(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
) -> bool {
    module.enabled
        && module.installed
        && !module.unsupported
        && !module.account_required
        && nuvio_module_health_allows_runtime(module)
        && certification.is_some_and(nuvio_certification_allows_runtime)
}

fn nuvio_module_needs_attention(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> bool {
    if module.replacement_recommendation_key.is_some() {
        return true;
    }
    if job.is_some_and(nuvio_certification_job_needs_attention) {
        return true;
    }
    if !module.enabled {
        return false;
    }
    if !module.installed || module.unsupported || module.account_required {
        return true;
    }
    !nuvio_module_can_run_now(module, certification)
}

fn nuvio_certification_job_needs_attention(job: &ExtensionSourceCertificationJob) -> bool {
    matches!(
        job.status.as_str(),
        "blocked" | "failed" | "cancelled" | "skipped"
    ) && !matches!(
        job.marketplace_state.as_deref(),
        Some("skipped_language" | "skipped_unknown_language" | "skipped_trust")
    )
}

fn nuvio_certification_allows_runtime(certification: &ExtensionSourceModuleCertification) -> bool {
    prism_certification_is_eligible(&certification.status)
        && !nuvio_certification_is_expired(certification)
        && nuvio_certification_policy_is_current(certification)
}

fn nuvio_certification_is_expired(certification: &ExtensionSourceModuleCertification) -> bool {
    certification
        .expires_at
        .is_some_and(|expires_at| expires_at <= Utc::now())
}

fn nuvio_certification_policy_is_current(
    certification: &ExtensionSourceModuleCertification,
) -> bool {
    certification.policy_version == prism_certification_policy_version()
}

fn nuvio_module_health_allows_runtime(module: &ExtensionSourceModule) -> bool {
    !matches!(
        module.health_state.as_str(),
        "broken" | "unsupported" | "account_required" | "disabled"
    )
}

fn nuvio_active_group_keys(modules: &[ExtensionSourceModule]) -> HashSet<String> {
    modules
        .iter()
        .filter(|module| module.enabled || module.installed)
        .map(nuvio_module_coalesce_key)
        .collect()
}

fn nuvio_coalesced_module_entities<F>(
    modules: &[ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
    include: F,
) -> Vec<ExtensionControlEntity>
where
    F: Fn(&ExtensionSourceModule) -> bool,
{
    let mut groups = BTreeMap::<String, Vec<&ExtensionSourceModule>>::new();
    for module in modules.iter().filter(|module| include(module)) {
        groups
            .entry(nuvio_module_coalesce_key(module))
            .or_default()
            .push(module);
    }
    let mut entities = groups
        .into_values()
        .filter(|variants| !variants.is_empty())
        .map(|mut variants| {
            variants.sort_by_key(|module| {
                nuvio_module_primary_sort_key(
                    module,
                    registry_by_id.get(&module.registry_id).copied(),
                    certification_by_module
                        .get(&module.source_module_id)
                        .copied(),
                    job_by_module.get(&module.source_module_id).copied(),
                )
            });
            nuvio_coalesced_module_entity(
                &variants,
                registry_by_id,
                certification_by_module,
                job_by_module,
            )
        })
        .collect::<Vec<_>>();
    entities.sort_by(|left, right| {
        left.title
            .to_ascii_lowercase()
            .cmp(&right.title.to_ascii_lowercase())
            .then_with(|| left.id.cmp(&right.id))
    });
    entities
}

fn nuvio_coalesced_module_entity(
    variants: &[&ExtensionSourceModule],
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
    certification_by_module: &BTreeMap<Uuid, &ExtensionSourceModuleCertification>,
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlEntity {
    let primary = variants[0];
    let primary_registry = registry_by_id.get(&primary.registry_id).copied();
    let primary_certification = certification_by_module
        .get(&primary.source_module_id)
        .copied();
    let primary_job = job_by_module.get(&primary.source_module_id).copied();
    let mut subtitle = nuvio_module_subtitle(primary, primary_certification, primary_job);
    if variants.len() > 1 {
        subtitle.push_str(&format!(" • {} repos", variants.len()));
    }
    let mut details = nuvio_module_details(
        primary,
        primary_registry,
        primary_certification,
        primary_job,
    );
    if variants.len() > 1 {
        details.push(format!("Repository variants: {}", variants.len()));
        for variant in variants {
            details.push(nuvio_module_variant_detail(
                variant,
                registry_by_id.get(&variant.registry_id).copied(),
                certification_by_module
                    .get(&variant.source_module_id)
                    .copied(),
                job_by_module.get(&variant.source_module_id).copied(),
            ));
        }
    }

    let mut actions = Vec::new();
    let mut seen_actions = HashSet::new();
    for variant in variants {
        let registry = registry_by_id.get(&variant.registry_id).copied();
        let repo_label = registry
            .map(|registry| registry.display_name.as_str())
            .unwrap_or("unknown repo");
        for mut action in nuvio_module_actions(variant, registry) {
            if variants.len() > 1 {
                action.label = format!("{} - {}", action.label, repo_label);
                if action.description.trim().is_empty() {
                    action.description = format!("Repository variant: {repo_label}.");
                } else {
                    action.description =
                        format!("{} Repository variant: {repo_label}.", action.description);
                }
            }
            let action_key = format!(
                "{}:{}:{}",
                action.id, variant.source_module_id, action.label
            );
            if seen_actions.insert(action_key) {
                actions.push(action);
            }
        }
    }

    ExtensionControlEntity {
        id: primary.source_module_id.to_string(),
        title: primary.display_name.clone(),
        subtitle: Some(subtitle),
        details,
        actions,
    }
}

fn nuvio_module_coalesce_key(module: &ExtensionSourceModule) -> String {
    let raw = module
        .plugin_package
        .as_deref()
        .or_else(|| {
            module
                .metadata_json
                .as_ref()
                .and_then(|value| value.pointer("/nuvio/moduleId"))
                .and_then(Value::as_str)
        })
        .unwrap_or(module.display_name.as_str());
    cloudstream_stable_text_id(raw)
}

fn nuvio_module_primary_sort_key(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> (i64, i64, i64, String) {
    let state_rank = if module.enabled {
        0
    } else if module.installed {
        1
    } else {
        2
    };
    let status_rank = nuvio_status_rank(nuvio_module_status(module, certification, job).as_deref());
    let trust_rank = registry
        .map(|registry| match registry.trust_class.as_str() {
            "curated" => 0,
            "maintainer_known" => 1,
            "custom" => 2,
            _ => 3,
        })
        .unwrap_or(4);
    (
        state_rank,
        status_rank,
        trust_rank,
        module.module_key.to_ascii_lowercase(),
    )
}

fn nuvio_status_rank(status: Option<&str>) -> i64 {
    match status.unwrap_or("unknown") {
        "certified" | "healthy" => 0,
        "degraded" | "probation" => 1,
        "available" => 2,
        "certifying" | "queued" | "running" => 3,
        "certification_expired" | "certification_policy_stale" => 4,
        "unknown" => 4,
        "broken" | "network_blocked" => 5,
        "unsupported" | "account_required" => 6,
        _ => 7,
    }
}

fn nuvio_module_status(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> Option<String> {
    if certification.is_some_and(nuvio_certification_is_expired) {
        return Some("certification_expired".to_string());
    }
    if certification
        .is_some_and(|certification| !nuvio_certification_policy_is_current(certification))
    {
        return Some("certification_policy_stale".to_string());
    }
    certification
        .map(|certification| certification.status.clone())
        .or_else(|| {
            job.and_then(|job| {
                job.marketplace_state
                    .clone()
                    .or_else(|| Some(job.status.clone()))
            })
        })
        .or_else(|| Some(module.health_state.clone()))
}

fn nuvio_module_state_label(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> String {
    if nuvio_module_can_run_now(module, certification) {
        if certification.is_some_and(|certification| certification.status == "degraded")
            || module.health_state == "degraded"
        {
            return "Ready with warnings".to_string();
        }
        return "Ready to search".to_string();
    }
    if module.enabled
        && module.installed
        && certification.is_some_and(|certification| {
            nuvio_certification_is_expired(certification)
                || !nuvio_certification_policy_is_current(certification)
        })
    {
        return "Needs recertification".to_string();
    }
    if module.enabled {
        return "Needs attention".to_string();
    }
    if module.installed {
        return "Installed, disabled".to_string();
    }
    if job.is_some_and(|job| matches!(job.status.as_str(), "queued" | "running")) {
        return "Certifying".to_string();
    }
    "Available".to_string()
}

fn nuvio_module_status_label(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> String {
    if let Some(certification) = certification {
        let status = certification.status.replace('_', " ");
        if nuvio_certification_is_expired(certification) {
            return format!("certification expired ({status})");
        }
        return status;
    }
    if let Some(job) = job {
        return job
            .marketplace_state
            .as_deref()
            .unwrap_or(job.status.as_str())
            .replace('_', " ");
    }
    module.health_state.replace('_', " ")
}

fn nuvio_module_variant_detail(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> String {
    let repo = registry
        .map(|registry| registry.display_name.as_str())
        .unwrap_or("unknown repo");
    let state = nuvio_module_state_label(module, certification, job);
    let status = nuvio_module_status_label(module, certification, job);
    format!(
        "Variant: {repo} - {state} - {status} - v{}",
        module.active_version.as_deref().unwrap_or("none")
    )
}

fn build_nuvio_repositories_section(
    registries: &[ExtensionSourceRegistry],
    modules: &[ExtensionSourceModule],
    job_by_module: &BTreeMap<Uuid, &ExtensionSourceCertificationJob>,
) -> ExtensionControlSection {
    let entities = registries
        .iter()
        .filter(|registry| !nuvio_registry_is_preset(registry))
        .map(|registry| {
            let untrusted_custom =
                registry.trust_class == "custom" && !registry.trusted_for_executable_updates;
            let registry_modules = modules
                .iter()
                .filter(|module| module.registry_id == registry.registry_id)
                .collect::<Vec<_>>();
            let registry_jobs = registry_modules
                .iter()
                .filter_map(|module| job_by_module.get(&module.source_module_id).copied())
                .collect::<Vec<_>>();
            let queued_or_running = registry_jobs
                .iter()
                .filter(|job| matches!(job.status.as_str(), "queued" | "running"))
                .count();
            let certified = registry_jobs
                .iter()
                .filter(|job| matches!(job.marketplace_state.as_deref(), Some("certified")))
                .count();
            let degraded = registry_jobs
                .iter()
                .filter(|job| matches!(job.marketplace_state.as_deref(), Some("degraded")))
                .count();
            let broken = registry_jobs
                .iter()
                .filter(|job| {
                    matches!(
                        job.marketplace_state.as_deref(),
                        Some("broken" | "unsupported" | "account_required" | "network_blocked")
                    )
                })
                .count();
            let skipped = registry_jobs
                .iter()
                .filter(|job| {
                    matches!(
                        job.marketplace_state.as_deref(),
                        Some("skipped_language" | "skipped_trust" | "skipped_unknown_language")
                    )
                })
                .count();
            let skipped_trust = registry_jobs
                .iter()
                .filter(|job| matches!(job.marketplace_state.as_deref(), Some("skipped_trust")))
                .count();
            let mut actions = vec![cloudstream_registry_action(
                "refresh_custom_repo",
                "Refresh",
                "Fetch the repository metadata again and update discovered source modules.",
                "secondary",
                registry.registry_id,
                None,
            )];
            if untrusted_custom {
                actions.push(cloudstream_registry_action(
                    "trust_custom_repo",
                    "Trust + certify",
                    "Trust this repository for scraper installs and immediately certify discovered scrapers.",
                    "primary",
                    registry.registry_id,
                    Some("Trust this Prism repository for executable scraper installs, then certify its discovered scrapers? Only do this for maintainers you trust."),
                ));
            } else if registry.enabled {
                actions.push(cloudstream_registry_action(
                    "certify_repository",
                    "Certify repo",
                    "Install, certify, and enable runnable scrapers from this repository.",
                    "secondary",
                    registry.registry_id,
                    Some("Certify scrapers from this Prism repository now?"),
                ));
            }
            if queued_or_running > 0 {
                actions.push(cloudstream_registry_action(
                    "cancel_certification",
                    "Cancel certification",
                    "Cancel queued Prism certification jobs for this repository.",
                    "secondary",
                    registry.registry_id,
                    Some("Cancel queued Prism certification jobs for this repository?"),
                ));
            }
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
            if !untrusted_custom && !registry.trusted_for_executable_updates {
                actions.push(cloudstream_registry_action(
                    "trust_custom_repo",
                    "Trust repo",
                    "Mark this repository as maintainer-known for explicit source module installs.",
                    "secondary",
                    registry.registry_id,
                    Some("Trust this custom Prism repository for executable source installs? Only do this for maintainers you trust."),
                ));
            }
            actions.push(nuvio_remove_registry_action(registry));
            ExtensionControlEntity {
                id: registry.registry_id.to_string(),
                title: registry.display_name.clone(),
                subtitle: Some(format!(
                    "{} • {}",
                    if untrusted_custom {
                        "Blocked until trusted"
                    } else if registry.enabled {
                        "Enabled"
                    } else {
                        "Disabled"
                    },
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
                    format!("Discovered scrapers: {}", registry_modules.len()),
                    if untrusted_custom {
                        "Certification: blocked until this repository is trusted".to_string()
                    } else {
                        "Certification: allowed".to_string()
                    },
                    format!("Certification queued or running: {queued_or_running}"),
                    format!("Certified: {certified}"),
                    format!("Degraded: {degraded}"),
                    format!("Broken or blocked: {broken}"),
                    format!("Skipped by policy: {skipped}"),
                    format!("Skipped because repo is untrusted: {skipped_trust}"),
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
        title: "Custom scraper repositories".to_string(),
        description: "Add a Nuvio manifest URL, then trust and certify the scrapers you want."
            .to_string(),
        policy: Some(control_policy_observed(
            "Custom repositories are inventoried immediately but cannot install executable scraper code until trusted.",
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
    registry_by_id: &BTreeMap<Uuid, &ExtensionSourceRegistry>,
) -> anyhow::Result<ExtensionControlSection> {
    let visible_modules = modules
        .iter()
        .filter(|module| {
            nuvio_registry_allows_module_catalog(registry_by_id.get(&module.registry_id).copied())
        })
        .cloned()
        .collect::<Vec<_>>();
    build_cloudstream_version_pins_section(store, &visible_modules)
        .await
        .map(|mut section| {
            section.id = "nuvioVersionPins".to_string();
            section.title = "Version pins".to_string();
            section.description =
                "Pin or roll back scraper versions when a source update breaks.".to_string();
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
                    || registry.trust_class != "custom"
                    || registry.trusted_for_executable_updates)
        })
        .unwrap_or(false);
    if module.enabled {
        actions.push(cloudstream_source_module_action(
            "disable_source_module",
            "Disable",
            "Disable and uninstall this Prism source module. Certification history stays visible in the marketplace.",
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
    if module.installed && !module.enabled {
        actions.push(cloudstream_source_module_action(
            "disable_source_module",
            "Uninstall",
            "Remove this Prism scraper from installed modules and return it to the marketplace.",
            "danger",
            module.source_module_id,
            Some("Uninstall this Prism scraper and keep its certification history?"),
        ));
    }
    if module.installed || module.enabled {
        actions.push(cloudstream_source_module_action(
            "certify_source_module",
            "Retry certification",
            "Run this scraper through Prism with a canary search and bounded materialization preflight.",
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

fn nuvio_module_subtitle(
    module: &ExtensionSourceModule,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> String {
    format!(
        "{} • {} • v{}",
        nuvio_module_state_label(module, certification, job),
        nuvio_module_status_label(module, certification, job),
        module.active_version.as_deref().unwrap_or("none")
    )
}

fn nuvio_module_search_behavior_detail(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> String {
    if nuvio_module_can_run_now(module, certification) {
        return "Search behavior: included in Prism searches".to_string();
    }
    if job.is_some_and(|job| matches!(job.status.as_str(), "queued" | "running")) {
        return "Search behavior: excluded while certification is running".to_string();
    }
    if !module.installed {
        let repo_trusted = registry
            .map(|registry| {
                registry.registry_type == "elixir_curated_nuvio_pack"
                    || registry.trust_class != "custom"
                    || registry.trusted_for_executable_updates
            })
            .unwrap_or(false);
        if repo_trusted {
            return "Search behavior: excluded until installed and certified".to_string();
        }
        return "Search behavior: excluded until the repository is trusted".to_string();
    }
    if !module.enabled {
        return "Search behavior: excluded because this scraper is disabled".to_string();
    }
    "Search behavior: excluded until certification succeeds".to_string()
}

fn nuvio_module_primary_issue_detail(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> Option<String> {
    if module.unsupported {
        return Some(format!(
            "Primary issue: unsupported{}",
            module
                .unsupported_reason
                .as_deref()
                .map(|reason| format!(" ({})", prism_truncate_diagnostic(reason, 180)))
                .unwrap_or_default()
        ));
    }
    if module.account_required {
        return Some("Primary issue: source requires an account before it can run".to_string());
    }
    if registry.is_some_and(|registry| {
        registry.trust_class == "custom" && !registry.trusted_for_executable_updates
    }) {
        return Some(
            "Primary issue: repository must be trusted before executable scraper installs"
                .to_string(),
        );
    }
    if let Some(job) = job
        && matches!(job.status.as_str(), "queued" | "running")
    {
        return Some(format!(
            "Primary issue: certification {}",
            job.status.replace('_', " ")
        ));
    }
    if let Some(certification) = certification {
        if nuvio_certification_is_expired(certification) {
            return Some("Primary issue: certification expired; retry certification".to_string());
        }
        if !nuvio_certification_policy_is_current(certification) {
            return Some(
                "Primary issue: certification policy changed; retry certification".to_string(),
            );
        }
        if !matches!(certification.status.as_str(), "certified" | "degraded") {
            let status = certification.status.replace('_', " ");
            let reason = certification
                .failure_class
                .as_deref()
                .map(|failure| format!(" ({})", failure.replace('_', " ")))
                .unwrap_or_default();
            return Some(format!("Primary issue: certification {status}{reason}"));
        }
        if module.enabled && module.installed && certification.status == "degraded" {
            return Some(
                "Primary issue: certified with warnings; Prism can search it but health is degraded"
                    .to_string(),
            );
        }
    } else if module.enabled {
        return Some("Primary issue: no current certification result".to_string());
    }
    if let Some(error) = module.last_error.as_deref() {
        return Some(format!(
            "Primary issue: {}",
            prism_truncate_diagnostic(error, 180)
        ));
    }
    None
}

fn nuvio_module_details(
    module: &ExtensionSourceModule,
    registry: Option<&ExtensionSourceRegistry>,
    certification: Option<&ExtensionSourceModuleCertification>,
    job: Option<&ExtensionSourceCertificationJob>,
) -> Vec<String> {
    let mut details = vec![format!(
        "Runtime: {}",
        nuvio_module_state_label(module, certification, job)
    )];
    details.push(nuvio_module_search_behavior_detail(
        module,
        registry,
        certification,
        job,
    ));
    if let Some(issue) = nuvio_module_primary_issue_detail(module, registry, certification, job) {
        details.push(issue);
    }
    if let Some(job) = job {
        let marketplace_state = job
            .marketplace_state
            .as_deref()
            .unwrap_or(job.status.as_str())
            .replace('_', " ");
        details.push(format!("Marketplace: {marketplace_state}"));
        if let Some(marketplace_state) = job.marketplace_state.as_deref()
            && marketplace_state != job.status
        {
            details.push(format!(
                "Certification job state: {}",
                job.status.replace('_', " ")
            ));
        }
        if let Some(language_eligibility) = job.language_eligibility.as_deref() {
            details.push(prism_language_eligibility_detail(language_eligibility));
        }
        if let Some(summary) = job.summary.as_deref() {
            details.push(format!(
                "Certification job: {}",
                prism_truncate_diagnostic(summary, 220)
            ));
        }
        if let Some(last_error) = job.last_error.as_deref() {
            details.push(format!(
                "Certification error: {}",
                prism_truncate_diagnostic(last_error, 220)
            ));
        }
        details.push(format!(
            "Last certification job: {}",
            job.updated_at.format("%Y-%m-%d %H:%M UTC")
        ));
    }
    match certification {
        Some(certification) => {
            if nuvio_certification_is_expired(certification) {
                details.push(format!(
                    "Certification: expired; last result {}",
                    certification.status.replace('_', " ")
                ));
            } else {
                details.push(format!(
                    "Certification: {}",
                    certification.status.replace('_', " ")
                ));
            }
            if let Some(failure_class) = certification.failure_class.as_deref() {
                details.push(format!("Failure class: {failure_class}"));
            }
            if let Some(summary) = certification.summary.as_deref() {
                details.push(format!(
                    "Probe: {}",
                    prism_truncate_diagnostic(summary, 220)
                ));
            }
            details.push(format!(
                "Last certified: {}",
                certification
                    .certified_at
                    .map(|value| value.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ));
            if let Some(artifact_sha256) = certification.artifact_sha256.as_deref() {
                details.push(format!("Certified artifact: {artifact_sha256}"));
            }
            if let Some(expires_at) = certification.expires_at {
                let label = if expires_at <= Utc::now() {
                    "Expired"
                } else {
                    "Certification expires"
                };
                details.push(format!(
                    "{label}: {}",
                    expires_at.format("%Y-%m-%d %H:%M UTC")
                ));
            }
        }
        None => {
            details.push(
                "Certification: unknown; enable runs certification before activation".to_string(),
            );
        }
    }
    details.extend(cloudstream_module_details(module, registry));
    details
}

fn prism_language_eligibility_detail(raw: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return "Language: certification policy recorded".to_string();
    };
    let summary = value
        .get("summary")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("language policy recorded");
    let tags = value
        .get("normalizedTags")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.trim().is_empty());
    match tags {
        Some(tags) => format!("Language: {summary} ({tags})"),
        None => format!("Language: {summary}"),
    }
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
                    "description": "Nuvio repository root URL or direct manifest.json URL.",
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
                    "label": "Trust + certify now",
                    "description": "Required before Prism can install and certify executable scrapers from this repository.",
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

fn prism_certify_enabled_sources_action() -> ExtensionControlAction {
    cloudstream_simple_action(
        "certify_enabled_sources",
        "Certify enabled",
        "Re-run enable-time certification for installed and enabled Prism scrapers.",
        "secondary",
        None,
        Some("Re-run certification for enabled Prism scrapers? Broken scrapers will be disabled."),
    )
}

async fn prism_certification_report(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<Value> {
    if let Some(source_module_id) = params
        .get("sourceModuleId")
        .and_then(serde_json::Value::as_str)
    {
        let source_module_id = Uuid::parse_str(source_module_id)
            .with_context(|| format!("invalid sourceModuleId '{source_module_id}'"))?;
        let certifications = store
            .list_source_module_certifications(source_module_id, 25)
            .await?
            .into_iter()
            .map(prism_certification_report_entry)
            .collect::<Vec<_>>();
        return Ok(json!({
            "sourceModuleId": source_module_id,
            "certifications": certifications,
        }));
    }
    let certifications = store
        .list_latest_source_module_certifications(instance_id)
        .await?
        .into_iter()
        .map(prism_certification_report_entry)
        .collect::<Vec<_>>();
    Ok(json!({
        "instanceId": instance_id,
        "certifications": certifications,
    }))
}

fn prism_certification_report_entry(certification: ExtensionSourceModuleCertification) -> Value {
    json!({
        "certificationId": certification.certification_id,
        "sourceModuleId": certification.source_module_id,
        "sourceModuleVersionId": certification.source_module_version_id,
        "artifactSha256": certification.artifact_sha256,
        "instanceId": certification.instance_id,
        "adapter": certification.adapter,
        "status": certification.status,
        "failureClass": certification.failure_class,
        "summary": certification.summary,
        "mediaTypeResults": certification.media_type_results_json,
        "materializationResults": certification.materialization_results_json,
        "probeTargets": certification.probe_targets_json,
        "candidateEvidence": certification.candidate_evidence_json,
        "runtimeVersion": certification.runtime_version,
        "policyVersion": certification.policy_version,
        "certifiedAt": certification.certified_at,
        "expiresAt": certification.expires_at,
        "createdAt": certification.created_at,
        "updatedAt": certification.updated_at,
    })
}

#[derive(Debug, Clone)]
struct PrismRuntimeSmokeOutcome {
    status: String,
    health_state: String,
    severity: String,
    reason: String,
    failure_class: Option<String>,
    candidate_count: usize,
    materializable_count: usize,
    warnings: Vec<String>,
    media_type_results: Value,
    materialization_results: Value,
    candidate_evidence: Value,
}

impl PrismRuntimeSmokeOutcome {
    fn new(
        status: &str,
        severity: &str,
        reason: impl Into<String>,
        candidate_count: usize,
        warnings: Vec<String>,
    ) -> Self {
        let health_state = prism_certification_status_health_state(status);
        Self {
            status: status.to_string(),
            health_state: health_state.to_string(),
            severity: severity.to_string(),
            reason: reason.into(),
            failure_class: None,
            candidate_count,
            materializable_count: 0,
            warnings,
            media_type_results: json!({}),
            materialization_results: json!({}),
            candidate_evidence: json!([]),
        }
    }

    fn with_failure_class(mut self, failure_class: impl Into<String>) -> Self {
        self.failure_class = Some(failure_class.into());
        self
    }

    fn with_preflight(
        mut self,
        materializable_count: usize,
        media_type_results: Value,
        materialization_results: Value,
        candidate_evidence: Value,
    ) -> Self {
        self.materializable_count = materializable_count;
        self.media_type_results = media_type_results;
        self.materialization_results = materialization_results;
        self.candidate_evidence = candidate_evidence;
        self
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrismRuntimeSmokeResponse {
    #[serde(default)]
    candidates: Vec<Value>,
    #[serde(default)]
    warnings: Vec<String>,
}

async fn smoke_prism_source_module_runtime(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    instance: &ExtensionInstance,
    module: &ExtensionSourceModule,
) -> anyhow::Result<PrismRuntimeSmokeOutcome> {
    let registry = nuvio_find_registry(store, instance.instance_id, module.registry_id).await?;
    let descriptor = prism_source_module_runtime_descriptor(store, module, &registry).await?;
    let module_id = prism_source_module_invocation_key(&descriptor);
    let smoke_requests = prism_runtime_smoke_requests(module);
    let provider = match context.selected_provider.as_ref() {
        Some(provider) => provider,
        None => {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                "Prism runtime provider is not registered for this instance",
                Vec::new(),
            )
            .await;
        }
    };
    let endpoint_json = match provider.endpoint_json.clone() {
        Some(value) => value,
        None => {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                "Prism runtime provider endpoint is missing",
                Vec::new(),
            )
            .await;
        }
    };
    let endpoint: ProviderEndpoint = match serde_json::from_value(endpoint_json) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                format!("Prism runtime provider endpoint is invalid: {err}"),
                Vec::new(),
            )
            .await;
        }
    };
    let base_url =
        match super::resolve_control_provider_transport_base_url(instance.instance_id, &endpoint)
            .await
        {
            Ok(base_url) => base_url,
            Err(err) => {
                return prism_runtime_smoke_failure(
                    store,
                    context,
                    module,
                    &module_id,
                    &smoke_requests,
                    format!("Prism runtime provider transport is unavailable: {err}"),
                    Vec::new(),
                )
                .await;
            }
        };
    let search_url = match prism_provider_search_url(&base_url) {
        Ok(url) => url,
        Err(err) => {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                format!("Prism runtime search URL could not be built: {err}"),
                Vec::new(),
            )
            .await;
        }
    };
    let client = match reqwest::Client::builder()
        .timeout(PRISM_RUNTIME_SMOKE_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                format!("Prism runtime probe client could not be built: {err}"),
                Vec::new(),
            )
            .await;
        }
    };
    let mut outcomes = Vec::new();
    for smoke_request in &smoke_requests {
        let invocation = json!({
            "schemaVersion": PRISM_PROVIDER_SCHEMA_VERSION,
            "request": smoke_request,
            "provider": {
                "providerId": provider.provider_id,
                "extensionId": context.extension.extension_id,
                "instanceId": instance.instance_id,
                "implementation": provider.implementation.as_deref().unwrap_or("prism"),
                "config": {
                    "sourceModules": [descriptor.clone()],
                    "resultLimit": 5,
                    "timeoutMs": PRISM_RUNTIME_SMOKE_PROVIDER_TIMEOUT_MS
                }
            }
        });
        let response = match client
            .post(search_url.clone())
            .json(&invocation)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return prism_runtime_smoke_failure(
                    store,
                    context,
                    module,
                    &module_id,
                    &smoke_requests,
                    format!("Prism runtime probe request failed: {err}"),
                    Vec::new(),
                )
                .await;
            }
        };
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                format!(
                    "Prism runtime probe returned {status}: {}",
                    prism_truncate_diagnostic(&body, 512)
                ),
                Vec::new(),
            )
            .await;
        }
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                return prism_runtime_smoke_failure(
                    store,
                    context,
                    module,
                    &module_id,
                    &smoke_requests,
                    format!("Prism runtime probe response could not be read: {err}"),
                    Vec::new(),
                )
                .await;
            }
        };
        if bytes.len() > 2 * 1024 * 1024 {
            return prism_runtime_smoke_failure(
                store,
                context,
                module,
                &module_id,
                &smoke_requests,
                "Prism runtime probe response exceeded 2 MiB",
                Vec::new(),
            )
            .await;
        }
        let upstream: PrismRuntimeSmokeResponse = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(err) => {
                return prism_runtime_smoke_failure(
                    store,
                    context,
                    module,
                    &module_id,
                    &smoke_requests,
                    format!("Prism runtime probe response was not valid JSON: {err}"),
                    Vec::new(),
                )
                .await;
            }
        };
        outcomes.push(
            certify_prism_runtime_smoke(
                &module_id,
                smoke_request,
                &upstream.candidates,
                &upstream.warnings,
            )
            .await,
        );
    }
    let outcome = aggregate_prism_runtime_smoke_outcomes(&smoke_requests, outcomes);
    record_prism_runtime_smoke_event(
        store,
        context,
        module,
        &module_id,
        &smoke_requests,
        &outcome,
    )
    .await?;
    Ok(outcome)
}

async fn prism_runtime_smoke_failure(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    module: &ExtensionSourceModule,
    module_id: &str,
    probe_targets: &[Value],
    reason: impl Into<String>,
    warnings: Vec<String>,
) -> anyhow::Result<PrismRuntimeSmokeOutcome> {
    let reason = reason.into();
    let failure_class = prism_failure_class_from_diagnostic(&reason);
    let status = prism_certification_status_from_failure(Some(failure_class));
    let media_type_results =
        prism_runtime_failure_media_type_results(probe_targets, failure_class, &reason, &warnings);
    let policy_evidence =
        prism_runtime_policy_evidence("runtime", std::slice::from_ref(&reason), &warnings);
    let outcome = PrismRuntimeSmokeOutcome::new(status, "error", reason, 0, warnings)
        .with_failure_class(failure_class)
        .with_preflight(
            0,
            media_type_results,
            json!({ "policyEvidence": policy_evidence.clone() }),
            Value::Array(policy_evidence),
        );
    record_prism_runtime_smoke_event(store, context, module, module_id, probe_targets, &outcome)
        .await?;
    Ok(outcome)
}

async fn record_prism_runtime_smoke_event(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    module: &ExtensionSourceModule,
    module_id: &str,
    probe_targets: &[Value],
    outcome: &PrismRuntimeSmokeOutcome,
) -> anyhow::Result<()> {
    let active_version = nuvio_active_source_module_version(store, module).await?;
    let source_module_version_id = active_version.as_ref().map(|version| version.version_id);
    let artifact_sha256 = active_version
        .as_ref()
        .and_then(|version| version.artifact_sha256.clone());
    let runtime_version = active_version
        .as_ref()
        .map(|version| version.version.clone())
        .or_else(|| module.active_version.clone());
    let now = Utc::now();
    let policy_version = prism_certification_policy_version();
    store
        .upsert_source_module_certification(&NewExtensionSourceModuleCertification {
            certification_id: Uuid::new_v4(),
            source_module_id: module.source_module_id,
            source_module_version_id,
            artifact_sha256,
            instance_id: module.instance_id,
            adapter: "nuvio_js_v1".to_string(),
            status: outcome.status.clone(),
            failure_class: outcome.failure_class.clone(),
            summary: Some(outcome.reason.clone()),
            media_type_results_json: outcome.media_type_results.clone(),
            materialization_results_json: outcome.materialization_results.clone(),
            probe_targets_json: Value::Array(probe_targets.to_vec()),
            candidate_evidence_json: outcome.candidate_evidence.clone(),
            runtime_version,
            policy_version: policy_version.clone(),
            certified_at: Some(now),
            expires_at: Some(now + chrono::Duration::days(7)),
        })
        .await?;
    store
        .create_source_health_event(&NewExtensionSourceHealthEvent {
            health_event_id: Uuid::new_v4(),
            source_module_id: module.source_module_id,
            event_type: "certification".to_string(),
            state: outcome.health_state.clone(),
            severity: outcome.severity.clone(),
            reason: Some(outcome.reason.clone()),
            evidence_json: Some(json!({
                "providerId": context.selected_provider.as_ref().map(|provider| provider.provider_id),
                "extensionId": context.extension.extension_id,
                "instanceId": context.selected_instance.as_ref().map(|instance| instance.instance_id),
                "moduleId": module_id,
                "certificationStatus": outcome.status.clone(),
                "failureClass": outcome.failure_class.clone(),
                "policyVersion": policy_version,
                "sandboxProfileVersion": PRISM_SANDBOX_PROFILE_VERSION,
                "egressPolicyVersion": PRISM_EGRESS_POLICY_VERSION,
                "mediaTypes": probe_targets.iter().map(prism_smoke_request_media_type).collect::<Vec<_>>(),
                "probeTargets": probe_targets,
                "candidateCount": outcome.candidate_count,
                "materializableCount": outcome.materializable_count,
                "mediaTypeResults": outcome.media_type_results.clone(),
                "materializationResults": outcome.materialization_results.clone(),
                "candidateEvidence": outcome.candidate_evidence.clone(),
                "warnings": outcome.warnings.clone(),
            })),
            observed_at: Some(now),
        })
        .await
}

async fn certify_prism_runtime_smoke(
    module_id: &str,
    smoke_request: &Value,
    candidates: &[Value],
    warnings: &[String],
) -> PrismRuntimeSmokeOutcome {
    let module_warnings = warnings
        .iter()
        .filter_map(|warning| {
            let (warning_module_id, detail) = parse_prism_runtime_warning(warning)?;
            (warning_module_id == module_id).then_some(detail)
        })
        .collect::<Vec<_>>();
    let health_warnings = module_warnings
        .iter()
        .filter(|warning| prism_runtime_warning_is_health_signal(warning))
        .cloned()
        .collect::<Vec<_>>();
    if !health_warnings.is_empty() {
        let reason = health_warnings.join(" | ");
        let (status, severity, failure_class) = prism_runtime_warning_certification(&reason);
        let media_type = prism_smoke_request_media_type(smoke_request);
        let policy_evidence = prism_runtime_policy_evidence(media_type, &health_warnings, &[]);
        let media_type_results = json!({
            media_type: {
                "status": status,
                "failureClass": failure_class,
                "candidateCount": candidates.len(),
                "materializableCount": 0,
                "summary": prism_truncate_diagnostic(&reason, 700),
                "policyEvidence": policy_evidence.clone(),
            }
        });
        return PrismRuntimeSmokeOutcome::new(
            status,
            severity,
            prism_truncate_diagnostic(&reason, 700),
            candidates.len(),
            module_warnings,
        )
        .with_failure_class(failure_class)
        .with_preflight(
            0,
            media_type_results,
            json!({ "policyEvidence": policy_evidence.clone() }),
            Value::Array(policy_evidence),
        );
    }
    let module_candidates = candidates
        .iter()
        .filter(|candidate| {
            let candidate_module_id = candidate
                .pointer("/sourceModule/id")
                .and_then(Value::as_str)
                .map(cloudstream_stable_text_id);
            candidate_module_id
                .as_deref()
                .is_none_or(|candidate_module_id| candidate_module_id == module_id)
        })
        .take(PRISM_CERTIFICATION_MAX_PREFLIGHT_CANDIDATES)
        .cloned()
        .collect::<Vec<_>>();
    if module_candidates.is_empty() {
        let media_type = smoke_request
            .get("mediaType")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let media_type_results = json!({
            media_type: {
                "status": "broken",
                "failureClass": "no_results",
                "candidateCount": 0,
                "materializableCount": 0,
                "summary": "runtime probe completed without stream candidates for the canary title"
            }
        });
        return PrismRuntimeSmokeOutcome::new(
            "broken",
            "error",
            "runtime probe completed without stream candidates for the canary title",
            0,
            module_warnings,
        )
        .with_failure_class("no_results")
        .with_preflight(0, media_type_results, json!({ "inspected": [] }), json!([]));
    }
    let mut reports = Vec::new();
    for candidate in &module_candidates {
        reports.push(preflight_stream_candidate(candidate).await);
    }
    classify_prism_preflight_reports(smoke_request, &module_candidates, reports, module_warnings)
}

fn aggregate_prism_runtime_smoke_outcomes(
    probe_targets: &[Value],
    outcomes: Vec<PrismRuntimeSmokeOutcome>,
) -> PrismRuntimeSmokeOutcome {
    if outcomes.is_empty() {
        return PrismRuntimeSmokeOutcome::new(
            "broken",
            "error",
            "certification did not run any media-type probes",
            0,
            Vec::new(),
        )
        .with_failure_class("no_probes");
    }

    let candidate_count = outcomes
        .iter()
        .map(|outcome| outcome.candidate_count)
        .sum::<usize>();
    let materializable_count = outcomes
        .iter()
        .map(|outcome| outcome.materializable_count)
        .sum::<usize>();
    let warnings = outcomes
        .iter()
        .flat_map(|outcome| outcome.warnings.iter().cloned())
        .collect::<Vec<_>>();
    let eligible_count = outcomes
        .iter()
        .filter(|outcome| prism_certification_is_eligible(&outcome.status))
        .count();
    let status = if eligible_count == outcomes.len() {
        if outcomes
            .iter()
            .any(|outcome| outcome.status.as_str() != "certified")
        {
            "degraded"
        } else {
            "certified"
        }
    } else if eligible_count > 0 {
        "degraded"
    } else {
        outcomes
            .iter()
            .max_by_key(|outcome| prism_certification_failure_priority(&outcome.status))
            .map(|outcome| outcome.status.as_str())
            .unwrap_or("broken")
    };
    let severity = match status {
        "certified" => "info",
        "degraded" | "unsupported" | "account_required" | "network_blocked" => "warning",
        _ => "error",
    };

    let media_type_pairs = probe_targets
        .iter()
        .zip(outcomes.iter())
        .map(|(request, outcome)| (prism_smoke_request_media_type(request), outcome))
        .collect::<Vec<_>>();
    let passed_media = media_type_pairs
        .iter()
        .filter(|(_, outcome)| prism_certification_is_eligible(&outcome.status))
        .map(|(media_type, _)| *media_type)
        .collect::<Vec<_>>();
    let failed_media = media_type_pairs
        .iter()
        .filter(|(_, outcome)| !prism_certification_is_eligible(&outcome.status))
        .map(|(media_type, outcome)| format!("{media_type}: {}", outcome.reason))
        .collect::<Vec<_>>();
    let summary = if failed_media.is_empty() && status == "certified" {
        format!(
            "certification probes returned materializable stream candidates for {}",
            passed_media.join(", ")
        )
    } else if failed_media.is_empty() {
        format!(
            "certification probes returned materializable candidates with degraded evidence for {}",
            passed_media.join(", ")
        )
    } else if passed_media.is_empty() {
        format!("certification failed for {}", failed_media.join("; "))
    } else {
        format!(
            "certification passed for {}; failed for {}",
            passed_media.join(", "),
            failed_media.join("; ")
        )
    };

    let mut media_type_results = serde_json::Map::new();
    let mut materialization_results = Vec::new();
    let mut candidate_evidence = Vec::new();
    for (request, outcome) in probe_targets.iter().zip(outcomes.iter()) {
        let media_type = prism_smoke_request_media_type(request);
        if let Some(results) = outcome.media_type_results.as_object() {
            for (key, value) in results {
                media_type_results.insert(key.clone(), value.clone());
            }
        } else {
            media_type_results.insert(
                media_type.to_string(),
                json!({
                    "status": outcome.status.clone(),
                    "failureClass": outcome.failure_class.clone(),
                    "candidateCount": outcome.candidate_count,
                    "materializableCount": outcome.materializable_count,
                    "summary": outcome.reason.clone(),
                }),
            );
        }
        materialization_results.push(json!({
            "mediaType": media_type,
            "result": outcome.materialization_results.clone(),
        }));
        if let Some(items) = outcome.candidate_evidence.as_array() {
            for item in items {
                let mut item = item.clone();
                if let Some(object) = item.as_object_mut() {
                    object.insert("mediaType".to_string(), json!(media_type));
                }
                candidate_evidence.push(item);
            }
        }
    }

    let mut aggregate = PrismRuntimeSmokeOutcome::new(
        status,
        severity,
        prism_truncate_diagnostic(&summary, 700),
        candidate_count,
        warnings,
    )
    .with_preflight(
        materializable_count,
        Value::Object(media_type_results),
        json!({ "mediaTypes": materialization_results }),
        Value::Array(candidate_evidence),
    );
    if status != "certified" {
        aggregate.failure_class = outcomes
            .iter()
            .find_map(|outcome| outcome.failure_class.clone());
    }
    aggregate
}

fn prism_certification_failure_priority(status: &str) -> u8 {
    match status {
        "broken" => 6,
        "network_blocked" => 5,
        "unsupported" => 4,
        "account_required" => 3,
        "unknown" => 2,
        _ => 1,
    }
}

fn prism_smoke_request_media_type(request: &Value) -> &str {
    request
        .get("mediaType")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

fn classify_prism_preflight_reports(
    smoke_request: &Value,
    candidates: &[Value],
    reports: Vec<StreamCandidatePreflightReport>,
    warnings: Vec<String>,
) -> PrismRuntimeSmokeOutcome {
    let media_type = smoke_request
        .get("mediaType")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let materializable_count = reports.iter().filter(|report| report.passed).count();
    let candidate_count = candidates.len();
    let first_failure = reports.iter().find(|report| !report.passed);
    let failure_class = first_failure
        .and_then(|report| report.failure_class.clone())
        .or_else(|| (materializable_count == 0).then(|| "no_results".to_string()));
    let status = if materializable_count == 0 {
        prism_certification_status_from_failure(failure_class.as_deref())
    } else if materializable_count < candidate_count {
        "degraded"
    } else {
        "certified"
    };
    let severity = match status {
        "certified" => "info",
        "degraded" | "unsupported" | "account_required" | "network_blocked" => "warning",
        _ => "error",
    };
    let summary = match (status, materializable_count, first_failure) {
        ("certified", count, _) => {
            format!("certification probe returned {count} materializable stream candidate(s)")
        }
        ("degraded", count, Some(report)) => format!(
            "certification found {count} materializable candidate(s), but at least one candidate failed preflight: {}",
            report.summary
        ),
        (_, 0, Some(report)) => report.summary.clone(),
        _ => "certification probe did not find a materializable stream candidate".to_string(),
    };
    let media_type_results = json!({
        media_type: {
            "status": status,
            "failureClass": failure_class,
            "candidateCount": candidate_count,
            "materializableCount": materializable_count,
            "summary": summary,
        }
    });
    let inspected = reports
        .iter()
        .map(StreamCandidatePreflightReport::evidence_json)
        .collect::<Vec<_>>();
    let candidate_evidence = candidates
        .iter()
        .zip(reports.iter())
        .enumerate()
        .map(|(index, (candidate, report))| {
            json!({
                "index": index,
                "title": candidate.get("title").cloned().unwrap_or(Value::Null),
                "quality": candidate.get("quality").cloned().unwrap_or(Value::Null),
                "sourceModule": candidate.get("sourceModule").cloned().unwrap_or(Value::Null),
                "delivery": prism_certification_delivery_evidence(candidate),
                "preflight": report,
            })
        })
        .collect::<Vec<_>>();
    let mut outcome = PrismRuntimeSmokeOutcome::new(
        status,
        severity,
        prism_truncate_diagnostic(&summary, 700),
        candidate_count,
        warnings,
    )
    .with_preflight(
        materializable_count,
        media_type_results,
        json!({ "inspected": inspected }),
        Value::Array(candidate_evidence),
    );
    outcome.failure_class = failure_class;
    outcome
}

fn prism_certification_delivery_evidence(candidate: &Value) -> Value {
    let Some(delivery) = candidate.get("delivery").and_then(Value::as_object) else {
        return Value::Null;
    };
    let mut object = serde_json::Map::new();
    for key in ["streamType", "resolveRequired", "expiresAt"] {
        if let Some(value) = delivery.get(key) {
            object.insert(key.to_string(), value.clone());
        }
    }
    if let Some(url) = delivery.get("url").and_then(Value::as_str) {
        object.insert(
            "url".to_string(),
            Value::String(prism_redacted_url_for_evidence(url)),
        );
    }
    if let Some(referer) = delivery.get("referer").and_then(Value::as_str) {
        object.insert(
            "referer".to_string(),
            Value::String(prism_redacted_url_for_evidence(referer)),
        );
    }
    if let Some(headers) = delivery.get("headers").and_then(Value::as_object) {
        object.insert(
            "headerNames".to_string(),
            Value::Array(headers.keys().cloned().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

fn prism_redacted_url_for_evidence(value: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(value) else {
        return "<invalid-url>".to_string();
    };
    if url.query().is_some() {
        url.set_query(Some("redacted=1"));
    }
    url.to_string()
}

fn parse_prism_runtime_warning(warning: &str) -> Option<(String, String)> {
    let trimmed = warning.trim();
    let prism_prefix = trimmed
        .strip_prefix("prism:")
        .or_else(|| trimmed.split_once(":prism:").map(|(_, rest)| rest))?;
    let (module_id, detail) = prism_prefix.split_once(':')?;
    let module_id = cloudstream_stable_text_id(module_id);
    if matches!(module_id.as_str(), "source" | "no-source-modules") {
        return None;
    }
    let detail = detail.trim();
    if detail.is_empty() {
        return None;
    }
    Some((module_id, detail.to_string()))
}

fn prism_runtime_failure_media_type_results(
    probe_targets: &[Value],
    failure_class: &str,
    reason: &str,
    warnings: &[String],
) -> Value {
    let targets = if probe_targets.is_empty() {
        vec!["runtime"]
    } else {
        probe_targets
            .iter()
            .map(prism_smoke_request_media_type)
            .collect::<Vec<_>>()
    };
    let diagnostics = vec![reason.to_string()];
    let mut object = serde_json::Map::new();
    for media_type in targets {
        object.insert(
            media_type.to_string(),
            json!({
                "status": prism_certification_status_from_failure(Some(failure_class)),
                "failureClass": failure_class,
                "candidateCount": 0,
                "materializableCount": 0,
                "summary": prism_truncate_diagnostic(reason, 700),
                "policyEvidence": prism_runtime_policy_evidence(media_type, &diagnostics, warnings),
            }),
        );
    }
    Value::Object(object)
}

fn prism_runtime_policy_evidence(
    media_type: &str,
    diagnostics: &[String],
    warnings: &[String],
) -> Vec<Value> {
    diagnostics
        .iter()
        .chain(warnings.iter())
        .filter_map(|diagnostic| prism_runtime_policy_evidence_entry(media_type, diagnostic))
        .collect()
}

fn prism_runtime_policy_evidence_entry(media_type: &str, diagnostic: &str) -> Option<Value> {
    let normalized = diagnostic.to_ascii_lowercase();
    let mut object = serde_json::Map::new();
    object.insert("mediaType".to_string(), json!(media_type));
    object.insert(
        "summary".to_string(),
        json!(prism_truncate_diagnostic(diagnostic, 500)),
    );
    object.insert(
        "sandboxProfileVersion".to_string(),
        json!(PRISM_SANDBOX_PROFILE_VERSION),
    );
    object.insert(
        "egressPolicyVersion".to_string(),
        json!(PRISM_EGRESS_POLICY_VERSION),
    );

    if let Some(destination) = prism_private_destination_from_diagnostic(diagnostic) {
        object.insert("kind".to_string(), json!("egress_block"));
        object.insert("failureClass".to_string(), json!(FAILURE_NETWORK_BLOCKED));
        object.insert("destination".to_string(), json!(destination));
        if let Some(resolved_ip) = prism_resolved_ip_from_diagnostic(diagnostic) {
            object.insert("resolvedIp".to_string(), json!(resolved_ip));
        }
        return Some(Value::Object(object));
    }

    if (normalized.contains("module '")
        && normalized.contains("is not available in the prism sandbox"))
        || normalized.contains("process is not defined")
        || normalized.contains("code generation from strings disallowed")
        || normalized.contains("wasm")
        || normalized.contains("webassembly")
    {
        object.insert("kind".to_string(), json!("sandbox_block"));
        object.insert(
            "failureClass".to_string(),
            json!("sandbox_policy_violation"),
        );
        return Some(Value::Object(object));
    }

    if normalized.contains("buffer allocation exceeded")
        || normalized.contains("response exceeded byte limit")
        || normalized.contains("exceeded 2 mib")
    {
        object.insert("kind".to_string(), json!("resource_limit"));
        object.insert("failureClass".to_string(), json!("resource_limit_exceeded"));
        return Some(Value::Object(object));
    }

    if normalized.contains("timed out") || normalized.contains("timeout") {
        object.insert("kind".to_string(), json!("timeout"));
        object.insert("failureClass".to_string(), json!(FAILURE_NETWORK_BLOCKED));
        return Some(Value::Object(object));
    }

    None
}

fn prism_private_destination_from_diagnostic(diagnostic: &str) -> Option<String> {
    let normalized = diagnostic.to_ascii_lowercase();
    let marker = "blocked private network destination:";
    let index = normalized.find(marker)?;
    let start = index + marker.len();
    diagnostic
        .get(start..)?
        .trim()
        .split_whitespace()
        .next()
        .map(|value| {
            value
                .trim_matches(|ch: char| ch == ',' || ch == ';')
                .to_string()
        })
        .filter(|value| !value.is_empty())
}

fn prism_resolved_ip_from_diagnostic(diagnostic: &str) -> Option<String> {
    let normalized = diagnostic.to_ascii_lowercase();
    let marker = " resolved to ";
    let index = normalized.find(marker)?;
    let start = index + marker.len();
    diagnostic
        .get(start..)?
        .trim()
        .split_whitespace()
        .next()
        .map(|value| {
            value
                .trim_matches(|ch: char| ch == ',' || ch == ';')
                .to_string()
        })
        .filter(|value| !value.is_empty())
}

fn prism_runtime_warning_is_health_signal(reason: &str) -> bool {
    let normalized = reason.to_ascii_lowercase();
    !normalized.contains("tmdb id is unavailable") && !normalized.contains("skipped because")
}

fn prism_runtime_warning_certification(reason: &str) -> (&'static str, &'static str, &'static str) {
    let normalized = reason.to_ascii_lowercase();
    if normalized.contains("blocked private network destination") {
        return ("network_blocked", "warning", FAILURE_NETWORK_BLOCKED);
    }
    if normalized.contains("unsupported") {
        return (
            "unsupported",
            "warning",
            FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
        );
    }
    if normalized.contains("account") && normalized.contains("not configured") {
        return ("account_required", "warning", FAILURE_ACCOUNT_REQUIRED);
    }
    if normalized.contains("runtime_error")
        || normalized.contains("scraping error")
        || normalized.contains("fetch failed")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
        || normalized.contains("enotfound")
        || normalized.contains("econn")
        || normalized.contains("404")
        || normalized.contains("returned html")
        || normalized.contains("is not installed")
        || normalized.contains("outside configured roots")
    {
        return (
            "broken",
            "error",
            prism_failure_class_from_diagnostic(reason),
        );
    }
    (
        "degraded",
        "warning",
        FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
    )
}

fn prism_failure_class_from_diagnostic(reason: &str) -> &'static str {
    let normalized = reason.to_ascii_lowercase();
    if normalized.contains("blocked private network destination") {
        FAILURE_NETWORK_BLOCKED
    } else if normalized.contains("account") || normalized.contains("login") {
        FAILURE_ACCOUNT_REQUIRED
    } else if normalized.contains("captcha")
        || normalized.contains("cloudflare")
        || normalized.contains("browser")
    {
        FAILURE_CAPTCHA_OR_BROWSER_REQUIRED
    } else if normalized.contains("unsafe") {
        FAILURE_UNSAFE_URL
    } else if normalized.contains("html")
        || normalized.contains("<!doctype")
        || normalized.contains("json")
        || normalized.contains("xml")
    {
        FAILURE_SOURCE_RETURNED_NON_MEDIA_RESPONSE
    } else if normalized.contains("timed out")
        || normalized.contains("timeout")
        || normalized.contains("enotfound")
        || normalized.contains("econn")
        || normalized.contains("tls")
        || normalized.contains("dns")
        || normalized.contains("fetch failed")
    {
        FAILURE_NETWORK_BLOCKED
    } else if normalized.contains("shape") || normalized.contains("getstreams") {
        FAILURE_INVALID_CANDIDATE_SHAPE
    } else {
        "runtime_exception"
    }
}

fn prism_certification_status_from_failure(failure_class: Option<&str>) -> &'static str {
    match failure_class {
        Some(class) if class == FAILURE_ACCOUNT_REQUIRED => "account_required",
        Some(class)
            if class == FAILURE_CAPTCHA_OR_BROWSER_REQUIRED
                || class == FAILURE_DRM_OR_LICENSE_REQUIRED
                || class == FAILURE_HOSTER_RESOLVER_MISSING
                || class == FAILURE_SOURCE_RETURNED_NON_MEDIA_RESPONSE =>
        {
            "unsupported"
        }
        Some(class) if class == FAILURE_NETWORK_BLOCKED => "network_blocked",
        Some(class) if class == FAILURE_MATERIALIZATION_PREFLIGHT_FAILED => "broken",
        Some(class) if class == FAILURE_INVALID_CANDIDATE_SHAPE || class == FAILURE_UNSAFE_URL => {
            "broken"
        }
        Some("no_results") => "broken",
        Some(_) | None => "broken",
    }
}

fn prism_certification_status_health_state(status: &str) -> &'static str {
    match status {
        "certified" => "healthy",
        "degraded" | "probation" => "degraded",
        "unsupported" => "unsupported",
        "account_required" => "account_required",
        "network_blocked" | "broken" => "broken",
        _ => "unknown",
    }
}

fn prism_certification_is_eligible(status: &str) -> bool {
    matches!(status, "certified" | "degraded" | "probation")
}

fn prism_provider_search_url(base_url: &str) -> anyhow::Result<reqwest::Url> {
    let mut base = reqwest::Url::parse(base_url).context("parsing Prism provider base URL")?;
    let mut path = base.path().trim_end_matches('/').to_string();
    path.push('/');
    base.set_path(&path);
    base.join(PRISM_PROVIDER_SEARCH_PATH)
        .context("building Prism provider search URL")
}

fn prism_runtime_smoke_requests(module: &ExtensionSourceModule) -> Vec<Value> {
    prism_runtime_smoke_media_types(module)
        .into_iter()
        .map(prism_runtime_smoke_request_for_media_type)
        .collect()
}

fn prism_runtime_smoke_request(module: &ExtensionSourceModule) -> Value {
    let media_type = prism_runtime_smoke_media_types(module)
        .into_iter()
        .next()
        .unwrap_or("movie");
    prism_runtime_smoke_request_for_media_type(media_type)
}

fn prism_runtime_smoke_request_for_media_type(media_type: &str) -> Value {
    match media_type {
        "anime" => json!({
            "mediaType": "anime",
            "title": "Cowboy Bebop",
            "year": 1998,
            "externalIds": {
                "tmdb": "30991",
                "tmdbSeries": "30991",
                "imdb": "tt0213338"
            },
            "titles": [
                { "value": "Cowboy Bebop", "kind": "primary" }
            ],
            "targets": [
                {
                    "targetKey": "s01e01",
                    "title": "Asteroid Blues",
                    "seasonNumber": 1,
                    "episodeNumber": 1
                }
            ],
            "preferences": {},
            "limit": 5
        }),
        "tv" => json!({
            "mediaType": "tv",
            "title": "Breaking Bad",
            "year": 2008,
            "externalIds": {
                "tmdb": "1396",
                "tmdbSeries": "1396",
                "imdb": "tt0903747",
                "tvdb": "81189"
            },
            "titles": [
                { "value": "Breaking Bad", "kind": "primary" }
            ],
            "targets": [
                {
                    "targetKey": "s01e01",
                    "title": "Pilot",
                    "seasonNumber": 1,
                    "episodeNumber": 1
                }
            ],
            "preferences": {},
            "limit": 5
        }),
        _ => json!({
            "mediaType": "movie",
            "title": "Batman: Mask of the Phantasm",
            "year": 1993,
            "externalIds": {
                "tmdb": "14919",
                "tmdbMovie": "14919",
                "imdb": "tt0106364"
            },
            "titles": [
                { "value": "Batman: Mask of the Phantasm", "kind": "primary" }
            ],
            "targets": [
                {
                    "targetKey": "movie",
                    "title": "Batman: Mask of the Phantasm"
                }
            ],
            "preferences": {},
            "limit": 5
        }),
    }
}

fn prism_runtime_smoke_media_types(module: &ExtensionSourceModule) -> Vec<&'static str> {
    let media_types = module
        .media_types_json
        .as_ref()
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    if media_types.is_empty() || media_types.contains("all") {
        return vec!["movie", "tv", "anime"];
    }
    let mut selected = Vec::new();
    if media_types.contains("movie") {
        selected.push("movie");
    }
    if media_types.contains("tv") || media_types.contains("series") {
        selected.push("tv");
    }
    if media_types.contains("anime") {
        selected.push("anime");
    }
    if selected.is_empty() {
        selected.push("movie");
    }
    selected
}

async fn prism_source_module_runtime_descriptor(
    store: &ExtensionStore<'_>,
    module: &ExtensionSourceModule,
    registry: &ExtensionSourceRegistry,
) -> anyhow::Result<Value> {
    let metadata = module.metadata_json.as_ref();
    let nuvio = metadata
        .and_then(|value| value.get("nuvio"))
        .and_then(Value::as_object);
    let active_version = nuvio_active_source_module_version(store, module).await?;
    let active_version_metadata = active_version
        .as_ref()
        .and_then(|version| version.metadata_json.as_ref());

    let mut descriptor = serde_json::Map::new();
    let module_id = nuvio_invocation_module_id(module);
    descriptor.insert("id".to_string(), json!(module_id));
    descriptor.insert("name".to_string(), json!(module.display_name));
    descriptor.insert("type".to_string(), json!(module.ecosystem));
    descriptor.insert(
        "adapter".to_string(),
        json!(
            nuvio
                .and_then(|value| value.get("adapter"))
                .and_then(Value::as_str)
                .unwrap_or("nuvio_js_v1")
        ),
    );
    descriptor.insert("enabled".to_string(), json!(true));
    descriptor.insert("installed".to_string(), json!(true));
    descriptor.insert(
        "requiresAccount".to_string(),
        json!(module.account_required),
    );
    descriptor.insert(
        "accountConfigured".to_string(),
        json!(!module.account_required),
    );
    descriptor.insert("registryKey".to_string(), json!(registry.registry_key));
    descriptor.insert("registryType".to_string(), json!(registry.registry_type));
    descriptor.insert("trustClass".to_string(), json!(registry.trust_class));
    descriptor.insert(
        "trustedForExecutableUpdates".to_string(),
        json!(registry.trusted_for_executable_updates),
    );
    descriptor.insert("healthState".to_string(), json!("available"));
    if let Some(value) = module.media_types_json.clone() {
        descriptor.insert("mediaTypes".to_string(), value);
    }
    if let Some(value) = module.language_tags_json.clone() {
        descriptor.insert("languageTags".to_string(), value);
    }
    if let Some(value) = module.source_domains_json.clone() {
        descriptor.insert("sourceDomains".to_string(), value);
    }
    if let Some(version) = active_version.as_ref() {
        descriptor.insert("activeVersion".to_string(), json!(version.version));
        descriptor.insert("version".to_string(), json!(version.version));
        if let Some(value) = version.artifact_url.as_deref() {
            descriptor.insert("artifactUrl".to_string(), json!(value));
        }
    } else if let Some(value) = module.active_version.as_deref() {
        descriptor.insert("activeVersion".to_string(), json!(value));
        descriptor.insert("version".to_string(), json!(value));
    }
    if let Some(value) = module.rollback_version.as_deref() {
        descriptor.insert("rollbackVersion".to_string(), json!(value));
    }
    if let Some(value) = module.pinned_version.as_deref() {
        descriptor.insert("pinnedVersion".to_string(), json!(value));
    }
    if let Some(value) = module.last_error.as_deref() {
        descriptor.insert("lastError".to_string(), json!(value));
    }
    if module.unsupported {
        descriptor.insert(
            "unsupportedReason".to_string(),
            json!(
                module
                    .unsupported_reason
                    .as_deref()
                    .unwrap_or("unsupported by Elixir source registry")
            ),
        );
    }
    if let Some(value) = nuvio.and_then(|value| value.get("moduleId")).cloned() {
        descriptor.insert("moduleId".to_string(), value);
    }
    if let Some(value) = nuvio.and_then(|value| value.get("hasSettings")).cloned() {
        descriptor.insert("hasSettings".to_string(), value);
    }
    if let Some(value) = nuvio.and_then(|value| value.get("formats")).cloned() {
        descriptor.insert("formats".to_string(), value);
    }
    if let Some(value) = active_version_metadata
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|value| value.get("scriptPath"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("containerPath"))
                .cloned()
        })
    {
        descriptor.insert("scriptPath".to_string(), value);
    }
    if let Some(value) = active_version_metadata
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|value| value.get("artifactSha256"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("sha256"))
                .cloned()
        })
    {
        descriptor.insert("artifactSha256".to_string(), value);
    }
    Ok(Value::Object(descriptor))
}

fn prism_source_module_invocation_key(value: &Value) -> String {
    value
        .get("id")
        .or_else(|| value.get("moduleId"))
        .or_else(|| value.get("sourceModuleId"))
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .map(cloudstream_stable_text_id)
        .unwrap_or_else(|| "source".to_string())
}

fn nuvio_invocation_module_id(module: &ExtensionSourceModule) -> String {
    module
        .metadata_json
        .as_ref()
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|nuvio| nuvio.get("moduleId"))
        .and_then(Value::as_str)
        .map(cloudstream_stable_text_id)
        .unwrap_or_else(|| {
            module
                .module_key
                .rsplit(':')
                .next()
                .map(cloudstream_stable_text_id)
                .unwrap_or_else(|| cloudstream_stable_text_id(&module.display_name))
        })
}

async fn nuvio_active_source_module_version(
    store: &ExtensionStore<'_>,
    module: &ExtensionSourceModule,
) -> anyhow::Result<Option<ExtensionSourceModuleVersion>> {
    let versions = store
        .list_source_module_versions(module.source_module_id)
        .await?;
    if let Some(active) = module.active_version.as_deref() {
        if let Some(version) = versions.iter().find(|version| version.version == active) {
            return Ok(Some(version.clone()));
        }
    }
    Ok(versions
        .iter()
        .find(|version| version.install_state == "active")
        .or_else(|| {
            versions
                .iter()
                .find(|version| version.install_state == "installed")
        })
        .cloned())
}

fn prism_truncate_diagnostic(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(max_chars) {
        out.push(ch);
    }
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

async fn nuvio_add_custom_repo(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<(Uuid, String)> {
    let url = cloudstream_param_string(params, "registryUrl")?;
    let display_name = params
        .get("displayName")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let trusted = cloudstream_param_bool(params, "trustedForExecutableUpdates", false)?;
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
            trust_class: "custom".to_string(),
            display_name,
            url: Some(url),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: trusted,
        },
        &snapshot,
    )
    .await?;
    Ok((
        registry_id,
        format!(
            "Added Nuvio repository '{}': {} module(s), {} version(s), {} disabled.",
            registry_key, summary.modules, summary.versions, summary.disabled_modules
        ),
    ))
}

async fn nuvio_refresh_registry(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    registry_id: Uuid,
) -> anyhow::Result<(Uuid, String)> {
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
    Ok((
        registry_id,
        format!(
            "Refreshed '{}': {} module(s), {} version(s), {} disabled.",
            registry.display_name, summary.modules, summary.versions, summary.disabled_modules
        ),
    ))
}

#[derive(Debug, Default, Clone)]
struct PrismRepositoryCertificationSummary {
    discovered: usize,
    processed: usize,
    certified: usize,
    degraded: usize,
    blocked: usize,
    failed: usize,
    skipped_language: usize,
    skipped_trust: usize,
    skipped_policy: usize,
    skipped_account: usize,
    skipped_unsupported: usize,
    skipped_unavailable: usize,
    capped: usize,
}

impl PrismRepositoryCertificationSummary {
    fn message(&self, registry_name: &str) -> String {
        format!(
            "Prism repository '{}' certification queued: {} discovered, {} queued, {} already certified, {} degraded, {} blocked, {} failed, {} skipped language, {} skipped trust, {} skipped policy, {} skipped account-required, {} skipped unsupported, {} unavailable, {} capped.",
            registry_name,
            self.discovered,
            self.processed,
            self.certified,
            self.degraded,
            self.blocked,
            self.failed,
            self.skipped_language,
            self.skipped_trust,
            self.skipped_policy,
            self.skipped_account,
            self.skipped_unsupported,
            self.skipped_unavailable,
            self.capped,
        )
    }
}

async fn prism_enqueue_repository_certification_jobs(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    registry_id: Uuid,
    requested_by: &str,
    reason: &str,
    manual: bool,
) -> anyhow::Result<PrismRepositoryCertificationSummary> {
    let registry = nuvio_find_registry(store, instance.instance_id, registry_id).await?;
    if !registry.enabled {
        if manual {
            anyhow::bail!(
                "Prism source repository '{}' is disabled",
                registry.display_name
            );
        }
        return Ok(PrismRepositoryCertificationSummary::default());
    }
    let policy = prism_marketplace_policy(instance);
    let latest_certifications = store
        .list_latest_source_module_certifications(instance.instance_id)
        .await?
        .into_iter()
        .map(|certification| (certification.source_module_id, certification))
        .collect::<BTreeMap<_, _>>();
    let all_modules = store
        .list_source_modules(Some(instance.instance_id), None)
        .await?
        .into_iter()
        .filter(|module| {
            module.registry_id == registry_id
                && (module.ecosystem == "nuvio" || module.ecosystem == "stremio")
        })
        .collect::<Vec<_>>();
    let mut summary = PrismRepositoryCertificationSummary {
        discovered: all_modules.len(),
        capped: all_modules
            .len()
            .saturating_sub(policy.max_auto_certify_modules_per_repo),
        ..Default::default()
    };
    let modules = all_modules
        .into_iter()
        .take(policy.max_auto_certify_modules_per_repo)
        .collect::<Vec<_>>();

    let custom_untrusted = registry.trust_class == "custom"
        && !registry.trusted_for_executable_updates
        && registry.registry_type != "elixir_curated_nuvio_pack";
    if custom_untrusted {
        for module in modules {
            let eligibility = prism_module_language_eligibility(&module, &policy);
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "skipped_trust",
                "Repository must be trusted before Prism installs executable scraper code.",
                Some(&eligibility),
            )
            .await?;
            summary.skipped_trust += 1;
        }
        return Ok(summary);
    }

    if !manual {
        let auto_allowed = if registry.trust_class == "custom" {
            policy.auto_certify_custom_repositories == "after_trust"
        } else {
            policy.auto_certify_trusted_repositories
        };
        if !auto_allowed {
            for module in modules {
                let eligibility = prism_module_language_eligibility(&module, &policy);
                prism_record_repository_certification_skip(
                    store,
                    instance,
                    &registry,
                    &module,
                    requested_by,
                    reason,
                    "skipped_policy",
                    "Repository auto-certification is disabled by Prism marketplace policy.",
                    Some(&eligibility),
                )
                .await?;
                summary.skipped_policy += 1;
            }
            return Ok(summary);
        }
    }

    for module in modules {
        if module.unsupported {
            let eligibility = prism_module_language_eligibility(&module, &policy);
            let skip_reason = module
                .unsupported_reason
                .as_deref()
                .unwrap_or("unsupported by Prism");
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "unsupported",
                skip_reason,
                Some(&eligibility),
            )
            .await?;
            store
                .set_source_module_enabled_state(
                    module.source_module_id,
                    false,
                    "unsupported",
                    Some(skip_reason),
                )
                .await?;
            summary.skipped_unsupported += 1;
            continue;
        }
        if module.account_required {
            let eligibility = prism_module_language_eligibility(&module, &policy);
            let reason = "Scraper requires an account before Prism can certify it.";
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "account_required",
                reason,
                Some(&eligibility),
            )
            .await?;
            store
                .set_source_module_enabled_state(
                    module.source_module_id,
                    false,
                    "account_required",
                    Some(reason),
                )
                .await?;
            summary.skipped_account += 1;
            continue;
        }

        let eligibility = prism_module_language_eligibility(&module, &policy);
        if !eligibility.certifiable {
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                &eligibility.state,
                &eligibility.summary,
                Some(&eligibility),
            )
            .await?;
            summary.skipped_language += 1;
            continue;
        }

        if let Err(err) =
            nuvio_validate_module_activation(store, instance.instance_id, &module).await
        {
            let message = err.to_string();
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "skipped_policy",
                &message,
                Some(&eligibility),
            )
            .await?;
            summary.skipped_policy += 1;
            continue;
        }

        let versions = store
            .list_source_module_versions(module.source_module_id)
            .await?;
        let Some(version) = cloudstream_preferred_module_version(&module, &versions, None) else {
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "unavailable",
                "Scraper has no available version to install.",
                Some(&eligibility),
            )
            .await?;
            summary.skipped_unavailable += 1;
            continue;
        };
        if !versions
            .iter()
            .any(|candidate| candidate.version == version)
        {
            prism_record_repository_certification_skip(
                store,
                instance,
                &registry,
                &module,
                requested_by,
                reason,
                "unavailable",
                "Selected scraper version was not present in the repository snapshot.",
                Some(&eligibility),
            )
            .await?;
            summary.skipped_unavailable += 1;
            continue;
        }
        if !manual
            && latest_certifications
                .get(&module.source_module_id)
                .is_some_and(|certification| {
                    certification.runtime_version.as_deref() == Some(version.as_str())
                        && certification.policy_version == prism_certification_policy_version()
                        && certification
                            .expires_at
                            .is_none_or(|expires_at| expires_at > Utc::now())
                        && prism_certification_is_eligible(&certification.status)
                })
        {
            summary.certified += 1;
            continue;
        }

        let job_id = Uuid::new_v4();
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id,
                instance_id: instance.instance_id,
                registry_id: Some(registry.registry_id),
                source_module_id: Some(module.source_module_id),
                requested_by: requested_by.to_string(),
                reason: reason.to_string(),
                status: "queued".to_string(),
                priority: 100,
                attempts: 0,
                max_attempts: 2,
                language_eligibility: Some(prism_language_eligibility_json(&eligibility)),
                marketplace_state: Some("certifying".to_string()),
                summary: Some(format!("Queued certification for version {version}.")),
                last_error: None,
            })
            .await?;
        summary.processed += 1;
    }

    Ok(summary)
}

fn prism_spawn_certification_worker(
    state: AppState,
    context: ExtensionControlContext,
    instance_id: Uuid,
) {
    tokio::spawn(async move {
        if let Err(err) = prism_run_certification_worker(state, context, instance_id).await {
            tracing::warn!(
                instance_id = %instance_id,
                error = %err,
                "Prism certification worker stopped with error"
            );
        }
    });
}

pub(super) async fn resume_prism_certification_jobs(state: AppState) -> anyhow::Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let context = super::load_extension_control_context(&state, &store, PRISM_EXTENSION_ID).await?;
    let Some(instance) = context.selected_instance.as_ref() else {
        return Ok(());
    };
    let instance_id = instance.instance_id;
    let requeued = store
        .requeue_running_source_certification_jobs(
            instance_id,
            "server restarted before Prism certification finished",
        )
        .await?;
    if requeued > 0 {
        tracing::info!(
            instance_id = %instance_id,
            jobs = requeued,
            "requeued interrupted Prism certification jobs"
        );
    }
    prism_spawn_certification_worker(state, context, instance_id);
    Ok(())
}

async fn prism_run_certification_worker(
    state: AppState,
    context: ExtensionControlContext,
    instance_id: Uuid,
) -> anyhow::Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    loop {
        let Some(job) = store
            .claim_next_source_certification_job(instance_id)
            .await?
        else {
            break;
        };
        if let Err(err) =
            prism_process_certification_job(&state, &store, &context, job.clone()).await
        {
            let message = prism_truncate_diagnostic(&err.to_string(), 700);
            store
                .finish_source_certification_job(
                    job.job_id,
                    "failed",
                    Some("broken"),
                    Some(&message),
                    Some(&message),
                )
                .await?;
            tracing::warn!(
                job_id = %job.job_id,
                source_module_id = ?job.source_module_id,
                error = %err,
                "Prism certification job failed"
            );
        }
    }
    Ok(())
}

async fn prism_process_certification_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    job: ExtensionSourceCertificationJob,
) -> anyhow::Result<()> {
    let instance = prism_find_instance(store, job.instance_id).await?;
    let Some(source_module_id) = job.source_module_id else {
        anyhow::bail!("Prism certification job has no source module id");
    };
    let module = nuvio_find_module(store, job.instance_id, source_module_id).await?;
    if module.unsupported {
        let reason = module
            .unsupported_reason
            .as_deref()
            .unwrap_or("unsupported by Prism");
        store
            .set_source_module_enabled_state(
                module.source_module_id,
                false,
                "unsupported",
                Some(reason),
            )
            .await?;
        store
            .finish_source_certification_job(
                job.job_id,
                "skipped",
                Some("unsupported"),
                Some(reason),
                None,
            )
            .await?;
        return Ok(());
    }
    if module.account_required {
        let reason = "Scraper requires an account before Prism can certify it.";
        store
            .set_source_module_enabled_state(
                module.source_module_id,
                false,
                "account_required",
                Some(reason),
            )
            .await?;
        store
            .finish_source_certification_job(
                job.job_id,
                "skipped",
                Some("account_required"),
                Some(reason),
                None,
            )
            .await?;
        return Ok(());
    }
    nuvio_validate_module_activation(store, job.instance_id, &module).await?;
    let policy = prism_marketplace_policy(&instance);
    let versions = store
        .list_source_module_versions(module.source_module_id)
        .await?;
    let version = cloudstream_preferred_module_version(&module, &versions, None)
        .ok_or_else(|| anyhow::anyhow!("'{}' has no available version", module.display_name))?;
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

    if let Err(err) = install_source_module_artifact(
        store,
        &state.settings.extensions.storage_root,
        &module,
        version_record,
    )
    .await
    {
        let message = prism_truncate_diagnostic(&err.to_string(), 700);
        if !policy.retain_failed_artifacts {
            let _ = remove_source_module_artifacts(store, &module, &message).await;
        } else {
            store
                .set_source_module_installed_state(
                    module.source_module_id,
                    false,
                    None,
                    "broken",
                    Some(&message),
                )
                .await?;
        }
        store
            .finish_source_certification_job(
                job.job_id,
                "failed",
                Some("broken"),
                Some(&message),
                Some(&message),
            )
            .await?;
        return Ok(());
    }

    store
        .set_source_module_active_version(
            module.source_module_id,
            Some(&version),
            module.active_version.as_deref(),
        )
        .await?;
    let versions = store
        .list_source_module_versions(module.source_module_id)
        .await?;
    cloudstream_mark_active_version(store, &versions, &version).await?;
    let module = nuvio_find_module(store, job.instance_id, module.source_module_id).await?;
    let outcome = match smoke_prism_source_module_runtime(store, context, &instance, &module).await
    {
        Ok(outcome) => outcome,
        Err(err) => PrismRuntimeSmokeOutcome::new(
            "broken",
            "error",
            prism_truncate_diagnostic(&err.to_string(), 700),
            0,
            Vec::new(),
        )
        .with_failure_class("runtime_error"),
    };
    if store
        .get_source_certification_job(job.job_id)
        .await?
        .is_some_and(|current| current.status == "cancelled")
    {
        if !policy.retain_failed_artifacts {
            let _ = remove_source_module_artifacts(store, &module, "certification cancelled").await;
        }
        return Ok(());
    }
    let marketplace_state = prism_marketplace_state_from_certification_status(&outcome.status);
    if prism_certification_is_eligible(&outcome.status) {
        // Repository certification is a trusted batch install path. Once a module
        // passes certification, it should be immediately usable in Prism searches.
        store
            .set_source_module_enabled_state(
                module.source_module_id,
                true,
                &outcome.health_state,
                outcome.failure_class.as_deref(),
            )
            .await?;
        store
            .finish_source_certification_job(
                job.job_id,
                if outcome.status == "degraded" {
                    "degraded"
                } else {
                    "succeeded"
                },
                Some(marketplace_state),
                Some(&outcome.reason),
                None,
            )
            .await?;
    } else {
        if !policy.retain_failed_artifacts {
            let _ = remove_source_module_artifacts(store, &module, &outcome.reason).await;
        }
        store
            .set_source_module_enabled_state(
                module.source_module_id,
                false,
                &outcome.health_state,
                Some(&outcome.reason),
            )
            .await?;
        store
            .finish_source_certification_job(
                job.job_id,
                "blocked",
                Some(marketplace_state),
                Some(&outcome.reason),
                Some(&outcome.reason),
            )
            .await?;
    }
    Ok(())
}

async fn prism_find_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<ExtensionInstance> {
    store
        .list_instances(None)
        .await?
        .into_iter()
        .find(|instance| instance.instance_id == instance_id)
        .ok_or_else(|| anyhow::anyhow!("Prism instance '{instance_id}' was not found"))
}

async fn prism_record_repository_certification_skip(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    registry: &ExtensionSourceRegistry,
    module: &ExtensionSourceModule,
    requested_by: &str,
    reason: &str,
    marketplace_state: &str,
    summary: &str,
    eligibility: Option<&PrismLanguageEligibility>,
) -> anyhow::Result<()> {
    store
        .create_source_certification_job(&NewExtensionSourceCertificationJob {
            job_id: Uuid::new_v4(),
            instance_id: instance.instance_id,
            registry_id: Some(registry.registry_id),
            source_module_id: Some(module.source_module_id),
            requested_by: requested_by.to_string(),
            reason: reason.to_string(),
            status: "skipped".to_string(),
            priority: 200,
            attempts: 0,
            max_attempts: 1,
            language_eligibility: eligibility.map(prism_language_eligibility_json),
            marketplace_state: Some(marketplace_state.to_string()),
            summary: Some(summary.to_string()),
            last_error: None,
        })
        .await
}

fn prism_marketplace_state_from_certification_status(status: &str) -> &'static str {
    match status {
        "certified" => "certified",
        "degraded" | "probation" => "degraded",
        "unsupported" => "unsupported",
        "account_required" => "account_required",
        "network_blocked" => "network_blocked",
        "broken" => "broken",
        _ => "unknown",
    }
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
    if registry.trust_class == "custom" && !registry.trusted_for_executable_updates {
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

fn nuvio_remove_registry_action(registry: &ExtensionSourceRegistry) -> ExtensionControlAction {
    let is_preset = nuvio_registry_is_preset(registry);
    let label = if is_preset {
        "Wipe preset"
    } else {
        "Remove repo"
    };
    let noun = if is_preset { "preset" } else { "repository" };
    let restore_hint = if registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY
        || registry.registry_type == "elixir_curated_nuvio_pack"
    {
        " Refresh recommended sources can restore bundled presets later."
    } else {
        ""
    };
    let confirm_text = format!(
        "Wipe '{}' from Prism? This removes the {noun}, every scraper discovered from it, queued certification jobs, and local scraper artifacts.{restore_hint}",
        registry.display_name
    );
    cloudstream_simple_action(
        "remove_registry",
        label,
        "Remove this source from Prism, including scraper descriptors, queued certification jobs, and local scraper artifacts.",
        "danger",
        Some(json!({ "registryId": registry.registry_id.to_string() })),
        Some(&confirm_text),
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
    let trusted = cloudstream_param_bool(params, "trustedForExecutableUpdates", false)?;
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
            trust_class: "custom".to_string(),
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

fn cloudstream_param_bool(
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
        Some(serde_json::Value::Number(value)) => Ok(value.as_i64().unwrap_or_default() != 0),
        Some(_) => anyhow::bail!("{key} must be true or false"),
        None => Ok(default),
    }
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
    use crate::{
        config::DatabaseConfig,
        db::{Database, models::ExtensionKind},
        extensions::store::{
            NewExtension, NewExtensionInstance, NewExtensionSourceModule,
            NewExtensionSourceModuleVersion, NewExtensionSourceRegistry,
        },
        runtime::model::{
            ContainerRuntimeMount, ContainerRuntimeSecurityState, ContainerRuntimeTmpfsMount,
        },
    };
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

    fn prism_language_test_module(language_tags_json: Option<Value>) -> ExtensionSourceModule {
        ExtensionSourceModule {
            source_module_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            registry_id: Uuid::new_v4(),
            module_key: "nuvio:test".to_string(),
            display_name: "Test Scraper".to_string(),
            ecosystem: "nuvio".to_string(),
            plugin_package: Some("test".to_string()),
            active_version: Some("1.0.0".to_string()),
            rollback_version: None,
            media_types_json: Some(json!(["movie"])),
            language_tags_json,
            region_tags_json: None,
            source_domains_json: None,
            account_required: false,
            unsupported: false,
            unsupported_reason: None,
            enabled: false,
            installed: false,
            pinned_version: None,
            health_state: "available".to_string(),
            replacement_recommendation_key: None,
            last_success_at: None,
            last_failure_at: None,
            last_error: None,
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    async fn insert_prism_test_module(
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
        registry_id: Uuid,
        module_key: &str,
        display_name: &str,
        language_tags_json: Option<Value>,
    ) -> anyhow::Result<Uuid> {
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: module_key.to_string(),
                display_name: display_name.to_string(),
                ecosystem: "nuvio".to_string(),
                plugin_package: Some(module_key.replace(':', "_")),
                active_version: None,
                rollback_version: None,
                media_types_json: Some(json!(["movie"])),
                language_tags_json,
                region_tags_json: None,
                source_domains_json: Some(json!(["example.test"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: false,
                installed: false,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: Some(json!({"nuvio": {"moduleId": module_key}})),
            })
            .await?;
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: Uuid::new_v4(),
                source_module_id,
                version: "1.0.0".to_string(),
                artifact_url: Some(format!("https://example.test/{module_key}.js")),
                artifact_sha256: Some("fixture-sha256".to_string()),
                signature: None,
                install_state: "available".to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: None,
                activated_at: None,
                metadata_json: Some(
                    json!({"nuvio": {"scriptPath": format!("/app/source-modules/{module_key}.js")}}),
                ),
            })
            .await?;
        Ok(source_module_id)
    }

    fn prism_test_certification(
        source_module_id: Uuid,
        instance_id: Uuid,
        status: &str,
        expires_at: Option<chrono::DateTime<Utc>>,
    ) -> ExtensionSourceModuleCertification {
        ExtensionSourceModuleCertification {
            certification_id: Uuid::new_v4(),
            source_module_id,
            source_module_version_id: None,
            artifact_sha256: Some("fixture-sha256".to_string()),
            instance_id,
            adapter: "nuvio_js_v1".to_string(),
            status: status.to_string(),
            failure_class: Some("unsafe_url".to_string()),
            summary: Some("certification passed for tv; failed for anime".to_string()),
            media_type_results_json: json!({}),
            materialization_results_json: json!({}),
            probe_targets_json: json!([]),
            candidate_evidence_json: json!([]),
            runtime_version: Some("1.0.0".to_string()),
            policy_version: prism_certification_policy_version(),
            certified_at: Some(Utc::now()),
            expires_at,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn prism_language_eligibility_prefers_english_japanese_and_allows_unknown_by_default() {
        let instance = crate::db::models::ExtensionInstance {
            instance_id: Uuid::new_v4(),
            extension_id: "elixir.sources.prism".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            runtime_version: None,
            rollback_version: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            enabled: true,
        };
        let policy = prism_marketplace_policy(&instance);
        assert_eq!(policy.preferred_language_tags, vec!["en", "ja"]);

        let english = prism_language_test_module(Some(json!(["English"])));
        let eligibility = prism_module_language_eligibility(&english, &policy);
        assert!(eligibility.certifiable);
        assert_eq!(eligibility.normalized_tags, vec!["en"]);

        let japanese = prism_language_test_module(Some(json!(["jpn"])));
        let eligibility = prism_module_language_eligibility(&japanese, &policy);
        assert!(eligibility.certifiable);
        assert_eq!(eligibility.normalized_tags, vec!["ja"]);

        let hindi = prism_language_test_module(Some(json!(["Hindi"])));
        let eligibility = prism_module_language_eligibility(&hindi, &policy);
        assert!(!eligibility.certifiable);
        assert_eq!(eligibility.state, "skipped_language");

        let unknown = prism_language_test_module(None);
        let eligibility = prism_module_language_eligibility(&unknown, &policy);
        assert!(eligibility.certifiable);
        assert_eq!(eligibility.state, "unknown_language");
    }

    #[test]
    fn prism_runtime_isolation_summary_reports_enforced_controls() {
        let security = prism_test_runtime_security();
        let runtime_state = prism_test_runtime_state(true);

        let summary = prism_runtime_isolation_summary(&security, Some(&runtime_state));

        assert_eq!(summary.health, "healthy");
        assert!(summary.missing.is_empty());
        assert!(
            summary
                .details
                .iter()
                .any(|detail| detail == "Read-only root filesystem: enforced")
        );
        assert!(
            summary
                .details
                .iter()
                .any(|detail| detail == "Docker socket: absent")
        );
    }

    #[test]
    fn prism_runtime_isolation_summary_reports_reduced_controls() {
        let security = prism_test_runtime_security();
        let mut runtime_state = prism_test_runtime_state(true);
        runtime_state.security.read_only_rootfs = false;
        runtime_state.security.no_new_privileges = false;
        runtime_state.mounts.push(ContainerRuntimeMount {
            mount_type: "bind".to_string(),
            source: Some("/var/run/docker.sock".to_string()),
            name: None,
            destination: "/var/run/docker.sock".to_string(),
            read_only: false,
        });

        let summary = prism_runtime_isolation_summary(&security, Some(&runtime_state));

        assert_eq!(summary.health, "degraded");
        assert!(
            summary
                .missing
                .contains(&"read-only root filesystem".to_string())
        );
        assert!(summary.missing.contains(&"no-new-privileges".to_string()));
        assert!(
            summary
                .missing
                .contains(&"Docker socket absent".to_string())
        );
    }

    #[test]
    fn prism_custom_isolation_gate_applies_only_to_custom_registries() {
        let mut registry = prism_test_source_registry("custom", "nuvio_manifest_json");
        assert!(prism_registry_requires_custom_isolation(&registry));

        registry.trust_class = "curated".to_string();
        assert!(!prism_registry_requires_custom_isolation(&registry));

        registry.trust_class = "custom".to_string();
        registry.registry_type = "elixir_curated_nuvio_pack".to_string();
        assert!(!prism_registry_requires_custom_isolation(&registry));
    }

    fn prism_test_source_registry(
        trust_class: &str,
        registry_type: &str,
    ) -> ExtensionSourceRegistry {
        ExtensionSourceRegistry {
            registry_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            registry_key: "nuvio.custom.fixture".to_string(),
            registry_type: registry_type.to_string(),
            trust_class: trust_class.to_string(),
            display_name: "Fixture Repo".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: false,
            trusted_for_executable_updates: trust_class != "custom",
            etag: None,
            last_modified: None,
            last_fetch_status: "unknown".to_string(),
            last_fetch_error: None,
            last_fetched_at: None,
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn prism_test_runtime_security() -> ManifestRuntimeSecurity {
        ManifestRuntimeSecurity {
            run_as_non_root: true,
            user: Some("1000:1000".to_string()),
            read_only_rootfs: true,
            no_new_privileges: true,
            drop_capabilities: vec!["ALL".to_string()],
            tmpfs: vec![crate::extensions::manifest::ManifestRuntimeTmpfs {
                path: "/tmp".to_string(),
                size_mb: Some(64),
            }],
            memory_limit_mb: Some(512),
            pids_limit: Some(128),
            cpu_quota: Some("1.0".to_string()),
            seccomp_profile: None,
            apparmor_profile: None,
            prohibit_docker_socket: true,
            prohibit_host_media_mounts: true,
        }
    }

    fn prism_test_runtime_state(read_only_source_mount: bool) -> ContainerRuntimeState {
        let mut labels = HashMap::new();
        labels.insert(
            "elixir.runtime.security.profile".to_string(),
            "hardened".to_string(),
        );
        ContainerRuntimeState {
            name: "elx-prism-source".to_string(),
            network_mode: None,
            labels,
            mounts: vec![ContainerRuntimeMount {
                mount_type: "bind".to_string(),
                source: Some("/host/source-modules".to_string()),
                name: None,
                destination: "/app/source-modules".to_string(),
                read_only: read_only_source_mount,
            }],
            published_ports: Vec::new(),
            security: ContainerRuntimeSecurityState {
                user: Some("1000:1000".to_string()),
                read_only_rootfs: true,
                no_new_privileges: true,
                cap_drop: vec!["ALL".to_string()],
                tmpfs: vec![ContainerRuntimeTmpfsMount {
                    path: "/tmp".to_string(),
                    options: Some("size=64m".to_string()),
                }],
                memory_limit_bytes: Some(512 * 1024 * 1024),
                pids_limit: Some(128),
                nano_cpus: Some(1_000_000_000),
                seccomp_profile: None,
                apparmor_profile: None,
            },
        }
    }

    #[test]
    fn prism_installed_disabled_module_can_be_enabled_or_uninstalled() {
        let mut module = prism_language_test_module(Some(json!(["English"])));
        module.installed = true;
        module.enabled = false;
        module.active_version = Some("1.0.0".to_string());
        let registry = ExtensionSourceRegistry {
            registry_id: module.registry_id,
            instance_id: module.instance_id,
            registry_key: "prism.fixture".to_string(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "maintainer_known".to_string(),
            display_name: "Prism Fixture".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: true,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let actions = nuvio_module_actions(&module, Some(&registry));
        assert!(
            actions
                .iter()
                .any(|action| { action.id == "enable_source_module" && action.label == "Enable" })
        );
        assert!(
            actions.iter().any(|action| {
                action.id == "disable_source_module" && action.label == "Uninstall"
            })
        );
    }

    #[test]
    fn prism_maintainer_repository_exposes_wipe_preset_action() {
        let mut context = arr_context("ready");
        context.summary.label = "Prism".to_string();
        context.control_binding = ExtensionControlBinding::Prism;
        let instance = context
            .selected_instance
            .clone()
            .expect("selected instance");
        let registry = ExtensionSourceRegistry {
            registry_id: Uuid::new_v4(),
            instance_id: instance.instance_id,
            registry_key: "prism.repo.phisher.nuvio".to_string(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "maintainer_known".to_string(),
            display_name: "Phisher Nuvio Providers".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: true,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let section = build_prism_recommended_section(
            &context,
            &instance,
            &[registry.clone()],
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(section.entities.len(), 1);
        assert_eq!(section.entities[0].title, "Phisher Nuvio Providers");
        let action = section.entities[0]
            .actions
            .iter()
            .find(|action| action.id == "remove_registry")
            .expect("maintainer preset should be wipeable");
        assert_eq!(action.label, "Wipe preset");
        assert_eq!(action.kind, "danger");
        assert!(
            action
                .confirm_text
                .as_deref()
                .is_some_and(|text| text.contains("every scraper discovered from it"))
        );

        let custom_section = build_nuvio_repositories_section(&[registry], &[], &BTreeMap::new());
        assert!(custom_section.entities.is_empty());
    }

    #[test]
    fn prism_custom_untrusted_repository_prompts_trust_and_certify() {
        let instance_id = Uuid::new_v4();
        let registry_id = Uuid::new_v4();
        let registry = ExtensionSourceRegistry {
            registry_id,
            instance_id,
            registry_key: "nuvio.custom.deadlyrocket".to_string(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "custom".to_string(),
            display_name: "All-in-One-Nuvio".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: false,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let mut module = prism_language_test_module(Some(json!(["English"])));
        module.instance_id = instance_id;
        module.registry_id = registry_id;
        module.display_name = "DeadlyRocket Fixture".to_string();

        let mut registry_by_id = BTreeMap::new();
        registry_by_id.insert(registry_id, &registry);
        let section = build_nuvio_repositories_section(
            &[registry.clone()],
            &[module.clone()],
            &BTreeMap::new(),
        );
        assert_eq!(section.entities.len(), 1);
        let entity = &section.entities[0];
        assert!(
            entity
                .subtitle
                .as_deref()
                .is_some_and(|subtitle| subtitle.contains("Blocked until trusted"))
        );
        assert!(entity.actions.iter().any(|action| {
            action.id == "trust_custom_repo" && action.label == "Trust + certify"
        }));
        assert!(
            entity.actions.iter().all(|action| {
                action.id != "certify_repository" || action.label != "Certify repo"
            })
        );
        assert!(
            entity
                .details
                .iter()
                .any(|detail| detail == "Certification: blocked until this repository is trusted")
        );
        let marketplace = build_nuvio_available_sources_section(
            &[module],
            &registry_by_id,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(
            marketplace.entities.is_empty(),
            "untrusted custom repo modules should stay collapsed under the repository card"
        );
    }

    #[test]
    fn prism_add_repository_trust_toggle_accepts_form_values() {
        let mut params = HashMap::new();
        params.insert("trustedForExecutableUpdates".to_string(), json!("on"));
        assert!(cloudstream_param_bool(&params, "trustedForExecutableUpdates", false).unwrap());

        params.insert("trustedForExecutableUpdates".to_string(), json!("true"));
        assert!(cloudstream_param_bool(&params, "trustedForExecutableUpdates", false).unwrap());

        params.insert("trustedForExecutableUpdates".to_string(), json!(1));
        assert!(cloudstream_param_bool(&params, "trustedForExecutableUpdates", false).unwrap());

        params.insert("trustedForExecutableUpdates".to_string(), json!("off"));
        assert!(!cloudstream_param_bool(&params, "trustedForExecutableUpdates", true).unwrap());

        params.insert("trustedForExecutableUpdates".to_string(), json!("false"));
        assert!(!cloudstream_param_bool(&params, "trustedForExecutableUpdates", true).unwrap());
    }

    #[test]
    fn prism_trusted_custom_repository_stays_custom_repository() {
        let mut context = arr_context("ready");
        context.summary.label = "Prism".to_string();
        context.control_binding = ExtensionControlBinding::Prism;
        let instance = context
            .selected_instance
            .clone()
            .expect("selected instance");
        let registry = ExtensionSourceRegistry {
            registry_id: Uuid::new_v4(),
            instance_id: instance.instance_id,
            registry_key: "nuvio.custom.deadlyrocket".to_string(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "custom".to_string(),
            display_name: "All-in-One-Nuvio".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: true,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let presets = build_prism_recommended_section(
            &context,
            &instance,
            &[registry.clone()],
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(presets.entities.is_empty());

        let custom = build_nuvio_repositories_section(&[registry], &[], &BTreeMap::new());
        assert_eq!(custom.entities.len(), 1);
        assert_eq!(custom.entities[0].title, "All-in-One-Nuvio");
        assert!(
            custom.entities[0]
                .subtitle
                .as_deref()
                .is_some_and(|subtitle| subtitle == "Enabled • custom")
        );
        assert!(
            custom.entities[0]
                .actions
                .iter()
                .all(|action| action.id != "trust_custom_repo")
        );
    }

    #[test]
    fn prism_recommended_registry_remove_action_wipes_preset() {
        let registry_id = Uuid::new_v4();
        let registry = ExtensionSourceRegistry {
            registry_id,
            instance_id: Uuid::new_v4(),
            registry_key: PRISM_RECOMMENDED_REGISTRY_KEY.to_string(),
            registry_type: "elixir_curated_nuvio_pack".to_string(),
            trust_class: "curated".to_string(),
            display_name: "Prism Recommended Sources".to_string(),
            url: None,
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: true,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let action = nuvio_remove_registry_action(&registry);
        let registry_id_text = registry_id.to_string();
        assert_eq!(action.id, "remove_registry");
        assert_eq!(action.label, "Wipe preset");
        assert_eq!(action.kind, "danger");
        assert_eq!(
            action
                .params
                .as_ref()
                .and_then(|params| params.get("registryId"))
                .and_then(|value| value.as_str()),
            Some(registry_id_text.as_str())
        );
        assert!(
            action
                .confirm_text
                .as_deref()
                .is_some_and(|text| text.contains("Refresh recommended sources can restore"))
        );
    }

    #[tokio::test]
    async fn prism_wiped_recommended_pack_stays_removed_until_explicit_restore()
    -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: PRISM_EXTENSION_ID.to_string(),
                name: "Prism".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: crate::db::models::ExtensionTrustLevel::Community,
                manifest_json: json!({"id": PRISM_EXTENSION_ID}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: PRISM_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        restore_prism_recommended_source_pack_for_instance(&store, instance_id, None, None).await?;
        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert!(
            registries
                .iter()
                .any(|registry| { registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY })
        );
        assert!(
            registries
                .iter()
                .any(|registry| { registry.registry_key == "prism.repo.phisher.nuvio" })
        );

        for registry in registries.iter().filter(|registry| {
            registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY
                || registry.registry_key == "prism.repo.phisher.nuvio"
        }) {
            record_prism_source_registry_tombstone(&store, registry, "removed_by_user").await?;
            store.delete_source_registry(registry.registry_id).await?;
        }
        crate::extensions::nuvio_registry::seed_prism_recommended_source_pack_for_instance(
            &store,
            instance_id,
            None,
            None,
        )
        .await?;
        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert!(
            !registries
                .iter()
                .any(|registry| { registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY })
        );
        assert!(
            !registries
                .iter()
                .any(|registry| { registry.registry_key == "prism.repo.phisher.nuvio" })
        );

        restore_prism_recommended_source_pack_for_instance(&store, instance_id, None, None).await?;
        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert!(
            registries
                .iter()
                .any(|registry| { registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY })
        );
        assert!(
            registries
                .iter()
                .any(|registry| { registry.registry_key == "prism.repo.phisher.nuvio" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn prism_boot_migration_keeps_recommended_missing_when_custom_repo_exists()
    -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: PRISM_EXTENSION_ID.to_string(),
                name: "Prism".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: crate::db::models::ExtensionTrustLevel::Community,
                manifest_json: json!({"id": PRISM_EXTENSION_ID}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: PRISM_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id: Uuid::new_v4(),
                instance_id,
                registry_key: "nuvio.custom.deadlyrocket".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "custom".to_string(),
                display_name: "All-in-One-Nuvio".to_string(),
                url: Some("https://example.test/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: false,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;

        let summary =
            crate::extensions::nuvio_registry::migrate_prism_recommended_source_pack_for_installed_instances(
                &store,
                None,
                None,
            )
            .await?;
        assert_eq!(summary.migrated_instances, 0);
        assert_eq!(summary.skipped_existing_instances, 1);
        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert!(
            !registries
                .iter()
                .any(|registry| { registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY })
        );
        assert_eq!(registries.len(), 1);
        Ok(())
    }

    #[test]
    fn prism_duplicate_repo_modules_are_split_by_runtime_bucket() {
        let instance_id = Uuid::new_v4();
        let curated_registry_id = Uuid::new_v4();
        let phisher_registry_id = Uuid::new_v4();
        let mut curated = prism_language_test_module(Some(json!(["English"])));
        curated.instance_id = instance_id;
        curated.registry_id = curated_registry_id;
        curated.module_key = "nuvio:prism-recommended:allwish".to_string();
        curated.display_name = "AllWish".to_string();
        curated.plugin_package = Some("allwish".to_string());
        curated.enabled = true;
        curated.installed = true;
        curated.active_version = Some("1.0.0".to_string());
        curated.health_state = "degraded".to_string();

        let mut phisher = curated.clone();
        phisher.source_module_id = Uuid::new_v4();
        phisher.registry_id = phisher_registry_id;
        phisher.module_key = "nuvio:prism-repo-phisher-nuvio:allwish".to_string();
        phisher.enabled = false;
        phisher.installed = true;

        let curated_registry = ExtensionSourceRegistry {
            registry_id: curated_registry_id,
            instance_id,
            registry_key: PRISM_RECOMMENDED_REGISTRY_KEY.to_string(),
            registry_type: "elixir_curated_nuvio_pack".to_string(),
            trust_class: "curated".to_string(),
            display_name: "Prism Recommended Sources".to_string(),
            url: None,
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: true,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let phisher_registry = ExtensionSourceRegistry {
            registry_id: phisher_registry_id,
            instance_id,
            registry_key: "prism.repo.phisher.nuvio".to_string(),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "maintainer_known".to_string(),
            display_name: "Phisher Nuvio Providers".to_string(),
            url: Some("https://example.test/manifest.json".to_string()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: false,
            etag: None,
            last_modified: None,
            last_fetch_status: "success".to_string(),
            last_fetch_error: None,
            last_fetched_at: Some(Utc::now()),
            metadata_json: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let modules = vec![curated, phisher];
        let mut registry_by_id = BTreeMap::new();
        registry_by_id.insert(curated_registry_id, &curated_registry);
        registry_by_id.insert(phisher_registry_id, &phisher_registry);
        let curated_certification = prism_test_certification(
            modules[0].source_module_id,
            instance_id,
            "degraded",
            Some(Utc::now() + chrono::Duration::days(7)),
        );
        let mut certification_by_module = BTreeMap::new();
        certification_by_module.insert(modules[0].source_module_id, &curated_certification);
        let job_by_module = BTreeMap::new();

        let ready = build_nuvio_ready_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        );
        assert_eq!(ready.entities.len(), 1);
        let entity = &ready.entities[0];
        assert_eq!(entity.title, "AllWish");
        assert!(
            entity
                .subtitle
                .as_deref()
                .is_some_and(|subtitle| subtitle.contains("Ready with warnings"))
        );
        assert!(
            entity
                .details
                .iter()
                .any(|detail| detail.contains("Prism Recommended Sources"))
        );
        assert!(
            entity
                .actions
                .iter()
                .all(|action| !action.label.contains("Phisher Nuvio Providers"))
        );

        let disabled = build_nuvio_disabled_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        );
        assert_eq!(disabled.entities.len(), 1);
        let entity = &disabled.entities[0];
        assert_eq!(entity.title, "AllWish");
        assert!(
            entity
                .subtitle
                .as_deref()
                .is_some_and(|subtitle| subtitle.contains("Installed, disabled"))
        );

        let available = build_nuvio_available_sources_section(
            &modules,
            &registry_by_id,
            &certification_by_module,
            &job_by_module,
        );
        assert!(available.entities.is_empty());
    }

    #[test]
    fn prism_expired_enabled_scraper_needs_recertification() {
        let instance_id = Uuid::new_v4();
        let mut module = prism_language_test_module(Some(json!(["English"])));
        module.instance_id = instance_id;
        module.enabled = true;
        module.installed = true;
        module.active_version = Some("1.0.0".to_string());
        module.health_state = "degraded".to_string();
        let certification = prism_test_certification(
            module.source_module_id,
            instance_id,
            "degraded",
            Some(Utc::now() - chrono::Duration::days(1)),
        );

        assert!(!nuvio_module_can_run_now(&module, Some(&certification)));
        assert!(nuvio_module_needs_attention(
            &module,
            Some(&certification),
            None
        ));
        assert!(
            nuvio_module_subtitle(&module, Some(&certification), None)
                .contains("Needs recertification")
        );
        assert!(
            nuvio_module_details(&module, None, Some(&certification), None)
                .iter()
                .any(|detail| detail.starts_with("Expired: "))
        );
    }

    #[test]
    fn prism_module_details_lead_with_behavior_and_bound_diagnostics() {
        let instance_id = Uuid::new_v4();
        let mut module = prism_language_test_module(Some(json!(["English"])));
        module.instance_id = instance_id;
        module.enabled = true;
        module.installed = true;
        module.active_version = Some("1.0.0".to_string());
        module.health_state = "broken".to_string();

        let mut certification = prism_test_certification(
            module.source_module_id,
            instance_id,
            "broken",
            Some(Utc::now() + chrono::Duration::days(7)),
        );
        certification.failure_class = Some("network_blocked".to_string());
        certification.summary = Some(format!("probe failed: {}", "x".repeat(500)));

        let job = ExtensionSourceCertificationJob {
            job_id: Uuid::new_v4(),
            instance_id,
            registry_id: Some(module.registry_id),
            source_module_id: Some(module.source_module_id),
            requested_by: "test".to_string(),
            reason: "certification".to_string(),
            status: "failed".to_string(),
            priority: 100,
            attempts: 1,
            max_attempts: 1,
            language_eligibility: None,
            marketplace_state: Some("broken".to_string()),
            summary: Some(format!("job summary: {}", "y".repeat(500))),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            last_error: Some(format!("job error: {}", "z".repeat(500))),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let details = nuvio_module_details(&module, None, Some(&certification), Some(&job));

        assert_eq!(details[0], "Runtime: Needs attention");
        assert_eq!(
            details[1],
            "Search behavior: excluded until certification succeeds"
        );
        assert_eq!(
            details[2],
            "Primary issue: certification broken (network blocked)"
        );
        for prefix in ["Certification job: ", "Certification error: ", "Probe: "] {
            let detail = details
                .iter()
                .find(|detail| detail.starts_with(prefix))
                .expect("bounded diagnostic detail");
            assert!(
                detail.ends_with("..."),
                "{prefix} detail should be visibly truncated"
            );
            assert!(
                detail.chars().count() <= prefix.chars().count() + 223,
                "{prefix} detail should stay bounded"
            );
        }
    }

    #[tokio::test]
    async fn prism_repository_certification_enqueue_skips_nonpreferred_languages()
    -> anyhow::Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: PRISM_EXTENSION_ID.to_string(),
                name: "Prism".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: crate::db::models::ExtensionTrustLevel::Community,
                manifest_json: json!({"id": PRISM_EXTENSION_ID}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: PRISM_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "prism.fixture".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: "Prism Fixture".to_string(),
                url: Some("https://example.test/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        insert_prism_test_module(
            &store,
            instance_id,
            registry_id,
            "nuvio:test:english",
            "English Fixture",
            Some(json!(["English"])),
        )
        .await?;
        insert_prism_test_module(
            &store,
            instance_id,
            registry_id,
            "nuvio:test:hindi",
            "Hindi Fixture",
            Some(json!(["Hindi"])),
        )
        .await?;

        let instance = store
            .get_instance(instance_id)
            .await?
            .expect("Prism test instance");
        let summary = prism_enqueue_repository_certification_jobs(
            &store,
            &instance,
            registry_id,
            "test",
            "repository_added",
            false,
        )
        .await?;
        assert_eq!(summary.discovered, 2);
        assert_eq!(summary.processed, 1);
        assert_eq!(summary.skipped_language, 1);

        let jobs = store
            .list_source_certification_jobs_for_registry(registry_id, 10)
            .await?;
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().any(|job| {
            job.status == "queued" && job.marketplace_state.as_deref() == Some("certifying")
        }));
        assert!(jobs.iter().any(|job| {
            job.status == "skipped" && job.marketplace_state.as_deref() == Some("skipped_language")
        }));
        Ok(())
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

    #[tokio::test]
    async fn prism_runtime_smoke_classifies_moviesdrive_fetch_failure_as_broken() {
        let warnings = vec![
            "prism:MoviesDrive: runtime_error: [Moviesdrive] Scraping error: fetch failed"
                .to_string(),
        ];
        let outcome = certify_prism_runtime_smoke("moviesdrive", &json!({}), &[], &warnings).await;

        assert_eq!(outcome.status, "broken");
        assert_eq!(outcome.health_state, "broken");
        assert_eq!(outcome.severity, "error");
        assert!(outcome.reason.contains("fetch failed"));
        assert_eq!(outcome.candidate_count, 0);
    }

    #[tokio::test]
    async fn prism_runtime_smoke_records_private_egress_policy_evidence() {
        let warnings = vec![
            "prism:FetchLocalhost: Prism source fetch blocked private network destination: 127.0.0.1"
                .to_string(),
        ];
        let outcome = certify_prism_runtime_smoke(
            "fetch-localhost",
            &json!({ "mediaType": "movie" }),
            &[],
            &warnings,
        )
        .await;

        assert_eq!(outcome.status, "network_blocked");
        assert_eq!(outcome.health_state, "broken");
        assert_eq!(
            outcome.failure_class.as_deref(),
            Some(FAILURE_NETWORK_BLOCKED)
        );

        let policy_evidence = outcome
            .candidate_evidence
            .as_array()
            .expect("candidate evidence array");
        assert_eq!(policy_evidence.len(), 1);
        assert_eq!(
            policy_evidence[0].get("kind").and_then(Value::as_str),
            Some("egress_block")
        );
        assert_eq!(
            policy_evidence[0]
                .get("destination")
                .and_then(Value::as_str),
            Some("127.0.0.1")
        );
        assert_eq!(
            policy_evidence[0]
                .get("egressPolicyVersion")
                .and_then(Value::as_str),
            Some(PRISM_EGRESS_POLICY_VERSION)
        );
    }

    #[test]
    fn prism_runtime_smoke_marks_preflighted_stream_candidates_certified() {
        let candidates = vec![json!({
            "sourceModule": {
                "id": "MoviesDrive"
            },
            "delivery": {
                "streamType": "direct_file",
                "url": "https://cdn.example.test/movie.mp4"
            }
        })];
        let outcome = classify_prism_preflight_reports(
            &json!({ "mediaType": "movie" }),
            &candidates,
            vec![StreamCandidatePreflightReport::passed("direct file passed")],
            Vec::new(),
        );

        assert_eq!(outcome.status, "certified");
        assert_eq!(outcome.health_state, "healthy");
        assert_eq!(outcome.severity, "info");
        assert_eq!(outcome.candidate_count, 1);
        assert_eq!(outcome.materializable_count, 1);
    }

    #[tokio::test]
    async fn prism_runtime_smoke_without_candidates_is_broken_no_results() {
        let outcome =
            certify_prism_runtime_smoke("moviesdrive", &json!({ "mediaType": "movie" }), &[], &[])
                .await;

        assert_eq!(outcome.status, "broken");
        assert_eq!(outcome.failure_class.as_deref(), Some("no_results"));
        assert_eq!(outcome.severity, "error");
        assert_eq!(outcome.candidate_count, 0);
    }

    #[test]
    fn prism_runtime_smoke_requests_cover_declared_media_types() {
        let module = prism_test_source_module(json!(["movie", "tv"]));
        let requests = prism_runtime_smoke_requests(&module);
        let media_types = requests
            .iter()
            .map(prism_smoke_request_media_type)
            .collect::<Vec<_>>();

        assert_eq!(media_types, vec!["movie", "tv"]);
        assert_eq!(
            prism_smoke_request_media_type(&prism_runtime_smoke_request(&module)),
            "movie"
        );
    }

    #[test]
    fn prism_runtime_smoke_aggregates_mixed_media_results_as_degraded() {
        let requests = vec![
            prism_runtime_smoke_request_for_media_type("movie"),
            prism_runtime_smoke_request_for_media_type("tv"),
        ];
        let movie = PrismRuntimeSmokeOutcome::new(
            "certified",
            "info",
            "movie candidate passed",
            1,
            Vec::new(),
        )
        .with_preflight(
            1,
            json!({
                "movie": {
                    "status": "certified",
                    "candidateCount": 1,
                    "materializableCount": 1,
                    "summary": "movie candidate passed"
                }
            }),
            json!({ "inspected": [] }),
            json!([]),
        );
        let tv = PrismRuntimeSmokeOutcome::new(
            "broken",
            "error",
            "runtime probe completed without stream candidates for the canary title",
            0,
            Vec::new(),
        )
        .with_failure_class("no_results")
        .with_preflight(
            0,
            json!({
                "tv": {
                    "status": "broken",
                    "failureClass": "no_results",
                    "candidateCount": 0,
                    "materializableCount": 0,
                    "summary": "runtime probe completed without stream candidates for the canary title"
                }
            }),
            json!({ "inspected": [] }),
            json!([]),
        );

        let outcome = aggregate_prism_runtime_smoke_outcomes(&requests, vec![movie, tv]);

        assert_eq!(outcome.status, "degraded");
        assert_eq!(outcome.health_state, "degraded");
        assert_eq!(outcome.failure_class.as_deref(), Some("no_results"));
        assert_eq!(outcome.candidate_count, 1);
        assert_eq!(outcome.materializable_count, 1);
        assert!(outcome.media_type_results.get("movie").is_some());
        assert!(outcome.media_type_results.get("tv").is_some());
    }

    fn prism_test_source_module(media_types_json: Value) -> ExtensionSourceModule {
        let now = Utc::now();
        ExtensionSourceModule {
            source_module_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            registry_id: Uuid::new_v4(),
            module_key: "test.module".to_string(),
            display_name: "Test Module".to_string(),
            ecosystem: "nuvio".to_string(),
            plugin_package: Some("test".to_string()),
            active_version: Some("1.0.0".to_string()),
            rollback_version: None,
            media_types_json: Some(media_types_json),
            language_tags_json: None,
            region_tags_json: None,
            source_domains_json: None,
            account_required: false,
            unsupported: false,
            unsupported_reason: None,
            enabled: false,
            installed: false,
            pinned_version: None,
            health_state: "available".to_string(),
            replacement_recommendation_key: None,
            last_success_at: None,
            last_failure_at: None,
            last_error: None,
            metadata_json: None,
            created_at: now,
            updated_at: now,
        }
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
