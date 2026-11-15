//! Setup of a marketplace stack's own members (for example its media manager)
//! by the stack's own code. The orchestrator still resolves every address and
//! credential into the patch, applies the drift policy and records readiness;
//! the stack's code reads and writes its own application. Nothing here knows a
//! particular application. See docs/architecture/arr-stack-marketplace-extension.md.
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use crate::drivers::{
    ApplyResult, ApplyStatus, DriftEvaluation, DriftField, DriftStatus, FieldSemantics,
    StateSnapshot,
};

#[async_trait::async_trait]
pub trait StackMemberSetup: Send + Sync {
    /// Drift of one resolved patch (the connector's patch JSON).
    /// `provider_url` is the member's own address as the other containers
    /// reach it (for example, where its peers should call it back).
    async fn evaluate_patch(
        &self,
        member_extension_id: &str,
        implementation: &str,
        patch: Value,
        provider_url: &str,
    ) -> Result<DriftEvaluation>;
    async fn apply_patch(
        &self,
        member_extension_id: &str,
        implementation: &str,
        patch: Value,
        provider_url: &str,
    ) -> Result<ApplyResult>;
    /// File names (inside the member's `/config`) its first-start setup reads.
    async fn bootstrap_files(
        &self,
        member_extension_id: &str,
        implementation: &str,
    ) -> Result<Vec<String>>;
    /// First-start setup from those files and Elixir's generated web login.
    /// Returns the member's API key, or `None` while it is not ready yet.
    async fn bootstrap(
        &self,
        member_extension_id: &str,
        implementation: &str,
        files: BTreeMap<String, String>,
        login: (String, String),
    ) -> Result<Option<String>>;
    /// Readiness probe once bootstrapped.
    async fn read_state(
        &self,
        member_extension_id: &str,
        implementation: &str,
    ) -> Result<StateSnapshot>;
}

static SETUP: RwLock<Option<Arc<dyn StackMemberSetup>>> = RwLock::new(None);

/// Installed at startup once the marketplace can serve stack code.
pub fn install(setup: Option<Arc<dyn StackMemberSetup>>) {
    if let Ok(mut slot) = SETUP.write() {
        *slot = setup;
    }
}

/// Installs the marketplace's long-lived executors as the stack setup.
#[cfg(unix)]
pub fn install_marketplace(state: &crate::state::AppState) {
    install(Some(Arc::new(
        crate::http::handlers::discovery::manager::ServeStackSetup::new(
            state.db_pool.clone(),
            state.marketplace.clone(),
        ),
    )));
}

/// The installed setup, for a marketplace stack member only.
pub(crate) fn for_member(extension_id: &str) -> Option<Arc<dyn StackMemberSetup>> {
    if !crate::marketplace::stack::is_stack_member_id(extension_id) {
        return None;
    }
    SETUP.read().ok().and_then(|slot| slot.clone())
}

/// A file name a stack may ask for: one plain name inside `/config`.
pub(crate) fn valid_bootstrap_file(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && !name.starts_with('.')
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriftWire {
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    non_comparable_fields: Vec<FieldWire>,
}
#[derive(Deserialize)]
struct FieldWire {
    name: String,
    semantics: String,
}

/// `{status, message, nonComparableFields}` from a stack's code.
pub fn drift_from_json(value: Value) -> Result<DriftEvaluation> {
    let wire: DriftWire = serde_json::from_value(value).context("stack drift reply shape")?;
    let status = match wire.status.as_str() {
        "in_sync" => DriftStatus::InSync,
        "drifted" => DriftStatus::Drifted,
        "unknown" => DriftStatus::Unknown,
        other => bail!("unknown stack drift status {other:?}"),
    };
    let fields = wire
        .non_comparable_fields
        .into_iter()
        .map(|field| {
            let semantics = match field.semantics.as_str() {
                "comparable" => FieldSemantics::Comparable,
                "normalized_comparable" => FieldSemantics::NormalizedComparable,
                "opaque_secret" => FieldSemantics::OpaqueSecret,
                "write_only" => FieldSemantics::WriteOnly,
                "derived_runtime" => FieldSemantics::DerivedRuntime,
                "observed_only" => FieldSemantics::ObservedOnly,
                other => bail!("unknown field semantics {other:?}"),
            };
            Ok(DriftField::new(field.name, semantics))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DriftEvaluation {
        status,
        message: wire.message,
        non_comparable_fields: fields,
    })
}

/// `{status: "applied" | "deferred", message}` from a stack's code.
pub fn apply_from_json(value: Value) -> Result<ApplyResult> {
    #[derive(Deserialize)]
    struct Wire {
        status: String,
        #[serde(default)]
        message: Option<String>,
    }
    let wire: Wire = serde_json::from_value(value).context("stack apply reply shape")?;
    let status = match wire.status.as_str() {
        "applied" => ApplyStatus::Applied,
        "deferred" => ApplyStatus::Deferred,
        other => bail!("unknown stack apply status {other:?}"),
    };
    Ok(ApplyResult {
        status,
        message: wire.message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wire_forms_map_to_driver_results() {
        let drift = drift_from_json(json!({"status": "drifted", "message": "root folders require repair: /tv",
            "nonComparableFields": [{"name": "apiKey", "semantics": "opaque_secret"}]}))
        .unwrap();
        assert_eq!(drift.status, DriftStatus::Drifted);
        assert_eq!(drift.non_comparable_fields[0].semantics, FieldSemantics::OpaqueSecret);
        assert!(drift_from_json(json!({"status": "maybe"})).is_err());
        let applied = apply_from_json(json!({"status": "deferred", "message": "later"})).unwrap();
        assert_eq!(applied.status, ApplyStatus::Deferred);
        assert!(valid_bootstrap_file("config.xml"));
        for bad in ["", "../x", "a/b", ".env", "x y"] {
            assert!(!valid_bootstrap_file(bad), "{bad}");
        }
        assert!(for_member("elixir.modules.sonarr").is_none());
    }
}
