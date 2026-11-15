//! Calls into a marketplace stack's own code for one of its members (for
//! example its media manager), and the orchestrator's setup of those members
//! through it. The server picks the general interface from the member
//! provider's capability, never from the application's name; the orchestrator
//! still resolves every address and credential into the patch, applies the
//! drift policy and records readiness, while the stack's code reads and writes
//! its own application. See docs/architecture/arr-stack-marketplace-extension.md.
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::drivers::{
    ApplyResult, ApplyStatus, DriftEvaluation, DriftField, DriftStatus, DriverPatch,
    FieldSemantics, PatchSemantics, PatchSideEffect, StateSnapshot,
};

/// Runs one operation of a stack's own code for one of its members and returns
/// the reply body. In production this is the stack's long-lived executor.
#[async_trait::async_trait]
pub trait StackCode: Send + Sync {
    async fn call(
        &self,
        member_extension_id: &str,
        interface: &str,
        operation: &str,
        request: Value,
    ) -> Result<Value>;
}

static CODE: RwLock<Option<Arc<dyn StackCode>>> = RwLock::new(None);

/// Installed at startup once the marketplace can serve stack code.
pub fn install(code: Option<Arc<dyn StackCode>>) {
    if let Ok(mut slot) = CODE.write() {
        *slot = code;
    }
}

/// Installs the marketplace's long-lived executors as the stack code.
#[cfg(unix)]
pub fn install_marketplace(state: &crate::state::AppState) {
    install(Some(Arc::new(
        crate::http::handlers::discovery::manager::ServeStackCode::new(
            state.db_pool.clone(),
            state.marketplace.clone(),
        ),
    )));
}

/// The installed stack code. A stack member is never handed to the legacy
/// stack's code instead, so its absence is an error.
pub(crate) fn code() -> Result<Arc<dyn StackCode>> {
    CODE.read()
        .ok()
        .and_then(|slot| slot.clone())
        .context("this stack's own code is not available on this server")
}

/// The general interface a member provider's capability answers.
pub(crate) fn interface_for(capability: &str) -> Option<&'static str> {
    match capability.trim() {
        "media.manager.tv" | "media.manager.anime" | "media.manager.movies" => {
            Some("media.manager")
        }
        "indexer.registry" => Some("indexer.list"),
        _ => None,
    }
}

/// How a setup patch to a stack member may be applied: a quality-policy preset
/// only when the owner asks for it, everything else is safe to reapply. These
/// are the rules the orchestrator applies to these patch kinds.
pub(crate) fn patch_semantics(patch: &DriverPatch) -> PatchSemantics {
    use crate::drivers::{MediaManagerMoviesPatch, MediaManagerTvPatch};
    match patch {
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::ApplyQualityPolicyPreset { .. })
        | DriverPatch::MediaManagerMovies(MediaManagerMoviesPatch::ApplyQualityPolicyPreset {
            ..
        }) => PatchSemantics::desired_change_only(PatchSideEffect::LiveApiWrite),
        _ => PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite),
    }
}

/// Setup of one stack member's provider by its stack's own code.
pub(crate) struct MemberSetup {
    code: Arc<dyn StackCode>,
    member: String,
    interface: &'static str,
}

/// `None` unless this is a marketplace stack member's provider of a general
/// interface (media manager, indexer list).
pub(crate) fn for_member(extension_id: &str, capability: &str) -> Result<Option<MemberSetup>> {
    if !crate::marketplace::stack::is_stack_member_id(extension_id) {
        return Ok(None);
    }
    let Some(interface) = interface_for(capability) else {
        return Ok(None);
    };
    Ok(Some(MemberSetup {
        code: code()?,
        member: extension_id.to_owned(),
        interface,
    }))
}

impl MemberSetup {
    async fn call(&self, operation: &str, request: Value) -> Result<Value> {
        self.code
            .call(&self.member, self.interface, operation, request)
            .await
    }

    /// Drift of one resolved patch (the connector's patch JSON).
    /// `provider_url` is the member's own address as the other containers
    /// reach it (for example, where its peers should call it back).
    pub(crate) async fn evaluate_patch(
        &self,
        patch: Value,
        provider_url: &str,
    ) -> Result<DriftEvaluation> {
        drift_from_json(
            self.call(
                "evaluate_patch",
                json!({ "patch": patch, "providerUrl": provider_url }),
            )
            .await?,
        )
    }

    pub(crate) async fn apply_patch(
        &self,
        patch: Value,
        provider_url: &str,
    ) -> Result<ApplyResult> {
        apply_from_json(
            self.call(
                "apply_patch",
                json!({ "patch": patch, "providerUrl": provider_url }),
            )
            .await?,
        )
    }

    /// File names (inside the member's `/config`) its first-start setup reads.
    pub(crate) async fn bootstrap_files(&self) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct Reply {
            files: Vec<String>,
        }
        let reply: Reply = serde_json::from_value(self.call("bootstrap_files", json!({})).await?)
            .context("stack bootstrap_files reply shape")?;
        Ok(reply.files)
    }

    /// First-start setup from those files and Elixir's generated web login.
    /// Returns the member's API key, or `None` while it is not ready yet.
    pub(crate) async fn bootstrap(
        &self,
        files: BTreeMap<String, String>,
        (username, password): (String, String),
    ) -> Result<Option<String>> {
        #[derive(Deserialize)]
        struct Reply {
            #[serde(rename = "apiKey", default)]
            api_key: Option<String>,
        }
        let reply: Reply = serde_json::from_value(
            self.call(
                "bootstrap",
                json!({ "files": files, "login": { "username": username, "password": password } }),
            )
            .await?,
        )
        .context("stack bootstrap reply shape")?;
        Ok(reply.api_key)
    }

    /// Readiness probe once bootstrapped.
    pub(crate) async fn read_state(&self) -> Result<StateSnapshot> {
        #[derive(Deserialize)]
        struct Reply {
            #[serde(default)]
            summary: Option<String>,
        }
        let reply: Reply = serde_json::from_value(self.call("read_state", json!({})).await?)
            .context("stack read_state reply shape")?;
        Ok(StateSnapshot {
            summary: reply.summary,
            activity: None,
        })
    }
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

    #[test]
    fn wire_forms_map_to_driver_results() {
        let drift = drift_from_json(
            json!({"status": "drifted", "message": "root folders require repair: /tv",
            "nonComparableFields": [{"name": "apiKey", "semantics": "opaque_secret"}]}),
        )
        .unwrap();
        assert_eq!(drift.status, DriftStatus::Drifted);
        assert_eq!(
            drift.non_comparable_fields[0].semantics,
            FieldSemantics::OpaqueSecret
        );
        assert!(drift_from_json(json!({"status": "maybe"})).is_err());
        let applied = apply_from_json(json!({"status": "deferred", "message": "later"})).unwrap();
        assert_eq!(applied.status, ApplyStatus::Deferred);
        assert!(valid_bootstrap_file("config.xml"));
        for bad in ["", "../x", "a/b", ".env", "x y"] {
            assert!(!valid_bootstrap_file(bad), "{bad}");
        }
        assert!(
            for_member("elixir.modules.sonarr", "media.manager.tv")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn interfaces_follow_the_capability_not_the_application() {
        assert_eq!(interface_for("media.manager.tv"), Some("media.manager"));
        assert_eq!(interface_for("media.manager.movies"), Some("media.manager"));
        assert_eq!(interface_for("indexer.registry"), Some("indexer.list"));
        assert_eq!(interface_for("media.search.tv"), None);
        assert_eq!(interface_for("downloader.torrent"), None);
    }

    #[test]
    fn stack_patch_semantics_match_the_orchestrator_rules() {
        use crate::drivers::PatchApplyPolicy;
        let preset = DriverPatch::from_manifest(
            "media.manager.tv",
            json!({"op": "apply_quality_policy_preset",
                "policy": {"preset": "modern_codecs", "profile_name": "Elixir Modern Codecs"}}),
        )
        .unwrap();
        let tags = DriverPatch::from_manifest(
            "media.manager.movies",
            json!({"op": "set_tags", "tags": ["elixir"]}),
        )
        .unwrap();
        assert_eq!(
            patch_semantics(&preset).apply_policy,
            PatchApplyPolicy::DesiredChangeOnly
        );
        assert_eq!(
            patch_semantics(&tags).apply_policy,
            PatchApplyPolicy::PeriodicSafe
        );
    }
}
