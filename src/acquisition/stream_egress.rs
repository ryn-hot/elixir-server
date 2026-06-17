use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::extensions::store::ExtensionStore;

pub const STREAM_HTTP_EGRESS_POLICY_SETTING_KEY: &str = "acquisition.stream_http_egress_policy";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamHttpEgressPolicy {
    AutoHttpOnly,
    AlwaysProtected,
    DirectOnly,
}

impl Default for StreamHttpEgressPolicy {
    fn default() -> Self {
        Self::AutoHttpOnly
    }
}

impl StreamHttpEgressPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AutoHttpOnly => "auto_http_only",
            Self::AlwaysProtected => "always_protected",
            Self::DirectOnly => "direct_only",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamEgressDecision {
    DirectHttps,
    ProtectedHttp,
    ProtectedMixedManifest,
    BlockedProtectedEgressUnavailable,
    RejectedByPolicy,
}

impl StreamEgressDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectHttps => "direct_https",
            Self::ProtectedHttp => "protected_http",
            Self::ProtectedMixedManifest => "protected_mixed_manifest",
            Self::BlockedProtectedEgressUnavailable => "blocked_protected_egress_unavailable",
            Self::RejectedByPolicy => "rejected_by_policy",
        }
    }

    pub fn route_label(self) -> &'static str {
        match self {
            Self::DirectHttps => "Direct HTTPS stream download",
            Self::ProtectedHttp | Self::ProtectedMixedManifest => {
                "HTTP stream download via protected egress"
            }
            Self::BlockedProtectedEgressUnavailable => {
                "Stream download blocked: protected egress unavailable"
            }
            Self::RejectedByPolicy => "Stream download rejected by egress policy",
        }
    }
}

pub fn stream_http_egress_policy_from_json(
    value: Option<&Value>,
) -> Option<StreamHttpEgressPolicy> {
    let value = value?;
    if let Some(raw) = value.as_str() {
        return stream_http_egress_policy_from_str(raw);
    }
    value
        .get("policy")
        .or_else(|| value.get("streamHttpEgressPolicy"))
        .or_else(|| value.get("stream_http_egress_policy"))
        .and_then(Value::as_str)
        .and_then(stream_http_egress_policy_from_str)
        .or_else(|| serde_json::from_value::<StreamHttpEgressPolicy>(value.clone()).ok())
}

pub fn stream_http_egress_policy_from_str(value: &str) -> Option<StreamHttpEgressPolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto_http_only" | "auto" | "default" => Some(StreamHttpEgressPolicy::AutoHttpOnly),
        "always_protected" | "protected" | "protect_all" => {
            Some(StreamHttpEgressPolicy::AlwaysProtected)
        }
        "direct_only" | "reject_http" | "reject" => Some(StreamHttpEgressPolicy::DirectOnly),
        _ => None,
    }
}

pub fn stream_http_egress_policy_to_value(policy: StreamHttpEgressPolicy) -> Value {
    json!(policy)
}

pub async fn load_saved_stream_http_egress_policy(
    store: &ExtensionStore<'_>,
) -> anyhow::Result<StreamHttpEgressPolicy> {
    Ok(store
        .get_extension_setting(STREAM_HTTP_EGRESS_POLICY_SETTING_KEY)
        .await?
        .as_ref()
        .and_then(|value| stream_http_egress_policy_from_json(Some(value)))
        .unwrap_or_default())
}

pub async fn save_stream_http_egress_policy(
    store: &ExtensionStore<'_>,
    policy: StreamHttpEgressPolicy,
) -> anyhow::Result<()> {
    if policy == StreamHttpEgressPolicy::default() {
        store
            .delete_extension_setting(STREAM_HTTP_EGRESS_POLICY_SETTING_KEY)
            .await?;
    } else {
        store
            .upsert_extension_setting(
                STREAM_HTTP_EGRESS_POLICY_SETTING_KEY,
                &stream_http_egress_policy_to_value(policy),
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_http_egress_policy_accepts_string_and_object_forms() {
        assert_eq!(
            stream_http_egress_policy_from_json(Some(&json!("auto_http_only"))),
            Some(StreamHttpEgressPolicy::AutoHttpOnly)
        );
        assert_eq!(
            stream_http_egress_policy_from_json(Some(&json!({"policy": "always_protected"}))),
            Some(StreamHttpEgressPolicy::AlwaysProtected)
        );
        assert_eq!(
            stream_http_egress_policy_from_json(Some(
                &json!({"streamHttpEgressPolicy": "direct_only"})
            )),
            Some(StreamHttpEgressPolicy::DirectOnly)
        );
        assert_eq!(
            stream_http_egress_policy_from_json(Some(&json!("unknown"))),
            None
        );
    }
}
