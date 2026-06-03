use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use uuid::Uuid;

use crate::download_broker::{
    DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID,
};

pub const ROUTE_ATTEMPT_LEDGER_KEY: &str = "routeAttemptLedger";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteAttemptStatus {
    Submitted,
    Failed,
    Blocked,
}

impl RouteAttemptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAttemptRecord {
    pub route_logical_id: String,
    pub provider_id: Option<Uuid>,
    pub implementation: Option<String>,
    pub attempt_key: String,
    pub download_id: Option<String>,
    pub status: RouteAttemptStatus,
    pub failure_class: Option<String>,
    pub reason: Option<String>,
    pub recorded_at: DateTime<Utc>,
}

impl RouteAttemptRecord {
    pub fn new(
        route_logical_id: &str,
        provider_id: Option<Uuid>,
        implementation: Option<&str>,
        download_id: Option<String>,
        status: RouteAttemptStatus,
        failure_class: Option<&str>,
        reason: Option<String>,
    ) -> Self {
        let implementation = normalize_optional(implementation);
        Self {
            route_logical_id: route_logical_id.trim().to_string(),
            provider_id,
            attempt_key: route_attempt_key(
                route_logical_id,
                provider_id,
                implementation.as_deref(),
            ),
            implementation,
            download_id,
            status,
            failure_class: normalize_optional(failure_class),
            reason: reason.and_then(|value| normalize_optional(Some(value.as_str()))),
            recorded_at: Utc::now(),
        }
    }

    pub fn to_json(&self) -> JsonValue {
        let mut object = JsonMap::new();
        object.insert(
            "routeLogicalId".to_string(),
            JsonValue::String(self.route_logical_id.clone()),
        );
        if let Some(provider_id) = self.provider_id {
            object.insert(
                "providerId".to_string(),
                JsonValue::String(provider_id.to_string()),
            );
        }
        if let Some(implementation) = self.implementation.as_ref() {
            object.insert(
                "implementation".to_string(),
                JsonValue::String(implementation.clone()),
            );
        }
        object.insert(
            "attemptKey".to_string(),
            JsonValue::String(self.attempt_key.clone()),
        );
        if let Some(download_id) = self.download_id.as_ref() {
            object.insert(
                "downloadId".to_string(),
                JsonValue::String(download_id.clone()),
            );
        }
        object.insert(
            "status".to_string(),
            JsonValue::String(self.status.as_str().to_string()),
        );
        if let Some(failure_class) = self.failure_class.as_ref() {
            object.insert(
                "failureClass".to_string(),
                JsonValue::String(failure_class.clone()),
            );
        }
        if let Some(reason) = self.reason.as_ref() {
            object.insert("reason".to_string(), JsonValue::String(reason.clone()));
        }
        object.insert(
            "recordedAt".to_string(),
            JsonValue::String(self.recorded_at.to_rfc3339()),
        );
        JsonValue::Object(object)
    }
}

pub fn route_attempt_key(
    route_logical_id: &str,
    provider_id: Option<Uuid>,
    implementation: Option<&str>,
) -> String {
    let family = route_attempt_family(route_logical_id);
    let provider = provider_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unresolved".to_string());
    match family {
        "debrid" => {
            let implementation = normalize_optional(implementation)
                .unwrap_or_else(|| "unknown".to_string())
                .to_ascii_lowercase();
            format!("debrid:{provider}:{implementation}")
        }
        "torrent" => format!("torrent:{provider}"),
        "usenet" => format!("usenet:{provider}"),
        other => format!("{other}:{provider}"),
    }
}

pub fn route_attempt_ledger(
    candidate_fingerprint: &str,
    attempts: &[RouteAttemptRecord],
) -> JsonValue {
    json!({
        "candidateFingerprint": candidate_fingerprint,
        "attempts": attempts.iter().map(RouteAttemptRecord::to_json).collect::<Vec<_>>(),
    })
}

pub fn attach_route_attempt_ledger(
    evidence: &mut JsonValue,
    candidate_fingerprint: &str,
    attempts: &[RouteAttemptRecord],
) {
    if attempts.is_empty() {
        return;
    }
    let Some(object) = evidence.as_object_mut() else {
        return;
    };
    let mut ledger = route_attempt_ledger(candidate_fingerprint, attempts);
    if let Some(existing) = object.get(ROUTE_ATTEMPT_LEDGER_KEY)
        && existing
            .get("candidateFingerprint")
            .and_then(JsonValue::as_str)
            == Some(candidate_fingerprint)
        && let (Some(existing_attempts), Some(new_attempts)) = (
            existing.get("attempts").and_then(JsonValue::as_array),
            ledger.get_mut("attempts").and_then(JsonValue::as_array_mut),
        )
    {
        let mut merged = existing_attempts.clone();
        merged.append(new_attempts);
        ledger["attempts"] = JsonValue::Array(merged);
    }
    object.insert(ROUTE_ATTEMPT_LEDGER_KEY.to_string(), ledger);
}

pub fn seed_route_attempt_ledger(
    evidence: &mut JsonValue,
    candidate_fingerprint: &str,
    ledger: Option<&JsonValue>,
) {
    let Some(ledger) = ledger else {
        return;
    };
    if ledger
        .get("candidateFingerprint")
        .and_then(JsonValue::as_str)
        != Some(candidate_fingerprint)
    {
        return;
    }
    if let Some(object) = evidence.as_object_mut() {
        object.insert(ROUTE_ATTEMPT_LEDGER_KEY.to_string(), ledger.clone());
    }
}

pub fn route_attempt_ledger_for_candidate(
    evidence: &JsonValue,
    candidate_fingerprint: &str,
) -> Option<JsonValue> {
    let ledger = evidence.get(ROUTE_ATTEMPT_LEDGER_KEY)?;
    (ledger
        .get("candidateFingerprint")
        .and_then(JsonValue::as_str)
        == Some(candidate_fingerprint))
    .then(|| ledger.clone())
}

pub fn route_attempt_record_from_evidence(
    evidence: &JsonValue,
    route_logical_id: &str,
    status: RouteAttemptStatus,
    download_id: Option<String>,
    failure_class: Option<&str>,
    reason: Option<String>,
) -> Option<RouteAttemptRecord> {
    let (provider_id, implementation) =
        route_attempt_descriptor_from_ledger(evidence, route_logical_id, download_id.as_deref())
            .or_else(|| {
                route_attempt_descriptor_from_submission_result(evidence, route_logical_id)
            })?;
    Some(RouteAttemptRecord::new(
        route_logical_id,
        Some(provider_id),
        implementation.as_deref(),
        download_id,
        status,
        failure_class,
        reason,
    ))
}

pub fn coverage_plan_with_route_attempt_ledger(
    plan: JsonValue,
    selected_candidate: &JsonValue,
) -> JsonValue {
    let Some(ledger) = selected_candidate.get(ROUTE_ATTEMPT_LEDGER_KEY) else {
        return plan;
    };
    match plan {
        JsonValue::Object(mut object) => {
            object.insert(ROUTE_ATTEMPT_LEDGER_KEY.to_string(), ledger.clone());
            JsonValue::Object(object)
        }
        other => json!({
            "coveragePlan": other,
            ROUTE_ATTEMPT_LEDGER_KEY: ledger,
        }),
    }
}

#[allow(dead_code)]
pub fn route_attempt_spent_keys(
    evidence: &JsonValue,
    candidate_fingerprint: &str,
) -> BTreeSet<String> {
    let Some(ledger) = evidence.get(ROUTE_ATTEMPT_LEDGER_KEY) else {
        return BTreeSet::new();
    };
    if ledger
        .get("candidateFingerprint")
        .and_then(JsonValue::as_str)
        != Some(candidate_fingerprint)
    {
        return BTreeSet::new();
    }
    ledger
        .get("attempts")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|attempt| {
            let status = attempt.get("status").and_then(JsonValue::as_str)?;
            matches!(status, "submitted" | "failed" | "blocked").then(|| {
                attempt
                    .get("attemptKey")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string)
            })?
        })
        .collect()
}

#[allow(dead_code)]
pub fn route_attempt_was_spent(
    evidence: &JsonValue,
    candidate_fingerprint: &str,
    attempt_key: &str,
) -> bool {
    route_attempt_spent_keys(evidence, candidate_fingerprint).contains(attempt_key)
}

fn route_attempt_family(route_logical_id: &str) -> &'static str {
    match route_logical_id {
        DEBRID_DEFAULT_LOGICAL_ID => "debrid",
        TORRENT_DEFAULT_LOGICAL_ID => "torrent",
        USENET_DEFAULT_LOGICAL_ID => "usenet",
        _ => "route",
    }
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn route_attempt_descriptor_from_ledger(
    evidence: &JsonValue,
    route_logical_id: &str,
    download_id: Option<&str>,
) -> Option<(Uuid, Option<String>)> {
    let attempts = evidence
        .get(ROUTE_ATTEMPT_LEDGER_KEY)?
        .get("attempts")?
        .as_array()?;
    if let Some(download_id) = normalize_optional(download_id)
        && let Some(descriptor) = attempts.iter().rev().find_map(|attempt| {
            (attempt.get("downloadId").and_then(JsonValue::as_str) == Some(download_id.as_str()))
                .then(|| route_attempt_descriptor_from_attempt(attempt, route_logical_id))?
        })
    {
        return Some(descriptor);
    }
    attempts
        .iter()
        .rev()
        .find_map(|attempt| route_attempt_descriptor_from_attempt(attempt, route_logical_id))
}

fn route_attempt_descriptor_from_attempt(
    attempt: &JsonValue,
    route_logical_id: &str,
) -> Option<(Uuid, Option<String>)> {
    if attempt.get("routeLogicalId").and_then(JsonValue::as_str) != Some(route_logical_id) {
        return None;
    }
    let provider_id = attempt
        .get("providerId")
        .and_then(JsonValue::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())?;
    let implementation = attempt
        .get("implementation")
        .and_then(JsonValue::as_str)
        .and_then(|value| normalize_optional(Some(value)));
    Some((provider_id, implementation))
}

fn route_attempt_descriptor_from_submission_result(
    evidence: &JsonValue,
    route_logical_id: &str,
) -> Option<(Uuid, Option<String>)> {
    let result = evidence.get("submissionResult")?;
    if result.get("routeLogicalId").and_then(JsonValue::as_str) != Some(route_logical_id) {
        return None;
    }
    let provider_id = result
        .get("routeProviderId")
        .and_then(JsonValue::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())?;
    let implementation = result
        .get("routeProviderImplementation")
        .and_then(JsonValue::as_str)
        .and_then(|value| normalize_optional(Some(value)));
    Some((provider_id, implementation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dfu3_route_attempt_key_is_route_family_specific() {
        let provider_id = Uuid::new_v4();
        assert_eq!(
            route_attempt_key(DEBRID_DEFAULT_LOGICAL_ID, Some(provider_id), Some("TorBox")),
            format!("debrid:{provider_id}:torbox")
        );
        assert_eq!(
            route_attempt_key(
                TORRENT_DEFAULT_LOGICAL_ID,
                Some(provider_id),
                Some("qbittorrent")
            ),
            format!("torrent:{provider_id}")
        );
        assert_eq!(
            route_attempt_key(USENET_DEFAULT_LOGICAL_ID, Some(provider_id), Some("nzbget")),
            format!("usenet:{provider_id}")
        );
    }

    #[test]
    fn dfu3_route_attempt_ledger_is_scoped_to_candidate_fingerprint() {
        let torbox_provider = Uuid::new_v4();
        let real_debrid_provider = Uuid::new_v4();
        let attempts = vec![
            RouteAttemptRecord::new(
                DEBRID_DEFAULT_LOGICAL_ID,
                Some(torbox_provider),
                Some("torbox"),
                None,
                RouteAttemptStatus::Failed,
                Some("no_seeds"),
                None,
            ),
            RouteAttemptRecord::new(
                DEBRID_DEFAULT_LOGICAL_ID,
                Some(real_debrid_provider),
                Some("real_debrid"),
                None,
                RouteAttemptStatus::Failed,
                Some("provider_unavailable"),
                None,
            ),
        ];
        let mut evidence = json!({});
        attach_route_attempt_ledger(&mut evidence, "v1:magnet:one", &attempts);

        assert!(route_attempt_was_spent(
            &evidence,
            "v1:magnet:one",
            &format!("debrid:{torbox_provider}:torbox")
        ));
        assert!(route_attempt_was_spent(
            &evidence,
            "v1:magnet:one",
            &format!("debrid:{real_debrid_provider}:real_debrid")
        ));
        assert!(!route_attempt_was_spent(
            &evidence,
            "v1:magnet:two",
            &format!("debrid:{torbox_provider}:torbox")
        ));
    }

    #[test]
    fn dfu3_route_attempt_record_can_be_reconstructed_from_ledger() {
        let provider_id = Uuid::new_v4();
        let attempts = vec![RouteAttemptRecord::new(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(provider_id),
            Some("torbox"),
            Some("job-1".to_string()),
            RouteAttemptStatus::Submitted,
            None,
            None,
        )];
        let mut evidence = json!({});
        attach_route_attempt_ledger(&mut evidence, "v1:magnet:one", &attempts);

        let failed = route_attempt_record_from_evidence(
            &evidence,
            DEBRID_DEFAULT_LOGICAL_ID,
            RouteAttemptStatus::Failed,
            Some("job-1".to_string()),
            Some("no_seeds"),
            None,
        )
        .expect("failed route attempt");
        assert_eq!(failed.provider_id, Some(provider_id));
        assert_eq!(failed.implementation.as_deref(), Some("torbox"));
        assert_eq!(failed.attempt_key, format!("debrid:{provider_id}:torbox"));
        assert_eq!(failed.failure_class.as_deref(), Some("no_seeds"));
    }

    #[test]
    fn dfu3_route_attempt_record_prefers_matching_download_id() {
        let torbox_provider = Uuid::new_v4();
        let premiumize_provider = Uuid::new_v4();
        let attempts = vec![
            RouteAttemptRecord::new(
                DEBRID_DEFAULT_LOGICAL_ID,
                Some(torbox_provider),
                Some("torbox"),
                Some("job-original".to_string()),
                RouteAttemptStatus::Submitted,
                None,
                None,
            ),
            RouteAttemptRecord::new(
                DEBRID_DEFAULT_LOGICAL_ID,
                Some(premiumize_provider),
                Some("premiumize"),
                None,
                RouteAttemptStatus::Failed,
                Some("provider_unavailable"),
                None,
            ),
        ];
        let mut evidence = json!({});
        attach_route_attempt_ledger(&mut evidence, "v1:magnet:one", &attempts);

        let failed = route_attempt_record_from_evidence(
            &evidence,
            DEBRID_DEFAULT_LOGICAL_ID,
            RouteAttemptStatus::Failed,
            Some("job-original".to_string()),
            Some("no_seeds"),
            None,
        )
        .expect("failed route attempt");
        assert_eq!(failed.provider_id, Some(torbox_provider));
        assert_eq!(failed.implementation.as_deref(), Some("torbox"));
        assert_eq!(
            failed.attempt_key,
            format!("debrid:{torbox_provider}:torbox")
        );
    }
}
