use std::collections::HashSet;

use anyhow::Result;

use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::ExtensionStore;
use crate::orchestrator::planner::PlanAction;

pub async fn missing_required_secrets_for_plan(
    store: &ExtensionStore<'_>,
    actions: &[PlanAction],
) -> Result<Vec<String>> {
    let mut missing = HashSet::new();
    for action in actions {
        if let PlanAction::EnsureRuntimeRunning { runtime, .. } = action {
            let required = required_secrets_from_runtime(&runtime.runtime.env)?;
            if required.is_empty() {
                continue;
            }
            let missing_for_instance =
                missing_required_secrets_for_instance(store, runtime.instance_id, &required)
                    .await?;
            missing.extend(missing_for_instance);
        }
    }
    let mut missing: Vec<_> = missing.into_iter().collect();
    missing.sort();
    Ok(missing)
}

pub fn has_unresolved_conflicts(conflicts: &[serde_json::Value]) -> bool {
    conflicts.iter().any(|conflict| {
        let code = conflict.get("code").and_then(|value| value.as_str());
        match code {
            Some("missing_required_secrets") => false,
            Some("slot_conflict") => {
                let policy = conflict
                    .get("policy")
                    .and_then(|value| value.as_str())
                    .unwrap_or("prompt");
                let resolved = conflict
                    .get("resolved")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                match policy {
                    "auto_replace" => false,
                    _ => !resolved,
                }
            }
            _ => true,
        }
    })
}
