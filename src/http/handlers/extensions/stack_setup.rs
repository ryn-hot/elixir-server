//! Marketplace stacks set themselves up.
//!
//! A stack's setup blueprint (`stack.<hash>.setup`) needs no answers from the
//! owner: they approved the stack's permissions when they installed it. So the
//! server runs it as soon as the stack offers it, through the same apply and
//! confirm steps the old "Finish setup" button used. A failed setup is shown
//! on the stack's card with a retry button; it is not retried in a loop. A
//! setup cut short by a server restart is retried once on the next start.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::Json;
use tokio_util::sync::CancellationToken;

use crate::db::models::{ExtensionKind, OrchestratorRun, OrchestratorRunStatus};
use crate::extensions::store::ExtensionStore;
use crate::http::error::ApiError;
use crate::state::AppState;
use crate::marketplace::stack::is_stack_member_id;

/// How often the runner looks for a stack whose setup has not run yet.
const TICK: Duration = Duration::from_secs(15);
/// The error the orchestrator records on runs a restart cut short.
const INTERRUPTED_BY_RESTART: &str = "server restarted";
/// Another apply (usually a background reconcile pass) held the lock for
/// longer than the plan waited. Nothing ran; the runner simply tries again.
const APPLY_LOCK_BUSY: &str = "orchestrator apply lock is already held";

/// One setup at a time: each one starts containers and waits for them.
static SETUP: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));
/// Setups this process already retried after a restart, so it happens once.
static RETRIED_AFTER_RESTART: LazyLock<std::sync::Mutex<HashSet<String>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

/// Where a stack's setup stands, for its status card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StackSetupState {
    /// Not offered yet: the stack is still being prepared.
    Preparing,
    /// Offered and waiting for the runner, or running now.
    Running,
    /// Ran to the end.
    Done,
    /// The last attempt failed, with the reason it gave.
    Failed(String),
}

/// The newest setup run of each stack setup blueprint, by blueprint id.
pub(super) async fn latest_setup_runs(
    store: &ExtensionStore<'_>,
) -> anyhow::Result<HashMap<String, OrchestratorRun>> {
    let mut latest: HashMap<String, OrchestratorRun> = HashMap::new();
    for status in [
        OrchestratorRunStatus::Pending,
        OrchestratorRunStatus::Running,
        OrchestratorRunStatus::Completed,
        OrchestratorRunStatus::Failed,
        OrchestratorRunStatus::Canceled,
    ] {
        for run in store.list_runs_by_source_status("blueprint", status).await? {
            let Some(blueprint_id) = run
                .plan_json
                .as_ref()
                .and_then(|plan| plan.get("blueprint_id"))
                .and_then(|value| value.as_str())
                .map(str::to_owned)
            else {
                continue;
            };
            if !is_stack_member_id(&blueprint_id) {
                continue;
            }
            let newer = latest
                .get(&blueprint_id)
                .is_none_or(|existing| run.created_at > existing.created_at);
            if newer {
                latest.insert(blueprint_id, run);
            }
        }
    }
    Ok(latest)
}

/// Where one stack setup stands, from its offer (`desired_blueprints`) and
/// its newest run.
pub(super) fn setup_state(
    setup_id: &str,
    pending: &HashSet<String>,
    applied: &HashSet<String>,
    runs: &HashMap<String, OrchestratorRun>,
) -> StackSetupState {
    match runs.get(setup_id) {
        Some(run) if run.status == OrchestratorRunStatus::Running => StackSetupState::Running,
        Some(run) if run.status == OrchestratorRunStatus::Failed => {
            let reason = run
                .error
                .clone()
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| "the setup stopped with an error".to_string());
            // Cut short by a restart (retried once) or by a busy lock (retried
            // until it runs): the runner tries again by itself.
            if (reason == INTERRUPTED_BY_RESTART && !already_retried(setup_id))
                || reason.contains(APPLY_LOCK_BUSY)
            {
                StackSetupState::Running
            } else {
                StackSetupState::Failed(reason)
            }
        }
        _ if applied.contains(setup_id) => StackSetupState::Done,
        _ if pending.contains(setup_id) => StackSetupState::Running,
        _ => StackSetupState::Preparing,
    }
}

fn already_retried(setup_id: &str) -> bool {
    RETRIED_AFTER_RESTART
        .lock()
        .map(|retried| retried.contains(setup_id))
        .unwrap_or(true)
}

/// Runs every offered stack setup that has not run yet. Started once at
/// server start; stops when `shutdown` is cancelled.
pub fn spawn_runner(state: AppState, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(TICK) => {}
            }
            if let Err(error) = run_due_setups(&state).await {
                tracing::warn!("stack setup runner: {error:#}");
            }
        }
    })
}

async fn run_due_setups(state: &AppState) -> anyhow::Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let extensions = store.list_extensions().await?;
    let mut pending = HashSet::new();
    let mut applied = HashSet::new();
    for desired in store.list_desired_blueprints(None).await? {
        if desired.applied {
            applied.insert(desired.blueprint_extension_id);
        } else {
            pending.insert(desired.blueprint_extension_id);
        }
    }
    let runs = latest_setup_runs(&store).await?;
    for setup in extensions.iter().filter(|row| {
        row.enabled && row.kind == ExtensionKind::Blueprint && is_stack_member_id(&row.extension_id)
    }) {
        let id = &setup.extension_id;
        // Only a stack that is still installed and switched on.
        if crate::marketplace::stack::owner_of(&state.db_pool, id)
            .await?
            .is_none()
        {
            continue;
        }
        let due = match runs.get(id) {
            Some(run) if run.status == OrchestratorRunStatus::Running => false,
            Some(run) if run.status == OrchestratorRunStatus::Failed => {
                let reason = run.error.as_deref().unwrap_or_default();
                reason.contains(APPLY_LOCK_BUSY)
                    || (reason == INTERRUPTED_BY_RESTART && mark_retried(id))
            }
            _ => pending.contains(id),
        };
        if !due {
            continue;
        }
        tracing::info!(blueprint = %id, "running marketplace stack setup");
        match run_setup(state, id).await {
            Ok(()) => tracing::info!(blueprint = %id, "marketplace stack setup finished"),
            Err(error) => {
                tracing::warn!(blueprint = %id, "marketplace stack setup failed: {error:#}")
            }
        }
    }
    Ok(())
}

/// True the first time it is called for `setup_id` in this process.
fn mark_retried(setup_id: &str) -> bool {
    RETRIED_AFTER_RESTART
        .lock()
        .map(|mut retried| retried.insert(setup_id.to_owned()))
        .unwrap_or(false)
}

/// Plans and runs one stack setup, exactly as Finish setup did: the planner
/// installs the stack's packages and the confirm step runs every phase. A
/// refusal before anything runs (missing secrets, conflicts) is recorded on
/// the run, so the card can say what stopped it.
pub(super) async fn run_setup(state: &AppState, setup_id: &str) -> anyhow::Result<()> {
    let _one_at_a_time = SETUP.lock().await;
    let plan = super::apply_blueprint(
        State(state.clone()),
        Json(super::ApplyBlueprintRequest {
            blueprint_id: setup_id.to_owned(),
            params: None,
        }),
    )
    .await
    .map_err(|error| anyhow::anyhow!(api_error_message(error)))?
    .0;
    match super::confirm_plan(State(state.clone()), Path(plan.plan_id.to_string()), None).await {
        Ok(_) => Ok(()),
        Err(error) => {
            let message = api_error_message(error);
            let store = ExtensionStore::new(&state.db_pool);
            if let Ok(Some(run)) = store.get_run(plan.plan_id).await
                && run.status == OrchestratorRunStatus::Pending
            {
                let _ = store
                    .update_run_status(
                        plan.plan_id,
                        OrchestratorRunStatus::Failed,
                        Some("failed"),
                        Some(&message),
                    )
                    .await;
            }
            Err(anyhow::anyhow!(message))
        }
    }
}

fn api_error_message(error: ApiError) -> String {
    match error {
        ApiError::BadRequest(message)
        | ApiError::Unauthorized(message)
        | ApiError::Forbidden(message)
        | ApiError::NotFound(message)
        | ApiError::Conflict(message)
        | ApiError::Internal(message)
        | ApiError::ConflictWithDetails { message, .. }
        | ApiError::Structured { message, .. } => message,
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct RetrySetupRequest {
    /// The stack's own extension id, or its setup blueprint's id.
    pub extension_id: String,
}

/// `POST /api/v1/extension-setup/retry`: runs a stack's setup again after it
/// failed. Answers at once; the card shows the progress.
pub async fn retry_setup(
    State(state): State<AppState>,
    Json(request): Json<RetrySetupRequest>,
) -> crate::http::error::ApiResult<Json<serde_json::Value>> {
    let store = ExtensionStore::new(&state.db_pool);
    let setup_id = setup_blueprint_for(&store, &request.extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("no stack setup belongs to this extension"))?;
    let runs = latest_setup_runs(&store).await.map_err(ApiError::from)?;
    if runs
        .get(&setup_id)
        .is_some_and(|run| run.status == OrchestratorRunStatus::Running)
    {
        return Err(ApiError::conflict("this stack's setup is already running"));
    }
    let task_state = state.clone();
    let task_setup = setup_id.clone();
    tokio::spawn(async move {
        if let Err(error) = run_setup(&task_state, &task_setup).await {
            tracing::warn!(blueprint = %task_setup, "marketplace stack setup retry failed: {error:#}");
        }
    });
    Ok(Json(serde_json::json!({ "setup_id": setup_id, "status": "started" })))
}

/// The setup blueprint of a stack, given the stack's own id or the setup's.
async fn setup_blueprint_for(
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> anyhow::Result<Option<String>> {
    let Some(extension) = store.get_extension(extension_id).await? else {
        return Ok(None);
    };
    if extension.kind == ExtensionKind::Blueprint && is_stack_member_id(extension_id) {
        return Ok(Some(extension.extension_id));
    }
    let Ok(manifest) = serde_json::from_value::<crate::extensions::manifest::ExtensionManifest>(
        extension.manifest_json.clone(),
    ) else {
        return Ok(None);
    };
    Ok(manifest.stack.as_ref().map(|stack| {
        crate::marketplace::stack::derived_id(&extension.extension_id, &stack.blueprint.id)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(status: OrchestratorRunStatus, error: Option<&str>) -> OrchestratorRun {
        OrchestratorRun {
            run_id: uuid::Uuid::new_v4(),
            source: "blueprint".to_string(),
            status,
            phase: None,
            plan_json: None,
            error: error.map(str::to_owned),
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
        }
    }

    #[test]
    fn a_setup_is_never_waiting_on_the_owner() {
        let setup = "stack.0000aaaa.setup";
        let none = HashSet::new();
        let offered = HashSet::from([setup.to_string()]);
        let no_runs = HashMap::new();
        assert_eq!(setup_state(setup, &none, &none, &no_runs), StackSetupState::Preparing);
        assert_eq!(setup_state(setup, &offered, &none, &no_runs), StackSetupState::Running);
        assert_eq!(setup_state(setup, &none, &offered, &no_runs), StackSetupState::Done);
    }

    #[test]
    fn only_real_failures_ask_for_a_retry() {
        let setup = "stack.0000bbbb.setup";
        let applied = HashSet::from([setup.to_string()]);
        let none = HashSet::new();
        // A busy lock: nothing ran, the runner tries again by itself.
        let busy = HashMap::from([(
            setup.to_string(),
            run(OrchestratorRunStatus::Failed, Some(APPLY_LOCK_BUSY)),
        )]);
        assert_eq!(setup_state(setup, &none, &applied, &busy), StackSetupState::Running);
        // A real failure keeps its reason and waits for the owner's retry.
        let failed = HashMap::from([(
            setup.to_string(),
            run(OrchestratorRunStatus::Failed, Some("Prowlarr did not become healthy")),
        )]);
        assert_eq!(
            setup_state(setup, &none, &applied, &failed),
            StackSetupState::Failed("Prowlarr did not become healthy".to_string())
        );
        let running = HashMap::from([(setup.to_string(), run(OrchestratorRunStatus::Running, None))]);
        assert_eq!(setup_state(setup, &none, &applied, &running), StackSetupState::Running);
    }
}
