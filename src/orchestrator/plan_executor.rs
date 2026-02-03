use std::sync::Arc;

use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

use crate::db::models::OperationStepStatus;
use crate::extensions::store::ExtensionStore;
use crate::orchestrator::lock::APPLY_LOCK_NAME;
use crate::orchestrator::executor::ExecutorAction;
use crate::state::AppState;

#[derive(Clone)]
pub struct PlanExecutor {
    state: Arc<AppState>,
}

pub struct PlannedStep {
    pub step_id: Uuid,
    pub action: ExecutorAction,
}

impl PlanExecutor {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn execute(&self, actions: Vec<ExecutorAction>) -> Result<()> {
        // Placeholder loop for plan execution; run/step tracking will wrap this later.
        self.state.orchestrator.apply_actions(actions).await
    }

    pub async fn execute_steps(&self, steps: Vec<PlannedStep>) -> Result<()> {
        let store = ExtensionStore::new(&self.state.db_pool);
        let owner_id = Uuid::new_v4().to_string();
        let ttl = Duration::from_secs(
            self.state
                .settings
                .extensions
                .apply_lock_ttl_seconds
                .max(1),
        );

        let mut acquired = false;
        for _ in 0..10 {
            if store.acquire_lock(APPLY_LOCK_NAME, &owner_id, ttl).await? {
                acquired = true;
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        if !acquired {
            return Err(anyhow!("orchestrator apply lock is already held"));
        }

        let result = self.execute_steps_locked(&store, steps).await;
        let _ = store.release_lock(APPLY_LOCK_NAME, &owner_id).await;
        result
    }

    async fn execute_steps_locked(
        &self,
        store: &ExtensionStore<'_>,
        steps: Vec<PlannedStep>,
    ) -> Result<()> {
        for step in steps {
            store
                .update_step_status(step.step_id, OperationStepStatus::Running, None)
                .await?;
            if let Err(err) = self
                .state
                .orchestrator
                .apply_actions(vec![step.action])
                .await
            {
                let _ = store
                    .update_step_status(
                        step.step_id,
                        OperationStepStatus::Failed,
                        Some(&err.to_string()),
                    )
                    .await;
                return Err(err);
            }
            store
                .update_step_status(step.step_id, OperationStepStatus::Completed, None)
                .await?;
        }
        Ok(())
    }
}
