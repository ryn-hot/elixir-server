use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::db::models::OperationStepStatus;
use crate::extensions::store::ExtensionStore;
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
