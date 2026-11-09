//! Actual peer/source/probe/disposal and real held SQL writer. The stop must
//! retain the returned claim WITHOUT ever giving it to a candidate task.
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::operation::{OwnedCandidate, Phase},
};

impl Configured {
    pub(in super::super) async fn finish_unstarted(
        &mut self,
        launch: &mut SourceLaunch,
        staged: &StagedInstallation,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        let source = launch.finish().await?;
        let probed = Box::pin(source.probe()).await?;
        let prepared = Box::pin(probed.retain()).await?;
        let limits = SourceExecutionLimits {
            profile: Default::default(),
            heap_bytes: 16 * MIB,
            timeout_millis: 30_000,
            max_file_bytes: 4 * MIB,
            json_bytes: MIB,
            pending_jobs: 128,
            sdk: Some(SourceSdkLimits {
                calls: 16,
                request_bytes: MIB,
                response_bytes: MIB,
            }),
        };
        let owner = staged.configured_instance().context("configured owner")?;
        // Block the ORIGINAL future's first activation transaction. It must
        // settle all quiesce/copy/claim SQL after the stop, without launching.
        let mut blocker = self.database.pool.begin().await?;
        sqlx::query(
            "UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1",
        )
        .execute(&mut *blocker)
        .await?;
        let mut inputs = Some((prepared, self.controller.take().context("controller")?));
        self.operation = Some(Box::new(OwnedCandidate::begin(&mut inputs, limits)?));
        assert!(inputs.is_none());
        let operation = self.operation.as_mut().context("operation")?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), operation.wait())
                .await
                .is_err()
        );
        assert!(operation.take_controller().is_err());
        operation.request_stop();
        blocker.rollback().await?;
        let done = operation.wait().await?;
        assert_eq!(done.phase, Phase::AwaitingCleanup);
        assert_eq!(
            done.revision,
            Some(7),
            "must retain the never-launched original candidate claim, not a native retirement"
        );
        assert!(operation.take_controller().is_err());
        assert!(
            Box::pin(operation.activate(&self.principal, Uuid::new_v4()))
                .await
                .is_err()
        );
        let record: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(owner.to_string())
        .fetch_one(&self.database.pool)
        .await?;
        let journal: serde_json::Value = serde_json::from_str(&record)?;
        assert_eq!(journal["activation"]["phase"], "candidate_starting");
        let state:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(staged.instance().to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(
            state,
            (2, 0, 0),
            "copy committed but publisher migration must not execute"
        );
        let values: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values")
                .fetch_one(&self.database.pool)
                .await?;
        let secrets: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
                .fetch_one(&self.database.pool)
                .await?;
        assert_eq!(
            (values, secrets),
            (0, 1),
            "only the sealed owner-input credential was copied"
        );
        // Corroboration only: a free lane is NOT the cleanup permission. The
        // production receipt consumed the original one-use returned claim.
        let lane = ControllerLease::acquire(&self.configuration.lane)?;
        assert!(crate::marketplace::lease::activity::Fence::load(&lane)?.is_none());
        drop(lane);
        let store = ExtensionStore::new(&self.database.pool);
        store.set_instance_enabled(owner, false).await?;
        store
            .update_instance_config(owner, Some(&json!({"owner":"keep"})))
            .await?;
        let before:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
            .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
        sqlx::query(
            "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
        )
        .bind(self.principal.profile_id.to_string())
        .execute(&self.database.pool)
        .await?;
        let mutation = Uuid::new_v4();
        assert!(
            Box::pin(operation.cancel(&self.principal, mutation))
                .await
                .is_err()
        );
        let revision:String=sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1")
            .bind(self.principal.profile_id.to_string()).fetch_one(&self.database.pool).await?;
        let mut capabilities = crate::authz::CapabilitySet::all();
        capabilities.remove(crate::authz::Capability::SecretsManage);
        let fresh = CurrentPrincipal::fixture_owner(
            self.principal.user_id,
            self.principal.home_id,
            self.principal.profile_id,
            self.principal.account_session_id,
            revision.parse()?,
            self.principal
                .session_expires_at
                .min(self.principal.access_expires_at),
            capabilities,
        );
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
            .execute(&self.database.pool).await?;
        sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
            .bind((1_i64 << 53) - 2)
            .execute(&self.database.pool)
            .await?;
        if let Some(peer) = peer.take() {
            peer.finish().await?;
        }
        let cancelled = Box::pin(operation.cancel(&fresh, mutation)).await?;
        assert_eq!(cancelled.phase, Phase::Cancelled);
        assert_eq!(cancelled.revision, Some(8));
        assert_eq!(
            Box::pin(operation.cancel(&fresh, mutation)).await?.revision,
            Some(8)
        );
        let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
            .fetch_one(&self.database.pool).await?;
        assert_eq!(remaining, 0);
        let after:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
            .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(before, after);
        self.controller = Some(operation.take_controller()?);
        self.operation = None;
        eprintln!(
            "owned candidate claim stop: original SQL completed, original returned claim permanently sealed without a native candidate launch or migration; fresh-owner offline final-capacity atomic cleanup/exact retry preserved settings"
        );
        Ok(())
    }
}
