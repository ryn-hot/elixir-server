//! Original real source/probe, followed by deterministic real grant revocation
//! at an acknowledged pre-claim SQL boundary. No candidate task may be started.
use super::*;
use crate::{
    db::DatabaseDriver,
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::operation::{OwnedCandidate, Phase},
};

impl Configured {
    pub(in super::super) async fn finish_pre_candidate_failure(
        &mut self,
        launch: &mut SourceLaunch,
        staged: &StagedInstallation,
        case: &str,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        let source = launch.finish().await?;
        let prepared = Box::pin(Box::pin(source.probe()).await?.retain()).await?;
        let copied = case.ends_with("_copy");
        let phase = if copied { "state_copied" } else { "quiescing" };
        // The trigger changes ONLY the grant after the original transition
        // writes. That transition returns normally; the NEXT authority check
        // fails. It does not fabricate a source result or disposal receipt.
        match self.database.driver {
            DatabaseDriver::Sqlite => {
                sqlx::query(&format!("CREATE TRIGGER preclaim_revoke AFTER UPDATE OF record_json ON marketplace_installations WHEN json_extract(NEW.record_json,'$.activation.phase')='{phase}' BEGIN UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{{}}',revision=revision+1,revocation_revision=revision+1; END"))
                    .execute(&self.database.pool).await?;
            }
            DatabaseDriver::Postgres => {
                sqlx::query(&format!("CREATE FUNCTION marketplace_fixture_preclaim_revoke() RETURNS trigger LANGUAGE plpgsql AS $fault$ BEGIN IF NEW.record_json::jsonb #>> '{{activation,phase}}' = '{phase}' THEN UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{{}}',revision=revision+1,revocation_revision=revision+1; END IF; RETURN NEW; END $fault$"))
                    .execute(&self.database.pool).await?;
                sqlx::query("CREATE TRIGGER preclaim_revoke AFTER UPDATE OF record_json ON marketplace_installations FOR EACH ROW EXECUTE FUNCTION marketplace_fixture_preclaim_revoke()")
                    .execute(&self.database.pool).await?;
            }
        }
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
        let mut blocker = self.database.pool.begin().await?;
        sqlx::query(
            "UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1",
        )
        .execute(&mut *blocker)
        .await?;
        let mut owners = Some((prepared, self.controller.take().context("controller")?));
        self.operation = Some(Box::new(OwnedCandidate::begin(&mut owners, limits)?));
        assert!(owners.is_none());
        let operation = self.operation.as_mut().context("operation")?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), operation.wait())
                .await
                .is_err()
        );
        assert!(operation.take_controller().is_err());
        blocker.rollback().await?;
        let failed = operation.wait().await?;
        assert_eq!(failed.phase, Phase::RecoveryRequired);
        assert!(operation.take_controller().is_err());
        assert!(
            Box::pin(operation.activate(&self.principal, Uuid::new_v4()))
                .await
                .is_err()
        );
        let id = staged.configured_instance().context("configured owner")?;
        let record: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(id.to_string())
        .fetch_one(&self.database.pool)
        .await?;
        let journal: serde_json::Value = serde_json::from_str(&record)?;
        assert_eq!(journal["activation"]["phase"], phase);
        let original_revision = if copied { 6 } else { 5 };
        assert_eq!(journal["revision"], original_revision);
        let state:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(staged.instance().to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(state, (if copied { 2 } else { 1 }, 0, 0));
        let secrets: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
                .fetch_one(&self.database.pool)
                .await?;
        assert_eq!(secrets, i64::from(copied));
        // Remove the explicit test fault only after the ORIGINAL task joined.
        match self.database.driver {
            DatabaseDriver::Sqlite => {
                sqlx::query("DROP TRIGGER preclaim_revoke")
                    .execute(&self.database.pool)
                    .await?;
            }
            DatabaseDriver::Postgres => {
                sqlx::query("DROP TRIGGER preclaim_revoke ON marketplace_installations")
                    .execute(&self.database.pool)
                    .await?;
                sqlx::query("DROP FUNCTION marketplace_fixture_preclaim_revoke()")
                    .execute(&self.database.pool)
                    .await?;
            }
        }
        let store = ExtensionStore::new(&self.database.pool);
        store.set_instance_enabled(id, false).await?;
        store
            .update_instance_config(id, Some(&json!({"owner":"keep"})))
            .await?;
        let before:(String,Option<String>,String,i64)=sqlx::query_as("SELECT instance_name,config_json,extension_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(id.to_string()).fetch_one(&self.database.pool).await?;
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
        sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
            .bind((1_i64 << 53) - 2)
            .execute(&self.database.pool)
            .await?;
        if let Some(peer) = peer.take() {
            peer.finish().await?;
        }
        let done = Box::pin(operation.cancel(&fresh, mutation)).await?;
        assert_eq!(done.phase, Phase::Cancelled);
        assert_eq!(done.revision, Some(original_revision + 1));
        assert_eq!(
            Box::pin(operation.cancel(&fresh, mutation)).await?.revision,
            done.revision
        );
        let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
            .fetch_one(&self.database.pool).await?;
        assert_eq!(remaining, 0);
        let after:(String,Option<String>,String,i64)=sqlx::query_as("SELECT instance_name,config_json,extension_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(id.to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(after, before);
        self.controller = Some(operation.take_controller()?);
        self.operation = None;
        eprintln!(
            "original pre-claim {phase} output survived a later real authority failure/lost waiter; fresh-owner offline final-capacity atomic cancellation and exact retry preserved settings, without candidate launch"
        );
        Ok(())
    }
}
