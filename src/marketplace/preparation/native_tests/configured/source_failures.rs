//! Original live source task/native probe/disposal followed by real controls.
//! Discovery/protection remain synthetic; cleanup receipts are NOT fabricated.
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::operation::{OwnedCandidate, Phase},
};

impl Configured {
    pub(in super::super) async fn source_failure(
        &mut self,
        launch: &mut Option<SourceLaunch>,
        staged: &StagedInstallation,
        case: &str,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        let owner = staged.configured_instance().context("owner")?;
        let store = ExtensionStore::new(&self.database.pool);
        let retention = case.ends_with("retention") || case.ends_with("ack");
        let committed = case.ends_with("ack");
        let mut direct = None;
        if retention {
            let matched = Box::pin(launch.as_mut().context("original launch")?.finish()).await?;
            let mut probed = Box::new(Some(Box::pin(matched.probe()).await?));
            if committed {
                // Actual successful native probe/disposal AND actual retention
                // commit. Discard only its ack, NOT its original disposal owner.
                Box::pin(
                    probed
                        .as_mut()
                        .as_mut()
                        .context("probe owner")?
                        .fixture_discard_retention_ack(),
                )
                .await?;
            }
            store
                .set_instance_enabled(owner, false)
                .await
                .context("owner disables the instance at the retention boundary")?;
            if !committed {
                let error = Box::pin(
                    probed
                        .as_mut()
                        .as_mut()
                        .context("probe owner")?
                        .fixture_discard_retention_ack(),
                )
                .await
                .err()
                .context("superseded retention accepted")?;
                assert!(
                    format!("{error:#}").contains("choices changed"),
                    "{error:#}"
                );
                assert!(
                    probed.is_some(),
                    "SQL error must retain original probed owner"
                );
            }
            direct = Some(Box::pin(probed.take().context("retained probe owner")?.close()).await?);
        } else {
            if case.ends_with("restore") {
                tokio::time::timeout(Duration::from_secs(110),async {
                    loop {
                        match launch.as_ref().context("source")?.phase() {
                            crate::marketplace::grant_store::installation::activation::SourcePhase::Restoring=>break Ok::<_,anyhow::Error>(()),
                            crate::marketplace::grant_store::installation::activation::SourcePhase::Opening=>sleep(Duration::from_millis(1)).await,
                            phase=>bail!("missed actual restoration boundary: {phase:?}"),
                        }
                    }
                }).await??;
                store
                    .set_instance_enabled(owner, false)
                    .await
                    .context("owner disables the instance during source restoration")?;
            }
            let mut inputs = Some((
                launch.take().context("original source")?,
                self.controller.take().context("original controller")?,
            ));
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
            self.operation = Some(Box::new(OwnedCandidate::begin_source(&mut inputs, limits)?));
            assert!(inputs.is_none());
            let operation = self.operation.as_mut().context("operation")?;
            if case.ends_with("probe") {
                tokio::time::timeout(Duration::from_secs(150), async {
                    loop {
                        match operation.view().await?.phase {
                            Phase::ProbingSource => break Ok::<_, anyhow::Error>(()),
                            Phase::PreparingSource => sleep(Duration::from_millis(1)).await,
                            phase => bail!("missed actual probe boundary: {phase:?}"),
                        }
                    }
                })
                .await??;
                store
                    .set_instance_enabled(owner, false)
                    .await
                    .context("owner disables the instance at the source probe boundary")?;
            }
            let ready = match tokio::time::timeout(Duration::from_millis(1), operation.wait()).await
            {
                Ok(result) => result?,
                Err(_) => operation.wait().await?,
            };
            assert_eq!(ready.phase, Phase::AwaitingCleanup);
            assert!(operation.take_controller().is_err());
            assert!(
                Box::pin(operation.activate(&self.principal, Uuid::new_v4()))
                    .await
                    .is_err()
            );
        }
        let record: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(owner.to_string())
        .fetch_one(&self.database.pool)
        .await?;
        let value: serde_json::Value = serde_json::from_str(&record)?;
        assert_eq!(
            value["activation"]["phase"],
            if committed {
                "source_prepared"
            } else {
                "starting"
            }
        );
        let empty:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1").bind(staged.instance().to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(empty, (1, 0, 0));
        let effects:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)").fetch_one(&self.database.pool).await?;
        assert_eq!(
            effects, 0,
            "source failure must not run migration/copy credentials"
        );
        store
            .update_instance_config(owner, Some(&json!({"owner":"keep"})))
            .await?;
        let before:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1").bind(owner.to_string()).fetch_one(&self.database.pool).await?;
        sqlx::query(
            "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
        )
        .bind(self.principal.profile_id.to_string())
        .execute(&self.database.pool)
        .await?;
        let mutation = Uuid::new_v4();
        if let Some(source) = &direct {
            assert!(source.cancel(&self.principal, mutation).await.is_err());
        } else {
            assert!(
                Box::pin(
                    self.operation
                        .as_mut()
                        .context("operation")?
                        .cancel(&self.principal, mutation)
                )
                .await
                .is_err()
            );
        }
        let revision:String=sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1").bind(self.principal.profile_id.to_string()).fetch_one(&self.database.pool).await?;
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
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1").execute(&self.database.pool).await?;
        sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
            .bind((1_i64 << 53) - 2)
            .execute(&self.database.pool)
            .await?;
        if let Some(peer) = peer.take() {
            peer.finish().await?;
        }
        let expected = if committed { 5 } else { 4 };
        if let Some(source) = &direct {
            assert_eq!(
                Box::pin(source.cancel(&fresh, mutation)).await?.revision(),
                expected
            );
            assert_eq!(
                Box::pin(source.cancel(&fresh, mutation)).await?.revision(),
                expected
            );
        } else {
            let operation = self.operation.as_mut().context("operation")?;
            let cancelled = Box::pin(operation.cancel(&fresh, mutation)).await?;
            assert_eq!(cancelled.phase, Phase::Cancelled);
            assert_eq!(cancelled.revision, Some(expected));
            assert_eq!(
                Box::pin(operation.cancel(&fresh, mutation)).await?.revision,
                Some(expected)
            );
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
        }
        let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)").fetch_one(&self.database.pool).await?;
        assert_eq!(remaining, 0);
        let after:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1").bind(owner.to_string()).fetch_one(&self.database.pool).await?;
        assert_eq!(before, after);
        eprintln!(
            "{case}: original native source disposal survived the error/lost retention ack; fresh-owner offline MAX-capacity cleanup kept settings and exact retry"
        );
        Ok(())
    }
}
