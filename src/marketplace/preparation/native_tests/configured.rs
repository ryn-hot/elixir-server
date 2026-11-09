//! Actual probe-to-configured-candidate composition. Peer/protection admission
//! is explicitly synthetic; native work, archives, owner checks and SQL are not.
use super::*;
use crate::{
    http::auth::CurrentPrincipal,
    marketplace::grant_store::{
        Actor,
        installation::{
            InstallTarget, OwnerInstallIntent, StagedInstallation,
            activation::{
                CredentialChange, OwnerActivationIntent, SourceLaunch,
                candidate::CandidateOrRecovery,
                commit::{self, OwnerCommitIntent},
            },
            runtime::{InstalledController, InstalledRuntime},
            standing::InstalledBinding,
        },
    },
    playback::PlaybackJobManager,
    secrets::SecretsManager,
};
use elixir_marketplace::source_execution::{RuntimeJson, SourceExecutionLimits, SourceSdkLimits};
use serde_json::json;

mod choices;
mod pre_candidate;
mod source_failures;
mod unstarted_candidate;

pub(super) const SOURCE: &str = "import{state,getSecret}from'@elixir/sdk/v1';export async function invoke(){return{cursor:await state.get('cursor'),key:await getSecret('source_api')}}export async function migrate(){await state.set('cursor',(await state.get('cursor')??0)+1)}export async function checkReady(){return(await state.get('cursor'))===1&&(await getSecret('source_api'))==='credential-private'}";

pub(super) async fn fresh_postgres(url: &str) -> Result<Database> {
    let parsed = reqwest::Url::parse(url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path().starts_with("/elixir_marketplace_activation_"),
        "dedicated loopback activation fixture database required"
    );
    let database = Database::connect(&DatabaseConfig {
        url: url.into(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    anyhow::ensure!(
        database.driver == crate::db::DatabaseDriver::Postgres,
        "PostgreSQL required"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')")
        .fetch_one(&database.pool).await?;
    anyhow::ensure!(count == 0, "fixture database must be empty; no reset");
    Ok(database)
}

pub(super) struct Configured {
    database: Database,
    principal: CurrentPrincipal,
    secrets: SecretsManager,
    configuration: InstalledRuntime,
    controller: Option<InstalledController>,
    operation:
        Option<Box<crate::marketplace::grant_store::installation::operation::OwnedCandidate>>,
    candidate_started: bool,
}
impl Configured {
    pub(super) async fn open(
        database: Database,
        runtime: Arc<dyn RuntimeManager>,
        lane: &Path,
        archive: &Path,
        image: &str,
        actor: &Actor,
    ) -> Result<Self> {
        let configuration = InstalledRuntime {
            runtime,
            playback: Arc::new(PlaybackJobManager::new(database.pool.clone(), None)),
            local_http: None,
            application_http: None,
            compiled_toolchains: None,
            lane: lane.into(),
            archive: archive.into(),
            deployment: format!("configured-probed-{}", Uuid::new_v4().simple()),
            image: image.into(),
            source_budget: SourceJobBudget {
                memory_bytes: 64 * MIB,
                cpu_millicores: 100,
                disk_bytes: 128 * MIB,
                input_bytes: 64 * MIB,
                output_bytes: 64 * MIB,
                wall_seconds: 120,
            },
            retention: crate::marketplace::archive::ArchiveBudget {
                bytes: 128 * MIB,
                releases: 2,
            },
        };
        let principal = actor.fixture_principal();
        let secrets = SecretsManager::from_key_bytes([57; 32], false);
        // Open before enrollment; never adopt an existing unknown history.
        let controller =
            InstalledController::open(database.clone(), secrets.clone(), configuration.clone())
                .await?;
        Ok(Self {
            database,
            principal,
            secrets,
            configuration,
            controller: Some(controller),
            operation: None,
            candidate_started: false,
        })
    }
    pub(super) fn install(&self, review: &GrantReview) -> Result<OwnerInstallIntent> {
        OwnerInstallIntent::new(
            &self.principal,
            review,
            1,
            0,
            Uuid::new_v4(),
            InstallTarget::New {
                name: "Probed source".into(),
            },
        )
    }
    pub(super) fn intent(
        &self,
        staged: &StagedInstallation,
        review: &GrantReview,
        mutation: Uuid,
    ) -> Result<OwnerActivationIntent> {
        OwnerActivationIntent::new_with_credentials(
            &self.principal,
            staged,
            review,
            1,
            mutation,
            &self.secrets,
            vec![CredentialChange::set(
                "source_api",
                "credential-private",
                false,
            )],
        )
    }
    pub(super) async fn finish(
        &mut self,
        launch: &mut SourceLaunch,
        staged: &StagedInstallation,
        case: &str,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        let source = launch.finish().await?;
        let probed = Box::pin(source.probe()).await?;
        let mut prepared = Box::pin(probed.retain()).await?;
        assert_eq!(prepared.reference().revision(), 4);
        let before: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&self.database.pool)
                .await?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
            .fetch_one(&self.database.pool)
            .await?;
        assert_eq!(
            count, 0,
            "restricted source probe cannot receive credentials or SDK effects"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values")
            .fetch_one(&self.database.pool)
            .await?;
        assert_eq!(count, 0, "source probing cannot run the state migration");
        assert!(!before.contains("credential-private"));
        assert!(ControllerLease::acquire(&self.configuration.lane).is_ok());
        let lost_authority = case == "retain_configured_authority";
        if lost_authority {
            sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1")
                .bind(self.principal.profile_id.to_string()).execute(&self.database.pool).await?;
            assert!(
                prepared
                    .claim_candidate(
                        self.controller
                            .as_mut()
                            .context("configured controller missing")?
                    )
                    .await
                    .is_err()
            );
            let after: String =
                sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                    .fetch_one(&self.database.pool)
                    .await?;
            assert_eq!(
                after, before,
                "a successful probe is not standing owner authorization"
            );
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
                    .fetch_one(&self.database.pool)
                    .await?;
            assert_eq!(count, 0);
            eprintln!(
                "configured probed source: lost owner revision refused continuation without copying credentials"
            );
        }
        if lost_authority || case == "retain_configured_cancel" {
            if lost_authority {
                assert!(
                    prepared
                        .cancel(&self.principal, Uuid::new_v4())
                        .await
                        .is_err(),
                    "stale launch authority cannot authorize cleanup"
                );
            }
            let revision: String = sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1")
                .bind(self.principal.profile_id.to_string()).fetch_one(&self.database.pool).await?;
            // Fixture reauthentication against the NEW revision; never rewind
            // the database or turn the launch context into fresh owner consent.
            let mut caps = crate::authz::CapabilitySet::all();
            caps.remove(crate::authz::Capability::SecretsManage);
            let principal = CurrentPrincipal::fixture_owner(
                self.principal.user_id,
                self.principal.home_id,
                self.principal.profile_id,
                self.principal.account_session_id,
                revision.parse()?,
                self.principal
                    .session_expires_at
                    .min(self.principal.access_expires_at),
                caps,
            );
            let owner = staged.configured_instance().context("configured owner")?;
            sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
                .bind(r#"{"owner":"preserved"}"#)
                .bind(owner.to_string())
                .execute(&self.database.pool)
                .await?;
            let before_owner: (String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
                .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
            sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}'")
                .execute(&self.database.pool)
                .await?;
            if let Some(peer) = peer.take() {
                peer.finish().await?;
            }
            // All normal writers are now exhausted. One non-growing final
            // cleanup is still reserved for this positively disposed probe.
            sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
                .bind((1_i64 << 53) - 2)
                .execute(&self.database.pool)
                .await?;
            let mutation = Uuid::new_v4();
            let cancelled = prepared.cancel(&principal, mutation).await?;
            assert_eq!(cancelled.revision(), prepared.reference().revision() + 1);
            assert_eq!(
                prepared.cancel(&principal, mutation).await?.revision(),
                cancelled.revision()
            );
            assert!(prepared.cancel(&principal, Uuid::new_v4()).await.is_err());
            assert!(
                prepared
                    .claim_candidate(
                        self.controller
                            .as_mut()
                            .context("configured controller missing")?
                    )
                    .await
                    .is_err()
            );
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
                .fetch_one(&self.database.pool)
                .await?;
            assert_eq!(count, 0);
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_state")
                    .fetch_one(&self.database.pool)
                    .await?;
            assert_eq!(count, 0);
            let after_owner: (String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
                .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
            assert_eq!(after_owner, before_owner);
            eprintln!(
                "configured probed source: independently authenticated offline cancellation preserved stable configuration and acknowledged exact retry at reserved capacity"
            );
            return Ok(());
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
        if matches!(
            case,
            "retain_configured_owned" | "retain_configured_owned_cancel"
        ) {
            use crate::marketplace::grant_store::installation::operation::{OwnedCandidate, Phase};
            let mut owners = Some((
                prepared,
                self.controller
                    .take()
                    .context("configured controller missing")?,
            ));
            let mut invalid = limits.clone();
            invalid.timeout_millis = 0;
            assert!(OwnedCandidate::begin(&mut owners, invalid).is_err());
            assert!(owners.is_some(), "preflight cannot consume original owners");
            self.operation = Some(Box::new(OwnedCandidate::begin(
                &mut owners,
                limits.clone(),
            )?));
            assert!(owners.is_none());
            let operation = self.operation.as_mut().context("operation missing")?;
            assert!(
                tokio::time::timeout(std::time::Duration::ZERO, operation.wait())
                    .await
                    .is_err()
            );
            assert!(
                operation.take_controller().is_err(),
                "a lost waiter cannot release ownership"
            );
            let ready = operation.wait().await?;
            assert_eq!(ready.phase, Phase::AwaitingCommit);
            assert!(
                operation.take_controller().is_err(),
                "readiness is not owner consent"
            );
            let mutation = Uuid::new_v4();
            if case == "retain_configured_owned_cancel" {
                let owner = staged.configured_instance().context("configured owner")?;
                let before_revision: i64 = sqlx::query_scalar(
                    "SELECT revision FROM marketplace_installations WHERE instance_id=$1",
                )
                .bind(owner.to_string())
                .fetch_one(&self.database.pool)
                .await?;
                sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
                    .bind(r#"{"owner":"preserved"}"#)
                    .bind(owner.to_string())
                    .execute(&self.database.pool)
                    .await?;
                let before_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
                    .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
                sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1")
                    .bind(self.principal.profile_id.to_string()).execute(&self.database.pool).await?;
                assert!(
                    operation.cancel(&self.principal, mutation).await.is_err(),
                    "stale launch authority cannot stop or cancel the owned result"
                );
                assert_eq!(operation.view().await?.phase, Phase::AwaitingCommit);
                let revision:String=sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1")
                    .bind(self.principal.profile_id.to_string()).fetch_one(&self.database.pool).await?;
                let mut caps = crate::authz::CapabilitySet::all();
                caps.remove(crate::authz::Capability::SecretsManage);
                let fresh = CurrentPrincipal::fixture_owner(
                    self.principal.user_id,
                    self.principal.home_id,
                    self.principal.profile_id,
                    self.principal.account_session_id,
                    revision.parse()?,
                    self.principal
                        .session_expires_at
                        .min(self.principal.access_expires_at),
                    caps,
                );
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
                    .execute(&self.database.pool).await?;
                if let Some(peer) = peer.take() {
                    peer.finish().await?;
                }
                sqlx::query(
                    "UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1",
                )
                .bind((1_i64 << 53) - 2)
                .execute(&self.database.pool)
                .await?;
                let cancelled = operation.cancel(&fresh, mutation).await?;
                assert_eq!(cancelled.phase, Phase::Cancelled);
                assert_eq!(
                    cancelled.revision,
                    Some(before_revision + 1),
                    "one atomic journal write, not discard followed by another cancellation"
                );
                assert_eq!(
                    operation.cancel(&fresh, mutation).await?.revision,
                    cancelled.revision
                );
                assert!(operation.cancel(&fresh, Uuid::new_v4()).await.is_err());
                assert!(
                    operation
                        .activate(&self.principal, Uuid::new_v4())
                        .await
                        .is_err()
                );
                let count:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)").fetch_one(&self.database.pool).await?;
                assert_eq!(
                    count, 0,
                    "cancel must remove only the unused private namespace"
                );
                let after_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
                    .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
                assert_eq!(after_owner, before_owner);
                let ledger: i64 = sqlx::query_scalar(
                    "SELECT revision FROM marketplace_installation_budget WHERE singleton=1",
                )
                .fetch_one(&self.database.pool)
                .await?;
                assert_eq!(
                    ledger,
                    (1_i64 << 53) - 1,
                    "exact retry must not spend another final revision"
                );
                self.controller = Some(operation.take_controller()?);
                self.operation = None;
                eprintln!(
                    "owned configured operation: real native migration/retirement, fresh-owner offline atomic cancellation at reserved final capacity, stable settings and exact retry passed after lost waiter"
                );
                return Ok(());
            }
            let activated = operation.activate(&self.principal, mutation).await?;
            assert_eq!(activated.phase, Phase::Activated);
            assert_eq!(
                operation
                    .activate(&self.principal, mutation)
                    .await?
                    .revision,
                activated.revision
            );
            assert!(
                operation
                    .cancel(&self.principal, Uuid::new_v4())
                    .await
                    .is_err()
            );
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
            let binding = InstalledBinding::load_configured(
                &self.database,
                self.principal.home_id,
                staged.extension(),
                staged
                    .configured_instance()
                    .context("configured owner missing")?,
                crate::marketplace::grant_store::installation::InstallBudget {
                    entries: 2,
                    bytes: 4 * MIB,
                },
            )
            .await?;
            self.controller
                .as_mut()
                .context("configured controller missing")?
                .begin(binding, limits, RuntimeJson::from_value(json!({}))?)?;
            let result = self
                .controller
                .as_mut()
                .context("configured controller missing")?
                .finish()
                .await?;
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(result.as_str())?,
                json!({"cursor":1,"key":"credential-private"})
            );
            eprintln!(
                "owned configured operation: real native migration/retirement, explicit activation and installed invocation passed after lost waiter"
            );
            return Ok(());
        }
        let claim = match prepared
            .claim_candidate(
                self.controller
                    .as_mut()
                    .context("configured controller missing")?,
            )
            .await?
        {
            CandidateOrRecovery::Claim(claim) => claim,
            CandidateOrRecovery::Recovery(_) => bail!("fresh original candidate required"),
        };
        let before: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&self.database.pool)
                .await?;
        assert!(
            prepared
                .claim_candidate(
                    self.controller
                        .as_mut()
                        .context("configured controller missing")?
                )
                .await
                .is_err(),
            "the handoff cannot issue a second candidate claim"
        );
        let after: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
            .fetch_one(&self.database.pool)
            .await?;
        assert_eq!(after, before);
        assert!(
            prepared
                .cancel(&self.principal, Uuid::new_v4())
                .await
                .is_err(),
            "post-probe cancellation cannot erase an issued candidate claim"
        );
        let mut slot = Some(claim);
        self.controller
            .as_mut()
            .context("configured controller missing")?
            .begin_candidate(&mut slot, limits.clone())?;
        self.candidate_started = true;
        assert!(slot.is_none());
        let retired = self
            .controller
            .as_mut()
            .context("configured controller missing")?
            .finish_candidate()
            .await?;
        self.candidate_started = false;
        retired
            .migration_result()
            .map_err(|error| anyhow::anyhow!("actual probed native readiness: {error:#}"))?;
        let committed = commit::activate(
            &retired,
            OwnerCommitIntent::new(&self.principal, &retired, Uuid::new_v4())?,
        )
        .await?;
        assert_eq!(committed.namespace, staged.instance());
        assert_eq!(
            Some(committed.configured_instance),
            staged.configured_instance()
        );
        let binding = InstalledBinding::load_configured(
            &self.database,
            self.principal.home_id,
            staged.extension(),
            committed.configured_instance,
            crate::marketplace::grant_store::installation::InstallBudget {
                entries: 2,
                bytes: 4 * MIB,
            },
        )
        .await?;
        self.controller
            .as_mut()
            .context("configured controller missing")?
            .begin(binding, limits, RuntimeJson::from_value(json!({}))?)?;
        let result = self
            .controller
            .as_mut()
            .context("configured controller missing")?
            .finish()
            .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(result.as_str())?,
            json!({"cursor":1,"key":"credential-private"})
        );
        eprintln!(
            "configured probed source: real restore/probe/disposal continued through credential copy, native readiness, commit and installed invocation"
        );
        Ok(())
    }
    pub(super) async fn finish_owned_activation(
        &mut self,
        request: crate::marketplace::grant_store::installation::activation::ActivationRequest,
        case: &str,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        use crate::marketplace::grant_store::installation::operation::{OwnedCandidate, Phase};
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
        let owner = request
            .staged()
            .configured_instance()
            .context("configured owner")?;
        let namespace = request.staged().instance();
        let extension = request.staged().extension().clone();
        // This successful request already captured its original staging
        // choices. A config write here would be a newer user choice and must
        // supersede it; dedicated choice-race cases exercise that behavior.
        let before_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
            .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
        let before: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(owner.to_string())
        .fetch_one(&self.database.pool)
        .await?;
        assert!(serde_json::from_str::<serde_json::Value>(&before)?["activation"].is_null());
        let sql_stop = case == "retain_configured_preclaim_sql_stop";
        let stopping = sql_stop || case == "retain_configured_preclaim_stop";
        let mut blocker = if sql_stop {
            let mut tx = self.database.pool.begin().await?;
            sqlx::query(
                "UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1",
            )
            .execute(&mut *tx)
            .await?;
            Some(tx)
        } else {
            None
        };
        let mut owners = Some((
            request,
            self.controller
                .take()
                .context("configured controller missing")?,
        ));
        let mut invalid = limits.clone();
        invalid.timeout_millis = 0;
        assert!(OwnedCandidate::begin_activation(&mut owners, invalid).is_err());
        assert!(
            owners.is_some(),
            "pure preflight must retain original request and controller"
        );
        self.operation = Some(Box::new(OwnedCandidate::begin_activation(
            &mut owners,
            limits.clone(),
        )?));
        assert!(owners.is_none());
        let operation = self
            .operation
            .as_mut()
            .context("activation operation missing")?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), operation.wait())
                .await
                .is_err()
        );
        assert!(operation.take_controller().is_err());
        if stopping {
            tokio::time::timeout(std::time::Duration::from_secs(150), async {
                loop {
                    let phase = operation.view().await?.phase;
                    if phase
                        == if sql_stop {
                            Phase::PreparingActivation
                        } else {
                            Phase::ClaimingSource
                        }
                    {
                        break Ok::<_, anyhow::Error>(());
                    }
                    match phase {
                        Phase::QueuedActivation | Phase::PreparingActivation => {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await
                        }
                        _ => bail!("missed original activation SQL boundary: {phase:?}"),
                    }
                }
            })
            .await
            .context("activation did not reach its owned SQL boundary")??;
            operation.request_stop();
            if let Some(blocker) = blocker.take() {
                blocker.rollback().await?;
            }
        }
        let ready = operation.wait().await?;
        assert_eq!(
            ready.phase,
            if stopping {
                Phase::AwaitingCleanup
            } else {
                Phase::AwaitingCommit
            }
        );
        assert!(operation.take_controller().is_err());
        let mutation = Uuid::new_v4();
        if stopping {
            let record: String = sqlx::query_scalar(
                "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
            )
            .bind(owner.to_string())
            .fetch_one(&self.database.pool)
            .await?;
            let journal: serde_json::Value = serde_json::from_str(&record)?;
            assert_eq!(
                journal["activation"]["phase"],
                if sql_stop { "prepared" } else { "starting" }
            );
            let head:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
                .bind(namespace.to_string()).fetch_one(&self.database.pool).await?;
            assert_eq!(head, (1, 0, 0));
            let effects:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
                .fetch_one(&self.database.pool).await?;
            assert_eq!(effects, 0);
            sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1")
                .bind(self.principal.profile_id.to_string()).execute(&self.database.pool).await?;
            assert!(
                Box::pin(operation.cancel(&self.principal, mutation))
                    .await
                    .is_err()
            );
            let revision:String=sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1")
                .bind(self.principal.profile_id.to_string()).fetch_one(&self.database.pool).await?;
            let mut caps = crate::authz::CapabilitySet::all();
            caps.remove(crate::authz::Capability::SecretsManage);
            let fresh = CurrentPrincipal::fixture_owner(
                self.principal.user_id,
                self.principal.home_id,
                self.principal.profile_id,
                self.principal.account_session_id,
                revision.parse()?,
                self.principal
                    .session_expires_at
                    .min(self.principal.access_expires_at),
                caps,
            );
            sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
                .execute(&self.database.pool).await?;
            if let Some(peer) = peer.take() {
                peer.finish().await?;
            }
            if !sql_stop {
                // The sealed original Starting claim gets one reserved final write.
                sqlx::query(
                    "UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1",
                )
                .bind((1_i64 << 53) - 2)
                .execute(&self.database.pool)
                .await?;
            }
            let cancelled = Box::pin(operation.cancel(&fresh, mutation)).await?;
            assert_eq!(cancelled.phase, Phase::Cancelled);
            assert_eq!(cancelled.revision, Some(if sql_stop { 3 } else { 4 }));
            assert_eq!(
                Box::pin(operation.cancel(&fresh, mutation)).await?.revision,
                cancelled.revision
            );
            let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
                .fetch_one(&self.database.pool).await?;
            assert_eq!(remaining, 0);
            let after_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
                .bind(owner.to_string()).fetch_one(&self.database.pool).await?;
            assert_eq!(after_owner, before_owner);
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
            eprintln!(
                "owned activation SQL boundary: lost waiter and {case} retained exact original SQL objects, no native source launch, fresh-owner offline cancellation passed"
            );
            return Ok(());
        }
        if matches!(
            case,
            "retain_configured_choice_instance" | "retain_configured_choice_extension"
        ) {
            Box::pin(choices::reject_superseded(
                &self.database,
                &self.principal,
                operation,
                (&extension, owner, namespace),
                mutation,
                case,
            ))
            .await?;
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
            return Ok(());
        }
        let activated = Box::pin(operation.activate(&self.principal, mutation)).await?;
        assert_eq!(activated.phase, Phase::Activated);
        if case == "retain_configured_choice_retry" {
            Box::pin(choices::retry_after_disable(
                &self.database,
                &self.principal,
                operation,
                &extension,
                owner,
                mutation,
                activated.revision,
            ))
            .await?;
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
            return Ok(());
        }
        self.controller = Some(operation.take_controller()?);
        self.operation = None;
        // Installed execution must not borrow the marketplace network owner.
        if let Some(peer) = peer.take() {
            peer.finish().await?;
        }
        let binding = Box::pin(InstalledBinding::load_configured(
            &self.database,
            self.principal.home_id,
            &extension,
            owner,
            crate::marketplace::grant_store::installation::InstallBudget {
                entries: 2,
                bytes: 4 * MIB,
            },
        ))
        .await?;
        self.controller
            .as_mut()
            .context("configured controller missing")?
            .begin(binding, limits, RuntimeJson::from_value(json!({}))?)?;
        let result = self
            .controller
            .as_mut()
            .context("configured controller missing")?
            .finish()
            .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(result.as_str())?,
            json!({"cursor":1,"key":"credential-private"})
        );
        eprintln!(
            "owned activation: retained before prepare/claim SQL through actual probe/candidate/activation; installed invocation passed after marketplace peer stopped"
        );
        Ok(())
    }
    pub(super) async fn finish_owned_source(
        &mut self,
        launch: &mut Option<SourceLaunch>,
        staged: &StagedInstallation,
        case: &str,
        peer: &mut Option<PeerPipeFixture>,
    ) -> Result<()> {
        use crate::marketplace::grant_store::installation::operation::{OwnedCandidate, Phase};
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
        // Staging deliberately allocated one EMPTY namespace/root pair before
        // SourceLaunch. Probing must preserve those exact rows, not delete them
        // or mistake their existence for candidate execution.
        let namespaces_before: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT namespace_id,instance_id,extension_id,home_id,purpose FROM marketplace_sdk_namespaces ORDER BY namespace_id",
        ).fetch_all(&self.database.pool).await?;
        let roots_before: Vec<(String, String, String, i64, i64, i64, String)> = sqlx::query_as(
            "SELECT instance_id,extension_id,home_id,revision,key_count,encoded_bytes,purpose FROM marketplace_instance_sdk_state ORDER BY instance_id",
        ).fetch_all(&self.database.pool).await?;
        assert_eq!(namespaces_before.len(), 1);
        assert_eq!(roots_before.len(), 1);
        assert_eq!(namespaces_before[0].0, staged.instance().to_string());
        assert_eq!(
            (roots_before[0].3, roots_before[0].4, roots_before[0].5),
            (1, 0, 0)
        );
        let mut owners = Some((
            launch.take().context("original source launch missing")?,
            self.controller
                .take()
                .context("configured controller missing")?,
        ));
        let mut invalid = limits.clone();
        invalid.timeout_millis = 0;
        assert!(OwnedCandidate::begin_source(&mut owners, invalid).is_err());
        assert!(
            owners.is_some(),
            "preflight must preserve the actual source owner"
        );
        self.operation = Some(Box::new(OwnedCandidate::begin_source(
            &mut owners,
            limits.clone(),
        )?));
        assert!(owners.is_none());
        let operation = self.operation.as_mut().context("operation missing")?;
        assert!(
            tokio::time::timeout(std::time::Duration::ZERO, operation.wait())
                .await
                .is_err()
        );
        assert!(operation.take_controller().is_err());
        let stopping = case == "retain_configured_preprobe_stop";
        if stopping {
            tokio::time::timeout(std::time::Duration::from_secs(150), async {
                loop {
                    let view = operation.view().await?;
                    match view.phase {
                        Phase::ProbingSource => break Ok::<_, anyhow::Error>(()),
                        Phase::PreparingSource => {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await
                        }
                        phase => bail!("missed actual source probe: {phase:?}"),
                    }
                }
            })
            .await
            .context("original source did not reach probe")??;
            operation.request_stop();
            assert_eq!(operation.view().await?.phase, Phase::Stopping);
        }
        let ready = operation.wait().await?;
        assert_eq!(
            ready.phase,
            if stopping {
                Phase::AwaitingCleanup
            } else {
                Phase::AwaitingCommit
            }
        );
        assert!(
            operation.take_controller().is_err(),
            "neither probing nor readiness grants owner consent"
        );
        let mutation = Uuid::new_v4();
        if stopping {
            let record: String =
                sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                    .fetch_one(&self.database.pool)
                    .await?;
            let journal: serde_json::Value = serde_json::from_str(&record)?;
            assert_eq!(journal["activation"]["phase"], "source_prepared");
            let namespaces_after: Vec<(String, String, String, String, String)> = sqlx::query_as(
                "SELECT namespace_id,instance_id,extension_id,home_id,purpose FROM marketplace_sdk_namespaces ORDER BY namespace_id",
            ).fetch_all(&self.database.pool).await?;
            let roots_after: Vec<(String, String, String, i64, i64, i64, String)> = sqlx::query_as(
                "SELECT instance_id,extension_id,home_id,revision,key_count,encoded_bytes,purpose FROM marketplace_instance_sdk_state ORDER BY instance_id",
            ).fetch_all(&self.database.pool).await?;
            assert_eq!(namespaces_after, namespaces_before);
            assert_eq!(roots_after, roots_before);
            let count:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
                .fetch_one(&self.database.pool).await?;
            assert_eq!(
                count, 0,
                "stop during probe must not copy secrets/state or claim a candidate"
            );
            if let Some(peer) = peer.take() {
                peer.finish().await?;
            }
            assert!(operation.activate(&self.principal, mutation).await.is_err());
            let cancelled = operation.cancel(&self.principal, mutation).await?;
            assert_eq!(cancelled.phase, Phase::Cancelled);
            assert_eq!(
                operation.cancel(&self.principal, mutation).await?.revision,
                cancelled.revision
            );
            let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
                .fetch_one(&self.database.pool).await?;
            assert_eq!(
                remaining, 0,
                "only authenticated cancellation removes the original unused staged namespace/root"
            );
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
            eprintln!(
                "owned pre-probe operation: lost waiter and stop during real native probe retained original disposal/SQL result; offline cancellation issued no candidate or SDK effects"
            );
            return Ok(());
        }
        assert_eq!(
            operation.activate(&self.principal, mutation).await?.phase,
            Phase::Activated
        );
        self.controller = Some(operation.take_controller()?);
        self.operation = None;
        let binding = InstalledBinding::load_configured(
            &self.database,
            self.principal.home_id,
            staged.extension(),
            staged
                .configured_instance()
                .context("configured owner missing")?,
            crate::marketplace::grant_store::installation::InstallBudget {
                entries: 2,
                bytes: 4 * MIB,
            },
        )
        .await?;
        self.controller
            .as_mut()
            .context("configured controller missing")?
            .begin(binding, limits, RuntimeJson::from_value(json!({}))?)?;
        let result = self
            .controller
            .as_mut()
            .context("configured controller missing")?
            .finish()
            .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(result.as_str())?,
            json!({"cursor":1,"key":"credential-private"})
        );
        eprintln!(
            "owned pre-probe operation: original source/probe/disposal/retention/candidate result survived lost waiter, explicit activation and installed invocation passed"
        );
        Ok(())
    }
    pub(super) async fn cancel(&mut self) -> Result<()> {
        if let Some(operation) = self.operation.as_mut() {
            operation.shutdown().await?;
        }
        self.operation = None;
        if let Some(controller) = self.controller.as_mut() {
            controller.cancel().await?;
            if self.candidate_started {
                let _retired = controller.finish_candidate().await?;
                self.candidate_started = false;
            }
        }
        Ok(())
    }
}

#[test]
fn configured_native_continuations_keep_bounded_stack_ownership() {
    fn activation_frame<F: std::future::Future>(
        _: impl FnOnce(
            &'static mut Configured,
            crate::marketplace::grant_store::installation::activation::ActivationRequest,
            &'static mut Option<PeerPipeFixture>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    fn parent_frame<F: std::future::Future>(
        _: impl FnOnce(Option<&'static str>, Option<&'static str>) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    fn frame<S: 'static, F: std::future::Future>(
        _: impl FnOnce(
            &'static mut Configured,
            &'static mut S,
            &'static StagedInstallation,
            &'static mut Option<PeerPipeFixture>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    // Type measurements only: no fabricated source/controller/probe objects.
    let postprobe = frame(|configured, source, staged, peer| {
        configured.finish(source, staged, "retain_configured_owned", peer)
    });
    let preprobe = frame(|configured, source, staged, peer| {
        configured.finish_owned_source(source, staged, "retain_configured_preprobe_owned", peer)
    });
    let source_failure = frame(|configured, source, staged, peer| {
        configured.source_failure(
            source,
            staged,
            "retain_configured_source_failure_probe",
            peer,
        )
    });
    let parent = parent_frame(live_preparation_cases);
    let unstarted =
        frame(|configured, source, staged, peer| configured.finish_unstarted(source, staged, peer));
    let pre_candidate = frame(|configured, source, staged, peer| {
        configured.finish_pre_candidate_failure(
            source,
            staged,
            "retain_configured_preparation_failure_copy",
            peer,
        )
    });
    let activation = activation_frame(|configured, request, peer| {
        configured.finish_owned_activation(request, "retain_configured_preclaim_owned", peer)
    });
    assert!(
        activation <= 128 * 1024,
        "native owned activation frame: {activation}"
    );
    assert!(parent <= 128 * 1024, "native parent frame: {parent}");
    assert!(
        pre_candidate <= 128 * 1024,
        "native pre-claim failure frame: {pre_candidate}"
    );
    assert!(
        unstarted <= 128 * 1024,
        "native unstarted candidate frame: {unstarted}"
    );
    assert!(
        postprobe <= 128 * 1024,
        "native post-probe frame: {postprobe}"
    );
    assert!(preprobe <= 128 * 1024, "native pre-probe frame: {preprobe}");
    assert!(
        source_failure <= 128 * 1024,
        "native source failure frame: {source_failure}"
    );
}
