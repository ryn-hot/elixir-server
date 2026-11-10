//! Local owner management DATA/facade. No request accepts a path, image,
//! allocation, Actor, CapturedTarget, source receipt or native capability.
use super::*;
use crate::marketplace::{
    application,
    grant_store::installation::{self, ExpectedControls, InstallAction, InstallTarget},
};
use crate::{db::Database, http::auth::CurrentPrincipal, secrets::SecretsManager};
use elixir_marketplace::{
    canonical,
    identity::{Digest, ExtensionId},
};
use serde::{Deserialize, Deserializer, de::IntoDeserializer};
use uuid::Uuid;
use zeroize::Zeroizing;

pub(crate) use installation::sharing::publisher::{
    Request as PublisherSeedingRequest, View as PublisherSeedingView,
};
pub(crate) use installation::targets::{TargetAuthorization, TargetRequest, TargetSnapshot};
pub(crate) const MAX_PUBLISHER_SEEDING_BYTES: usize =
    installation::sharing::publisher::MAX_REQUEST_BYTES;
pub(crate) const MAX_INSTALL_BYTES: usize = 4096;
pub(crate) const MAX_DIRECT_INSTALL_BYTES: usize =
    elixir_marketplace::publication::MAX_PUBLICATION_BYTES + MAX_INSTALL_BYTES;
pub(crate) const MAX_ACTION_BYTES: usize = elixir_marketplace::source_execution::MAX_ARGUMENT_BYTES;
const MAX_REVISION: i64 = (1i64 << 53) - 1;

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], max: usize) -> Result<T> {
    let value = canonical::parse(bytes, max)?;
    let mut ignored = false;
    let decoded = serde_ignored::deserialize(value.into_deserializer(), |_| ignored = true)?;
    if ignored {
        bail!("unknown installation request field");
    }
    Ok(decoded)
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TargetInput {
    New { name: String },
    Existing { instance_id: Uuid },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallRequest {
    schema_version: u32,
    query_id: Uuid,
    extension_id: ExtensionId,
    release_id: Digest,
    target: TargetInput,
    expected_revision: i64,
    mutation_id: Uuid,
    controls: ExpectedControls,
}
impl InstallRequest {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes, MAX_INSTALL_BYTES)?;
        let existing = match &value.target {
            TargetInput::New { name } => {
                if name.is_empty()
                    || name.len() > 128
                    || name.chars().any(char::is_control)
                    || value.expected_revision != 0
                {
                    bail!("invalid new installation target");
                }
                None
            }
            TargetInput::Existing { instance_id } => Some(*instance_id),
        };
        if value.schema_version != 1
            || value.query_id.is_nil()
            || value.mutation_id.is_nil()
            || !(0..MAX_REVISION).contains(&value.expected_revision)
        {
            bail!("invalid original installation request");
        }
        value.controls.validate(existing)?;
        if existing.is_some() && value.expected_revision == 0 {
            bail!("existing target requires its original journal revision");
        }
        Ok(value)
    }
    fn into_action(self) -> (Uuid, InstallAction) {
        (
            self.query_id,
            InstallAction {
                extension: self.extension_id,
                release: self.release_id,
                target: match self.target {
                    TargetInput::New { name } => InstallTarget::New { name },
                    TargetInput::Existing { instance_id } => InstallTarget::Existing {
                        instance: instance_id,
                    },
                },
                expected_revision: self.expected_revision,
                mutation: self.mutation_id,
                controls: self.controls,
            },
        )
    }
}
/// The warning acknowledgement binds the complete signed publication, not a
/// name, URL, preview boolean, peer score or owner-supplied "verified" field.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectInstallRequest {
    schema_version: u32,
    request_id: Uuid,
    publication: elixir_marketplace::publication::PreparedPublication,
    accepted_reference: Digest,
    accept_unqualified: bool,
    #[serde(default)]
    upload_source: bool,
    target: TargetInput,
    expected_revision: i64,
    mutation_id: Uuid,
    controls: ExpectedControls,
}
impl DirectInstallRequest {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes, MAX_DIRECT_INSTALL_BYTES)?;
        let publication = value.publication.to_bytes()?;
        if value.schema_version != 1
            || !value.accept_unqualified
            || Digest::of(&publication) != value.accepted_reference
        {
            bail!("explicit acceptance of the exact direct reference is required");
        }
        // Apply exactly the ordinary target/revision/controls validation. No
        // fake search card or paid-adoption proof is constructed.
        InstallRequest::parse(&canonical::to_vec(&serde_json::json!({
            "schema_version":1,"query_id":value.request_id,
            "extension_id":value.publication.release.payload.extension_id,
            "release_id":value.publication.release_id()?,
            "target":match &value.target {
                TargetInput::New {name} => serde_json::json!({"kind":"new","name":name}),
                TargetInput::Existing {instance_id} => serde_json::json!({"kind":"existing","instance_id":instance_id}),
            },
            "expected_revision":value.expected_revision,"mutation_id":value.mutation_id,"controls":value.controls,
        }))?)?;
        Ok(value)
    }
    fn into_parts(
        self,
    ) -> Result<(
        Uuid,
        InstallAction,
        elixir_marketplace::publication::PreparedPublication,
        Digest,
        bool,
    )> {
        let action = InstallAction {
            extension: self.publication.release.payload.extension_id.clone(),
            release: self.publication.release_id()?,
            target: match self.target {
                TargetInput::New { name } => InstallTarget::New { name },
                TargetInput::Existing { instance_id } => InstallTarget::Existing {
                    instance: instance_id,
                },
            },
            expected_revision: self.expected_revision,
            mutation: self.mutation_id,
            controls: self.controls,
        };
        Ok((
            self.request_id,
            action,
            self.publication,
            self.accepted_reference,
            self.upload_source,
        ))
    }
}
fn secret<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Zeroizing<String>, D::Error> {
    String::deserialize(decoder).map(Zeroizing::new)
}
// No Debug/Serialize; request diagnostics cannot print input credential values.
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum CredentialInput {
    Set {
        key: String,
        #[serde(deserialize_with = "secret")]
        value: Zeroizing<String>,
        rotatable: bool,
    },
    Remove {
        key: String,
    },
}
fn credentials<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Vec<installation::activation::CredentialChange>, D::Error> {
    let values = Vec::<CredentialInput>::deserialize(decoder)?;
    let changes: Vec<_> = values
        .into_iter()
        .map(|input| match input {
            CredentialInput::Set {
                key,
                value,
                rotatable,
            } => installation::activation::CredentialChange::Set {
                key,
                value,
                rotatable,
            },
            CredentialInput::Remove { key } => {
                installation::activation::CredentialChange::Remove { key }
            }
        })
        .collect();
    installation::activation::CredentialChange::validate_input(&changes)
        .map_err(|_| serde::de::Error::custom("invalid bounded credential input"))?;
    Ok(changes)
}
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum ActionInput {
    Approve {
        review_id: Digest,
        expected_revision: i64,
        mutation_id: Uuid,
        automatic_updates: bool,
    },
    RetryStage {
        mutation_id: Uuid,
    },
    Setup {
        mutation_id: Uuid,
        #[serde(deserialize_with = "credentials")]
        credentials: Vec<installation::activation::CredentialChange>,
    },
    Activate {
        mutation_id: Uuid,
    },
    Cancel {
        mutation_id: Uuid,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActionRequest {
    schema_version: u32,
    request: ActionInput,
}
impl ActionRequest {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes, MAX_ACTION_BYTES)?;
        if value.schema_version != 1 {
            bail!("unsupported installation action schema");
        }
        let mutation = match &value.request {
            ActionInput::Approve {
                expected_revision,
                mutation_id,
                ..
            } => {
                if !(0..MAX_REVISION).contains(expected_revision) {
                    bail!("invalid approval revision");
                }
                *mutation_id
            }
            ActionInput::RetryStage { mutation_id }
            | ActionInput::Setup { mutation_id, .. }
            | ActionInput::Activate { mutation_id }
            | ActionInput::Cancel { mutation_id } => *mutation_id,
        };
        if mutation.is_nil() {
            bail!("installation action mutation must be nonzero");
        }
        Ok(value)
    }
    fn into_action(self, principal: CurrentPrincipal) -> Result<application::Action> {
        let action = match self.request {
            ActionInput::Approve {
                review_id,
                expected_revision,
                mutation_id,
                automatic_updates,
            } => {
                if !(0..MAX_REVISION).contains(&expected_revision) {
                    bail!("invalid approval revision");
                }
                application::Action::Approve {
                    principal,
                    selection: crate::marketplace::grant_store::ApprovalSelection {
                        review_id,
                        expected_revision,
                        mutation_id,
                        automatic_updates,
                    },
                }
            }
            ActionInput::RetryStage { mutation_id } => application::Action::RetryStage {
                principal,
                mutation: mutation_id,
            },
            ActionInput::Setup {
                mutation_id,
                credentials,
            } => application::Action::Setup {
                principal,
                mutation: mutation_id,
                credentials,
            },
            ActionInput::Activate { mutation_id } => application::Action::Activate {
                principal,
                mutation: mutation_id,
            },
            ActionInput::Cancel { mutation_id } => application::Action::Cancel {
                principal,
                mutation: mutation_id,
            },
        };
        action.validate()?;
        Ok(action)
    }
}
#[derive(Serialize)]
pub(crate) struct Started {
    schema_version: u32,
    pub operation_id: Uuid,
}
#[derive(Serialize)]
pub(crate) struct InstallationView {
    schema_version: u32,
    review_id: Option<Digest>,
    approval_revision: Option<i64>,
    #[serde(flatten)]
    command: application::View,
}
#[derive(Serialize)]
pub(crate) struct CurrentInstallation {
    schema_version: u32,
    operation: Option<InstallationView>,
}
#[derive(Serialize)]
pub(crate) struct Acknowledgement {
    schema_version: u32,
    operation_id: Uuid,
    receipt: application::ActionReceipt,
}
/// Original HTTP input continuation only; cannot issue grants or new actions.
pub(crate) struct UploadAuthorization {
    source: crate::marketplace::grant_store::source_authorization::SourceAuthorization,
    principal: CurrentPrincipal,
    operation: Uuid,
}
impl MarketplaceService {
    pub(crate) async fn check_source_upload(
        &self,
        database: &Database,
        authorization: &UploadAuthorization,
    ) -> Result<()> {
        authorization.source.current(database).await?;
        let view = self
            .installation
            .view_for(&authorization.principal, authorization.operation)?;
        if view.initial_request.upload_source != Some(true) {
            bail!("original source upload changed");
        }
        Ok(())
    }
    pub(crate) async fn require_source_upload_owner(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        id: Uuid,
    ) -> Result<()> {
        crate::marketplace::preferences::load(database, principal, chrono::Utc::now()).await?;
        let view = self.installation.view_for(principal, id)?;
        if view.initial_request.upload_source != Some(true) {
            bail!("this original operation does not accept SDK input");
        }
        Ok(())
    }
    pub(crate) async fn claim_source_upload(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        operation: Uuid,
        upload: Uuid,
        size: u64,
    ) -> Result<(super::super::preparation::UploadClaim, UploadAuthorization)> {
        self.require_source_upload_owner(database, principal, operation)
            .await?;
        let source =
            crate::marketplace::grant_store::source_authorization::SourceAuthorization::capture(
                database, principal,
            )
            .await?;
        let view = self.installation.view_for(principal, operation)?;
        if view.phase != application::Phase::Preparing {
            bail!("source upload is not currently waiting");
        }
        let preparation = view
            .preparation
            .context("original upload preparation is not ready")?
            .preparation_id;
        let claim = self.preparation.claim_upload(preparation, upload, size)?;
        Ok((
            claim,
            UploadAuthorization {
                source,
                principal: principal.clone(),
                operation,
            },
        ))
    }
    pub(crate) async fn set_publisher_seeding(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        request: installation::sharing::publisher::Request,
    ) -> Result<installation::sharing::publisher::View> {
        let budget = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installation allocation unavailable"))?
            .as_ref()
            .context("publisher seeding not admitted by trusted startup")?
            .allocation
            .installations;
        let observation = if let Some(publication) = request.publication() {
            if self.stop_requested() {
                bail!("marketplace is stopping");
            }
            Some(self.client()?.verify_publication(publication).await?)
        } else {
            None
        };
        installation::sharing::publisher::change(
            database,
            principal,
            request,
            observation.as_ref(),
            budget,
        )
        .await
    }
    pub(crate) async fn publisher_seeding_view(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        extension: &ExtensionId,
        instance: Uuid,
    ) -> Result<installation::sharing::publisher::View> {
        installation::sharing::publisher::view(database, principal, extension, instance).await
    }
    pub(crate) fn request_direct_install(
        &self,
        principal: &CurrentPrincipal,
        request: DirectInstallRequest,
        secrets: SecretsManager,
    ) -> Result<Started> {
        let (query, action, publication, reference, upload) = request.into_parts()?;
        if let Some(operation_id) = self
            .installation
            .acknowledge_source_start(principal, query, &action, reference, upload)?
        {
            return Ok(Started {
                schema_version: 1,
                operation_id,
            });
        }
        if self.stop_requested() {
            bail!("marketplace is stopping");
        }
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installation allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("installation management not admitted by trusted local startup")?;
        let context = InstallContext {
            inner: self.inner.clone(),
            search: self.search.clone(),
            preparation: self.preparation.clone(),
            client: self.client()?,
            direct: None,
            #[cfg(test)]
            fixture: None,
        }
        .with_direct(principal.clone(), publication, reference, query);
        let context = if upload {
            context.with_upload()?
        } else {
            context
        };
        let host = self.installation_host(&context.client, secrets, allocation)?;
        let operation_id = self.installation.start_direct(
            context,
            host,
            principal.clone(),
            query,
            action,
            reference,
        )?;
        if self.stop_requested() {
            self.installation.request_stop();
            bail!("marketplace is stopping; direct install retained for cleanup");
        }
        Ok(Started {
            schema_version: 1,
            operation_id,
        })
    }
    /// Trusted local policy only, never callable through management requests.
    /// A profile is configuration, not protected/source/native authority.
    pub(in crate::marketplace) fn configure_installation(
        &self,
        allocation: application::Allocation,
    ) -> Result<()> {
        self.configure_installation_runtime(allocation, None)
    }
    pub(in crate::marketplace) fn configure_installation_runtime(
        &self,
        allocation: application::Allocation,
        runtime: Option<installation::runtime::InstalledRuntime>,
    ) -> Result<()> {
        allocation.validate()?;
        if let Some(runtime) = &runtime {
            if runtime.source_budget != allocation.source
                || (runtime.retention.bytes, runtime.retention.releases)
                    != (allocation.retention.bytes, allocation.retention.releases)
                || !crate::marketplace::runtime::image_is_pinned(&runtime.image)
            {
                bail!("installation runtime differs from its trusted allocation");
            }
        }
        let mut slot = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installation allocation unavailable"))?;
        if slot.is_some() {
            bail!("installation allocation already configured");
        }
        *slot = Some(super::InstallationConfiguration {
            allocation,
            runtime,
        });
        Ok(())
    }
    pub(crate) fn installation_review_available(&self) -> bool {
        !self.stop_requested()
            && self
                .installation_allocation
                .lock()
                .is_ok_and(|slot| slot.is_some())
            && self
                .client()
                .is_ok_and(|client| client.catalog_preparation_ready().unwrap_or(false))
    }
    pub(crate) async fn installation_targets(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        request: TargetRequest,
    ) -> Result<TargetSnapshot> {
        let _permit = self
            .target_reads
            .clone()
            .try_acquire_owned()
            .context("target choice view busy")?;
        installation::targets::snapshot(database, principal, request).await
    }
    pub(crate) fn request_install(
        &self,
        principal: &CurrentPrincipal,
        request: InstallRequest,
        secrets: SecretsManager,
    ) -> Result<Started> {
        let (query, action) = request.into_action();
        if let Some(operation_id) = self
            .installation
            .acknowledge_start(principal, query, &action)?
        {
            return Ok(Started {
                schema_version: 1,
                operation_id,
            });
        }
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installation allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("installation management not admitted by trusted local startup")?;
        #[cfg(test)]
        if let Some(context) = self
            .installation_context
            .lock()
            .map_err(|_| anyhow::anyhow!("native fixture context unavailable"))?
            .take()
        {
            let host = self.installation_host(&context.client, secrets, allocation)?;
            return self
                .installation
                .start(context, host, principal.clone(), query, action)
                .map(|operation_id| Started {
                    schema_version: 1,
                    operation_id,
                });
        }
        self.start_install(principal, query, action, secrets, allocation)
            .map(|operation_id| Started {
                schema_version: 1,
                operation_id,
            })
    }
    pub(crate) async fn current_installation(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
    ) -> Result<CurrentInstallation> {
        let operation = match self.installation.current_for(principal)? {
            Some(id) => self
                .installation_status(database, principal, id)
                .await
                .map(Some)?,
            None => None,
        };
        Ok(CurrentInstallation {
            schema_version: 1,
            operation,
        })
    }
    pub(crate) async fn installation_status(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        id: Uuid,
    ) -> Result<InstallationView> {
        let command = self.installation.view_for(principal, id)?;
        let review_id = command
            .review
            .as_ref()
            .map(|review| review.review_id())
            .transpose()?;
        let approval_revision = if command
            .available_actions
            .contains(&application::ActionKind::Approve)
        {
            let review = command
                .review
                .as_ref()
                .context("original approval review missing")?;
            let row:Option<(String,i64)>=sqlx::query_as("SELECT SUBSTR(home_id,1,37),revision FROM marketplace_extension_grants WHERE extension_id=$1")
                .bind(review.scope().extension_id().as_str()).fetch_optional(&database.pool).await?;
            Some(match row {
                None => 0,
                Some((home, revision))
                    if home == principal.home_id.to_string()
                        && (1..MAX_REVISION).contains(&revision) =>
                {
                    revision
                }
                _ => bail!("approval binding changed or needs recovery"),
            })
        } else {
            None
        };
        Ok(InstallationView {
            schema_version: 1,
            review_id,
            approval_revision,
            command,
        })
    }
    pub(crate) fn request_install_action(
        &self,
        principal: &CurrentPrincipal,
        id: Uuid,
        request: ActionRequest,
    ) -> Result<Acknowledgement> {
        let action = request.into_action(principal.clone())?;
        self.install_action(id, action)
            .map(|receipt| Acknowledgement {
                schema_version: 1,
                operation_id: id,
                receipt,
            })
    }
    /// Supplies ONE original Selection-bearing context for real HTTP/native
    /// tests. Discovery/protection are synthetic; actual resource ownership is
    /// bound before this context is made. Not compiled into production.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_install_context(
        &self,
        mut context: InstallContext,
    ) -> Result<()> {
        let mut slot = self
            .installation_context
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture context unavailable"))?;
        if slot.is_some() {
            bail!("original fixture context already retained");
        }
        // The real upload handler addresses this service's preparation owner;
        // a synthetic selection must not create a second inaccessible owner.
        context.preparation = self.preparation.clone();
        *slot = Some(context);
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_original_installation(&self) -> application::Service {
        self.installation.clone()
    }
}

#[cfg(test)]
mod tests;
