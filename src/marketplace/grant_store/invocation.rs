//! Explicit owner action against an exact quarantine, NOT installation or
//! activation. There is deliberately no public route or automatic caller.
use super::*;
use crate::marketplace::{
    quarantine::execution::{Broker, NativeInvocation},
    search::Selection,
    worker::PublicSourceObservation,
};
use elixir_marketplace::source_execution::{RuntimeJson, SourceExecutionLimits, SourceSdkReply};

/// Private controller selection for ONE invocation. No Deserialize/Clone and no
/// conversion from a grant, network message, or preparation-success receipt.
pub(in crate::marketplace) struct OwnerInvocation {
    actor: Actor,
    review_id: Digest,
    grant_revision: i64,
    limits: SourceExecutionLimits,
    arguments: RuntimeJson,
}
impl OwnerInvocation {
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        review: &GrantReview,
        grant_revision: i64,
        limits: SourceExecutionLimits,
        arguments: RuntimeJson,
    ) -> Result<Self> {
        if grant_revision < 1 {
            bail!("owner invocation requires a committed grant revision");
        }
        let actor = Actor::from_principal(principal, !review.scope().secret_keys().is_empty())?;
        limits.validate()?;
        Ok(Self {
            actor,
            review_id: review.review_id()?,
            grant_revision,
            limits,
            arguments,
        })
    }
}

struct LocalInvocation<'a> {
    database: &'a Database,
    secrets: &'a SecretsManager,
    selected: &'a mut Selection,
    actor: Actor,
    grant: &'a CommittedOwnerGrant,
    requested: &'a GrantReview,
    instance: Uuid,
    source: PublicSourceObservation,
    descriptor: elixir_marketplace::source_job::SourceMatchDescriptor,
    summary: elixir_marketplace::source_job::SourceMatchSummary,
    expires: tokio::time::Instant,
    checked: Option<tokio::time::Instant>,
    native: Option<NativeInvocation>,
}
impl LocalInvocation<'_> {
    fn source_current(&self) -> Result<()> {
        if let Some(native) = &self.native {
            native.require_invoke(&self.descriptor)?;
        }
        if tokio::time::Instant::now() >= self.expires || Utc::now() >= self.actor.expires {
            bail!("owner source invocation expired");
        }
        self.selected.client().require_blob_owner()?;
        self.source
            .require_matches_source(&self.descriptor, &self.summary)
    }
}
#[async_trait::async_trait]
impl Broker for LocalInvocation<'_> {
    fn native_started(&mut self, invocation: NativeInvocation) -> Result<()> {
        self.source_current()?;
        if self.native.is_some() {
            bail!("owner invocation already owns its native executor");
        }
        invocation.require_invoke(&self.descriptor)?;
        self.native = Some(invocation);
        Ok(())
    }
    async fn retire_effects(&mut self) -> Result<()> {
        // No HTTP/gateway children in this local-only broker. The executor's
        // scope must already have been revoked; process retirement is joined
        // independently by the original source job, never inferred here.
        if let Some(native) = self.native.take() {
            if native.check().is_ok() {
                bail!("owner SDK scope remained live after its native exchange");
            }
        }
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        self.source_current()?;
        // Periodic durable/publisher checks are shared across frames, not
        // repeated per chunk or deferred indefinitely by a productive child.
        if self
            .checked
            .is_none_or(|last| last.elapsed() >= std::time::Duration::from_millis(500))
        {
            self.selected.current().await?;
            sdk::authorize_invocation(
                self.database,
                &self.actor,
                self.grant,
                self.requested,
                self.instance,
            )
            .await?;
            self.source_current()?;
            self.checked = Some(tokio::time::Instant::now());
        }
        Ok(())
    }
    async fn request(&mut self, raw: &RuntimeJson, maximum: usize) -> Result<SourceSdkReply> {
        self.source_current()?;
        self.native
            .as_ref()
            .context("owner SDK requires its actual native executor")?
            .current()
            .await?;
        // Every effect rechecks actor/home/grant/instance in its own actual
        // transaction, even when the polling observation is still fresh.
        sdk::LocalScope {
            database: self.database,
            secrets: self.secrets,
            grant: self.grant,
            requested: self.requested,
            instance: self.instance,
        }
        .execute(
            raw,
            Some(sdk::InvocationAuthority {
                actor: &self.actor,
                maximum_reply_bytes: maximum,
            }),
            || self.source_current(),
        )
        .await
    }
}

impl GrantCheckedQuarantine {
    /// One explicitly requested source call. Merely approving a grant never
    /// runs code. Public-source TTL is specific to this foreground quarantine,
    /// not a future installed runtime's long-lived execution authority.
    pub(in crate::marketplace) async fn invoke_for_owner(
        &mut self,
        selected: &mut Selection,
        database: &Database,
        secrets: &SecretsManager,
        call: OwnerInvocation,
    ) -> Result<RuntimeJson> {
        self.invoke_owner_bound(
            selected,
            database,
            secrets,
            call,
            #[cfg(unix)]
            None,
        )
        .await
    }
    /// The image must come from this exact source job's actual reproduction and
    /// import. A grant, signed config digest or persisted summary cannot supply it.
    /// This remains a private explicit foreground action, NOT installed activation.
    #[cfg(unix)]
    pub(in crate::marketplace) async fn invoke_compiled_for_owner(
        &mut self,
        image: &crate::marketplace::quarantine::image_reference::RuntimeImage,
        selected: &mut Selection,
        database: &Database,
        secrets: &SecretsManager,
        call: OwnerInvocation,
    ) -> Result<RuntimeJson> {
        image.require_source(self.source.source())?;
        self.invoke_owner_bound(selected, database, secrets, call, Some(image))
            .await
    }
    async fn invoke_owner_bound(
        &mut self,
        selected: &mut Selection,
        database: &Database,
        secrets: &SecretsManager,
        call: OwnerInvocation,
        #[cfg(unix)] image: Option<&crate::marketplace::quarantine::image_reference::RuntimeImage>,
    ) -> Result<RuntimeJson> {
        self.ensure_current(database).await?;
        if call.review_id != self.requested.review_id()?
            || call.grant_revision != self.grant.revision
        {
            bail!("owner invocation does not match the reviewed source/grant revision");
        }
        let actor = call.actor;
        if actor.home != self.grant.home_id {
            bail!("owner invocation belongs to a different home");
        }
        selected.maintain().await?;
        self.source
            .refresh_public_source(selected.public_source()?)?;
        let binding = self.source.source().execution_source()?;
        let descriptor = self.source.source().execution_descriptor();
        if canonical::to_vec(selected.metadata()?.release_payload())?
            != canonical::to_vec(&descriptor.release)?
        {
            bail!("owner invocation selection differs from the quarantined source");
        }
        let resources = self
            .requested
            .scope()
            .runtime()
            .context("owner invocation has no runtime grant")?;
        self.source
            .source()
            .require_execution_resources(resources)?;
        let mut broker = LocalInvocation {
            database,
            secrets,
            selected,
            actor,
            grant: &self.grant,
            requested: &self.requested,
            instance: self.instance_id,
            source: binding,
            descriptor,
            summary: self.source.source().summary().clone(),
            expires: self.source.source().expires(),
            checked: None,
            native: None,
        };
        broker.current().await?;
        #[cfg(unix)]
        let result = if let Some(image) = image {
            self.source
                .source_mut()
                .invoke_container(image, call.limits, &call.arguments, &mut broker)
                .await?
        } else {
            self.source
                .source_mut()
                .invoke_source(call.limits, &call.arguments, &mut broker)
                .await?
        };
        #[cfg(not(unix))]
        let result = self
            .source
            .source_mut()
            .invoke_source(call.limits, &call.arguments, &mut broker)
            .await?;
        // Force final DB/session/head checks after confirmed native cleanup,
        // rather than exposing a value under the periodic polling grace.
        broker.checked = None;
        broker.current().await?;
        Ok(result)
    }
}

#[cfg(all(test, unix))]
pub(in crate::marketplace) mod native_tests;
