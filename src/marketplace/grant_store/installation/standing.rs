//! Durable ACTIVE-source restrictions, not an execution or source-byte permit.
//! No discovery receipt, issuer, browser session or latest-head enrollment is
//! used for retained execution. Every effect checks this binding in its own
//! transaction; an independent native controller still owns/rechecks real code.
use super::*;

/// A non-serializable snapshot of the exact active capsule. This cannot enable
/// an instance, enroll a pending source, start code, or prove disk equality.
pub(in crate::marketplace) struct InstalledBinding {
    configured_instance: Option<Uuid>,
    home: Uuid,
    extension: ExtensionId,
    instance: Uuid,
    capsule: Digest,
    review: GrantReview,
    budget: InstallBudget,
    provider: Option<super::providers::Expectation>,
}
impl InstalledBinding {
    pub(super) fn generation(&self) -> super::runtime::Generation {
        super::runtime::Generation {
            configured_instance: self.configured_instance,
            home: self.home,
            extension: self.extension.clone(),
            instance: self.instance,
            capsule: self.capsule,
        }
    }
    pub(in crate::marketplace) async fn load(
        database: &Database,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
        budget: InstallBudget,
    ) -> Result<Self> {
        Self::load_scope(database, home, extension, instance, None, budget).await
    }
    /// Explicit configured-owner lookup. Never falls back to legacy or chooses
    /// another configured instance when this journal is missing.
    pub(in crate::marketplace) async fn load_configured(
        database: &Database,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
        budget: InstallBudget,
    ) -> Result<Self> {
        Self::load_scope(database, home, extension, instance, Some(instance), budget).await
    }
    async fn load_scope(
        database: &Database,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
        configured_instance: Option<Uuid>,
        budget: InstallBudget,
    ) -> Result<Self> {
        if home.is_nil() || instance.is_nil() {
            bail!("invalid installed-source identity");
        }
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let (grant, capsule) = Box::pin(read_active(
            &mut tx,
            database.driver,
            home,
            extension,
            instance,
            configured_instance,
            budget,
        ))
        .await?;
        let facts = capsule.facts()?;
        let (_, review) = capsule.source.review(facts.release())?;
        let binding = Self {
            configured_instance,
            home: grant.home_id,
            extension: extension.clone(),
            instance: capsule.instance,
            capsule: Digest::of(canonical::to_vec(capsule.as_ref())?),
            review,
            budget,
            provider: None,
        };
        tx.commit().await?;
        Ok(binding)
    }
    /// Snapshot restrictions, not authority. The original controller must still
    /// verify real source bytes/native ownership independently.
    pub(in crate::marketplace) async fn load_provider(
        database: &Database,
        coordinates: &super::providers::Coordinates,
        budget: InstallBudget,
        expected: Option<Digest>,
    ) -> Result<(Self, super::providers::Descriptor)> {
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let (grant, capsule) = Box::pin(read_active(
            &mut tx,
            database.driver,
            coordinates.home,
            &coordinates.extension,
            coordinates.instance,
            Some(coordinates.instance),
            budget,
        ))
        .await?;
        let descriptor = super::providers::describe(
            &mut tx,
            database.driver,
            coordinates.home,
            &coordinates.extension,
            coordinates.instance,
            &capsule,
            coordinates.provider,
        )
        .await?;
        if expected.is_some_and(|revision| revision != descriptor.revision) {
            bail!("source provider revision changed");
        }
        let (_, review) = capsule.source.review(capsule.facts()?.release())?;
        let binding = Self {
            configured_instance: Some(coordinates.instance),
            home: grant.home_id,
            extension: coordinates.extension.clone(),
            instance: capsule.instance,
            capsule: Digest::of(canonical::to_vec(capsule.as_ref())?),
            review,
            budget,
            provider: Some(super::providers::Expectation {
                provider: coordinates.provider,
                revision: descriptor.revision,
            }),
        };
        tx.commit().await?;
        Ok((binding, descriptor))
    }
    async fn require_provider(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        capsule: &Capsule,
    ) -> Result<()> {
        if let Some(expected) = &self.provider {
            let descriptor = super::providers::describe(
                tx,
                driver,
                self.home,
                &self.extension,
                self.configured_instance
                    .context("provider requires configured ownership")?,
                capsule,
                expected.provider,
            )
            .await?;
            if descriptor.revision != expected.revision {
                bail!("source provider revision changed");
            }
        }
        Ok(())
    }
    /// Fresh durable restriction check, not a reusable execution authorization.
    pub(in crate::marketplace) async fn current(&self, database: &Database) -> Result<()> {
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        self.require_in_transaction(&mut tx, database.driver)
            .await?;
        tx.rollback().await?;
        Ok(())
    }
    pub(in crate::marketplace::grant_store) fn review(&self) -> &GrantReview {
        &self.review
    }
    pub(in crate::marketplace::grant_store) fn instance(&self) -> Uuid {
        self.instance
    }
    pub(in crate::marketplace::grant_store) fn coordinates(
        &self,
    ) -> super::super::sdk::namespaces::Coordinates<'_> {
        super::super::sdk::namespaces::Coordinates {
            home: self.home,
            extension: &self.extension,
            instance: self.configured_instance.unwrap_or(self.instance),
            namespace: self.instance,
        }
    }
    /// Exact enrollment DATA for independent archive/native verification. This
    /// grants no byte, native, discovery or installation authority.
    pub(super) async fn material(&self, database: &Database) -> Result<Box<Capsule>> {
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let (_, active) = Box::pin(read_active(
            &mut tx,
            database.driver,
            self.home,
            &self.extension,
            self.configured_instance.unwrap_or(self.instance),
            self.configured_instance,
            self.budget,
        ))
        .await?;
        if Digest::of(canonical::to_vec(active.as_ref())?) != self.capsule {
            bail!("installed source generation changed");
        }
        self.require_provider(&mut tx, database.driver, &active)
            .await?;
        tx.commit().await?;
        Ok(active)
    }
    pub(in crate::marketplace::grant_store) async fn require_in_transaction(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<CommittedOwnerGrant> {
        Ok(self.current_material(tx, driver).await?.0)
    }
    /// Exact current enrollment DATA under the caller's existing transaction.
    /// No native, transport or reusable permission authority is created here.
    pub(super) async fn current_material(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<(CommittedOwnerGrant, Box<Capsule>)> {
        let (grant, active) = Box::pin(read_active(
            tx,
            driver,
            self.home,
            &self.extension,
            self.configured_instance.unwrap_or(self.instance),
            self.configured_instance,
            self.budget,
        ))
        .await?;
        if Digest::of(canonical::to_vec(active.as_ref())?) != self.capsule {
            bail!("installed source generation changed");
        }
        self.require_provider(tx, driver, &active).await?;
        Ok((grant, active))
    }
    /// Reviewed manifest DATA only. Keep capsule/journal internals inside the
    /// installation layer; an SDK reader cannot edit or mint enrollment data.
    pub(in crate::marketplace::grant_store) async fn current_manifest(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<crate::extensions::manifest::ExtensionManifest> {
        let (_, active) = self.current_material(tx, driver).await?;
        let (manifest, _) = active.source.review(active.facts()?.release())?;
        Ok(manifest)
    }
}

async fn read_active(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
    configured_instance: Option<Uuid>,
    budget: InstallBudget,
) -> Result<(CommittedOwnerGrant, Box<Capsule>)> {
    // Same lock order as installation/security writers: home -> grant ->
    // installation ledger/journal -> security -> extension -> instance.
    crate::marketplace::preferences::require_home_binding(tx, home, driver).await?;
    let base = "SELECT extension_id,home_id,revision,revocation_revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1";
    let query = if driver == DatabaseDriver::Postgres {
        format!("{base} FOR SHARE")
    } else {
        base.to_owned()
    };
    let row = sqlx::query(&query)
        .bind(extension.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .context("installed grant is missing")?;
    let revoked: i64 = row.try_get("revocation_revision")?;
    let grant = StoredGrant::parse(row)?
        .active()
        .context("installed grant is disabled")?;
    if grant.home_id != home || !(0..=grant.revision).contains(&revoked) {
        bail!("installed grant home or revocation boundary differs");
    }
    store::read_budget(tx, driver, budget).await?;
    let mut stored = store::load_configured_shared(tx, driver, extension, configured_instance)
        .await?
        .context("installed journal is missing; no implicit repair")?;
    if stored.journal.home != home {
        bail!("installed source belongs to a different home");
    }
    if let Some(fence) = &stored.journal.activation {
        // These source-only preparation phases cannot write candidate state.
        // A future migration/quiescence phase must explicitly define whether
        // active writes are allowed; do not silently default to allowing it.
        match fence.phase {
            record::ActivationPhase::Prepared
            | record::ActivationPhase::Starting
            | record::ActivationPhase::SourcePrepared => {}
            record::ActivationPhase::Quiescing
            | record::ActivationPhase::StateCopied
            | record::ActivationPhase::CandidateStarting => {
                bail!("installed generation is quiescing for an explicit cutover");
            }
        }
    }
    let active = stored
        .journal
        .active
        .as_ref()
        .context("no active installed source")?;
    if (configured_instance.is_none() && active.instance != instance)
        || configured_instance.is_some_and(|owner| owner != instance)
        || active.grant_revision <= revoked
        || active.grant_revision > grant.revision
    {
        bail!("installed instance or enrollment grant epoch differs");
    }
    let facts = active.facts()?;
    let (_, requested) = active.source.review(facts.release())?;
    // Enrollment/update consent uses CommittedOwnerGrant::covers (including
    // auto-update policy). Already active code instead keeps its ORIGINAL,
    // narrower scope under later manual approvals; it never adopts that newer
    // release or its additional permissions. Revocation still fences it.
    if requested.scope().runtime().is_none()
        || requested
            .scope()
            .delta_from(grant.review.scope())?
            .requires_approval()
    {
        bail!("installed source exceeds the currently approved scope");
    }
    crate::marketplace::security_state::retained::require_in_transaction(tx, driver, &facts)
        .await?;
    store::require_active_metadata(tx, driver, &stored.journal).await?;
    super::super::sdk::lock_instance(tx, &grant, instance, driver).await?;
    if let Some(owner) = configured_instance {
        super::super::sdk::namespaces::require(
            tx,
            super::super::sdk::namespaces::Coordinates {
                home,
                extension,
                instance: owner,
                namespace: active.instance,
            },
            super::super::sdk::namespaces::Purpose::Installed,
        )
        .await?;
    } else {
        super::super::sdk::namespaces::require_identity(
            tx,
            home,
            extension,
            instance,
            super::super::sdk::namespaces::Purpose::Installed,
        )
        .await?;
    }
    let state: (String, String, String) = sqlx::query_as(
        "SELECT extension_id,home_id,purpose FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(active.instance.to_string())
    .fetch_optional(&mut **tx)
    .await?
    .context("installed instance state is missing; no implicit initialization")?;
    if state.0 != extension.as_str() || state.1 != home.to_string() || state.2 != "installed" {
        bail!("installed state belongs to a different source or home");
    }
    Ok((
        grant,
        stored
            .journal
            .active
            .take()
            .context("active source disappeared")?,
    ))
}

#[cfg(test)]
pub(super) mod tests;
