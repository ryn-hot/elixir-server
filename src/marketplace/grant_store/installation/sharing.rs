//! Exact ACTIVE source + current paid admission, not saved status/cache data.
use super::*;
use elixir_marketplace::{
    records::Blob, source_job::SourceMatchDescriptor, worker_sharing::SharingInventory,
};
pub(in crate::marketplace) mod hints;
#[cfg(test)]
mod native_tests;
pub(in crate::marketplace) mod publisher;

pub(in crate::marketplace) struct SharingSource {
    binding: standing::InstalledBinding,
    admission: Admission,
    descriptor: SourceMatchDescriptor,
    generation: Digest,
}
enum Admission {
    Catalog(MetadataObservation),
    Publisher {
        consent: Box<publisher::Consent>,
        publication: Box<crate::marketplace::worker::PublicationObservation>,
    },
}

#[cfg(test)]
mod tests {
    use super::super::standing::tests::local_http::configured;
    use super::super::tests::{Fixture, budget, descriptor, signed_fixture};
    use super::*;
    pub(super) async fn metadata(f: &Fixture) -> Result<(MetadataObservation, Box<dyn Fn(bool)>)> {
        let (_, policy) = descriptor(&signed_fixture().await?)?;
        let (observation, owner) =
            MetadataObservation::fixture_for_verified_descriptor(f.descriptor.clone(), &policy)?;
        Ok((observation, Box::new(owner)))
    }
    #[tokio::test]
    async fn sharing_needs_the_exact_active_configured_source_and_current_admission() -> Result<()>
    {
        let mut f = Fixture::new().await?;
        let staged = f
            .stage_configured(
                InstallTarget::New {
                    name: "Pending".into(),
                },
                Uuid::new_v4(),
                0,
                budget(),
            )
            .await?;
        let pending = staged.configured_instance().context("pending owner")?;
        let (admission, _pending_owner) = metadata(&f).await?;
        assert!(
            SharingSource::capture(&f.db, f.actor.home, pending, admission, budget())
                .await
                .is_err()
        );
        let (owner, _) = configured(&f, "Active").await?;
        let (other, _) = configured(&f, "Other").await?;
        let (admission, _held_owner) = metadata(&f).await?;
        let held = SharingSource::capture(&f.db, f.actor.home, owner, admission, budget()).await?;
        let (admission, _other_owner) = metadata(&f).await?;
        let other = SharingSource::capture(&f.db, f.actor.home, other, admission, budget()).await?;
        let inventory = held.inventory()?;
        assert_eq!(inventory.blobs.len(), 2);
        for blob in &inventory.blobs {
            assert_eq!(
                held.descriptor(blob)?.workspace.release_id,
                held.descriptor.workspace.release_id
            );
        }
        assert_ne!(inventory.generation, other.inventory()?.generation);
        assert!(
            held.descriptor(&Blob {
                sha256: Digest::of(b"unknown"),
                size: 1
            })
            .is_err()
        );
        sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
            .bind(owner.to_string())
            .execute(&f.db.pool)
            .await?;
        assert!(held.current(&f.db).await.is_err());
        other.current(&f.db).await?;
        sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
            .bind(owner.to_string())
            .execute(&f.db.pool)
            .await?;
        held.current(&f.db).await?;
        f.upgrade("1.1.0", None).await?;
        let (admission, _new_owner) = metadata(&f).await?;
        assert!(
            SharingSource::capture(&f.db, f.actor.home, owner, admission, budget())
                .await
                .is_err(),
            "new head does not substitute for installed bytes"
        );
        Ok(())
    }
    #[tokio::test]
    async fn sharing_does_not_survive_its_original_metadata_owner_or_grant() -> Result<()> {
        let f = Fixture::new().await?;
        let (owner, _) = configured(&f, "Active").await?;
        let (_, policy) = descriptor(&signed_fixture().await?)?;
        let (observation, stop) =
            MetadataObservation::fixture_for_verified_descriptor(f.descriptor.clone(), &policy)?;
        let held =
            SharingSource::capture(&f.db, f.actor.home, owner, observation, budget()).await?;
        stop(false);
        assert!(held.current(&f.db).await.is_err());
        let (admission, _renewed_owner) = metadata(&f).await?;
        let held = SharingSource::capture(&f.db, f.actor.home, owner, admission, budget()).await?;
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE WHERE extension_id=$1")
            .bind(f.descriptor.release.payload.extension_id.as_str())
            .execute(&f.db.pool)
            .await?;
        assert!(held.current(&f.db).await.is_err());
        Ok(())
    }
}
impl SharingSource {
    pub(in crate::marketplace) fn valid_for(&self) -> std::time::Duration {
        match &self.admission {
            Admission::Catalog(metadata) => metadata.valid_for(),
            Admission::Publisher { publication, .. } => publication.valid_for(),
        }
    }
    pub(in crate::marketplace) fn same_archive(&self, other: &Self) -> bool {
        self.generation == other.generation
    }
    pub(in crate::marketplace) async fn capture(
        database: &Database,
        home: Uuid,
        instance: Uuid,
        metadata: MetadataObservation,
        budget: InstallBudget,
    ) -> Result<Self> {
        metadata.support()?;
        let release = &metadata.descriptor().release.payload;
        let id = Digest::of(canonical::to_vec(release)?);
        let binding = standing::InstalledBinding::load_configured(
            database,
            home,
            &release.extension_id,
            instance,
            budget,
        )
        .await?;
        let capsule = binding.material(database).await?;
        let descriptor = capsule.descriptor()?;
        if descriptor.workspace.release_id != id
            || canonical::to_vec(&descriptor.release)? != canonical::to_vec(release)?
        {
            bail!("sharing admission differs from the active installed source");
        }
        let generation = Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/installed-sharing/v1","home":home,"instance":instance,
            "capsule":Digest::of(canonical::to_vec(capsule.as_ref())?),"issuer_policy":metadata.issuer_policy(),
        }))?);
        let source = Self {
            binding,
            admission: Admission::Catalog(metadata),
            descriptor,
            generation,
        };
        source.current(database).await?;
        Ok(source)
    }
    pub(in crate::marketplace) async fn current(&self, database: &Database) -> Result<()> {
        match &self.admission {
            Admission::Catalog(metadata) => {
                metadata.support()?;
                crate::marketplace::security_state::require_current(database, metadata).await?;
                self.binding.current(database).await
            }
            Admission::Publisher {
                consent,
                publication,
            } => consent.current(database, &self.binding, publication).await,
        }
    }
    pub(in crate::marketplace) fn inventory(&self) -> Result<SharingInventory> {
        match &self.admission {
            Admission::Catalog(metadata) => {
                metadata.support()?;
            }
            Admission::Publisher { publication, .. } => {
                publication.summary()?;
            }
        }
        let inventory = SharingInventory {
            generation: self.generation,
            blobs: self
                .descriptor
                .workspace
                .descriptors()?
                .into_iter()
                .map(|slot| slot.blob)
                .collect(),
        };
        inventory.validate()?;
        Ok(inventory)
    }
    pub(in crate::marketplace) fn descriptor(&self, blob: &Blob) -> Result<SourceMatchDescriptor> {
        if !self
            .inventory()?
            .blobs
            .iter()
            .any(|b| b.sha256 == blob.sha256 && b.size == blob.size)
        {
            bail!("sharing blob is outside the original installed archive");
        }
        Ok(self.descriptor.clone())
    }
    pub(in crate::marketplace) async fn capture_publisher(
        database: &Database,
        home: Uuid,
        instance: Uuid,
        consent: publisher::Consent,
        publication: crate::marketplace::worker::PublicationObservation,
        budget: InstallBudget,
    ) -> Result<Self> {
        let release = &publication.publication().release.payload;
        let binding = standing::InstalledBinding::load_configured(
            database,
            home,
            &release.extension_id,
            instance,
            budget,
        )
        .await?;
        let active = binding.material(database).await?;
        let descriptor = active.descriptor()?;
        if canonical::to_vec(&descriptor.release)? != canonical::to_vec(release)? {
            bail!("publisher seeding admission differs from the active installed bytes");
        }
        let generation = consent.generation(&binding)?;
        let source = Self {
            binding,
            descriptor,
            generation,
            admission: Admission::Publisher {
                consent: Box::new(consent),
                publication: Box::new(publication),
            },
        };
        source.current(database).await?;
        Ok(source)
    }
}
