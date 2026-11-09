//! Actual crypto/source archives and SQL. Discovery/protection owners are
//! synthetic; these are not native/public-source or activation qualifications.
use super::*;
use crate::{
    config::DatabaseConfig,
    marketplace::{
        security_state,
        worker::{MetadataObservation, PublicSourceObservation},
    },
};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor, evidence::IssuerPolicy, records::SignedRecord,
};
use std::{path::Path, time::Duration};

#[path = "cancellation_tests.rs"]
mod cancellation_tests;

pub(super) async fn signed_fixture() -> Result<serde_json::Value> {
    signed_fixture_version("1.0.0").await
}
async fn signed_fixture_version(version: &str) -> Result<serde_json::Value> {
    signed_fixture_slug(version, "fixture-provider").await
}
async fn signed_fixture_slug(version: &str, slug: &str) -> Result<serde_json::Value> {
    signed_fixture_source(version, slug, None).await
}
async fn signed_fixture_source(
    version: &str,
    slug: &str,
    source: Option<&str>,
) -> Result<serde_json::Value> {
    signed_fixture_network(version, slug, source, true).await
}
async fn signed_fixture_network(
    version: &str,
    slug: &str,
    source: Option<&str>,
    network: bool,
) -> Result<serde_json::Value> {
    signed_fixture_options(version, slug, source, network, None).await
}
async fn signed_fixture_options(
    version: &str,
    slug: &str,
    source: Option<&str>,
    network: bool,
    patch: Option<&serde_json::Value>,
) -> Result<serde_json::Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/fixture.mjs");
    let output=tokio::time::timeout(Duration::from_secs(15),tokio::process::Command::new("node")
        .args(["--input-type=module","-e","import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));process.stdout.write(JSON.stringify(createFixture({operational:true,slug:process.argv[3],sourceBytes:process.argv[4]?Buffer.from(process.argv[4]):undefined,transformManifest(m){m.version=process.argv[2];m.runtime.security={sdk_state_limit_bytes:1048576};if(process.argv[5]==='off')m.permissions=m.permissions.filter(p=>p!=='network.egress');if(process.argv[6])Object.assign(m,JSON.parse(process.argv[6]))}})));" ])
        .arg(path).arg(version).arg(slug).arg(source.unwrap_or("")).arg(if network{"on"}else{"off"})
        .arg(patch.map(serde_json::to_string).transpose()?.unwrap_or_default()).kill_on_drop(true).output()).await??;
    if !output.status.success() || output.stdout.len() > 128 * 1024 {
        bail!("installation SDK fixture generation failed");
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
pub(super) fn descriptor(value: &serde_json::Value) -> Result<(CandidateDescriptor, IssuerPolicy)> {
    let mut d = CandidateDescriptor::parse(&serde_json::to_vec(
        &serde_json::json!({"schema_version":1,
        "publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]}),
    )?)?;
    let now = Utc::now().timestamp() as u64;
    for (i, pair) in d.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.issued_at = now;
        credential.expires_at = now + 3600;
        pair.credential = SignedRecord::sign(credential, None, &SigningKey::from_bytes(&[3; 32]))?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            endorsement,
            None,
            &SigningKey::from_bytes(&[10 + i as u8; 32]),
        )?;
    }
    Ok((
        d,
        IssuerPolicy::new([value["issuer_key"].as_str().context("issuer")?.parse()?])?,
    ))
}
pub(super) fn budget() -> InstallBudget {
    InstallBudget {
        entries: 4,
        bytes: 4 * 1024 * 1024,
    }
}
pub(super) struct Fixture {
    pub(super) root: tempfile::TempDir,
    pub(super) db: Database,
    pub(super) actor: Actor,
    pub(super) metadata: MetadataObservation,
    pub(super) public: PublicSourceObservation,
    pub(super) archive: ArchivedSource,
    pub(super) revision: i64,
    pub(super) descriptor: CandidateDescriptor,
    pub(super) meta_owner: Box<dyn Fn(bool) + Send + Sync>,
    pub(super) public_owner: Box<dyn Fn(bool) + Send + Sync>,
}
impl Fixture {
    pub(super) async fn without_network() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let db = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.path().join("install.sqlite").display()),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        Self::with_database_scope(root, db, None, "fixture-provider", None, false, None).await
    }
    pub(super) async fn new() -> Result<Self> {
        Self::with_source(None).await
    }
    pub(super) async fn with_lan() -> Result<Self> {
        Self::with_lan_source(None).await
    }
    pub(super) async fn with_lan_source(source: Option<&str>) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let db = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.path().join("install.sqlite").display()),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        let patch = serde_json::json!({
            "permissions":["runtime.manage_containers","network.egress","live.network.private"],
            "control_surface":{"owned_settings":[
                {"id":"playlist","label":"Playlist URL","type":"password","secret":true,
                 "ownership":"managed","storage":{"type":"instance_secret","key":"playlist_url"},
                 "lan_endpoint":{"enabled_by":"localNetwork","access":"exact_url"}},
                {"id":"service","label":"Service URL","type":"text","ownership":"managed",
                 "storage":{"type":"instance_setting","key":"service_url"},
                 "lan_endpoint":{"enabled_by":"localNetwork","access":"origin"}},
                {"id":"localNetwork","label":"Local network access","type":"toggle","default":false,
                 "storage":{"type":"instance_setting","key":"localNetwork"}}
            ]}
        });
        Self::with_database_scope(
            root,
            db,
            None,
            "fixture-lan-provider",
            source,
            true,
            Some(&patch),
        )
        .await
    }
    pub(super) async fn with_source(source: Option<&str>) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let db = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.path().join("install.sqlite").display()),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        Self::with_database_and_source(root, db, None, "fixture-provider", source).await
    }
    pub(super) async fn with_database(root: tempfile::TempDir, db: Database) -> Result<Self> {
        Self::with_database_and_actor(root, db, None, "fixture-provider").await
    }
    pub(super) async fn with_database_and_actor(
        root: tempfile::TempDir,
        db: Database,
        actor: Option<Actor>,
        slug: &str,
    ) -> Result<Self> {
        Self::with_database_and_source(root, db, actor, slug, None).await
    }
    pub(super) async fn with_database_and_source(
        root: tempfile::TempDir,
        db: Database,
        actor: Option<Actor>,
        slug: &str,
        source: Option<&str>,
    ) -> Result<Self> {
        Self::with_database_scope(root, db, actor, slug, source, true, None).await
    }
    async fn with_database_scope(
        root: tempfile::TempDir,
        db: Database,
        actor: Option<Actor>,
        slug: &str,
        source: Option<&str>,
        network: bool,
        patch: Option<&serde_json::Value>,
    ) -> Result<Self> {
        db.run_migrations().await?;
        let actor = match actor {
            Some(actor) => actor,
            None => super::super::tests::owner(&db).await?,
        };
        let preferences = String::from_utf8(canonical::to_vec(
            &crate::marketplace::preferences::Preferences::default(),
        )?)?;
        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
            .bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&db.pool).await?;
        let value = signed_fixture_options("1.0.0", slug, source, network, patch).await?;
        let (release, package, proof) =
            crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
        let (descriptor, policy) = descriptor(&value)?;
        let (metadata, meta_owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
        security_state::admit(&db, &metadata).await?;
        let (public, public_owner) = PublicSourceObservation::fixture_for_source(
            release.release_id(),
            Digest::of(b"SPDX-License-Identifier: MIT\n"),
        );
        let archive = crate::marketplace::archive::fixture_from_package(
            &root.path().canonicalize()?.join("archive"),
            &release,
            &package,
            &proof,
        )
        .await?;
        let material = Material::from_archive(&archive, &metadata)?;
        material.current(&metadata, &public)?;
        let grant = super::super::approve_review_with_binding(
            &db,
            &actor,
            &material.review,
            &ApprovalSelection {
                review_id: material.review.review_id()?,
                expected_revision: 0,
                mutation_id: Uuid::new_v4(),
                automatic_updates: false,
            },
            Utc::now(),
            true,
        )
        .await?;
        Ok(Self {
            root,
            db,
            actor,
            metadata,
            public,
            archive,
            revision: grant.revision(),
            descriptor,
            meta_owner: Box::new(meta_owner),
            public_owner: Box::new(public_owner),
        })
    }
    pub(super) fn material(&self) -> Result<Material> {
        Material::from_archive(&self.archive, &self.metadata)
    }
    /// Real newly signed bytes/review/head/grant for an update fixture. This
    /// does not synthesize pending enrollment, native completion or activation.
    pub(super) async fn upgrade(
        &mut self,
        version: &str,
        source: Option<&str>,
    ) -> Result<CommittedOwnerGrant> {
        let value = signed_fixture_source(version, "fixture-provider", source).await?;
        let (release, package, proof) =
            crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
        let (mut descriptor, policy) = descriptor(&value)?;
        if descriptor.release.payload.extension_id != self.descriptor.release.payload.extension_id {
            bail!("fixture update must keep its publisher/extension identity");
        }
        descriptor.head.payload.sequence = self
            .descriptor
            .head
            .payload
            .sequence
            .checked_add(1)
            .context("fixture head sequence")?;
        descriptor.head = SignedRecord::sign(
            descriptor.head.payload,
            Some(Digest::of(descriptor.publisher.to_bytes()?)),
            &SigningKey::from_bytes(&[2; 32]),
        )?;
        let (metadata, meta_owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
        security_state::admit(&self.db, &metadata).await?;
        let (public, public_owner) = PublicSourceObservation::fixture_for_source(
            release.release_id(),
            Digest::of(b"SPDX-License-Identifier: MIT\n"),
        );
        let archive = crate::marketplace::archive::fixture_from_package(
            &self.root.path().canonicalize()?.join("archive"),
            &release,
            &package,
            &proof,
        )
        .await?;
        let material = Material::from_archive(&archive, &metadata)?;
        material.current(&metadata, &public)?;
        let grant = super::super::approve_review_with_binding(
            &self.db,
            &self.actor,
            &material.review,
            &ApprovalSelection {
                review_id: material.review.review_id()?,
                expected_revision: self.revision,
                mutation_id: Uuid::new_v4(),
                automatic_updates: false,
            },
            Utc::now(),
            true,
        )
        .await?;
        self.revision = grant.revision();
        self.descriptor = descriptor;
        self.metadata = metadata;
        self.public = public;
        self.archive = archive;
        self.meta_owner = Box::new(meta_owner);
        self.public_owner = Box::new(public_owner);
        Ok(grant)
    }
    fn intent(&self, mutation: Uuid, expected: i64) -> Result<OwnerInstallIntent> {
        OwnerInstallIntent::for_actor(
            self.actor.clone(),
            &self.material()?.review,
            self.revision,
            expected,
            mutation,
        )
    }
    pub(super) async fn stage(
        &self,
        mutation: Uuid,
        expected: i64,
        limit: InstallBudget,
    ) -> Result<StagedInstallation> {
        stage_checked(
            &self.db,
            &self.metadata,
            &self.public,
            self.material()?,
            self.intent(mutation, expected)?,
            limit,
        )
        .await
    }
    pub(super) async fn stage_configured(
        &self,
        target: InstallTarget,
        mutation: Uuid,
        expected: i64,
        limit: InstallBudget,
    ) -> Result<StagedInstallation> {
        let material = self.material()?;
        let mut intent = self.intent(mutation, expected)?;
        intent.configured = Some(configured::Target::new(
            &intent,
            target,
            material.review.scope().extension_id(),
        )?);
        stage_checked(
            &self.db,
            &self.metadata,
            &self.public,
            material,
            intent,
            limit,
        )
        .await
    }
    pub(super) async fn counts(&self) -> Result<(i64, i64, i64)> {
        Ok(sqlx::query_as("SELECT (SELECT COUNT(*) FROM extensions),(SELECT COUNT(*) FROM extension_instances),(SELECT COUNT(*) FROM marketplace_installations)").fetch_one(&self.db.pool).await?)
    }
}

#[tokio::test]
async fn explicit_staging_is_disabled_durable_and_exact_retries_create_no_new_instance()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let mutation = Uuid::new_v4();
    let staged = f.stage(mutation, 0, budget()).await?;
    assert_eq!(staged.revision(), 1);
    assert_eq!(staged.release(), f.archive.release_id());
    assert_eq!(
        staged.extension(),
        f.material()?.review.scope().extension_id()
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    let enabled:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM extensions WHERE enabled=TRUE)+(SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE)+(SELECT COUNT(*) FROM providers) AS BIGINT)").fetch_one(&f.db.pool).await?;
    assert_eq!(
        enabled, 0,
        "staging is not activation or provider registration"
    );
    let retried = f.stage(mutation, 0, budget()).await?;
    assert_eq!(retried.instance(), staged.instance());
    let mut tx = f.db.pool.begin().await?;
    let row = store::load(&mut tx, f.db.driver, staged.extension())
        .await?
        .unwrap();
    assert!(row.journal.active.is_none() && row.journal.previous.is_none());
    assert_eq!(
        row.journal
            .pending
            .as_ref()
            .unwrap()
            .facts()?
            .release()
            .release_id(),
        staged.release()
    );
    tx.rollback().await?;
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            f.root.path().join("install.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert_eq!(
        f.stage(mutation, 0, budget()).await?.instance(),
        staged.instance()
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    let changed = f.intent(mutation, 1)?;
    assert!(
        stage_checked(
            &f.db,
            &f.metadata,
            &f.public,
            f.material()?,
            changed,
            budget()
        )
        .await
        .is_err()
    );
    assert!(f.stage(Uuid::new_v4(), 0, budget()).await.is_err());
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn failed_staging_never_leaves_metadata_instances_or_repairs_missing_authority() -> Result<()>
{
    for fault in [
        "quota",
        "grant",
        "grant_epoch",
        "session",
        "capability",
        "home",
        "foreign_home",
        "public",
        "metadata",
        "old_head",
        "unknown_extension",
        "missing_budget",
    ] {
        let f = Fixture::new().await?;
        let mut intent = f.intent(Uuid::new_v4(), 0)?;
        let material = f.material()?;
        match fault {
            "quota" => {}
            "grant" => intent.grant_revision += 1,
            "grant_epoch" => {
                sqlx::query(
                    "UPDATE marketplace_extension_grants SET revocation_revision=revision+1",
                )
                .execute(&f.db.pool)
                .await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='installation fixture'").execute(&f.db.pool).await?;
            }
            "capability" => {
                sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "home" => {
                sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL")
                    .execute(&f.db.pool)
                    .await?;
            }
            "foreign_home" => {
                let other = super::super::tests::owner(&f.db).await?;
                sqlx::query("UPDATE marketplace_preferences SET home_id=$1,approved_by_user_id=$2")
                    .bind(other.home.to_string())
                    .bind(other.user.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "public" => (f.public_owner)(false),
            "metadata" => (f.meta_owner)(false),
            "old_head" => {
                let mut d = f.descriptor.clone();
                d.head.payload.sequence += 1;
                d.head = SignedRecord::sign(
                    d.head.payload,
                    d.head.authority_record_id,
                    &SigningKey::from_bytes(&[2; 32]),
                )?;
                let records = elixir_marketplace::security_job::SecurityRecords {
                    schema_version: 1,
                    extension_id: d.release.payload.extension_id.clone(),
                    publisher: d.publisher,
                    head: Some(d.head),
                    revocations: vec![],
                };
                security_state::fixture_observe_signed(&f.db, records).await?;
            }
            "unknown_extension" => {
                sqlx::query("INSERT INTO extensions (extension_id,name,version,kind,manifest_json,enabled) VALUES ($1,'unknown','1.0.0','module','{}',FALSE)").bind(material.review.scope().extension_id().as_str()).execute(&f.db.pool).await?;
            }
            "missing_budget" => {
                sqlx::query("DELETE FROM marketplace_installation_budget")
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        let limit = if fault == "quota" {
            InstallBudget {
                entries: 4,
                bytes: ROW_BYTES as u64 + 1,
            }
        } else {
            budget()
        };
        assert!(
            stage_checked(&f.db, &f.metadata, &f.public, material, intent, limit)
                .await
                .is_err(),
            "{fault}"
        );
        assert_eq!(
            f.counts().await?,
            (i64::from(fault == "unknown_extension"), 0, 0),
            "{fault}"
        );
        if fault != "missing_budget" {
            let counts:(i64,i64)=sqlx::query_as("SELECT entries,encoded_bytes FROM marketplace_installation_budget WHERE singleton=1").fetch_one(&f.db.pool).await?;
            assert_eq!(counts, (0, 0), "{fault}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn staged_corruption_and_lost_rows_fail_without_recreation_or_counter_reset() -> Result<()> {
    for fault in [
        "hash",
        "instance",
        "state",
        "state_values",
        "enabled",
        "extension_enabled",
        "manifest",
        "kind",
        "signer",
        "publisher",
        "scope",
        "missing_journal",
    ] {
        let f = Fixture::new().await?;
        let mutation = Uuid::new_v4();
        let staged = f.stage(mutation, 0, budget()).await?;
        match fault {
            "hash" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"bad").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "instance" => {
                sqlx::query("DELETE FROM extension_instances WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "state" => {
                sqlx::query("DELETE FROM marketplace_instance_sdk_state WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "state_values" => {
                sqlx::query("INSERT INTO marketplace_instance_sdk_values (instance_id,key,revision,value_encrypted,encoded_bytes) VALUES ($1,'hidden',1,'bad',3)").bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "enabled" => {
                sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "extension_enabled" => {
                sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
                    .bind(staged.extension().as_str())
                    .execute(&f.db.pool)
                    .await?;
            }
            "manifest" => {
                sqlx::query("UPDATE extensions SET manifest_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "kind" => {
                sqlx::query("UPDATE extensions SET kind='connector'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "signer" => {
                sqlx::query("UPDATE extensions SET signing_key_id='changed'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "publisher" => {
                sqlx::query("UPDATE extensions SET publisher_name='changed'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "scope" => {
                sqlx::query("UPDATE marketplace_installations SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "missing_journal" => {
                sqlx::query("DELETE FROM marketplace_installations")
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        let before = f.counts().await?;
        assert!(f.stage(mutation, 0, budget()).await.is_err(), "{fault}");
        assert_eq!(f.counts().await?, before, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn explicit_grant_revocation_survives_reapproval_even_with_a_corrupt_journal() -> Result<()> {
    for corrupt in [false, true] {
        let f = Fixture::new().await?;
        let mutation = Uuid::new_v4();
        let staged = f.stage(mutation, 0, budget()).await?;
        if corrupt {
            sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                .bind(Digest::of(b"bad").to_string())
                .execute(&f.db.pool)
                .await?;
        }
        let revoked = super::super::revoke_as(
            &f.db,
            &f.actor,
            staged.extension(),
            1,
            Uuid::new_v4(),
            Utc::now(),
        )
        .await?;
        assert_eq!(revoked, 2);
        let floor: i64 = sqlx::query_scalar(
            "SELECT revocation_revision FROM marketplace_extension_grants WHERE extension_id=$1",
        )
        .bind(staged.extension().as_str())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(floor, 2);
        let review = f.material()?.review;
        let granted = super::super::approve_review_with_binding(
            &f.db,
            &f.actor,
            &review,
            &ApprovalSelection {
                review_id: review.review_id()?,
                expected_revision: 2,
                mutation_id: Uuid::new_v4(),
                automatic_updates: false,
            },
            Utc::now(),
            true,
        )
        .await?;
        assert_eq!(granted.revision(), 3);
        assert!(
            f.stage(mutation, 0, budget()).await.is_err(),
            "reapproval must not revive old install intent"
        );
        assert_eq!(f.counts().await?, (1, 1, 1));
        let floor: i64 = sqlx::query_scalar(
            "SELECT revocation_revision FROM marketplace_extension_grants WHERE extension_id=$1",
        )
        .bind(staged.extension().as_str())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(floor, 2);
        if !corrupt {
            let mut tx = f.db.pool.begin().await?;
            let record = store::load(&mut tx, f.db.driver, staged.extension())
                .await?
                .unwrap();
            assert!(
                record.journal.pending.as_ref().unwrap().grant_revision < record.floor,
                "a fresh enabled grant must not erase the old capsule's revocation boundary"
            );
            tx.rollback().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn revocation_needs_neither_installation_records_nor_a_healthy_metadata_budget() -> Result<()>
{
    for fault in [
        "missing_journal",
        "missing_budget",
        "corrupt_budget",
        "grant_json",
    ] {
        let f = Fixture::new().await?;
        let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
        let sql = match fault {
            "missing_journal" => "DELETE FROM marketplace_installations",
            "missing_budget" => "DELETE FROM marketplace_installation_budget",
            "corrupt_budget" => {
                "UPDATE marketplace_installation_budget SET entries=0,encoded_bytes=0"
            }
            "grant_json" => "UPDATE marketplace_extension_grants SET review_json='{}'",
            _ => unreachable!(),
        };
        sqlx::query(sql).execute(&f.db.pool).await?;
        let before = f.counts().await?;
        let revision = super::super::revoke_as(
            &f.db,
            &f.actor,
            staged.extension(),
            1,
            Uuid::new_v4(),
            Utc::now(),
        )
        .await?;
        assert_eq!(revision, 2, "{fault}");
        let state: (i64, i64, i64) = sqlx::query_as("SELECT revision,revocation_revision,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants")
            .fetch_one(&f.db.pool).await?;
        assert_eq!(state, (2, 2, 0), "{fault}");
        assert_eq!(
            f.counts().await?,
            before,
            "revocation never repairs {fault}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL installation staging concurrency/restart fixture"]
async fn postgres_staging_serializes_contenders_and_recovers_exact_retry_after_reopen() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_INSTALL_POSTGRES_URL")
        .context("fresh dedicated PG installation fixture URL")?;
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    if db.driver != DatabaseDriver::Postgres {
        bail!("PG fixture required");
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'",
    )
    .fetch_one(&db.pool)
    .await?;
    if tables != 0 {
        bail!("dedicated PG installation database must be empty; no reset");
    }
    let mut f = Fixture::with_database(tempfile::tempdir()?, db).await?;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let (a, b) = tokio::join!(f.stage(first, 0, budget()), f.stage(second, 0, budget()));
    let (staged, mutation) = match (a, b) {
        (Ok(a), Err(_)) => (a, first),
        (Err(_), Ok(b)) => (b, second),
        _ => bail!("exactly one competing initial install must commit"),
    };
    assert_eq!(f.counts().await?, (1, 1, 1));
    let instance = staged.instance();
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert_eq!(f.stage(mutation, 0, budget()).await?.instance(), instance);
    assert_eq!(f.counts().await?, (1, 1, 1));
    super::super::revoke_as(
        &f.db,
        &f.actor,
        staged.extension(),
        1,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    assert!(f.stage(mutation, 0, budget()).await.is_err());
    let floor: i64 =
        sqlx::query_scalar("SELECT revocation_revision FROM marketplace_extension_grants")
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(floor, 2);
    // Cleanup has the same lock order on PostgreSQL, but no dependence on an
    // enabled grant, fresh discovery/source or usable grant-review JSON.
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    let cancel_a = Uuid::new_v4();
    let cancel_b = Uuid::new_v4();
    let (a, b) = tokio::join!(
        cancellation::cancel(
            &f.db,
            cancellation::OwnerCancelIntent::for_actor(f.actor.clone(), &staged, cancel_a)?,
            budget()
        ),
        cancellation::cancel(
            &f.db,
            cancellation::OwnerCancelIntent::for_actor(f.actor.clone(), &staged, cancel_b)?,
            budget()
        )
    );
    let cancelled = match (a, b) {
        (Ok(a), Err(_)) => {
            assert_eq!(a.revision(), 2);
            cancel_a
        }
        (Err(_), Ok(b)) => {
            assert_eq!(b.revision(), 2);
            cancel_b
        }
        _ => bail!("exactly one competing cancellation must commit"),
    };
    assert_eq!(f.counts().await?, (1, 0, 1));
    let state: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_state")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(state, 0);
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url: std::env::var("ELIXIR_TEST_MARKETPLACE_INSTALL_POSTGRES_URL")?,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert_eq!(
        cancellation::cancel(
            &f.db,
            cancellation::OwnerCancelIntent::for_actor(f.actor.clone(), &staged, cancelled)?,
            budget()
        )
        .await?
        .revision(),
        2
    );
    assert_eq!(f.counts().await?, (1, 0, 1));
    f.db.pool.close().await;
    Ok(())
}
