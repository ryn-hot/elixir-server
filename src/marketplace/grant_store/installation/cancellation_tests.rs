use super::super::cancellation::{OwnerCancelIntent, cancel};
use super::*;

fn cancellation(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
) -> Result<OwnerCancelIntent> {
    OwnerCancelIntent::for_actor(f.actor.clone(), staged, mutation)
}

#[tokio::test]
async fn cancelled_retry_refuses_reappeared_orphan_namespace_without_mutating_any_rows()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
    let mutation = Uuid::new_v4();
    cancel(&f.db, cancellation(&f, &staged, mutation)?, budget()).await?;
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,manifest_json,enabled) VALUES('foreign-namespace.fixture','foreign','1.0.0','module','{}',FALSE)")
        .execute(&f.db.pool).await?;
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,'foreign-namespace.fixture','foreign configured instance',FALSE)")
        .bind(other.to_string()).execute(&f.db.pool).await?;
    // Raw persisted corruption, forbidden by the namespace constructor: a
    // marker at the cancelled ID survives with neither its SDK head nor its
    // original operational instance. Do not acknowledge clean absence.
    sqlx::query("INSERT INTO marketplace_sdk_namespaces(namespace_id,instance_id,extension_id,home_id,purpose) VALUES($1,$2,$3,$4,'installed')")
        .bind(staged.instance().to_string()).bind(other.to_string()).bind(staged.extension().as_str())
        .bind(f.actor.home.to_string()).execute(&f.db.pool).await?;
    let before: (String, i64) =
        sqlx::query_as("SELECT record_json,revision FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?;
    assert!(
        cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
            .await
            .is_err()
    );
    assert_eq!(
        before,
        sqlx::query_as::<_, (String, i64)>(
            "SELECT record_json,revision FROM marketplace_installations"
        )
        .fetch_one(&f.db.pool)
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
            .fetch_one(&f.db.pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_instance_sdk_state")
            .fetch_one(&f.db.pool)
            .await?,
        0
    );
    Ok(())
}

#[test]
fn operational_metadata_hash_is_order_independent_but_never_discards_manifest_fields() -> Result<()>
{
    let root = elixir_marketplace::identity::PublicKey::from_bytes(
        SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes(),
    )?;
    let extension = ExtensionId::new(&root, "fixture")?;
    let value = || {
        record::OperationalMetadata {
        extension: extension.clone(),
        name:"fixture".into(),version:"1.0.0".into(),kind:"module".into(),publisher:"fixture".into(),
        key:"fixture key".into(),trust:"community".into(),package:Digest::of(b"package"),
        manifest:r#"{"preferences":{"providers":{"z":{"prefer":["x"]},"a":{"prefer":["y"]}}},"default":0.5}"#.into(),
    }
    };
    let a = value();
    let mut b = value();
    b.manifest = r#"{ "default":0.5,"preferences":{"providers":{"a":{"prefer":["y"]},"z":{"prefer":["x"]}}}}"#.into();
    assert_eq!(a.id()?, b.id()?);
    b.manifest = r#"{"preferences":{"providers":{"a":{"prefer":["y"]},"z":{"prefer":["x"]}}},"default":0.25}"#.into();
    assert_ne!(a.id()?, b.id()?);
    b.manifest = r#"{"preferences":{"providers":{"a":{"prefer":["y"]},"z":{"prefer":["x"]}}},"default":0.5,"extra":true}"#.into();
    assert_ne!(a.id()?, b.id()?);
    Ok(())
}

#[tokio::test]
async fn restaging_a_cancelled_identity_requires_fresh_source_and_updates_only_known_disabled_metadata()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
    cancel(&f.db, cancellation(&f, &staged, Uuid::new_v4())?, budget()).await?;
    let value = signed_fixture_version("1.1.0").await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    let (mut d, policy) = descriptor(&value)?;
    d.head.payload.sequence = 1;
    d.head = SignedRecord::sign(
        d.head.payload,
        d.head.authority_record_id,
        &SigningKey::from_bytes(&[2; 32]),
    )?;
    let (metadata, meta_owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    security_state::admit(&f.db, &metadata).await?;
    let (public, public_owner) = PublicSourceObservation::fixture_for_source(
        release.release_id(),
        Digest::of(b"SPDX-License-Identifier: MIT\n"),
    );
    f.archive = crate::marketplace::archive::fixture_from_package(
        &f.root.path().canonicalize()?.join("updated-archive"),
        &release,
        &package,
        &proof,
    )
    .await?;
    f.metadata = metadata;
    f.meta_owner = Box::new(meta_owner);
    f.public = public;
    f.public_owner = Box::new(public_owner);
    f.descriptor = d;
    let material = f.material()?;
    assert!(
        f.stage(Uuid::new_v4(), 2, budget()).await.is_err(),
        "old permission review does not authorize replacement code"
    );
    assert_eq!(f.counts().await?, (1, 0, 1));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT version FROM extensions WHERE extension_id=$1")
            .bind(staged.extension().as_str())
            .fetch_one(&f.db.pool)
            .await?,
        "1.0.0"
    );
    let granted = super::super::super::approve_review_with_binding(
        &f.db,
        &f.actor,
        &material.review,
        &ApprovalSelection {
            review_id: material.review.review_id()?,
            expected_revision: 1,
            mutation_id: Uuid::new_v4(),
            automatic_updates: false,
        },
        Utc::now(),
        true,
    )
    .await?;
    f.revision = granted.revision();
    let next = f.stage(Uuid::new_v4(), 2, budget()).await?;
    assert_eq!(next.release(), release.release_id());
    assert_ne!(next.instance(), staged.instance());
    assert_eq!(next.revision(), 3);
    let row: (String,String,String,i64) = sqlx::query_as("SELECT version,package_hash,manifest_json,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extensions WHERE extension_id=$1")
        .bind(next.extension().as_str()).fetch_one(&f.db.pool).await?;
    assert_eq!(row.0, "1.1.0");
    assert_eq!(row.1, release.payload().package.sha256.to_string());
    assert_eq!(row.3, 0);
    let manifest: crate::extensions::manifest::ExtensionManifest = serde_json::from_str(&row.2)?;
    assert_eq!(manifest.version, "1.1.0");
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn cancellation_is_durable_exact_and_repeated_restage_has_no_instance_or_history_growth()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let first_mutation = Uuid::new_v4();
    let staged = f.stage(first_mutation, 0, budget()).await?;
    let mutation = Uuid::new_v4();
    let before: i64 =
        sqlx::query_scalar("SELECT encoded_bytes FROM marketplace_installation_budget")
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(
        cancel(
            &f.db,
            cancellation(&f, &staged, mutation)?,
            InstallBudget {
                entries: 1,
                bytes: before as u64
            }
        )
        .await?
        .revision(),
        2
    );
    assert_eq!(f.counts().await?, (1, 0, 1));
    let state: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_state")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(state, 0);
    let after: i64 =
        sqlx::query_scalar("SELECT encoded_bytes FROM marketplace_installation_budget")
            .fetch_one(&f.db.pool)
            .await?;
    assert!(
        after < before,
        "compact cancellation must reclaim source-record metadata"
    );
    assert!(f.stage(first_mutation, 0, budget()).await.is_err());
    assert_eq!(
        cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
            .await?
            .revision(),
        2
    );
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
        cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
            .await?
            .revision(),
        2
    );
    let mut revision = 2;
    for _ in 0..8 {
        let next = f.stage(Uuid::new_v4(), revision, budget()).await?;
        assert_eq!(next.revision(), revision + 1);
        assert_ne!(
            next.instance(),
            staged.instance(),
            "never resurrect cancelled instance IDs"
        );
        assert_eq!(f.counts().await?, (1, 1, 1));
        // An old exact retry is no longer acknowledged after a newer action;
        // it must never cancel that action's fresh candidate.
        assert!(
            cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
                .await
                .is_err()
        );
        assert_eq!(f.counts().await?, (1, 1, 1));
        revision = cancel(&f.db, cancellation(&f, &next, Uuid::new_v4())?, budget())
            .await?
            .revision();
        assert_eq!(f.counts().await?, (1, 0, 1));
        let ledger: (i64, i64) =
            sqlx::query_as("SELECT entries,encoded_bytes FROM marketplace_installation_budget")
                .fetch_one(&f.db.pool)
                .await?;
        assert_eq!(ledger.0, 1, "one identity, no per-attempt history");
        assert!(
            ledger.1 <= after + 16,
            "only bounded revision digits may grow"
        );
    }
    assert_eq!(
        f.material()?.review.release_id(),
        staged.release(),
        "cancel never deletes retained source"
    );
    sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,'unexpected-resurrection',FALSE)")
        .bind(staged.instance().to_string()).bind(staged.extension().as_str()).execute(&f.db.pool).await?;
    let newest: (String, i64) =
        sqlx::query_as("SELECT record_json,revision FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?;
    assert!(f.stage(Uuid::new_v4(), revision, budget()).await.is_err());
    assert_eq!(
        f.counts().await?,
        (1, 1, 1),
        "resurrection is never silently repaired"
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>(
            "SELECT record_json,revision FROM marketplace_installations"
        )
        .fetch_one(&f.db.pool)
        .await?,
        newest
    );
    Ok(())
}

#[tokio::test]
async fn old_staging_records_decode_without_rewrite_and_tombstones_cannot_also_be_pending()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
    let json: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
        .fetch_one(&f.db.pool)
        .await?;
    let old: Journal = canonical::decode(json.as_bytes(), record::MAX_RECORD_BYTES)?;
    old.validate()?;
    assert!(
        !json.contains("cancelled"),
        "unchanged legacy staging shape"
    );
    assert_eq!(String::from_utf8(canonical::to_vec(&old)?)?, json);
    let mut value: serde_json::Value = serde_json::from_str(&json)?;
    value["cancelled"] = serde_json::Value::Null;
    assert!(
        canonical::decode::<Journal>(&canonical::to_vec(&value)?, record::MAX_RECORD_BYTES)
            .is_err(),
        "no silent explicit-null normalization"
    );
    cancel(&f.db, cancellation(&f, &staged, Uuid::new_v4())?, budget()).await?;
    let json: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
        .fetch_one(&f.db.pool)
        .await?;
    let mut tombstone: serde_json::Value = serde_json::from_str(&json)?;
    tombstone["pending"] = value["pending"].clone();
    let hybrid: Journal =
        canonical::decode(&canonical::to_vec(&tombstone)?, record::MAX_RECORD_BYTES)?;
    assert!(
        hybrid.validate().is_err(),
        "a tombstone is not a pending/executable capsule"
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_or_withdrawn_source_and_revoked_or_unparseable_grant_do_not_block_cleanup()
-> Result<()> {
    for fault in ["offline", "withdrawn", "revoked", "grant_json"] {
        let f = Fixture::new().await?;
        let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
        (f.meta_owner)(false);
        (f.public_owner)(false);
        if fault == "withdrawn" {
            let d = &f.descriptor;
            let revocation = SignedRecord::sign(
                elixir_marketplace::records::Revocation {
                    root_key: d.publisher.payload.root_key,
                    extension_id: d.release.payload.extension_id.clone(),
                    release_ids: vec![staged.release()],
                    release_keys: vec![],
                    reason: "offline cancellation fixture withdrawal".into(),
                },
                None,
                &SigningKey::from_bytes(&[1; 32]),
            )?;
            assert!(
                security_state::fixture_observe_signed(
                    &f.db,
                    elixir_marketplace::security_job::SecurityRecords {
                        schema_version: 1,
                        extension_id: staged.extension().clone(),
                        publisher: d.publisher.clone(),
                        head: Some(d.head.clone()),
                        revocations: vec![revocation],
                    },
                )
                .await?
            );
        }
        if fault == "revoked" || fault == "grant_json" {
            super::super::super::revoke_as(
                &f.db,
                &f.actor,
                staged.extension(),
                1,
                Uuid::new_v4(),
                Utc::now(),
            )
            .await?;
        }
        if fault == "grant_json" {
            sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
                .execute(&f.db.pool)
                .await?;
        }
        let mutation = Uuid::new_v4();
        assert_eq!(
            cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
                .await?
                .revision(),
            2,
            "{fault}"
        );
        assert_eq!(
            cancel(&f.db, cancellation(&f, &staged, mutation)?, budget())
                .await?
                .revision(),
            2,
            "{fault}"
        );
        assert_eq!(f.counts().await?, (1, 0, 1), "{fault}");
        let grant: (i64, i64, i64) = sqlx::query_as("SELECT revision,revocation_revision,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants")
            .fetch_one(&f.db.pool).await?;
        assert_eq!(
            grant,
            if matches!(fault, "revoked" | "grant_json") {
                (2, 2, 0)
            } else {
                (1, 0, 1)
            },
            "cancellation never reapproves/revokes {fault}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_rejects_changed_authority_corruption_and_operational_dependants_atomically()
-> Result<()> {
    for fault in [
        "session",
        "capability",
        "home",
        "foreign_owner",
        "grant_home",
        "grant_epoch",
        "missing_budget",
        "budget",
        "journal",
        "missing_instance",
        "state",
        "state_values",
        "instance_enabled",
        "extension_enabled",
        "manifest",
        "kind",
        "signer",
        "config",
        "provider",
        "secret",
        "runtime_log",
        "extra_instance",
    ] {
        let f = Fixture::new().await?;
        let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
        let mut actor = f.actor.clone();
        match fault {
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='offline cancellation fixture'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "capability" => {
                sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "home" => {
                sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL").execute(&f.db.pool).await?;
            }
            "foreign_owner" => {
                actor = super::super::super::tests::owner(&f.db).await?;
            }
            "grant_home" => {
                let other = super::super::super::tests::owner(&f.db).await?;
                sqlx::query("UPDATE marketplace_extension_grants SET home_id=$1")
                    .bind(other.home.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "grant_epoch" => {
                sqlx::query(
                    "UPDATE marketplace_extension_grants SET revocation_revision=revision+1",
                )
                .execute(&f.db.pool)
                .await?;
            }
            "missing_budget" => {
                sqlx::query("DELETE FROM marketplace_installation_budget")
                    .execute(&f.db.pool)
                    .await?;
            }
            "budget" => {
                sqlx::query("UPDATE marketplace_installation_budget SET entries=0,encoded_bytes=0")
                    .execute(&f.db.pool)
                    .await?;
            }
            "journal" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"bad").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "missing_instance" => {
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
                sqlx::query("INSERT INTO marketplace_instance_sdk_values (instance_id,key,revision,value_encrypted,encoded_bytes) VALUES ($1,'unexpected',1,'bad',3)").bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "instance_enabled" => {
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
            "config" => {
                sqlx::query("UPDATE extension_instances SET config_json='{}' WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "provider" => {
                sqlx::query("INSERT INTO providers (provider_id,instance_id,capability,cardinality) VALUES ($1,$2,'unexpected','one')").bind(Uuid::new_v4().to_string()).bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "secret" => {
                sqlx::query("INSERT INTO secrets (secret_id,scope,scope_id,key,value_encrypted) VALUES ($1,'instance',$2,'unexpected','preserve me')").bind(Uuid::new_v4().to_string()).bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "runtime_log" => {
                sqlx::query("INSERT INTO runtime_logs (log_id,instance_id,log_uri) VALUES ($1,$2,'fixture://preserve-me')").bind(Uuid::new_v4().to_string()).bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "extra_instance" => {
                sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,'unknown',FALSE)").bind(Uuid::new_v4().to_string()).bind(staged.extension().as_str()).execute(&f.db.pool).await?;
            }
            _ => unreachable!(),
        }
        let before = f.counts().await?;
        let dependants: (i64,i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM providers),(SELECT COUNT(*) FROM secrets),(SELECT COUNT(*) FROM runtime_logs),(SELECT COUNT(*) FROM marketplace_instance_sdk_state),(SELECT COUNT(*) FROM marketplace_instance_sdk_values)")
            .fetch_one(&f.db.pool).await?;
        let record: (String, i64) =
            sqlx::query_as("SELECT record_json,revision FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?;
        let ledger: Option<(i64, i64, i64)> = sqlx::query_as(
            "SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget",
        )
        .fetch_optional(&f.db.pool)
        .await?;
        let intent = OwnerCancelIntent::for_actor(actor, &staged, Uuid::new_v4())?;
        assert!(cancel(&f.db, intent, budget()).await.is_err(), "{fault}");
        assert_eq!(
            f.counts().await?,
            before,
            "{fault}: do not delete or repair"
        );
        assert_eq!(sqlx::query_as::<_,(i64,i64,i64,i64,i64)>("SELECT (SELECT COUNT(*) FROM providers),(SELECT COUNT(*) FROM secrets),(SELECT COUNT(*) FROM runtime_logs),(SELECT COUNT(*) FROM marketplace_instance_sdk_state),(SELECT COUNT(*) FROM marketplace_instance_sdk_values)")
            .fetch_one(&f.db.pool).await?, dependants, "{fault}: preserve operational/private rows");
        assert_eq!(
            sqlx::query_as::<_, (String, i64)>(
                "SELECT record_json,revision FROM marketplace_installations"
            )
            .fetch_one(&f.db.pool)
            .await?,
            record,
            "{fault}"
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget"
            )
            .fetch_optional(&f.db.pool)
            .await?,
            ledger,
            "{fault}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn mutation_reuse_wrong_targets_and_stale_cancellation_cannot_remove_a_candidate()
-> Result<()> {
    let f = Fixture::new().await?;
    let stage_mutation = Uuid::new_v4();
    let staged = f.stage(stage_mutation, 0, budget()).await?;
    assert!(
        cancel(&f.db, cancellation(&f, &staged, stage_mutation)?, budget())
            .await
            .is_err()
    );
    let mutation = Uuid::new_v4();
    for (instance, release, revision) in [
        (Uuid::new_v4(), staged.release(), 1),
        (staged.instance(), Digest::of(b"other release"), 1),
        (staged.instance(), staged.release(), 2),
    ] {
        let wrong = StagedInstallation {
            choices: None,
            configured_instance: None,
            extension: staged.extension().clone(),
            instance,
            release,
            revision,
        };
        assert!(
            cancel(&f.db, cancellation(&f, &wrong, mutation)?, budget())
                .await
                .is_err()
        );
        assert_eq!(f.counts().await?, (1, 1, 1));
    }
    cancel(&f.db, cancellation(&f, &staged, mutation)?, budget()).await?;
    let wrong = StagedInstallation {
        choices: None,
        configured_instance: None,
        extension: staged.extension().clone(),
        instance: Uuid::new_v4(),
        release: staged.release(),
        revision: 1,
    };
    assert!(
        cancel(&f.db, cancellation(&f, &wrong, mutation)?, budget())
            .await
            .is_err()
    );
    assert!(
        f.stage(mutation, 2, budget()).await.is_err(),
        "mutation IDs cannot switch action kind"
    );
    let next = f.stage(Uuid::new_v4(), 2, budget()).await?;
    let stale = StagedInstallation {
        choices: None,
        configured_instance: None,
        extension: staged.extension().clone(),
        instance: staged.instance(),
        release: staged.release(),
        revision: next.revision(),
    };
    assert!(
        cancel(&f.db, cancellation(&f, &stale, Uuid::new_v4())?, budget())
            .await
            .is_err()
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn competing_cancellations_have_one_head_and_exact_winner_retry_is_safe() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, budget()).await?;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let (left, right) = tokio::join!(
        cancel(&f.db, cancellation(&f, &staged, a)?, budget()),
        cancel(&f.db, cancellation(&f, &staged, b)?, budget())
    );
    let winner = match (left, right) {
        (Ok(_), Err(_)) => a,
        (Err(_), Ok(_)) => b,
        _ => bail!("only one different cancellation may commit"),
    };
    assert_eq!(
        cancel(&f.db, cancellation(&f, &staged, winner)?, budget())
            .await?
            .revision(),
        2
    );
    assert_eq!(f.counts().await?, (1, 0, 1));
    Ok(())
}
