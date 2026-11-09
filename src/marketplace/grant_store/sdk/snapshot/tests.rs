//! Bounded primitive tests only, NOT installation/native authorization. The
//! integration suite independently exercises the real controller transaction.
use super::*;
use crate::marketplace::grant_store::tests as grant_tests;
use serde_json::json;

struct Fixture {
    _dir: tempfile::TempDir,
    db: Database,
    home: Uuid,
    review: GrantReview,
    owner: Uuid,
    source: Uuid,
    target: Uuid,
    manager: SecretsManager,
}
impl Fixture {
    async fn new(quota: u64) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db = grant_tests::connect(&dir.path().join("copy.sqlite")).await?;
        let actor = grant_tests::owner(&db).await?;
        let review = review(quota)?;
        sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES($1,'snapshot primitive','1.0.0','module','community','{}',FALSE)")
            .bind(review.scope().extension_id().as_str()).execute(&db.pool).await?;
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'stable configured instance',FALSE)")
            .bind(owner.to_string()).bind(review.scope().extension_id().as_str()).execute(&db.pool).await?;
        for instance in [source, target] {
            let mut tx = db.pool.begin().await?;
            namespaces::initialize(
                &mut tx,
                db.driver,
                namespaces::Coordinates {
                    home: actor.home,
                    extension: review.scope().extension_id(),
                    instance: owner,
                    namespace: instance,
                },
                namespaces::Purpose::Installed,
            )
            .await?;
            tx.commit().await?;
        }
        Ok(Self {
            _dir: dir,
            db,
            home: actor.home,
            review,
            owner,
            source,
            target,
            manager: SecretsManager::from_key_bytes([93; 32], false),
        })
    }
    fn scope<'a>(&'a self, instance: Uuid, review: &'a GrantReview) -> Namespace<'a> {
        Namespace {
            home: self.home,
            extension: self.review.scope().extension_id(),
            instance,
            owner: self.owner,
            review,
        }
    }
    async fn seed(&self, count: usize, value: Value) -> Result<()> {
        let mut tx = self.db.pool.begin().await?;
        let mut bytes = 0;
        for i in 0..count {
            let key = format!("key-{i:03}");
            let envelope = Envelope {
                schema_version: 2,
                owner_instance_id: Some(self.owner),
                home_id: self.home,
                extension_id: self.review.scope().extension_id().clone(),
                instance_id: self.source,
                key: key.clone(),
                revision: 7,
                value: value.clone(),
            };
            let cipher = self.manager.encrypt(&serde_json::to_string(&envelope)?)?;
            bytes += cipher.len() as i64;
            sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,$2,7,$3,$4)")
                .bind(self.source.to_string()).bind(key).bind(&cipher).bind(cipher.len() as i64).execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=7,key_count=$1,encoded_bytes=$2 WHERE instance_id=$3")
            .bind(count as i64).bind(bytes).bind(self.source.to_string()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn empty_target(&self) -> Result<()> {
        let head:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(self.target.to_string()).fetch_one(&self.db.pool).await?;
        assert_eq!(head, (1, 0, 0));
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1",
        )
        .bind(self.target.to_string())
        .fetch_one(&self.db.pool)
        .await?;
        assert_eq!(count, 0);
        Ok(())
    }
}
fn review(quota: u64) -> Result<GrantReview> {
    let mut value = serde_json::to_value(crate::marketplace::grants::fixture_review()?)?;
    value["scope"]["runtime"]["state_bytes"] = json!(quota);
    Ok(serde_json::from_value(value)?)
}

#[tokio::test]
async fn owner_configured_copy_seals_set_and_remove_without_mutating_the_source_credentials()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let key = f
        .review
        .scope()
        .secret_keys()
        .first()
        .context("fixture key")?;
    let old = generation_secrets::seal(&f.manager, source.coordinates(), key, "original-private")?;
    sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,TRUE)")
        .bind(f.source.to_string()).bind(key).bind(Uuid::new_v4().to_string()).bind(&old).bind(old.len() as i64).execute(&f.db.pool).await?;
    let changed = generation_secrets::seal(&f.manager, target.coordinates(), key, "new-private")?;
    for change in [
        generation_secrets::PreparedChange::Set {
            key: key.clone(),
            cipher: changed,
            rotatable: false,
        },
        generation_secrets::PreparedChange::Remove { key: key.clone() },
    ] {
        let is_set = matches!(change, generation_secrets::PreparedChange::Set { .. });
        let mut tx = f.db.pool.begin().await?;
        let original = fingerprint(&mut tx, f.db.driver, source).await?;
        let seal = copy_with_credentials(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            target,
            Some(&[change]),
        )
        .await?;
        assert!(seal.source == Some(original.clone()));
        assert!(fingerprint(&mut tx, f.db.driver, source).await? == original);
        let actual =
            generation_secrets::read(&mut tx, &f.manager, target.coordinates(), key).await?;
        if is_set {
            assert_eq!(&**actual.context("configured key")?, "new-private");
        } else {
            assert!(actual.is_none());
        }
        audit_migrated(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            target,
            &seal,
        )
        .await?;
        discard(&mut tx, f.db.driver, Some(source), target, &seal).await?;
        tx.commit().await?;
        f.empty_target().await?;
        let unchanged: String = sqlx::query_scalar(
            "SELECT value_encrypted FROM marketplace_generation_secrets WHERE namespace_id=$1",
        )
        .bind(f.source.to_string())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(unchanged, old);
    }
    Ok(())
}

#[tokio::test]
async fn owner_credential_context_or_declared_key_mismatch_rolls_back_the_whole_state_copy()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    f.seed(1, json!("unchanged source state")).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let key = f
        .review
        .scope()
        .secret_keys()
        .first()
        .context("fixture key")?;
    for fault in ["namespace", "key", "undeclared"] {
        let wrong = SecretsManager::from_key_bytes([94; 32], false);
        let cipher = generation_secrets::seal(
            if fault == "key" { &wrong } else { &f.manager },
            if fault == "namespace" {
                source.coordinates()
            } else {
                target.coordinates()
            },
            key,
            "private-not-echoed",
        )?;
        let changes = [generation_secrets::PreparedChange::Set {
            key: if fault == "undeclared" {
                "not_declared".into()
            } else {
                key.clone()
            },
            cipher,
            rotatable: false,
        }];
        let mut tx = f.db.pool.begin().await?;
        let original = fingerprint(&mut tx, f.db.driver, source).await?;
        let error = copy_with_credentials(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            target,
            Some(&changes),
        )
        .await
        .err()
        .context("must reject invalid owner credential data")?;
        assert!(!format!("{error:#}").contains("private-not-echoed"));
        assert!(fingerprint(&mut tx, f.db.driver, source).await? == original);
        tx.rollback().await?;
        f.empty_target().await?;
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1",
        )
        .bind(f.target.to_string())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(rows, 0);
    }
    Ok(())
}

#[tokio::test]
async fn credential_setup_uses_the_one_mib_request_bound_not_the_larger_json_envelope() -> Result<()>
{
    assert_eq!(generation_secrets::MAX_SETUP_BYTES, 1024 * 1024);
    let f = Fixture::new(1024 * 1024).await?;
    // Bounds-only review data, not a source/permission authorization fixture.
    let mut expanded = serde_json::to_value(&f.review)?;
    expanded["scope"]["secret_keys"] =
        json!((0..12).map(|i| format!("setup_{i:02}")).collect::<Vec<_>>());
    let review: GrantReview = serde_json::from_value(expanded)?;
    let target = f.scope(f.target, &review);
    let mut changes = Vec::new();
    for key in review.scope().secret_keys() {
        changes.push(generation_secrets::PreparedChange::Set {
            key: key.clone(),
            cipher: generation_secrets::seal(
                &f.manager,
                target.coordinates(),
                key,
                &"x".repeat(65536),
            )?,
            rotatable: false,
        });
    }
    let mut tx = f.db.pool.begin().await?;
    let error = copy_with_credentials(
        &mut tx,
        f.db.driver,
        &f.manager,
        None,
        target,
        Some(&changes),
    )
    .await
    .err()
    .context("setup must enforce the 1MiB aggregate ciphertext budget")?;
    assert!(format!("{error:#}").contains("byte bound"));
    tx.rollback().await?;
    f.empty_target().await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn full_128_key_64k_values_copy_in_bounded_batches_and_original_remains_identical()
-> Result<()> {
    let f = Fixture::new(16 * 1024 * 1024).await?;
    let value = json!("x".repeat(65534));
    assert_eq!(RuntimeJson::serialized_len(&value)?, 65536);
    f.seed(128, value).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let mut tx = f.db.pool.begin().await?;
    let original = fingerprint(&mut tx, f.db.driver, source).await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    assert_eq!(seal.target.state.count, 128);
    assert!(seal.source == Some(original.clone()));
    assert!(fingerprint(&mut tx, f.db.driver, source).await? == original);
    assert!(
        copy(&mut tx, f.db.driver, &f.manager, Some(source), target)
            .await
            .is_err()
    );
    verify(&mut tx, f.db.driver, Some(source), target, &seal).await?;
    let audited = audit_migrated(
        &mut tx,
        f.db.driver,
        &f.manager,
        Some(source),
        target,
        &seal,
    )
    .await?;
    assert_eq!(audited.state.count, 128);
    assert!(audited == seal.target);
    discard(&mut tx, f.db.driver, Some(source), target, &seal).await?;
    assert!(fingerprint(&mut tx, f.db.driver, source).await? == original);
    tx.commit().await?;
    f.empty_target().await
}

#[tokio::test]
async fn generation_seals_bind_the_stable_owner_even_if_a_caller_supplies_changed_coordinates()
-> Result<()> {
    let f = Fixture::new(16 * 1024 * 1024).await?;
    f.seed(1, json!("preserved")).await?;
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'other configured instance',FALSE)")
        .bind(other.to_string()).bind(f.review.scope().extension_id().as_str()).execute(&f.db.pool).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let mut tx = f.db.pool.begin().await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    sqlx::query("UPDATE marketplace_sdk_namespaces SET instance_id=$1 WHERE namespace_id=$2")
        .bind(other.to_string())
        .bind(f.target.to_string())
        .execute(&mut *tx)
        .await?;
    assert!(
        audit_migrated(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            target,
            &seal
        )
        .await
        .is_err()
    );
    assert!(
        audit_migrated(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            Namespace {
                owner: other,
                ..target
            },
            &seal
        )
        .await
        .is_err(),
        "a retained seal is tied to its original stable owner, not just state counters"
    );
    tx.rollback().await?;
    f.empty_target().await
}

#[tokio::test]
async fn generation_ciphertext_cannot_be_reassigned_by_rehashing_all_unencrypted_metadata()
-> Result<()> {
    let f = Fixture::new(16 * 1024 * 1024).await?;
    f.seed(1, json!("private original value")).await?;
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'other owner',FALSE)")
        .bind(other.to_string()).bind(f.review.scope().extension_id().as_str()).execute(&f.db.pool).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let changed = Namespace {
        owner: other,
        ..target
    };
    let mut tx = f.db.pool.begin().await?;
    copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    sqlx::query("UPDATE marketplace_sdk_namespaces SET instance_id=$1 WHERE namespace_id=$2")
        .bind(other.to_string())
        .bind(f.target.to_string())
        .execute(&mut *tx)
        .await?;
    // An attacker with SQL but not the encryption key can recompute every
    // public hash/counter. The authenticated encrypted owner must still fail.
    let forged = CopySeal {
        source: Some(fingerprint(&mut tx, f.db.driver, source).await?),
        target: fingerprint(&mut tx, f.db.driver, changed).await?,
    };
    verify_candidate(&mut tx, f.db.driver, Some(source), changed, &forged).await?;
    let error = audit_migrated(
        &mut tx,
        f.db.driver,
        &f.manager,
        Some(source),
        changed,
        &forged,
    )
    .await
    .err()
    .context("forged owner must fail plaintext audit")?;
    assert!(!format!("{error:#}").contains("private original value"));
    tx.rollback().await?;
    f.empty_target().await
}

#[tokio::test]
async fn migrated_audit_rejects_source_cipher_changes_even_with_unchanged_heads() -> Result<()> {
    let f = Fixture::new(16 * 1024 * 1024).await?;
    f.seed(1, json!("original")).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let mut tx = f.db.pool.begin().await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    let envelope = Envelope {
        schema_version: 2,
        owner_instance_id: Some(f.owner),
        home_id: f.home,
        extension_id: f.review.scope().extension_id().clone(),
        instance_id: f.source,
        key: "key-000".into(),
        revision: 7,
        value: json!("tampered"),
    };
    let cipher = f.manager.encrypt(&serde_json::to_string(&envelope)?)?;
    let old: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1",
    )
    .bind(f.source.to_string())
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(old.len(), cipher.len());
    sqlx::query(
        "UPDATE marketplace_instance_sdk_values SET value_encrypted=$1 WHERE instance_id=$2",
    )
    .bind(cipher)
    .bind(f.source.to_string())
    .execute(&mut *tx)
    .await?;
    verify_candidate(&mut tx, f.db.driver, Some(source), target, &seal).await?;
    assert!(
        audit_migrated(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            target,
            &seal
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn migrated_audit_validates_each_plaintext_identity_and_does_not_echo_it() -> Result<()> {
    for fault in [
        "home",
        "instance",
        "owner",
        "key",
        "revision",
        "schema",
        "extra",
        "key_manager",
    ] {
        let f = Fixture::new(16 * 1024 * 1024).await?;
        f.seed(1, json!("original")).await?;
        let source = f.scope(f.source, &f.review);
        let target = f.scope(f.target, &f.review);
        let mut tx = f.db.pool.begin().await?;
        let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
        let mut plain = serde_json::to_value(Envelope {
            schema_version: 2,
            owner_instance_id: Some(f.owner),
            home_id: f.home,
            extension_id: f.review.scope().extension_id().clone(),
            instance_id: f.target,
            key: "key-000".into(),
            revision: 2,
            value: json!("private-value-must-not-echo"),
        })?;
        match fault {
            "home" => plain["home_id"] = json!(Uuid::new_v4()),
            "instance" => plain["instance_id"] = json!(Uuid::new_v4()),
            "owner" => plain["owner_instance_id"] = json!(Uuid::new_v4()),
            "key" => plain["key"] = json!("different"),
            "revision" => plain["revision"] = json!(1),
            "schema" => plain["schema_version"] = json!(3),
            "extra" => plain["private-field-must-not-echo"] = json!(true),
            _ => {}
        }
        let cipher = f.manager.encrypt(&serde_json::to_string(&plain)?)?;
        sqlx::query("UPDATE marketplace_instance_sdk_values SET value_encrypted=$1,encoded_bytes=$2 WHERE instance_id=$3")
            .bind(&cipher).bind(cipher.len() as i64).bind(f.target.to_string()).execute(&mut *tx).await?;
        sqlx::query(
            "UPDATE marketplace_instance_sdk_state SET encoded_bytes=$1 WHERE instance_id=$2",
        )
        .bind(cipher.len() as i64)
        .bind(f.target.to_string())
        .execute(&mut *tx)
        .await?;
        verify_candidate(&mut tx, f.db.driver, Some(source), target, &seal).await?;
        let wrong = SecretsManager::from_key_bytes([94; 32], false);
        let manager = if fault == "key_manager" {
            &wrong
        } else {
            &f.manager
        };
        let error = audit_migrated(&mut tx, f.db.driver, manager, Some(source), target, &seal)
            .await
            .err()
            .context("expected full audit rejection")?;
        let message = format!("{error:#}");
        assert!(!message.contains("private-value-must-not-echo"));
        assert!(!message.contains("private-field-must-not-echo"));
        tx.rollback().await?;
    }
    Ok(())
}

#[tokio::test]
async fn lower_or_zero_candidate_quota_and_wrong_key_cannot_partially_commit() -> Result<()> {
    let f = Fixture::new(16 * 1024 * 1024).await?;
    f.seed(20, json!("x".repeat(65534))).await?;
    for quota in [0, 1024 * 1024] {
        let smaller = review(quota)?;
        let mut tx = f.db.pool.begin().await?;
        assert!(
            copy(
                &mut tx,
                f.db.driver,
                &f.manager,
                Some(f.scope(f.source, &f.review)),
                f.scope(f.target, &smaller)
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        f.empty_target().await?;
    }
    let wrong = SecretsManager::from_key_bytes([94; 32], false);
    let mut tx = f.db.pool.begin().await?;
    assert!(
        copy(
            &mut tx,
            f.db.driver,
            &wrong,
            Some(f.scope(f.source, &f.review)),
            f.scope(f.target, &f.review)
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    f.empty_target().await
}

#[tokio::test]
async fn zero_quota_empty_copy_is_allowed_but_no_cross_home_or_same_instance_adoption() -> Result<()>
{
    let f = Fixture::new(0).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let mut tx = f.db.pool.begin().await?;
    assert!(
        copy(&mut tx, f.db.driver, &f.manager, Some(source), source)
            .await
            .is_err()
    );
    assert!(
        copy(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(Namespace {
                home: Uuid::new_v4(),
                ..source
            }),
            target
        )
        .await
        .is_err()
    );
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    discard(&mut tx, f.db.driver, Some(source), target, &seal).await?;
    tx.commit().await?;
    f.empty_target().await
}

#[tokio::test]
async fn secret_namespace_bounds_and_intersection_never_copy_global_or_unreviewed_values()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    let mut old = serde_json::to_value(&f.review)?;
    old["scope"]["secret_keys"] = json!(
        (0..64)
            .map(|i| format!("secret_{i:02}"))
            .collect::<Vec<_>>()
    );
    let old: GrantReview = serde_json::from_value(old)?;
    let mut new = serde_json::to_value(&f.review)?;
    new["scope"]["secret_keys"] = json!(["secret_01", "secret_new"]);
    let new: GrantReview = serde_json::from_value(new)?;
    for i in 0..64 {
        let key = format!("secret_{i:02}");
        let cipher = generation_secrets::seal(
            &f.manager,
            f.scope(f.source, &old).coordinates(),
            &key,
            "private",
        )?;
        sqlx::query("INSERT INTO marketplace_generation_secrets(secret_id,namespace_id,key,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,FALSE)")
            .bind(Uuid::new_v4().to_string()).bind(f.source.to_string()).bind(key).bind(&cipher).bind(cipher.len() as i64).execute(&f.db.pool).await?;
    }
    sqlx::query("INSERT INTO secrets(secret_id,scope,key,value_encrypted,rotatable) VALUES($1,'global','secret_new',$2,FALSE)")
        .bind(Uuid::new_v4().to_string()).bind(f.manager.encrypt("global-forbidden")?).execute(&f.db.pool).await?;
    let source = f.scope(f.source, &old);
    let target = f.scope(f.target, &new);
    let mut tx = f.db.pool.begin().await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    assert_eq!(seal.source.as_ref().unwrap().secrets.count, 64);
    assert_eq!(seal.target.secrets.count, 1);
    let key: String =
        sqlx::query_scalar("SELECT key FROM marketplace_generation_secrets WHERE namespace_id=$1")
            .bind(f.target.to_string())
            .fetch_one(&mut *tx)
            .await?;
    assert_eq!(key, "secret_01");
    discard(&mut tx, f.db.driver, Some(source), target, &seal).await?;
    tx.commit().await?;
    let cipher = generation_secrets::seal(&f.manager, source.coordinates(), "secret_64", "extra")?;
    sqlx::query("INSERT INTO marketplace_generation_secrets(secret_id,namespace_id,key,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,'secret_64',$3,$4,FALSE)")
        .bind(Uuid::new_v4().to_string()).bind(f.source.to_string()).bind(&cipher).bind(cipher.len() as i64).execute(&f.db.pool).await?;
    let mut tx = f.db.pool.begin().await?;
    assert!(
        copy(&mut tx, f.db.driver, &f.manager, Some(source), target)
            .await
            .is_err()
    );
    tx.rollback().await?;
    f.empty_target().await
}

#[tokio::test]
async fn generation_secret_copy_rebinds_context_and_full_audit_rejects_a_relocated_ciphertext()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let key = f
        .review
        .scope()
        .secret_keys()
        .first()
        .context("fixture secret")?;
    let old = generation_secrets::seal(
        &f.manager,
        source.coordinates(),
        key,
        "private secret must not echo",
    )?;
    sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,TRUE)")
        .bind(f.source.to_string()).bind(key).bind(Uuid::new_v4().to_string()).bind(&old).bind(old.len() as i64).execute(&f.db.pool).await?;
    let mut tx = f.db.pool.begin().await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    let new: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM marketplace_generation_secrets WHERE namespace_id=$1",
    )
    .bind(f.target.to_string())
    .fetch_one(&mut *tx)
    .await?;
    assert_ne!(new, old);
    assert_eq!(
        &**generation_secrets::open(&f.manager, target.coordinates(), key, &new)?,
        "private secret must not echo"
    );
    assert!(generation_secrets::open(&f.manager, source.coordinates(), key, &new).is_err());
    audit_migrated(
        &mut tx,
        f.db.driver,
        &f.manager,
        Some(source),
        target,
        &seal,
    )
    .await?;
    // Recompute every public hash after moving an authentic source ciphertext.
    // The cryptographically bound namespace must still reject the relocation.
    sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted=$1,encoded_bytes=$2 WHERE namespace_id=$3")
        .bind(&old).bind(old.len() as i64).bind(f.target.to_string()).execute(&mut *tx).await?;
    let forged = CopySeal {
        source: seal.source,
        target: fingerprint(&mut tx, f.db.driver, target).await?,
    };
    verify_candidate(&mut tx, f.db.driver, Some(source), target, &forged).await?;
    let error = audit_migrated(
        &mut tx,
        f.db.driver,
        &f.manager,
        Some(source),
        target,
        &forged,
    )
    .await
    .err()
    .context("relocated secret must fail full audit")?;
    assert!(!format!("{error:#}").contains("private secret must not echo"));
    tx.rollback().await?;
    f.empty_target().await
}

#[tokio::test]
async fn legacy_to_generation_secret_copy_and_migrated_discard_preserve_all_legacy_addresses()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    let source = Namespace {
        instance: f.owner,
        ..f.scope(f.source, &f.review)
    };
    let target = f.scope(f.target, &f.review);
    let key = f
        .review
        .scope()
        .secret_keys()
        .first()
        .context("fixture secret")?;
    let mut tx = f.db.pool.begin().await?;
    namespaces::initialize(
        &mut tx,
        f.db.driver,
        source.coordinates(),
        namespaces::Purpose::Installed,
    )
    .await?;
    tx.commit().await?;
    for (kind, id) in [
        ("instance", Some(f.owner)),
        ("instance", Some(f.target)),
        ("provider", Some(f.owner)),
        ("global", None),
    ] {
        sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,$2,$3,$4,$5,TRUE)")
            .bind(Uuid::new_v4().to_string()).bind(kind).bind(id.map(|id|id.to_string())).bind(key).bind(f.manager.encrypt("preserved legacy secret")?).execute(&f.db.pool).await?;
    }
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT secret_id,value_encrypted FROM secrets ORDER BY secret_id")
            .fetch_all(&f.db.pool)
            .await?;
    let mut tx = f.db.pool.begin().await?;
    let seal = copy(&mut tx, f.db.driver, &f.manager, Some(source), target).await?;
    assert_eq!(seal.target.secrets.count, 1);
    let new: (String,i64) = sqlx::query_as("SELECT value_encrypted,CAST(CASE WHEN rotatable THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_generation_secrets WHERE namespace_id=$1")
        .bind(f.target.to_string()).fetch_one(&mut *tx).await?;
    assert_eq!(
        &**generation_secrets::open(&f.manager, target.coordinates(), key, &new.0)?,
        "preserved legacy secret"
    );
    assert_eq!(new.1, 1);
    assert!(f.manager.decrypt(&new.0).is_err());
    let envelope = Envelope {
        schema_version: 2,
        home_id: f.home,
        extension_id: target.extension.clone(),
        instance_id: f.target,
        owner_instance_id: Some(f.owner),
        key: "migration-cursor".into(),
        revision: 3,
        value: json!("partial migration data"),
    };
    let cipher = f.manager.encrypt(&serde_json::to_string(&envelope)?)?;
    sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'migration-cursor',3,$2,$3)")
        .bind(f.target.to_string()).bind(&cipher).bind(cipher.len() as i64).execute(&mut *tx).await?;
    sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=3,key_count=1,encoded_bytes=$1 WHERE instance_id=$2")
        .bind(cipher.len() as i64).bind(f.target.to_string()).execute(&mut *tx).await?;
    // Data primitive only: this fixture does NOT supply native retirement.
    discard_migrated(
        &mut tx,
        f.db.driver,
        &f.manager,
        Some(source),
        target,
        &seal,
    )
    .await?;
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT secret_id,value_encrypted FROM secrets ORDER BY secret_id")
            .fetch_all(&mut *tx)
            .await?;
    assert_eq!(after, before);
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(remaining, 0);
    let owners: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(owners, 1);
    tx.commit().await?;
    f.empty_target().await
}

#[tokio::test]
async fn secret_copy_rejects_cross_configured_owner_and_generation_to_legacy_downgrade()
-> Result<()> {
    let f = Fixture::new(1024 * 1024).await?;
    let source = f.scope(f.source, &f.review);
    let target = f.scope(f.target, &f.review);
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'other configured owner',FALSE)")
        .bind(other.to_string()).bind(source.extension.as_str()).execute(&f.db.pool).await?;
    let mut tx = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_sdk_namespaces SET instance_id=$1 WHERE namespace_id=$2")
        .bind(other.to_string())
        .bind(f.target.to_string())
        .execute(&mut *tx)
        .await?;
    assert!(
        copy(
            &mut tx,
            f.db.driver,
            &f.manager,
            Some(source),
            Namespace {
                owner: other,
                ..target
            }
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    let mut tx = f.db.pool.begin().await?;
    let legacy = Namespace {
        instance: f.owner,
        ..target
    };
    namespaces::initialize(
        &mut tx,
        f.db.driver,
        legacy.coordinates(),
        namespaces::Purpose::Installed,
    )
    .await?;
    assert!(
        copy(&mut tx, f.db.driver, &f.manager, Some(source), legacy)
            .await
            .is_err()
    );
    tx.rollback().await?;
    f.empty_target().await
}
