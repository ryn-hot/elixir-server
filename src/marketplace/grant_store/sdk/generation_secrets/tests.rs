//! Real SQL/crypto data primitives. No native/runtime/activation permit is
//! inferred from this fixture or an initialized private namespace.
use super::*;
use crate::extensions::store::ExtensionStore;
use crate::marketplace::grant_store::tests as grant_tests;
use serde_json::json;

struct Fixture {
    _dir: tempfile::TempDir,
    db: Database,
    grant: CommittedOwnerGrant,
    review: GrantReview,
    owners: [Uuid; 2],
    slots: [Uuid; 3],
    manager: SecretsManager,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db = grant_tests::connect(&dir.path().join("generation-secrets.sqlite")).await?;
        let actor = grant_tests::owner(&db).await?;
        let mut value = serde_json::to_value(crate::marketplace::grants::fixture_review()?)?;
        value["scope"]["secret_keys"] = json!(["api_key", "missing"]);
        value["scope"]["runtime"]["state_bytes"] = json!(1024 * 1024);
        let review: GrantReview = serde_json::from_value(value)?;
        let grant = super::super::super::approve_review(
            &db,
            &actor,
            &review,
            &grant_tests::selection(&review, 0)?,
            Utc::now(),
        )
        .await?;
        sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,manifest_json,enabled) VALUES($1,'generation secret primitive','1.0.0','module','{}',FALSE)")
            .bind(grant.extension_id.as_str()).execute(&db.pool).await?;
        let owners = [Uuid::new_v4(), Uuid::new_v4()];
        for owner in owners {
            sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,$3,FALSE)")
                .bind(owner.to_string()).bind(grant.extension_id.as_str()).bind(format!("configured-{owner}")).execute(&db.pool).await?;
        }
        let slots = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (namespace, instance) in slots.into_iter().zip([owners[0], owners[0], owners[1]]) {
            let mut tx = db.pool.begin().await?;
            namespaces::initialize(
                &mut tx,
                db.driver,
                Coordinates {
                    home: actor.home,
                    extension: &grant.extension_id,
                    instance,
                    namespace,
                },
                namespaces::Purpose::Installed,
            )
            .await?;
            tx.commit().await?;
        }
        Ok(Self {
            _dir: dir,
            db,
            grant,
            review,
            owners,
            slots,
            manager: SecretsManager::from_key_bytes([83; 32], false),
        })
    }
    fn scope(&self, index: usize) -> Coordinates<'_> {
        Coordinates {
            home: self.grant.home_id,
            extension: &self.grant.extension_id,
            instance: self.owners[usize::from(index == 2)],
            namespace: self.slots[index],
        }
    }
    async fn put(&self, index: usize, key: &str, value: &str) -> Result<String> {
        let cipher = seal(&self.manager, self.scope(index), key, value)?;
        self.put_cipher(index, key, &cipher).await?;
        Ok(cipher)
    }
    async fn put_cipher(&self, index: usize, key: &str, cipher: &str) -> Result<()> {
        sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,FALSE)")
            .bind(self.slots[index].to_string()).bind(key).bind(Uuid::new_v4().to_string()).bind(cipher).bind(cipher.len() as i64).execute(&self.db.pool).await?;
        Ok(())
    }
    async fn call(&self, index: usize, value: Value) -> Result<SourceSdkReply> {
        let raw = RuntimeJson::from_value(value)?;
        let request = SourceSdkRequest::parse(&raw)?;
        let mut tx = self.db.pool.begin().await?;
        let reply = super::super::perform(
            &mut tx,
            &self.manager,
            &self.grant,
            &self.review,
            self.scope(index),
            &request,
        )
        .await?;
        tx.commit().await?;
        Ok(reply)
    }
}
fn value(reply: SourceSdkReply) -> Result<Value> {
    let SourceSdkReply::Value(value) = reply else {
        bail!("value reply required");
    };
    Ok(serde_json::from_str(value.as_str())?)
}

#[test]
fn exact_context_and_raw_64k_control_values_are_bound_without_expanding_the_ciphertext_budget()
-> Result<()> {
    let review = crate::marketplace::grants::fixture_review()?;
    let other: ExtensionId = format!(
        "{}/other-fixture",
        review
            .scope()
            .extension_id()
            .as_str()
            .rsplit_once('/')
            .unwrap()
            .0
    )
    .parse()?;
    let scope = Coordinates {
        home: Uuid::new_v4(),
        extension: review.scope().extension_id(),
        instance: Uuid::new_v4(),
        namespace: Uuid::new_v4(),
    };
    let manager = SecretsManager::from_key_bytes([84; 32], true);
    let text = "\0".repeat(MAX_PLAINTEXT);
    let cipher = seal(&manager, scope, "api_key", &text)?;
    assert!(cipher.len() <= ENTRY_BYTES as usize);
    assert_eq!(&**open(&manager, scope, "api_key", &cipher)?, &text);
    for changed in [
        Coordinates {
            home: Uuid::new_v4(),
            ..scope
        },
        Coordinates {
            extension: &other,
            ..scope
        },
        Coordinates {
            instance: Uuid::new_v4(),
            ..scope
        },
        Coordinates {
            namespace: Uuid::new_v4(),
            ..scope
        },
    ] {
        assert!(open(&manager, changed, "api_key", &cipher).is_err());
    }
    assert!(open(&manager, scope, "other_key", &cipher).is_err());
    assert!(open(&manager, scope, "api_key", &manager.encrypt(&text)?).is_err());
    assert!(seal(&manager, scope, "api_key", &"x".repeat(MAX_PLAINTEXT + 1)).is_err());
    assert!(
        seal(
            &manager,
            Coordinates {
                namespace: scope.instance,
                ..scope
            },
            "api_key",
            "no legacy alias"
        )
        .is_err()
    );
    assert!(manager.decrypt(&cipher).is_err());
    Ok(())
}

#[tokio::test]
async fn sdk_reads_only_its_private_generation_store_and_rejects_cross_namespace_ciphertexts()
-> Result<()> {
    let f = Fixture::new().await?;
    let cipher = f.put(0, "api_key", "private generation value").await?;
    for (scope, id, key) in [
        ("instance", Some(f.slots[0]), "api_key"),
        ("instance", Some(f.slots[0]), "missing"),
        ("instance", Some(f.owners[0]), "missing"),
        ("provider", Some(f.owners[0]), "missing"),
        ("global", None, "missing"),
    ] {
        sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,$2,$3,$4,$5,FALSE)")
            .bind(Uuid::new_v4().to_string()).bind(scope).bind(id.map(|id|id.to_string())).bind(key).bind(f.manager.encrypt("legacy shadow must not leak")?).execute(&f.db.pool).await?;
    }
    assert_eq!(
        value(
            f.call(0, json!({"operation":"secret","key":"api_key"}))
                .await?
        )?,
        json!("private generation value")
    );
    assert_eq!(
        value(
            f.call(0, json!({"operation":"secret","key":"missing"}))
                .await?
        )?,
        Value::Null
    );
    assert!(matches!(
        f.call(0, json!({"operation":"secret","key":"undeclared"}))
            .await?,
        SourceSdkReply::Denied
    ));
    assert_eq!(
        value(
            f.call(1, json!({"operation":"secret","key":"api_key"}))
                .await?
        )?,
        Value::Null
    );
    f.put_cipher(1, "api_key", &cipher).await?;
    let error = f
        .call(1, json!({"operation":"secret","key":"api_key"}))
        .await
        .err()
        .context("ciphertext relocation must fail")?;
    assert!(!format!("{error:#}").contains("private generation value"));
    f.put_cipher(2, "api_key", &cipher).await?;
    assert!(
        f.call(2, json!({"operation":"secret","key":"api_key"}))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn sdk_state_uses_the_private_namespace_and_encrypts_its_stable_configured_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    assert_eq!(
        value(
            f.call(0, json!({"operation":"state_set","key":"cursor","value":7}))
                .await?
        )?,
        json!(true)
    );
    assert_eq!(
        value(
            f.call(0, json!({"operation":"state_get","key":"cursor"}))
                .await?
        )?,
        json!(7)
    );
    for index in [1, 2] {
        assert_eq!(
            value(
                f.call(index, json!({"operation":"state_get","key":"cursor"}))
                    .await?
            )?,
            Value::Null
        );
    }
    let cipher: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1",
    )
    .bind(f.slots[0].to_string())
    .fetch_one(&f.db.pool)
    .await?;
    let envelope: Value = serde_json::from_str(&f.manager.decrypt(&cipher)?)?;
    assert_eq!(envelope["schema_version"], json!(2));
    assert_eq!(envelope["owner_instance_id"], json!(f.owners[0]));
    assert_eq!(envelope["instance_id"], json!(f.slots[0]));
    sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'cursor',2,$2,$3)")
        .bind(f.slots[1].to_string()).bind(&cipher).bind(cipher.len() as i64).execute(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=2,key_count=1,encoded_bytes=$1 WHERE instance_id=$2")
        .bind(cipher.len() as i64).bind(f.slots[1].to_string()).execute(&f.db.pool).await?;
    assert!(
        f.call(1, json!({"operation":"state_get","key":"cursor"}))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn legacy_pruning_cannot_delete_private_generation_secrets_and_namespace_fk_preserves_the_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    f.put(0, "api_key", "original").await?;
    f.put(1, "api_key", "candidate").await?;
    let store = ExtensionStore::new(&f.db.pool);
    assert_eq!(store.prune_orphaned_instance_secrets().await?, 0);
    // SQL primitive/FK direction only, NOT evidence of native retirement.
    sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1")
        .bind(f.slots[1].to_string())
        .execute(&f.db.pool)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(count, 1);
    assert_eq!(
        value(
            f.call(0, json!({"operation":"secret","key":"api_key"}))
                .await?
        )?,
        json!("original")
    );
    let owners: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(owners, 2);
    Ok(())
}
