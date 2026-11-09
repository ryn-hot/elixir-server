//! Real SQL ownership/FK primitives, not runtime/activation/GC authorization.
use super::*;
use crate::db::models::SecretScope;
use crate::extensions::store::{ExtensionStore, NewSecret};
use crate::marketplace::grant_store::tests as grant_tests;

#[test]
fn encrypted_v1_identity_coordinates_remain_compatible_but_cannot_be_rebound_to_a_generation_owner()
-> Result<()> {
    let extension = crate::marketplace::grants::fixture_review()?
        .scope()
        .extension_id()
        .clone();
    let instance = Uuid::new_v4();
    let home = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let mut envelope = Envelope {
        schema_version: 1,
        home_id: home,
        extension_id: extension.clone(),
        instance_id: instance,
        owner_instance_id: None,
        key: "cursor".into(),
        revision: 1,
        value: Value::Null,
    };
    envelope.require_coordinates(home, &extension, instance, instance)?;
    assert!(
        envelope
            .require_coordinates(home, &extension, instance, owner)
            .is_err()
    );
    assert!(
        !serde_json::to_value(&envelope)?
            .as_object()
            .unwrap()
            .contains_key("owner_instance_id")
    );
    envelope.schema_version = 2;
    envelope.owner_instance_id = Some(owner);
    envelope.require_coordinates(home, &extension, instance, owner)?;
    assert!(
        envelope
            .require_coordinates(home, &extension, instance, instance)
            .is_err()
    );
    assert!(
        envelope
            .require_coordinates(Uuid::new_v4(), &extension, instance, owner)
            .is_err()
    );
    Ok(())
}

struct Fixture {
    _dir: tempfile::TempDir,
    db: Database,
    home: Uuid,
    extension: ExtensionId,
    owners: [Uuid; 2],
}
impl Fixture {
    async fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db = grant_tests::connect(&dir.path().join("namespaces.sqlite")).await?;
        let actor = grant_tests::owner(&db).await?;
        let extension = crate::marketplace::grants::fixture_review()?
            .scope()
            .extension_id()
            .clone();
        sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES($1,'namespace primitives','1.0.0','module','community','{}',FALSE)")
            .bind(extension.as_str()).execute(&db.pool).await?;
        let owners = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, owner) in owners.iter().enumerate() {
            sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,config_json,enabled) VALUES($1,$2,$3,$4,FALSE)")
                .bind(owner.to_string()).bind(extension.as_str()).bind(format!("configured-{index}"))
                .bind(r#"{"setting":true}"#).execute(&db.pool).await?;
        }
        Ok(Self {
            _dir: dir,
            db,
            home: actor.home,
            extension,
            owners,
        })
    }
    fn coordinates(&self, instance: Uuid, namespace: Uuid) -> Coordinates<'_> {
        Coordinates {
            home: self.home,
            extension: &self.extension,
            instance,
            namespace,
        }
    }
    async fn create(&self, instance: Uuid, namespace: Uuid, purpose: Purpose) -> Result<()> {
        let mut tx = self.db.pool.begin().await?;
        initialize(
            &mut tx,
            self.db.driver,
            self.coordinates(instance, namespace),
            purpose,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[tokio::test]
async fn validation_uses_the_stable_instance_index_not_an_extension_wide_scan() -> Result<()> {
    let f = Fixture::new().await?;
    let namespace = Uuid::new_v4();
    f.create(f.owners[0], namespace, Purpose::Installed).await?;
    f.create(f.owners[1], Uuid::new_v4(), Purpose::Installed)
        .await?;
    let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {OWNERSHIP_SQL}"))
        .bind(namespace.to_string())
        .bind(f.owners[0].to_string())
        .bind(f.extension.as_str())
        .bind(f.home.to_string())
        .bind(Purpose::Installed.as_str())
        .fetch_all(&f.db.pool)
        .await?;
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.try_get("detail"))
        .collect::<std::result::Result<_, _>>()?;
    assert!(
        plan.iter()
            .any(|line| line.contains("marketplace_sdk_namespaces_instance")
                && line.contains("instance_id=?")),
        "{plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|line| line.contains("marketplace_sdk_namespaces_extension_purpose")),
        "{plan:?}"
    );
    let mut tx = f.db.pool.begin().await?;
    require(
        &mut tx,
        f.coordinates(f.owners[0], namespace),
        Purpose::Installed,
    )
    .await?;
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn configured_instances_have_independent_bounded_generations_without_replacing_operational_rows()
-> Result<()> {
    let f = Fixture::new().await?;
    let generations = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    assert!(
        f.create(f.owners[0], f.owners[1], Purpose::Installed)
            .await
            .is_err(),
        "private generations must not alias a different configured instance's secret scope"
    );
    for namespace in generations {
        f.create(f.owners[0], namespace, Purpose::Installed).await?;
    }
    assert!(
        f.create(f.owners[0], Uuid::new_v4(), Purpose::Installed)
            .await
            .is_err()
    );
    let other = Uuid::new_v4();
    f.create(f.owners[1], other, Purpose::Installed).await?;
    let mut tx = f.db.pool.begin().await?;
    require(
        &mut tx,
        f.coordinates(f.owners[1], other),
        Purpose::Installed,
    )
    .await?;
    assert!(
        require(
            &mut tx,
            f.coordinates(f.owners[0], other),
            Purpose::Installed
        )
        .await
        .is_err()
    );
    assert!(
        require_identity(
            &mut tx,
            f.home,
            &f.extension,
            generations[0],
            Purpose::Installed
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM extension_instances),(SELECT COUNT(*) FROM marketplace_sdk_namespaces),(SELECT COUNT(*) FROM marketplace_instance_sdk_state)")
        .fetch_one(&f.db.pool).await?;
    assert_eq!(counts, (2, 4, 4));
    Ok(())
}

#[tokio::test]
async fn deleting_only_a_private_namespace_cannot_cascade_into_configured_instance_or_its_dependants()
-> Result<()> {
    let f = Fixture::new().await?;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    f.create(f.owners[0], first, Purpose::Installed).await?;
    f.create(f.owners[0], second, Purpose::Installed).await?;
    let provider = Uuid::new_v4();
    let media = Uuid::new_v4();
    sqlx::query("INSERT INTO providers(provider_id,instance_id,capability,cardinality) VALUES($1,$2,'media.fixture','many')")
        .bind(provider.to_string()).bind(f.owners[0].to_string()).execute(&f.db.pool).await?;
    sqlx::query("INSERT INTO media_items(id,type,title) VALUES($1,'movie','preserved media')")
        .bind(media.to_string())
        .execute(&f.db.pool)
        .await?;
    sqlx::query("INSERT INTO media_ownerships(ownership_id,media_item_id,owner_type,owner_provider_id,owner_instance_id,owner_extension_id) VALUES($1,$2,'extension',$3,$4,$5)")
        .bind(Uuid::new_v4().to_string()).bind(media.to_string()).bind(provider.to_string())
        .bind(f.owners[0].to_string()).bind(f.extension.as_str()).execute(&f.db.pool).await?;
    for namespace in [first, second] {
        sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'key',1,'structural-fixture',18)")
            .bind(namespace.to_string()).execute(&f.db.pool).await?;
    }
    // Deliberately raw SQL: only proves FK direction, NOT authorized cleanup.
    sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1")
        .bind(first.to_string())
        .execute(&f.db.pool)
        .await?;
    let counts:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM extension_instances),(SELECT COUNT(*) FROM providers),(SELECT COUNT(*) FROM media_ownerships),(SELECT COUNT(*) FROM marketplace_instance_sdk_state),(SELECT COUNT(*) FROM marketplace_instance_sdk_values)")
        .fetch_one(&f.db.pool).await?;
    assert_eq!(counts, (2, 1, 1, 1, 1));
    let ownership: (String, String) =
        sqlx::query_as("SELECT owner_instance_id,owner_provider_id FROM media_ownerships")
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(ownership, (f.owners[0].to_string(), provider.to_string()));
    let config: String =
        sqlx::query_scalar("SELECT config_json FROM extension_instances WHERE instance_id=$1")
            .bind(f.owners[0].to_string())
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(config, r#"{"setting":true}"#);
    Ok(())
}

#[tokio::test]
async fn purpose_scope_corruption_and_orphan_markers_never_become_foreground_or_generic_secret_authority()
-> Result<()> {
    let f = Fixture::new().await?;
    let namespace = Uuid::new_v4();
    f.create(f.owners[0], namespace, Purpose::Installed).await?;
    assert!(
        f.create(f.owners[0], f.owners[0], Purpose::Foreground)
            .await
            .is_err()
    );
    assert!(
        f.create(f.owners[1], Uuid::new_v4(), Purpose::Foreground)
            .await
            .is_err()
    );
    f.create(f.owners[1], f.owners[1], Purpose::Foreground)
        .await?;
    let foreign_home = grant_tests::owner(&f.db).await?.home;
    for fault in ["owner", "extension", "home", "purpose", "root"] {
        let mut tx = f.db.pool.begin().await?;
        match fault {
            "owner" => {
                sqlx::query(
                    "UPDATE marketplace_sdk_namespaces SET instance_id=$1 WHERE namespace_id=$2",
                )
                .bind(f.owners[1].to_string())
                .bind(namespace.to_string())
                .execute(&mut *tx)
                .await?;
            }
            "extension" => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET extension_id='foreign' WHERE instance_id=$1")
                .bind(namespace.to_string()).execute(&mut *tx).await?;
            }
            "home" => {
                sqlx::query(
                    "UPDATE marketplace_instance_sdk_state SET home_id=$1 WHERE instance_id=$2",
                )
                .bind(foreign_home.to_string())
                .bind(namespace.to_string())
                .execute(&mut *tx)
                .await?;
            }
            "purpose" => {
                sqlx::query("UPDATE marketplace_sdk_namespaces SET purpose='foreground' WHERE namespace_id=$1")
                .bind(namespace.to_string()).execute(&mut *tx).await?;
            }
            _ => {
                sqlx::query("DELETE FROM marketplace_instance_sdk_state WHERE instance_id=$1")
                    .bind(namespace.to_string())
                    .execute(&mut *tx)
                    .await?;
            }
        }
        assert!(
            require(
                &mut tx,
                f.coordinates(f.owners[0], namespace),
                Purpose::Installed
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
    }
    sqlx::query("DELETE FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(namespace.to_string())
        .execute(&f.db.pool)
        .await?;
    let store = ExtensionStore::new(&f.db.pool);
    for id in [namespace, f.owners[0]] {
        assert!(
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(id),
                    key: "private".into(),
                    value_encrypted: "not written".into(),
                    rotatable: false
                })
                .await
                .is_err()
        );
    }
    for id in [namespace, f.owners[0], Uuid::new_v4()] {
        sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'preserved','structural fixture',FALSE)")
            .bind(Uuid::new_v4().to_string()).bind(id.to_string()).execute(&f.db.pool).await?;
    }
    assert_eq!(store.prune_orphaned_instance_secrets().await?, 1);
    let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(
        retained, 2,
        "a registered generation is not an orphaned operational instance"
    );
    let mut tx = f.db.pool.begin().await?;
    assert!(
        initialize(
            &mut tx,
            f.db.driver,
            Coordinates {
                home: Uuid::new_v4(),
                ..f.coordinates(f.owners[0], Uuid::new_v4())
            },
            Purpose::Installed
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    Ok(())
}
