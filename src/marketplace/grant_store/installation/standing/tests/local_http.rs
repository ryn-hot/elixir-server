//! Real signed manifests/archive, SQL and bound encryption. Initial ACTIVE
//! enrollment is deliberately synthetic, NOT native/activation qualification.
use super::*;
use crate::marketplace::grant_store::sdk::{generation_secrets, local_http};

pub(in crate::marketplace::grant_store::installation) async fn configured(
    f: &Fixture,
    name: &str,
) -> Result<(Uuid, InstalledBinding)> {
    let staged = f
        .stage_configured(
            super::super::super::InstallTarget::New { name: name.into() },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let owner = staged.configured_instance.context("configured owner")?;
    let mut connection = crate::marketplace::durability::connection(&f.db).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, f.db.driver).await?;
    let ledger = store::lock_budget(&mut tx, limit()).await?;
    let old = store::load_configured(&mut tx, f.db.driver, staged.extension(), Some(owner))
        .await?
        .context("configured journal")?;
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    journal.active = journal.pending.take();
    journal.revision += 1;
    journal.mutation = Uuid::new_v4();
    journal.request = Digest::of(b"synthetic configured LAN restriction fixture");
    store::write(&mut tx, ledger, Some(&old), journal, old.floor, limit()).await?;
    sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
        .bind(staged.extension().as_str())
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(owner.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let held =
        InstalledBinding::load_configured(&f.db, f.actor.home, staged.extension(), owner, limit())
            .await?;
    Ok((owner, held))
}
pub(in crate::marketplace::grant_store::installation) async fn config(
    f: &Fixture,
    owner: Uuid,
    value: Value,
) -> Result<()> {
    sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
        .bind(serde_json::to_string(&value)?)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    Ok(())
}
pub(in crate::marketplace::grant_store::installation) async fn secret(
    f: &Fixture,
    held: &InstalledBinding,
    value: &str,
) -> Result<()> {
    let coordinates = held.coordinates();
    let cipher = generation_secrets::seal(&manager(), coordinates, "playlist_url", value)?;
    sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,'playlist_url',$2,$3,$4,FALSE) ON CONFLICT(namespace_id,key) DO UPDATE SET value_encrypted=excluded.value_encrypted,encoded_bytes=excluded.encoded_bytes")
        .bind(coordinates.namespace.to_string()).bind(Uuid::new_v4().to_string())
        .bind(&cipher).bind(cipher.len() as i64).execute(&f.db.pool).await?;
    Ok(())
}

#[tokio::test]
async fn configured_local_endpoints_require_an_actual_true_owner_toggle_and_current_instance()
-> Result<()> {
    let f = Fixture::with_lan().await?;
    let (owner, held) = configured(&f, "one").await?;
    let (_, other) = configured(&f, "two").await?;
    let raw = "http://nas.home.arpa:8080/player_api.php?action=live";
    for enable in [Value::Null, json!(false), json!("true"), json!(1)] {
        config(
            &f,
            owner,
            json!({"localNetwork":enable,"service_url":"http://nas.home.arpa:8080/"}),
        )
        .await?;
        assert!(
            local_http::load(&f.db, &manager(), &held, raw)
                .await?
                .is_none()
        );
    }
    config(
        &f,
        owner,
        json!({"localNetwork":true,"service_url":"http://nas.home.arpa:8080/"}),
    )
    .await?;
    let selected = local_http::load(&f.db, &manager(), &held, raw)
        .await?
        .context("owner endpoint")?;
    assert!(
        local_http::load(&f.db, &manager(), &other, raw)
            .await?
            .is_none()
    );
    assert!(
        local_http::load(&f.db, &manager(), &held, "http://other.home.arpa:8080/")
            .await?
            .is_none()
    );
    let denylist = crate::live::upstream::LocalDestinationDenylist::new(
        vec!["192.168.1.10".parse()?],
        vec![],
    )?;
    assert!(selected.policy_for(raw, false, denylist.clone()).is_err());
    assert!(selected.policy_for(raw, true, denylist).is_ok());
    config(
        &f,
        owner,
        json!({"localNetwork":false,"service_url":"http://nas.home.arpa:8080/"}),
    )
    .await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await?
            .is_none()
    );
    sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn exact_local_endpoint_reads_only_the_bound_generation_secret_and_detects_rotation()
-> Result<()> {
    let f = Fixture::with_lan().await?;
    let (owner, held) = configured(&f, "one").await?;
    let raw = "http://playlist.home.arpa/list.m3u?token=owner-secret";
    config(&f, owner, json!({"localNetwork":true})).await?;
    // A legacy owner secret must NOT be adopted by a generation-owned lookup.
    let cipher = manager().encrypt(raw)?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'playlist_url',$3,FALSE)")
        .bind(Uuid::new_v4().to_string()).bind(owner.to_string()).bind(cipher).execute(&f.db.pool).await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await?
            .is_none()
    );
    secret(&f, &held, raw).await?;
    let first = local_http::load(&f.db, &manager(), &held, raw)
        .await?
        .context("generation endpoint")?;
    assert!(
        local_http::load(
            &f.db,
            &manager(),
            &held,
            &raw.replace("owner-secret", "other")
        )
        .await?
        .is_none()
    );
    let rotated = raw.replace("owner-secret", "rotated");
    secret(&f, &held, &rotated).await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await?
            .is_none()
    );
    let second = local_http::load(&f.db, &manager(), &held, &rotated)
        .await?
        .context("rotated endpoint")?;
    assert_ne!(first.revision(), second.revision());
    // Corrupt/cross-key ciphertext fails, even with a matching legacy secret.
    sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted='invalid',encoded_bytes=7 WHERE namespace_id=$1")
        .bind(held.coordinates().namespace.to_string()).execute(&f.db.pool).await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn an_original_scope_without_local_permission_requires_no_additional_database_pass()
-> Result<()> {
    let f = Fixture::new().await?;
    let (_, held) = configured(&f, "public-only").await?;
    f.db.pool.close().await;
    assert!(
        local_http::load(&f.db, &manager(), &held, "https://provider.example/")
            .await?
            .is_none(),
        "absence of local permission must not repeat database/manifest work or become authority"
    );
    Ok(())
}

#[tokio::test]
async fn configured_local_endpoint_snapshot_cannot_survive_grant_revocation() -> Result<()> {
    let f = Fixture::with_lan().await?;
    let (owner, held) = configured(&f, "one").await?;
    let raw = "http://nas.home.arpa:8080/player_api.php";
    config(
        &f,
        owner,
        json!({"localNetwork":true,"service_url":"http://nas.home.arpa:8080/"}),
    )
    .await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await?
            .is_some()
    );
    super::super::super::super::revoke_as(
        &f.db,
        &f.actor,
        held.review().scope().extension_id(),
        f.revision,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    assert!(
        local_http::load(&f.db, &manager(), &held, raw)
            .await
            .is_err()
    );
    Ok(())
}
