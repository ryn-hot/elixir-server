use super::*;
use crate::{config::DatabaseConfig, db::Database};

async fn database() -> Result<Database> {
    let db = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..Default::default()
    })
    .await?;
    db.run_migrations().await?;
    Ok(db)
}
fn key(seed: u8) -> String {
    WireguardSecrets::from_keys(Zeroizing::new([seed; 32]), None)
        .unwrap()
        .device_public_key()
        .unwrap()
}
#[test]
fn private_key_aliases_resolve_to_the_same_public_identity_without_exposing_secrets() -> Result<()>
{
    let bytes = [17; 32];
    let mut alias = bytes;
    alias[0] ^= 7;
    alias[31] ^= 128;
    let config = |private| {
        format!(
            "[Interface]\nPrivateKey={}\n[Peer]\nPublicKey={}\n",
            STANDARD.encode(private),
            key(9)
        )
    };
    assert_eq!(
        wireguard_keys(&config(bytes))?,
        wireguard_keys(&config(alias))?
    );
    assert!(wireguard_keys("[Peer]\nPrivateKey=not-a-device").is_err());
    assert!(wireguard_keys(&"x".repeat(256 * 1024 + 1)).is_err());
    Ok(())
}
#[tokio::test]
async fn reservations_are_atomic_purpose_bound_and_exact_retryable() -> Result<()> {
    let db = database().await?;
    reserve(&db.pool, &[(key(1), Purpose::Downloads)]).await?;
    assert!(
        reserve(
            &db.pool,
            &[
                (key(2), Purpose::MarketplaceHttp),
                (key(1), Purpose::MarketplaceNetwork)
            ]
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM network_vpn_identity_purposes")
            .fetch_one(&db.pool)
            .await?,
        1
    );
    current(&db.pool, &key(1), Purpose::Downloads).await?;
    assert!(
        current(&db.pool, &key(1), Purpose::MarketplaceHttp)
            .await
            .is_err()
    );
    reserve(&db.pool, &[(key(1), Purpose::Downloads)]).await?;
    let a = [(key(3), Purpose::MarketplaceHttp)];
    let b = [(key(3), Purpose::MarketplaceNetwork)];
    let (first, second) = tokio::join!(reserve(&db.pool, &a), reserve(&db.pool, &b));
    assert_ne!(first.is_ok(), second.is_ok());
    Ok(())
}
#[tokio::test]
async fn full_registry_preserves_old_bindings_without_growth_or_implicit_reset() -> Result<()> {
    let db = database().await?;
    let existing = key(5);
    reserve(&db.pool, &[(existing.clone(), Purpose::MarketplaceHttp)]).await?;
    let mut tx = db.pool.begin().await?;
    for i in 1..MAX_IDENTITIES {
        sqlx::query("INSERT INTO network_vpn_identity_purposes(identity_sha256,purpose) VALUES($1,'downloads')")
            .bind(Digest::of(i.to_le_bytes()).to_string()).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    reserve(&db.pool, &[(existing.clone(), Purpose::MarketplaceHttp)]).await?;
    assert!(
        reserve(&db.pool, &[(key(6), Purpose::Downloads)])
            .await
            .is_err()
    );
    current(&db.pool, &existing, Purpose::MarketplaceHttp).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM network_vpn_identity_purposes")
            .fetch_one(&db.pool)
            .await?,
        MAX_IDENTITIES
    );
    Ok(())
}
