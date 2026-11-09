use super::*;
use crate::{
    marketplace::{grant_store::tests::owner, preferences::local_setup::bind_initial_home},
    network::protection::import_wireguard_profile,
};
use serde_json::json;

fn text(private: u8) -> String {
    format!(
        "[Interface]\nPrivateKey = {}\nAddress = 10.66.0.2/32\nDNS = 1.1.1.1\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 8.8.8.8:51820\n",
        STANDARD.encode([private; 32]),
        STANDARD.encode([34; 32])
    )
}
async fn save(database: &Database, secrets: &SecretsManager, private: u8) -> Result<()> {
    import_wireguard_profile(&database.pool,secrets,serde_json::from_value(json!({"profileId":"marketplace-network","name":"Marketplace connection","purpose":"marketplace_network","config":text(private)}))?).await?;
    Ok(())
}
fn profile() -> Result<Profile> {
    let mut value = super::super::tests::profile_json();
    value["provider"] = json!({"type":"saved_wireguard","profile_id":"marketplace-network"});
    Profile::parse(&serde_json::to_vec(&value)?)
}
#[test]
fn saved_full_tunnel_compiles_only_the_native_bounded_policy_and_private_frame() -> Result<()> {
    let value = prepared(parse(&text(17))?, &OrdinaryLimits::default())?;
    assert_eq!(
        value.policy.tunnel_addresses,
        vec!["10.66.0.2".parse::<std::net::IpAddr>()?]
    );
    assert_eq!(
        value.policy.endpoints[0].address,
        "8.8.8.8".parse::<std::net::IpAddr>()?
    );
    assert_eq!(value.policy.endpoints[0].local_port, 51821);
    assert_eq!(value.setup.mtu, 1420);
    assert_eq!(value.setup.keepalive_seconds, 25);
    assert_eq!(value.secrets.private_frame()[..32], [17; 32]);
    value.setup.validate(&value.policy)?;
    assert!(
        profile()?.policy().is_err(),
        "a stored ID cannot manufacture a packet policy before resolution"
    );
    Ok(())
}
#[test]
fn saved_config_rejects_hooks_dns_names_split_tunnels_duplicate_fields_and_unbounded_shapes()
-> Result<()> {
    let base = text(17);
    for invalid in [
        base.replace("8.8.8.8:51820", "provider.example:51820"),
        base.replace("8.8.8.8:51820", "127.0.0.1:51820"),
        base.replace("0.0.0.0/0", "10.0.0.0/8"),
        base.replace("10.66.0.2/32", "10.66.0.2/33"),
        base.replace("10.66.0.2/32", "::1/128"),
        base.replace("DNS = 1.1.1.1", "DNS = private.search.invalid"),
        base.replace("DNS = 1.1.1.1", "DNS = 1.1.1.1\nPostUp = arbitrary command"),
        format!("{base}\nPublicKey = {}", STANDARD.encode([35; 32])),
        format!("{base}\n[Peer]\nPublicKey = {}", STANDARD.encode([35; 32])),
        base.replace(
            "Address = 10.66.0.2/32",
            "Address = 10.66.0.2/32\naDdReSs = 10.66.0.3/32",
        ),
        format!("{base}\nPersistentKeepalive = 121"),
        base.replace("DNS = 1.1.1.1", "DNS = 1.1.1.1\nListenPort = 0"),
        base.replace("DNS = 1.1.1.1", "DNS = 1.1.1.1\nMTU = 9001"),
        "x".repeat(256 * 1024 + 1),
    ] {
        assert!(validate(&invalid).is_err(), "unsupported config accepted");
    }
    Ok(())
}
#[tokio::test]
async fn saved_startup_uses_encrypted_import_without_copying_private_keys_and_detects_rotation()
-> Result<()> {
    let (_directory, database) = super::super::tests::database().await?;
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, uuid::Uuid::new_v4()).await?;
    let manager = SecretsManager::from_key_bytes([77; 32], false);
    save(&database, &manager, 17).await?;
    let original = prepare_vpn(&database, &manager, &profile()?).await?;
    let binding = original
        .binding
        .as_ref()
        .context("saved connection binding")?;
    binding.current().await?;
    let copied: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM secrets WHERE key LIKE 'marketplace.vpn.%'")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        copied, 0,
        "startup consumes the saved encrypted config, not copied secret references"
    );
    let ciphertexts: Vec<String> = sqlx::query_scalar("SELECT value_encrypted FROM secrets")
        .fetch_all(&database.pool)
        .await?;
    assert!(
        ciphertexts.iter().all(
            |value| value.starts_with("enc:v1:") && !value.contains(&STANDARD.encode([17; 32]))
        )
    );
    save(&database, &manager, 19).await?;
    assert!(binding.current().await.is_err());
    let next = prepare_vpn(&database, &manager, &profile()?).await?;
    assert_ne!(original.identity.device, next.identity.device);
    next.binding.as_ref().unwrap().current().await?;
    sqlx::query(
        "UPDATE download_network_profiles SET enabled=FALSE WHERE id='marketplace-network'",
    )
    .execute(&database.pool)
    .await?;
    assert!(next.binding.as_ref().unwrap().current().await.is_err());
    assert!(prepare_vpn(&database, &manager, &profile()?).await.is_err());
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_transfer_accounting WHERE singleton=1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        revision, 0,
        "resolving saved data does not start a provider or spend transfer credit"
    );
    Ok(())
}
#[tokio::test]
async fn ambiguous_connection_profiles_are_prevented_by_the_database_not_only_a_preflight()
-> Result<()> {
    let (_directory, database) = super::super::tests::database().await?;
    let manager = SecretsManager::from_key_bytes([77; 32], false);
    save(&database, &manager, 17).await?;
    let second=sqlx::query("INSERT INTO download_network_profiles(id,name,kind,enabled,strict,scope,config_json,status,active) VALUES('second-network','Second','wireguard_config',TRUE,TRUE,'marketplace_network','{}','unknown',FALSE)").execute(&database.pool).await;
    assert!(second.is_err());
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM download_network_profiles WHERE scope='marketplace_network'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit real public DNS/TLS and protected participation through a disposable saved WireGuard profile; no external provider/account/off-site privacy or full HTTP startup qualification"]
async fn native_saved_connection_reaches_protected_participation_and_withdrawal_stops_the_original_owner()
-> Result<()> {
    use crate::marketplace::{network_lab::NetworkLab, worker::ParticipationPhase};
    use futures_util::FutureExt;
    let (_directory, database) = super::super::tests::database().await?;
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, uuid::Uuid::new_v4()).await?;
    let manager = SecretsManager::from_key_bytes([77; 32], false);
    let mut lab = NetworkLab::new(database.clone())?;
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_GUARD_IMAGE")?;
    let worker_image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let outcome=std::panic::AssertUnwindSafe(async {
        lab.start_saved_connection(&guard_image,&manager).await?;
        let prepared=prepare_vpn(&database,&manager,&profile()?).await?;
        let binding=prepared.binding.as_ref().context("actual saved binding")?;binding.current().await?;
        let client=lab.start_participating(&worker_image).await?;
        tokio::time::timeout(Duration::from_secs(45),async {
            loop {
                if client.snapshot().phase==ParticipationPhase::Running {break;}
                ensure!(client.snapshot().phase!=ParticipationPhase::Failed,"protected participation failed before withdrawal");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_,anyhow::Error>(())
        }).await??;
        sqlx::query("UPDATE download_network_profiles SET enabled=FALSE WHERE id='marketplace-network'").execute(&database.pool).await?;
        ensure!(binding.current().await.is_err(),"withdrawn saved profile was still current");
        client.invalidate_connection();
        tokio::time::timeout(Duration::from_secs(45),async {
            while client.snapshot().phase!=ParticipationPhase::Failed {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await?;
        ensure!(!client.provider_discovery_enabled(),"withdrawn protection still admits provider discovery");
        eprintln!("actual saved encrypted import -> native WireGuard/public DNS+TLS/protected participation -> SQL withdrawal and original protected-owner failure; cleanup follows; no provider account, custom CA, off-site privacy or full HTTP/AppStarter qualification");
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    lab.cleanup().await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
