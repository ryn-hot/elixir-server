//! Actual import/storage/crypto; no live gateway/provider health claim.
use super::*;
use crate::{config::DatabaseConfig, db::Database};
use base64::{Engine as _, engine::general_purpose::STANDARD};

async fn fixture() -> Result<(Database, SecretsManager)> {
    let db = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..Default::default()
    })
    .await?;
    db.run_migrations().await?;
    Ok((db, SecretsManager::from_key_bytes([41; 32], false)))
}
fn config(bytes: [u8; 32]) -> String {
    format!(
        "[Interface]\nPrivateKey = {}\nAddress = 10.64.0.2/32\nDNS = 1.1.1.1\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n",
        STANDARD.encode(bytes),
        STANDARD.encode([9; 32])
    )
}
async fn import(
    db: &Database,
    secrets: &SecretsManager,
    id: &str,
    private: [u8; 32],
    purpose: VpnProfilePurpose,
) -> Result<DownloadNetworkProfileImportResponse> {
    let mut request = serde_json::json!({"profileId":id,"name":id,"config":config(private),
        "purpose":match purpose {VpnProfilePurpose::Downloads=>"downloads",VpnProfilePurpose::MarketplaceHttp=>"marketplace_http",VpnProfilePurpose::MarketplaceNetwork=>"marketplace_network"}});
    if purpose != VpnProfilePurpose::MarketplaceNetwork {
        request["gatewayImage"] = serde_json::json!(format!("sha256:{}", "a".repeat(64)));
    }
    import_wireguard_profile(&db.pool, secrets, serde_json::from_value(request)?).await
}
#[tokio::test]
async fn purpose_import_never_changes_download_selection_or_accepts_a_core_profile_overwrite()
-> Result<()> {
    let (db, secrets) = fixture().await?;
    import(&db, &secrets, "core", [3; 32], VpnProfilePurpose::Downloads).await?;
    sqlx::query(
        "UPDATE download_network_profiles SET active=TRUE,status='protected' WHERE id='core'",
    )
    .execute(&db.pool)
    .await?;
    import(
        &db,
        &secrets,
        "extension-http",
        [7; 32],
        VpnProfilePurpose::MarketplaceHttp,
    )
    .await?;
    assert_eq!(
        active_live_egress_profile(&db.pool, &secrets)
            .await?
            .context("core")?
            .profile_id,
        "core"
    );
    assert_eq!(
        marketplace_http_profile(&db.pool, &secrets)
            .await?
            .context("explicit app profile")?
            .profile_id,
        "extension-http"
    );
    assert_eq!(
        list_download_network_profiles(&db.pool)
            .await?
            .profiles
            .len(),
        1
    );
    let settings = Settings::default();
    let switch = switch_download_protection_profile(
        &settings,
        &db.pool,
        &secrets,
        serde_json::from_value(
            serde_json::json!({"targetProfileId":"extension-http","apply":true}),
        )?,
    )
    .await?;
    assert!(!switch.applied && switch.blocker.is_some());
    assert!(
        import(
            &db,
            &secrets,
            "core",
            [11; 32],
            VpnProfilePurpose::MarketplaceHttp
        )
        .await
        .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "extension-http",
            [11; 32],
            VpnProfilePurpose::Downloads
        )
        .await
        .is_err()
    );
    assert_eq!(
        active_live_egress_profile(&db.pool, &secrets)
            .await?
            .context("unchanged core")?
            .profile_id,
        "core"
    );
    Ok(())
}
#[tokio::test]
async fn clamped_aliases_are_denied_in_both_directions_before_secret_or_profile_writes()
-> Result<()> {
    let (db, secrets) = fixture().await?;
    import(&db, &secrets, "core", [3; 32], VpnProfilePurpose::Downloads).await?;
    let mut alias = [3; 32];
    alias[0] ^= 7;
    alias[31] ^= 128;
    assert!(
        import(
            &db,
            &secrets,
            "alias",
            alias,
            VpnProfilePurpose::MarketplaceHttp
        )
        .await
        .is_err()
    );
    assert!(
        load_download_network_profile(&db.pool, "alias")
            .await?
            .is_none()
    );
    import(
        &db,
        &secrets,
        "extension-http",
        [7; 32],
        VpnProfilePurpose::MarketplaceHttp,
    )
    .await?;
    assert!(
        import(
            &db,
            &secrets,
            "core-alias",
            [7; 32],
            VpnProfilePurpose::Downloads
        )
        .await
        .is_err()
    );
    let public = vpn_identity::wireguard_keys(&config([7; 32]))?.remove(0);
    assert!(
        reserve_marketplace_network_identity(&db.pool, &secrets, &public)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM download_network_profiles")
            .fetch_one(&db.pool)
            .await?,
        2
    );
    Ok(())
}
#[tokio::test]
async fn rotated_old_keys_remain_reserved_and_legacy_core_only_never_supplies_application_protection()
-> Result<()> {
    let (db, secrets) = fixture().await?;
    import(&db, &secrets, "core", [3; 32], VpnProfilePurpose::Downloads).await?;
    assert!(
        marketplace_http_profile(&db.pool, &secrets)
            .await?
            .is_none()
    );
    import(
        &db,
        &secrets,
        "extension-http",
        [7; 32],
        VpnProfilePurpose::MarketplaceHttp,
    )
    .await?;
    import(
        &db,
        &secrets,
        "extension-http",
        [11; 32],
        VpnProfilePurpose::MarketplaceHttp,
    )
    .await?;
    assert!(
        import(
            &db,
            &secrets,
            "old-app-key",
            [7; 32],
            VpnProfilePurpose::Downloads
        )
        .await
        .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "second-http",
            [17; 32],
            VpnProfilePurpose::MarketplaceHttp
        )
        .await
        .is_err()
    );
    let public = vpn_identity::wireguard_keys(&config([11; 32]))?.remove(0);
    vpn_identity::current(&db.pool, &public, Purpose::MarketplaceHttp).await?;
    let default: DownloadNetworkProfileImportRequest =
        serde_json::from_value(serde_json::json!({"config":"x"}))?;
    assert_eq!(default.purpose, VpnProfilePurpose::Downloads);
    assert!(
        serde_json::from_value::<DownloadNetworkProfileImportRequest>(
            serde_json::json!({"config":"x","purpose":"arbitrary_network"})
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn dedicated_network_import_is_not_a_downloader_http_selection_or_cross_purpose_key()
-> Result<()> {
    let (db, secrets) = fixture().await?;
    import(&db, &secrets, "core", [3; 32], VpnProfilePurpose::Downloads).await?;
    sqlx::query(
        "UPDATE download_network_profiles SET active=TRUE,status='protected' WHERE id='core'",
    )
    .execute(&db.pool)
    .await?;
    import(
        &db,
        &secrets,
        "marketplace-network",
        [17; 32],
        VpnProfilePurpose::MarketplaceNetwork,
    )
    .await?;
    assert_eq!(
        marketplace_network_material(&db.pool, &secrets, "marketplace-network")
            .await?
            .as_str(),
        config([17; 32]).trim()
    );
    assert_eq!(
        active_live_egress_profile(&db.pool, &secrets)
            .await?
            .context("core remains selected")?
            .profile_id,
        "core"
    );
    assert!(
        marketplace_http_profile(&db.pool, &secrets)
            .await?
            .is_none()
    );
    assert_eq!(
        list_download_network_profiles(&db.pool)
            .await?
            .profiles
            .len(),
        1
    );
    assert!(
        marketplace_network_material(&db.pool, &secrets, "core")
            .await
            .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "core",
            [19; 32],
            VpnProfilePurpose::MarketplaceNetwork
        )
        .await
        .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "marketplace-network",
            [19; 32],
            VpnProfilePurpose::MarketplaceHttp
        )
        .await
        .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "network-second",
            [19; 32],
            VpnProfilePurpose::MarketplaceNetwork
        )
        .await
        .is_err()
    );
    assert!(
        import(
            &db,
            &secrets,
            "other-core",
            [17; 32],
            VpnProfilePurpose::Downloads
        )
        .await
        .is_err()
    );
    let switch = switch_download_protection_profile(
        &Settings::default(),
        &db.pool,
        &secrets,
        serde_json::from_value(
            serde_json::json!({"targetProfileId":"marketplace-network","apply":true}),
        )?,
    )
    .await?;
    assert!(!switch.applied && switch.blocker.is_some());
    sqlx::query(
        "UPDATE download_network_profiles SET enabled=FALSE WHERE id='marketplace-network'",
    )
    .execute(&db.pool)
    .await?;
    assert!(
        marketplace_network_material(&db.pool, &secrets, "marketplace-network")
            .await
            .is_err()
    );
    Ok(())
}
