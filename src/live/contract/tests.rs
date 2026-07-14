use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};

use super::*;

fn scope() -> ProviderContract {
    ProviderContract::new(
        [LiveItemType::Event, LiveItemType::Channel],
        [
            StreamProtocol::Hls,
            StreamProtocol::Dash,
            StreamProtocol::HttpProgressive,
            StreamProtocol::MpegTs,
        ],
    )
    .expect("valid test scope")
}

fn example(name: &str) -> Vec<u8> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../docs/contracts/examples/live")
        .join(name);
    std::fs::read(root).expect("checked contract example")
}

fn valid_item(id: &str) -> Value {
    json!({
        "id": id,
        "itemType": "event",
        "title": "Fixture event",
        "subtitle": null,
        "description": null,
        "status": "live",
        "startsAt": "2026-07-10T19:00:00Z",
        "endsAt": "2026-07-10T21:00:00Z",
        "posterUrl": null,
        "backgroundUrl": null,
        "logoUrl": null,
        "categories": [],
        "badges": [],
        "facts": []
    })
}

fn page(items: Vec<Value>) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "items": items,
        "nextCursor": null,
        "cache": {"maxAgeSeconds": 10, "staleWhileRevalidateSeconds": 20}
    }))
    .expect("page json")
}

#[test]
fn s11_frozen_provider_goldens_convert_to_canonical_types() -> Result<()> {
    let provider_scope = scope();
    let health = parse_health_response(
        br#"{"status":"healthy","contractVersions":[1],"details":["ready"],"future":true}"#,
    )?;
    assert_eq!(health.status, ProviderHealthStatus::Healthy);

    let catalogs =
        parse_catalogs_response(&example("provider-catalogs-response.json"), &provider_scope)?;
    assert_eq!(catalogs.catalogs.len(), 1);
    assert_eq!(catalogs.catalogs[0].id, "live_events");

    let catalog =
        parse_catalog_page_response(&example("provider-catalog-response.json"), &provider_scope)?;
    assert_eq!(catalog.items.len(), 1);
    assert!(catalog.diagnostics.is_empty());
    assert_eq!(catalog.items[0].status, LiveItemStatus::Live);

    let metadata = parse_meta_response(
        &example("provider-meta-response.json"),
        &provider_scope,
        "provider-event-id",
    )?;
    assert_eq!(metadata.streams.len(), 2);
    assert_eq!(metadata.streams[0].id, "primary");

    let now = Utc.with_ymd_and_hms(2026, 7, 10, 20, 0, 0).unwrap();
    let resolved = parse_resolve_response(
        &example("provider-resolve-response.json"),
        &provider_scope,
        "primary",
        now,
    )?;
    assert_eq!(resolved.descriptor.protocol, StreamProtocol::Hls);
    assert_eq!(resolved.descriptor.request_headers.len(), 1);
    let debug = format!("{:?}", resolved.descriptor);
    assert!(!debug.contains("fixture-canary"));
    assert!(!debug.contains("https://"));

    let refreshed = parse_refresh_response(
        &example("provider-refresh-response.json"),
        &provider_scope,
        now,
    )?;
    assert_eq!(refreshed.descriptor.stream_id, "primary-refresh-1");
    Ok(())
}

#[test]
fn s11_catalog_shell_enforces_duplicates_depth_counts_and_partial_threshold() -> Result<()> {
    let provider_scope = scope();
    let duplicate_key = br#"{"items":[],"items":[],"nextCursor":null,"cache":{"maxAgeSeconds":0,"staleWhileRevalidateSeconds":0}}"#;
    assert_eq!(
        parse_catalog_page_response(duplicate_key, &provider_scope)
            .expect_err("duplicate JSON keys fail")
            .code(),
        ContractErrorCode::DuplicateJsonKey
    );

    let oversized = page(
        (0..101)
            .map(|index| valid_item(&format!("item-{index}")))
            .collect(),
    );
    assert_eq!(
        parse_catalog_page_response(&oversized, &provider_scope)
            .expect_err("item shell is bounded")
            .code(),
        ContractErrorCode::LimitExceeded
    );

    let mut ten_invalid = (0..100)
        .map(|index| valid_item(&format!("item-{index}")))
        .collect::<Vec<_>>();
    for item in ten_invalid.iter_mut().take(10) {
        item["status"] = json!("invalid-status");
    }
    let accepted = parse_catalog_page_response(&page(ten_invalid.clone()), &provider_scope)?;
    assert_eq!(accepted.items.len(), 90);
    assert_eq!(accepted.diagnostics.len(), 10);

    ten_invalid[10]["status"] = json!("invalid-status");
    assert_eq!(
        parse_catalog_page_response(&page(ten_invalid), &provider_scope)
            .expect_err("more than ten percent fails the page")
            .code(),
        ContractErrorCode::TooManyInvalidItems
    );

    let duplicate_ids = page(vec![valid_item("same"), valid_item("same")]);
    assert_eq!(
        parse_catalog_page_response(&duplicate_ids, &provider_scope)
            .expect_err("duplicate provider item ids fail deterministically")
            .code(),
        ContractErrorCode::DuplicateId
    );

    let mut deep = json!({"leaf": true});
    for _ in 0..40 {
        deep = json!({"nested": deep});
    }
    let deep_body = serde_json::to_vec(&json!({
        "items": [],
        "nextCursor": null,
        "cache": {"maxAgeSeconds": 0, "staleWhileRevalidateSeconds": 0},
        "future": deep
    }))?;
    assert_eq!(
        parse_catalog_page_response(&deep_body, &provider_scope)
            .expect_err("JSON depth is bounded before conversion")
            .code(),
        ContractErrorCode::LimitExceeded
    );
    Ok(())
}

#[test]
fn s11_unresolved_choices_and_filter_submissions_are_data_only_and_exact() -> Result<()> {
    let provider_scope = scope();
    let mut metadata: Value = serde_json::from_slice(&example("provider-meta-response.json"))?;
    metadata["streams"][0]["url"] = json!("https://should-never-reach-a-client.invalid/live");
    assert_eq!(
        parse_meta_response(
            &serde_json::to_vec(&metadata)?,
            &provider_scope,
            "provider-event-id"
        )
        .expect_err("stream choices cannot contain source material")
        .code(),
        ContractErrorCode::ForbiddenField
    );

    let catalogs =
        parse_catalogs_response(&example("provider-catalogs-response.json"), &provider_scope)?;
    let catalog = &catalogs.catalogs[0];
    catalog.validate_filter_submission(&BTreeMap::from([
        (
            "category".to_string(),
            FilterValue::Multiple(vec!["football".to_string()]),
        ),
        ("liveOnly".to_string(), FilterValue::Toggle(true)),
    ]))?;
    assert_eq!(
        catalog
            .validate_filter_submission(&BTreeMap::from([(
                "category".to_string(),
                FilterValue::Multiple(vec!["unknown".to_string()]),
            )]))
            .expect_err("unknown option is not downgraded to an unfiltered call")
            .code(),
        ContractErrorCode::InvalidFilter
    );
    assert_eq!(
        catalog
            .validate_filter_submission(&BTreeMap::from([(
                "unknown".to_string(),
                FilterValue::Text("value".to_string()),
            )]))
            .expect_err("unknown filter fails")
            .code(),
        ContractErrorCode::InvalidFilter
    );
    Ok(())
}

#[test]
fn s11_resolved_descriptors_fail_closed_on_credentials_drm_protocols_and_expiry() -> Result<()> {
    let provider_scope = scope();
    let now = Utc.with_ymd_and_hms(2026, 7, 10, 20, 0, 0).unwrap();
    let valid: Value = serde_json::from_slice(&example("provider-resolve-response.json"))?;

    let mut drm = valid.clone();
    drm["descriptor"]["drm"]["kind"] = json!("widevine");
    assert_eq!(
        resolve_error(&drm, &provider_scope, now),
        ContractErrorCode::DrmUnsupported
    );

    let mut scheme = valid.clone();
    scheme["descriptor"]["protocol"] = json!("dash");
    scheme["descriptor"]["url"] = json!("rtmp://origin.example.test/live");
    assert_eq!(
        resolve_error(&scheme, &provider_scope, now),
        ContractErrorCode::InvalidUrl
    );

    let mut header = valid.clone();
    header["descriptor"]["requestHeaders"] = json!({"Host": "attacker.invalid"});
    assert_eq!(
        resolve_error(&header, &provider_scope, now),
        ContractErrorCode::InvalidCredentials
    );

    let mut missing_authority = valid.clone();
    missing_authority["descriptor"]["credentialAuthorities"] = json!([]);
    assert_eq!(
        resolve_error(&missing_authority, &provider_scope, now),
        ContractErrorCode::InvalidCredentials
    );

    let mut public = valid.clone();
    public["descriptor"]["clientDisclosure"] = json!("public");
    assert_eq!(
        resolve_error(&public, &provider_scope, now),
        ContractErrorCode::InvalidCredentials
    );

    let mut expired = valid.clone();
    expired["descriptor"]["expiresAt"] = json!("2026-07-10T19:59:59Z");
    assert_eq!(
        resolve_error(&expired, &provider_scope, now),
        ContractErrorCode::DescriptorExpired
    );

    let mut p2p = valid.clone();
    p2p["descriptor"]["url"] = json!("https://origin.example.test/live?info_hash=abc");
    assert_eq!(
        resolve_error(&p2p, &provider_scope, now),
        ContractErrorCode::InvalidUrl
    );
    Ok(())
}

fn resolve_error(
    value: &Value,
    provider_scope: &ProviderContract,
    now: chrono::DateTime<Utc>,
) -> ContractErrorCode {
    parse_resolve_response(
        &serde_json::to_vec(value).expect("descriptor json"),
        provider_scope,
        "primary",
        now,
    )
    .expect_err("descriptor should fail")
    .code()
}

#[test]
fn s11_provider_config_projection_rejects_secrets_paths_and_unbounded_values() {
    validate_provider_config(&json!({
        "fixtureFault": "none",
        "fixtureDelayMs": 25,
        "fixtureOriginBaseUrl": "http://fixture-origin.invalid"
    }))
    .expect("bounded non-secret provider config");

    for unsafe_config in [
        json!({"api_key": "secret-value"}),
        json!({"nested": {"refreshToken": "secret-value"}}),
        json!({"hostMediaPath": "/srv/media"}),
        json!({"safe": "Bearer credential-canary"}),
        json!({"safe": "elx_live_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}),
    ] {
        assert_eq!(
            validate_provider_config(&unsafe_config)
                .expect_err("unsafe provider config fails closed")
                .code(),
            ContractErrorCode::UnsafeProviderConfig
        );
    }
}

#[test]
fn s11_seeded_contract_fuzz_never_panics_and_preserves_id_bounds() {
    let provider_scope = scope();
    let mut rng = StdRng::seed_from_u64(0xE11A_11CE);
    for _ in 0..1_024 {
        let length = rng.gen_range(0..=1_024);
        let bytes = (0..length).map(|_| rng.r#gen::<u8>()).collect::<Vec<_>>();
        let _ = parse_catalog_page_response(&bytes, &provider_scope);
        let _ = parse_meta_response(&bytes, &provider_scope, "expected-item");
        let _ = parse_resolve_response(&bytes, &provider_scope, "expected-stream", Utc::now());
    }

    for length in 1..=128 {
        let id = "a".repeat(length);
        CatalogPageRequest {
            catalog_id: id,
            cursor: None,
            limit: 1,
            filters: BTreeMap::new(),
        }
        .validate()
        .expect("path-safe catalog id within frozen bound");
    }
    for invalid in ["", "contains/slash", "contains space", &"a".repeat(129)] {
        assert!(
            CatalogPageRequest {
                catalog_id: invalid.to_string(),
                cursor: None,
                limit: 1,
                filters: BTreeMap::new(),
            }
            .validate()
            .is_err()
        );
    }
}

#[test]
fn s11_contract_enums_reject_unknown_values_instead_of_defaulting() -> Result<()> {
    let provider_scope = scope();
    let mut item = valid_item("unknown-enum");
    item["status"] = json!("future_status");
    assert_eq!(
        parse_catalog_page_response(&page(vec![item]), &provider_scope)
            .expect_err("unknown status fails the containing page")
            .code(),
        ContractErrorCode::TooManyInvalidItems
    );

    let invalid_scope = ProviderContract::new(BTreeSet::new(), [StreamProtocol::Hls]);
    assert_eq!(
        invalid_scope
            .expect_err("empty item type declaration fails")
            .code(),
        ContractErrorCode::InvalidShape
    );
    Ok(())
}
