//! Protocol/scheduling DATA checks, not native/public-network qualification.
use super::*;
use serde_json::{Value, json};

fn principal() -> CurrentPrincipal {
    CurrentPrincipal::fixture_owner(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        1,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        crate::authz::CapabilitySet::all(),
    )
}
fn input() -> Result<Value> {
    let vector: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    Ok(
        json!({"schema_version":1,"query_id":Uuid::new_v4(),"extension_id":vector["release"]["payload"]["extension_id"],
        "release_id":Digest::of(b"typed request DATA only"),"target":{"kind":"new","name":"A"},
        "expected_revision":0,"mutation_id":Uuid::new_v4(),"controls":{"kind":"absent"}}),
    )
}
#[test]
fn direct_install_requires_exact_explicit_acceptance_without_fabricated_search_or_paid_proofs()
-> Result<()> {
    let vector: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let publication = elixir_marketplace::publication::PreparedPublication::parse(
        &serde_json::to_vec(&json!({
            "schema_version":1,"publisher":vector["publisher"],"release":vector["release"],"head":vector["head"]
        }))?,
    )?;
    let value = json!({"schema_version":1,"request_id":Uuid::new_v4(),"publication":publication,
        "accepted_reference":Digest::of(publication.to_bytes()?),"accept_unqualified":true,
        "target":{"kind":"new","name":"Explicit early tester"},"expected_revision":0,
        "mutation_id":Uuid::new_v4(),"controls":{"kind":"absent"}});
    let (_, action, checked, _, upload) =
        DirectInstallRequest::parse(&serde_json::to_vec(&value)?)?.into_parts()?;
    assert!(!upload);
    let mut uploaded = value.clone();
    uploaded["upload_source"] = json!(true);
    assert!(
        DirectInstallRequest::parse(&serde_json::to_vec(&uploaded)?)?
            .into_parts()?
            .4
    );
    uploaded["upload_source"] = json!("true");
    assert!(DirectInstallRequest::parse(&serde_json::to_vec(&uploaded)?).is_err());
    assert_eq!(action.release, checked.release_id()?);
    for (field, invalid) in [
        ("accept_unqualified", json!(false)),
        ("accepted_reference", json!(Digest::of(b"wrong"))),
        ("request_id", json!(Uuid::nil())),
        ("paid", json!(true)),
        ("peer", json!("1".repeat(64))),
        ("source_verified", json!(true)),
        ("expected_revision", json!(1)),
        (
            "publication",
            json!({"schema_version":1,"publisher":vector["publisher"],"release":vector["release"],
            "head":vector["head"],"gate_evidence":[]}),
        ),
    ] {
        let mut invalid_value = value.clone();
        invalid_value[field] = invalid;
        assert!(
            DirectInstallRequest::parse(&serde_json::to_vec(&invalid_value)?).is_err(),
            "{field}"
        );
    }
    let encoded = serde_json::to_string(&value)?;
    assert!(
        DirectInstallRequest::parse(
            format!("{{\"accept_unqualified\":true,{}", &encoded[1..]).as_bytes()
        )
        .is_err()
    );
    assert!(DirectInstallRequest::parse(&vec![b' '; MAX_DIRECT_INSTALL_BYTES + 1]).is_err());
    Ok(())
}
#[test]
fn installation_request_is_closed_bounded_and_carries_only_original_data() -> Result<()> {
    let valid = input()?;
    InstallRequest::parse(&serde_json::to_vec(&valid)?)?;
    for (field, value) in [
        ("schema_version", json!(2)),
        ("query_id", json!(Uuid::nil())),
        ("mutation_id", json!(Uuid::nil())),
        ("expected_revision", json!(-1)),
        ("expected_revision", json!(1)),
        ("path", json!("/arbitrary")),
        ("allocation", json!({"memory_bytes":999999999})),
        ("actor", json!({"role":"owner"})),
        ("native_owner", json!(true)),
        (
            "target",
            json!({"kind":"new","name":"A","image":"attacker"}),
        ),
        ("target", json!({"kind":"new","name":"x".repeat(129)})),
        ("controls", json!({"kind":"absent","revision":1})),
    ] {
        let mut data = valid.clone();
        data[field] = value;
        assert!(
            InstallRequest::parse(&serde_json::to_vec(&data)?).is_err(),
            "{field}"
        );
    }
    let encoded = serde_json::to_string(&valid)?;
    assert!(
        InstallRequest::parse(format!("{{\"schema_version\":1,{}", &encoded[1..]).as_bytes())
            .is_err()
    );
    assert!(InstallRequest::parse(&vec![b' '; MAX_INSTALL_BYTES + 1]).is_err());
    Ok(())
}
#[test]
fn existing_target_requires_original_journal_and_matching_control_versions() -> Result<()> {
    let mut value = input()?;
    let id = Uuid::new_v4();
    value["target"] = json!({"kind":"existing","instance_id":id});
    value["controls"] = json!({"kind":"existing","extension_revision":1,"instance_revision":2});
    assert!(InstallRequest::parse(&serde_json::to_vec(&value)?).is_err());
    value["expected_revision"] = json!(2);
    InstallRequest::parse(&serde_json::to_vec(&value)?)?;
    value["controls"]["instance_revision"] = json!(MAX_REVISION);
    assert!(InstallRequest::parse(&serde_json::to_vec(&value)?).is_err());
    value["controls"] = json!({"kind":"new_instance","extension_revision":1});
    assert!(InstallRequest::parse(&serde_json::to_vec(&value)?).is_err());
    Ok(())
}
fn setup(credentials: Value) -> Value {
    json!({"schema_version":1,"request":{"action":"setup","mutation_id":Uuid::new_v4(),"credentials":credentials}})
}
#[test]
fn original_request_echo_roundtrips_without_restamping_or_creating_capabilities() -> Result<()> {
    let first = input()?;
    let mut existing = first.clone();
    existing["target"] = json!({"kind":"existing","instance_id":Uuid::new_v4()});
    existing["expected_revision"] = json!(2);
    existing["controls"] = json!({"kind":"existing","extension_revision":3,"instance_revision":4});
    for original in [first, existing] {
        let (query, action) = InstallRequest::parse(&serde_json::to_vec(&original)?)?.into_action();
        let data = application::InitialRequest::new(query, &action);
        let encoded = serde_json::to_vec(&data)?;
        assert!(encoded.len() <= MAX_INSTALL_BYTES);
        assert_eq!(serde_json::from_slice::<Value>(&encoded)?, original);
        InstallRequest::parse(&encoded)?;
    }
    Ok(())
}
#[test]
fn setup_values_are_closed_bounded_and_transferred_directly_into_zeroizing_inputs() -> Result<()> {
    let data = setup(
        json!([{"operation":"set","key":"source_api","value":"private-fixture","rotatable":false}]),
    );
    let parsed = ActionRequest::parse(&serde_json::to_vec(&data)?)?;
    match parsed.into_action(principal())? {
        application::Action::Setup { credentials, .. } => match &credentials[0] {
            installation::activation::CredentialChange::Set { value, .. } => {
                assert_eq!(value.as_str(), "private-fixture")
            }
            _ => bail!("wrong credential operation"),
        },
        _ => bail!("wrong action"),
    }
    for credentials in [
        json!([{"operation":"set","key":"source_api","value":"x".repeat(65537),"rotatable":false}]),
        json!([{"operation":"set","key":"bad\nkey","value":"private-fixture","rotatable":false}]),
        json!([{"operation":"remove","key":"k","value":"unexpected"}]),
        json!([{"operation":"remove","key":"k"},{"operation":"remove","key":"k"}]),
        json!(
            (0..65)
                .map(|n| json!({"operation":"remove","key":format!("k{n}")}))
                .collect::<Vec<_>>()
        ),
    ] {
        assert!(ActionRequest::parse(&serde_json::to_vec(&setup(credentials))?).is_err());
    }
    assert!(ActionRequest::parse(&vec![b' '; MAX_ACTION_BYTES + 1]).is_err());
    Ok(())
}
#[test]
fn action_envelopes_reject_unknown_actor_payloads_nil_mutations_and_inexact_numbers() -> Result<()>
{
    for kind in ["retry_stage", "activate", "cancel"] {
        let good =
            json!({"schema_version":1,"request":{"action":kind,"mutation_id":Uuid::new_v4()}});
        ActionRequest::parse(&serde_json::to_vec(&good)?)?.into_action(principal())?;
        let mut bad = good.clone();
        bad["request"]["mutation_id"] = json!(Uuid::nil());
        assert!(ActionRequest::parse(&serde_json::to_vec(&bad)?).is_err());
        let mut bad = good.clone();
        bad["request"]["principal"] = json!({"role":"owner"});
        assert!(ActionRequest::parse(&serde_json::to_vec(&bad)?).is_err());
        let encoded = serde_json::to_string(&good)?;
        assert!(
            ActionRequest::parse(format!("{{\"schema_version\":1,{}", &encoded[1..]).as_bytes())
                .is_err()
        );
    }
    let bad = json!({"schema_version":1,"request":{"action":"approve","mutation_id":Uuid::new_v4(),"expected_revision":1.5,"review_id":Digest::of(b"review"),"automatic_updates":false}});
    assert!(ActionRequest::parse(&serde_json::to_vec(&bad)?).is_err());
    Ok(())
}
#[tokio::test]
async fn an_unadmitted_service_never_bootstraps_or_allocates_from_a_management_request()
-> Result<()> {
    let service = MarketplaceService::default();
    assert!(!service.installation_review_available());
    let p = principal();
    let secrets = SecretsManager::from_key_bytes([55; 32], false);
    assert!(
        service
            .request_install(
                &p,
                InstallRequest::parse(&serde_json::to_vec(&input()?)?)?,
                secrets.clone()
            )
            .is_err()
    );
    service.configure_installation(application::fixture_allocation())?;
    assert!(
        !service.installation_review_available(),
        "allocation is not a protected connection"
    );
    assert!(
        service
            .configure_installation(application::fixture_allocation())
            .is_err()
    );
    let request = InstallRequest::parse(&serde_json::to_vec(&input()?)?)?;
    assert!(service.request_install(&p, request, secrets).is_err());
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    assert!(service.install_view(Uuid::new_v4()).is_err());
    service.shutdown().await?;
    Ok(())
}
