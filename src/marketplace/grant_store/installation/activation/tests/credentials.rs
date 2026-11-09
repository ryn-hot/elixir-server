//! Real owner authentication, source-scoped encrypted provisioning and copy
//! transactions. Probe completion and direct SDK calls are explicit fixtures;
//! native first readiness is covered by the separate configured native cases.
use super::*;
use crate::marketplace::grant_store::{
    installation::{
        InstallTarget,
        runtime::{InstalledController, tests::configuration},
    },
    sdk,
};
use elixir_marketplace::source_execution::{RuntimeJson, SourceSdkReply};
use serde_json::{Value, json};

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
async fn stage(f: &Fixture) -> Result<StagedInstallation> {
    let mutation = Uuid::new_v4();
    f.stage_configured(
        InstallTarget::New {
            name: format!("Credential owner {mutation}"),
        },
        mutation,
        0,
        limit(),
    )
    .await
}
fn create(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
    changes: Vec<CredentialChange>,
) -> Result<OwnerActivationIntent> {
    OwnerActivationIntent::new_with_credentials(
        &configured_runtime::principal(f),
        staged,
        &f.material()?.review,
        f.revision,
        mutation,
        &manager(),
        changes,
    )
}
async fn secret(f: &Fixture, claim: &super::super::candidate::CandidateClaim) -> Result<Value> {
    let raw = RuntimeJson::from_value(json!({"operation":"secret","key":"source_api"}))?;
    let reply = sdk::CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.binding(),
    }
    .fixture_execute(&raw, 65536, || Ok(()))
    .await?;
    let SourceSdkReply::Value(value) = reply else {
        bail!("expected private credential value");
    };
    Ok(serde_json::from_str(value.as_str())?)
}

#[tokio::test]
async fn credential_requests_are_exact_private_nonce_independent_and_source_bound() -> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let mutation = Uuid::new_v4();
    let first = create(
        &f,
        &a,
        mutation,
        vec![CredentialChange::set(
            "source_api",
            "low-entropy-private-value",
            false,
        )],
    )?;
    let second = create(
        &f,
        &a,
        mutation,
        vec![CredentialChange::set(
            "source_api",
            "low-entropy-private-value",
            false,
        )],
    )?;
    let source = f.material()?.source.id()?;
    let plan = plan(&f)?.id();
    assert_eq!(
        first.request_id(source, plan)?,
        second.request_id(source, plan)?
    );
    let sdk::generation_secrets::PreparedChange::Set { cipher: one, .. } =
        &first.credentials.as_ref().unwrap().changes()[0]
    else {
        bail!("set");
    };
    let sdk::generation_secrets::PreparedChange::Set { cipher: two, .. } =
        &second.credentials.as_ref().unwrap().changes()[0]
    else {
        bail!("set");
    };
    assert_ne!(
        one, two,
        "fresh AEAD nonces must not make exact retry IDs unstable"
    );
    assert!(!one.contains("low-entropy-private-value"));
    for change in [
        CredentialChange::set("source_api", "changed", false),
        CredentialChange::set("source_api", "low-entropy-private-value", true),
        CredentialChange::Remove {
            key: "source_api".into(),
        },
    ] {
        assert_ne!(
            first.request_id(source, plan)?,
            create(&f, &a, mutation, vec![change])?.request_id(source, plan)?
        );
    }
    assert_ne!(
        first.request_id(source, plan)?,
        intent(&f, &a, mutation)?.request_id(source, plan)?
    );
    let b = stage(&f).await?;
    assert_ne!(
        first.request_id(source, plan)?,
        create(
            &f,
            &b,
            mutation,
            vec![CredentialChange::set(
                "source_api",
                "low-entropy-private-value",
                false
            )]
        )?
        .request_id(source, plan)?
    );
    Ok(())
}

#[tokio::test]
async fn credential_constructor_rejects_undeclared_duplicate_unbounded_or_legacy_inputs()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    for changes in [
        vec![],
        vec![CredentialChange::set(
            "not_declared",
            "private-marker",
            false,
        )],
        vec![
            CredentialChange::set("source_api", "one", false),
            CredentialChange::Remove {
                key: "source_api".into(),
            },
        ],
        vec![CredentialChange::set(
            "source_api",
            "x".repeat(65537),
            false,
        )],
        (0..65)
            .map(|_| CredentialChange::Remove {
                key: "source_api".into(),
            })
            .collect(),
    ] {
        let error = create(&f, &a, Uuid::new_v4(), changes)
            .err()
            .context("must reject invalid credential input")?;
        assert!(!format!("{error:#}").contains("private-marker"));
    }
    create(
        &f,
        &a,
        Uuid::new_v4(),
        vec![CredentialChange::set(
            "source_api",
            "\0".repeat(65536),
            false,
        )],
    )?;
    let legacy = Fixture::new().await?;
    let old = legacy.stage(Uuid::new_v4(), 0, limit()).await?;
    assert!(
        create(
            &legacy,
            &old,
            Uuid::new_v4(),
            vec![CredentialChange::set("source_api", "value", false)]
        )
        .is_err()
    );
    let present: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(present, 0, "preparing input is not a credential write");
    Ok(())
}

#[tokio::test]
async fn credential_setup_requires_actual_owner_secret_capability_not_an_untrusted_patch()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let mut no_secret = crate::authz::CapabilitySet::all();
    no_secret.remove(Capability::SecretsManage);
    let missing = CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        f.actor.authorization_revision,
        f.actor.expires,
        no_secret,
    );
    let mut query = configured_runtime::principal(&f);
    query.transport = AccountAuthTransport::Query;
    let mut managed = configured_runtime::principal(&f);
    managed.profile_type = ProfileType::Managed;
    let mut admin = configured_runtime::principal(&f);
    admin.role = HomeRole::Admin;
    for principal in [missing, query, managed, admin] {
        assert!(
            OwnerActivationIntent::new_with_credentials(
                &principal,
                &a,
                &f.material()?.review,
                f.revision,
                Uuid::new_v4(),
                &manager(),
                vec![CredentialChange::set("source_api", "private", false)]
            )
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn credentials_exist_only_in_the_sealed_candidate_before_native_start_and_remain_read_only()
-> Result<()> {
    let f = Fixture::new().await?;
    let config = configuration(&f, IMAGE, "credential-component")?;
    let mut owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
    let a = stage(&f).await?;
    let b = stage(&f).await?;
    let claim = configured_runtime::claim_with_credentials(
        &f,
        &mut owner,
        &config,
        &a,
        Some(vec![CredentialChange::set(
            "source_api",
            "scoped-private-value",
            true,
        )]),
    )
    .await?;
    assert_eq!(secret(&f, &claim).await?, json!("scoped-private-value"));
    let (cipher,bytes):(String,i64)=sqlx::query_as("SELECT value_encrypted,encoded_bytes FROM marketplace_generation_secrets WHERE namespace_id=$1")
        .bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(cipher.len() as i64, bytes);
    assert!(SecretsManager::is_bound(&cipher));
    let other: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1",
    )
    .bind(b.instance().to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(other, 0);
    let journal: String = sqlx::query_scalar(
        "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
    )
    .bind(a.configured_instance.unwrap().to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert!(!journal.contains("scoped-private-value"));
    // A retry reconstructs fresh ciphertexts, but its keyed request ID must
    // acknowledge the same durable attempt without replacing its saved seal.
    let saved: Journal = canonical::decode(journal.as_bytes(), record::MAX_RECORD_BYTES)?;
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        IMAGE,
        &config.source_budget,
    )?;
    let checked = |intent| -> Result<Step<'_>> {
        Ok(Step {
            database: &f.db,
            metadata: &f.metadata,
            public: &f.public,
            material: f.material()?,
            intent: Arc::new(intent),
            plan: &plan,
            budget: limit(),
        })
    };
    let same = create(
        &f,
        &a,
        saved.mutation,
        vec![CredentialChange::set(
            "source_api",
            "scoped-private-value",
            true,
        )],
    )?;
    assert!(matches!(
        prepare_checked(checked(same)?).await?,
        PreparedOrRecovery::Recovery(_)
    ));
    let different = create(
        &f,
        &a,
        saved.mutation,
        vec![CredentialChange::set(
            "source_api",
            "different-private-value",
            true,
        )],
    )?;
    assert!(prepare_checked(checked(different)?).await.is_err());
    let unchanged: String = sqlx::query_scalar(
        "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
    )
    .bind(a.configured_instance.unwrap().to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(
        unchanged, journal,
        "retry must not rewrite the seal or reissue a launch claim"
    );
    let changed = sdk::generation_secrets::seal(
        &manager(),
        sdk::namespaces::Coordinates {
            home: f.actor.home,
            extension: a.extension(),
            instance: a.configured_instance.unwrap(),
            namespace: a.instance(),
        },
        "source_api",
        "changed-without-owner-intent",
    )?;
    sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted=$1,encoded_bytes=$2 WHERE namespace_id=$3")
        .bind(&changed).bind(changed.len() as i64).bind(a.instance().to_string()).execute(&f.db.pool).await?;
    assert!(
        secret(&f, &claim).await.is_err(),
        "credentials cannot be changed after their copy seal"
    );
    sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted=$1,encoded_bytes=$2 WHERE namespace_id=$3")
        .bind(&cipher).bind(bytes).bind(a.instance().to_string()).execute(&f.db.pool).await?;
    assert_eq!(secret(&f, &claim).await?, json!("scoped-private-value"));
    Ok(())
}

#[tokio::test]
async fn credential_copy_rechecks_current_authority_and_rolls_back_wrong_key_without_partial_state()
-> Result<()> {
    for fault in ["key", "authority"] {
        let f = Fixture::new().await?;
        let config = configuration(&f, IMAGE, "credential-rollback")?;
        let mut owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
        let a = stage(&f).await?;
        let material = f.material()?;
        let plan = SourceActivationPlan::new(
            material.authority.verify()?.release(),
            &material.review,
            IMAGE,
            &config.source_budget,
        )?;
        let request = Arc::new(OwnerActivationIntent::new_with_credentials(
            &configured_runtime::principal(&f),
            &a,
            &material.review,
            f.revision,
            Uuid::new_v4(),
            &SecretsManager::from_key_bytes([if fault == "key" { 58 } else { 57 }; 32], false),
            vec![CredentialChange::set(
                "source_api",
                "private-no-echo",
                false,
            )],
        )?);
        let checked = || -> Result<Step<'_>> {
            Ok(Step {
                database: &f.db,
                metadata: &f.metadata,
                public: &f.public,
                material: f.material()?,
                intent: request.clone(),
                plan: &plan,
                budget: limit(),
            })
        };
        let prepared = match prepare_checked(checked()?).await? {
            PreparedOrRecovery::Prepared(p) => p,
            _ => bail!("prepared"),
        };
        let start = match claim_checked(checked()?, &prepared).await? {
            StartOrRecovery::Start(s) => s,
            _ => bail!("start"),
        };
        let retained = source_prepared::retain(&f, &start.native_check(), &plan, limit()).await?;
        let fenced = super::super::quiescence::quiesce_checked(checked()?, &retained).await?;
        let scope = super::super::quiescence::observe_checked(checked()?, &fenced).await?;
        let guard = owner.quiesce(scope).await?;
        if fault == "authority" {
            sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1")
                .bind(f.actor.profile.to_string()).execute(&f.db.pool).await?;
        }
        let before: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(a.configured_instance.unwrap().to_string())
        .fetch_one(&f.db.pool)
        .await?;
        let mut copy = checked()?;
        copy.database = guard.database();
        let error = super::super::state_copy::copy_checked(copy, &guard)
            .await
            .err()
            .context("credential copy must fail")?;
        assert!(!format!("{error:#}").contains("private-no-echo"));
        let after: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(a.configured_instance.unwrap().to_string())
        .fetch_one(&f.db.pool)
        .await?;
        assert_eq!(before, after);
        let head:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(head, (1, 0, 0));
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
            .fetch_one(&f.db.pool)
            .await?;
        assert_eq!(count, 0);
    }
    Ok(())
}
