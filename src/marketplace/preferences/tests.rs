use super::*;
use crate::config::DatabaseConfig;

async fn connect(path: &std::path::Path) -> Result<Database> {
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", path.display()),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok(database)
}
async fn owner(database: &Database) -> Result<Actor> {
    super::super::grant_store::tests::owner(database).await
}
fn change(revision: i64) -> Change {
    Change {
        expected_revision: revision,
        mutation_id: Uuid::new_v4(),
        preferences: Preferences {
            metering: ConnectionMetering::Unmetered,
            contribution_disclosure_version: Some(PARTICIPATION_DISCLOSURE_VERSION),
            ..Default::default()
        },
    }
}

#[test]
fn closed_preferences_keep_contribution_and_paid_support_consent_separate() -> Result<()> {
    let default = Preferences::default();
    default.validate()?;
    assert_eq!(default.contribution, ContributionMode::Auto);
    assert_eq!(default.metering, ConnectionMetering::Unknown);
    assert_eq!(default.contribution_disclosure_version, None);
    assert!(!default.paid_support_consent);
    let mut approved = change(0).preferences;
    approved.validate()?;
    assert!(
        !approved.paid_support_consent,
        "contribution cannot imply public support"
    );
    approved.paid_support_consent = true;
    assert!(approved.validate().is_err());
    approved.issuer_disclosure_version = Some(ISSUER_DISCLOSURE_VERSION);
    approved.validate()?;
    approved.contribution_disclosure_version = Some(PARTICIPATION_DISCLOSURE_VERSION + 1);
    assert!(approved.validate().is_err());
    let original = serde_json::json!({"expected_revision":0,"mutation_id":Uuid::new_v4(),"preferences":default});
    Change::parse(&serde_json::to_vec(&original)?)?;
    let mut extra = original.clone();
    extra["limits"] = serde_json::json!({"memory":999999});
    assert!(Change::parse(&serde_json::to_vec(&extra)?).is_err());
    let duplicate = serde_json::to_string(&original)?.replacen(
        "\"expected_revision\":0",
        "\"expected_revision\":0,\"expected_revision\":1",
        1,
    );
    assert!(Change::parse(duplicate.as_bytes()).is_err());
    let mut negative = original;
    negative["expected_revision"] = (-1).into();
    assert!(Change::parse(&serde_json::to_vec(&negative)?).is_err());
    Ok(())
}

#[tokio::test]
async fn preferences_survive_restart_deduplicate_exact_mutations_and_preserve_opt_out() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("preferences.sqlite");
    let database = connect(&path).await?;
    let actor = owner(&database).await?;
    let initial = load_as(&database, &actor, Utc::now()).await?;
    assert_eq!(initial.revision, 0);
    assert_eq!(initial.preferences, Preferences::default());
    let approved = change(0);
    let saved = save_as(&database, &actor, &approved, Utc::now()).await?;
    assert_eq!(saved.revision, 1);
    assert_eq!(
        save_as(&database, &actor, &approved, Utc::now()).await?,
        saved
    );
    let reused = Change {
        expected_revision: 0,
        mutation_id: approved.mutation_id,
        preferences: Preferences {
            contribution: ContributionMode::Off,
            ..approved.preferences.clone()
        },
    };
    assert!(
        save_as(&database, &actor, &reused, Utc::now())
            .await
            .is_err()
    );
    let mut opt_out = change(1);
    opt_out.preferences.contribution = ContributionMode::Off;
    let saved = save_as(&database, &actor, &opt_out, Utc::now()).await?;
    assert_eq!(saved.revision, 2);
    assert!(
        save_as(&database, &actor, &approved, Utc::now())
            .await
            .is_err()
    );
    database.pool.close().await;
    let database = connect(&path).await?;
    assert_eq!(load_as(&database, &actor, Utc::now()).await?, saved);
    assert_eq!(load_internal(&database).await?, saved);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_preferences")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 1, "no lifetime preference event history");
    Ok(())
}

#[tokio::test]
async fn stale_owner_scope_and_capability_revision_cannot_change_preferences() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = connect(&directory.path().join("owners.sqlite")).await?;
    let actor = owner(&database).await?;
    save_as(&database, &actor, &change(0), Utc::now()).await?;
    let foreign = owner(&database).await?;
    assert!(load_as(&database, &foreign, Utc::now()).await.is_err());
    assert!(
        save_as(&database, &foreign, &change(1), Utc::now())
            .await
            .is_err()
    );
    sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&database.pool)
        .await?;
    assert!(
        save_as(&database, &actor, &change(1), Utc::now())
            .await
            .is_err()
    );
    assert!(load_as(&database, &actor, Utc::now()).await.is_err());
    assert_eq!(load_internal(&database).await?.revision, 1);
    Ok(())
}

#[tokio::test]
async fn revoked_owner_session_cannot_load_or_mutate_bound_preferences() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = connect(&directory.path().join("revoked.sqlite")).await?;
    let actor = owner(&database).await?;
    save_as(&database, &actor, &change(0), Utc::now()).await?;
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='preferences fixture' WHERE user_id=$1")
        .bind(actor.user.to_string()).execute(&database.pool).await?;
    assert!(load_as(&database, &actor, Utc::now()).await.is_err());
    assert!(
        save_as(&database, &actor, &change(1), Utc::now())
            .await
            .is_err()
    );
    assert_eq!(load_internal(&database).await?.revision, 1);
    Ok(())
}

#[tokio::test]
async fn missing_or_corrupt_preferences_are_not_recreated_as_default_consent() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = connect(&directory.path().join("broken.sqlite")).await?;
    let actor = owner(&database).await?;
    save_as(&database, &actor, &change(0), Utc::now()).await?;
    sqlx::query("UPDATE marketplace_preferences SET preferences_json='broken'")
        .execute(&database.pool)
        .await?;
    assert!(load_internal(&database).await.is_err());
    assert!(
        save_as(&database, &actor, &change(1), Utc::now())
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM marketplace_preferences")
        .execute(&database.pool)
        .await?;
    database.run_migrations().await?;
    assert!(load_internal(&database).await.is_err());
    assert!(
        save_as(&database, &actor, &change(0), Utc::now())
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_preferences")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn an_unbound_multi_home_server_cannot_be_claimed_by_whichever_account_calls_first()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = connect(&directory.path().join("unbound.sqlite")).await?;
    let first = owner(&database).await?;
    let second = owner(&database).await?;
    for actor in [&first, &second] {
        assert!(load_as(&database, actor, Utc::now()).await.is_err());
        assert!(
            save_as(&database, actor, &change(0), Utc::now())
                .await
                .is_err()
        );
    }
    assert_eq!(load_internal(&database).await?.revision, 0);
    Ok(())
}

#[test]
fn observation_rejects_revision_rollback_and_same_revision_substitution() -> Result<()> {
    let mut last = Snapshot {
        revision: 0,
        preferences: Preferences::default(),
    };
    let approved = Snapshot {
        revision: 1,
        preferences: change(0).preferences,
    };
    let observed = accept_observation(&mut last, approved.clone())?;
    assert_eq!(observed.revision, Some(1));
    assert_eq!(observed.metering, Metering::Unmetered);
    assert_eq!(
        observed.accepted_disclosure_version,
        Some(PARTICIPATION_DISCLOSURE_VERSION)
    );
    let substituted = Snapshot {
        revision: 1,
        preferences: Preferences {
            contribution: ContributionMode::Off,
            ..approved.preferences.clone()
        },
    };
    assert!(accept_observation(&mut last, substituted).is_err());
    assert!(
        accept_observation(
            &mut last,
            Snapshot {
                revision: 0,
                preferences: Preferences::default()
            }
        )
        .is_err()
    );
    assert_eq!(last, approved);
    Ok(())
}

#[tokio::test]
async fn independent_pools_cannot_both_commit_one_expected_preference_revision() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("concurrent.sqlite");
    let database = connect(&path).await?;
    let other = connect(&path).await?;
    let actor = owner(&database).await?;
    let first = change(0);
    let mut second = change(0);
    second.preferences.contribution = ContributionMode::Off;
    let (first, second) = tokio::join!(
        save_as(&database, &actor, &first, Utc::now()),
        save_as(&other, &actor, &second, Utc::now()),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(load_internal(&database).await?.revision, 1);
    Ok(())
}

#[tokio::test]
async fn local_observer_follows_committed_choices_and_pauses_on_unknown_or_regressed_storage()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = connect(&directory.path().join("observer.sqlite")).await?;
    let actor = owner(&database).await?;
    let mut approved = change(0);
    approved.preferences.contribution = ContributionMode::On;
    save_as(&database, &actor, &approved, Utc::now()).await?;
    let observer = Observer::start(database.clone()).await?;
    let mut settings = observer.subscribe();
    assert_eq!(settings.borrow().mode, ContributionMode::On);
    let mut off = change(1);
    off.preferences.contribution = ContributionMode::Off;
    save_as(&database, &actor, &off, Utc::now()).await?;
    timeout(Duration::from_secs(16), async {
        loop {
            if settings.borrow_and_update().mode == ContributionMode::Off {
                break;
            }
            settings.changed().await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    sqlx::query("UPDATE marketplace_preferences SET preferences_json='broken'")
        .execute(&database.pool)
        .await?;
    timeout(Duration::from_secs(16), async {
        loop {
            let sample = *settings.borrow_and_update();
            if sample.metering == Metering::Unknown && sample.accepted_disclosure_version.is_none()
            {
                break;
            }
            settings.changed().await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL").execute(&database.pool).await?;
    settings.borrow_and_update();
    timeout(Duration::from_secs(16), settings.changed()).await??;
    assert_eq!(settings.borrow().metering, Metering::Unknown);
    assert_eq!(settings.borrow().accepted_disclosure_version, None);
    drop(observer);
    timeout(Duration::from_secs(1), settings.changed())
        .await?
        .expect_err("observer drop must close its authority source");
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL preferences database integration"]
async fn postgres_preferences_commit_concurrently_survive_restart_and_reject_missing_authority()
-> Result<()> {
    let url = std::env::var("ELIXIR_MARKETPLACE_TEST_POSTGRES_PREFERENCES_URL")
        .context("requires a fresh dedicated local PostgreSQL preferences database")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        parsed.host_str() == Some("127.0.0.1")
            && parsed.path() == "/elixir_marketplace_preferences_test"
            && matches!(parsed.scheme(), "postgres" | "postgresql"),
        "requires dedicated loopback preferences test database"
    );
    let config = DatabaseConfig {
        url,
        max_connections: 2,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    let tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
            .fetch_one(&database.pool)
            .await?;
    anyhow::ensure!(
        tables == 0,
        "PostgreSQL preferences test database must be empty"
    );
    database.run_migrations().await?;
    let actor = owner(&database).await?;
    let other = Database::connect(&config).await?;
    let first = change(0);
    let mut second = change(0);
    second.preferences.contribution = ContributionMode::Off;
    let (a, b) = tokio::join!(
        save_as(&database, &actor, &first, Utc::now()),
        save_as(&other, &actor, &second, Utc::now())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let saved = load_as(&database, &actor, Utc::now()).await?;
    assert_eq!(saved.revision, 1);
    database.pool.close().await;
    other.pool.close().await;
    let restarted = Database::connect(&config).await?;
    restarted.run_migrations().await?;
    assert_eq!(load_as(&restarted, &actor, Utc::now()).await?, saved);
    sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&restarted.pool)
        .await?;
    assert!(
        save_as(&restarted, &actor, &change(1), Utc::now())
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM marketplace_preferences")
        .execute(&restarted.pool)
        .await?;
    restarted.run_migrations().await?;
    assert!(load_internal(&restarted).await.is_err());
    restarted.pool.close().await;
    Ok(())
}
