use super::*;
use crate::{
    config::DatabaseConfig,
    marketplace::{
        grant_store::tests::owner,
        preferences::{Change, ConnectionMetering, load_as, load_internal, save_as},
        worker::PARTICIPATION_DISCLOSURE_VERSION,
    },
};
use chrono::Utc;
use elixir_marketplace::policy::ContributionMode;

async fn create(path: &std::path::Path) -> Result<Database> {
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", path.display()),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok(database)
}

#[tokio::test]
async fn trusted_multi_home_setup_preserves_unaccepted_defaults_and_remote_home_boundary()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("homes.sqlite");
    let database = create(&path).await?;
    let selected = owner(&database).await?;
    let other = owner(&database).await?;
    assert!(load_as(&database, &selected, Utc::now()).await.is_err());
    assert!(load_as(&database, &other, Utc::now()).await.is_err());
    let mutation = Uuid::new_v4();
    let receipt = bind_initial_home(&database, selected.home, mutation).await?;
    assert_eq!(
        receipt,
        BindingReceipt {
            home_id: selected.home,
            revision: 1
        }
    );
    assert_eq!(
        bind_initial_home(&database, selected.home, mutation).await?,
        receipt
    );
    let saved = load_as(&database, &selected, Utc::now()).await?;
    assert_eq!(saved.revision, 1);
    assert_eq!(saved.preferences, Preferences::default());
    assert_eq!(saved.preferences.metering, ConnectionMetering::Unknown);
    assert_eq!(saved.preferences.contribution_disclosure_version, None);
    assert_eq!(saved.preferences.issuer_disclosure_version, None);
    assert!(!saved.preferences.paid_support_consent);
    assert!(load_as(&database, &other, Utc::now()).await.is_err());
    let change = Change {
        expected_revision: 1,
        mutation_id: Uuid::new_v4(),
        preferences: Preferences {
            contribution: ContributionMode::Off,
            ..Default::default()
        },
    };
    assert!(
        save_as(&database, &other, &change, Utc::now())
            .await
            .is_err()
    );
    let off = save_as(&database, &selected, &change, Utc::now()).await?;
    assert!(
        bind_initial_home(&database, selected.home, mutation)
            .await
            .is_err()
    );
    assert!(
        bind_initial_home(&database, other.home, Uuid::new_v4())
            .await
            .is_err()
    );
    database.pool.close().await;
    let reopened = connect_existing(&format!("sqlite://{}", path.display())).await?;
    assert_eq!(load_as(&reopened, &selected, Utc::now()).await?, off);
    assert_eq!(off.revision, 2);
    assert_eq!(off.preferences.contribution, ContributionMode::Off);
    reopened.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn local_setup_cannot_replace_remote_consent_or_existing_bindings() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = create(&directory.path().join("bound.sqlite")).await?;
    let selected = owner(&database).await?;
    let change = Change {
        expected_revision: 0,
        mutation_id: Uuid::new_v4(),
        preferences: Preferences {
            contribution_disclosure_version: Some(PARTICIPATION_DISCLOSURE_VERSION),
            metering: ConnectionMetering::Unmetered,
            ..Default::default()
        },
    };
    let saved = save_as(&database, &selected, &change, Utc::now()).await?;
    let other = owner(&database).await?;
    for home in [selected.home, other.home] {
        assert!(
            bind_initial_home(&database, home, change.mutation_id)
                .await
                .is_err()
        );
        assert!(
            bind_initial_home(&database, home, Uuid::new_v4())
                .await
                .is_err()
        );
    }
    assert_eq!(load_as(&database, &selected, Utc::now()).await?, saved);
    Ok(())
}

#[tokio::test]
async fn missing_home_owner_profile_or_membership_cannot_bind_settings() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = create(&directory.path().join("authority.sqlite")).await?;
    let actor = owner(&database).await?;
    assert!(
        bind_initial_home(&database, Uuid::nil(), Uuid::new_v4())
            .await
            .is_err()
    );
    assert!(
        bind_initial_home(&database, actor.home, Uuid::nil())
            .await
            .is_err()
    );
    assert!(
        bind_initial_home(&database, Uuid::new_v4(), Uuid::new_v4())
            .await
            .is_err()
    );
    sqlx::query("UPDATE home_members SET role='admin' WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&database.pool)
        .await?;
    assert!(
        bind_initial_home(&database, actor.home, Uuid::new_v4())
            .await
            .is_err()
    );
    sqlx::query("UPDATE home_members SET role='owner' WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&database.pool)
        .await?;
    // Removing a profile cascades sessions/revisions, not the ownership row.
    sqlx::query("DELETE FROM profiles WHERE home_id=$1")
        .bind(actor.home.to_string())
        .execute(&database.pool)
        .await?;
    assert!(
        bind_initial_home(&database, actor.home, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(load_internal(&database).await?.revision, 0);
    Ok(())
}

#[tokio::test]
async fn local_setup_never_repairs_missing_or_corrupt_authority() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = create(&directory.path().join("damaged.sqlite")).await?;
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, Uuid::new_v4()).await?;
    sqlx::query("UPDATE marketplace_preferences SET preferences_json='broken'")
        .execute(&database.pool)
        .await?;
    assert!(
        bind_initial_home(&database, actor.home, Uuid::new_v4())
            .await
            .is_err()
    );
    let json: String = sqlx::query_scalar("SELECT preferences_json FROM marketplace_preferences")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(json, "broken");
    sqlx::query("DELETE FROM marketplace_preferences")
        .execute(&database.pool)
        .await?;
    assert!(
        bind_initial_home(&database, actor.home, Uuid::new_v4())
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
async fn two_database_operators_cannot_claim_one_initial_binding() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("concurrent.sqlite");
    let database = create(&path).await?;
    let first = owner(&database).await?;
    let second = owner(&database).await?;
    let other = connect_existing(&format!("sqlite://{}", path.display())).await?;
    let first_mutation = Uuid::new_v4();
    let second_mutation = Uuid::new_v4();
    let (a, b) = tokio::join!(
        bind_initial_home(&database, first.home, first_mutation),
        bind_initial_home(&other, second.home, second_mutation)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let (winner, mutation, loser) = if a.is_ok() {
        (&first, first_mutation, &second)
    } else {
        (&second, second_mutation, &first)
    };
    assert_eq!(
        bind_initial_home(&database, winner.home, mutation)
            .await?
            .revision,
        1
    );
    assert_eq!(load_as(&database, winner, Utc::now()).await?.revision, 1);
    assert!(load_as(&database, loser, Utc::now()).await.is_err());
    other.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn setup_connector_cannot_create_database_files_or_print_supplied_credentials() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let missing = directory.path().join("must-not-create.sqlite");
    for suffix in ["", "?mode=rwc"] {
        assert!(
            connect_existing(&format!("sqlite://{}{suffix}", missing.display()))
                .await
                .is_err()
        );
        assert!(!missing.exists());
    }
    assert!(connect_existing("sqlite::memory:").await.is_err());
    assert!(connect_existing("sqlite:any?mode=memory").await.is_err());
    let secret = "never-echo-this-setup-password";
    let error =
        match connect_existing(&format!("not-a-driver://operator:{secret}@127.0.0.1/test")).await {
            Ok(_) => panic!("unsupported driver accepted"),
            Err(error) => error,
        };
    assert!(!format!("{error:#}").contains(secret));
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL owner-setup database integration"]
async fn postgres_local_setup_serializes_initial_claims_and_keeps_remote_preferences() -> Result<()>
{
    let url = std::env::var("ELIXIR_MARKETPLACE_TEST_POSTGRES_OWNER_SETUP_URL")
        .context("requires a fresh dedicated local PostgreSQL owner-setup database")?;
    let parsed = reqwest::Url::parse(&url)?;
    ensure!(
        parsed.host_str() == Some("127.0.0.1")
            && parsed.path() == "/elixir_marketplace_owner_setup_test"
            && matches!(parsed.scheme(), "postgres" | "postgresql"),
        "requires dedicated loopback owner-setup test database"
    );
    let database = connect_existing(&url).await?;
    let tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
            .fetch_one(&database.pool)
            .await?;
    ensure!(
        tables == 0,
        "PostgreSQL owner-setup test database must be empty"
    );
    database.run_migrations().await?;
    let first = owner(&database).await?;
    let second = owner(&database).await?;
    let other = connect_existing(&url).await?;
    let a_id = Uuid::new_v4();
    let b_id = Uuid::new_v4();
    let (a, b) = tokio::join!(
        bind_initial_home(&database, first.home, a_id),
        bind_initial_home(&other, second.home, b_id)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let (winner, mutation, loser) = if a.is_ok() {
        (&first, a_id, &second)
    } else {
        (&second, b_id, &first)
    };
    let receipt = bind_initial_home(&other, winner.home, mutation).await?;
    assert_eq!(receipt.revision, 1);
    assert!(load_as(&database, loser, Utc::now()).await.is_err());
    let off = save_as(
        &database,
        winner,
        &Change {
            expected_revision: 1,
            mutation_id: Uuid::new_v4(),
            preferences: Preferences {
                contribution: ContributionMode::Off,
                ..Default::default()
            },
        },
        Utc::now(),
    )
    .await?;
    assert!(
        bind_initial_home(&other, winner.home, mutation)
            .await
            .is_err()
    );
    database.pool.close().await;
    other.pool.close().await;
    let reopened = connect_existing(&url).await?;
    assert_eq!(load_as(&reopened, winner, Utc::now()).await?, off);
    reopened.pool.close().await;
    Ok(())
}
