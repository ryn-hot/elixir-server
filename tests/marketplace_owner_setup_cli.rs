//! Exercise the shipped local command with a disposable database, no server,
//! provider, account session, marketplace peers or inherited user environment.
use anyhow::Result;
use elixir_server::{
    auth::home_profiles::HomeProfileRepository,
    config::DatabaseConfig,
    db::Database,
    marketplace::preferences::{Preferences, local_setup::connect_existing},
};
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};
use uuid::Uuid;

async fn command(url: Option<&str>, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_marketplace-owner-setup"));
    command.env_clear().kill_on_drop(true).args(args);
    if let Some(url) = url {
        command.env("ELIXIR_MARKETPLACE_SETUP_DATABASE_URL", url);
    }
    Ok(timeout(Duration::from_secs(15), command.output()).await??)
}
async fn home(database: &Database) -> Result<Uuid> {
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id,email,password_hash) VALUES ($1,$2,'local setup fixture')")
        .bind(user.to_string())
        .bind(format!("{user}@setup.example.test"))
        .execute(&database.pool)
        .await?;
    Ok(HomeProfileRepository::new(&database.pool)
        .ensure_owner_home(user)
        .await?
        .home
        .id)
}

#[tokio::test]
async fn actual_cli_binds_one_home_retries_exactly_and_refuses_replacement() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}",
        directory.path().join("server.sqlite").display()
    );
    let database = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let first = home(&database).await?.to_string();
    let second = home(&database).await?.to_string();
    database.pool.close().await;
    let mutation = Uuid::new_v4().to_string();
    let args = ["--home-id", &first, "--mutation-id", &mutation];
    let output = command(Some(&url), &args).await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        receipt,
        serde_json::json!({"home_id": first, "revision": 1})
    );
    assert!(output.stderr.is_empty());
    let retry = command(Some(&url), &args).await?;
    assert!(retry.status.success());
    assert_eq!(retry.stdout, output.stdout);
    let replacement = command(
        Some(&url),
        &["--home-id", &second, "--mutation-id", &mutation],
    )
    .await?;
    assert!(!replacement.status.success());
    assert!(replacement.stdout.is_empty());
    let new_mutation = Uuid::new_v4().to_string();
    assert!(
        !command(
            Some(&url),
            &["--home-id", &first, "--mutation-id", &new_mutation]
        )
        .await?
        .status
        .success()
    );
    let database = connect_existing(&url).await?;
    let (home, revision, json, approved_by): (String, i64, String, Option<String>) =
        sqlx::query_as("SELECT home_id,revision,preferences_json,approved_by_user_id FROM marketplace_preferences WHERE singleton=1")
            .fetch_one(&database.pool).await?;
    assert_eq!(home, first);
    assert_eq!(revision, 1);
    assert_eq!(
        serde_json::from_str::<Preferences>(&json)?,
        Preferences::default()
    );
    assert_eq!(
        approved_by, None,
        "local binding must not forge an owner's consent"
    );
    database.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn actual_cli_help_and_failures_do_not_create_databases_or_echo_credentials() -> Result<()> {
    let help = command(None, &["--help"]).await?;
    assert!(help.status.success());
    assert!(help.stderr.is_empty());
    let missing = command(None, &[]).await?;
    assert!(!missing.status.success());
    let home = Uuid::new_v4().to_string();
    let mutation = Uuid::new_v4().to_string();
    let args = ["--home-id", &home, "--mutation-id", &mutation];
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("never-created.sqlite");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(!command(Some(&url), &args).await?.status.success());
    assert!(!path.exists());
    let secret = "never-echo-this-local-setup-secret";
    let output = command(
        Some(&format!("unsupported://operator:{secret}@127.0.0.1/test")),
        &args,
    )
    .await?;
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    assert!(output.stdout.is_empty());
    Ok(())
}
