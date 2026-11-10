//! Existing-file-only operator setup; never use the driver's infallible URL
//! serializer on user-supplied SQLite paths (0.8.6 panics for :memory:).
use anyhow::{Result, ensure};
use sqlx::sqlite::SqliteConnectOptions;
use std::{path::Path, str::FromStr};

pub(super) async fn existing_url(value: &str) -> Result<reqwest::Url> {
    let tail = value
        .strip_prefix("sqlite://")
        .or_else(|| value.strip_prefix("sqlite:"))
        .ok_or_else(|| anyhow::anyhow!("invalid setup SQLite URL"))?;
    let options = SqliteConnectOptions::from_str(value)
        .map_err(|_| anyhow::anyhow!("invalid setup SQLite URL"))?;
    // Use a constant URL only to parse the already-validated query. Filenames
    // are not URL authorities: ':'/'@'/brackets must never become a port/login.
    let mut query = reqwest::Url::parse("sqlite://setup")
        .map_err(|_| anyhow::anyhow!("invalid setup SQLite URL"))?;
    query.set_query(value.split_once('?').map(|(_, query)| query));
    let pairs = query.query_pairs().into_owned().collect::<Vec<_>>();
    ensure!(
        tail.split('?').next() != Some(":memory:")
            && options.get_filename() != Path::new(":memory:")
            && !pairs
                .iter()
                .any(|(key, value)| key == "mode" && value == "memory"),
        "setup requires an existing persistent database"
    );
    // Resolve existing files before opening them. Absolute, fully escaped paths
    // also prevent SQLite's special relative `file:` URI names being re-read
    // as memory databases or a different target. Never create a path here.
    let path = tokio::fs::canonicalize(options.get_filename())
        .await
        .map_err(|_| anyhow::anyhow!("setup SQLite database must already exist"))?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|_| anyhow::anyhow!("setup SQLite database must already exist"))?;
    ensure!(
        metadata.is_file(),
        "setup SQLite database must be a regular file"
    );
    let filename = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid setup SQLite filename"))?;
    let mut normalized =
        reqwest::Url::parse(&format!("sqlite://{}", urlencoding::encode(filename)))
            .map_err(|_| anyhow::anyhow!("invalid setup SQLite URL"))?;
    // Preserve read-only and validated cache/immutable/VFS options. SQLx keeps
    // read_only once any ro mode was supplied; preserve that duplicate-mode
    // behavior, but never preserve rwc/create, even after the existence check.
    let read_only = pairs
        .iter()
        .any(|(key, value)| key == "mode" && value == "ro");
    normalized
        .query_pairs_mut()
        .extend_pairs(
            pairs
                .iter()
                .filter(|(key, _)| key != "mode")
                .map(|(key, value)| (key, value)),
        )
        .append_pair("mode", if read_only { "ro" } else { "rw" });
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Connection, sqlite::SqliteConnection};

    #[tokio::test]
    async fn memory_missing_and_non_file_targets_fail_without_creation_or_secret_echo() -> Result<()>
    {
        let directory = tempfile::tempdir()?;
        let missing = directory.path().join("must-not-create.sqlite");
        let base = format!(
            "sqlite://{}",
            urlencoding::encode(missing.to_str().unwrap())
        );
        for value in [
            "sqlite::memory:".to_owned(),
            "sqlite://:memory:?mode=rwc".to_owned(),
            "sqlite://%3Amemory%3A".to_owned(),
            "sqlite:any?mode=memory".to_owned(),
            "sqlite:any?mode=memory&mode=rw".to_owned(),
            base.clone(),
            format!("{base}?mode=rwc"),
            format!(
                "sqlite://{}",
                urlencoding::encode(directory.path().to_str().unwrap())
            ),
        ] {
            assert!(existing_url(&value).await.is_err());
            assert!(!missing.exists());
        }
        let secret = "never-echo-this-setup-password";
        for value in [
            format!("sqlite://operator:{secret}@missing.invalid/test"),
            format!("{base}?unknown={secret}"),
            format!("{base}?mode={secret}"),
        ] {
            let error = existing_url(&value).await.unwrap_err();
            assert!(!format!("{error:#}").contains(secret));
            assert!(!missing.exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn existing_escaped_files_roundtrip_and_read_only_stays_read_only() -> Result<()> {
        sqlx::any::install_default_drivers();
        let directory = tempfile::tempdir()?;
        let mut names = vec!["plain.sqlite", "日本語 space #.sqlite"];
        #[cfg(unix)]
        names.extend(["relative:port@name.sqlite", "literal?and#name.sqlite"]);
        for name in names {
            let path = directory.path().join(name);
            let mut creator = SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;
            sqlx::query("CREATE TABLE setup_probe(value BIGINT NOT NULL)")
                .execute(&mut creator)
                .await?;
            sqlx::query("INSERT INTO setup_probe VALUES (7)")
                .execute(&mut creator)
                .await?;
            creator.close().await?;
            let encoded = urlencoding::encode(path.to_str().unwrap());
            for modes in ["mode=rw", "mode=rwc", "mode=ro", "mode=ro&mode=rwc"] {
                let normalized =
                    existing_url(&format!("sqlite://{encoded}?{modes}&cache=private")).await?;
                assert_eq!(
                    normalized
                        .query_pairs()
                        .filter(|(key, _)| key == "mode")
                        .count(),
                    1
                );
                assert!(
                    !normalized
                        .query_pairs()
                        .any(|(key, value)| key == "mode" && value == "rwc")
                );
                let pool = sqlx::any::AnyPoolOptions::new()
                    .max_connections(1)
                    .connect(normalized.as_str())
                    .await?;
                let actual: String =
                    sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name='main'")
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    tokio::fs::canonicalize(actual).await?,
                    tokio::fs::canonicalize(&path).await?
                );
                assert_eq!(
                    sqlx::query_scalar::<_, i64>("SELECT value FROM setup_probe")
                        .fetch_one(&pool)
                        .await?,
                    7
                );
                let update = sqlx::query("UPDATE setup_probe SET value=value")
                    .execute(&pool)
                    .await;
                assert_eq!(update.is_err(), modes.starts_with("mode=ro"));
                pool.close().await;
            }
        }
        Ok(())
    }
}
