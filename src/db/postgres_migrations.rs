use std::borrow::Cow;

use anyhow::{Result, bail};
use sqlx::migrate::{Migration, Migrator};

use super::MIGRATOR;

const POSTGRES_0042: &str = include_str!("postgres/0042_layered_scraper_source_registry.sql");
const POSTGRES_0056: &str = include_str!("postgres/0056_live_streaming.sql");

const ADAPTED_VERSIONS: &[i64] = &[1, 6, 8, 14, 24, 26, 41, 42, 47, 50, 56];
const SQLITE_BOOLEAN_DEFAULT_FALSE: &str = "BOOLEAN NOT NULL DEFAULT 0";
const SQLITE_BOOLEAN_DEFAULT_TRUE: &str = "BOOLEAN NOT NULL DEFAULT 1";

pub(super) fn migrator() -> Result<Migrator> {
    let mut adapted_versions = Vec::new();
    let mut migrations = Vec::with_capacity(MIGRATOR.iter().len());

    for migration in MIGRATOR.iter() {
        let (migration, adapted) = adapt(migration)?;
        if adapted {
            adapted_versions.push(migration.version);
        }
        migrations.push(migration);
    }

    if adapted_versions != ADAPTED_VERSIONS {
        bail!(
            "PostgreSQL migration adapter version set changed: expected {ADAPTED_VERSIONS:?}, got {adapted_versions:?}"
        );
    }

    Ok(Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: MIGRATOR.ignore_missing,
        locking: MIGRATOR.locking,
    })
}

fn adapt(migration: &Migration) -> Result<(Migration, bool)> {
    let original_sql = migration.sql.as_ref();
    let mut sql = original_sql
        .replace(
            SQLITE_BOOLEAN_DEFAULT_FALSE,
            "BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .replace(SQLITE_BOOLEAN_DEFAULT_TRUE, "BOOLEAN NOT NULL DEFAULT TRUE");

    match migration.version {
        24 => {
            replace_exactly_once(
                &mut sql,
                "INSERT OR IGNORE INTO",
                "INSERT INTO",
                migration.version,
            )?;
            replace_exactly_once(
                &mut sql,
                "FROM providers;",
                "FROM providers\nON CONFLICT (provider_id) DO NOTHING;",
                migration.version,
            )?;
        }
        42 => sql = POSTGRES_0042.to_string(),
        56 => sql = POSTGRES_0056.to_string(),
        _ => {}
    }

    for forbidden in [
        SQLITE_BOOLEAN_DEFAULT_FALSE,
        SQLITE_BOOLEAN_DEFAULT_TRUE,
        "INSERT OR IGNORE",
    ] {
        if sql.contains(forbidden) {
            bail!(
                "PostgreSQL migration {} retains unsupported SQL fragment {forbidden:?}",
                migration.version
            );
        }
    }

    let adapted = sql != original_sql;
    if adapted != ADAPTED_VERSIONS.contains(&migration.version) {
        bail!(
            "PostgreSQL migration {} changed outside the explicit adapter allowlist",
            migration.version
        );
    }

    Ok((
        Migration::new(
            migration.version,
            migration.description.clone(),
            migration.migration_type,
            Cow::Owned(sql),
        ),
        adapted,
    ))
}

fn replace_exactly_once(
    sql: &mut String,
    source: &str,
    replacement: &str,
    version: i64,
) -> Result<()> {
    let occurrences = sql.matches(source).count();
    if occurrences != 1 {
        bail!(
            "PostgreSQL migration {version} expected one {source:?} fragment, found {occurrences}"
        );
    }
    *sql = sql.replacen(source, replacement, 1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn adapter_preserves_order_metadata_and_explicit_change_set() -> Result<()> {
        let postgres = migrator()?;
        assert_eq!(postgres.iter().len(), MIGRATOR.iter().len());

        let changed: Vec<i64> = MIGRATOR
            .iter()
            .zip(postgres.iter())
            .filter_map(|(sqlite, postgres)| {
                assert_eq!(sqlite.version, postgres.version);
                assert_eq!(sqlite.description, postgres.description);
                assert_eq!(sqlite.migration_type, postgres.migration_type);
                (sqlite.sql != postgres.sql).then_some(sqlite.version)
            })
            .collect();
        assert_eq!(changed, ADAPTED_VERSIONS);
        assert!(postgres.iter().all(|migration| {
            !migration.sql.contains(SQLITE_BOOLEAN_DEFAULT_FALSE)
                && !migration.sql.contains(SQLITE_BOOLEAN_DEFAULT_TRUE)
                && !migration.sql.contains("INSERT OR IGNORE")
        }));
        Ok(())
    }
}
