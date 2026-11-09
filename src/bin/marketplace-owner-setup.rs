//! Local database-operator command. Never starts a server, worker or provider.
use anyhow::{Context, Result, ensure};
use elixir_server::marketplace::preferences::local_setup::{bind_initial_home, connect_existing};
use std::{collections::BTreeMap, ffi::OsString, time::Duration};
use tokio::time::timeout;
use uuid::Uuid;

const USAGE: &str = "usage: marketplace-owner-setup --home-id UUID --mutation-id UUID\n\
Reads ELIXIR_MARKETPLACE_SETUP_DATABASE_URL. Requires direct access to an existing,\n\
migrated server database. Binds pristine marketplace settings only; never grants\n\
contribution/paid-support consent, changes an existing binding or starts networking.\n\
Keep the mutation UUID for an exact retry after an uncertain commit.";

#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    home: Uuid,
    mutation: Uuid,
}
fn arguments(values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>> {
    let mut values = values.peekable();
    let mut parsed = BTreeMap::new();
    while let Some(flag) = values.next() {
        if flag == "--help" || flag == "-h" {
            ensure!(
                parsed.is_empty() && values.peek().is_none(),
                "help must be used alone"
            );
            return Ok(None);
        }
        let flag = flag
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid argument name"))?;
        ensure!(
            matches!(flag.as_str(), "--home-id" | "--mutation-id"),
            "unknown argument; {USAGE}"
        );
        let value = values.next().context("argument requires a UUID")?;
        let value = value.to_str().context("argument requires a UUID")?;
        let value = Uuid::parse_str(value).context("argument requires a UUID")?;
        ensure!(!value.is_nil(), "argument requires a non-nil UUID");
        ensure!(parsed.insert(flag, value).is_none(), "duplicate argument");
    }
    ensure!(parsed.len() == 2, "missing required arguments; {USAGE}");
    Ok(Some(Arguments {
        home: parsed["--home-id"],
        mutation: parsed["--mutation-id"],
    }))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Do not print chained SQL/connection errors or the URL/credentials.
        eprintln!("marketplace owner setup failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let Some(args) = arguments(std::env::args_os().skip(1))? else {
        println!("{USAGE}");
        return Ok(());
    };
    let url = std::env::var("ELIXIR_MARKETPLACE_SETUP_DATABASE_URL")
        .context("ELIXIR_MARKETPLACE_SETUP_DATABASE_URL must name the existing server database")?;
    let database = timeout(Duration::from_secs(10), connect_existing(&url))
        .await
        .context("setup database connection deadline")??;
    let result = timeout(Duration::from_secs(10), bind_initial_home(&database, args.home, args.mutation))
        .await.context("setup commit outcome is unknown; retry only with the same home and mutation UUIDs")?
        .context("setup refused or commit failed; inspect settings before retrying with the same home and mutation UUIDs");
    timeout(Duration::from_secs(5), database.pool.close())
        .await
        .context("setup connection shutdown deadline; inspect settings before retrying")?;
    println!("{}", serde_json::to_string(&result?)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(values: &[&str]) -> Result<Option<Arguments>> {
        arguments(values.iter().map(OsString::from))
    }
    #[test]
    fn help_is_explicit_and_requires_no_database() -> Result<()> {
        assert_eq!(parse(&["--help"])?, None);
        assert!(parse(&["--help", "--home-id"]).is_err());
        Ok(())
    }
    #[test]
    fn only_exact_non_nil_setup_arguments_are_accepted() -> Result<()> {
        let home = Uuid::new_v4().to_string();
        let mutation = Uuid::new_v4().to_string();
        let valid = ["--home-id", &home, "--mutation-id", &mutation];
        let args = parse(&valid)?.unwrap();
        assert_eq!(args.home.to_string(), home);
        assert_eq!(args.mutation.to_string(), mutation);
        for invalid in [
            vec![],
            vec!["--home-id", &home],
            vec!["--home-id", &home, "--home-id", &home],
            vec![
                "--home-id",
                "00000000-0000-0000-0000-000000000000",
                "--mutation-id",
                &mutation,
            ],
            vec!["--home-id", &home, "--mutation-id", "not-a-uuid"],
            vec!["--database-url", "never-print-secret"],
        ] {
            assert!(parse(&invalid).is_err());
        }
        Ok(())
    }
}
