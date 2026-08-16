use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, ensure};
use elixir_server::acquisition::anime_qualification::training_dataset::{
    AnimeIntegratedDiagnosticCompileConfig, compile_anime_integrated_diagnostic_corpus,
};

const USAGE: &str = "usage: anime-semantic-integrated-diagnostic-corpus \
  --source PATH --output-root PATH";

fn main() {
    if let Err(error) = run() {
        eprintln!("anime semantic integrated diagnostic corpus failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary =
        compile_anime_integrated_diagnostic_corpus(AnimeIntegratedDiagnosticCompileConfig {
            source_path: required_path(&arguments, "--source")?,
            output_root: required_path(&arguments, "--output-root")?,
        })?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
    let allowed = ["--source", "--output-root"];
    let mut parsed = BTreeMap::new();
    while let Some(raw_flag) = arguments.next() {
        if raw_flag == "--help" || raw_flag == "-h" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let flag = raw_flag
            .into_string()
            .map_err(|_| anyhow::anyhow!("argument name is not valid UTF-8"))?;
        ensure!(
            allowed.contains(&flag.as_str()),
            "unknown argument {flag:?}; {USAGE}"
        );
        let value = arguments
            .next()
            .with_context(|| format!("{flag} requires a value; {USAGE}"))?;
        ensure!(!value.is_empty(), "{flag} value is empty");
        ensure!(
            parsed.insert(flag.clone(), PathBuf::from(value)).is_none(),
            "duplicate argument {flag:?}"
        );
    }
    ensure!(
        allowed.iter().all(|flag| parsed.contains_key(*flag)),
        "missing required arguments; {USAGE}"
    );
    Ok(parsed)
}

fn required_path(arguments: &BTreeMap<String, PathBuf>, name: &str) -> Result<PathBuf> {
    arguments
        .get(name)
        .cloned()
        .with_context(|| format!("missing {name}; {USAGE}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrated_diagnostic_cli_requires_both_paths() {
        assert!(
            parse_arguments(
                [
                    OsString::from("--source"),
                    OsString::from("source.json"),
                    OsString::from("--output-root"),
                    OsString::from("output"),
                ]
                .into_iter()
            )
            .is_ok()
        );
        assert!(
            parse_arguments(
                [OsString::from("--source"), OsString::from("source.json"),].into_iter()
            )
            .is_err()
        );
    }
}
