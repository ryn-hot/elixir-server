use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, ensure};
use elixir_server::acquisition::anime_qualification::training_dataset::{
    AnimeIntegratedCorpusProfile, AnimeIntegratedDiagnosticCompileConfig,
    compile_anime_integrated_diagnostic_corpus,
};

const USAGE: &str = "usage: anime-semantic-integrated-diagnostic-corpus \
  --source PATH --output-root PATH \
  [--profile clean_validation_diagnostic_v1|clean_acceptance_v1]";

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
            profile: requested_profile(&arguments)?,
        })?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
    let allowed = ["--source", "--output-root", "--profile"];
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
        ["--source", "--output-root"]
            .iter()
            .all(|flag| parsed.contains_key(*flag)),
        "missing required arguments; {USAGE}"
    );
    Ok(parsed)
}

fn requested_profile(
    arguments: &BTreeMap<String, PathBuf>,
) -> Result<AnimeIntegratedCorpusProfile> {
    let Some(value) = arguments.get("--profile") else {
        return Ok(AnimeIntegratedCorpusProfile::CleanValidationDiagnosticV1);
    };
    match value.to_str() {
        Some("clean_validation_diagnostic_v1") => {
            Ok(AnimeIntegratedCorpusProfile::CleanValidationDiagnosticV1)
        }
        Some("clean_acceptance_v1") => Ok(AnimeIntegratedCorpusProfile::CleanAcceptanceV1),
        _ => anyhow::bail!("unsupported integrated corpus profile; {USAGE}"),
    }
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

    #[test]
    fn integrated_diagnostic_cli_accepts_only_fixed_profiles() {
        let accepted = parse_arguments(
            [
                OsString::from("--source"),
                OsString::from("source.json"),
                OsString::from("--output-root"),
                OsString::from("output"),
                OsString::from("--profile"),
                OsString::from("clean_acceptance_v1"),
            ]
            .into_iter(),
        )
        .expect("acceptance arguments");
        assert_eq!(
            requested_profile(&accepted).expect("acceptance profile"),
            AnimeIntegratedCorpusProfile::CleanAcceptanceV1
        );

        let mut rejected = accepted;
        rejected.insert("--profile".to_string(), PathBuf::from("custom"));
        assert!(requested_profile(&rejected).is_err());
    }
}
