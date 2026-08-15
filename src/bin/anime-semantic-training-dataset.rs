use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::training_dataset::{
    AnimeTrainingCompileConfig, compile_anime_training_dataset, validate_anime_training_dataset,
};

const USAGE: &str =
    "usage: anime-semantic-training-dataset <compile|validate> [--source PATH] --output-root PATH";

fn main() {
    if let Err(error) = run() {
        eprintln!("anime semantic training dataset failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let command = arguments
        .next()
        .context(USAGE)?
        .into_string()
        .map_err(|_| anyhow::anyhow!("command is not valid UTF-8"))?;
    let arguments = parse_arguments(arguments)?;
    let output_root = required_path(&arguments, "--output-root")?;
    let summary = match command.as_str() {
        "compile" => compile_anime_training_dataset(AnimeTrainingCompileConfig {
            source_path: required_path(&arguments, "--source")?,
            output_root,
        })?,
        "validate" => {
            ensure!(
                !arguments.contains_key("--source"),
                "validate does not accept --source; {USAGE}"
            );
            validate_anime_training_dataset(&output_root)?
        }
        _ => bail!("unknown command {command:?}; {USAGE}"),
    };
    println!(
        "{}",
        serde_json::to_string(&summary).context("encoding training dataset summary")?
    );
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
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
            matches!(flag.as_str(), "--source" | "--output-root"),
            "unknown argument {flag:?}; {USAGE}"
        );
        let value = arguments
            .next()
            .ok_or_else(|| anyhow::anyhow!("{flag} requires a path; {USAGE}"))?;
        ensure!(!value.is_empty(), "{flag} path is empty");
        ensure!(
            parsed.insert(flag.clone(), PathBuf::from(value)).is_none(),
            "duplicate argument {flag:?}"
        );
    }
    ensure!(
        parsed.contains_key("--output-root"),
        "missing --output-root; {USAGE}"
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
    fn cli_rejects_unknown_and_duplicate_arguments() {
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
        assert!(
            parse_arguments(
                [
                    OsString::from("--output-root"),
                    OsString::from("one"),
                    OsString::from("--output-root"),
                    OsString::from("two"),
                ]
                .into_iter()
            )
            .is_err()
        );
    }
}
