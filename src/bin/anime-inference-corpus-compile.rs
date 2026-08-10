use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::corpus_compiler::{
    AnimeCorpusCompileConfig, compile_anime_corpus_blueprint,
};

const USAGE: &str = "usage: anime-inference-corpus-compile --blueprint PATH --output-root PATH";

fn main() {
    if let Err(error) = run() {
        eprintln!("anime inference corpus compilation failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary = compile_anime_corpus_blueprint(AnimeCorpusCompileConfig {
        blueprint_path: required_path(&arguments, "--blueprint")?,
        output_root: required_path(&arguments, "--output-root")?,
    })?;
    println!(
        "{}",
        serde_json::to_string(&summary).context("encoding corpus compiler summary")?
    );
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
    let allowed = ["--blueprint", "--output-root"];
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
            .ok_or_else(|| anyhow::anyhow!("{flag} requires a path; {USAGE}"))?;
        ensure!(!value.is_empty(), "{flag} path is empty");
        ensure!(
            parsed.insert(flag.clone(), PathBuf::from(value)).is_none(),
            "duplicate argument {flag:?}"
        );
    }
    if parsed.len() != allowed.len() {
        bail!("missing required arguments; {USAGE}");
    }
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
    fn compiler_cli_requires_exact_arguments() {
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
        assert!(
            parse_arguments(
                [
                    OsString::from("--blueprint"),
                    OsString::from("one.json"),
                    OsString::from("--blueprint"),
                    OsString::from("two.json"),
                ]
                .into_iter()
            )
            .is_err()
        );
        assert!(
            parse_arguments(
                [
                    OsString::from("--blueprint"),
                    OsString::from("blueprint.json"),
                    OsString::from("--output-root"),
                    OsString::from("compiled"),
                ]
                .into_iter()
            )
            .is_ok()
        );
    }
}
