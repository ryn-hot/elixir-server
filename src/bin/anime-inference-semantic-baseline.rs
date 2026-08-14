use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::{
    AnimeSemanticBaselineRunConfig, run_anime_semantic_baseline,
};

const USAGE: &str = "usage: anime-inference-semantic-baseline --corpus PATH --output PATH";

fn main() {
    if let Err(error) = run() {
        eprintln!("anime semantic baseline failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary = run_anime_semantic_baseline(AnimeSemanticBaselineRunConfig {
        corpus_path: required_path(&arguments, "--corpus")?,
        output_path: required_path(&arguments, "--output")?,
    })?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
    let allowed = ["--corpus", "--output"];
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
            parsed.insert(flag, PathBuf::from(value)).is_none(),
            "duplicate argument"
        );
    }
    if !allowed.iter().all(|flag| parsed.contains_key(*flag)) {
        bail!("missing required arguments; {USAGE}");
    }
    Ok(parsed)
}

fn required_path(arguments: &BTreeMap<String, PathBuf>, name: &str) -> Result<PathBuf> {
    let path = arguments
        .get(name)
        .cloned()
        .with_context(|| format!("missing {name}; {USAGE}"))?;
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(std::env::current_dir()
        .context("reading the current directory")?
        .join(path))
}
