use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::{
    AnimeSemanticCorpusReplayConfig, run_anime_semantic_corpus_replay,
};

const USAGE: &str = "usage: anime-inference-semantic-replay \
  --corpus PATH --recorded-report PATH --output PATH [--baseline-failures-only]";

struct Arguments {
    paths: BTreeMap<String, PathBuf>,
    baseline_failures_only: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("anime semantic replay failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary = run_anime_semantic_corpus_replay(AnimeSemanticCorpusReplayConfig {
        corpus_path: required_path(&arguments.paths, "--corpus")?,
        recorded_report_path: required_path(&arguments.paths, "--recorded-report")?,
        output_path: required_path(&arguments.paths, "--output")?,
        baseline_failures_only: arguments.baseline_failures_only,
    })
    .await?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

fn parse_arguments(mut arguments: impl Iterator<Item = OsString>) -> Result<Arguments> {
    let allowed = ["--corpus", "--recorded-report", "--output"];
    let mut parsed = BTreeMap::new();
    let mut baseline_failures_only = false;
    while let Some(raw_flag) = arguments.next() {
        if raw_flag == "--help" || raw_flag == "-h" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let flag = raw_flag
            .into_string()
            .map_err(|_| anyhow::anyhow!("argument name is not valid UTF-8"))?;
        if flag == "--baseline-failures-only" {
            ensure!(!baseline_failures_only, "duplicate argument");
            baseline_failures_only = true;
            continue;
        }
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
    Ok(Arguments {
        paths: parsed,
        baseline_failures_only,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alm9_replay_cli_accepts_exact_inputs_and_optional_focus() {
        let arguments = [
            "--corpus",
            "corpus.json",
            "--recorded-report",
            "recorded.json",
            "--output",
            "replay.json",
            "--baseline-failures-only",
        ]
        .into_iter()
        .map(OsString::from);
        let parsed = parse_arguments(arguments).unwrap();
        assert!(parsed.baseline_failures_only);
        assert_eq!(parsed.paths.len(), 3);
    }

    #[test]
    fn alm9_replay_cli_rejects_unknown_arguments() {
        let error = parse_arguments([OsString::from("--model")].into_iter())
            .err()
            .expect("unknown argument must fail");
        assert!(error.to_string().contains("unknown argument"));
    }
}
