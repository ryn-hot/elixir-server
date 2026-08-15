use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::{
    AnimeSemanticCorpusRunConfig, run_anime_semantic_corpus,
};
use elixir_server::anime_matching::ANIME_MATCH_PROMPT_REVISION;

const USAGE: &str = "usage: anime-inference-semantic-corpus \
  --corpus PATH --manifest PATH --runtime-profile PATH --model PATH \
  --runtime-artifact PATH --output PATH [--semantic-prompt-revision REVISION] \
  [--baseline-failures-only]";

struct Arguments {
    paths: BTreeMap<String, PathBuf>,
    semantic_prompt_revision: String,
    baseline_failures_only: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("anime semantic corpus failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary = run_anime_semantic_corpus(AnimeSemanticCorpusRunConfig {
        corpus_path: required_path(&arguments.paths, "--corpus")?,
        manifest_path: required_path(&arguments.paths, "--manifest")?,
        runtime_profile_path: required_path(&arguments.paths, "--runtime-profile")?,
        model_path: required_path(&arguments.paths, "--model")?,
        runtime_artifact_path: required_path(&arguments.paths, "--runtime-artifact")?,
        output_path: required_path(&arguments.paths, "--output")?,
        semantic_prompt_revision: arguments.semantic_prompt_revision,
        baseline_failures_only: arguments.baseline_failures_only,
    })
    .await?;
    println!("{}", serde_json::to_string(&summary)?);
    ensure!(
        summary.failed == 0,
        "{} corpus cases failed",
        summary.failed
    );
    Ok(())
}

fn parse_arguments(mut arguments: impl Iterator<Item = OsString>) -> Result<Arguments> {
    let allowed = [
        "--corpus",
        "--manifest",
        "--runtime-profile",
        "--model",
        "--runtime-artifact",
        "--output",
    ];
    let mut parsed = BTreeMap::new();
    let mut semantic_prompt_revision = None;
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
        if flag == "--semantic-prompt-revision" {
            ensure!(semantic_prompt_revision.is_none(), "duplicate argument");
            let value = arguments
                .next()
                .ok_or_else(|| anyhow::anyhow!("{flag} requires a value; {USAGE}"))?
                .into_string()
                .map_err(|_| anyhow::anyhow!("prompt revision is not valid UTF-8"))?;
            ensure!(!value.trim().is_empty(), "prompt revision is empty");
            semantic_prompt_revision = Some(value);
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
        semantic_prompt_revision: semantic_prompt_revision
            .unwrap_or_else(|| ANIME_MATCH_PROMPT_REVISION.to_string()),
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

    fn required_arguments() -> Vec<OsString> {
        [
            "--corpus",
            "corpus.json",
            "--manifest",
            "manifest.json",
            "--runtime-profile",
            "profile.json",
            "--model",
            "model.gguf",
            "--runtime-artifact",
            "runtime.exe",
            "--output",
            "report.json",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    #[test]
    fn alm9_semantic_corpus_cli_accepts_focused_failure_mode() {
        let mut arguments = required_arguments();
        arguments.push(OsString::from("--baseline-failures-only"));

        let parsed = parse_arguments(arguments.into_iter()).unwrap();

        assert!(parsed.baseline_failures_only);
        assert_eq!(parsed.semantic_prompt_revision, ANIME_MATCH_PROMPT_REVISION);
        assert_eq!(parsed.paths.len(), 6);
    }

    #[test]
    fn alm9_semantic_corpus_cli_accepts_frozen_selector_prompt_revision() {
        let mut arguments = required_arguments();
        arguments.extend([
            OsString::from("--semantic-prompt-revision"),
            OsString::from("anime-semantic-evidence-v4"),
        ]);

        let parsed = parse_arguments(arguments.into_iter()).unwrap();

        assert_eq!(
            parsed.semantic_prompt_revision,
            "anime-semantic-evidence-v4"
        );
    }

    #[test]
    fn alm9_semantic_corpus_cli_rejects_duplicate_focused_flag() {
        let mut arguments = required_arguments();
        arguments.extend([
            OsString::from("--baseline-failures-only"),
            OsString::from("--baseline-failures-only"),
        ]);

        assert!(parse_arguments(arguments.into_iter()).is_err());
    }
}
