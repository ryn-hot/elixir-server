use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::anime_matching::{
    AnimeInferenceModelSmokeConfig, run_anime_inference_model_smoke,
};

const USAGE: &str = "usage: anime-inference-model-smoke \
  --runtime-id ID --manifest PATH --runtime-profile PATH --model PATH \
  --runtime-artifact PATH --model-build-report PATH --model-source-lock PATH \
  --qualification-lock PATH --request-corpus PATH --producer-commit SHA \
  --producer-run-id ID --output PATH";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("anime inference model smoke failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let config = AnimeInferenceModelSmokeConfig {
        runtime_id: required_text(&arguments, "--runtime-id")?,
        manifest_path: required_path(&arguments, "--manifest")?,
        runtime_profile_path: required_path(&arguments, "--runtime-profile")?,
        model_path: required_path(&arguments, "--model")?,
        runtime_artifact_path: required_path(&arguments, "--runtime-artifact")?,
        model_build_report_path: required_path(&arguments, "--model-build-report")?,
        model_source_lock_path: required_path(&arguments, "--model-source-lock")?,
        qualification_lock_path: required_path(&arguments, "--qualification-lock")?,
        request_corpus_path: required_path(&arguments, "--request-corpus")?,
        producer_commit: required_text(&arguments, "--producer-commit")?,
        producer_run_id: required_text(&arguments, "--producer-run-id")?,
        output_path: required_path(&arguments, "--output")?,
    };
    let output = config.output_path.clone();
    run_anime_inference_model_smoke(config).await?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "status": "passed",
            "modelSmokeReport": output,
        }))?
    );
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, OsString>> {
    let allowed = [
        "--runtime-id",
        "--manifest",
        "--runtime-profile",
        "--model",
        "--runtime-artifact",
        "--model-build-report",
        "--model-source-lock",
        "--qualification-lock",
        "--request-corpus",
        "--producer-commit",
        "--producer-run-id",
        "--output",
    ];
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
            parsed.insert(flag.clone(), value).is_none(),
            "duplicate argument {flag:?}"
        );
    }
    if parsed.len() != allowed.len() {
        bail!("missing required arguments; {USAGE}");
    }
    Ok(parsed)
}

fn required_path(arguments: &BTreeMap<String, OsString>, name: &str) -> Result<PathBuf> {
    arguments
        .get(name)
        .cloned()
        .map(PathBuf::from)
        .with_context(|| format!("missing {name}; {USAGE}"))
}

fn required_text(arguments: &BTreeMap<String, OsString>, name: &str) -> Result<String> {
    arguments
        .get(name)
        .cloned()
        .with_context(|| format!("missing {name}; {USAGE}"))?
        .into_string()
        .map_err(|_| anyhow::anyhow!("{name} is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_arguments() -> Vec<OsString> {
        [
            ("--runtime-id", "macos-x86_64-metal-cpu"),
            ("--manifest", "manifest.json"),
            ("--runtime-profile", "runtime-profile.json"),
            ("--model", "model.gguf"),
            ("--runtime-artifact", "runtime.tar.gz"),
            ("--model-build-report", "model-build-report.json"),
            ("--model-source-lock", "model-sources.lock.json"),
            ("--qualification-lock", "qualification.lock.json"),
            ("--request-corpus", "requests.json"),
            (
                "--producer-commit",
                "cccccccccccccccccccccccccccccccccccccccc",
            ),
            ("--producer-run-id", "123"),
            ("--output", "model-smoke-report.json"),
        ]
        .into_iter()
        .flat_map(|(flag, value)| [OsString::from(flag), OsString::from(value)])
        .collect()
    }

    #[test]
    fn parses_complete_strict_contract() {
        let parsed = parse_arguments(complete_arguments().into_iter()).unwrap();
        assert_eq!(parsed.len(), 12);
        assert_eq!(parsed["--producer-run-id"], OsString::from("123"));
    }

    #[test]
    fn rejects_unknown_duplicate_and_missing_inputs() {
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
        let mut duplicate = complete_arguments();
        duplicate.extend([OsString::from("--output"), OsString::from("second.json")]);
        assert!(parse_arguments(duplicate.into_iter()).is_err());
        let mut missing = complete_arguments();
        missing.truncate(missing.len() - 2);
        assert!(parse_arguments(missing.into_iter()).is_err());
    }
}
