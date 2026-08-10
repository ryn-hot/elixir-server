use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::anime_matching::{
    AnimeInferenceHardwareCertificationConfig, run_anime_inference_hardware_certification,
};

const USAGE: &str = "usage: anime-inference-hardware-certify \
  --target-id ID --runtime-id ID --commit-sha SHA --run-id ID \
  --manifest PATH --runtime-profile PATH --model PATH --runtime-artifact PATH \
  --request-corpus PATH --playback-report PATH --playback-command PATH \
  --api-url URL --output PATH";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("anime inference hardware certification failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let config = AnimeInferenceHardwareCertificationConfig {
        target_id: required_text(&arguments, "--target-id")?,
        runtime_id: required_text(&arguments, "--runtime-id")?,
        commit_sha: required_text(&arguments, "--commit-sha")?,
        run_id: required_text(&arguments, "--run-id")?,
        manifest_path: required_path(&arguments, "--manifest")?,
        runtime_profile_path: required_path(&arguments, "--runtime-profile")?,
        model_path: required_path(&arguments, "--model")?,
        runtime_artifact_path: required_path(&arguments, "--runtime-artifact")?,
        request_corpus_path: required_path(&arguments, "--request-corpus")?,
        playback_report_path: required_path(&arguments, "--playback-report")?,
        playback_command_path: required_path(&arguments, "--playback-command")?,
        api_url: required_text(&arguments, "--api-url")?,
        output_path: required_path(&arguments, "--output")?,
    };
    let output = config.output_path.clone();
    run_anime_inference_hardware_certification(config).await?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "status": "complete",
            "observation": output,
        }))?
    );
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, OsString>> {
    let allowed = [
        "--target-id",
        "--runtime-id",
        "--commit-sha",
        "--run-id",
        "--manifest",
        "--runtime-profile",
        "--model",
        "--runtime-artifact",
        "--request-corpus",
        "--playback-report",
        "--playback-command",
        "--api-url",
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
            ("--target-id", "macos-intel-amd-real-device"),
            ("--runtime-id", "macos-x86_64-metal-cpu"),
            ("--commit-sha", "cccccccccccccccccccccccccccccccccccccccc"),
            ("--run-id", "123"),
            ("--manifest", "manifest.json"),
            ("--runtime-profile", "runtime-profile.json"),
            ("--model", "model.gguf"),
            ("--runtime-artifact", "runtime.tar.gz"),
            ("--request-corpus", "hardware-requests.json"),
            ("--playback-report", "certification.json"),
            ("--playback-command", "ffmpeg-command.json"),
            ("--api-url", "http://127.0.0.1:3000/health"),
            ("--output", "observation.json"),
        ]
        .into_iter()
        .flat_map(|(flag, value)| [OsString::from(flag), OsString::from(value)])
        .collect()
    }

    #[test]
    fn parses_complete_strict_contract() {
        let parsed = parse_arguments(complete_arguments().into_iter()).unwrap();
        assert_eq!(
            parsed["--runtime-id"],
            OsString::from("macos-x86_64-metal-cpu")
        );
    }

    #[test]
    fn rejects_unknown_duplicate_and_missing_inputs() {
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
        let mut duplicate = complete_arguments();
        duplicate.extend([OsString::from("--run-id"), OsString::from("124")]);
        assert!(parse_arguments(duplicate.into_iter()).is_err());
        let mut missing = complete_arguments();
        missing.truncate(missing.len() - 2);
        assert!(parse_arguments(missing.into_iter()).is_err());
    }
}
