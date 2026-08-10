use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::acquisition::anime_qualification::{
    AnimeQualificationRunConfig, run_anime_inference_qualification,
};

const USAGE: &str = "usage: anime-inference-qualification \
  --corpus PATH --identity PATH --manifest PATH --runtime-profile PATH \
  --model PATH --runtime-artifact PATH --runtime-source-lock PATH \
  --scorer PATH [--gpu-preflight-evidence PATH] --output PATH";

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("anime inference qualification failed: {error:#}");
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let summary = run_anime_inference_qualification(AnimeQualificationRunConfig {
        corpus_path: required_path(&arguments, "--corpus")?,
        identity_path: required_path(&arguments, "--identity")?,
        manifest_path: required_path(&arguments, "--manifest")?,
        runtime_profile_path: required_path(&arguments, "--runtime-profile")?,
        model_path: required_path(&arguments, "--model")?,
        runtime_artifact_path: required_path(&arguments, "--runtime-artifact")?,
        runtime_source_lock_path: required_path(&arguments, "--runtime-source-lock")?,
        scorer_path: required_path(&arguments, "--scorer")?,
        gpu_preflight_evidence_path: arguments.get("--gpu-preflight-evidence").cloned(),
        output_path: required_path(&arguments, "--output")?,
    })
    .await?;
    println!(
        "{}",
        serde_json::to_string(&summary).context("encoding qualification summary")?
    );
    Ok(())
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<BTreeMap<String, PathBuf>> {
    let allowed = [
        "--corpus",
        "--identity",
        "--manifest",
        "--runtime-profile",
        "--model",
        "--runtime-artifact",
        "--runtime-source-lock",
        "--scorer",
        "--gpu-preflight-evidence",
        "--output",
    ];
    let required = [
        "--corpus",
        "--identity",
        "--manifest",
        "--runtime-profile",
        "--model",
        "--runtime-artifact",
        "--runtime-source-lock",
        "--scorer",
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
            .ok_or_else(|| anyhow::anyhow!("{flag} requires a path; {USAGE}"))?;
        ensure!(!value.is_empty(), "{flag} path is empty");
        ensure!(
            parsed.insert(flag.clone(), PathBuf::from(value)).is_none(),
            "duplicate argument {flag:?}"
        );
    }
    if !required.iter().all(|flag| parsed.contains_key(*flag)) {
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
    fn alm9_cli_rejects_unknown_and_duplicate_arguments() {
        assert!(parse_arguments([OsString::from("--unknown")].into_iter()).is_err());
        assert!(
            parse_arguments(
                [
                    OsString::from("--corpus"),
                    OsString::from("one"),
                    OsString::from("--corpus"),
                    OsString::from("two"),
                ]
                .into_iter()
            )
            .is_err()
        );
    }

    #[test]
    fn alm9_cli_requires_sealed_profile_model_and_runtime_inputs() {
        let arguments = [
            "--corpus",
            "corpus.json",
            "--identity",
            "identity.json",
            "--manifest",
            "manifest.json",
            "--runtime-profile",
            "profile.json",
            "--model",
            "model.gguf",
            "--runtime-artifact",
            "runtime.tar.gz",
            "--runtime-source-lock",
            "runtime.lock.json",
            "--scorer",
            "scorer.py",
            "--output",
            "output.json",
        ]
        .into_iter()
        .map(OsString::from);
        let parsed = parse_arguments(arguments).expect("complete ALM-9 CLI should parse");
        assert_eq!(parsed["--runtime-profile"], PathBuf::from("profile.json"));
        assert_eq!(parsed["--model"], PathBuf::from("model.gguf"));
    }

    #[test]
    fn alm9_cli_accepts_optional_bound_gpu_preflight_evidence() {
        let arguments = [
            "--corpus",
            "corpus.json",
            "--identity",
            "identity.json",
            "--manifest",
            "manifest.json",
            "--runtime-profile",
            "profile.json",
            "--model",
            "model.gguf",
            "--runtime-artifact",
            "runtime.zip",
            "--runtime-source-lock",
            "runtime.lock.json",
            "--scorer",
            "scorer.py",
            "--gpu-preflight-evidence",
            "gpu-preflight.json",
            "--output",
            "output.json",
        ]
        .into_iter()
        .map(OsString::from);
        let parsed = parse_arguments(arguments).expect("GPU qualification CLI should parse");
        assert_eq!(
            parsed["--gpu-preflight-evidence"],
            PathBuf::from("gpu-preflight.json")
        );
    }
}
