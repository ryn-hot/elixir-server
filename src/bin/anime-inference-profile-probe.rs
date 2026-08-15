use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use elixir_server::anime_matching::{
    AnimeInferenceProfileProbeConfig, run_anime_inference_profile_probe,
};

const USAGE: &str = "usage: anime-inference-profile-probe \
  --runtime-id ID --manifest PATH --model PATH --runtime-artifact PATH --output PATH \
  [--semantic-probe-corpus PATH --semantic-prompt-revision REVISION]";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("anime inference profile probe failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let config = AnimeInferenceProfileProbeConfig {
        runtime_id: required_text(&arguments, "--runtime-id")?,
        manifest_path: required_path(&arguments, "--manifest")?,
        model_path: required_path(&arguments, "--model")?,
        runtime_artifact_path: required_path(&arguments, "--runtime-artifact")?,
        output_path: required_path(&arguments, "--output")?,
        semantic_probe_corpus_path: optional_path(&arguments, "--semantic-probe-corpus"),
        semantic_prompt_revision: optional_text(&arguments, "--semantic-prompt-revision")?,
    };
    let output = config
        .output_path
        .to_str()
        .context("--output is not valid UTF-8 for the JSON completion summary")?
        .to_string();
    run_anime_inference_profile_probe(config).await?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "status": "complete",
            "runtimeProfile": output,
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
        "--model",
        "--runtime-artifact",
        "--output",
        "--semantic-probe-corpus",
        "--semantic-prompt-revision",
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
    for required in &allowed[..5] {
        if !parsed.contains_key(*required) {
            bail!("missing required argument {required}; {USAGE}");
        }
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

fn optional_path(arguments: &BTreeMap<String, OsString>, name: &str) -> Option<PathBuf> {
    arguments.get(name).cloned().map(PathBuf::from)
}

fn optional_text(arguments: &BTreeMap<String, OsString>, name: &str) -> Result<Option<String>> {
    arguments
        .get(name)
        .cloned()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!("{name} is not valid UTF-8"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_arguments() -> Vec<OsString> {
        [
            ("--runtime-id", "macos-x86_64-metal-cpu"),
            ("--manifest", "manifest.json"),
            ("--model", "model.gguf"),
            ("--runtime-artifact", "runtime.tar.gz"),
            ("--output", "runtime-profile.json"),
        ]
        .into_iter()
        .flat_map(|(flag, value)| [OsString::from(flag), OsString::from(value)])
        .collect()
    }

    #[test]
    fn parser_requires_exact_complete_arguments() {
        assert_eq!(
            parse_arguments(complete_arguments().into_iter())
                .unwrap()
                .len(),
            5
        );
        let mut missing = complete_arguments();
        missing.truncate(missing.len() - 2);
        assert!(parse_arguments(missing.into_iter()).is_err());
    }

    #[test]
    fn parser_rejects_unknown_and_duplicate_arguments() {
        let mut unknown = complete_arguments();
        unknown.extend([OsString::from("--worker"), OsString::from("worker")]);
        assert!(parse_arguments(unknown.into_iter()).is_err());

        let mut duplicate = complete_arguments();
        duplicate.extend([
            OsString::from("--runtime-id"),
            OsString::from("linux-x86_64-cpu"),
        ]);
        assert!(parse_arguments(duplicate.into_iter()).is_err());
    }

    #[test]
    fn parser_accepts_paired_semantic_probe_arguments() {
        let mut arguments = complete_arguments();
        arguments.extend([
            OsString::from("--semantic-probe-corpus"),
            OsString::from("semantic.json"),
            OsString::from("--semantic-prompt-revision"),
            OsString::from("anime-semantic-evidence-v5"),
        ]);
        assert_eq!(parse_arguments(arguments.into_iter()).unwrap().len(), 7);
    }
}
