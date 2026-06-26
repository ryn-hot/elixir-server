use std::{env, path::PathBuf};

use anyhow::{Context, Result, bail};
use elixir_server::playback::{
    certification::{
        CertificationStatus, CertificationSuite, HardwareCertificationConfig,
        run_hardware_certification,
    },
    hardware::HardwarePreference,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args(env::args().skip(1).collect())?;
    let artifact_dir = config.artifact_dir.clone();
    let report = run_hardware_certification(config).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status": report.status,
            "target_id": report.target_id,
            "suite": report.suite,
            "hardware_api": report.hardware_api,
            "passed": report.cases.passed,
            "failed": report.cases.failed,
            "skipped": report.cases.skipped,
            "artifact_digest": report.artifact_digest,
        }))?
    );
    if report.status == CertificationStatus::Passed {
        Ok(())
    } else {
        bail!(
            "playback hardware certification failed; see {}/certification.json",
            artifact_dir.display()
        )
    }
}

fn parse_args(args: Vec<String>) -> Result<HardwareCertificationConfig> {
    let mut suite = CertificationSuite::Smoke;
    let mut hardware_api = "auto".to_string();
    let mut corpus_root = PathBuf::from("../data/playback-corpus/public");
    let mut artifact_dir = PathBuf::from("target/playback-hardware-certification");
    let mut target_id = "local-hardware".to_string();
    let mut require_hardware = true;
    let mut allow_software_fallback_test = true;
    let mut case_timeout_seconds = 180_u64;
    let mut output_seconds = 6.0_f64;
    let mut skip_public_if_missing = false;

    let mut idx = 0usize;
    while idx < args.len() {
        let arg = &args[idx];
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--suite" => suite = CertificationSuite::parse(next_value(&args, &mut idx, arg)?)?,
            "--hardware-api" => hardware_api = next_value(&args, &mut idx, arg)?.to_string(),
            "--corpus-root" => corpus_root = PathBuf::from(next_value(&args, &mut idx, arg)?),
            "--artifact-dir" => artifact_dir = PathBuf::from(next_value(&args, &mut idx, arg)?),
            "--target-id" => target_id = next_value(&args, &mut idx, arg)?.to_string(),
            "--require-hardware" => {
                require_hardware = parse_bool(next_value(&args, &mut idx, arg)?)?
            }
            "--allow-software-fallback-test" => {
                allow_software_fallback_test = parse_bool(next_value(&args, &mut idx, arg)?)?
            }
            "--case-timeout-seconds" => {
                case_timeout_seconds = next_value(&args, &mut idx, arg)?
                    .parse()
                    .context("parse --case-timeout-seconds")?
            }
            "--output-seconds" => {
                output_seconds = next_value(&args, &mut idx, arg)?
                    .parse()
                    .context("parse --output-seconds")?
            }
            "--skip-public-if-missing" => skip_public_if_missing = true,
            other => bail!("unknown argument {other:?}; pass --help for usage"),
        }
        idx += 1;
    }

    let mut config =
        HardwareCertificationConfig::new(suite, hardware_api.clone(), corpus_root, artifact_dir);
    config.hardware_preference = HardwarePreference::parse(&hardware_api);
    config.target_id = target_id;
    config.require_hardware = require_hardware;
    config.allow_software_fallback_test = allow_software_fallback_test;
    config.case_timeout_seconds = case_timeout_seconds;
    config.output_seconds = output_seconds;
    config.skip_public_if_missing = skip_public_if_missing;
    Ok(config)
}

fn next_value<'a>(args: &'a [String], idx: &mut usize, flag: &str) -> Result<&'a str> {
    *idx += 1;
    args.get(*idx)
        .map(String::as_str)
        .with_context(|| format!("{flag} requires a value"))
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("invalid boolean {other:?}"),
    }
}

fn print_help() {
    println!(
        r#"playback-hardware-certify

Runs Elixir playback hardware certification on the current machine.

Usage:
  playback-hardware-certify [options]

Options:
  --suite smoke|robust|torture
  --hardware-api auto|videotoolbox|qsv|nvenc|amf|vaapi|off
  --corpus-root <path>
  --artifact-dir <path>
  --target-id <id>
  --require-hardware true|false
  --allow-software-fallback-test true|false
  --case-timeout-seconds <seconds>
  --output-seconds <seconds>
  --skip-public-if-missing

Safety:
  This binary never provisions cloud resources. Cloud orchestration must invoke
  it explicitly on a prepared target machine.
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_args() {
        let config = parse_args(vec![
            "--suite".to_string(),
            "robust".to_string(),
            "--hardware-api".to_string(),
            "nvenc".to_string(),
            "--require-hardware".to_string(),
            "false".to_string(),
        ])
        .unwrap();
        assert_eq!(config.suite, CertificationSuite::Robust);
        assert_eq!(config.hardware_api_label, "nvenc");
        assert!(!config.require_hardware);
    }

    #[test]
    fn rejects_unknown_args() {
        assert!(parse_args(vec!["--nightly".to_string()]).is_err());
    }
}
