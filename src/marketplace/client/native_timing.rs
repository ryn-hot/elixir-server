//! Actual current Node/native-verifier stdio, with TEST metadata, network
//! denied by the outer test profile. Not protected/public-source/UI proof.
use super::*;
use elixir_marketplace::{admission::CandidateDescriptor, catalog::SearchQuery};
use std::{io::Read, path::PathBuf, process::Stdio, time::Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit current-source offline Node/native/Rust IPC timing with twenty signed TEST descriptors; no VPN, source admission, billing or installs"]
async fn actual_offline_search_and_head_rpc_timing() -> Result<()> {
    anyhow::ensure!(
        super::super::search_timing::enabled(),
        "explicit timing opt-in required"
    );
    let fixture = PathBuf::from(
        std::env::var_os("ELIXIR_TEST_SEARCH_RPC_FIXTURE")
            .context("signed TEST fixture path required")?,
    );
    let mut bytes = vec![];
    std::fs::File::open(fixture)?
        .take(MAX_CONTROL_FRAME_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    let descriptors: Vec<CandidateDescriptor> = canonical::decode(&bytes, MAX_CONTROL_FRAME_BYTES)?;
    anyhow::ensure!(
        descriptors.len() == 20,
        "exactly twenty TEST descriptors required"
    );
    let issuer = descriptors[0]
        .gate_evidence
        .first()
        .context("TEST issuer unavailable")?
        .credential
        .payload
        .issuer_key_id;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("workspace missing")?
        .canonicalize()?;
    let verifier = root.join("target/debug/elixir-marketplace-verifier");
    anyhow::ensure!(verifier.is_file(), "existing native verifier required");
    let directory = tempfile::tempdir()?;
    let state = directory.path().canonicalize()?.join("metadata.sqlite");
    let mut child = tokio::process::Command::new("node")
        .arg(root.join("crates/elixir_marketplace/worker/src/worker.mjs"))
        .arg("--state")
        .arg(state)
        .arg("--verifier")
        .arg(verifier)
        .arg("--issuer-key")
        .arg(issuer.to_string())
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let process = PrivateProcessStdio {
        input: child.stdin.take().context("worker stdin missing")?,
        output: child.stdout.take().context("worker stdout missing")?,
        diagnostics: child.stderr.take(),
        child,
    };
    let mut client = PrivateWorkerClient::new(process)?;
    let operation = async {
        for descriptor in &descriptors {
            let response = client.request(PeerOperation::Offer { descriptor: Box::new(descriptor.clone()) }).await?;
            anyhow::ensure!(matches!(response.response, PeerOutcome::Offered { .. }), "fresh TEST metadata was not admitted");
        }
        let ids: Vec<_> = descriptors.iter().map(|d| d.release.payload.extension_id.clone()).collect();
        let mut samples = vec![];
        for sample in 0..20 {
            let began = Instant::now();
            let search = client.request(PeerOperation::Search { query: SearchQuery { text: String::new(), tags: vec![], limit: 20 }, cursor: None }).await?;
            let search_us = began.elapsed().as_micros();
            let PeerOutcome::Search { page } = &search.response else { bail!("actual worker search failed"); };
            anyhow::ensure!(page.results.len() == 20 && page.metadata_only && !page.network_complete, "actual search scope differs");
            let began = Instant::now();
            let heads = client.control(WorkerControlOperation::CatalogHeads { extension_ids: ids.clone() }).await?;
            let heads_us = began.elapsed().as_micros();
            let WorkerControlOutcome::CatalogHeads { rows } = &heads.control_response else { bail!("actual worker heads failed"); };
            anyhow::ensure!(rows.len() == 20, "actual head prefix unexpectedly incomplete");
            for (row, descriptor) in rows.iter().zip(&descriptors) {
                anyhow::ensure!(row.extension_id == descriptor.release.payload.extension_id, "head scope differs");
                let PeerOutcome::Head { state } = &row.response.response else { bail!("actual head unavailable"); };
                anyhow::ensure!(canonical::to_vec(&state.publisher)? == canonical::to_vec(&descriptor.publisher)?
                    && canonical::to_vec(&state.head)? == canonical::to_vec(&Some(descriptor.head.clone()))?, "actual head bytes differ");
            }
            if sample >= 4 { samples.push(serde_json::json!({"search_request_id":search.request_id,
                "heads_request_id":heads.request_id,"search_us":search_us,"heads_us":heads_us})); }
        }
        eprintln!("{}", serde_json::json!({"stage":"offline_search_rpc_result","samples":samples,
            "rows":20,"warm_samples":16,"fixture_only":true,"current_worker_source":true,
            "scope":"host offline Node/native/Rust stdio only; no cgroup, protected owner, public-source, HTTP/UI, billing, install, playback or scale qualification"}));
        Ok::<(), anyhow::Error>(())
    }.await;
    // Keep this original attachment through failure; EOF must join the actual
    // worker and its original native verifier. No retry/replacement process.
    let cleanup = client.close_attachment().await;
    client.close().await;
    operation?;
    anyhow::ensure!(
        cleanup?.success(),
        "actual worker did not exit cleanly on EOF"
    );
    eprintln!(
        "{{\"stage\":\"offline_search_rpc_cleanup\",\"original_worker_joined\":true,\"native_verifier_joined\":true}}"
    );
    Ok(())
}
