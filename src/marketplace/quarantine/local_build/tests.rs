//! Authentication/ownership unit fixtures, NOT actual native build proof.
use super::*;
use elixir_marketplace::{admission::CandidateDescriptor, retained_authority::AuthorityRecord};

async fn fixture(path: &Path) -> Result<(SourceMatchedQuarantine, VerifiedRetainedAuthority)> {
    let source = super::super::build_inputs::tests::compiled_source(path).await?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/publication-interop.mjs");
    let out = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new("node")
            .arg(script)
            .args(["sample", "compiled"])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !out.status.success() || out.stdout.len() > 128 * 1024 {
        bail!("local proof unit fixture failed");
    }
    let value: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    let descriptor = CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]
    }))?)?;
    Ok((
        source,
        AuthorityRecord::from_descriptor(&descriptor)?.verify()?,
    ))
}
fn record(source: &SourceMatchedQuarantine, secrets: &SecretsManager) -> Result<LocalBuildRecord> {
    // Deliberately synthetic authentication DATA. This bypasses the real mint
    // ONLY inside this unit module and cannot qualify a native build/import.
    let d = super::super::super::runtime::build::execution(&source.execution_descriptor(), 1)?;
    let binding = Binding {
        schema_version: 1,
        release: source.workspace().release_id,
        source: Digest::of(canonical::to_vec(source.summary())?),
        artifact: d.output.artifact,
        platform: d.inputs.platform,
        config: Digest::of(b"unit image DATA"),
        compiler: d.toolchain_image_digest,
    };
    let seal = secrets.encrypt_bound(MARKER, &binding.associated()?)?;
    Ok(LocalBuildRecord { binding, seal })
}
#[tokio::test]
async fn authenticated_retained_binding_rejects_tampering_wrong_key_unapproved_compiler_and_foreground_reuse()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().canonicalize()?;
    let (mut source, authority) = fixture(&path.join("source")).await?;
    let secrets = SecretsManager::from_key_bytes([93; 32], false);
    let value = record(&source, &secrets)?;
    let value = LocalBuildRecord::parse(&value.to_bytes()?)?;
    let approved = ApprovedToolchains::from_operator_digests([value.binding.compiler])?;
    assert!(
        value
            .bind_retained(
                &mut source,
                &authority,
                &ApprovedToolchains::default(),
                &secrets
            )
            .is_err()
    );
    assert!(
        value
            .bind_retained(
                &mut source,
                &authority,
                &approved,
                &SecretsManager::from_key_bytes([94; 32], true)
            )
            .is_err()
    );
    assert!(source.retained_build.is_none());
    assert!(crate::marketplace::archive::ImageExportStaging::new(&source).is_err());
    let mut tampered = value.clone();
    tampered.binding.config = Digest::of(b"different image");
    assert!(
        tampered
            .bind_retained(&mut source, &authority, &approved, &secrets)
            .is_err()
    );
    let mut plaintext = value.clone();
    plaintext.seal = MARKER.into();
    assert!(
        plaintext
            .bind_retained(
                &mut source,
                &authority,
                &approved,
                &SecretsManager::from_key_bytes([93; 32], true)
            )
            .is_err()
    );
    assert!(source.retained_build.is_none());
    let original_expires = source.expires();
    let live = value.bind_retained(&mut source, &authority, &approved, &secrets)?;
    live.require_source(&source)?;
    // Constructor only: no file allocation/native export is qualified here.
    drop(crate::marketplace::archive::ImageExportStaging::new(
        &source,
    )?);
    assert_eq!(source.expires(), original_expires);
    assert!(source.require_public_source().is_err());
    assert!(source.require_foreground_manifest().is_err());
    let (public, _health) = crate::marketplace::worker::PublicSourceObservation::fixture_for_source(
        source.workspace().release_id,
        source.summary().license.sha256,
    );
    assert!(source.refresh_public_source(public).is_err());
    let (mut foreign, _) = fixture(&path.join("foreign")).await?;
    value.bind_retained(&mut foreign, &authority, &approved, &secrets)?;
    assert!(
        live.require_source(&foreign).is_err(),
        "equal records cannot replace the actual native source owner"
    );
    source.source.job.expires += Duration::from_millis(1);
    assert!(live.require_source(&source).is_err());
    source.source.job.expires = original_expires;
    live.require_source(&source)?;
    // Unit-only wrapper revocation, not evidence of native cleanup completion.
    source.disposed = true;
    assert!(live.require_source(&source).is_err());
    assert!(source.ensure_active().is_err());
    Ok(())
}
#[test]
fn record_parser_is_closed_bounded_and_never_accepts_a_plain_verified_flag() {
    assert!(LocalBuildRecord::parse(br#"{"verified":true}"#).is_err());
    assert!(LocalBuildRecord::parse(&vec![b' '; MAX_BYTES + 1]).is_err());
}
