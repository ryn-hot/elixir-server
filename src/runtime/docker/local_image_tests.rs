//! No pull, CREATE, START or network test. Native case inspects an already local image only.
use super::*;

#[tokio::test]
async fn local_image_rejects_unsafe_references_before_cli() {
    let runtime = DockerRuntimeManager::new(Some("/must-not-run-local-image-fixture".into()));
    for reference in [
        "",
        "--help",
        "image\nother",
        "image tag",
        "image\0tag",
        "é:tag",
        "sha256:short",
        &"a".repeat(513),
    ] {
        let error = runtime.local_image_id(reference).await.unwrap_err();
        assert_eq!(error.to_string(), "invalid local platform image reference");
    }
}

#[tokio::test]
#[ignore = "read-only local Docker lookup; set ELIXIR_TEST_LOCAL_PLATFORM_IMAGE and ELIXIR_TEST_LOCAL_PLATFORM_IMAGE_ID"]
async fn local_image_resolves_existing_tag_and_exact_id_without_pulling() -> Result<()> {
    let reference = std::env::var("ELIXIR_TEST_LOCAL_PLATFORM_IMAGE")?;
    let expected = std::env::var("ELIXIR_TEST_LOCAL_PLATFORM_IMAGE_ID")?;
    anyhow::ensure!(expected.strip_prefix("sha256:").is_some_and(immutable_id));
    let runtime = DockerRuntimeManager::new(None);
    assert_eq!(runtime.local_image_id(&reference).await?, expected);
    assert_eq!(runtime.local_image_id(&expected).await?, expected);
    Ok(())
}
