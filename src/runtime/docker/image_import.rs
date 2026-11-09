//! Bounded read-only daemon observations. These do not import/run publisher code.
use super::*;
use elixir_marketplace::{
    canonical, container_image::import::MAX_INSPECTION_BYTES, identity::Digest,
};
impl DockerRuntimeManager {
    #[cfg(unix)]
    pub fn with_marketplace_container_guardian(
        mut self,
        profile: crate::runtime::container_guardian::Profile,
    ) -> Self {
        self.container_guardian = Some(std::sync::Arc::new(profile));
        self
    }
    #[cfg(unix)]
    pub fn with_marketplace_import_profile(
        mut self,
        profile: crate::runtime::image_import::Profile,
    ) -> Self {
        self.import_profile = Some(std::sync::Arc::new(profile));
        self.local_observer = Default::default();
        self.local_cli_binding = Default::default();
        self
    }
    // Deliberately unpinned for the initial independent CLI/daemon identity
    // comparison. All mutations and attachments use docker_command instead.
    pub(super) async fn import_observe(&self, args: &[String]) -> Result<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let mut child = Command::new(&self.docker_bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut output = child.stdout.take().context("import observation stdout")?;
        let mut errors = child.stderr.take().context("import observation stderr")?;
        timeout(Duration::from_secs(10), async {
            let (out, err, status) = tokio::try_join!(
                async {
                    let mut b = vec![];
                    (&mut output)
                        .take(MAX_INSPECTION_BYTES as u64 + 1)
                        .read_to_end(&mut b)
                        .await?;
                    Ok::<_, anyhow::Error>(b)
                },
                async {
                    let mut b = vec![];
                    (&mut errors).take(4097).read_to_end(&mut b).await?;
                    Ok::<_, anyhow::Error>(b)
                },
                async { Ok::<_, anyhow::Error>(child.wait().await?) }
            )?;
            if !status.success() || out.len() > MAX_INSPECTION_BYTES || !err.is_empty() {
                bail!("actual daemon import observation unavailable");
            }
            Ok(out)
        })
        .await
        .context("daemon import observation deadline")?
    }
    pub(super) async fn image_import_daemon_id(&self) -> Result<String> {
        #[cfg(unix)]
        if self.import_profile.is_some() {
            let value = self
                .local_observer()
                .await?
                .get(local_observation::Query::Info)
                .await?
                .context("local daemon information absent")?;
            return Self::require_import_daemon_id(&value["ID"]);
        }
        let bytes = self
            .import_observe(&["info".into(), "--format".into(), "{{json .ID}}".into()])
            .await?;
        let value = canonical::parse(&bytes, 1024)?;
        Self::require_import_daemon_id(&value)
    }
    pub(super) fn require_import_daemon_id(value: &Value) -> Result<String> {
        let id = value.as_str().context("daemon identity missing")?;
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'-'))
        {
            bail!("invalid local daemon identity");
        }
        Ok(id.into())
    }
    pub(super) async fn image_import_inspect(&self, image: Digest) -> Result<Vec<u8>> {
        #[cfg(unix)]
        if self.import_profile.is_some() {
            let value = self
                .local_observer()
                .await?
                .get(local_observation::Query::Image(image))
                .await?
                .context("exact local image is absent")?;
            if value["Id"].as_str() != Some(&image.to_string()) {
                bail!("local daemon image identity differs");
            }
            return Ok(canonical::to_vec(&value)?);
        }
        self.import_observe(&[
            "image".into(),
            "inspect".into(),
            "--format".into(),
            "{{json .}}".into(),
            image.to_string(),
        ])
        .await
    }
}
