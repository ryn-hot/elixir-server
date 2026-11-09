//! Closed create-only lifecycle for the journaled provider worker. No image
//! pull, general command, reusable-name adoption or arbitrary namespace input.
use super::*;
use crate::{
    marketplace::{
        provider::{WorkerCreateAttempt, WorkerProfile, WorkerStartAttempt},
        runtime::verify_fixed_worker_state,
    },
    runtime::provider_creation::WorkerCreation,
};
use anyhow::ensure;
const ENTRY: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-launcher";

impl DockerRuntimeManager {
    async fn provider_worker_image(&self, profile: &WorkerProfile) -> Result<()> {
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        ensure!(
            image["Id"] == profile.spec().image
                && image["Config"]["Entrypoint"] == serde_json::json!([ENTRY])
                && image["Config"]
                    .get("Cmd")
                    .is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty))
                && image["Config"].get("Volumes").is_none_or(
                    |v| v.is_null() || v.as_object().is_some_and(serde_json::Map::is_empty)
                ),
            "provider worker image execution/storage differs"
        );
        for feature in ["control", "network_control", "fixed_state"] {
            ensure!(
                image["Config"]["Labels"][format!("elixir.marketplace.worker.{feature}")] == "1",
                "provider worker protocol missing"
            );
        }
        ensure!(
            image["Config"]["Labels"]["elixir.marketplace.worker.connectivity_probe"] == "2",
            "provider worker probe protocol missing"
        );
        Ok(())
    }
    async fn provider_worker_parent(&self, profile: &WorkerProfile) -> Result<()> {
        let (parent, handle) = profile.parent();
        ensure!(
            self.inspect_provider_guard_actor(handle, parent)
                .await?
                .running,
            "provider guard is not live"
        );
        let (root, root_handle) = parent.parent();
        ensure!(
            self.provider_native_namespace(&root_handle, root).await? == parent.namespace(),
            "provider worker parent namespace changed"
        );
        Ok(())
    }
    async fn inspect_provider_worker_actor(
        &self,
        handle: &ContainerHandle,
        profile: &WorkerProfile,
    ) -> Result<()> {
        ensure!(
            immutable_id(&handle.id) && handle.name == profile.spec().name,
            "provider worker native identity differs"
        );
        self.provider_worker_image(profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::provider_worker_owner(&value, handle, profile)?;
        ensure!(
            value["Config"]["Entrypoint"] == serde_json::json!([ENTRY])
                && value["Config"]["Cmd"] == serde_json::json!(profile.spec().command),
            "provider worker executable differs"
        );
        let state = self
            .describe_container_runtime_state(&handle.id)
            .await?
            .context("provider worker configuration unavailable")?;
        verify_fixed_worker_state(handle, profile.fixed(), &state)
    }
    pub(super) async fn create_provider_worker_actor(
        &self,
        profile: &WorkerProfile,
        attempt: WorkerCreateAttempt,
    ) -> Result<WorkerCreation> {
        let preflight = async {
            attempt.require_profile(profile)?;
            container_resource_args(&profile.spec().security)?;
            self.provider_worker_image(profile).await?;
            self.provider_worker_parent(profile).await?;
            ensure!(
                self.find_container_id(&profile.spec().name)
                    .await?
                    .is_none(),
                "worker name exists; original recovery required"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = preflight {
            return Ok(WorkerCreation::not_attempted(attempt, error));
        }
        self.create_container_entrypoint(profile.spec(), false, Some(ENTRY))
            .await
            .map(WorkerCreation::Created)
    }
    pub(super) async fn launch_provider_worker_actor(
        &self,
        handle: &ContainerHandle,
        profile: &WorkerProfile,
        attempt: WorkerStartAttempt,
    ) -> Result<PrivateProcessStdio> {
        attempt.require(profile, handle)?;
        self.inspect_provider_worker_actor(handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            value["State"]["Status"] == "created" && Self::application_gateway_stopped(&value),
            "worker START requires exact never-started object"
        );
        self.provider_worker_parent(profile).await?;
        let mut child = self.docker_command().await?
            .args(["start", "--attach", "--interactive", &handle.id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("worker input missing")?,
            output: child.stdout.take().context("worker output missing")?,
            diagnostics: child.stderr.take(),
            child,
        })
    }
    pub(super) async fn inspect_unstarted_provider_worker_actor(
        &self,
        profile: &WorkerProfile,
    ) -> Result<ContainerHandle> {
        let id = self
            .find_container_id(&profile.spec().name)
            .await?
            .context("worker create uncertain; name absence is not retirement")?;
        let handle = ContainerHandle {
            id,
            name: profile.spec().name.clone(),
        };
        self.inspect_provider_worker_actor(&handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            value["State"]["Status"] == "created" && Self::application_gateway_stopped(&value),
            "unknown worker CREATE recovery requires exact unstarted object"
        );
        Ok(handle)
    }
    fn provider_worker_owner(
        value: &Value,
        handle: &ContainerHandle,
        profile: &WorkerProfile,
    ) -> Result<()> {
        ensure!(
            immutable_id(&handle.id)
                && handle.name == profile.spec().name
                && value["Id"] == handle.id
                && value["Name"] == format!("/{}", handle.name)
                && profile
                    .spec()
                    .labels
                    .iter()
                    .all(|(k, v)| value["Config"]["Labels"][k] == *v),
            "provider worker native owner differs"
        );
        Ok(())
    }
    pub(super) async fn stop_provider_worker_actor(
        &self,
        handle: &ContainerHandle,
        profile: &WorkerProfile,
    ) -> Result<()> {
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::provider_worker_owner(&value, handle, profile)?;
        if Self::application_gateway_stopped(&value) {
            return Ok(());
        }
        let attempt = self
            .run_capture_with_timeout(
                &["kill".into(), "--signal=KILL".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::provider_worker_owner(&value, handle, profile)?;
        if !Self::application_gateway_stopped(&value) {
            attempt?;
            bail!("provider worker native termination unconfirmed");
        }
        Ok(())
    }
    pub(super) async fn remove_provider_worker_actor(
        &self,
        handle: &ContainerHandle,
        profile: &WorkerProfile,
    ) -> Result<()> {
        if self.find_container_id(&handle.id).await?.is_none() {
            return Ok(());
        }
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::provider_worker_owner(&value, handle, profile)?;
        ensure!(
            Self::application_gateway_stopped(&value),
            "worker removal requires confirmed stopped state"
        );
        let attempt = self
            .run_capture_with_timeout(
                &["rm".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        if self.find_container_id(&handle.id).await?.is_some() {
            attempt?;
            bail!("provider worker native removal unconfirmed");
        }
        Ok(())
    }
}
