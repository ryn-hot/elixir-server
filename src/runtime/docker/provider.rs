//! Provider-only adapter. Shared tunnel inspection still requires the exact
//! closed provider profile/image; no arbitrary container spec or execution API.
use super::application_gateway::handle_matches;
use super::*;
use crate::{
    marketplace::provider::{CreateAttempt, ENTRYPOINT, Profile, StartAttempt},
    runtime::provider_creation::ProviderCreation,
};
use anyhow::ensure;

impl DockerRuntimeManager {
    pub(super) async fn create_provider_actor(
        &self,
        profile: &Profile,
        attempt: CreateAttempt,
    ) -> Result<ProviderCreation> {
        let preflight = async {
            attempt.require_profile(profile)?;
            container_resource_args(&profile.spec().security)?;
            Self::gateway_image_contract(
                &self
                    .gateway_inspection("image", &profile.spec().image)
                    .await?,
                profile,
            )?;
            self.require_application_gateway_uplink(profile).await?;
            let uplink = self
                .inspect_packet_uplink(&profile.spec().network)
                .await?
                .context("provider uplink unavailable")?;
            ensure!(uplink.members.is_empty(), "provider uplink is not empty");
            ensure!(
                self.find_container_id(&profile.spec().name)
                    .await?
                    .is_none(),
                "provider name exists; original recovery required"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = preflight {
            return Ok(ProviderCreation::not_attempted(attempt, error));
        }
        // Return the original CID without any fallible post-create inspection.
        self.create_container_entrypoint(profile.spec(), false, Some(ENTRYPOINT))
            .await
            .map(ProviderCreation::Created)
    }
    pub(super) async fn inspect_provider_actor(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
    ) -> Result<ContainerState> {
        handle_matches(handle, profile)?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        let state =
            Self::verify_application_gateway_configuration(&value, handle, profile, &image)?;
        self.require_application_gateway_uplink(profile).await?;
        let uplink = self
            .inspect_packet_uplink(&profile.spec().network)
            .await?
            .context("provider uplink unavailable")?;
        ensure!(
            uplink.members.keys().all(|id| id == &handle.id),
            "provider uplink has a foreign member"
        );
        if state.running {
            let endpoint = uplink
                .members
                .get(&handle.id)
                .context("running provider lost its uplink")?;
            let native = value
                .pointer("/NetworkSettings/Networks")
                .and_then(Value::as_object)
                .and_then(|networks| networks.values().next())
                .and_then(|network| network.get("EndpointID"))
                .and_then(Value::as_str)
                .context("provider endpoint unavailable")?;
            ensure!(
                endpoint.endpoint_id == native && endpoint.name == handle.name,
                "provider uplink endpoint differs"
            );
        }
        Ok(state)
    }
    pub(super) async fn launch_provider_actor(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
        attempt: StartAttempt,
    ) -> Result<PrivateProcessStdio> {
        attempt.require(profile, handle)?;
        let state = self.inspect_provider_actor(handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            state.status == "created" && Self::application_gateway_stopped(&value),
            "provider start requires its exact never-started actor"
        );
        let mut child = self.docker_command().await?
            .args(["start", "--attach", "--interactive", &handle.id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("missing provider input")?,
            output: child.stdout.take().context("missing provider output")?,
            diagnostics: child.stderr.take(),
            child,
        })
    }
    pub(super) async fn inspect_unstarted_provider_actor(
        &self,
        profile: &Profile,
    ) -> Result<ContainerHandle> {
        let id = self
            .find_container_id(&profile.spec().name)
            .await?
            .context("provider create uncertain; name absence is not retirement")?;
        let handle = ContainerHandle {
            id,
            name: profile.spec().name.clone(),
        };
        let state = self.inspect_provider_actor(&handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            state.status == "created" && Self::application_gateway_stopped(&value),
            "unbound provider recovery requires its never-started actor"
        );
        Ok(handle)
    }
    pub(super) async fn stop_provider_actor(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
    ) -> Result<()> {
        handle_matches(handle, profile)?;
        timeout(Duration::from_secs(15), async {
            let value = self.gateway_inspection("container", &handle.id).await?;
            Self::require_application_gateway_owner(&value, handle, profile)?;
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
            Self::require_application_gateway_owner(&value, handle, profile)?;
            if !Self::application_gateway_stopped(&value) {
                attempt?;
                bail!("original provider termination unconfirmed");
            }
            Ok(())
        })
        .await
        .context("provider native stop deadline")?
    }
    pub(super) async fn remove_provider_actor(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
    ) -> Result<()> {
        handle_matches(handle, profile)?;
        if self.find_container_id(&handle.id).await?.is_none() {
            return Ok(());
        }
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::require_application_gateway_owner(&value, handle, profile)?;
        ensure!(
            Self::application_gateway_stopped(&value),
            "provider removal requires confirmed stopped state"
        );
        let attempt = self
            .run_capture_with_timeout(
                &["rm".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        if self.find_container_id(&handle.id).await?.is_some() {
            attempt?;
            bail!("provider removal unconfirmed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
