//! Fixed provider namespace observation and create-only guard lifecycle.
//! No general command, image pull, arbitrary namespace or network permission.
use super::*;
use crate::marketplace::provider::{GuardCreateAttempt, GuardProfile, GuardStartAttempt, Profile};
use crate::runtime::provider_creation::GuardCreation;
use anyhow::ensure;
const ENTRY: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-netguard";

fn namespace(text: &str) -> Result<u64> {
    let text = text.trim();
    let value = text
        .strip_prefix("net:[")
        .and_then(|v| v.strip_suffix(']'))
        .context("invalid provider namespace observation")?;
    ensure!(
        !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "invalid namespace number"
    );
    let inode: u64 = value.parse()?;
    ensure!(
        (1..(1u64 << 53)).contains(&inode),
        "provider namespace number bounds"
    );
    Ok(inode)
}
impl DockerRuntimeManager {
    pub(super) async fn provider_native_namespace(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
    ) -> Result<u64> {
        ensure!(
            self.inspect_provider_actor(handle, profile).await?.running,
            "provider namespace is not live"
        );
        let (status, out, _) = self
            .packet_network_capture(&[
                "exec".into(),
                "--user".into(),
                "0:0".into(),
                handle.id.clone(),
                "/usr/bin/readlink".into(),
                "/proc/self/ns/net".into(),
            ])
            .await?;
        ensure!(
            status.success() && out.len() <= 64,
            "provider namespace read failed"
        );
        let inode = namespace(&out)?;
        ensure!(
            self.inspect_provider_actor(handle, profile).await?.running,
            "provider exited during namespace read"
        );
        Ok(inode)
    }
    async fn provider_guard_image(&self, profile: &GuardProfile) -> Result<()> {
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
            "provider guard image has incompatible execution or storage"
        );
        for feature in [
            "schema",
            "kernel_report",
            "firewall_integrity",
            "network_probe",
            "provider_tun_probe",
            "provider_firewall",
            "credit_usage",
        ] {
            ensure!(
                image["Config"]["Labels"][format!("elixir.marketplace.guard.{feature}")] == "1",
                "provider guard image protocol missing"
            );
        }
        Ok(())
    }
    async fn provider_guard_parent(&self, profile: &GuardProfile) -> Result<()> {
        let (parent, handle) = profile.parent();
        ensure!(
            self.provider_native_namespace(&handle, parent).await? == profile.namespace(),
            "provider original namespace changed"
        );
        Ok(())
    }
    pub(super) async fn create_provider_guard_actor(
        &self,
        profile: &GuardProfile,
        attempt: GuardCreateAttempt,
    ) -> Result<GuardCreation> {
        let preflight = async {
            attempt.require_profile(profile)?;
            container_resource_args(&profile.spec().security)?;
            self.provider_guard_image(profile).await?;
            self.provider_guard_parent(profile).await?;
            ensure!(
                self.find_container_id(&profile.spec().name)
                    .await?
                    .is_none(),
                "guard name exists; original recovery required"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = preflight {
            return Ok(GuardCreation::not_attempted(attempt, error));
        }
        self.create_container_entrypoint(profile.spec(), false, Some(ENTRY))
            .await
            .map(GuardCreation::Created)
    }
    pub(super) async fn inspect_provider_guard_actor(
        &self,
        handle: &ContainerHandle,
        profile: &GuardProfile,
    ) -> Result<PrivateActorInspection> {
        ensure!(
            immutable_id(&handle.id) && handle.name == profile.spec().name,
            "guard requires original native identity"
        );
        self.provider_guard_image(profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        let actual = Self::extract_private_actor(&value)?;
        profile.verify(handle, &actual)?;
        Ok(actual)
    }
    pub(super) async fn launch_provider_guard_actor(
        &self,
        handle: &ContainerHandle,
        profile: &GuardProfile,
        attempt: GuardStartAttempt,
    ) -> Result<PrivateProcessStdio> {
        attempt.require(profile, handle)?;
        self.inspect_provider_guard_actor(handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            value["State"]["Status"] == "created" && Self::application_gateway_stopped(&value),
            "guard START requires exact never-started object"
        );
        self.provider_guard_parent(profile).await?;
        let mut child = Command::new(&self.docker_bin)
            .args(["start", "--attach", "--interactive", &handle.id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("guard input missing")?,
            output: child.stdout.take().context("guard output missing")?,
            diagnostics: child.stderr.take(),
            child,
        })
    }
    pub(super) async fn inspect_unstarted_provider_guard_actor(
        &self,
        profile: &GuardProfile,
    ) -> Result<ContainerHandle> {
        let id = self
            .find_container_id(&profile.spec().name)
            .await?
            .context("guard create uncertain; name absence is not retirement")?;
        let handle = ContainerHandle {
            id,
            name: profile.spec().name.clone(),
        };
        self.inspect_provider_guard_actor(&handle, profile).await?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        ensure!(
            value["State"]["Status"] == "created" && Self::application_gateway_stopped(&value),
            "guard unknown CREATE recovery requires exact unstarted object"
        );
        Ok(handle)
    }
    fn guard_owner(value: &Value, handle: &ContainerHandle, profile: &GuardProfile) -> Result<()> {
        ensure!(
            immutable_id(&handle.id)
                && handle.name == profile.spec().name
                && value["Id"] == handle.id
                && value["Name"] == format!("/{}", handle.name)
                && profile
                    .spec()
                    .labels
                    .iter()
                    .all(|(key, v)| value["Config"]["Labels"][key] == *v),
            "guard native owner differs"
        );
        Ok(())
    }
    pub(super) async fn stop_provider_guard_actor(
        &self,
        handle: &ContainerHandle,
        profile: &GuardProfile,
    ) -> Result<()> {
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::guard_owner(&value, handle, profile)?;
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
        Self::guard_owner(&value, handle, profile)?;
        if !Self::application_gateway_stopped(&value) {
            attempt?;
            bail!("guard native termination unconfirmed");
        }
        Ok(())
    }
    pub(super) async fn remove_provider_guard_actor(
        &self,
        handle: &ContainerHandle,
        profile: &GuardProfile,
    ) -> Result<()> {
        if self.find_container_id(&handle.id).await?.is_none() {
            return Ok(());
        }
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::guard_owner(&value, handle, profile)?;
        ensure!(
            Self::application_gateway_stopped(&value),
            "guard removal requires confirmed stopped state"
        );
        let attempt = self
            .run_capture_with_timeout(
                &["rm".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        if self.find_container_id(&handle.id).await?.is_some() {
            attempt?;
            bail!("guard native removal unconfirmed");
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provider_namespace_observation_is_exact_bounded_and_not_a_network_grant() {
        assert_eq!(namespace("net:[12345]\n").unwrap(), 12345);
        for bad in [
            "net:[0]",
            "net:[-1]",
            "pid:[12345]",
            "net:[9007199254740992]",
            "net:[123] extra",
            "net:[1]\nnet:[2]",
        ] {
            assert!(namespace(bad).is_err(), "{bad}");
        }
    }
}
