//! Native process lifecycle only, not a protected application-data capability.
//! No public route calls this without its original installed/native owner.
use super::*;
use crate::marketplace::runtime::application_http::HttpCreateAttempt;
use crate::marketplace::runtime::application_http::{ApplicationHttpProfile, ENTRYPOINT};
use crate::runtime::http_creation::HttpCreation;

fn require_handle(handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<()> {
    if !immutable_id(&handle.id) || handle.name != profile.spec().name {
        bail!("application HTTP requires its original immutable helper identity");
    }
    Ok(())
}
fn features(labels: &HashMap<String, String>) -> Result<()> {
    for name in [
        "sdk_http",
        "sdk_http_stream",
        "sdk_http_stream_large",
        "native_deadline",
    ] {
        if labels
            .get(&format!("elixir.marketplace.worker.{name}"))
            .map(String::as_str)
            != Some("1")
        {
            bail!("application HTTP requires its exact immutable image capabilities");
        }
    }
    Ok(())
}
impl DockerRuntimeManager {
    pub(super) fn require_application_http_owner(
        value: &Value,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<ContainerState> {
        require_handle(handle, profile)?;
        let state = Self::extract_state(value)?;
        let labels = Self::extract_labels(value);
        if state.id != handle.id
            || state.name != profile.spec().name
            || profile
                .spec()
                .labels
                .iter()
                .any(|(k, v)| labels.get(k) != Some(v))
            || value
                .pointer("/State/Running")
                .and_then(Value::as_bool)
                .is_none()
            || !matches!(
                state.status.as_str(),
                "created" | "running" | "paused" | "exited" | "dead"
            )
            || matches!(state.status.as_str(), "running" | "paused") != state.running
        {
            bail!("application HTTP owner or native state differs");
        }
        Ok(state)
    }
    fn verify_application_http_configuration(
        value: &Value,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<ContainerState> {
        let state = Self::require_application_http_owner(value, handle, profile)?;
        Self::verify_private_start_intent(value, profile.spec(), ENTRYPOINT)?;
        profile.verify(handle, &Self::extract_private_actor(value)?)?;
        Ok(state)
    }
    fn application_http_stopped(value: &Value) -> bool {
        matches!(
            value.pointer("/State/Status").and_then(Value::as_str),
            Some("created" | "exited" | "dead")
        ) && ["Running", "Paused", "Restarting"].iter().all(|field| {
            value
                .pointer(&format!("/State/{field}"))
                .and_then(Value::as_bool)
                == Some(false)
        }) && value.pointer("/State/Pid").and_then(Value::as_i64) == Some(0)
    }
}

// All methods inspect by immutable ID, never a reusable lookup name. Unknown
// create/start/kill outcomes are returned unchanged for the held owner's durable
// reconciliation; there is no implicit replacement, retry, refund or repair.
impl DockerRuntimeManager {
    pub(super) async fn inspect_unstarted_application_http_actor(
        &self,
        profile: &ApplicationHttpProfile,
    ) -> Result<ContainerHandle> {
        let id = self
            .find_container_id(&profile.spec().name)
            .await?
            .context("HTTP creation remains uncertain; name absence is not retirement")?;
        let handle = ContainerHandle {
            id,
            name: profile.spec().name.clone(),
        };
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::verify_application_http_configuration(&value, &handle, profile)?;
        if state.status != "created" || !Self::application_http_stopped(&value) {
            bail!("unbound HTTP recovery requires its exact never-started helper");
        }
        Ok(handle)
    }
    pub(super) async fn create_application_http_actor(
        &self,
        profile: &ApplicationHttpProfile,
        attempt: HttpCreateAttempt,
    ) -> Result<HttpCreation> {
        let preflight = async {
            attempt.require_profile(profile)?;
            features(&self.image_labels(&profile.spec().image).await?)?;
            if self
                .find_container_id(&profile.spec().name)
                .await?
                .is_some()
            {
                bail!("application HTTP name already exists; original owned recovery required");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = preflight {
            return Ok(HttpCreation::not_attempted(attempt, error));
        }
        // Return CID without another fallible post-create operation. The caller
        // already has write-ahead intent and must bind CID before START.
        self.create_container_entrypoint(profile.spec(), false, Some(ENTRYPOINT))
            .await
            .map(HttpCreation::Created)
    }
    pub(super) async fn start_application_http_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<()> {
        require_handle(handle, profile)?;
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::verify_application_http_configuration(&value, handle, profile)?;
        if state.status != "created" || !Self::application_http_stopped(&value) {
            bail!("application HTTP start requires its exact never-started helper");
        }
        features(&self.image_labels(&profile.spec().image).await?)?;
        self.run_capture(&["start".into(), handle.id.clone()])
            .await?;
        Ok(())
    }
    pub(super) async fn attach_application_http_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<PrivateProcessStdio> {
        require_handle(handle, profile)?;
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::verify_application_http_configuration(&value, handle, profile)?;
        if state.status != "running" || !state.running {
            bail!("application HTTP attachment requires its running original helper");
        }
        self.attach_private_stdio(handle, &profile.spec().labels)
            .await
    }
    pub(super) async fn confirm_application_http_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<()> {
        require_handle(handle, profile)?;
        let value = self.inspect_container(&handle.id).await?;
        Self::verify_application_http_configuration(&value, handle, profile)?;
        Self::verify_private_native_exit(&value, handle, &profile.spec().labels)
    }
    pub(super) async fn stop_application_http_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<()> {
        require_handle(handle, profile)?;
        timeout(Duration::from_secs(15), async {
            let value = self.inspect_container(&handle.id).await?;
            Self::require_application_http_owner(&value, handle, profile)?;
            if Self::application_http_stopped(&value) {
                return Ok(());
            }
            let attempt = self
                .run_capture_with_timeout(
                    &["kill".into(), "--signal=KILL".into(), handle.id.clone()],
                    Some(DOCKER_PROBE_COMMAND_TIMEOUT),
                )
                .await;
            let value = self.inspect_container(&handle.id).await?;
            Self::require_application_http_owner(&value, handle, profile)?;
            if !Self::application_http_stopped(&value) {
                attempt?;
                bail!("application HTTP native termination remains unconfirmed");
            }
            Ok(())
        })
        .await
        .context("application HTTP stop deadline")?
    }
    pub(super) async fn remove_application_http_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationHttpProfile,
    ) -> Result<()> {
        require_handle(handle, profile)?;
        if self.find_container_id(&handle.id).await?.is_none() {
            return Ok(());
        }
        let value = self.inspect_container(&handle.id).await?;
        Self::require_application_http_owner(&value, handle, profile)?;
        if !Self::application_http_stopped(&value) {
            bail!("application HTTP removal requires positively stopped native state");
        }
        let attempt = self
            .run_capture_with_timeout(
                &["rm".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        if self.find_container_id(&handle.id).await?.is_some() {
            attempt?;
            bail!("application HTTP removal remains unconfirmed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
