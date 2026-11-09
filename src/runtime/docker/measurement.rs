//! The measurement actor never loads extension bytes or accepts commands.
use super::*;
use crate::runtime::measurement::{ENTRYPOINT, MeasurementSpec, name};

impl DockerRuntimeManager {
    pub(super) async fn create_measurement_actor(
        &self,
        profile: &MeasurementSpec,
    ) -> Result<ContainerHandle> {
        let spec = profile.spec();
        if self.get_container_handle(&spec.name).await?.is_some() {
            bail!("measurement name already exists; explicit owned recovery required");
        }
        self.create_container_entrypoint(spec, false, Some(ENTRYPOINT))
            .await
    }
    pub(super) async fn start_measurement_actor(
        &self,
        handle: &ContainerHandle,
        profile: &MeasurementSpec,
    ) -> Result<()> {
        let spec = profile.spec();
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::require_measurement_actor(&value, handle)?;
        if state.status != "created" || state.running || state.name != spec.name {
            bail!("measurement start requires its exact never-started actor");
        }
        Self::verify_private_start_intent(&value, spec, ENTRYPOINT)?;
        crate::marketplace::runtime::verify_measurement_configuration(
            spec,
            &Self::extract_runtime_state(&value),
            profile.limits(),
        )?;
        self.run_capture(&["start".into(), handle.id.clone()])
            .await?;
        Ok(())
    }
    pub(super) fn require_measurement_actor(
        value: &Value,
        handle: &ContainerHandle,
    ) -> Result<ContainerState> {
        let state = Self::extract_state(value)?;
        let labels = Self::extract_labels(value);
        let deployment = labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 64
                    && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .context("measurement deployment unavailable")?;
        if !immutable_id(&handle.id)
            || handle.name != handle.id
            || state.id != handle.id
            || state.name != name(deployment)
            || labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("measurement-v1")
            || labels.get(ELIXIR_EXTENSION_ID_LABEL).map(String::as_str)
                != Some("elixir-marketplace-measurement")
            || labels.get(ELIXIR_INSTANCE_ID_LABEL)
                != Some(&format!("marketplace-measurement-{deployment}"))
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
            bail!("unowned or ambiguous native measurement identity");
        }
        Ok(state)
    }
    pub(super) async fn stop_measurement_actor(&self, handle: &ContainerHandle) -> Result<()> {
        timeout(Duration::from_secs(15), async {
            let value = self.inspect_container(&handle.id).await?;
            if !Self::require_measurement_actor(&value, handle)?.running {
                return Ok(());
            }
            let attempt = self
                .run_capture_with_timeout(
                    &["kill".into(), "--signal=KILL".into(), handle.id.clone()],
                    Some(DOCKER_PROBE_COMMAND_TIMEOUT),
                )
                .await;
            let value = self.inspect_container(&handle.id).await?;
            if Self::require_measurement_actor(&value, handle)?.running {
                attempt?;
                bail!("native measurement is still running");
            }
            Ok(())
        })
        .await
        .context("native measurement stop deadline")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn measurement_cleanup_requires_immutable_owned_role_and_positive_native_state() -> Result<()> {
        let id = "a".repeat(64);
        let handle = ContainerHandle {
            id: id.clone(),
            name: id.clone(),
        };
        let fixture = serde_json::json!({
            "Id":id,"Name":"/elixir-marketplace-measurement-owned",
            "State":{"Status":"running","Running":true},
            "Config":{"Labels":{
                "elixir.managed":"true","elixir.deployment_id":"owned",
                "elixir.extension_id":"elixir-marketplace-measurement",
                "elixir.instance_id":"marketplace-measurement-owned",
                "elixir.marketplace.isolation":"measurement-v1"
            }}
        });
        assert!(DockerRuntimeManager::require_measurement_actor(&fixture, &handle)?.running);
        for pointer in [
            "/Id",
            "/Name",
            "/State/Status",
            "/Config/Labels/elixir.managed",
            "/Config/Labels/elixir.deployment_id",
            "/Config/Labels/elixir.extension_id",
            "/Config/Labels/elixir.instance_id",
            "/Config/Labels/elixir.marketplace.isolation",
        ] {
            let mut wrong = fixture.clone();
            *wrong.pointer_mut(pointer).unwrap() = "foreign".into();
            assert!(
                DockerRuntimeManager::require_measurement_actor(&wrong, &handle).is_err(),
                "{pointer}"
            );
        }
        let mut unknown = fixture.clone();
        unknown["State"].as_object_mut().unwrap().remove("Running");
        assert!(DockerRuntimeManager::require_measurement_actor(&unknown, &handle).is_err());
        let mut contradictory = fixture.clone();
        contradictory["State"]["Running"] = false.into();
        assert!(DockerRuntimeManager::require_measurement_actor(&contradictory, &handle).is_err());
        let named = ContainerHandle {
            id,
            name: "elixir-marketplace-measurement-owned".into(),
        };
        assert!(DockerRuntimeManager::require_measurement_actor(&fixture, &named).is_err());
        Ok(())
    }
}
