//! Reconcile ONLY a recorded, exact creation binding that never started.
//! Returned identity is observation data; the controller fsyncs it before any
//! owned stop/remove. No creation, execution or name-absence acknowledgement.
use super::*;
use crate::marketplace::runtime::source_writer::SourceWriterIntent;

impl DockerRuntimeManager {
    pub(super) fn verify_unbound_source_creation(
        value: &Value,
        handle: &ContainerHandle,
        intent: &SourceWriterIntent,
    ) -> Result<()> {
        intent.validate()?;
        let identity = ContainerHandle {
            id: handle.id.clone(),
            name: handle.id.clone(),
        };
        let state = Self::require_quarantine_writer(value, &identity)?;
        let labels = Self::extract_labels(value);
        if handle.name != intent.name()
            || state.name != intent.name()
            || state.status != "created"
            || state.running
            || labels.get(ELIXIR_DEPLOYMENT_ID_LABEL).map(String::as_str)
                != Some(intent.deployment())
            || labels.get(CONTAINER_SPEC_HASH_LABEL).map(String::as_str)
                != Some(intent.fingerprint())
            || ["Running", "Paused", "Restarting", "Dead", "OOMKilled"]
                .iter()
                .any(|key| {
                    value
                        .pointer(&format!("/State/{key}"))
                        .and_then(Value::as_bool)
                        != Some(false)
                })
            || value.pointer("/State/Pid").and_then(Value::as_u64) != Some(0)
            || value.pointer("/State/ExitCode").and_then(Value::as_i64) != Some(0)
            || value.pointer("/State/Error").and_then(Value::as_str) != Some("")
            || ["StartedAt", "FinishedAt"].iter().any(|key| {
                value
                    .pointer(&format!("/State/{key}"))
                    .and_then(Value::as_str)
                    != Some("0001-01-01T00:00:00Z")
            })
            || value
                .pointer("/HostConfig/RestartPolicy/Name")
                .and_then(Value::as_str)
                != Some("no")
            || value.pointer("/HostConfig/Init").and_then(Value::as_bool) != Some(false)
        {
            bail!("unbound source recovery requires its exact never-started creation binding");
        }
        Ok(())
    }
    pub(super) async fn inspect_unstarted_source_creation(
        &self,
        intent: &SourceWriterIntent,
    ) -> Result<ContainerHandle> {
        intent.validate()?;
        let name = intent.name();
        let id = self
            .find_container_id(&name)
            .await?
            .context("source creation remains uncertain; name absence is not retirement")?;
        let handle = ContainerHandle { id, name };
        let value = self.inspect_container(&handle.id).await?;
        Self::verify_unbound_source_creation(&value, &handle, intent)?;
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unbound_source_observation_requires_exact_binding_and_never_started_state() -> Result<()> {
        let intent: SourceWriterIntent = serde_json::from_value(
            serde_json::json!({"deployment":"owned","fingerprint":"a".repeat(64)}),
        )?;
        let h = ContainerHandle {
            id: "b".repeat(64),
            name: intent.name(),
        };
        let value = serde_json::json!({"Id":h.id,"Name":format!("/{}",h.name),
            "Config":{"Labels":{"elixir.managed":"true","elixir.deployment_id":"owned",
                "elixir.extension_id":"elixir-marketplace-source","elixir.instance_id":"marketplace-source-owned",
                "elixir.marketplace.isolation":"quarantine-v1","elixir.spec_hash":"a".repeat(64)}},
            "State":{"Status":"created","Running":false,"Paused":false,"Restarting":false,"Dead":false,"OOMKilled":false,
                "Pid":0,"ExitCode":0,"Error":"","StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"},
            "HostConfig":{"Init":false,"RestartPolicy":{"Name":"no"}}});
        DockerRuntimeManager::verify_unbound_source_creation(&value, &h, &intent)?;
        for (pointer, wrong) in [
            ("/Id", serde_json::json!("c".repeat(64))),
            ("/Name", serde_json::json!("/foreign")),
            (
                "/Config/Labels/elixir.deployment_id",
                serde_json::json!("foreign"),
            ),
            (
                "/Config/Labels/elixir.spec_hash",
                serde_json::json!("c".repeat(64)),
            ),
            (
                "/Config/Labels/elixir.marketplace.isolation",
                serde_json::json!("foreign"),
            ),
            ("/State/Status", serde_json::json!("exited")),
            ("/State/Running", serde_json::json!(true)),
            ("/State/Paused", serde_json::json!(true)),
            ("/State/Pid", serde_json::json!(1)),
            (
                "/State/StartedAt",
                serde_json::json!("2026-01-01T00:00:00Z"),
            ),
            (
                "/State/FinishedAt",
                serde_json::json!("2026-01-01T00:00:00Z"),
            ),
            ("/State/Error", serde_json::json!("start failed")),
            (
                "/HostConfig/RestartPolicy/Name",
                serde_json::json!("always"),
            ),
            ("/HostConfig/Init", serde_json::json!(true)),
        ] {
            let mut bad = value.clone();
            *bad.pointer_mut(pointer).unwrap() = wrong;
            assert!(
                DockerRuntimeManager::verify_unbound_source_creation(&bad, &h, &intent).is_err(),
                "{pointer}"
            );
        }
        Ok(())
    }
}
