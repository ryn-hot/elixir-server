//! Exact native lifecycle, NOT a live VPN/readiness/admission capability. The
//! original private owner must retain create/start/material tasks and journal.
use super::*;
use crate::{
    marketplace::application_egress::{
        ApplicationGatewayProfile, ENTRYPOINT, GatewayCreateAttempt,
    },
    runtime::gateway_creation::GatewayCreation,
};

/// Private closed variants only. A caller cannot pass an arbitrary spec,
/// executable, resource profile or image contract through RuntimeManager.
pub(super) trait TunnelActor {
    fn spec(&self) -> &ContainerSpec;
    fn image_entrypoint(&self) -> &'static str;
    fn entrypoint(&self) -> &'static str;
    fn provider(&self) -> bool {
        false
    }
    fn network_owner(&self) -> Option<String> {
        None
    }
}
impl TunnelActor for ApplicationGatewayProfile {
    fn spec(&self) -> &ContainerSpec {
        ApplicationGatewayProfile::spec(self)
    }
    fn image_entrypoint(&self) -> &'static str {
        "/gluetun-entrypoint"
    }
    fn entrypoint(&self) -> &'static str {
        ENTRYPOINT
    }
}
impl TunnelActor for crate::marketplace::provider::Profile {
    fn spec(&self) -> &ContainerSpec {
        crate::marketplace::provider::Profile::spec(self)
    }
    fn image_entrypoint(&self) -> &'static str {
        crate::marketplace::provider::ENTRYPOINT
    }
    fn entrypoint(&self) -> &'static str {
        crate::marketplace::provider::ENTRYPOINT
    }
    fn provider(&self) -> bool {
        true
    }
    fn network_owner(&self) -> Option<String> {
        Some(self.expected_network_owner())
    }
}
pub(super) fn handle_matches(handle: &ContainerHandle, profile: &impl TunnelActor) -> Result<()> {
    if !immutable_id(&handle.id) || handle.name != profile.spec().name {
        bail!("gateway requires its original immutable native identity");
    }
    Ok(())
}
fn empty_array(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
        || matches!(value,Some(Value::Array(values)) if values.is_empty())
}
fn empty_map(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
        || matches!(value,Some(Value::Object(values)) if values.is_empty())
}
fn working_directory(value: Option<&Value>) -> Result<&str> {
    match value {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(value)) if value.len() <= 4096 && !value.contains('\0') => Ok(value),
        _ => bail!("invalid gateway working directory"),
    }
}
fn environment(value: &Value) -> Result<BTreeMap<String, String>> {
    let values = value
        .as_array()
        .context("gateway image environment unavailable")?;
    if values.len() > 256 {
        bail!("gateway environment bounds");
    }
    let mut map = BTreeMap::new();
    for value in values {
        let value = value
            .as_str()
            .context("invalid gateway environment entry")?;
        let (key, value) = value
            .split_once('=')
            .context("invalid gateway environment entry")?;
        if key.is_empty()
            || key.len() > 128
            || value.len() > 8192
            || value.contains('\0')
            || !key
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
            || map.insert(key.to_owned(), value.to_owned()).is_some()
        {
            bail!("gateway environment contains invalid or duplicate keys");
        }
    }
    Ok(map)
}
fn strings(value: Option<&Value>) -> Result<Vec<String>> {
    match value {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::Array(values)) if values.len() <= 256 => values
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .context("invalid gateway native string array")
            })
            .collect(),
        _ => bail!("invalid gateway native string array"),
    }
}
fn caps_match(value: Option<&Value>, expected: &[String]) -> Result<bool> {
    let values = strings(value)?;
    let normalize = |v: &str| v.to_ascii_uppercase().trim_start_matches("CAP_").to_owned();
    let actual: HashSet<_> = values.iter().map(|v| normalize(v)).collect();
    Ok(actual.len() == values.len() && actual == expected.iter().map(|v| normalize(v)).collect())
}
impl DockerRuntimeManager {
    /// Bounded even for a foreign collision's enormous labels/environment.
    pub(super) async fn gateway_inspection(&self, kind: &str, reference: &str) -> Result<Value> {
        let mut args = if kind == "image" {
            vec!["image".into()]
        } else {
            vec![]
        };
        args.extend([
            "inspect".into(),
            "--format".into(),
            "{{json .}}".into(),
            reference.into(),
        ]);
        let (status, output, _) = self.packet_network_capture(&args).await?;
        if !status.success() {
            bail!("original gateway inspection unavailable");
        }
        let value: Value =
            serde_json::from_str(&output).context("invalid gateway native inspection")?;
        if !value.is_object() {
            bail!("invalid gateway native inspection shape");
        }
        Ok(value)
    }
    pub(super) fn gateway_image_contract(
        image: &Value,
        profile: &impl TunnelActor,
    ) -> Result<BTreeMap<String, String>> {
        if image.get("Id").and_then(Value::as_str) != Some(profile.spec().image.as_str())
            || image.pointer("/Config/Entrypoint")
                != Some(&serde_json::json!([profile.image_entrypoint()]))
            || !empty_array(image.pointer("/Config/Cmd"))
            || !empty_map(image.pointer("/Config/Volumes"))
        {
            bail!(
                "gateway requires its immutable platform image without anonymous volumes or extra command"
            );
        }
        if profile.provider() {
            for (key, expected) in [
                ("schema", "1"),
                ("state", "opaque-private-v1"),
                ("mode", "tunnel_only"),
                ("protocol", "MASQUE"),
                ("masque_options", "h3-only"),
                ("connectivity_checks", "supervisor"),
            ] {
                if image
                    .pointer(&format!("/Config/Labels/elixir.marketplace.provider.{key}"))
                    .and_then(Value::as_str)
                    != Some(expected)
                {
                    bail!("provider image lacks its fixed private protocol contract");
                }
            }
            if image.pointer("/Config/Healthcheck/Test") != Some(&serde_json::json!(["NONE"])) {
                bail!("provider inherited health/proxy entrypoint is not disabled");
            }
        }
        let mut env = environment(
            image
                .pointer("/Config/Env")
                .context("gateway image environment unavailable")?,
        )?;
        // Image content is trusted platform code, not publisher code. Still
        // reject embedded credential/hook settings instead of silently using
        // them in place of the original private material channel.
        for key in [
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "ENV",
            "BASH_ENV",
            "WIREGUARD_PRIVATE_KEY",
            "WIREGUARD_PRESHARED_KEY",
            "OPENVPN_USER",
            "OPENVPN_PASSWORD",
            "OPENVPN_KEY_PASSPHRASE",
        ] {
            if env.get(key).is_some_and(|value| !value.is_empty()) {
                bail!("gateway image contains embedded hooks or credentials");
            }
        }
        for value in &profile.spec().env {
            env.insert(value.name.clone(), value.value.clone());
        }
        Ok(env)
    }
    pub(super) fn require_application_gateway_owner(
        value: &Value,
        handle: &ContainerHandle,
        profile: &impl TunnelActor,
    ) -> Result<ContainerState> {
        handle_matches(handle, profile)?;
        let state = Self::extract_state(value)?;
        let labels = Self::extract_labels(value);
        if state.id != handle.id
            || state.name != profile.spec().name
            || profile
                .spec()
                .labels
                .iter()
                .any(|(key, value)| labels.get(key) != Some(value))
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
            bail!("gateway owner or native state differs from its original record");
        }
        Ok(state)
    }
    pub(super) fn application_gateway_stopped(value: &Value) -> bool {
        matches!(
            value.pointer("/State/Status").and_then(Value::as_str),
            Some("created" | "exited" | "dead")
        ) && ["Running", "Paused", "Restarting"].iter().all(|key| {
            value
                .pointer(&format!("/State/{key}"))
                .and_then(Value::as_bool)
                == Some(false)
        }) && value.pointer("/State/Pid").and_then(Value::as_i64) == Some(0)
    }
    pub(super) fn verify_application_gateway_configuration(
        value: &Value,
        handle: &ContainerHandle,
        profile: &impl TunnelActor,
        image: &Value,
    ) -> Result<ContainerState> {
        let state = Self::require_application_gateway_owner(value, handle, profile)?;
        let spec = profile.spec();
        let env = Self::gateway_image_contract(image, profile)?;
        let actual = environment(
            value
                .pointer("/Config/Env")
                .context("gateway native environment unavailable")?,
        )?;
        let equal = |pointer: &str, expected: Value| value.pointer(pointer) == Some(&expected);
        let options = strings(value.pointer("/HostConfig/SecurityOpt"))?;
        let mut expected_tmpfs = serde_json::Map::new();
        for mount in &spec.security.tmpfs {
            let formatted = format_tmpfs(mount);
            let (path, options) = formatted
                .split_once(':')
                .context("gateway tmpfs options missing")?;
            expected_tmpfs.insert(path.into(), Value::String(options.into()));
        }
        let ulimits = value
            .pointer("/HostConfig/Ulimits")
            .and_then(Value::as_array)
            .context("gateway ulimits unavailable")?;
        let expected_limits = BTreeMap::from([
            ("core", 0_i64),
            ("nofile", 256),
            (
                "fsize",
                spec.security
                    .file_size_limit_bytes
                    .context("fixed tunnel file limit missing")? as i64,
            ),
        ]);
        let mut seen = HashSet::new();
        for limit in ulimits {
            let name = limit
                .get("Name")
                .and_then(Value::as_str)
                .context("invalid gateway ulimit")?;
            if !seen.insert(name)
                || expected_limits.get(name).is_none_or(|expected| {
                    limit.get("Soft").and_then(Value::as_i64) != Some(*expected)
                        || limit.get("Hard").and_then(Value::as_i64) != Some(*expected)
                })
            {
                bail!("gateway ulimits differ");
            }
        }
        let mounts = value
            .get("Mounts")
            .and_then(Value::as_array)
            .context("gateway mounts unavailable")?;
        let mut mounted = HashSet::new();
        for mount in mounts {
            let path = mount
                .get("Destination")
                .and_then(Value::as_str)
                .context("invalid gateway mount")?;
            if mount.get("Type").and_then(Value::as_str) != Some("tmpfs")
                || !expected_tmpfs.contains_key(path)
                || !mounted.insert(path)
                || mount.get("RW").and_then(Value::as_bool) != Some(true)
            {
                bail!("gateway has an unexpected native mount");
            }
        }
        if !mounted.is_empty() && mounted.len() != expected_tmpfs.len() {
            bail!("gateway tmpfs mount observation is incomplete");
        }
        let networks = value
            .pointer("/NetworkSettings/Networks")
            .and_then(Value::as_object)
            .context("gateway network attachment unavailable")?;
        if networks.len() != 1 {
            bail!("gateway has unexpected native network membership");
        }
        let endpoint = networks.values().next().unwrap();
        let network = endpoint
            .get("NetworkID")
            .and_then(Value::as_str)
            .context("gateway network identity unavailable")?;
        let endpoint_id = endpoint
            .get("EndpointID")
            .and_then(Value::as_str)
            .context("gateway endpoint identity unavailable")?;
        let created = state.status == "created";
        if (network != spec.network && !(created && network.is_empty()))
            || !(immutable_id(endpoint_id) || created && endpoint_id.is_empty())
        {
            bail!("gateway original network identity differs");
        }
        let ports = value.pointer("/NetworkSettings/Ports");
        if !empty_map(ports)
            && !ports
                .and_then(Value::as_object)
                .is_some_and(|rows| rows.values().all(|v| empty_array(Some(v))))
        {
            bail!("gateway exposes native ports");
        }
        let config = value.get("Config").context("gateway config unavailable")?;
        if actual != env
            || !equal("/Config/Image", Value::String(spec.image.clone()))
            || !equal("/Image", Value::String(spec.image.clone()))
            || if spec.command.is_empty() {
                !empty_array(value.pointer("/Config/Cmd"))
            } else {
                !equal("/Config/Cmd", serde_json::to_value(&spec.command)?)
            }
            || !equal(
                "/Config/Entrypoint",
                serde_json::json!([profile.entrypoint()]),
            )
            || (profile.provider()
                && value.pointer("/Config/Healthcheck/Test") != Some(&serde_json::json!(["NONE"])))
            || !equal("/Config/User", Value::String("0:0".into()))
            || !equal("/Config/OpenStdin", Value::Bool(true))
            || !equal("/Config/Tty", Value::Bool(false))
            || working_directory(config.get("WorkingDir"))?
                != working_directory(image.pointer("/Config/WorkingDir"))?
            || !equal(
                "/HostConfig/NetworkMode",
                Value::String(spec.network.clone()),
            )
            || !equal("/HostConfig/ReadonlyRootfs", Value::Bool(true))
            || !equal("/HostConfig/Privileged", Value::Bool(false))
            || options.len() != 1
            || !matches!(
                options[0].as_str(),
                "no-new-privileges" | "no-new-privileges:true"
            )
            || !caps_match(value.pointer("/HostConfig/CapAdd"), &spec.cap_add)?
            || !caps_match(value.pointer("/HostConfig/CapDrop"), &spec.cap_drop)?
            || !equal("/HostConfig/Tmpfs", Value::Object(expected_tmpfs))
            || !equal(
                "/HostConfig/Memory",
                serde_json::json!(
                    spec.security
                        .memory_limit_mb
                        .context("fixed tunnel memory limit missing")?
                        * 1024
                        * 1024
                ),
            )
            || !equal(
                "/HostConfig/MemorySwap",
                serde_json::json!(
                    spec.security
                        .memory_swap_limit_mb
                        .context("fixed tunnel swap limit missing")?
                        * 1024
                        * 1024
                ),
            )
            || !equal(
                "/HostConfig/NanoCpus",
                serde_json::json!(if profile.provider() {
                    200_000_000
                } else {
                    100_000_000
                }),
            )
            || !equal("/HostConfig/PidsLimit", serde_json::json!(64))
            || !equal("/HostConfig/ShmSize", serde_json::json!(1024 * 1024))
            || seen.len() != expected_limits.len()
            || !equal("/HostConfig/Init", Value::Bool(spec.security.init_process))
            || !equal("/HostConfig/PidMode", Value::String(String::new()))
            || !equal("/HostConfig/IpcMode", Value::String("private".into()))
            || !matches!(
                value
                    .pointer("/HostConfig/UsernsMode")
                    .and_then(Value::as_str),
                Some("" | "private")
            )
            || !equal("/HostConfig/RestartPolicy/Name", Value::String("no".into()))
            || !equal("/HostConfig/PublishAllPorts", Value::Bool(false))
            || !equal("/HostConfig/LogConfig/Type", Value::String("none".into()))
            || !empty_map(value.pointer("/HostConfig/LogConfig/Config"))
            || !equal("/HostConfig/Sysctls", serde_json::to_value(&spec.sysctls)?)
            || !equal(
                "/HostConfig/Devices",
                serde_json::json!([{"PathOnHost":"/dev/net/tun","PathInContainer":"/dev/net/tun","CgroupPermissions":"rwm"}]),
            )
            || !empty_map(value.pointer("/HostConfig/PortBindings"))
            || [
                "Binds",
                "Mounts",
                "DeviceRequests",
                "Links",
                "ExtraHosts",
                "Dns",
                "DnsSearch",
                "DnsOptions",
            ]
            .iter()
            .any(|key| !empty_array(value.pointer(&format!("/HostConfig/{key}"))))
        {
            bail!("gateway actual configuration differs from its original closed profile");
        }
        Ok(state)
    }
    pub(super) async fn require_application_gateway_uplink(
        &self,
        profile: &impl TunnelActor,
    ) -> Result<()> {
        let actual = self
            .inspect_packet_uplink(&profile.spec().network)
            .await?
            .context("original gateway network unavailable")?;
        if actual.id != profile.spec().network {
            bail!("gateway network identity changed");
        }
        let deployment = actual
            .labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .context("gateway network owner unavailable")?;
        if profile
            .network_owner()
            .is_some_and(|expected| expected != *deployment)
        {
            bail!("provider network belongs to another deployment");
        }
        PacketNetworkSpec::new(deployment, actual.ipv6, actual.internal)?.verify(&actual)
    }
    pub(super) async fn create_application_gateway_actor(
        &self,
        profile: &ApplicationGatewayProfile,
        attempt: GatewayCreateAttempt,
    ) -> Result<GatewayCreation> {
        let preflight = async {
            attempt.require_profile(profile)?;
            Self::gateway_image_contract(
                &self
                    .gateway_inspection("image", &profile.spec().image)
                    .await?,
                profile,
            )?;
            self.require_application_gateway_uplink(profile).await?;
            if self
                .find_container_id(&profile.spec().name)
                .await?
                .is_some()
            {
                bail!("gateway name exists; original owned recovery required");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = preflight {
            return Ok(GatewayCreation::not_attempted(attempt, error));
        }
        // No fallible post-create work before returning the original CID.
        self.create_container_entrypoint(profile.spec(), false, Some(ENTRYPOINT))
            .await
            .map(GatewayCreation::Created)
    }
    pub(super) async fn start_application_gateway_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationGatewayProfile,
    ) -> Result<()> {
        handle_matches(handle, profile)?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        let state =
            Self::verify_application_gateway_configuration(&value, handle, profile, &image)?;
        if state.status != "created" || !Self::application_gateway_stopped(&value) {
            bail!("gateway start requires its exact never-started actor");
        }
        self.require_application_gateway_uplink(profile).await?;
        self.run_capture(&["start".into(), handle.id.clone()])
            .await?;
        Ok(())
    }
    pub(super) async fn inspect_unstarted_application_gateway_actor(
        &self,
        profile: &ApplicationGatewayProfile,
    ) -> Result<ContainerHandle> {
        let id = self
            .find_container_id(&profile.spec().name)
            .await?
            .context("gateway creation uncertain; name absence is not retirement")?;
        let handle = ContainerHandle {
            id,
            name: profile.spec().name.clone(),
        };
        let value = self.gateway_inspection("container", &handle.id).await?;
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        let state =
            Self::verify_application_gateway_configuration(&value, &handle, profile, &image)?;
        if state.status != "created" || !Self::application_gateway_stopped(&value) {
            bail!("unbound gateway recovery requires its never-started actor");
        }
        Ok(handle)
    }
    pub(super) async fn attach_application_gateway_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationGatewayProfile,
    ) -> Result<PrivateProcessStdio> {
        handle_matches(handle, profile)?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        let state =
            Self::verify_application_gateway_configuration(&value, handle, profile, &image)?;
        if state.status != "running" || !state.running {
            bail!("gateway material attachment requires its original running actor");
        }
        self.attach_private_stdio(handle, &profile.spec().labels)
            .await
    }
    pub(super) async fn stop_application_gateway_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationGatewayProfile,
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
                bail!("original gateway native termination unconfirmed");
            }
            Ok(())
        })
        .await
        .context("original gateway stop deadline")?
    }
    pub(super) async fn observe_application_gateway_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationGatewayProfile,
        original: Option<&crate::marketplace::application_egress::ApplicationGatewayObservation>,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        handle_matches(handle, profile)?;
        let value = self.gateway_inspection("container", &handle.id).await?;
        let image = self
            .gateway_inspection("image", &profile.spec().image)
            .await?;
        let state =
            Self::verify_application_gateway_configuration(&value, handle, profile, &image)?;
        if state.status != "running" || !state.running {
            bail!("application gateway observation requires its original running actor");
        }
        // Always override BOTH values, including at bootstrap. Neither an image
        // default nor a partial expectation may select a weaker verification.
        let expected = original.map(|observed| observed.policy_hashes());
        let (status, output, diagnostics) = self
            .packet_network_capture(&[
                "exec".into(),
                "--user".into(),
                "0:0".into(),
                "--env".into(),
                format!(
                    "ELIXIR_APP_POLICY4={}",
                    expected.map(|v| v[0].to_string()).unwrap_or_default()
                ),
                "--env".into(),
                format!(
                    "ELIXIR_APP_POLICY6={}",
                    expected.map(|v| v[1].to_string()).unwrap_or_default()
                ),
                handle.id.clone(),
                "/bin/busybox".into(),
                "timeout".into(),
                "-s".into(),
                "KILL".into(),
                "5".into(),
                "/bin/sh".into(),
                "-c".into(),
                crate::marketplace::application_egress::OBSERVE_GUARD.into(),
            ])
            .await?;
        if !status.success() {
            bail!(
                "application gateway kernel guard or tunnel unavailable ({})",
                probe_failure_phase(&diagnostics)
            );
        }
        let observed =
            crate::marketplace::application_egress::ApplicationGatewayObservation::parse(&output)?;
        if let Some(original) = original
            && !observed.same_guard(original)
        {
            bail!("original application gateway tunnel or policy changed");
        }
        let value = self.gateway_inspection("container", &handle.id).await?;
        let state = Self::require_application_gateway_owner(&value, handle, profile)?;
        if state.status != "running" || !state.running {
            bail!("original application gateway stopped during observation");
        }
        Ok(observed)
    }
    pub(super) async fn remove_application_gateway_actor(
        &self,
        handle: &ContainerHandle,
        profile: &ApplicationGatewayProfile,
    ) -> Result<()> {
        handle_matches(handle, profile)?;
        if self.find_container_id(&handle.id).await?.is_none() {
            return Ok(());
        }
        let value = self.gateway_inspection("container", &handle.id).await?;
        Self::require_application_gateway_owner(&value, handle, profile)?;
        if !Self::application_gateway_stopped(&value) {
            bail!("gateway removal requires positively stopped native state");
        }
        let attempt = self
            .run_capture_with_timeout(
                &["rm".into(), handle.id.clone()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await;
        if self.find_container_id(&handle.id).await?.is_some() {
            attempt?;
            bail!("original gateway removal unconfirmed");
        }
        Ok(())
    }
}

fn probe_failure_phase(diagnostics: &str) -> &'static str {
    match diagnostics
        .lines()
        .find_map(|line| line.strip_prefix("ELIXIR_APP_PROBE_FAILURE:"))
    {
        Some("expectations") => "expectations",
        Some("policy4") => "policy4",
        Some("policy6") => "policy6",
        Some("tunnel_link") => "tunnel_link",
        Some("mtu") => "mtu",
        Some("resources") => "resources",
        _ => "unknown",
    }
}
#[cfg(test)]
mod probe_diagnostics_tests {
    use super::*;
    #[test]
    fn native_probe_diagnostics_return_only_compiled_phase_names_not_stderr_contents() {
        assert_eq!(
            probe_failure_phase("untrusted output\nELIXIR_APP_PROBE_FAILURE:policy6\n"),
            "policy6"
        );
        for invalid in [
            "private text",
            "ELIXIR_APP_PROBE_FAILURE:secret-value",
            "ELIXIR_APP_PROBE_FAILURE:mtu injected",
        ] {
            assert_eq!(probe_failure_phase(invalid), "unknown");
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
