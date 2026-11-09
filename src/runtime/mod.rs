use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::runtime::model::{
    ContainerHandle, ContainerRuntimeState, ContainerSpec, ContainerState,
    FixedMarketplaceVolumeSpec, OwnedDirectoryVolumeSpec, PrivateActorInspection,
    PrivateFileVolumeSpec, QuarantineMarketplaceVolumeSpec,
};

pub mod docker;
#[cfg(unix)]
pub mod container_guardian;
#[cfg(unix)]
pub mod gateway_creation;
pub mod health;
pub mod http_creation;
pub mod measurement;
pub mod model;
#[cfg(unix)]
pub mod image_import;
pub mod packet_network;
pub mod probe;
#[cfg(unix)]
pub mod provider_creation;
#[cfg(unix)]
pub mod provider_observation;

/// Non-serializable private process transport. It carries untrusted bytes, not
/// marketplace verification or permission to perform network/install actions.
pub struct PrivateProcessStdio {
    pub(crate) child: tokio::process::Child,
    pub(crate) input: tokio::process::ChildStdin,
    pub(crate) output: tokio::process::ChildStdout,
    pub(crate) diagnostics: Option<tokio::process::ChildStderr>,
}

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub data_root: String,
    pub extensions_root: String,
    pub downloads_root: String,
    pub media_root: String,
}

impl RuntimePaths {
    pub fn from_roots(storage_root: &str, media_root: &str) -> Self {
        let storage_path = absolutize_path(storage_root);
        let data_root_path = storage_path.parent().unwrap_or(&storage_path).to_path_buf();
        let data_root = data_root_path.to_string_lossy().to_string();
        let extensions_root = storage_path.to_string_lossy().to_string();
        let downloads_root = data_root_path
            .join("downloads")
            .to_string_lossy()
            .to_string();
        let media_root = absolutize_path(media_root).to_string_lossy().to_string();
        Self {
            data_root,
            extensions_root,
            downloads_root,
            media_root,
        }
    }

    pub fn deployment_id(&self) -> String {
        deployment_id_for_storage_root(&self.extensions_root)
    }
}

fn absolutize_path(raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path,
    }
}

pub fn deployment_id_for_storage_root(storage_root: &str) -> String {
    let storage_path = absolutize_path(storage_root);
    let canonical = storage_path.to_string_lossy();
    let hash = blake3::hash(canonical.as_bytes()).to_hex().to_string();
    format!("storage-{}", &hash[..16])
}

#[async_trait]
pub trait RuntimeManager: Send + Sync {
    #[cfg(unix)]
    fn marketplace_container_guardian(&self) -> Option<std::sync::Arc<container_guardian::Profile>> { None }
    #[cfg(unix)]
    fn marketplace_import_profile(&self)->Option<std::sync::Arc<image_import::Profile>> {None}
    async fn marketplace_daemon_id(&self)->anyhow::Result<String> {anyhow::bail!("runtime lacks local image-import daemon identity")}
    async fn inspect_marketplace_import_image(&self,_image:elixir_marketplace::identity::Digest)->anyhow::Result<Vec<u8>> {anyhow::bail!("runtime lacks actual imported-image inspection")}
    #[cfg(unix)]
    async fn provider_transport(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<provider_observation::ProviderTransport> {
        anyhow::bail!("runtime lacks owned provider transport observation")
    }
    #[cfg(unix)]
    async fn probe_provider_exit(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
        _ipv6: bool,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks protected provider DNS/TLS observation")
    }
    #[cfg(unix)]
    async fn provider_namespace(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<u64> {
        anyhow::bail!("runtime lacks owned provider namespace observation")
    }
    #[cfg(unix)]
    async fn create_provider_worker(
        &self,
        _profile: &crate::marketplace::provider::WorkerProfile,
        attempt: crate::marketplace::provider::WorkerCreateAttempt,
    ) -> anyhow::Result<provider_creation::WorkerCreation> {
        Ok(provider_creation::WorkerCreation::not_attempted(
            attempt,
            anyhow::anyhow!("runtime lacks owned provider workers"),
        ))
    }
    #[cfg(unix)]
    async fn launch_provider_worker(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::WorkerProfile,
        _attempt: crate::marketplace::provider::WorkerStartAttempt,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime lacks provider worker startup")
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider_worker(
        &self,
        _profile: &crate::marketplace::provider::WorkerProfile,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks unstarted provider worker recovery")
    }
    #[cfg(unix)]
    async fn stop_provider_worker(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::WorkerProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks provider worker stop")
    }
    #[cfg(unix)]
    async fn remove_provider_worker(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::WorkerProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks provider worker removal")
    }
    #[cfg(unix)]
    async fn create_provider_guard(
        &self,
        _profile: &crate::marketplace::provider::GuardProfile,
        attempt: crate::marketplace::provider::GuardCreateAttempt,
    ) -> anyhow::Result<provider_creation::GuardCreation> {
        Ok(provider_creation::GuardCreation::not_attempted(
            attempt,
            anyhow::anyhow!("runtime lacks provider namespace guards"),
        ))
    }
    #[cfg(unix)]
    async fn launch_provider_guard(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::GuardProfile,
        _attempt: crate::marketplace::provider::GuardStartAttempt,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime lacks provider guard startup")
    }
    #[cfg(unix)]
    async fn inspect_provider_guard(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::GuardProfile,
    ) -> anyhow::Result<PrivateActorInspection> {
        anyhow::bail!("runtime lacks provider guard inspection")
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider_guard(
        &self,
        _profile: &crate::marketplace::provider::GuardProfile,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks unstarted provider guard recovery")
    }
    #[cfg(unix)]
    async fn stop_provider_guard(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::GuardProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks provider guard stop")
    }
    #[cfg(unix)]
    async fn remove_provider_guard(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::GuardProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks provider guard removal")
    }
    #[cfg(unix)]
    async fn create_provider(
        &self,
        _profile: &crate::marketplace::provider::Profile,
        attempt: crate::marketplace::provider::CreateAttempt,
    ) -> anyhow::Result<provider_creation::ProviderCreation> {
        Ok(provider_creation::ProviderCreation::not_attempted(
            attempt,
            anyhow::anyhow!("runtime lacks private providers"),
        ))
    }
    #[cfg(unix)]
    async fn launch_provider(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
        _attempt: crate::marketplace::provider::StartAttempt,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime lacks private provider startup")
    }
    #[cfg(unix)]
    async fn inspect_provider(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<ContainerState> {
        anyhow::bail!("runtime lacks owned provider inspection")
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider(
        &self,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks exact unstarted provider recovery")
    }
    #[cfg(unix)]
    async fn stop_provider(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned provider stop")
    }
    #[cfg(unix)]
    async fn remove_provider(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::provider::Profile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned provider removal")
    }
    /// Closed create-only gateway boundary. An original synchronized child
    /// intent supplies the single-use attempt; neither this profile nor a CID
    /// is a network/readiness/installed-grant capability.
    #[cfg(unix)]
    async fn create_application_gateway(
        &self,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
        attempt: crate::marketplace::application_egress::GatewayCreateAttempt,
    ) -> anyhow::Result<gateway_creation::GatewayCreation> {
        Ok(gateway_creation::GatewayCreation::not_attempted(
            attempt,
            anyhow::anyhow!("runtime lacks create-only application gateways"),
        ))
    }
    #[cfg(unix)]
    async fn start_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks inspected application gateway startup")
    }
    #[cfg(unix)]
    async fn inspect_unstarted_application_gateway(
        &self,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks exact unstarted application gateway recovery")
    }
    #[cfg(unix)]
    async fn attach_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime lacks private application gateway material streams")
    }
    #[cfg(unix)]
    async fn stop_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned application gateway termination")
    }
    /// Read-only exact-owner kernel context. This is not a connectivity probe,
    /// installed grant or permission to release application HTTP input.
    #[cfg(unix)]
    async fn observe_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        anyhow::bail!("runtime lacks independent application gateway observations")
    }
    /// Fresh complete kernel verification against the original full native
    /// observation. The expectation is configuration, never cached readiness.
    /// Unsupported runtimes fail closed; they must not silently re-bootstrap.
    #[cfg(unix)]
    async fn observe_unchanged_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
        _original: &crate::marketplace::application_egress::ApplicationGatewayObservation,
    ) -> anyhow::Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        anyhow::bail!("runtime lacks unchanged application gateway observations")
    }
    #[cfg(unix)]
    async fn remove_application_gateway(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks stopped-only application gateway removal")
    }
    async fn ensure_packet_uplink(
        &self,
        _spec: &packet_network::PacketNetworkSpec,
    ) -> anyhow::Result<packet_network::PacketNetworkInspection> {
        anyhow::bail!("runtime does not support owned marketplace uplinks")
    }
    async fn inspect_packet_uplink(
        &self,
        _reference: &str,
    ) -> anyhow::Result<Option<packet_network::PacketNetworkInspection>> {
        anyhow::bail!("runtime does not support owned marketplace network inspection")
    }
    async fn remove_packet_uplink(
        &self,
        _spec: &packet_network::PacketNetworkSpec,
        _id: &str,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support owned marketplace network removal")
    }
    async fn create_isolated_packet_actor(
        &self,
        _spec: &ContainerSpec,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime does not support private packet actors")
    }
    async fn inspect_private_actor(
        &self,
        _handle: &ContainerHandle,
    ) -> anyhow::Result<PrivateActorInspection> {
        anyhow::bail!("runtime does not support private actor inspection")
    }
    async fn image_labels(&self, _image: &str) -> anyhow::Result<HashMap<String, String>> {
        anyhow::bail!("runtime does not support image metadata")
    }
    /// Read-only resolution of an already local, trusted platform image.
    /// No pull, container creation or publisher-selected image authorization.
    async fn local_image_id(&self, _reference: &str) -> anyhow::Result<String> {
        anyhow::bail!("runtime does not support local immutable image resolution")
    }
    async fn ensure_network(&self, name: &str) -> anyhow::Result<()>;
    async fn ensure_container(&self, spec: &ContainerSpec) -> anyhow::Result<ContainerHandle>;
    async fn create_host_measurement(
        &self,
        _spec: &measurement::MeasurementSpec,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks create-only read-only host measurement")
    }
    async fn start_host_measurement(
        &self,
        _handle: &ContainerHandle,
        _spec: &measurement::MeasurementSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks inspected immutable host measurement startup")
    }
    async fn confirm_host_measurement_success(
        &self,
        _handle: &ContainerHandle,
        _spec: &measurement::MeasurementSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks native host measurement exit verification")
    }
    async fn stop_host_measurement(&self, _handle: &ContainerHandle) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned host measurement termination")
    }
    /// Stateless foreground helper CREATE ONLY. Persist the original intent
    /// before this call and its immutable identity before the separate start.
    /// No marketplace sharing allowance, VPN health or installed grant is implied.
    async fn create_application_http(
        &self,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
        attempt: crate::marketplace::runtime::application_http::HttpCreateAttempt,
    ) -> anyhow::Result<http_creation::HttpCreation> {
        Ok(http_creation::HttpCreation::not_attempted(
            attempt,
            anyhow::anyhow!("runtime lacks create-only application HTTP"),
        ))
    }
    async fn start_application_http(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks inspected application HTTP startup")
    }
    /// Recovery DATA only: observe the original name's exact never-started
    /// profile, returning its CID without starting/removing anything. Absence
    /// of a name does not prove that an uncertain CREATE has completed.
    async fn inspect_unstarted_application_http(
        &self,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks inspected unstarted application HTTP recovery")
    }
    async fn attach_application_http(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime lacks private application HTTP streams")
    }
    /// Natural native exit only, not an acknowledgement of HTTP-owner or
    /// source-invocation retirement; those original owners must still be joined.
    async fn confirm_application_http_success(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks application HTTP native exit verification")
    }
    async fn stop_application_http(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned application HTTP termination")
    }
    /// Remove only a positively stopped owned ID; confirm absence independently.
    async fn remove_application_http(
        &self,
        _handle: &ContainerHandle,
        _profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks owned application HTTP removal")
    }
    /// Offline source writer CREATE ONLY. The controller must durably bind its
    /// immutable identity before calling the separate inspected start boundary.
    async fn create_quarantine_marketplace_writer(
        &self,
        _spec: &ContainerSpec,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime does not support create-only native source writers")
    }
    /// Cleanup-only observation of the original creation intent's never-started
    /// actor. A missing name is NOT successful retirement of an uncertain create.
    async fn inspect_unstarted_quarantine_marketplace_writer(
        &self,
        _intent: &crate::marketplace::runtime::source_writer::SourceWriterIntent,
    ) -> anyhow::Result<ContainerHandle> {
        anyhow::bail!("runtime lacks original unstarted source creation inspection")
    }
    async fn start_quarantine_marketplace_writer(
        &self,
        _handle: &ContainerHandle,
        _spec: &ContainerSpec,
        _limits: &crate::marketplace::runtime::SourceJobLimits,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support inspected immutable native source starts")
    }
    async fn create_private_file_volume(
        &self,
        _spec: &PrivateFileVolumeSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support private file volumes")
    }
    async fn ensure_owned_directory_volume(
        &self,
        _spec: &OwnedDirectoryVolumeSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support owned directory volumes")
    }
    async fn ensure_fixed_marketplace_volume(
        &self,
        _spec: &FixedMarketplaceVolumeSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support guarded marketplace storage")
    }
    async fn ensure_quarantine_marketplace_volume(
        &self,
        _spec: &QuarantineMarketplaceVolumeSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support guarded marketplace source quarantine")
    }
    /// Create ONE empty, exclusively claimed local build-output volume. No
    /// helper/container is launched; initialization uses the durably bound
    /// source-writer path. Existing volumes must never be adopted or reset.
    async fn create_marketplace_build_output_volume(&self, _slot: &str) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks guarded build output allocation")
    }
    /// Exactly owned deterministic slot only. No running actor is stopped by
    /// this storage cleanup; its original source-job owner must join it first.
    async fn remove_marketplace_build_output_volume(&self, _slot: &str) -> anyhow::Result<()> {
        anyhow::bail!("runtime lacks confirmed build output removal")
    }
    /// Opportunistic idle-slot cleanup for legacy runtimes which cannot create
    /// builds. NOT an acknowledgement for any actual/recorded build operation.
    async fn cleanup_idle_marketplace_build_output(&self, _slot: &str) -> anyhow::Result<()> {
        Ok(())
    }
    /// Inspect a local cache's backend and ownership before returning its
    /// optional bounded identity label. Metadata grants no native authority.
    async fn source_cache_state(
        &self,
        _name: &str,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<model::SourceCacheState> {
        Ok(model::SourceCacheState::Unsupported)
    }
    async fn attach_private_stdio(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support private process streams")
    }
    /// Natural success of this immutable owned native job, BEFORE stop/cleanup.
    /// Attachment EOF/CLI exit and a forced stop are not process-success proof.
    async fn confirm_quarantine_marketplace_success(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support native marketplace exit verification")
    }
    /// Fixed-purpose independent UID-10005 process, not arbitrary worker exec.
    async fn exec_marketplace_connectivity_probe(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace connectivity probes")
    }
    /// Closed source-text helper, separate from the untrusted marketplace worker.
    async fn exec_marketplace_source_preflight(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace source preflight")
    }
    async fn exec_marketplace_radicle_source(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support native-owned Radicle source verification")
    }
    /// Fixed private UID-10005 SDK HTTP helper. This launches no publisher code
    /// and supplies no installed-grant authority; the held controller must
    /// independently authorize each request and retain its retirement owner.
    async fn exec_marketplace_sdk_http(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace SDK HTTP")
    }
    /// Fixed duplex-stream helper. The caller must authorize the original
    /// request, bound every frame and join the native child's actual retirement.
    /// This alone supplies no installed grant or application-data VPN owner.
    async fn exec_marketplace_sdk_http_stream(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace SDK HTTP streaming")
    }
    /// Fixed native descriptor verifier, never a command supplied by a peer.
    /// Keys must come from the controller's fixed worker configuration.
    async fn exec_marketplace_metadata_verifier(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
        _issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace metadata verification")
    }
    async fn exec_marketplace_security_verifier(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
        _issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace security verification")
    }
    async fn exec_marketplace_evidence_verifier(
        &self,
        _handle: &ContainerHandle,
        _required_labels: &HashMap<String, String>,
        _issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> anyhow::Result<PrivateProcessStdio> {
        anyhow::bail!("runtime does not support independent marketplace evidence verification")
    }
    async fn private_file_volume_owned(
        &self,
        _name: &str,
        _required_labels: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<bool> {
        anyhow::bail!("runtime does not support private file volumes")
    }
    async fn remove_private_file_volume(
        &self,
        _name: &str,
        _required_labels: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support private file volumes")
    }
    async fn get_container_handle(&self, name: &str) -> anyhow::Result<Option<ContainerHandle>>;
    async fn start_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    async fn exec_container_command(
        &self,
        _handle: &ContainerHandle,
        _user: &str,
        _command: &[String],
    ) -> anyhow::Result<String> {
        anyhow::bail!("runtime does not support container command execution")
    }
    async fn stop_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    /// Immediate stop of a controller-owned offline quarantine writer only.
    /// Implementations must verify its immutable identity and native role;
    /// this is not graceful application shutdown or permission to delete data.
    async fn stop_quarantine_marketplace_writer(
        &self,
        _handle: &ContainerHandle,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support immediate owned quarantine stop")
    }
    async fn rename_container(
        &self,
        handle: &ContainerHandle,
        new_name: &str,
    ) -> anyhow::Result<ContainerHandle>;
    async fn remove_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    async fn container_logs(
        &self,
        handle: &ContainerHandle,
        since: Option<DateTime<Utc>>,
    ) -> anyhow::Result<String>;
    async fn inspect(&self, handle: &ContainerHandle) -> anyhow::Result<ContainerState>;
    async fn describe_container_runtime_state(
        &self,
        _container_name: &str,
    ) -> anyhow::Result<Option<ContainerRuntimeState>> {
        Ok(None)
    }
    async fn read_container_file(
        &self,
        handle: &ContainerHandle,
        path: &str,
    ) -> anyhow::Result<Option<Vec<u8>>>;
    async fn copy_host_path_to_container(
        &self,
        handle: &ContainerHandle,
        source_path: &Path,
        destination_path: &str,
    ) -> anyhow::Result<()>;
    async fn ensure_container_directories(
        &self,
        handle: &ContainerHandle,
        paths: &[String],
    ) -> anyhow::Result<()>;
    async fn ensure_container_directories_owned_like(
        &self,
        handle: &ContainerHandle,
        reference_path: &str,
        paths: &[String],
    ) -> anyhow::Result<bool>;
}
