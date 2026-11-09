//! Bounded private process RPC. Responses remain untrusted signed metadata;
//! strict framing/schema checks are not cryptographic or installation approval.
use std::time::Duration;

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    MAX_CONTROL_FRAME_BYTES, canonical,
    evidence::EvidenceBatch,
    identity::ExtensionId,
    peer::{PeerOperation, PeerOutcome, PeerRequest, PeerResponse},
    worker_blob::BlobReadState,
    worker_control::{
        WorkerControlOperation, WorkerControlOutcome, WorkerControlRequest, WorkerControlResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time::timeout,
};

use crate::runtime::PrivateProcessStdio;

const PRIVATE_DEADLINE: Duration = Duration::from_secs(10);
const NETWORK_START_DEADLINE: Duration = Duration::from_secs(45);

fn control_deadline(base: Duration, operation: &WorkerControlOperation) -> Duration {
    // Preserve deliberately shorter diagnostic/test deadlines. Starting a WAN
    // listener includes rendezvous, not just a local bind. No other command
    // inherits the longer startup allowance, even after cancellation/failure.
    if base >= PRIVATE_DEADLINE && matches!(operation, WorkerControlOperation::NetworkStart { .. })
    {
        base.max(NETWORK_START_DEADLINE)
    } else {
        base
    }
}

pub struct PrivateWorkerClient {
    process: Option<PrivateProcessStdio>,
    diagnostics: tokio::task::JoinHandle<()>,
    diagnostic_overflow: mpsc::Receiver<()>,
    prefix: u64,
    sequence: u32,
    poisoned: bool,
    deadline: Duration,
}

impl PrivateWorkerClient {
    pub(crate) fn new(process: PrivateProcessStdio) -> Result<Self> {
        Self::with_deadline(process, PRIVATE_DEADLINE)
    }

    fn with_deadline(mut process: PrivateProcessStdio, deadline: Duration) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        // Remove diagnostics from the process without exposing any peer text
        // as logs. The helper drains at most 64 KiB, not an infinite stream.
        let mut diagnostic = process
            .diagnostics
            .take()
            .context("missing private diagnostics")?;
        let diagnostics = tokio::spawn(async move {
            let mut buffer = [0u8; 1024];
            let mut bytes = 0;
            while bytes <= 64 * 1024 {
                let remaining = (64 * 1024 + 1 - bytes).min(buffer.len());
                match diagnostic.read(&mut buffer[..remaining]).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => bytes += read,
                }
            }
            let _ = sender.send(()).await;
        });
        let random = uuid::Uuid::new_v4();
        let prefix =
            u64::from(u32::from_be_bytes(random.as_bytes()[..4].try_into().unwrap()) & 0x7fff_ffff)
                << 20;
        Ok(Self {
            process: Some(process),
            diagnostics,
            diagnostic_overflow: receiver,
            prefix,
            sequence: 0,
            poisoned: false,
            deadline,
        })
    }

    /// `&mut self` permits one in-flight request and no hidden unbounded queue.
    /// The service must bound queue admission and hold one controller lease.
    fn next_id(&mut self) -> Result<u64> {
        if self.poisoned || self.sequence >= 0x000f_ffff {
            bail!("private worker channel is unavailable");
        }
        self.sequence += 1;
        Ok(self.prefix | u64::from(self.sequence))
    }

    pub async fn request(&mut self, operation: PeerOperation) -> Result<PeerResponse> {
        let id = self.next_id()?;
        let request = PeerRequest {
            schema_version: 1,
            request_id: id,
            request: operation,
        };
        request.validate()?;
        let encoded = canonical::to_vec(&request)?;
        #[cfg(test)]
        let timing = matches!(&request.request, PeerOperation::Search { .. })
            .then(|| {
                super::search_timing::Rpc::begin(
                    id,
                    super::search_timing::RpcKind::LocalSearch,
                    encoded.len(),
                )
            })
            .flatten();
        self.exchange(
            &encoded,
            MAX_CONTROL_FRAME_BYTES,
            self.deadline,
            #[cfg(test)]
            timing,
            |bytes| {
                let response = PeerResponse::parse(bytes)?;
                if response.request_id != Some(id) {
                    bail!("private response correlation mismatch");
                }
                if !peer_kind_matches(&request.request, &response.response) {
                    bail!("private response operation differs from request");
                }
                Ok(response)
            },
        )
        .await
    }

    /// Controller-scheduled cache work only. The caller separately admits the
    /// evidence pass and reserves transfer/work budgets. Never expose this
    /// command through the peer dispatcher or treat its receipt as popularity.
    pub async fn merge_evidence(
        &mut self,
        extension_id: ExtensionId,
        batch: EvidenceBatch,
    ) -> Result<WorkerControlResponse> {
        self.control(WorkerControlOperation::MergeEvidence {
            extension_id,
            batch,
        })
        .await
    }

    /// Private controller operations only. ProtectedWorker is the production
    /// caller for network commands; OfflineWorker exposes no such capability.
    pub(super) async fn control(
        &mut self,
        operation: WorkerControlOperation,
    ) -> Result<WorkerControlResponse> {
        let id = self.next_id()?;
        let request = WorkerControlRequest {
            schema_version: 1,
            request_id: id,
            control: operation,
        };
        request.validate()?;
        let encoded = canonical::to_vec(&request)?;
        let reply_limit = request.control.reply_limit();
        let deadline = control_deadline(self.deadline, &request.control);
        #[cfg(test)]
        let timing = matches!(
            &request.control,
            WorkerControlOperation::CatalogHeads { .. }
        )
        .then(|| {
            super::search_timing::Rpc::begin(
                id,
                super::search_timing::RpcKind::CatalogHeads,
                encoded.len(),
            )
        })
        .flatten();
        self.exchange(
            &encoded,
            reply_limit,
            deadline,
            #[cfg(test)]
            timing,
            |bytes| {
                let response = WorkerControlResponse::parse(bytes)?;
                if response.request_id != id
                    || !control_kind_matches(&request.control, &response.control_response)
                {
                    bail!("private receipt correlation, operation or work differs");
                }
                Ok(response)
            },
        )
        .await
    }

    async fn exchange<T>(
        &mut self,
        encoded: &[u8],
        reply_limit: usize,
        deadline: Duration,
        #[cfg(test)] mut timing: Option<super::search_timing::Rpc>,
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<T> {
        if self.poisoned {
            bail!("private worker channel is unavailable");
        }
        if encoded.is_empty() || encoded.len() > MAX_CONTROL_FRAME_BYTES {
            bail!("private worker request exceeds frame budget");
        }
        // Cancellation after any I/O leaves an ambiguous stream. Keep it
        // poisoned and kill the attachment unless a full correlated reply wins.
        self.poisoned = true;
        let process = self
            .process
            .as_mut()
            .context("private worker attachment is closed")?;
        let mut cancellation = KillOnCancellation {
            child: &mut process.child,
            armed: true,
        };
        let operation = async {
            process
                .input
                .write_all(&(encoded.len() as u32).to_be_bytes())
                .await?;
            process.input.write_all(encoded).await?;
            process.input.flush().await?;
            #[cfg(test)]
            if let Some(timing) = &mut timing {
                timing.written();
            }
            let length = process.output.read_u32().await? as usize;
            #[cfg(test)]
            if let Some(timing) = &mut timing {
                timing.header(length);
            }
            if length == 0 || length > reply_limit {
                bail!("invalid private response length");
            }
            let mut bytes = vec![0; length];
            process.output.read_exact(&mut bytes).await?;
            #[cfg(test)]
            if let Some(timing) = &mut timing {
                timing.body();
            }
            // Decode and correlate before unpoisoning. A syntactically valid
            // reply of the wrong private/public kind is still terminal.
            let result = decode(&bytes);
            #[cfg(test)]
            if let Some(timing) = &mut timing {
                timing.decoded();
            }
            result
        };
        let outcome = tokio::select! {
            result=timeout(deadline,operation)=>result.context("private worker deadline exceeded").and_then(|result|result),
            Some(())=self.diagnostic_overflow.recv()=>Err(anyhow::anyhow!("private worker diagnostic limit exceeded")),
        };
        #[cfg(test)]
        if let Some(timing) = &mut timing {
            timing.finished(outcome.is_ok());
        }
        if outcome.is_ok() {
            self.poisoned = false;
            cancellation.armed = false;
        }
        outcome
    }

    /// The supervisor stops its validated container separately. Closing this
    /// handle does not delete persisted catalog or installed security state.
    pub async fn close(mut self) {
        let _ = self.close_attachment().await;
        self.diagnostics.abort();
    }

    async fn close_attachment(&mut self) -> Result<std::process::ExitStatus> {
        self.poisoned = true;
        let mut process = self
            .process
            .take()
            .context("private worker attachment is closed")?;
        // Tokio's Unix ChildStdin::shutdown is a no-op: holding that writer
        // while waiting prevents the worker from ever observing EOF. Drop the
        // ORIGINAL descriptor first; never create a replacement pipe/child.
        drop(process.input);
        let mut cancellation = KillOnCancellation {
            child: &mut process.child,
            armed: true,
        };
        let status = match timeout(Duration::from_secs(2), cancellation.child.wait()).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = cancellation.child.start_kill();
                timeout(Duration::from_secs(2), cancellation.child.wait()).await??
            }
        };
        cancellation.armed = false;
        self.diagnostics.abort();
        Ok(status)
    }
}

fn peer_kind_matches(request: &PeerOperation, response: &PeerOutcome) -> bool {
    matches!(
        (request, response),
        (_, PeerOutcome::Rejected { .. })
            | (PeerOperation::Changes { .. }, PeerOutcome::Changes { .. })
            | (PeerOperation::Search { .. }, PeerOutcome::Search { .. })
            | (
                PeerOperation::Inventory { .. },
                PeerOutcome::Inventory { .. }
            )
            | (PeerOperation::Offer { .. }, PeerOutcome::Offered { .. })
            | (PeerOperation::GetHead { .. }, PeerOutcome::Head { .. })
            | (
                PeerOperation::GetEvidence { .. },
                PeerOutcome::Evidence { .. }
            )
    )
}
fn control_kind_matches(request: &WorkerControlOperation, response: &WorkerControlOutcome) -> bool {
    match (request, response) {
        (_, WorkerControlOutcome::Rejected { .. }) => true,
        (
            WorkerControlOperation::CatalogHeads { extension_ids },
            WorkerControlOutcome::CatalogHeads { rows },
        ) => {
            !rows.is_empty()
                && rows.len() <= extension_ids.len()
                && rows
                    .iter()
                    .zip(extension_ids)
                    .all(|(row, id)| &row.extension_id == id)
        }
        (
            WorkerControlOperation::SharingSet {
                inventory,
                resolve_installed,
                ..
            },
            WorkerControlOutcome::Sharing { state },
        ) => {
            use elixir_marketplace::worker_sharing::SharingState;
            state.validate().is_ok()
                && match state {
                    SharingState::Ready {
                        generation: Some(generation),
                        blobs,
                        ..
                    } => *generation == inventory.generation && *blobs == inventory.blobs.len(),
                    SharingState::Ready {
                        generation: None,
                        blobs: 0,
                        ..
                    } => inventory.blobs.is_empty() && !resolve_installed,
                    SharingState::Resolve { generation, .. } => {
                        *generation == inventory.generation && *resolve_installed
                    }
                    SharingState::Read {
                        generation, blob, ..
                    } => {
                        *generation == inventory.generation
                            && (*resolve_installed
                                || inventory
                                    .blobs
                                    .iter()
                                    .any(|b| b.sha256 == blob.sha256 && b.size == blob.size))
                    }
                    _ => false,
                }
        }
        (WorkerControlOperation::SharingPoll, WorkerControlOutcome::Sharing { state }) => {
            state.validate().is_ok()
                && !matches!(
                    state,
                    elixir_marketplace::worker_sharing::SharingState::Accepted { .. }
                )
        }
        (
            WorkerControlOperation::SharingAuthorize { request, .. },
            WorkerControlOutcome::Sharing {
                state:
                    elixir_marketplace::worker_sharing::SharingState::Accepted {
                        request: actual,
                        offset: 0,
                    },
            },
        ) => request == actual,
        (
            WorkerControlOperation::SharingChunk {
                request, offset, ..
            },
            WorkerControlOutcome::Sharing {
                state:
                    elixir_marketplace::worker_sharing::SharingState::Accepted {
                        request: actual,
                        offset: observed,
                    },
            },
        ) => request == actual && offset == observed,
        (
            WorkerControlOperation::BlobProviders { job, sha256 },
            WorkerControlOutcome::BlobProviders {
                job: actual,
                sha256: observed,
                peers,
            },
        ) => {
            job == actual
                && sha256 == observed
                && elixir_marketplace::worker_blob::validate_provider_hints(peers).is_ok()
        }
        (
            WorkerControlOperation::BlobStart { job, .. },
            WorkerControlOutcome::BlobTransfer {
                job: actual,
                state: BlobReadState::Started,
            },
        ) => job == actual,
        (
            WorkerControlOperation::BlobCancel { job },
            WorkerControlOutcome::BlobTransfer {
                job: actual,
                state: BlobReadState::Cancelled,
            },
        ) => job == actual,
        (
            WorkerControlOperation::BlobRead { job, offset },
            WorkerControlOutcome::BlobTransfer { job: actual, state },
        ) => {
            job == actual
                && match state {
                    BlobReadState::Data {
                        offset: observed, ..
                    }
                    | BlobReadState::Pending { offset: observed }
                    | BlobReadState::Failed { offset: observed } => offset == observed,
                    BlobReadState::Complete { blob, .. } => *offset == blob.size,
                    _ => false,
                }
        }
        (
            WorkerControlOperation::CatalogSecurityBegin {
                generation,
                revision,
                ..
            },
            WorkerControlOutcome::CatalogSecurity {
                generation: actual,
                revision: observed,
                ready,
            },
        ) => generation == actual && revision == observed && !ready,
        (
            WorkerControlOperation::CatalogSecurityApply { generation, batch },
            WorkerControlOutcome::CatalogSecurity {
                generation: actual,
                revision,
                ..
            },
        ) => generation == actual && batch.revision == *revision,
        (
            WorkerControlOperation::CatalogSecurityFinish {
                generation,
                revision,
                ..
            },
            WorkerControlOutcome::CatalogSecurity {
                generation: actual,
                revision: observed,
                ready,
            },
        ) => generation == actual && revision == observed && *ready,
        (WorkerControlOperation::CatalogJobStatus, WorkerControlOutcome::CatalogJob { .. }) => true,
        (WorkerControlOperation::CatalogMaintain, WorkerControlOutcome::CatalogJob { state }) => {
            state.kind == Some(elixir_marketplace::worker_control::CatalogJobKind::Maintenance)
                && state.phase == elixir_marketplace::worker_control::CatalogJobPhase::Running
        }
        (
            WorkerControlOperation::CatalogPopulate { peer, .. }
            | WorkerControlOperation::CatalogSync { peer, .. },
            WorkerControlOutcome::CatalogJob { state },
        ) => {
            state.kind == Some(elixir_marketplace::worker_control::CatalogJobKind::Population)
                && state.peer.as_ref() == Some(peer)
                && state.phase == elixir_marketplace::worker_control::CatalogJobPhase::Running
        }
        (
            WorkerControlOperation::MergeEvidence { batch, .. },
            WorkerControlOutcome::EvidenceMerged { accepted },
        ) => *accepted <= batch.pairs.len(),
        (
            WorkerControlOperation::NetworkStart { config },
            WorkerControlOutcome::NetworkState { status },
        ) => {
            status.active
                && status.roles() == config.roles
                && status.within_limits(&config.limits).is_ok()
        }
        (WorkerControlOperation::NetworkStop, WorkerControlOutcome::NetworkState { status }) => {
            !status.active
        }
        (WorkerControlOperation::NetworkStatus, WorkerControlOutcome::NetworkState { .. }) => true,
        (WorkerControlOperation::NetworkRefresh, WorkerControlOutcome::NetworkState { status }) => {
            status.active
        }
        (
            WorkerControlOperation::NetworkRequest { peer, request },
            WorkerControlOutcome::NetworkReply {
                peer: actual,
                response,
                ..
            },
        ) => peer == actual && peer_kind_matches(request, &response.response),
        _ => false,
    }
}

struct KillOnCancellation<'a> {
    child: &'a mut tokio::process::Child,
    armed: bool,
}
impl Drop for KillOnCancellation<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.child.start_kill();
        }
    }
}

impl Drop for PrivateWorkerClient {
    fn drop(&mut self) {
        self.diagnostics.abort();
        if let Some(process) = &mut self.process {
            let _ = process.child.start_kill();
        }
    }
}

#[cfg(test)]
#[path = "client/native_timing.rs"]
mod native_timing;

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::{catalog::SearchQuery, worker_network::PeerKey};
    use std::process::Stdio;

    #[test]
    fn head_batch_receipts_bind_a_nonempty_exact_prefix_not_just_a_count() -> Result<()> {
        let vector: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let root = vector["release"]["payload"]["extension_id"]
            .as_str()
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        let ids = (0..3)
            .map(|n| format!("{root}/batch-{n}").parse())
            .collect::<elixir_marketplace::Result<Vec<elixir_marketplace::identity::ExtensionId>>>(
            )?;
        let operation = WorkerControlOperation::CatalogHeads {
            extension_ids: ids.clone(),
        };
        for (positions, allowed) in [
            (vec![0], true),
            (vec![0, 1, 2], true),
            (vec![], false),
            (vec![1], false),
            (vec![0, 2], false),
            (vec![0, 1, 2, 0], false),
        ] {
            let rows = positions
                .into_iter()
                .map(
                    |n| elixir_marketplace::worker_control::CatalogHeadReadback {
                        extension_id: ids[n].clone(),
                        response: PeerResponse {
                            schema_version: 1,
                            request_id: Some(1),
                            response: PeerOutcome::Rejected {
                                code: elixir_marketplace::peer::RejectionCode::Invalid,
                            },
                        },
                    },
                )
                .collect();
            assert_eq!(
                control_kind_matches(&operation, &WorkerControlOutcome::CatalogHeads { rows }),
                allowed
            );
        }
        Ok(())
    }
    #[test]
    fn sharing_receipts_bind_lease_scope_phase_request_and_exact_offset() -> Result<()> {
        use elixir_marketplace::{
            identity::Digest,
            records::Blob,
            worker_sharing::{SharingInventory, SharingState},
        };
        let generation = Digest::of(b"lease");
        let request = Digest::of(b"source request");
        let blob = Blob {
            sha256: Digest::of(b"installed archive"),
            size: 128,
        };
        let sha256 = blob.sha256;
        let inventory = SharingInventory {
            generation,
            blobs: vec![blob.clone()],
        };
        let operation = |resolve_installed| WorkerControlOperation::SharingSet {
            inventory: inventory.clone(),
            lease_ms: 15_000,
            resolve_installed,
        };
        let outcome = |state| WorkerControlOutcome::Sharing { state };
        assert!(control_kind_matches(
            &operation(true),
            &outcome(SharingState::Ready {
                generation: Some(generation),
                blobs: 1,
                serving: false
            })
        ));
        assert!(!control_kind_matches(
            &operation(true),
            &outcome(SharingState::Ready {
                generation: Some(request),
                blobs: 1,
                serving: false
            })
        ));
        assert!(!control_kind_matches(
            &operation(true),
            &outcome(SharingState::Ready {
                generation: Some(generation),
                blobs: 2,
                serving: false
            })
        ));
        let resolve = || {
            outcome(SharingState::Resolve {
                generation,
                request,
                sha256,
            })
        };
        assert!(control_kind_matches(&operation(true), &resolve()));
        assert!(!control_kind_matches(&operation(false), &resolve()));
        assert!(control_kind_matches(
            &WorkerControlOperation::SharingPoll,
            &resolve()
        ));
        let read = || {
            outcome(SharingState::Read {
                generation,
                request,
                blob: Blob {
                    sha256: request,
                    size: 128,
                },
                offset: 0,
                maximum: 128,
            })
        };
        assert!(control_kind_matches(&operation(true), &read()));
        assert!(!control_kind_matches(&operation(false), &read()));
        let ack = |request, offset| outcome(SharingState::Accepted { request, offset });
        let authorize = WorkerControlOperation::SharingAuthorize {
            generation,
            request,
            blob: Some(blob),
        };
        assert!(control_kind_matches(&authorize, &ack(request, 0)));
        assert!(!control_kind_matches(&authorize, &ack(generation, 0)));
        assert!(!control_kind_matches(&authorize, &ack(request, 1)));
        let chunk = WorkerControlOperation::SharingChunk {
            generation,
            request,
            offset: 64,
            data: None,
        };
        assert!(control_kind_matches(&chunk, &ack(request, 64)));
        assert!(!control_kind_matches(&chunk, &ack(request, 0)));
        assert!(!control_kind_matches(
            &WorkerControlOperation::SharingPoll,
            &ack(request, 0)
        ));
        assert!(!control_kind_matches(
            &WorkerControlOperation::NetworkStatus,
            &resolve()
        ));
        Ok(())
    }

    #[test]
    fn provider_receipts_bind_nonce_digest_and_bounded_distinct_transport_identities() -> Result<()>
    {
        use elixir_marketplace::identity::Digest;
        let job = Digest::of(b"lookup nonce");
        let sha256 = Digest::of(b"signed blob");
        let operation = WorkerControlOperation::BlobProviders { job, sha256 };
        let receipt =
            |job, sha256, peers| WorkerControlOutcome::BlobProviders { job, sha256, peers };
        let peer = PeerKey::try_from("1".repeat(64))?;
        assert!(control_kind_matches(
            &operation,
            &receipt(job, sha256, vec![])
        ));
        assert!(control_kind_matches(
            &operation,
            &receipt(job, sha256, vec![peer.clone()])
        ));
        assert!(!control_kind_matches(
            &operation,
            &receipt(Digest::of(b"other nonce"), sha256, vec![])
        ));
        assert!(!control_kind_matches(
            &operation,
            &receipt(job, Digest::of(b"other blob"), vec![])
        ));
        assert!(!control_kind_matches(
            &operation,
            &receipt(job, sha256, vec![peer.clone(), peer])
        ));
        let peers = (1..=7)
            .map(|n| PeerKey::try_from(format!("{n:064x}")))
            .collect::<elixir_marketplace::Result<Vec<_>>>()?;
        assert!(!control_kind_matches(
            &operation,
            &receipt(job, sha256, peers)
        ));
        assert!(!control_kind_matches(
            &WorkerControlOperation::NetworkStatus,
            &receipt(job, sha256, vec![])
        ));
        Ok(())
    }

    fn query() -> PeerOperation {
        PeerOperation::Search {
            query: SearchQuery {
                text: String::new(),
                tags: vec![],
                limit: 1,
            },
            cursor: None,
        }
    }

    fn fake(mode: &str, deadline: Duration) -> Result<PrivateWorkerClient> {
        let script = r#"
            const mode=process.argv[1]; let input=Buffer.alloc(0);
            process.stdin.on('data',chunk=>{
              input=Buffer.concat([input,chunk]);
              if(input.length<4||input.length<4+input.readUInt32BE(0)) return;
              const length=input.readUInt32BE(0),request=JSON.parse(input.subarray(4,4+length)); input=input.subarray(4+length);
              if(mode==='stall') return;
              if(mode==='diagnostics'){ process.stderr.write(Buffer.alloc(70000,120)); return; }
              if(mode==='partial'){ process.stdout.write(Buffer.from([0,0])); return; }
              if(mode==='oversize'){ const header=Buffer.alloc(4); header.writeUInt32BE(262145); process.stdout.write(header); return; }
              if(mode==='control_frame_excess'){ const header=Buffer.alloc(4); header.writeUInt32BE(4097); process.stdout.write(header); return; }
              if(mode==='blob_frame_excess'){ const header=Buffer.alloc(4); header.writeUInt32BE(96*1024+1); process.stdout.write(header); return; }
              let response={schema_version:1,request_id:request.request_id,response:{type:'rejected',code:'invalid'}};
              if(request.control) {
                const operation=request.control.operation;
                const active=['network_start','network_refresh'].includes(operation);
                const roles=request.control.config?.roles ?? {consumer:true,catalog:false,routing:false};
                const status={active,consumer:active&&roles.consumer,catalog:active&&roles.catalog,routing:active&&roles.routing,
                  peers:[],pending_dials:0,pending_handshakes:0,candidates:0,lookups:0,topics:active?1:0,adaptive:false};
                let outcome=operation==='merge_evidence'?{type:'evidence_merged',accepted:request.control.batch.pairs.length}:{type:'network_state',status};
                if(operation==='network_request')outcome={type:'network_reply',peer:request.control.peer,control_bytes:512,
                  response:{schema_version:1,request_id:1,response:{type:'rejected',code:'unauthorized'}}};
                if(operation.startsWith('catalog_security_'))outcome={type:'catalog_security',generation:request.control.generation,
                  revision:operation==='catalog_security_apply'?request.control.batch.revision:request.control.revision,
                  ready:operation==='catalog_security_finish'};
                else if(operation.startsWith('catalog_'))outcome={type:'catalog_job',state:{
                  phase:operation==='catalog_job_status'?'idle':'running',kind:['catalog_populate','catalog_sync'].includes(operation)?'population':operation==='catalog_maintain'?'maintenance':null,
                  peer:['catalog_populate','catalog_sync'].includes(operation)?request.control.peer:null,admitted:0,unchanged:0,rejected:0,indexed:0,removed:0}};
                if(operation.startsWith('blob_') || mode==='blob_bogus_status') {
                  const blob={sha256:'sha256:'+require('node:crypto').createHash('sha256').update(Buffer.alloc(65536,7)).digest('hex'),size:65536};
                  const state=operation==='blob_start'?{phase:'started'}:operation==='blob_cancel'?{phase:'cancelled'}:
                    request.control.offset===65536?{phase:'complete',blob,integrity_only:true}:
                    {phase:'data',offset:request.control.offset??0,data:Buffer.alloc(65536,7).toString('base64url')};
                  outcome={type:'blob_transfer',job:request.control.job??'sha256:'+'1'.repeat(64),state};
                  if(mode==='blob_wrong_job')outcome.job='sha256:'+'f'.repeat(64);
                  if(mode==='blob_wrong_offset')state.offset++;
                  if(mode==='blob_bad_encoding')state.data='Bw==';
                  if(mode==='blob_wrong_phase')outcome.state={phase:'started'};
                  if(mode==='blob_false_integrity')state.integrity_only=false;
                }
                if(mode==='catalog_wrong_peer')outcome.state.peer='3'.repeat(64);
                if(mode==='catalog_wrong_kind'){outcome.state.kind='maintenance';outcome.state.peer=null;}
                if(mode==='catalog_late_ack')outcome.state.phase='complete';
                if(mode==='catalog_excess')outcome.state.admitted=21;
                if(mode==='catalog_security_wrong_generation')outcome.generation='sha256:'+'f'.repeat(64);
                if(mode==='catalog_security_wrong_revision')outcome.revision++;
                if(mode==='catalog_security_premature_ready')outcome.ready=true;
                if(mode==='catalog_security_unfinished')outcome.ready=false;
                response={schema_version:1,request_id:request.request_id,control_response:outcome};
                if(mode==='control_excess')response.control_response.accepted++;
                if(mode==='control_reject')response.control_response={type:'rejected',code:'unauthorized'};
                if(mode==='network_adaptive')status.adaptive=true;
                if(mode==='network_flood')status.pending_handshakes=5;
                if(mode==='network_physical_counts'){status.classifying_peers=1;status.blob_peers=1;}
                if(mode==='network_physical_flood'){status.classifying_peers=4;status.blob_peers=5;}
                if(mode==='network_blob_overflow')status.blob_peers=Number.MAX_SAFE_INTEGER;
                if(mode==='network_physical_false_off')status.blob_peers=1;
                if(mode==='network_wrong_roles')status.routing=!status.routing;
                if(mode==='network_false_off'){status.active=true;status.consumer=true;status.topics=1;}
                if(mode==='network_wrong_peer')outcome.peer='3'.repeat(64);
                if(mode==='network_wrong_kind')outcome.response.response={type:'offered',release_id:'sha256:'+'0'.repeat(64)};
              }
              if(mode==='wrong_id') response.request_id++;
              if(mode==='null_id') response.request_id=null;
              if(mode==='wrong_kind') {delete response.control_response;response.response={type:'offered',release_id:'sha256:'+'0'.repeat(64)};}
              let body=JSON.stringify(response);
              if(mode==='duplicate') body=body.replace('{','{"schema_version":1,');
              const bytes=Buffer.from(body),header=Buffer.alloc(4); header.writeUInt32BE(bytes.length);
              const frame=Buffer.concat([header,bytes]);
              for(let i=0;i<frame.length;i+=3) process.stdout.write(frame.subarray(i,i+3));
            });
            setInterval(()=>{},1000);
        "#;
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, mode])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        PrivateWorkerClient::with_deadline(
            PrivateProcessStdio {
                input: child.stdin.take().unwrap(),
                output: child.stdout.take().unwrap(),
                diagnostics: child.stderr.take(),
                child,
            },
            deadline,
        )
    }

    #[tokio::test]
    async fn private_catalog_job_acknowledgements_bind_operation_peer_phase_and_request()
    -> Result<()> {
        use elixir_marketplace::worker_control::{CATALOG_CONTROL_BUDGET, CatalogJobPhase};
        for operation in [
            WorkerControlOperation::CatalogJobStatus,
            WorkerControlOperation::CatalogMaintain,
            WorkerControlOperation::CatalogPopulate {
                peer: PeerKey::try_from("1".repeat(64))?,
                max_control_bytes: CATALOG_CONTROL_BUDGET,
            },
            WorkerControlOperation::CatalogSync {
                peer: PeerKey::try_from("1".repeat(64))?,
                max_control_bytes: CATALOG_CONTROL_BUDGET,
            },
        ] {
            let mut client = fake("valid", Duration::from_secs(3))?;
            let result = client.control(operation).await?;
            assert!(matches!(
                result.control_response,
                WorkerControlOutcome::CatalogJob { .. }
            ));
            client.close().await;
        }
        for mode in [
            "catalog_wrong_peer",
            "catalog_wrong_kind",
            "catalog_late_ack",
            "catalog_excess",
            "wrong_id",
            "duplicate",
        ] {
            for incremental in [false, true] {
                let mut client = fake(mode, Duration::from_secs(3))?;
                let peer = PeerKey::try_from("1".repeat(64))?;
                let operation = if incremental {
                    WorkerControlOperation::CatalogSync {
                        peer,
                        max_control_bytes: CATALOG_CONTROL_BUDGET,
                    }
                } else {
                    WorkerControlOperation::CatalogPopulate {
                        peer,
                        max_control_bytes: CATALOG_CONTROL_BUDGET,
                    }
                };
                assert!(client.control(operation).await.is_err(), "{mode}");
                assert!(client.poisoned, "{mode}");
                client.close().await;
            }
        }
        let mut client = fake("control_reject", Duration::from_secs(3))?;
        let result = client
            .control(WorkerControlOperation::CatalogMaintain)
            .await?;
        assert!(matches!(
            result.control_response,
            WorkerControlOutcome::Rejected { .. }
        ));
        assert!(!client.poisoned);
        client.close().await;
        // A poll can legitimately return a completed job rather than a start ACK.
        let state = elixir_marketplace::worker_control::CatalogJobState {
            phase: CatalogJobPhase::Complete,
            kind: Some(elixir_marketplace::worker_control::CatalogJobKind::Maintenance),
            peer: None,
            admitted: 0,
            unchanged: 0,
            rejected: 0,
            indexed: 1,
            removed: 0,
        };
        assert!(control_kind_matches(
            &WorkerControlOperation::CatalogJobStatus,
            &WorkerControlOutcome::CatalogJob { state }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn private_security_receipts_bind_generation_revision_and_baseline_phase() -> Result<()> {
        use elixir_marketplace::{
            catalog_fence::{FenceBatch, PublisherFence, baseline_digest},
            identity::Digest,
        };
        fn operation(stage: u8) -> Result<WorkerControlOperation> {
            let data: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
            )))?;
            let publishers = vec![PublisherFence {
                schema_version: 1,
                root_key: serde_json::from_value(data["publisher"]["payload"]["root_key"].clone())?,
                sequence: 0,
                payload_id: Digest::of(b"private protocol fixture, not signed authority"),
                conflicted: false,
            }];
            let generation = Digest::of(b"private catalog generation");
            Ok(match stage {
                0 => WorkerControlOperation::CatalogSecurityBegin {
                    generation,
                    revision: 7,
                    publishers: 1,
                    extensions: 0,
                    digest: baseline_digest(&publishers, &[])?,
                },
                1 => WorkerControlOperation::CatalogSecurityApply {
                    generation,
                    batch: FenceBatch {
                        schema_version: 1,
                        revision: 7,
                        publishers,
                        extensions: vec![],
                    },
                },
                _ => WorkerControlOperation::CatalogSecurityFinish {
                    generation,
                    revision: 7,
                },
            })
        }
        let mut client = fake("valid", Duration::from_secs(3))?;
        for stage in 0..3 {
            assert!(matches!(
                client.control(operation(stage)?).await?.control_response,
                WorkerControlOutcome::CatalogSecurity { revision: 7, .. }
            ));
        }
        client.close().await;
        for (mode, stage) in [
            ("catalog_security_wrong_generation", 0),
            ("catalog_security_wrong_revision", 1),
            ("catalog_security_premature_ready", 0),
            ("catalog_security_unfinished", 2),
        ] {
            let mut client = fake(mode, Duration::from_secs(3))?;
            assert!(client.control(operation(stage)?).await.is_err(), "{mode}");
            assert!(client.poisoned, "{mode}");
            assert!(client.request(query()).await.is_err());
            client.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn closing_drops_original_stdin_before_wait_and_cannot_reopen_the_stream() -> Result<()> {
        let mut child = tokio::process::Command::new("node")
            .args(["-e", "const timer=setInterval(()=>{},1000);process.stdin.resume();process.stdin.once('end',()=>clearInterval(timer));process.stdout.write('READY');"])
            .env_clear().env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
        let mut client = PrivateWorkerClient::new(PrivateProcessStdio {
            input: child.stdin.take().context("stdin")?,
            output: child.stdout.take().context("stdout")?,
            diagnostics: child.stderr.take(),
            child,
        })?;
        let mut ready = [0; 5];
        timeout(
            Duration::from_secs(3),
            client
                .process
                .as_mut()
                .context("attachment")?
                .output
                .read_exact(&mut ready),
        )
        .await??;
        assert_eq!(&ready, b"READY");
        assert!(
            client.close_attachment().await?.success(),
            "EOF must permit natural exit, not timeout/kill"
        );
        assert!(client.process.is_none());
        assert!(client.poisoned);
        assert!(client.request(query()).await.is_err());
        assert!(
            client.close_attachment().await.is_err(),
            "the consumed attachment cannot be replaced"
        );
        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn noncooperative_close_keeps_bounded_kill_and_original_wait() -> Result<()> {
        let mut client = fake("stall", Duration::from_secs(3))?;
        let status = client.close_attachment().await?;
        assert!(
            !status.success(),
            "a forced exit is not a graceful-close receipt"
        );
        assert!(client.process.is_none());
        assert!(client.poisoned);
        assert!(client.request(query()).await.is_err());
        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn private_channel_handles_fragmentation_and_keeps_request_ids_scoped() -> Result<()> {
        let mut client = fake("valid", Duration::from_secs(3))?;
        let first = client.request(query()).await?;
        let second = client.request(query()).await?;
        assert_ne!(first.request_id, second.request_id);
        assert!(first.request_id.unwrap() < (1u64 << 53));
        assert!(matches!(first.response, PeerOutcome::Rejected { .. }));
        assert!(!client.poisoned);
        client.close().await;
        Ok(())
    }

    fn evidence() -> (ExtensionId, EvidenceBatch) {
        let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))
        .unwrap();
        (
            fixture["release"]["payload"]["extension_id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            EvidenceBatch::parse(&serde_json::to_vec(&fixture["evidence"]).unwrap()).unwrap(),
        )
    }

    #[tokio::test]
    async fn private_evidence_receipts_share_correlation_but_not_the_public_protocol() -> Result<()>
    {
        let mut client = fake("valid", Duration::from_secs(3))?;
        let (id, batch) = evidence();
        let expected = batch.pairs.len();
        let receipt = client.merge_evidence(id, batch).await?;
        assert!(
            matches!(receipt.control_response, WorkerControlOutcome::EvidenceMerged { accepted } if accepted == expected)
        );
        let peer = client.request(query()).await?;
        assert_eq!(peer.request_id, Some(receipt.request_id + 1));
        assert!(!client.poisoned);
        client.close().await;
        let mut client = fake("control_reject", Duration::from_secs(3))?;
        let (id, batch) = evidence();
        assert!(matches!(
            client.merge_evidence(id, batch).await?.control_response,
            WorkerControlOutcome::Rejected { .. }
        ));
        assert!(
            !client.poisoned,
            "a correlated cache rejection is not transport corruption"
        );
        client.request(query()).await?;
        client.close().await;
        Ok(())
    }

    fn network_start() -> WorkerControlOperation {
        use elixir_marketplace::{
            identity::Digest,
            worker_network::{BootstrapEndpoint, NetworkConfig, NetworkRoles},
        };
        WorkerControlOperation::NetworkStart {
            config: NetworkConfig {
                bootstrap: vec![BootstrapEndpoint {
                    address: "1.1.1.1".parse().unwrap(),
                    port: 49737,
                }],
                topics: vec![Digest::of(b"elixir-marketplace/catalog/v1")],
                limits: Default::default(),
                roles: NetworkRoles {
                    consumer: true,
                    catalog: false,
                    routing: false,
                },
            },
        }
    }
    fn network_request() -> WorkerControlOperation {
        WorkerControlOperation::NetworkRequest {
            peer: "1".repeat(64).try_into().unwrap(),
            request: query(),
        }
    }
    #[test]
    fn only_wan_start_gets_a_longer_private_deadline() {
        assert_eq!(
            control_deadline(PRIVATE_DEADLINE, &network_start()),
            NETWORK_START_DEADLINE
        );
        for operation in [
            network_request(),
            WorkerControlOperation::NetworkStatus,
            WorkerControlOperation::NetworkRefresh,
            WorkerControlOperation::NetworkStop,
        ] {
            assert_eq!(
                control_deadline(PRIVATE_DEADLINE, &operation),
                PRIVATE_DEADLINE
            );
        }
        let short = Duration::from_millis(400);
        assert_eq!(control_deadline(short, &network_start()), short);
        assert_eq!(
            control_deadline(Duration::from_secs(60), &network_start()),
            Duration::from_secs(60)
        );
    }
    #[tokio::test]
    async fn network_control_correlates_roles_peer_identity_and_operation() -> Result<()> {
        let mut client = fake("valid", Duration::from_secs(3))?;
        assert!(
            matches!(client.control(network_start()).await?.control_response,WorkerControlOutcome::NetworkState {status} if status.active && status.consumer && !status.routing)
        );
        assert!(matches!(
            client.control(network_request()).await?.control_response,
            WorkerControlOutcome::NetworkReply {
                control_bytes: 512,
                ..
            }
        ));
        assert!(
            matches!(client.control(WorkerControlOperation::NetworkStop).await?.control_response,WorkerControlOutcome::NetworkState {status} if !status.active)
        );
        client.close().await;
        let mut client = fake("network_physical_counts", Duration::from_secs(3))?;
        assert!(matches!(
            client.control(network_start()).await?.control_response,
            WorkerControlOutcome::NetworkState { status }
                if status.classifying_peers == 1 && status.blob_peers == 1
        ));
        client.close().await;
        for (mode, operation) in [
            ("network_adaptive", network_start()),
            ("network_flood", network_start()),
            ("network_physical_flood", network_start()),
            ("network_blob_overflow", network_start()),
            (
                "network_physical_false_off",
                WorkerControlOperation::NetworkStop,
            ),
            ("network_wrong_roles", network_start()),
            ("network_false_off", WorkerControlOperation::NetworkStop),
            ("network_wrong_peer", network_request()),
            ("network_wrong_kind", network_request()),
            ("wrong_id", network_start()),
            ("control_frame_excess", network_start()),
        ] {
            let mut client = fake(mode, Duration::from_secs(3))?;
            assert!(client.control(operation).await.is_err(), "{mode}");
            assert!(client.poisoned, "{mode}");
            assert!(client.request(query()).await.is_err());
            client.close().await;
        }
        Ok(())
    }

    #[test]
    fn changes_reply_kind_is_metadata_only_and_cannot_replace_a_search_reply() -> Result<()> {
        let operation = PeerOperation::Changes {
            limit: 20,
            after: None,
        };
        let response: PeerResponse = serde_json::from_value(
            serde_json::json!({"schema_version":1,"request_id":1,
            "response":{"type":"changes","page":{"schema_version":1,"entries":[],"next_cursor":null,
            "checkpoint":{"epoch":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","revision":0,"extension_id":null},
            "network_complete":false,"metadata_only":true}}}),
        )?;
        response.validate()?;
        assert!(peer_kind_matches(&operation, &response.response));
        assert!(!peer_kind_matches(&query(), &response.response));
        Ok(())
    }

    #[tokio::test]
    async fn private_blob_chunks_have_a_specific_reply_ceiling_and_bind_job_offset_and_phase()
    -> Result<()> {
        use elixir_marketplace::{
            identity::Digest,
            records::Blob,
            worker_blob::{
                BLOB_REPLY_ALLOWANCE, BLOB_REQUEST_ALLOWANCE, BlobDownloadSpec, decode_chunk,
            },
        };
        let job = Digest::of(b"private blob job");
        let read = |offset| WorkerControlOperation::BlobRead { job, offset };
        let mut client = fake("valid", Duration::from_secs(5))?;
        let blob = Blob {
            sha256: Digest::of(vec![7; 65536]),
            size: 65536,
        };
        assert!(matches!(
            client
                .control(WorkerControlOperation::BlobStart {
                    job,
                    peer: "1".repeat(64).try_into()?,
                    transfer: BlobDownloadSpec {
                        blob,
                        maximum_bytes: 65536,
                        timeout_ms: 5000,
                        upload_bytes: BLOB_REQUEST_ALLOWANCE,
                        download_bytes: BLOB_REPLY_ALLOWANCE + 65536
                    }
                })
                .await?
                .control_response,
            WorkerControlOutcome::BlobTransfer {
                state: BlobReadState::Started,
                ..
            }
        ));
        let response = client.control(read(0)).await?;
        let WorkerControlOutcome::BlobTransfer {
            state: BlobReadState::Data { offset, data },
            ..
        } = response.control_response
        else {
            bail!("expected blob data")
        };
        assert_eq!(offset, 0);
        assert_eq!(decode_chunk(&data)?, vec![7; 65536]);
        assert!(matches!(
            client.control(read(65536)).await?.control_response,
            WorkerControlOutcome::BlobTransfer {
                state: BlobReadState::Complete {
                    integrity_only: true,
                    ..
                },
                ..
            }
        ));
        client.close().await;
        for (mode, operation) in [
            ("blob_wrong_job", read(0)),
            ("blob_wrong_offset", read(0)),
            ("blob_bad_encoding", read(0)),
            ("blob_wrong_phase", read(0)),
            ("blob_false_integrity", read(65536)),
            ("blob_frame_excess", read(0)),
            ("blob_bogus_status", WorkerControlOperation::NetworkStatus),
        ] {
            let mut client = fake(mode, Duration::from_secs(5))?;
            assert!(client.control(operation).await.is_err(), "{mode}");
            assert!(client.poisoned, "{mode}");
            assert!(client.request(query()).await.is_err());
            client.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_private_merge_receipts_and_cancellation_never_leave_a_reusable_stream()
    -> Result<()> {
        for mode in [
            "duplicate",
            "wrong_id",
            "null_id",
            "wrong_kind",
            "control_excess",
            "control_frame_excess",
            "oversize",
        ] {
            let mut client = fake(mode, Duration::from_secs(3))?;
            let (id, batch) = evidence();
            assert!(client.merge_evidence(id, batch).await.is_err(), "{mode}");
            assert!(client.poisoned, "{mode}");
            assert!(client.request(query()).await.is_err(), "{mode}");
            client.close().await;
        }
        let mut client = fake("stall", Duration::from_secs(3))?;
        let (id, batch) = evidence();
        assert!(
            timeout(Duration::from_millis(100), client.merge_evidence(id, batch))
                .await
                .is_err()
        );
        assert!(client.poisoned);
        assert!(client.request(query()).await.is_err());
        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn malformed_or_uncorrelated_replies_poison_the_channel() -> Result<()> {
        for mode in ["duplicate", "wrong_id", "null_id", "wrong_kind", "oversize"] {
            let mut client = fake(mode, Duration::from_secs(3))?;
            assert!(client.request(query()).await.is_err(), "{mode}");
            assert!(client.poisoned);
            assert!(client.request(query()).await.is_err());
            client.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn deadlines_and_diagnostic_floods_do_not_leave_reusable_streams() -> Result<()> {
        for mode in ["partial", "stall", "diagnostics"] {
            let mut client = fake(mode, Duration::from_millis(400))?;
            assert!(client.request(query()).await.is_err(), "{mode}");
            assert!(client.poisoned);
            client.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_requests_poison_and_kill_the_attachment() -> Result<()> {
        let mut client = fake("stall", Duration::from_secs(5))?;
        assert!(
            timeout(Duration::from_millis(100), client.request(query()))
                .await
                .is_err()
        );
        assert!(client.poisoned);
        assert!(client.request(query()).await.is_err());
        client.close().await;
        Ok(())
    }
}
