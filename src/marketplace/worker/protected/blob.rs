//! One parent-owned transfer on the existing protected private channel.
use super::*;
use elixir_marketplace::{
    identity::Digest,
    records::Blob,
    worker_blob::{BlobReadState, decode_chunk},
};

#[derive(Default)]
pub(super) struct State {
    active: Option<Active>,
    // A queued start whose reply was lost may already own a child connection.
    // Never promote "no accepted receipt" into "nothing needs cancellation".
    pending_start: Option<Digest>,
}
struct Active {
    job: Digest,
    expected: Blob,
    offset: u64,
}
impl State {
    fn prepare(&self, operation: &WorkerControlOperation) -> Result<()> {
        match operation {
            WorkerControlOperation::BlobStart { .. }
                if self.active.is_none() && self.pending_start.is_none() =>
            {
                Ok(())
            }
            WorkerControlOperation::BlobRead { job, offset }
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.job == *job && active.offset == *offset) =>
            {
                Ok(())
            }
            WorkerControlOperation::BlobCancel { .. } => Ok(()),
            _ => bail!("protected blob job, offset or phase differs"),
        }
    }
    fn starting(&mut self, operation: &WorkerControlOperation) -> Result<()> {
        self.prepare(operation)?;
        if let WorkerControlOperation::BlobStart { job, .. } = operation {
            self.pending_start = Some(*job);
        }
        Ok(())
    }
    fn cancellation_is_noop(&self, job: Digest) -> bool {
        self.active.as_ref().is_none_or(|active| active.job != job)
            && self.pending_start != Some(job)
    }
    fn apply(
        &mut self,
        operation: &WorkerControlOperation,
        response: WorkerControlResponse,
    ) -> Result<BlobReadState> {
        let WorkerControlOutcome::BlobTransfer { job, state } = response.control_response else {
            bail!("protected blob control rejected")
        };
        match (operation, &state) {
            (
                WorkerControlOperation::BlobStart {
                    job: expected,
                    transfer,
                    ..
                },
                BlobReadState::Started,
            ) if *expected == job => {
                self.active = Some(Active {
                    job,
                    expected: transfer.blob.clone(),
                    offset: 0,
                });
                self.pending_start = None;
            }
            (
                WorkerControlOperation::BlobRead {
                    job: expected,
                    offset,
                },
                state,
            ) if *expected == job => {
                let active = self.active.as_mut().context("protected blob owner lost")?;
                match state {
                    BlobReadState::Data {
                        offset: observed,
                        data,
                    } if observed == offset => {
                        let size = decode_chunk(data)?.len() as u64;
                        if active.offset != *offset
                            || size > active.expected.size.saturating_sub(active.offset)
                        {
                            bail!("protected blob exceeds expected size");
                        }
                        active.offset += size;
                    }
                    BlobReadState::Pending { offset: observed }
                    | BlobReadState::Failed { offset: observed }
                        if observed == offset => {}
                    BlobReadState::Complete {
                        blob,
                        integrity_only: true,
                    } if active.offset == active.expected.size
                        && blob.size == active.expected.size
                        && blob.sha256 == active.expected.sha256 =>
                    {
                        self.active = None;
                    }
                    _ => bail!("protected blob receipt differs"),
                }
            }
            (WorkerControlOperation::BlobCancel { job: expected }, BlobReadState::Cancelled)
                if *expected == job
                    && self.active.as_ref().is_none_or(|active| active.job == job)
                    && self.pending_start.is_none_or(|pending| pending == job) =>
            {
                self.active = None;
                self.pending_start = None;
            }
            _ => bail!("protected blob receipt scope differs"),
        }
        Ok(state)
    }
    pub(super) fn reset(&mut self) {
        self.active = None;
        self.pending_start = None;
    }
}

impl ProtectedWorker {
    pub(super) async fn blob_control(
        &mut self,
        operation: WorkerControlOperation,
    ) -> Result<BlobReadState> {
        self.blob_control_retained(&operation).await
    }
    /// Borrow the same original command so a typed pre-dispatch check can
    /// return it to the existing bounded pending slot without a new job ID,
    /// transfer allowance, relative timeout or start/cancellation acknowledgement.
    pub(super) async fn blob_control_retained(
        &mut self,
        operation: &WorkerControlOperation,
    ) -> Result<BlobReadState> {
        self.blob.prepare(operation)?;
        if let WorkerControlOperation::BlobCancel { job } = operation {
            // A stale cancellation must never stop a replacement job. All blob
            // starts/EOFs/stops are serialized by this same parent owner.
            if self.blob.cancellation_is_noop(*job) {
                return Ok(BlobReadState::Cancelled);
            }
        } else {
            if !self.blob_transport {
                bail!("owned worker does not support blob transport");
            }
            if matches!(operation, WorkerControlOperation::BlobStart { .. }) {
                self.supervisor.require_ready()?;
            } else {
                self.supervisor.require_active()?;
            }
        }
        let request = elixir_marketplace::worker_control::WorkerControlRequest {
            schema_version: 1,
            request_id: 1,
            control: canonical_operation(operation)?,
        };
        request.validate()?;
        // Keep the exact local selection for semantic validation after IPC.
        self.blob.starting(operation)?;
        let response = self.network_control(request.control).await?;
        let result = self.blob.apply(operation, response);
        if result.is_err()
            && let Some(stop) = self.supervisor.stop.take()
        {
            let _ = stop.send(());
        }
        let state = result?;
        if !matches!(state, BlobReadState::Cancelled) {
            self.supervisor.require_active()?;
        }
        Ok(state)
    }
}

fn canonical_operation(operation: &WorkerControlOperation) -> Result<WorkerControlOperation> {
    // Closed trusted command, at most a few hundred bytes. No peer JSON enters
    // this copy; preserving it avoids granting authority to a worker receipt.
    Ok(elixir_marketplace::canonical::decode(
        &elixir_marketplace::canonical::to_vec(operation)?,
        4096,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use elixir_marketplace::worker_blob::{
        BLOB_REPLY_ALLOWANCE, BLOB_REQUEST_ALLOWANCE, BlobDownloadSpec,
    };
    fn response(job: Digest, state: BlobReadState) -> WorkerControlResponse {
        WorkerControlResponse {
            schema_version: 1,
            request_id: 1,
            control_response: WorkerControlOutcome::BlobTransfer { job, state },
        }
    }
    fn start(job: Digest) -> WorkerControlOperation {
        WorkerControlOperation::BlobStart {
            job,
            peer: "1".repeat(64).try_into().unwrap(),
            transfer: BlobDownloadSpec {
                blob: Blob {
                    sha256: Digest::of(b"1234"),
                    size: 4,
                },
                maximum_bytes: 4,
                timeout_ms: 1000,
                upload_bytes: BLOB_REQUEST_ALLOWANCE,
                download_bytes: BLOB_REPLY_ALLOWANCE + 4,
            },
        }
    }
    #[test]
    fn a_lost_start_reply_is_not_a_noop_cancel_or_permission_to_start_a_replacement() -> Result<()>
    {
        let job = Digest::of(b"uncertain-start");
        let other = Digest::of(b"different-job");
        let mut state = State::default();
        let begin = start(job);
        state.starting(&begin)?;
        assert!(state.active.is_none());
        assert!(!state.cancellation_is_noop(job));
        assert!(
            state.cancellation_is_noop(other),
            "an old cancellation cannot stop a different pending start"
        );
        assert!(state.prepare(&start(other)).is_err());
        assert!(
            state
                .apply(&begin, response(other, BlobReadState::Started))
                .is_err()
        );
        assert_eq!(state.pending_start, Some(job));
        assert!(
            state
                .apply(
                    &WorkerControlOperation::BlobCancel { job },
                    response(other, BlobReadState::Cancelled)
                )
                .is_err()
        );
        assert!(!state.cancellation_is_noop(job));
        state.apply(
            &WorkerControlOperation::BlobCancel { job },
            response(job, BlobReadState::Cancelled),
        )?;
        assert!(state.cancellation_is_noop(job));
        state.prepare(&start(other))?;
        state.starting(&start(other))?;
        state.reset(); // only the actual acknowledged network-stop path resets
        assert!(state.cancellation_is_noop(other));
        Ok(())
    }
    #[test]
    fn parent_blob_state_binds_exact_size_digest_offset_and_complete_phase() -> Result<()> {
        let job = Digest::of(b"job");
        let mut state = State::default();
        let begin = start(job);
        state.prepare(&begin)?;
        state.apply(&begin, response(job, BlobReadState::Started))?;
        assert!(state.prepare(&begin).is_err());
        let read = WorkerControlOperation::BlobRead { job, offset: 0 };
        assert!(
            state
                .apply(
                    &read,
                    response(
                        job,
                        BlobReadState::Complete {
                            blob: Blob {
                                sha256: Digest::of(b"1234"),
                                size: 4
                            },
                            integrity_only: true
                        }
                    )
                )
                .is_err()
        );
        assert!(
            state
                .apply(
                    &read,
                    response(
                        job,
                        BlobReadState::Data {
                            offset: 0,
                            data: URL_SAFE_NO_PAD.encode(b"too long")
                        }
                    )
                )
                .is_err()
        );
        state.apply(
            &read,
            response(
                job,
                BlobReadState::Data {
                    offset: 0,
                    data: URL_SAFE_NO_PAD.encode(b"1234"),
                },
            ),
        )?;
        assert!(state.prepare(&read).is_err());
        let end = WorkerControlOperation::BlobRead { job, offset: 4 };
        state.prepare(&end)?;
        assert!(
            state
                .apply(
                    &end,
                    response(
                        job,
                        BlobReadState::Complete {
                            blob: Blob {
                                sha256: Digest::of(b"fake"),
                                size: 4
                            },
                            integrity_only: true
                        }
                    )
                )
                .is_err()
        );
        state.apply(
            &end,
            response(
                job,
                BlobReadState::Complete {
                    blob: Blob {
                        sha256: Digest::of(b"1234"),
                        size: 4,
                    },
                    integrity_only: true,
                },
            ),
        )?;
        assert!(state.active.is_none());
        state.prepare(&begin)?;
        Ok(())
    }
    #[test]
    fn parent_blob_state_never_advances_on_wrong_scope_or_a_failed_chunk() -> Result<()> {
        let job = Digest::of(b"job");
        let mut state = State::default();
        let begin = start(job);
        state.apply(&begin, response(job, BlobReadState::Started))?;
        let read = WorkerControlOperation::BlobRead { job, offset: 0 };
        assert!(
            state
                .prepare(&WorkerControlOperation::BlobRead {
                    job: Digest::of(b"other"),
                    offset: 0
                })
                .is_err()
        );
        assert!(
            state
                .apply(
                    &read,
                    response(Digest::of(b"other"), BlobReadState::Pending { offset: 0 })
                )
                .is_err()
        );
        state.apply(&read, response(job, BlobReadState::Failed { offset: 0 }))?;
        state.prepare(&read)?;
        state.apply(
            &WorkerControlOperation::BlobCancel { job },
            response(job, BlobReadState::Cancelled),
        )?;
        assert!(state.active.is_none());
        Ok(())
    }
}
