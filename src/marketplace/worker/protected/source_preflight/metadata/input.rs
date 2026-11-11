use elixir_marketplace::{Result, admission::CandidateDescriptor, identity::Digest};

/// Checked canonical bytes and digest of the same immutable borrowed body.
/// Callers cannot supply their own digest or substitute a different body.
pub(super) struct CheckedDescriptor<'a> {
    descriptor: &'a CandidateDescriptor,
    bytes: Vec<u8>,
    id: Digest,
}
impl<'a> CheckedDescriptor<'a> {
    pub(super) fn new(descriptor: &'a CandidateDescriptor) -> Result<Self> {
        let bytes = descriptor.to_bytes()?;
        let id = Digest::of(&bytes);
        Ok(Self {
            descriptor,
            bytes,
            id,
        })
    }
    pub(super) fn descriptor(&self) -> &'a CandidateDescriptor {
        self.descriptor
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(super) fn id(&self) -> Digest {
        self.id
    }
}
