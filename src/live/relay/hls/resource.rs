use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use reqwest::Url;
use uuid::Uuid;

use super::rewrite::HlsRewriteError;

const RESOURCE_ID_BYTES: usize = 24;
const RESOURCE_ID_PREFIX: &str = "lr1_";

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HlsResourceId(String);

impl HlsResourceId {
    pub fn parse(value: &str) -> Option<Self> {
        let encoded = value.strip_prefix(RESOURCE_ID_PREFIX)?;
        let decoded = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        if decoded.len() != RESOURCE_ID_BYTES || URL_SAFE_NO_PAD.encode(decoded) != encoded {
            return None;
        }
        Some(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    fn generate() -> Self {
        let mut bytes = [0_u8; RESOURCE_ID_BYTES];
        OsRng.fill_bytes(&mut bytes);
        Self(format!(
            "{RESOURCE_ID_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(bytes)
        ))
    }
}

impl fmt::Debug for HlsResourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("HlsResourceId")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for HlsResourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct HlsManifestScope([u8; 32]);

impl HlsManifestScope {
    pub fn from_stable_key(key: &[u8]) -> Result<Self, HlsRewriteError> {
        if key.is_empty() || key.len() > 4_096 {
            return Err(HlsRewriteError::InvalidManifestScope);
        }
        Ok(Self(*blake3::hash(key).as_bytes()))
    }
}

impl fmt::Debug for HlsManifestScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HlsManifestScope([OPAQUE])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HlsResourceKind {
    Playlist,
    MediaSegment,
    InitializationSegment,
    EncryptionKey,
    PartialSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HlsByteRange {
    pub length: u64,
    pub offset: Option<u64>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct HlsResourceDescriptor {
    url: Url,
    kind: HlsResourceKind,
    byte_range: Option<HlsByteRange>,
}

impl HlsResourceDescriptor {
    pub(crate) fn new_for_relay_root(url: Url, kind: HlsResourceKind) -> Self {
        Self {
            url,
            kind,
            byte_range: None,
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub const fn kind(&self) -> HlsResourceKind {
        self.kind
    }

    pub const fn byte_range(&self) -> Option<HlsByteRange> {
        self.byte_range
    }
}

impl fmt::Debug for HlsResourceDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HlsResourceDescriptor")
            .field("url", &"[REDACTED]")
            .field("kind", &self.kind)
            .field("byte_range", &self.byte_range)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlsResourceLimits {
    pub max_resources: usize,
    pub retired_revision_grace: u64,
}

impl Default for HlsResourceLimits {
    fn default() -> Self {
        Self {
            max_resources: 4_096,
            retired_revision_grace: 2,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ResourceKey {
    url: Url,
    kind: HlsResourceKind,
    byte_range: Option<HlsByteRange>,
}

impl Hash for ResourceKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.url.as_str().hash(state);
        self.kind.hash(state);
        self.byte_range.hash(state);
    }
}

#[derive(Clone)]
struct ResourceEntry {
    descriptor: HlsResourceDescriptor,
    last_seen_by_scope: HashMap<HlsManifestScope, u64>,
}

#[derive(Clone)]
pub struct HlsResourceMap {
    session_id: Uuid,
    control_fencing_token: i64,
    limits: HlsResourceLimits,
    scope_revisions: HashMap<HlsManifestScope, u64>,
    active_revision: Option<(HlsManifestScope, u64)>,
    entries: HashMap<HlsResourceId, ResourceEntry>,
    reverse: HashMap<ResourceKey, HlsResourceId>,
}

impl HlsResourceMap {
    pub fn new(
        session_id: Uuid,
        control_fencing_token: i64,
        limits: HlsResourceLimits,
    ) -> Result<Self, HlsRewriteError> {
        if control_fencing_token <= 0 || limits.max_resources == 0 {
            return Err(HlsRewriteError::InvalidResourceMapConfiguration);
        }
        Ok(Self {
            session_id,
            control_fencing_token,
            limits,
            scope_revisions: HashMap::new(),
            active_revision: None,
            entries: HashMap::new(),
            reverse: HashMap::new(),
        })
    }

    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub const fn control_fencing_token(&self) -> i64 {
        self.control_fencing_token
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn resolve(
        &self,
        resource_id: &HlsResourceId,
        control_fencing_token: i64,
    ) -> Result<HlsResourceDescriptor, HlsRewriteError> {
        self.require_fence(control_fencing_token)?;
        self.entries
            .get(resource_id)
            .map(|entry| entry.descriptor.clone())
            .ok_or(HlsRewriteError::UnknownResource)
    }

    pub fn take_over(
        &mut self,
        expected_fencing_token: i64,
        new_fencing_token: i64,
    ) -> Result<(), HlsRewriteError> {
        self.require_fence(expected_fencing_token)?;
        if new_fencing_token <= self.control_fencing_token {
            return Err(HlsRewriteError::StaleControlFence);
        }
        self.control_fencing_token = new_fencing_token;
        self.scope_revisions.clear();
        self.active_revision = None;
        self.entries.clear();
        self.reverse.clear();
        Ok(())
    }

    pub(super) fn begin_revision(
        &mut self,
        scope: HlsManifestScope,
        control_fencing_token: i64,
    ) -> Result<(), HlsRewriteError> {
        self.require_fence(control_fencing_token)?;
        if self.active_revision.is_some() {
            return Err(HlsRewriteError::ResourceMapInvariant);
        }
        if !self.scope_revisions.contains_key(&scope)
            && self.scope_revisions.len() >= self.limits.max_resources
        {
            return Err(HlsRewriteError::ResourceLimitExceeded);
        }
        let revision = self
            .scope_revisions
            .get(&scope)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(HlsRewriteError::ResourceRevisionExhausted)?;
        self.scope_revisions.insert(scope, revision);
        self.active_revision = Some((scope, revision));
        let oldest = revision.saturating_sub(self.limits.retired_revision_grace);
        self.entries.retain(|_, entry| {
            entry
                .last_seen_by_scope
                .retain(|entry_scope, seen| *entry_scope != scope || *seen >= oldest);
            !entry.last_seen_by_scope.is_empty()
        });
        self.rebuild_reverse();
        Ok(())
    }

    pub(super) fn finish_revision(&mut self) -> Result<(), HlsRewriteError> {
        self.active_revision
            .take()
            .ok_or(HlsRewriteError::ResourceMapInvariant)?;
        self.scope_revisions.retain(|scope, _| {
            self.entries
                .values()
                .any(|entry| entry.last_seen_by_scope.contains_key(scope))
        });
        Ok(())
    }

    pub(super) fn validate_active_resources<F>(
        &self,
        validator: &mut F,
    ) -> Result<(), HlsRewriteError>
    where
        F: FnMut(&HlsResourceDescriptor) -> Result<(), HlsRewriteError>,
    {
        let (scope, revision) = self
            .active_revision
            .ok_or(HlsRewriteError::ResourceMapInvariant)?;
        for entry in self
            .entries
            .values()
            .filter(|entry| entry.last_seen_by_scope.get(&scope).copied() == Some(revision))
        {
            validator(&entry.descriptor)?;
        }
        Ok(())
    }

    pub(super) fn register(
        &mut self,
        url: Url,
        kind: HlsResourceKind,
        byte_range: Option<HlsByteRange>,
    ) -> Result<HlsResourceId, HlsRewriteError> {
        let (scope, revision) = self
            .active_revision
            .ok_or(HlsRewriteError::ResourceMapInvariant)?;
        let key = ResourceKey {
            url: url.clone(),
            kind,
            byte_range,
        };
        if let Some(resource_id) = self.reverse.get(&key).cloned() {
            let entry = self
                .entries
                .get_mut(&resource_id)
                .ok_or(HlsRewriteError::ResourceMapInvariant)?;
            entry.last_seen_by_scope.insert(scope, revision);
            return Ok(resource_id);
        }
        if self.entries.len() >= self.limits.max_resources {
            return Err(HlsRewriteError::ResourceLimitExceeded);
        }
        let resource_id = (0..16)
            .map(|_| HlsResourceId::generate())
            .find(|candidate| !self.entries.contains_key(candidate))
            .ok_or(HlsRewriteError::ResourceIdGenerationFailed)?;
        self.entries.insert(
            resource_id.clone(),
            ResourceEntry {
                descriptor: HlsResourceDescriptor {
                    url,
                    kind,
                    byte_range,
                },
                last_seen_by_scope: HashMap::from([(scope, revision)]),
            },
        );
        self.reverse.insert(key, resource_id.clone());
        Ok(resource_id)
    }

    fn require_fence(&self, control_fencing_token: i64) -> Result<(), HlsRewriteError> {
        if control_fencing_token != self.control_fencing_token {
            return Err(HlsRewriteError::StaleControlFence);
        }
        Ok(())
    }

    fn rebuild_reverse(&mut self) {
        self.reverse = self
            .entries
            .iter()
            .map(|(id, entry)| {
                (
                    ResourceKey {
                        url: entry.descriptor.url.clone(),
                        kind: entry.descriptor.kind,
                        byte_range: entry.descriptor.byte_range,
                    },
                    id.clone(),
                )
            })
            .collect();
    }
}

impl fmt::Debug for HlsResourceMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HlsResourceMap")
            .field("session_id", &self.session_id)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("manifest_scope_count", &self.scope_revisions.len())
            .field("revision_active", &self.active_revision.is_some())
            .field("resource_count", &self.entries.len())
            .finish()
    }
}
