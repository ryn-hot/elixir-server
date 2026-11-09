//! Explicit owner preview, not ordinary discovery or installation. This view
//! never contains a paid score, executable/source verification or install token.
use super::worker::PublicationObservation;
use anyhow::{Result, bail};
use elixir_marketplace::{
    identity::{Digest, ExtensionId},
    records::{MediaTag, RuntimeProfile},
};
use serde::Serialize;

#[derive(Serialize)]
pub struct ImportPreview {
    pub schema_version: u32,
    pub reference_id: Digest,
    pub extension_id: ExtensionId,
    pub release_id: Digest,
    pub publisher_id: String,
    pub name: String,
    pub publisher: String,
    pub version: String,
    pub description: String,
    pub media_tags: Vec<MediaTag>,
    pub runtime_profile: RuntimeProfile,
    pub stable: bool,
    pub signatures_verified: bool,
    pub security_tracked: bool,
    pub metadata_only: bool,
    pub discovery_qualified: bool,
    pub source_verified: bool,
    pub install_available: bool,
    pub valid_for_seconds: u64,
    pub warning: &'static str,
}
impl ImportPreview {
    pub(in crate::marketplace) fn checked(
        value: &PublicationObservation,
        tracked: bool,
    ) -> Result<Self> {
        let summary = value.summary()?;
        let valid_for_seconds = value.valid_for().as_secs();
        if valid_for_seconds == 0 {
            bail!("direct-reference preview expired");
        }
        let publication = value.publication();
        let release = &publication.release.payload;
        Ok(Self {
            schema_version: 1,
            reference_id: summary.reference_id,
            extension_id: summary.extension_id.clone(),
            release_id: summary.release_id,
            publisher_id: publication.publisher.payload.root_key.publisher_id(),
            name: release.display_name.clone(),
            publisher: release.publisher_name.clone(),
            version: release.version.clone(),
            description: release.description.clone(),
            media_tags: release.media_tags.clone(),
            runtime_profile: release.runtime_profile,
            stable: summary.stable,
            signatures_verified: true,
            security_tracked: tracked,
            metadata_only: true,
            discovery_qualified: false,
            source_verified: false,
            install_available: false,
            valid_for_seconds,
            warning: "Direct developer reference: paid adoption is unqualified. Signatures do not prove safe code. Public source, package, executable and permission checks remain required. Nothing has been installed or made searchable.",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::{canonical, publication::PreparedPublication};

    #[test]
    fn a_native_receipt_preview_never_promotes_a_direct_reference_to_discovery_or_installation()
    -> Result<()> {
        let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        let publication = PreparedPublication::parse(&serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"publisher":fixture["publisher"],"release":fixture["release"],"head":fixture["head"]
        }))?)?;
        let (receipt, owner) =
            PublicationObservation::fixture_for_verified_publication(publication)?;
        let value = ImportPreview::checked(&receipt, false)?;
        let json = canonical::to_vec(&value)?;
        let view: serde_json::Value = serde_json::from_slice(&json)?;
        assert_eq!(view["signatures_verified"], true);
        for field in [
            "discovery_qualified",
            "source_verified",
            "install_available",
            "security_tracked",
        ] {
            assert_eq!(view[field], false, "{field}");
        }
        assert!(view.get("support").is_none());
        assert!(json.len() < 4096);
        owner(false);
        assert!(ImportPreview::checked(&receipt, false).is_err());
        Ok(())
    }
}
