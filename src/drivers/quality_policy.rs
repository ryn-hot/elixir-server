use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use crate::drivers::patches::{
    CustomFormatSpec, QualityPolicyPresetId, QualityPolicyPresetSpec, QualityProfileSpec,
};

pub const ELIXIR_STOCK_TRASH_PROFILE: &str = "Elixir TRaSH";
pub const ELIXIR_MODERN_CODECS_PROFILE: &str = "Elixir Modern Codecs";
pub const ELIXIR_STORAGE_SAVER_PROFILE: &str = "Elixir Storage Saver";
pub const ELIXIR_MODERN_CODECS_4K_PROFILE: &str = "Elixir Modern Codecs 4K";
pub const ELIXIR_STORAGE_SAVER_4K_PROFILE: &str = "Elixir Storage Saver 4K";
pub const ELIXIR_STOCK_TRASH_4K_PROFILE: &str = "Elixir TRaSH 4K";

const SONARR_WEB_1080P_ALLOWED: &[&str] = &["WEBDL-1080p", "WEBRip-1080p"];
const SONARR_WEB_2160P_ALLOWED: &[&str] = &["WEBDL-2160p", "WEBRip-2160p"];

const SONARR_WEB_1080P_BASE: &[&str] = &[
    "br-disk",
    "lq",
    "lq-release-title",
    "x265-hd",
    "extras",
    "av1",
    "repack-proper",
    "repack2",
    "repack3",
    "amzn",
    "atvp",
    "cc",
    "dcu",
    "dscp",
    "dsnp",
    "hbo",
    "hmax",
    "hulu",
    "it",
    "max",
    "nf",
    "pcok",
    "pmtp",
    "sho",
    "stan",
    "syfy",
    "hd-streaming-boost",
    "web-tier-01",
    "web-tier-02",
    "web-tier-03",
    "web-scene",
];

const SONARR_WEB_2160P_BASE: &[&str] = &[
    "hdr",
    "br-disk",
    "lq",
    "lq-release-title",
    "x265-hd",
    "extras",
    "av1",
    "repack-proper",
    "repack2",
    "repack3",
    "amzn",
    "atvp",
    "cc",
    "dcu",
    "dscp",
    "dsnp",
    "hbo",
    "hmax",
    "hulu",
    "it",
    "max",
    "nf",
    "pcok",
    "pmtp",
    "sho",
    "stan",
    "syfy",
    "uhd-streaming-boost",
    "hd-streaming-boost",
    "web-tier-01",
    "web-tier-02",
    "web-tier-03",
    "web-scene",
];

const RADARR_HD_ALLOWED: &[&str] = &["Bluray-1080p", "WEBDL-1080p", "WEBRip-1080p"];
const RADARR_UHD_ALLOWED: &[&str] = &["Bluray-2160p", "WEBDL-2160p", "WEBRip-2160p"];

const RADARR_HD_BASE: &[&str] = &[
    "x265-hd",
    "br-disk",
    "lq",
    "lq-release-title",
    "extras",
    "av1",
    "repack-proper",
    "repack2",
    "repack3",
    "amzn",
    "atv",
    "atvp",
    "bcore",
    "crit",
    "dsnp",
    "hbo",
    "hmax",
    "hulu",
    "it",
    "ma",
    "max",
    "nf",
    "pcok",
    "pmtp",
    "play",
    "roku",
    "stan",
    "hd-bluray-tier-01",
    "hd-bluray-tier-02",
    "hd-bluray-tier-03",
    "web-tier-01",
    "web-tier-02",
    "web-tier-03",
];

const RADARR_UHD_BASE: &[&str] = &[
    "hdr",
    "dv-boost",
    "hdr10plus-boost",
    "dv-wo-hdr-fallback",
    "generated-dynamic-hdr",
    "x265-no-hdrdv",
    "br-disk",
    "lq",
    "lq-release-title",
    "extras",
    "av1",
    "repack-proper",
    "repack2",
    "repack3",
    "amzn",
    "atv",
    "atvp",
    "bcore",
    "crit",
    "dsnp",
    "hbo",
    "hmax",
    "hulu",
    "it",
    "ma",
    "max",
    "nf",
    "pcok",
    "pmtp",
    "play",
    "roku",
    "stan",
    "uhd-bluray-tier-01",
    "uhd-bluray-tier-02",
    "uhd-bluray-tier-03",
    "web-tier-01",
    "web-tier-02",
    "web-tier-03",
];

#[derive(Debug, Clone)]
pub struct SonarrQualityPolicyPlan {
    pub quality_profile: QualityProfileSpec,
    pub custom_formats: Vec<CustomFormatSpec>,
}

#[derive(Debug, Clone)]
pub struct RadarrQualityPolicyPlan {
    pub quality_profile: QualityProfileSpec,
    pub custom_formats: Vec<CustomFormatSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SonarrProfileVariant {
    Web1080p,
    Web2160p,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RadarrProfileVariant {
    BlurayWeb1080p,
    BlurayWeb2160p,
}

#[derive(Debug, Clone, Copy)]
enum VendoredFormatSource {
    Sonarr,
    Radarr,
}

#[derive(Debug, Deserialize)]
struct VendoredTrashCustomFormat {
    name: String,
    #[serde(default)]
    include_custom_format_when_renaming: bool,
    #[serde(default, rename = "includeCustomFormatWhenRenaming")]
    include_custom_format_when_renaming_v4: bool,
    specifications: Vec<Value>,
    #[serde(default)]
    trash_scores: HashMap<String, i32>,
}

pub fn build_sonarr_quality_policy_plan(
    policy: &QualityPolicyPresetSpec,
) -> Result<SonarrQualityPolicyPlan> {
    let variant = infer_sonarr_profile_variant(&policy.profile_name);
    let quality_profile = build_sonarr_quality_profile(policy, variant);
    let mut custom_formats = sonarr_base_formats(variant)?;

    apply_modern_codec_overrides(
        VendoredFormatSource::Sonarr,
        policy,
        variant.is_uhd(),
        &mut custom_formats,
    )?;

    Ok(SonarrQualityPolicyPlan {
        quality_profile,
        custom_formats,
    })
}

pub fn build_radarr_quality_policy_plan(
    policy: &QualityPolicyPresetSpec,
) -> Result<RadarrQualityPolicyPlan> {
    let variant = infer_radarr_profile_variant(&policy.profile_name);
    let quality_profile = build_radarr_quality_profile(policy, variant);
    let mut custom_formats = radarr_base_formats(variant)?;

    apply_modern_codec_overrides(
        VendoredFormatSource::Radarr,
        policy,
        variant.is_uhd(),
        &mut custom_formats,
    )?;

    Ok(RadarrQualityPolicyPlan {
        quality_profile,
        custom_formats,
    })
}

pub fn is_elixir_managed_sonarr_quality_profile(name: &str) -> bool {
    is_elixir_managed_quality_profile_name(name)
}

pub fn is_elixir_managed_radarr_quality_profile(name: &str) -> bool {
    is_elixir_managed_quality_profile_name(name)
}

fn is_elixir_managed_quality_profile_name(name: &str) -> bool {
    matches!(
        name.trim(),
        ELIXIR_STOCK_TRASH_PROFILE
            | ELIXIR_MODERN_CODECS_PROFILE
            | ELIXIR_STORAGE_SAVER_PROFILE
            | ELIXIR_STOCK_TRASH_4K_PROFILE
            | ELIXIR_MODERN_CODECS_4K_PROFILE
            | ELIXIR_STORAGE_SAVER_4K_PROFILE
    )
}

fn build_sonarr_quality_profile(
    policy: &QualityPolicyPresetSpec,
    variant: SonarrProfileVariant,
) -> QualityProfileSpec {
    let (allowed, cutoff) = match variant {
        SonarrProfileVariant::Web1080p => (
            SONARR_WEB_1080P_ALLOWED
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            Some("WEB 1080p".to_string()),
        ),
        SonarrProfileVariant::Web2160p => (
            SONARR_WEB_2160P_ALLOWED
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            Some("WEB 2160p".to_string()),
        ),
    };

    build_profile_spec(policy, allowed, cutoff)
}

fn build_radarr_quality_profile(
    policy: &QualityPolicyPresetSpec,
    variant: RadarrProfileVariant,
) -> QualityProfileSpec {
    let (allowed, cutoff) = match variant {
        RadarrProfileVariant::BlurayWeb1080p => (
            RADARR_HD_ALLOWED
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            Some("Bluray-1080p".to_string()),
        ),
        RadarrProfileVariant::BlurayWeb2160p => (
            RADARR_UHD_ALLOWED
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            Some("Bluray-2160p".to_string()),
        ),
    };

    build_profile_spec(policy, allowed, cutoff)
}

fn build_profile_spec(
    policy: &QualityPolicyPresetSpec,
    allowed: Vec<String>,
    cutoff: Option<String>,
) -> QualityProfileSpec {
    QualityProfileSpec {
        name: policy.profile_name.clone(),
        cutoff,
        allowed,
        upgrade_allowed: Some(true),
        min_format_score: Some(0),
        min_upgrade_format_score: None,
        cutoff_format_score: Some(10000),
    }
}

fn sonarr_base_formats(variant: SonarrProfileVariant) -> Result<Vec<CustomFormatSpec>> {
    let slugs = match variant {
        SonarrProfileVariant::Web1080p => SONARR_WEB_1080P_BASE,
        SonarrProfileVariant::Web2160p => SONARR_WEB_2160P_BASE,
    };

    slugs
        .iter()
        .map(|slug| load_vendored_custom_format(VendoredFormatSource::Sonarr, slug, None))
        .collect()
}

fn radarr_base_formats(variant: RadarrProfileVariant) -> Result<Vec<CustomFormatSpec>> {
    let slugs = match variant {
        RadarrProfileVariant::BlurayWeb1080p => RADARR_HD_BASE,
        RadarrProfileVariant::BlurayWeb2160p => RADARR_UHD_BASE,
    };

    slugs
        .iter()
        .map(|slug| load_vendored_custom_format(VendoredFormatSource::Radarr, slug, None))
        .collect()
}

fn apply_modern_codec_overrides(
    source: VendoredFormatSource,
    policy: &QualityPolicyPresetSpec,
    is_uhd: bool,
    custom_formats: &mut Vec<CustomFormatSpec>,
) -> Result<()> {
    match policy.preset {
        QualityPolicyPresetId::StockTrash => {}
        QualityPolicyPresetId::ModernCodecs if !is_uhd => {
            override_score(custom_formats, "x265 (HD)", 0)?;
            override_score(custom_formats, "AV1", 0)?;
            custom_formats.push(load_vendored_custom_format(
                source,
                "x265-no-hdrdv",
                Some(0),
            )?);
        }
        QualityPolicyPresetId::ModernCodecs => {
            override_score(custom_formats, "AV1", 0)?;
        }
        QualityPolicyPresetId::StorageSaver if !is_uhd => {
            override_score(custom_formats, "x265 (HD)", 25)?;
            override_score(custom_formats, "AV1", 25)?;
            custom_formats.push(load_vendored_custom_format(
                source,
                "x265-no-hdrdv",
                Some(25),
            )?);
        }
        QualityPolicyPresetId::StorageSaver => {
            override_score(custom_formats, "AV1", 25)?;
        }
    }
    Ok(())
}

fn override_score(formats: &mut [CustomFormatSpec], name: &str, score: i32) -> Result<()> {
    let normalized = normalize_name(name);
    let format = formats
        .iter_mut()
        .find(|format| normalize_name(&format.name) == normalized)
        .ok_or_else(|| anyhow::anyhow!("custom format '{}' is missing", name))?;
    format.score = Some(score);
    Ok(())
}

fn infer_sonarr_profile_variant(profile_name: &str) -> SonarrProfileVariant {
    if is_uhd_profile_name(profile_name) {
        SonarrProfileVariant::Web2160p
    } else {
        SonarrProfileVariant::Web1080p
    }
}

fn infer_radarr_profile_variant(profile_name: &str) -> RadarrProfileVariant {
    if is_uhd_profile_name(profile_name) {
        RadarrProfileVariant::BlurayWeb2160p
    } else {
        RadarrProfileVariant::BlurayWeb1080p
    }
}

fn is_uhd_profile_name(profile_name: &str) -> bool {
    let normalized = normalize_name(profile_name);
    normalized.contains("2160") || normalized.contains("4k") || normalized.contains("uhd")
}

impl SonarrProfileVariant {
    fn is_uhd(self) -> bool {
        matches!(self, Self::Web2160p)
    }
}

impl RadarrProfileVariant {
    fn is_uhd(self) -> bool {
        matches!(self, Self::BlurayWeb2160p)
    }
}

fn load_vendored_custom_format(
    source: VendoredFormatSource,
    slug: &str,
    score_override: Option<i32>,
) -> Result<CustomFormatSpec> {
    let raw = vendored_custom_format_json(source, slug)
        .ok_or_else(|| anyhow::anyhow!("unknown vendored custom format '{slug}'"))?;
    let parsed: VendoredTrashCustomFormat = serde_json::from_str(raw)
        .with_context(|| format!("parsing vendored custom format '{slug}'"))?;
    let default_score = parsed
        .trash_scores
        .get("default")
        .copied()
        .unwrap_or_default();
    if parsed.specifications.is_empty() {
        bail!("vendored custom format '{}' has no specifications", slug);
    }
    Ok(CustomFormatSpec {
        name: parsed.name,
        include: Vec::new(),
        exclude: Vec::new(),
        score: Some(score_override.unwrap_or(default_score)),
        include_custom_format_when_renaming: Some(
            parsed.include_custom_format_when_renaming
                || parsed.include_custom_format_when_renaming_v4,
        ),
        specifications: parsed.specifications,
    })
}

fn vendored_custom_format_json(source: VendoredFormatSource, slug: &str) -> Option<&'static str> {
    match source {
        VendoredFormatSource::Sonarr => sonarr_trash_custom_format_json(slug),
        VendoredFormatSource::Radarr => radarr_trash_custom_format_json(slug),
    }
}

fn sonarr_trash_custom_format_json(slug: &str) -> Option<&'static str> {
    match slug {
        "amzn" => Some(include_str!("quality_policy/sonarr_cf/amzn.json")),
        "atvp" => Some(include_str!("quality_policy/sonarr_cf/atvp.json")),
        "av1" => Some(include_str!("quality_policy/sonarr_cf/av1.json")),
        "br-disk" => Some(include_str!("quality_policy/sonarr_cf/br-disk.json")),
        "cc" => Some(include_str!("quality_policy/sonarr_cf/cc.json")),
        "dcu" => Some(include_str!("quality_policy/sonarr_cf/dcu.json")),
        "dscp" => Some(include_str!("quality_policy/sonarr_cf/dscp.json")),
        "dsnp" => Some(include_str!("quality_policy/sonarr_cf/dsnp.json")),
        "extras" => Some(include_str!("quality_policy/sonarr_cf/extras.json")),
        "hbo" => Some(include_str!("quality_policy/sonarr_cf/hbo.json")),
        "hd-streaming-boost" => Some(include_str!(
            "quality_policy/sonarr_cf/hd-streaming-boost.json"
        )),
        "hdr" => Some(include_str!("quality_policy/sonarr_cf/hdr.json")),
        "hmax" => Some(include_str!("quality_policy/sonarr_cf/hmax.json")),
        "hulu" => Some(include_str!("quality_policy/sonarr_cf/hulu.json")),
        "it" => Some(include_str!("quality_policy/sonarr_cf/it.json")),
        "lq" => Some(include_str!("quality_policy/sonarr_cf/lq.json")),
        "lq-release-title" => Some(include_str!(
            "quality_policy/sonarr_cf/lq-release-title.json"
        )),
        "max" => Some(include_str!("quality_policy/sonarr_cf/max.json")),
        "nf" => Some(include_str!("quality_policy/sonarr_cf/nf.json")),
        "pcok" => Some(include_str!("quality_policy/sonarr_cf/pcok.json")),
        "pmtp" => Some(include_str!("quality_policy/sonarr_cf/pmtp.json")),
        "repack-proper" => Some(include_str!("quality_policy/sonarr_cf/repack-proper.json")),
        "repack2" => Some(include_str!("quality_policy/sonarr_cf/repack2.json")),
        "repack3" => Some(include_str!("quality_policy/sonarr_cf/repack3.json")),
        "sho" => Some(include_str!("quality_policy/sonarr_cf/sho.json")),
        "stan" => Some(include_str!("quality_policy/sonarr_cf/stan.json")),
        "syfy" => Some(include_str!("quality_policy/sonarr_cf/syfy.json")),
        "uhd-streaming-boost" => Some(include_str!(
            "quality_policy/sonarr_cf/uhd-streaming-boost.json"
        )),
        "web-scene" => Some(include_str!("quality_policy/sonarr_cf/web-scene.json")),
        "web-tier-01" => Some(include_str!("quality_policy/sonarr_cf/web-tier-01.json")),
        "web-tier-02" => Some(include_str!("quality_policy/sonarr_cf/web-tier-02.json")),
        "web-tier-03" => Some(include_str!("quality_policy/sonarr_cf/web-tier-03.json")),
        "x265-hd" => Some(include_str!("quality_policy/sonarr_cf/x265-hd.json")),
        "x265-no-hdrdv" => Some(include_str!("quality_policy/sonarr_cf/x265-no-hdrdv.json")),
        _ => None,
    }
}

fn radarr_trash_custom_format_json(slug: &str) -> Option<&'static str> {
    match slug {
        "amzn" => Some(include_str!("quality_policy/radarr_cf/amzn.json")),
        "atv" => Some(include_str!("quality_policy/radarr_cf/atv.json")),
        "atvp" => Some(include_str!("quality_policy/radarr_cf/atvp.json")),
        "av1" => Some(include_str!("quality_policy/radarr_cf/av1.json")),
        "bcore" => Some(include_str!("quality_policy/radarr_cf/bcore.json")),
        "br-disk" => Some(include_str!("quality_policy/radarr_cf/br-disk.json")),
        "crit" => Some(include_str!("quality_policy/radarr_cf/crit.json")),
        "dsnp" => Some(include_str!("quality_policy/radarr_cf/dsnp.json")),
        "dv-boost" => Some(include_str!("quality_policy/radarr_cf/dv-boost.json")),
        "dv-wo-hdr-fallback" => Some(include_str!(
            "quality_policy/radarr_cf/dv-wo-hdr-fallback.json"
        )),
        "extras" => Some(include_str!("quality_policy/radarr_cf/extras.json")),
        "generated-dynamic-hdr" => Some(include_str!(
            "quality_policy/radarr_cf/generated-dynamic-hdr.json"
        )),
        "hbo" => Some(include_str!("quality_policy/radarr_cf/hbo.json")),
        "hd-bluray-tier-01" => Some(include_str!(
            "quality_policy/radarr_cf/hd-bluray-tier-01.json"
        )),
        "hd-bluray-tier-02" => Some(include_str!(
            "quality_policy/radarr_cf/hd-bluray-tier-02.json"
        )),
        "hd-bluray-tier-03" => Some(include_str!(
            "quality_policy/radarr_cf/hd-bluray-tier-03.json"
        )),
        "hdr" => Some(include_str!("quality_policy/radarr_cf/hdr.json")),
        "hdr10plus-boost" => Some(include_str!(
            "quality_policy/radarr_cf/hdr10plus-boost.json"
        )),
        "hmax" => Some(include_str!("quality_policy/radarr_cf/hmax.json")),
        "hulu" => Some(include_str!("quality_policy/radarr_cf/hulu.json")),
        "it" => Some(include_str!("quality_policy/radarr_cf/it.json")),
        "lq" => Some(include_str!("quality_policy/radarr_cf/lq.json")),
        "lq-release-title" => Some(include_str!(
            "quality_policy/radarr_cf/lq-release-title.json"
        )),
        "ma" => Some(include_str!("quality_policy/radarr_cf/ma.json")),
        "max" => Some(include_str!("quality_policy/radarr_cf/max.json")),
        "nf" => Some(include_str!("quality_policy/radarr_cf/nf.json")),
        "pcok" => Some(include_str!("quality_policy/radarr_cf/pcok.json")),
        "play" => Some(include_str!("quality_policy/radarr_cf/play.json")),
        "pmtp" => Some(include_str!("quality_policy/radarr_cf/pmtp.json")),
        "repack-proper" => Some(include_str!("quality_policy/radarr_cf/repack-proper.json")),
        "repack2" => Some(include_str!("quality_policy/radarr_cf/repack2.json")),
        "repack3" => Some(include_str!("quality_policy/radarr_cf/repack3.json")),
        "roku" => Some(include_str!("quality_policy/radarr_cf/roku.json")),
        "stan" => Some(include_str!("quality_policy/radarr_cf/stan.json")),
        "uhd-bluray-tier-01" => Some(include_str!(
            "quality_policy/radarr_cf/uhd-bluray-tier-01.json"
        )),
        "uhd-bluray-tier-02" => Some(include_str!(
            "quality_policy/radarr_cf/uhd-bluray-tier-02.json"
        )),
        "uhd-bluray-tier-03" => Some(include_str!(
            "quality_policy/radarr_cf/uhd-bluray-tier-03.json"
        )),
        "web-tier-01" => Some(include_str!("quality_policy/radarr_cf/web-tier-01.json")),
        "web-tier-02" => Some(include_str!("quality_policy/radarr_cf/web-tier-02.json")),
        "web-tier-03" => Some(include_str!("quality_policy/radarr_cf/web-tier-03.json")),
        "x265-hd" => Some(include_str!("quality_policy/radarr_cf/x265-hd.json")),
        "x265-no-hdrdv" => Some(include_str!("quality_policy/radarr_cf/x265-no-hdrdv.json")),
        _ => None,
    }
}

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sonarr_stock_trash_uses_default_block_scores() {
        let plan = build_sonarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::StockTrash,
            profile_name: ELIXIR_STOCK_TRASH_PROFILE.to_string(),
        })
        .expect("plan");

        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        assert_eq!(x265.score, Some(-10000));
        assert_eq!(av1.score, Some(-10000));
    }

    #[test]
    fn sonarr_modern_codecs_unblocks_hd_hevc_and_av1() {
        let plan = build_sonarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::ModernCodecs,
            profile_name: ELIXIR_MODERN_CODECS_PROFILE.to_string(),
        })
        .expect("plan");
        assert_eq!(
            plan.quality_profile.cutoff.as_deref(),
            Some("WEB 1080p")
        );

        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let x265_no_hdr = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (no HDR/DV)")
            .expect("x265 no hdrdv");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        assert_eq!(x265.score, Some(0));
        assert_eq!(x265_no_hdr.score, Some(0));
        assert_eq!(av1.score, Some(0));
    }

    #[test]
    fn sonarr_storage_saver_prefers_modern_codecs_without_overriding_source_tiers() {
        let plan = build_sonarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::StorageSaver,
            profile_name: ELIXIR_STORAGE_SAVER_PROFILE.to_string(),
        })
        .expect("plan");
        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        let web_tier = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "WEB Tier 01")
            .expect("web tier 01");

        assert_eq!(x265.score, Some(25));
        assert_eq!(av1.score, Some(25));
        assert!(
            web_tier.score.unwrap_or_default() > x265.score.unwrap_or_default(),
            "storage saver must not outrank TRaSH source tiers"
        );
    }

    #[test]
    fn radarr_stock_trash_uses_default_block_scores() {
        let plan = build_radarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::StockTrash,
            profile_name: ELIXIR_STOCK_TRASH_PROFILE.to_string(),
        })
        .expect("plan");

        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        assert_eq!(x265.score, Some(-10000));
        assert_eq!(av1.score, Some(-10000));
    }

    #[test]
    fn radarr_modern_codecs_unblocks_hd_hevc_and_av1() {
        let plan = build_radarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::ModernCodecs,
            profile_name: ELIXIR_MODERN_CODECS_PROFILE.to_string(),
        })
        .expect("plan");

        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let x265_no_hdr = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (no HDR/DV)")
            .expect("x265 no hdrdv");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        assert_eq!(x265.score, Some(0));
        assert_eq!(x265_no_hdr.score, Some(0));
        assert_eq!(av1.score, Some(0));
    }

    #[test]
    fn radarr_storage_saver_prefers_modern_codecs_without_overriding_source_tiers() {
        let plan = build_radarr_quality_policy_plan(&QualityPolicyPresetSpec {
            preset: QualityPolicyPresetId::StorageSaver,
            profile_name: ELIXIR_STORAGE_SAVER_PROFILE.to_string(),
        })
        .expect("plan");

        let x265 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "x265 (HD)")
            .expect("x265 hd");
        let av1 = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "AV1")
            .expect("av1");
        let web_tier = plan
            .custom_formats
            .iter()
            .find(|format| format.name == "WEB Tier 01")
            .expect("web tier 01");

        assert_eq!(x265.score, Some(25));
        assert_eq!(av1.score, Some(25));
        assert!(
            web_tier.score.unwrap_or_default() > x265.score.unwrap_or_default(),
            "storage saver must not outrank TRaSH source tiers"
        );
    }
}
