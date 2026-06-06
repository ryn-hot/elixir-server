#![allow(dead_code)]

use std::{collections::BTreeSet, str::FromStr};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    acquisition::subscriptions::{
        AcquisitionRequestMode, AcquisitionRequestScope, AcquisitionRoutePolicy,
    },
    db::models::MediaType,
    extensions::ExternalIds,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionRequestOrigin {
    Api,
    FindMedia,
    LibraryDetail,
}

impl Default for AcquisitionRequestOrigin {
    fn default() -> Self {
        Self::Api
    }
}

impl AcquisitionRequestOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::FindMedia => "find_media",
            Self::LibraryDetail => "library_detail",
        }
    }
}

impl FromStr for AcquisitionRequestOrigin {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "api" => Ok(Self::Api),
            "find_media" | "findmedia" | "find-media" => Ok(Self::FindMedia),
            "library_detail" | "library" | "details" | "detail" => Ok(Self::LibraryDetail),
            other => bail!("unknown acquisition request origin '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScopedAddSelectionType {
    Movie,
    EntireTitle,
    Episode,
    Season,
    Range,
    SelectedTargets,
    AnimeArc,
}

impl ScopedAddSelectionType {
    pub fn request_scope(self) -> AcquisitionRequestScope {
        match self {
            Self::Movie => AcquisitionRequestScope::Movie,
            Self::EntireTitle => AcquisitionRequestScope::Subscription,
            Self::Episode => AcquisitionRequestScope::Episode,
            Self::Season => AcquisitionRequestScope::Season,
            Self::Range => AcquisitionRequestScope::Range,
            Self::SelectedTargets => AcquisitionRequestScope::SelectedTargets,
            Self::AnimeArc => AcquisitionRequestScope::AnimeArc,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScopedAddUnselectedTargetsPolicy {
    Ignore,
}

impl Default for ScopedAddUnselectedTargetsPolicy {
    fn default() -> Self {
        Self::Ignore
    }
}

impl ScopedAddUnselectedTargetsPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ignore => "ignore",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScopedAddMediaIdentity {
    pub kind: MediaType,
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub aliases: Vec<String>,
}

impl ScopedAddMediaIdentity {
    pub fn validated(&self) -> Result<Self> {
        let title = self.title.trim();
        if title.is_empty() {
            bail!("scoped add media title is required");
        }
        Ok(Self {
            kind: self.kind,
            title: title.to_string(),
            year: self.year,
            external_ids: self.external_ids.clone(),
            aliases: normalized_scoped_aliases(&self.aliases),
        })
    }
}

fn normalized_scoped_aliases(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut aliases = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_ascii_lowercase()) {
            continue;
        }
        aliases.push(trimmed.to_string());
    }
    aliases
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScopedAddSelection {
    #[serde(rename = "type")]
    pub selection_type: ScopedAddSelectionType,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub episode_start: Option<i32>,
    #[serde(default)]
    pub episode_end: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_start: Option<i32>,
    #[serde(default)]
    pub absolute_episode_end: Option<i32>,
    #[serde(default)]
    pub target_keys: Vec<String>,
    #[serde(default)]
    pub arc_id: Option<String>,
    #[serde(default)]
    pub arc_label: Option<String>,
}

impl ScopedAddSelection {
    pub fn validated(&self) -> Result<Self> {
        let target_keys = canonical_target_keys(&self.target_keys)?;
        match self.selection_type {
            ScopedAddSelectionType::Movie | ScopedAddSelectionType::EntireTitle => {
                if !target_keys.is_empty() {
                    bail!("{} scoped add cannot include targetKeys", self.type_label());
                }
            }
            ScopedAddSelectionType::Episode => {
                if target_keys.is_empty()
                    && self.absolute_episode_number.is_none()
                    && !(self.season_number.is_some() && self.episode_number.is_some())
                {
                    bail!(
                        "episode scoped add requires targetKeys, seasonNumber plus episodeNumber, or absoluteEpisodeNumber"
                    );
                }
                validate_positive_opt(self.season_number, "seasonNumber")?;
                validate_positive_opt(self.episode_number, "episodeNumber")?;
                validate_positive_opt(self.absolute_episode_number, "absoluteEpisodeNumber")?;
            }
            ScopedAddSelectionType::Season => {
                validate_positive_required(self.season_number, "seasonNumber")?;
            }
            ScopedAddSelectionType::Range => {
                validate_range_selection(self, !target_keys.is_empty())?;
            }
            ScopedAddSelectionType::SelectedTargets => {
                if target_keys.is_empty() {
                    bail!("selected-target scoped add requires targetKeys");
                }
            }
            ScopedAddSelectionType::AnimeArc => {
                if target_keys.is_empty() {
                    bail!("anime-arc scoped add requires targetKeys");
                }
                let has_arc_identity = self
                    .arc_id
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    || self
                        .arc_label
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty());
                if !has_arc_identity {
                    bail!("anime-arc scoped add requires arcId or arcLabel");
                }
            }
        }

        Ok(Self {
            selection_type: self.selection_type,
            season_number: self.season_number,
            episode_number: self.episode_number,
            episode_start: self.episode_start,
            episode_end: self.episode_end,
            absolute_episode_number: self.absolute_episode_number,
            absolute_episode_start: self.absolute_episode_start,
            absolute_episode_end: self.absolute_episode_end,
            target_keys,
            arc_id: trim_optional_string(self.arc_id.as_deref()),
            arc_label: trim_optional_string(self.arc_label.as_deref()),
        })
    }

    pub fn request_scope(&self) -> AcquisitionRequestScope {
        self.selection_type.request_scope()
    }

    fn type_label(&self) -> &'static str {
        match self.selection_type {
            ScopedAddSelectionType::Movie => "movie",
            ScopedAddSelectionType::EntireTitle => "entire-title",
            ScopedAddSelectionType::Episode => "episode",
            ScopedAddSelectionType::Season => "season",
            ScopedAddSelectionType::Range => "range",
            ScopedAddSelectionType::SelectedTargets => "selected-target",
            ScopedAddSelectionType::AnimeArc => "anime-arc",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScopedAddScopeDocument {
    pub origin: AcquisitionRequestOrigin,
    #[serde(default)]
    pub source_provider_id: Option<Uuid>,
    #[serde(default)]
    pub route_policy: Option<AcquisitionRoutePolicy>,
    pub media: ScopedAddMediaIdentity,
    pub selection: ScopedAddSelection,
    #[serde(default)]
    pub unselected_targets_policy: ScopedAddUnselectedTargetsPolicy,
}

impl ScopedAddScopeDocument {
    pub fn find_media(
        source_provider_id: Option<Uuid>,
        route_policy: Option<AcquisitionRoutePolicy>,
        media: ScopedAddMediaIdentity,
        selection: ScopedAddSelection,
    ) -> Result<Self> {
        Self {
            origin: AcquisitionRequestOrigin::FindMedia,
            source_provider_id,
            route_policy,
            media,
            selection,
            unselected_targets_policy: ScopedAddUnselectedTargetsPolicy::Ignore,
        }
        .validated()
    }

    pub fn validated(&self) -> Result<Self> {
        Ok(Self {
            origin: self.origin,
            source_provider_id: self.source_provider_id,
            route_policy: self.route_policy,
            media: self.media.validated()?,
            selection: self.selection.validated()?,
            unselected_targets_policy: self.unselected_targets_policy,
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewRequest {
    #[serde(default)]
    pub provider_id: Option<Uuid>,
    pub media_type: MediaType,
    pub result: ScopedAddMediaIdentity,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewResponse {
    pub media: ScopedAddMediaIdentity,
    pub capabilities: FindMediaScopePreviewCapabilities,
    #[serde(default)]
    pub seasons: Vec<FindMediaScopePreviewSeason>,
    #[serde(default)]
    pub arcs: Vec<FindMediaScopePreviewArc>,
    #[serde(default)]
    pub blockers: Vec<FindMediaScopePreviewBlocker>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewCapabilities {
    pub entire_title: bool,
    pub seasons: bool,
    pub episode_range: bool,
    pub selected_episodes: bool,
    pub anime_arcs: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewSeason {
    pub season_number: i32,
    pub episode_count: usize,
    #[serde(default)]
    pub episodes: Vec<FindMediaScopePreviewEpisode>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewEpisode {
    pub target_key: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub title: Option<String>,
    pub air_date: Option<String>,
    pub thumbnail_url: Option<String>,
    pub overview: Option<String>,
    pub runtime_minutes: Option<i32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewArc {
    pub arc_id: String,
    pub label: String,
    #[serde(default)]
    pub target_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopePreviewBlocker {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopedAddRequest {
    pub provider_id: Uuid,
    pub media_type: MediaType,
    pub result: ScopedAddMediaIdentity,
    pub scope: ScopedAddSelection,
    #[serde(default)]
    pub route_policy: Option<AcquisitionRoutePolicy>,
}

impl FindMediaScopedAddRequest {
    pub fn scope_document(&self) -> Result<ScopedAddScopeDocument> {
        ScopedAddScopeDocument::find_media(
            Some(self.provider_id),
            self.route_policy,
            self.result.validated()?,
            self.scope.validated()?,
        )
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaScopedAddResponse {
    pub accepted: bool,
    pub subscription_id: Uuid,
    pub request_mode: AcquisitionRequestMode,
    pub request_origin: AcquisitionRequestOrigin,
    pub request_scope: AcquisitionRequestScope,
    pub target_count: usize,
    pub status: String,
}

pub fn canonical_target_keys(values: &[String]) -> Result<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for value in values {
        let key = canonical_acquisition_target_key(value)?;
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    Ok(keys)
}

pub fn canonical_acquisition_target_key(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        bail!("targetKey cannot be empty");
    }
    if normalized == "MOVIE" {
        return Ok(normalized);
    }
    if let Some((season, episode)) = parse_season_episode_key(&normalized) {
        return Ok(format!("S{season:02}E{episode:02}"));
    }
    if let Some(absolute) = parse_absolute_episode_key(&normalized) {
        return Ok(format!("A{absolute:04}"));
    }
    if let Some(date) = normalized.strip_prefix("DATE:") {
        let date = date.trim();
        if !date.is_empty() {
            return Ok(format!("DATE:{date}"));
        }
    }
    bail!("targetKey must be MOVIE, SxxEyy, Axxxx, or DATE:yyyy-mm-dd");
}

fn validate_range_selection(selection: &ScopedAddSelection, has_target_keys: bool) -> Result<()> {
    if has_target_keys {
        return Ok(());
    }
    if let Some(season) = selection.season_number {
        validate_positive_required(Some(season), "seasonNumber")?;
        let start = selection.episode_start.or(selection.episode_number);
        let end = selection.episode_end.or(start);
        validate_positive_required(start, "episodeStart")?;
        validate_positive_required(end, "episodeEnd")?;
        validate_ordered_range(start.unwrap(), end.unwrap(), "episode")?;
        return Ok(());
    }
    let start = selection
        .absolute_episode_start
        .or(selection.absolute_episode_number);
    let end = selection.absolute_episode_end.or(start);
    validate_positive_required(start, "absoluteEpisodeStart")?;
    validate_positive_required(end, "absoluteEpisodeEnd")?;
    validate_ordered_range(start.unwrap(), end.unwrap(), "absolute episode")?;
    Ok(())
}

fn validate_positive_required(value: Option<i32>, label: &str) -> Result<()> {
    match value {
        Some(value) if value > 0 => Ok(()),
        Some(_) => bail!("{label} must be greater than zero"),
        None => bail!("{label} is required"),
    }
}

fn validate_positive_opt(value: Option<i32>, label: &str) -> Result<()> {
    if let Some(value) = value {
        validate_positive_required(Some(value), label)?;
    }
    Ok(())
}

fn validate_ordered_range(start: i32, end: i32, label: &str) -> Result<()> {
    if (start - end).abs() > 2000 {
        bail!("{label} range is too large");
    }
    Ok(())
}

fn parse_season_episode_key(value: &str) -> Option<(i32, i32)> {
    let rest = value.strip_prefix('S')?;
    let (season, episode) = rest.split_once('E')?;
    let season = parse_positive_digits(season)?;
    let episode = parse_positive_digits(episode)?;
    Some((season, episode))
}

fn parse_absolute_episode_key(value: &str) -> Option<i32> {
    let rest = value.strip_prefix('A')?;
    parse_positive_digits(rest)
}

fn parse_positive_digits(value: &str) -> Option<i32> {
    if value.is_empty() || !value.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let parsed = value.parse::<i32>().ok()?;
    (parsed > 0).then_some(parsed)
}

fn trim_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn media() -> ScopedAddMediaIdentity {
        ScopedAddMediaIdentity {
            kind: MediaType::Anime,
            title: "Example Show".to_string(),
            year: Some(2026),
            external_ids: Some(ExternalIds {
                anilist: Some("123".to_string()),
                tvdb_series: Some("456".to_string()),
                ..ExternalIds::default()
            }),
            aliases: Vec::new(),
        }
    }

    #[test]
    fn scoped_add_document_serializes_season_shape() -> Result<()> {
        let document = ScopedAddScopeDocument::find_media(
            Some(Uuid::nil()),
            Some(AcquisitionRoutePolicy::DebridFirst),
            media(),
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::Season,
                season_number: Some(2),
                target_keys: vec!["s2e1".to_string(), "S02E02".to_string()],
                ..empty_selection(ScopedAddSelectionType::Season)
            },
        )?;
        let value = serde_json::to_value(&document)?;

        assert_eq!(value["origin"], json!("find_media"));
        assert_eq!(value["selection"]["type"], json!("season"));
        assert_eq!(value["selection"]["seasonNumber"], json!(2));
        assert_eq!(
            value["selection"]["targetKeys"],
            json!(["S02E01", "S02E02"])
        );
        assert_eq!(value["unselectedTargetsPolicy"], json!("ignore"));

        let round_trip: ScopedAddScopeDocument = serde_json::from_value(value)?;
        assert_eq!(round_trip.validated()?, document);
        Ok(())
    }

    #[test]
    fn scoped_add_document_serializes_range_shape() -> Result<()> {
        let document = ScopedAddScopeDocument::find_media(
            Some(Uuid::nil()),
            None,
            media(),
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::Range,
                season_number: Some(3),
                episode_start: Some(21),
                episode_end: Some(25),
                ..empty_selection(ScopedAddSelectionType::Range)
            },
        )?;
        let value = serde_json::to_value(&document)?;

        assert_eq!(value["selection"]["type"], json!("range"));
        assert_eq!(value["selection"]["seasonNumber"], json!(3));
        assert_eq!(value["selection"]["episodeStart"], json!(21));
        assert_eq!(value["selection"]["episodeEnd"], json!(25));
        assert_eq!(
            document.selection.request_scope(),
            AcquisitionRequestScope::Range
        );
        Ok(())
    }

    #[test]
    fn scoped_add_document_serializes_selected_targets_shape() -> Result<()> {
        let document = ScopedAddScopeDocument::find_media(
            Some(Uuid::nil()),
            None,
            media(),
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::SelectedTargets,
                target_keys: vec![
                    "s01e01".to_string(),
                    "S01E01".to_string(),
                    "a12".to_string(),
                ],
                ..empty_selection(ScopedAddSelectionType::SelectedTargets)
            },
        )?;
        let value = serde_json::to_value(&document)?;

        assert_eq!(value["selection"]["type"], json!("selected_targets"));
        assert_eq!(value["selection"]["targetKeys"], json!(["S01E01", "A0012"]));
        assert_eq!(
            document.selection.request_scope(),
            AcquisitionRequestScope::SelectedTargets
        );
        Ok(())
    }

    #[test]
    fn scoped_add_document_serializes_anime_arc_shape() -> Result<()> {
        let document = ScopedAddScopeDocument::find_media(
            Some(Uuid::nil()),
            None,
            media(),
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::AnimeArc,
                target_keys: vec!["A031".to_string(), "a44".to_string()],
                arc_id: Some("arlong-park".to_string()),
                arc_label: Some(" Arlong Park ".to_string()),
                ..empty_selection(ScopedAddSelectionType::AnimeArc)
            },
        )?;
        let value = serde_json::to_value(&document)?;

        assert_eq!(value["selection"]["type"], json!("anime_arc"));
        assert_eq!(value["selection"]["arcId"], json!("arlong-park"));
        assert_eq!(value["selection"]["arcLabel"], json!("Arlong Park"));
        assert_eq!(value["selection"]["targetKeys"], json!(["A0031", "A0044"]));
        assert_eq!(
            document.selection.request_scope(),
            AcquisitionRequestScope::AnimeArc
        );
        Ok(())
    }

    #[test]
    fn scoped_add_request_builds_find_media_scope_document() -> Result<()> {
        let provider_id = Uuid::new_v4();
        let request = FindMediaScopedAddRequest {
            provider_id,
            media_type: MediaType::Series,
            result: ScopedAddMediaIdentity {
                kind: MediaType::Series,
                title: " Scoped TV ".to_string(),
                year: None,
                external_ids: None,
                aliases: Vec::new(),
            },
            scope: ScopedAddSelection {
                selection_type: ScopedAddSelectionType::Episode,
                target_keys: vec!["s1e7".to_string()],
                ..empty_selection(ScopedAddSelectionType::Episode)
            },
            route_policy: Some(AcquisitionRoutePolicy::DebridOnly),
        };

        let document = request.scope_document()?;
        assert_eq!(document.origin, AcquisitionRequestOrigin::FindMedia);
        assert_eq!(document.source_provider_id, Some(provider_id));
        assert_eq!(document.media.title, "Scoped TV");
        assert_eq!(document.selection.target_keys, vec!["S01E07"]);
        assert_eq!(
            document.selection.request_scope(),
            AcquisitionRequestScope::Episode
        );
        Ok(())
    }

    #[test]
    fn invalid_scoped_selection_is_rejected_before_submit() {
        let selection = ScopedAddSelection {
            selection_type: ScopedAddSelectionType::AnimeArc,
            ..empty_selection(ScopedAddSelectionType::AnimeArc)
        };
        let error = selection.validated().unwrap_err().to_string();
        assert!(error.contains("anime-arc scoped add requires targetKeys"));

        let selection = ScopedAddSelection {
            selection_type: ScopedAddSelectionType::SelectedTargets,
            target_keys: vec!["not-a-target".to_string()],
            ..empty_selection(ScopedAddSelectionType::SelectedTargets)
        };
        let error = selection.validated().unwrap_err().to_string();
        assert!(error.contains("targetKey must be"));
    }

    fn empty_selection(selection_type: ScopedAddSelectionType) -> ScopedAddSelection {
        ScopedAddSelection {
            selection_type,
            season_number: None,
            episode_number: None,
            episode_start: None,
            episode_end: None,
            absolute_episode_number: None,
            absolute_episode_start: None,
            absolute_episode_end: None,
            target_keys: Vec::new(),
            arc_id: None,
            arc_label: None,
        }
    }
}
