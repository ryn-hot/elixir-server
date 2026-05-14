use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use elixir_classifier::hint::{
    FileInput as ClassifierFileInput, HintParser, LibraryType as ClassifierLibraryType,
    anime_parser_adapter::AnimeParserAdapter,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::{
    acquisition::{
        release_resolution::models::{
            AnimeEpisodeType, AnimeMatchOutcome, NewAcquisitionAnimeGraphSnapshot,
            ReleaseConfidence, ReleaseCoverageKind, ReleaseCoverageState, ReleaseKind,
            ReleaseResolverKind,
        },
        subscriptions::{AcquisitionTargetState, NewAcquisitionTarget},
    },
    db::models::MediaType,
    extensions::ExternalIds,
    library::{AniListSeasonChainEntry, AniZipMapping},
};

pub const ANIME_SHOKO_STYLE_RESOLVER_VERSION: &str = "rr3-anime-shoko-style-v0";
pub const SHOKO_REFERENCE_COMMIT: &str = "74a673ed57daef76ac6ac1c745728bebcfbd870b";
pub const SHOKO_REFERENCE_REPOSITORY: &str = "https://github.com/ShokoAnime/ShokoServer";
pub const ANIME_PRE_DOWNLOAD_PARSER_VERSION: &str = "rr3d-anime-pre-download-parser-v0";

const RR3_METADATA_GRAPH_FINGERPRINT_PREFIX: &str = "rr3c-anime-graph";

static LEADING_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\s*(?:\[(?P<square>[^\]]{1,120})\]|【(?P<wide>[^】]{1,120})】)")
        .expect("valid anime release group regex")
});
static BRACKET_SEGMENT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?:\[(?P<square>[^\]]{1,180})\]|【(?P<wide>[^】]{1,180})】)")
        .expect("valid anime bracket regex")
});
static SXXEYY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\bS0*(?P<season>\d{1,2})\s*E0*(?P<episode>\d{1,4})(?:\s*[-~～]\s*(?:E)?0*(?P<end>\d{1,4}))?")
        .expect("valid anime SxxEyy regex")
});
static SEASON_DASH_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\bS0*(?P<season>\d{1,2})\s*[-_.\s]+\s*0*(?P<episode>\d{1,4})(?:v\d+)?\b")
        .expect("valid anime Sxx dash episode regex")
});
static SEASON_WORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\bS0*(?P<s>\d{1,2})\b|(?P<ordinal>\d+)(?:st|nd|rd|th)\s+Season|Season\s*0*(?P<season>\d{1,2})|Part\s*0*(?P<part>\d{1,2})|Cour\s*0*(?P<cour>\d{1,2}))")
        .expect("valid anime season word regex")
});
static TITLE_SEASON_CODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bS0+(?P<season>\d{1,2})\b").expect("valid anime title season code regex")
});
static DASH_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[^\p{L}\p{N}])[-_\s]+0*(?P<start>\d{1,4})(?:\s*[-~～]\s*0*(?P<end>\d{1,4}))?(?:v\d+)?(?:\b|[^\p{L}\p{N}])")
        .expect("valid anime dash episode regex")
});
static TRAILING_EPISODE_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\s*[-_\s]+0*\d{1,4}(?:\s*[-~～]\s*0*\d{1,4})?(?:v\d+)?\s*$")
        .expect("valid anime trailing episode suffix regex")
});
static TITLE_TRAILING_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?P<title>.+?)\s+[-_]\s+0*\d{1,4}(?:\s*[-~～]\s*0*\d{1,4})?(?:v\d+)?\s*$")
        .expect("valid anime title trailing episode regex")
});
static TITLE_TRAILING_BATCH_MARKER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)\s+\b(?:complete\s+(?:series|season|collection)|season\s+\d+\s+complete|batch)\b.*$",
    )
    .expect("valid anime title trailing batch marker regex")
});
static BATCH_EPISODE_RANGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)\b(?:complete\s+(?:series|season|collection)|season\s+\d+\s+complete|batch)\s+0*(?P<start>\d{1,4})(?:\s*[-~～]\s*0*(?P<end>\d{1,4}))?",
    )
    .expect("valid anime batch episode range regex")
});
static BRACKET_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)^0*(?P<start>\d{1,4})(?:\s*[-~～]\s*0*(?P<end>\d{1,4}))?\s*(?:END|FIN|完|集|话|話)?$",
    )
    .expect("valid anime bracket episode regex")
});
static CHINESE_EPISODE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)第\s*0*(?P<episode>\d{1,4})\s*(?:集|话|話)")
        .expect("valid Chinese episode regex")
});
static VERSION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[^\p{L}\p{N}])(?:v(?P<v>\d)\b|\[v(?P<bracket>\d)\]|(?P<episode>\d{1,4})v(?P<episode_v>\d)\b|repack(?P<repack>\d*)|rerip(?P<rerip>\d*))")
        .expect("valid anime version regex")
});
static CRC32_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:\[|\()(?P<crc>[a-f0-9]{8})(?:\]|\))").expect("valid CRC32 regex")
});
static RESOLUTION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?P<resolution>2160p|1080p10|1080p|720p|576p|540p|480p|360p|1920x1080|1280x720|4k|uhd)")
        .expect("valid anime resolution regex")
});
static CODEC_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?P<codec>HEVC|H\.?265|x265|H\.?264|x264|AVC|AV1|VP9)\b")
        .expect("valid anime codec regex")
});
static AUDIO_CODEC_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?P<audio>AAC|FLAC|OPUS|EAC3|AC3|DTS|TrueHD)\b")
        .expect("valid anime audio codec regex")
});

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeBatchKind {
    Single,
    Range,
    SeasonPack,
    MultiSeasonPack,
    CompleteSeries,
    Movie,
    UnknownBatch,
}

impl Default for AnimeBatchKind {
    fn default() -> Self {
        Self::Single
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeParsedQuality {
    pub resolution: Option<String>,
    pub source: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub dual_audio: bool,
    pub multi_sub: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeParsedRelease {
    pub parser_version: String,
    pub original_title: String,
    pub normalized_title: Option<String>,
    pub series_title: Option<String>,
    pub alt_titles: Vec<String>,
    pub release_group: Option<String>,
    pub season_number: Option<i32>,
    pub episode_numbers: Vec<i32>,
    pub absolute_episode_numbers: Vec<i32>,
    pub episode_start_number: Option<i32>,
    pub episode_end_number: Option<i32>,
    pub episode_type: AnimeEpisodeType,
    pub batch_kind: AnimeBatchKind,
    pub version: Option<u8>,
    pub crc32: Option<String>,
    pub quality: AnimeParsedQuality,
    pub audio_languages: Vec<String>,
    pub subtitle_languages: Vec<String>,
    pub confidence: ReleaseConfidence,
    pub review_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AnimeAliasMatchKind {
    Fuzzy,
    Suffix,
    Prefix,
    Exact,
}

impl AnimeAliasMatchKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fuzzy => "fuzzy",
            Self::Suffix => "suffix",
            Self::Prefix => "prefix",
            Self::Exact => "exact",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeAliasEntry {
    pub display: String,
    pub normalized: String,
    pub tokens: Vec<String>,
    pub source: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeAliasTable {
    pub entries: Vec<AnimeAliasEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeAliasMatch {
    pub display: String,
    pub normalized: String,
    pub source: String,
    pub kind: AnimeAliasMatchKind,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateTarget {
    pub target_key: String,
    pub canonical_key: Option<String>,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub tvdb_episode_id: Option<String>,
    pub anidb_episode_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateScoringContext {
    pub graph_fingerprint: Option<String>,
    pub aliases: Vec<String>,
    pub targets: Vec<AnimeCandidateTarget>,
}

#[derive(Debug, Clone, Default)]
pub struct AnimeCandidateInput {
    pub title: String,
    pub source_kind: String,
    pub quality: Option<String>,
    pub size_bytes: Option<u64>,
    pub seeders: Option<u32>,
    pub cached_debrid: Option<bool>,
    pub rank: Option<u32>,
    pub source_score: Option<f64>,
    pub supported_routes: Vec<String>,
    pub default_route: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeReleaseFileInput {
    pub file_key: String,
    pub file_id: Option<String>,
    pub file_index: Option<i64>,
    pub path: String,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateTargetMatch {
    pub target_key: String,
    pub canonical_key: Option<String>,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub match_reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateScoreBreakdown {
    pub identity: f64,
    pub coverage: f64,
    pub quality: f64,
    pub route: f64,
    pub source: f64,
    pub total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateScore {
    pub resolver_version: String,
    pub parsed: AnimeParsedRelease,
    pub alias_matches: Vec<AnimeAliasMatch>,
    pub target_matches: Vec<AnimeCandidateTargetMatch>,
    pub outcome: AnimeMatchOutcome,
    pub confidence: ReleaseConfidence,
    pub score: f64,
    pub breakdown: AnimeCandidateScoreBreakdown,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeFileCoverageEntry {
    pub target_key: String,
    pub canonical_key: Option<String>,
    pub release_file_key: Option<String>,
    pub file_id: Option<String>,
    pub file_index: Option<i64>,
    pub path: Option<String>,
    pub coverage_kind: ReleaseCoverageKind,
    pub confidence: ReleaseConfidence,
    pub score: Option<f64>,
    pub reason: String,
    pub state: ReleaseCoverageState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeFileCoveragePlan {
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub release_kind: ReleaseKind,
    pub confidence: ReleaseConfidence,
    pub requires_file_list: bool,
    pub selected_file_keys: Vec<String>,
    pub entries: Vec<AnimeFileCoverageEntry>,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
}

impl AnimeCandidateTarget {
    pub fn from_graph_target(target: &AnimeGraphTarget) -> Self {
        Self {
            target_key: target.target_key.clone(),
            canonical_key: Some(target.canonical_key.clone()),
            title: target.title.clone(),
            season_number: target.season_number,
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            tvdb_episode_id: target.tvdb_episode_id.clone(),
            anidb_episode_id: target.anidb_episode_id.clone(),
        }
    }
}

impl AnimeCandidateScoringContext {
    pub fn from_graph(graph: &AnimeMetadataGraph) -> Self {
        Self {
            graph_fingerprint: Some(graph.fingerprint.clone()),
            aliases: graph.aliases.clone(),
            targets: graph
                .targets
                .iter()
                .map(AnimeCandidateTarget::from_graph_target)
                .collect(),
        }
    }
}

pub fn parse_anime_release_title(input: &str) -> AnimeParsedRelease {
    let original_title = input.trim().to_string();
    let normalized_input = normalize_fullwidth_digits(&original_title);
    let release_group = parse_anime_release_group(&normalized_input);
    let bracket_segments = extract_bracket_segments(&normalized_input);
    let classifier_hints = anime_classifier_hints(&original_title);
    let classifier_hint = classifier_hints.iter().max_by(|left, right| {
        left.parser_confidence
            .partial_cmp(&right.parser_confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let extracted_title = extract_anime_series_title(&normalized_input, &bracket_segments);
    let series_title = extracted_title
        .or_else(|| classifier_hint.map(|hint| cleanup_anime_title(&hint.title)))
        .filter(|title| !title.trim().is_empty());
    let normalized_title = series_title.as_deref().map(normalize_anime_title);
    let alt_titles = classifier_hint
        .map(|hint| {
            hint.alt_titles
                .iter()
                .map(|title| cleanup_anime_title(title))
                .filter(|title| !title.is_empty())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let sxxeyy = parse_sxxeyy_numbers(&normalized_input);
    let season_number = sxxeyy
        .as_ref()
        .map(|parsed| parsed.0)
        .or_else(|| parse_season_dash_episode(&normalized_input).map(|parsed| parsed.0))
        .or_else(|| parse_season_number(&normalized_input))
        .or_else(|| classifier_hint.and_then(|hint| hint.season));
    let episode_numbers = sxxeyy
        .as_ref()
        .map(|parsed| expand_episode_numbers(parsed.1, parsed.2.unwrap_or(parsed.1), 200))
        .unwrap_or_default();
    let mut absolute_episode_numbers = if episode_numbers.is_empty() {
        parse_absolute_episode_numbers(&normalized_input, &bracket_segments)
    } else {
        Vec::new()
    };
    if absolute_episode_numbers.is_empty()
        && episode_numbers.is_empty()
        && let Some(hint) = classifier_hint
        && let Some(absolute) = hint.absolute_episode
    {
        absolute_episode_numbers.push(absolute);
    }
    absolute_episode_numbers.sort_unstable();
    absolute_episode_numbers.dedup();

    let episode_start_number = episode_numbers
        .first()
        .copied()
        .or_else(|| absolute_episode_numbers.first().copied());
    let episode_end_number = episode_numbers
        .last()
        .copied()
        .or_else(|| absolute_episode_numbers.last().copied());
    let episode_type = parse_anime_episode_type(&normalized_input);
    let batch_kind = parse_anime_batch_kind(
        &normalized_input,
        episode_type,
        &episode_numbers,
        &absolute_episode_numbers,
    );
    let version = parse_anime_version(&normalized_input);
    let crc32 = parse_crc32(&normalized_input);
    let quality = parse_anime_quality(&normalized_input);
    let (audio_languages, subtitle_languages) = parse_anime_languages(&normalized_input);

    let mut review_reasons = Vec::new();
    if series_title.is_none() {
        review_reasons.push("missing_series_title".to_string());
    }
    if episode_numbers.is_empty()
        && absolute_episode_numbers.is_empty()
        && !matches!(episode_type, AnimeEpisodeType::Movie)
    {
        review_reasons.push("missing_episode_number".to_string());
    }
    if matches!(
        batch_kind,
        AnimeBatchKind::CompleteSeries
            | AnimeBatchKind::SeasonPack
            | AnimeBatchKind::MultiSeasonPack
            | AnimeBatchKind::UnknownBatch
    ) {
        review_reasons.push("file_list_required_for_pack".to_string());
    }
    review_reasons.sort();
    review_reasons.dedup();

    let confidence = if review_reasons
        .iter()
        .any(|reason| reason == "missing_series_title")
    {
        ReleaseConfidence::ReviewRequired
    } else if review_reasons.is_empty()
        && (!episode_numbers.is_empty() || !absolute_episode_numbers.is_empty())
    {
        ReleaseConfidence::High
    } else if !review_reasons.is_empty() {
        ReleaseConfidence::ReviewRequired
    } else {
        ReleaseConfidence::Medium
    };

    AnimeParsedRelease {
        parser_version: ANIME_PRE_DOWNLOAD_PARSER_VERSION.to_string(),
        original_title,
        normalized_title,
        series_title,
        alt_titles,
        release_group,
        season_number,
        episode_numbers,
        absolute_episode_numbers,
        episode_start_number,
        episode_end_number,
        episode_type,
        batch_kind,
        version,
        crc32,
        quality,
        audio_languages,
        subtitle_languages,
        confidence,
        review_reasons,
    }
}

#[derive(Debug, Clone)]
pub struct AnimeMetadataGraphInput {
    pub title: String,
    pub year: Option<i32>,
    pub seed_anilist_id: String,
    pub seed_season_number: i32,
    pub external_ids: ExternalIds,
    pub seasons: Vec<AnimeSeasonMapping>,
}

#[derive(Debug, Clone)]
pub struct AnimeSeasonMapping {
    pub season: AniListSeasonChainEntry,
    pub mapping: Option<AniZipMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeMetadataGraph {
    pub resolver_version: String,
    pub seed_anilist_id: String,
    pub root_anilist_id: String,
    pub title: String,
    pub year: Option<i32>,
    pub external_ids: ExternalIds,
    pub seasons: Vec<AnimeGraphSeason>,
    pub targets: Vec<AnimeGraphTarget>,
    pub aliases: Vec<String>,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeGraphSeason {
    pub season_number: i32,
    pub anilist_id: String,
    pub title: String,
    pub format: Option<String>,
    pub season_year: Option<i32>,
    pub start_year: Option<i32>,
    pub status: Option<String>,
    pub episodes: Option<i32>,
    pub next_airing_episode: Option<i32>,
    pub next_airing_at: Option<DateTime<Utc>>,
    pub confidence: f32,
    pub mapping_available: bool,
    pub mapped_episode_count: usize,
    pub target_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeGraphTarget {
    pub source: AnimeGraphTargetSource,
    pub target_key: String,
    pub canonical_key: String,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub air_date: Option<String>,
    pub air_time: Option<DateTime<Utc>>,
    pub anilist_season_id: String,
    pub anilist_status: Option<String>,
    pub tvdb_series_id: Option<String>,
    pub tvdb_episode_id: Option<String>,
    pub anidb_anime_id: Option<String>,
    pub anidb_episode_id: Option<String>,
    pub season: AnimeGraphSeasonRef,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeGraphTargetSource {
    AniZip,
    AniListNextAiring,
}

impl AnimeGraphTargetSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AniZip => "anizip",
            Self::AniListNextAiring => "anilist_next_airing",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeGraphSeasonRef {
    pub season_number: i32,
    pub anilist_id: String,
    pub title: String,
    pub format: Option<String>,
    pub season_year: Option<i32>,
    pub start_year: Option<i32>,
    pub status: Option<String>,
    pub episodes: Option<i32>,
    pub next_airing_episode: Option<i32>,
    pub next_airing_at: Option<i64>,
    pub confidence: f32,
}

impl AnimeMetadataGraph {
    pub fn next_airing_at(&self) -> Option<DateTime<Utc>> {
        self.seasons
            .iter()
            .filter_map(|season| season.next_airing_at)
            .min()
    }

    pub fn graph_json(&self) -> JsonValue {
        serde_json::to_value(self).unwrap_or_else(|_| json!({}))
    }

    pub fn aliases_json(&self) -> JsonValue {
        json!(self.aliases)
    }

    pub fn to_graph_snapshot_input(
        &self,
        subscription_id: Option<Uuid>,
        owner_id: impl Into<String>,
    ) -> NewAcquisitionAnimeGraphSnapshot {
        let seed_season = self
            .seasons
            .iter()
            .find(|season| season.anilist_id == self.seed_anilist_id)
            .or_else(|| self.seasons.first());
        NewAcquisitionAnimeGraphSnapshot {
            graph_snapshot_id: None,
            subscription_id,
            owner_id: owner_id.into(),
            media_type: MediaType::Anime,
            anilist_root_id: parse_i64(&self.root_anilist_id),
            anilist_season_id: seed_season.and_then(|season| parse_i64(&season.anilist_id)),
            anilist_status: seed_season.and_then(|season| season.status.clone()),
            anilist_next_airing_at: self.next_airing_at(),
            tvdb_series_id: parse_i64_option(primary_tvdb_series_id(&self.external_ids).as_ref()),
            anidb_anime_id: parse_i64_option(self.external_ids.anidb.as_ref()),
            fingerprint: self.fingerprint.clone(),
            graph: self.graph_json(),
            aliases: self.aliases_json(),
        }
    }

    pub fn to_new_acquisition_targets(
        &self,
        release_delay_seconds: i64,
        now: DateTime<Utc>,
    ) -> Vec<NewAcquisitionTarget> {
        self.targets
            .iter()
            .map(|target| NewAcquisitionTarget {
                target_key: Some(target.target_key.clone()),
                media_type: Some(MediaType::Anime),
                title: Some(target.title.clone()),
                season_number: target.season_number,
                episode_number: target.episode_number,
                absolute_episode_number: target.absolute_episode_number,
                air_date: target.air_date.clone(),
                air_time: target.air_time,
                metadata: Some(json!({
                    "source": target.source.as_str(),
                    "graphSource": "rr3_metadata_graph",
                    "resolverVersion": self.resolver_version,
                    "graphFingerprint": self.fingerprint,
                    "targetCanonicalKey": target.canonical_key,
                    "externalIds": self.external_ids,
                    "aliases": self.aliases,
                    "anilistRootId": self.root_anilist_id,
                    "anilistSeasonId": target.anilist_season_id,
                    "anilistSeason": target.season,
                    "anilistStatus": target.anilist_status,
                    "tvdbSeriesId": target.tvdb_series_id,
                    "tvdbEpisodeId": target.tvdb_episode_id,
                    "anidbAnimeId": target.anidb_anime_id,
                    "anidbEpisodeId": target.anidb_episode_id,
                    "raw": target.raw,
                })),
                state: Some(AcquisitionTargetState::Pending),
                next_search_after: Some(next_search_after_for_air_time(
                    target.air_time,
                    release_delay_seconds,
                    now,
                )),
            })
            .collect()
    }
}

impl From<&AniListSeasonChainEntry> for AnimeGraphSeasonRef {
    fn from(value: &AniListSeasonChainEntry) -> Self {
        Self {
            season_number: value.season_number,
            anilist_id: value.anilist_id.clone(),
            title: value.title.clone(),
            format: value.format.clone(),
            season_year: value.season_year,
            start_year: value.start_year,
            status: value.status.clone(),
            episodes: value.episodes,
            next_airing_episode: value.next_airing_episode,
            next_airing_at: value.next_airing_at,
            confidence: value.confidence,
        }
    }
}

pub fn build_anime_metadata_graph(input: AnimeMetadataGraphInput) -> AnimeMetadataGraph {
    let mut external_ids = input.external_ids.clone();
    if external_ids.anilist.is_none() && !input.seed_anilist_id.trim().is_empty() {
        external_ids.anilist = Some(input.seed_anilist_id.clone());
    }

    let season_inputs = normalized_season_inputs(&input);
    for season in &season_inputs {
        if let Some(mapping) = season.mapping.as_ref() {
            merge_external_ids(&mut external_ids, &mapping.ids);
        }
    }
    normalize_series_external_ids(&mut external_ids);

    let root_anilist_id = season_inputs
        .iter()
        .min_by_key(|item| {
            (
                item.season.season_number,
                item.season.anilist_id.parse::<i64>().unwrap_or(i64::MAX),
                item.season.anilist_id.clone(),
            )
        })
        .map(|item| item.season.anilist_id.clone())
        .unwrap_or_else(|| input.seed_anilist_id.clone());

    let mut aliases = BTreeSet::new();
    insert_alias(&mut aliases, &input.title);
    let mut targets_by_key: BTreeMap<String, AnimeGraphTarget> = BTreeMap::new();
    let mut mapped_counts_by_season = HashMap::<String, usize>::new();

    for season_input in &season_inputs {
        insert_alias(&mut aliases, &season_input.season.title);
        if let Some(mapping) = season_input.mapping.as_ref() {
            for title in mapping.titles.values() {
                insert_alias(&mut aliases, title);
            }
            for episode in &mapping.episodes {
                let Some(target) = graph_target_from_anizip(
                    &input,
                    &external_ids,
                    &season_input.season,
                    mapping,
                    episode,
                ) else {
                    continue;
                };
                *mapped_counts_by_season
                    .entry(season_input.season.anilist_id.clone())
                    .or_default() += 1;
                insert_best_target(&mut targets_by_key, target);
            }
        }

        if let Some(target) =
            graph_target_from_next_airing(&input, &external_ids, &season_input.season)
        {
            targets_by_key
                .entry(target.target_key.clone())
                .or_insert(target);
        }
    }

    let mut target_counts_by_season = HashMap::<String, usize>::new();
    for target in targets_by_key.values() {
        *target_counts_by_season
            .entry(target.anilist_season_id.clone())
            .or_default() += 1;
    }

    let mut seasons = season_inputs
        .iter()
        .map(|item| AnimeGraphSeason {
            season_number: item.season.season_number,
            anilist_id: item.season.anilist_id.clone(),
            title: item.season.title.clone(),
            format: item.season.format.clone(),
            season_year: item.season.season_year,
            start_year: item.season.start_year,
            status: item.season.status.clone(),
            episodes: item.season.episodes,
            next_airing_episode: item.season.next_airing_episode,
            next_airing_at: item
                .season
                .next_airing_at
                .and_then(|timestamp| Utc.timestamp_opt(timestamp, 0).single()),
            confidence: item.season.confidence,
            mapping_available: item.mapping.is_some(),
            mapped_episode_count: mapped_counts_by_season
                .get(&item.season.anilist_id)
                .copied()
                .unwrap_or_default(),
            target_count: target_counts_by_season
                .get(&item.season.anilist_id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    seasons.sort_by_key(|season| {
        (
            season.season_number,
            season.anilist_id.parse::<i64>().unwrap_or(i64::MAX),
            season.anilist_id.clone(),
        )
    });

    let targets = targets_by_key.into_values().collect::<Vec<_>>();
    let aliases = aliases.into_iter().collect::<Vec<_>>();
    let fingerprint = graph_fingerprint(
        &input.seed_anilist_id,
        &root_anilist_id,
        &external_ids,
        &targets,
    );

    AnimeMetadataGraph {
        resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        seed_anilist_id: input.seed_anilist_id,
        root_anilist_id,
        title: input.title,
        year: input.year,
        external_ids,
        seasons,
        targets,
        aliases,
        fingerprint,
    }
}

pub fn build_anime_alias_table(context: &AnimeCandidateScoringContext) -> AnimeAliasTable {
    let mut entries_by_key = BTreeMap::<String, AnimeAliasEntry>::new();
    for alias in &context.aliases {
        insert_alias_entry(&mut entries_by_key, alias, "graph_alias", 50);
    }
    for target in &context.targets {
        insert_alias_entry(&mut entries_by_key, &target.title, "target_title", 10);
    }
    AnimeAliasTable {
        entries: entries_by_key.into_values().collect(),
    }
}

pub fn score_anime_candidate_for_graph(
    graph: &AnimeMetadataGraph,
    candidate: &AnimeCandidateInput,
) -> AnimeCandidateScore {
    score_anime_candidate(&AnimeCandidateScoringContext::from_graph(graph), candidate)
}

pub fn score_anime_candidate(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
) -> AnimeCandidateScore {
    let parsed = parse_anime_release_title(&candidate.title);
    let alias_table = build_anime_alias_table(context);
    let alias_matches = match_anime_aliases(&alias_table, &parsed);
    let target_matches = match_candidate_targets(context, &parsed);

    let mut review_reasons = parsed.review_reasons.clone();
    let mut rejection_reasons = Vec::new();

    if alias_matches.is_empty() {
        rejection_reasons.push("no_graph_alias_match".to_string());
    } else if alias_match_is_ambiguous(&alias_matches) {
        review_reasons.push("ambiguous_alias_match".to_string());
    }

    let has_episode_evidence =
        !parsed.episode_numbers.is_empty() || !parsed.absolute_episode_numbers.is_empty();
    if target_matches.is_empty() {
        if has_episode_evidence {
            rejection_reasons.push("no_graph_target_coverage".to_string());
        } else if parsed.episode_type != AnimeEpisodeType::Movie {
            review_reasons.push("missing_graph_target_coverage".to_string());
        }
    }

    if parsed.confidence == ReleaseConfidence::ReviewRequired {
        review_reasons.extend(parsed.review_reasons.iter().cloned());
    }

    if matches!(
        parsed.batch_kind,
        AnimeBatchKind::CompleteSeries
            | AnimeBatchKind::SeasonPack
            | AnimeBatchKind::MultiSeasonPack
    ) {
        review_reasons.push("file_list_required_for_pack".to_string());
    }

    review_reasons.sort();
    review_reasons.dedup();
    rejection_reasons.sort();
    rejection_reasons.dedup();

    let breakdown =
        anime_candidate_score_breakdown(candidate, &parsed, &alias_matches, &target_matches);
    let outcome = if !rejection_reasons.is_empty() {
        AnimeMatchOutcome::Rejected
    } else if !review_reasons.is_empty() {
        AnimeMatchOutcome::Deferred
    } else {
        AnimeMatchOutcome::Planned
    };
    let confidence = match outcome {
        AnimeMatchOutcome::Rejected => ReleaseConfidence::Low,
        AnimeMatchOutcome::Deferred => ReleaseConfidence::ReviewRequired,
        _ if alias_matches
            .first()
            .is_some_and(|item| item.kind == AnimeAliasMatchKind::Exact)
            && !target_matches.is_empty() =>
        {
            ReleaseConfidence::High
        }
        _ if !target_matches.is_empty() => ReleaseConfidence::Medium,
        _ => ReleaseConfidence::Low,
    };

    AnimeCandidateScore {
        resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        parsed,
        alias_matches,
        target_matches,
        outcome,
        confidence,
        score: breakdown.total,
        breakdown,
        review_reasons,
        rejection_reasons,
    }
}

pub fn plan_anime_file_coverage(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
) -> AnimeFileCoveragePlan {
    let candidate_score = score_anime_candidate(context, candidate);
    let release_kind = anime_release_kind(&candidate_score.parsed);
    let coverage_kind = anime_coverage_kind(release_kind);
    let mut review_reasons = candidate_score.review_reasons.clone();
    let mut rejection_reasons = candidate_score.rejection_reasons.clone();
    let pack_requires_file_coverage = matches!(
        release_kind,
        ReleaseKind::SeasonPack | ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack
    );
    if !files.is_empty() && pack_requires_file_coverage {
        review_reasons.retain(|reason| {
            !matches!(
                reason.as_str(),
                "file_list_required_for_pack"
                    | "missing_episode_number"
                    | "missing_graph_target_coverage"
            )
        });
    } else if !files.is_empty() {
        review_reasons.retain(|reason| reason != "file_list_required_for_pack");
    }

    if !rejection_reasons.is_empty() {
        return anime_file_coverage_plan(
            release_kind,
            ReleaseConfidence::Low,
            false,
            Vec::new(),
            review_reasons,
            rejection_reasons,
        );
    }

    if matches!(
        release_kind,
        ReleaseKind::Single | ReleaseKind::MultiEpisode
    ) {
        let entries = candidate_score
            .target_matches
            .iter()
            .map(|target| AnimeFileCoverageEntry {
                target_key: target.target_key.clone(),
                canonical_key: target.canonical_key.clone(),
                release_file_key: None,
                file_id: None,
                file_index: None,
                path: None,
                coverage_kind,
                confidence: candidate_score.confidence,
                score: Some(target.score),
                reason: target.match_reason.clone(),
                state: ReleaseCoverageState::Planned,
            })
            .collect::<Vec<_>>();
        if entries.is_empty() && candidate_score.confidence != ReleaseConfidence::ReviewRequired {
            review_reasons.push("missing_graph_target_coverage".to_string());
        }
        let confidence = if review_reasons.is_empty() && !entries.is_empty() {
            candidate_score.confidence
        } else {
            ReleaseConfidence::ReviewRequired
        };
        return anime_file_coverage_plan(
            release_kind,
            confidence,
            false,
            entries,
            review_reasons,
            rejection_reasons,
        );
    }

    if files.is_empty() {
        review_reasons.push("file_list_required".to_string());
        if matches!(
            release_kind,
            ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack
        ) {
            review_reasons.push("file_selection_required".to_string());
        }
        return anime_file_coverage_plan(
            release_kind,
            ReleaseConfidence::ReviewRequired,
            true,
            Vec::new(),
            review_reasons,
            rejection_reasons,
        );
    }

    let expected_targets =
        expected_anime_pack_targets(context, &candidate_score.parsed, release_kind);
    let mut entries = Vec::new();
    let mut selected_file_keys = BTreeSet::new();
    let mut covered_targets = BTreeSet::new();
    let mut duplicate_targets = BTreeSet::new();
    let mut unmapped_media_files = Vec::new();
    let mut unselectable_wanted_files = Vec::new();
    let mut has_media_files = false;

    for file in files.iter().filter(|file| is_anime_media_file(&file.path)) {
        if is_anime_sample_or_extra_file(&file.path) {
            continue;
        }
        has_media_files = true;
        let file_candidate = AnimeCandidateInput {
            title: file.path.clone(),
            source_kind: candidate.source_kind.clone(),
            quality: candidate.quality.clone(),
            size_bytes: file.size_bytes.and_then(|value| u64::try_from(value).ok()),
            seeders: candidate.seeders,
            cached_debrid: candidate.cached_debrid,
            rank: candidate.rank,
            source_score: candidate.source_score,
            supported_routes: candidate.supported_routes.clone(),
            default_route: candidate.default_route.clone(),
        };
        let file_score = score_anime_candidate(context, &file_candidate);
        if file_score.outcome == AnimeMatchOutcome::Rejected
            || file_score.confidence == ReleaseConfidence::ReviewRequired
            || file_score.target_matches.is_empty()
        {
            unmapped_media_files.push(file.path.clone());
            continue;
        }
        for target in file_score.target_matches {
            if !expected_targets.is_empty() && !expected_targets.contains(&target.target_key) {
                unmapped_media_files.push(file.path.clone());
                continue;
            }
            if !covered_targets.insert(target.target_key.clone()) {
                duplicate_targets.insert(target.target_key.clone());
                continue;
            }
            if !file.selectable {
                unselectable_wanted_files.push(file.path.clone());
                continue;
            }
            selected_file_keys.insert(file.file_key.clone());
            entries.push(AnimeFileCoverageEntry {
                target_key: target.target_key,
                canonical_key: target.canonical_key,
                release_file_key: Some(file.file_key.clone()),
                file_id: file.file_id.clone(),
                file_index: file.file_index,
                path: Some(file.path.clone()),
                coverage_kind,
                confidence: file_score.confidence,
                score: Some(target.score),
                reason: target.match_reason,
                state: ReleaseCoverageState::Planned,
            });
        }
    }

    if !has_media_files {
        review_reasons.push("no_media_files".to_string());
    }
    if !unmapped_media_files.is_empty() {
        review_reasons.push("unmapped_media_files".to_string());
    }
    if !duplicate_targets.is_empty() {
        review_reasons.push("duplicate_target_file_match".to_string());
    }
    if !unselectable_wanted_files.is_empty() {
        review_reasons.push("wanted_file_not_selectable".to_string());
    }
    if entries.is_empty() {
        review_reasons.push("file_list_does_not_cover_wanted_targets".to_string());
    }
    let covered = entries
        .iter()
        .map(|entry| entry.target_key.clone())
        .collect::<BTreeSet<_>>();
    if !expected_targets.is_empty()
        && !expected_targets
            .iter()
            .all(|target| covered.contains(target))
    {
        review_reasons.push("file_list_does_not_cover_expected_targets".to_string());
    }

    review_reasons.sort();
    review_reasons.dedup();
    rejection_reasons.sort();
    rejection_reasons.dedup();

    let confidence =
        if rejection_reasons.is_empty() && review_reasons.is_empty() && !entries.is_empty() {
            ReleaseConfidence::High
        } else {
            ReleaseConfidence::ReviewRequired
        };
    let mut plan = anime_file_coverage_plan(
        release_kind,
        confidence,
        false,
        entries,
        review_reasons,
        rejection_reasons,
    );
    plan.selected_file_keys = selected_file_keys.into_iter().collect();
    plan
}

pub fn merge_external_ids(target: &mut ExternalIds, source: &ExternalIds) {
    if target.imdb.is_none() {
        target.imdb = source.imdb.clone();
    }
    if target.tmdb.is_none() {
        target.tmdb = source.tmdb.clone();
    }
    if target.tvdb.is_none() {
        target.tvdb = source.tvdb.clone();
    }
    if target.tvdb_series.is_none() {
        target.tvdb_series = source.tvdb_series.clone();
    }
    if target.tvdb_movie.is_none() {
        target.tvdb_movie = source.tvdb_movie.clone();
    }
    if target.anilist.is_none() {
        target.anilist = source.anilist.clone();
    }
    if target.anidb.is_none() {
        target.anidb = source.anidb.clone();
    }
    if target.mal.is_none() {
        target.mal = source.mal.clone();
    }
    if target.kitsu.is_none() {
        target.kitsu = source.kitsu.clone();
    }
}

pub fn infer_anizip_season_number(mapping: &AniZipMapping) -> Option<i32> {
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for season in mapping
        .episodes
        .iter()
        .filter_map(|episode| episode.season_number)
        .filter(|season| *season > 0)
    {
        *counts.entry(season).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(season, _)| season)
}

fn normalized_season_inputs(input: &AnimeMetadataGraphInput) -> Vec<AnimeSeasonMapping> {
    let mut seasons = input.seasons.clone();
    if seasons.is_empty() {
        seasons.push(AnimeSeasonMapping {
            season: AniListSeasonChainEntry {
                season_number: input.seed_season_number.max(1),
                anilist_id: input.seed_anilist_id.clone(),
                title: input.title.clone(),
                format: None,
                season_year: input.year,
                start_year: input.year,
                status: None,
                episodes: None,
                next_airing_episode: None,
                next_airing_at: None,
                confidence: 1.0,
            },
            mapping: None,
        });
    }
    seasons.sort_by_key(|item| {
        (
            item.season.season_number,
            item.season.anilist_id.parse::<i64>().unwrap_or(i64::MAX),
            item.season.anilist_id.clone(),
        )
    });
    let mut seen = HashSet::new();
    seasons
        .into_iter()
        .filter(|item| seen.insert(item.season.anilist_id.clone()))
        .collect()
}

fn graph_target_from_anizip(
    input: &AnimeMetadataGraphInput,
    external_ids: &ExternalIds,
    season: &AniListSeasonChainEntry,
    mapping: &AniZipMapping,
    episode: &crate::library::AniZipEpisodeRecord,
) -> Option<AnimeGraphTarget> {
    let season_number = episode.season_number.or(Some(season.season_number));
    let episode_number = episode.episode_number.filter(|episode| *episode > 0);
    let absolute_episode_number = episode
        .absolute_episode_number
        .filter(|episode| *episode > 0);
    let target_key = graph_target_key(season_number, episode_number, absolute_episode_number)?;
    let air_date = extract_air_date(&episode.raw);
    let air_time = air_date
        .as_deref()
        .and_then(|value| parse_air_time(value).or_else(|| parse_air_date(value)))
        .or_else(|| extract_air_timestamp(&episode.raw));
    let title = episode.title.clone().unwrap_or_else(|| {
        format!(
            "{} {}",
            input.title,
            display_target_key(season_number, episode_number, absolute_episode_number)
        )
    });
    let tvdb_series_id = primary_tvdb_series_id(external_ids).or_else(|| {
        mapping
            .ids
            .tvdb_series
            .clone()
            .or_else(|| mapping.ids.tvdb.clone())
    });
    let anidb_anime_id = mapping
        .ids
        .anidb
        .clone()
        .or_else(|| external_ids.anidb.clone());
    let canonical_key = canonical_coverage_key(
        &season.anilist_id,
        tvdb_series_id.as_deref(),
        season_number,
        episode_number,
        absolute_episode_number,
        episode.tvdb_id.as_deref(),
        episode.anidb_eid.as_deref(),
    );

    Some(AnimeGraphTarget {
        source: AnimeGraphTargetSource::AniZip,
        target_key,
        canonical_key,
        title,
        season_number,
        episode_number,
        absolute_episode_number,
        air_date,
        air_time,
        anilist_season_id: season.anilist_id.clone(),
        anilist_status: season.status.clone(),
        tvdb_series_id,
        tvdb_episode_id: episode.tvdb_id.clone(),
        anidb_anime_id,
        anidb_episode_id: episode.anidb_eid.clone(),
        season: AnimeGraphSeasonRef::from(season),
        raw: episode.raw.clone(),
    })
}

fn graph_target_from_next_airing(
    input: &AnimeMetadataGraphInput,
    external_ids: &ExternalIds,
    season: &AniListSeasonChainEntry,
) -> Option<AnimeGraphTarget> {
    let episode_number = season.next_airing_episode.filter(|episode| *episode > 0)?;
    let season_number = (season.season_number > 0).then_some(season.season_number);
    let target_key = graph_target_key(season_number, Some(episode_number), None)?;
    let air_time = season
        .next_airing_at
        .and_then(|timestamp| Utc.timestamp_opt(timestamp, 0).single());
    let air_date = air_time.map(|value| value.date_naive().to_string());
    let tvdb_series_id = primary_tvdb_series_id(external_ids);
    let canonical_key = canonical_coverage_key(
        &season.anilist_id,
        tvdb_series_id.as_deref(),
        season_number,
        Some(episode_number),
        None,
        None,
        None,
    );

    Some(AnimeGraphTarget {
        source: AnimeGraphTargetSource::AniListNextAiring,
        target_key,
        canonical_key,
        title: format!(
            "{} {}",
            input.title,
            display_target_key(season_number, Some(episode_number), None)
        ),
        season_number,
        episode_number: Some(episode_number),
        absolute_episode_number: None,
        air_date,
        air_time,
        anilist_season_id: season.anilist_id.clone(),
        anilist_status: season.status.clone(),
        tvdb_series_id,
        tvdb_episode_id: None,
        anidb_anime_id: external_ids.anidb.clone(),
        anidb_episode_id: None,
        season: AnimeGraphSeasonRef::from(season),
        raw: json!({
            "source": "anilist_next_airing",
            "nextAiringEpisode": episode_number,
            "nextAiringAt": season.next_airing_at,
        }),
    })
}

fn insert_best_target(
    targets_by_key: &mut BTreeMap<String, AnimeGraphTarget>,
    target: AnimeGraphTarget,
) {
    match targets_by_key.get(&target.target_key) {
        Some(existing) if target_evidence_score(existing) >= target_evidence_score(&target) => {}
        _ => {
            targets_by_key.insert(target.target_key.clone(), target);
        }
    }
}

fn target_evidence_score(target: &AnimeGraphTarget) -> i32 {
    let mut score = 0;
    if target.tvdb_series_id.is_some() {
        score += 2;
    }
    if target.tvdb_episode_id.is_some() {
        score += 4;
    }
    if target.anidb_anime_id.is_some() {
        score += 2;
    }
    if target.anidb_episode_id.is_some() {
        score += 5;
    }
    if target.absolute_episode_number.is_some() {
        score += 1;
    }
    if target.air_time.is_some() {
        score += 1;
    }
    if target.source == AnimeGraphTargetSource::AniZip {
        score += 2;
    }
    score
}

fn insert_alias_entry(
    entries_by_key: &mut BTreeMap<String, AnimeAliasEntry>,
    display: &str,
    source: &str,
    priority: i32,
) {
    let cleaned = cleanup_anime_title(display);
    let normalized = normalize_anime_alias(&cleaned);
    let tokens = anime_alias_tokens(&cleaned);
    if normalized.is_empty() || tokens.is_empty() || is_metadata_segment(&cleaned) {
        return;
    }
    let key = format!("{normalized}:{source}");
    let entry = AnimeAliasEntry {
        display: cleaned,
        normalized,
        tokens,
        source: source.to_string(),
        priority,
    };
    match entries_by_key.get(&key) {
        Some(existing) if existing.priority >= entry.priority => {}
        _ => {
            entries_by_key.insert(key, entry);
        }
    }
}

fn match_anime_aliases(
    alias_table: &AnimeAliasTable,
    parsed: &AnimeParsedRelease,
) -> Vec<AnimeAliasMatch> {
    let mut titles = BTreeSet::new();
    if let Some(title) = parsed.series_title.as_deref() {
        titles.insert(cleanup_anime_title(title));
    }
    for title in &parsed.alt_titles {
        titles.insert(cleanup_anime_title(title));
    }
    let mut matches = Vec::new();
    for title in titles {
        let title_normalized = normalize_anime_alias(&title);
        let title_tokens = anime_alias_tokens(&title);
        if title_normalized.is_empty() || title_tokens.is_empty() {
            continue;
        }
        for alias in &alias_table.entries {
            if let Some((kind, score)) = score_alias_match(&title_normalized, &title_tokens, alias)
            {
                matches.push(AnimeAliasMatch {
                    display: alias.display.clone(),
                    normalized: alias.normalized.clone(),
                    source: alias.source.clone(),
                    kind,
                    score: score + f64::from(alias.priority) / 1000.0,
                });
            }
        }
    }
    matches.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.kind.cmp(&left.kind))
            .then_with(|| left.normalized.cmp(&right.normalized))
    });
    let mut seen = BTreeSet::new();
    matches
        .into_iter()
        .filter(|item| seen.insert((item.normalized.clone(), item.kind)))
        .take(5)
        .collect()
}

fn score_alias_match(
    title_normalized: &str,
    title_tokens: &[String],
    alias: &AnimeAliasEntry,
) -> Option<(AnimeAliasMatchKind, f64)> {
    if title_normalized == alias.normalized {
        return Some((AnimeAliasMatchKind::Exact, 100.0));
    }
    if title_tokens == alias.tokens.as_slice() {
        return Some((AnimeAliasMatchKind::Exact, 98.0));
    }
    if title_tokens.len() > alias.tokens.len()
        && title_tokens.starts_with(alias.tokens.as_slice())
        && alias_remainder_is_release_context(&title_tokens[alias.tokens.len()..])
    {
        return Some((AnimeAliasMatchKind::Prefix, 86.0));
    }
    if alias.tokens.len() > title_tokens.len()
        && alias.tokens.starts_with(title_tokens)
        && alias_remainder_is_release_context(&alias.tokens[title_tokens.len()..])
    {
        return Some((AnimeAliasMatchKind::Suffix, 76.0));
    }
    let overlap = token_overlap_score(title_tokens, &alias.tokens)?;
    (overlap >= 0.72).then_some((AnimeAliasMatchKind::Fuzzy, overlap * 70.0))
}

fn alias_match_is_ambiguous(matches: &[AnimeAliasMatch]) -> bool {
    let Some(best) = matches.first() else {
        return false;
    };
    if best.kind == AnimeAliasMatchKind::Exact {
        return false;
    }
    matches.iter().skip(1).any(|candidate| {
        candidate.normalized != best.normalized
            && (best.score - candidate.score).abs() <= 8.0
            && candidate.kind >= AnimeAliasMatchKind::Suffix
    })
}

fn match_candidate_targets(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
) -> Vec<AnimeCandidateTargetMatch> {
    let mut matches = Vec::new();
    let episode_numbers = parsed
        .episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let absolute_numbers = parsed
        .absolute_episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for target in &context.targets {
        if let (Some(parsed_season), Some(target_season), Some(target_episode)) = (
            parsed.season_number,
            target.season_number,
            target.episode_number,
        ) && parsed_season == target_season
            && episode_numbers.contains(&target_episode)
        {
            matches.push(candidate_target_match(
                target,
                "season_episode",
                100.0 + target_identity_bonus(target),
            ));
            continue;
        }
        if let Some(target_absolute) = target.absolute_episode_number
            && absolute_numbers.contains(&target_absolute)
        {
            matches.push(candidate_target_match(
                target,
                "absolute_episode",
                92.0 + target_identity_bonus(target),
            ));
            continue;
        }
        if parsed.season_number.is_none()
            && let Some(target_episode) = target.episode_number
            && episode_numbers.contains(&target_episode)
            && context.targets.len() == 1
        {
            matches.push(candidate_target_match(
                target,
                "single_target_episode",
                70.0,
            ));
        }
    }
    matches.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.target_key.cmp(&right.target_key))
    });
    matches.dedup_by(|left, right| left.target_key == right.target_key);
    matches
}

fn candidate_target_match(
    target: &AnimeCandidateTarget,
    reason: &str,
    score: f64,
) -> AnimeCandidateTargetMatch {
    AnimeCandidateTargetMatch {
        target_key: target.target_key.clone(),
        canonical_key: target.canonical_key.clone(),
        title: target.title.clone(),
        season_number: target.season_number,
        episode_number: target.episode_number,
        absolute_episode_number: target.absolute_episode_number,
        match_reason: reason.to_string(),
        score,
    }
}

fn target_identity_bonus(target: &AnimeCandidateTarget) -> f64 {
    let mut score = 0.0;
    if target.tvdb_episode_id.is_some() {
        score += 8.0;
    }
    if target.anidb_episode_id.is_some() {
        score += 10.0;
    }
    score
}

fn anime_candidate_score_breakdown(
    candidate: &AnimeCandidateInput,
    parsed: &AnimeParsedRelease,
    alias_matches: &[AnimeAliasMatch],
    target_matches: &[AnimeCandidateTargetMatch],
) -> AnimeCandidateScoreBreakdown {
    let identity = alias_matches
        .first()
        .map(|item| item.score)
        .unwrap_or_default();
    let coverage = target_matches
        .iter()
        .map(|item| item.score)
        .sum::<f64>()
        .min(250.0);
    let quality = anime_quality_score(candidate.quality.as_deref(), parsed);
    let route = anime_route_score(candidate);
    let source = anime_source_score(candidate);
    let total = identity + coverage + quality + route + source;
    AnimeCandidateScoreBreakdown {
        identity,
        coverage,
        quality,
        route,
        source,
        total,
    }
}

fn anime_quality_score(candidate_quality: Option<&str>, parsed: &AnimeParsedRelease) -> f64 {
    let mut score = 0.0;
    let quality_text = candidate_quality
        .map(str::to_string)
        .or_else(|| parsed.quality.resolution.clone())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if quality_text.contains("2160") || quality_text.contains("4k") {
        score += 45.0;
    } else if quality_text.contains("1080") {
        score += 35.0;
    } else if quality_text.contains("720") {
        score += 20.0;
    } else if quality_text.contains("480") {
        score += 8.0;
    }
    match parsed.quality.source.as_deref() {
        Some("web_dl") => score += 10.0,
        Some("blu_ray") => score += 12.0,
        Some("web_rip") => score += 6.0,
        Some("hdtv") => score += 4.0,
        _ => {}
    }
    if parsed.version.unwrap_or(1) > 1 {
        score += 3.0;
    }
    score
}

fn anime_route_score(candidate: &AnimeCandidateInput) -> f64 {
    let mut score = 0.0;
    if candidate
        .supported_routes
        .iter()
        .any(|route| route.eq_ignore_ascii_case("acquisition.debrid.default"))
    {
        score += 12.0;
    }
    if candidate
        .supported_routes
        .iter()
        .any(|route| route.eq_ignore_ascii_case("acquisition.torrent.default"))
    {
        score += 6.0;
    }
    if candidate.cached_debrid == Some(true) {
        score += 18.0;
    }
    score
}

fn anime_source_score(candidate: &AnimeCandidateInput) -> f64 {
    let mut score = candidate.source_score.unwrap_or_default() * 20.0;
    if let Some(seeders) = candidate.seeders {
        score += f64::from(seeders.min(200)) / 10.0;
    }
    if let Some(rank) = candidate.rank {
        score += f64::from(100_u32.saturating_sub(rank.min(100))) / 10.0;
    }
    if matches!(candidate.source_kind.as_str(), "magnet" | "torrent") {
        score += 3.0;
    }
    score
}

fn alias_remainder_is_release_context(tokens: &[String]) -> bool {
    !tokens.is_empty()
        && tokens.iter().all(|token| {
            matches!(
                token.as_str(),
                "s" | "season"
                    | "part"
                    | "cour"
                    | "ova"
                    | "oad"
                    | "ona"
                    | "special"
                    | "movie"
                    | "the"
                    | "final"
                    | "series"
                    | "batch"
                    | "complete"
                    | "collection"
            ) || token.chars().all(|ch| ch.is_ascii_digit())
                || token.strip_prefix('s').is_some_and(|suffix| {
                    !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())
                })
                || roman_numeral_value(token).is_some()
        })
}

fn token_overlap_score(left: &[String], right: &[String]) -> Option<f64> {
    if left.is_empty() || right.is_empty() {
        return None;
    }
    if left.len().min(right.len()) < 2 {
        return None;
    }
    let left = left.iter().collect::<BTreeSet<_>>();
    let right = right.iter().collect::<BTreeSet<_>>();
    let intersection = left.intersection(&right).count() as f64;
    let denominator = left.len().max(right.len()) as f64;
    Some(intersection / denominator)
}

fn anime_release_kind(parsed: &AnimeParsedRelease) -> ReleaseKind {
    match parsed.batch_kind {
        AnimeBatchKind::Single | AnimeBatchKind::Movie => {
            if parsed.episode_numbers.len() > 1 || parsed.absolute_episode_numbers.len() > 1 {
                ReleaseKind::MultiEpisode
            } else {
                ReleaseKind::Single
            }
        }
        AnimeBatchKind::Range => ReleaseKind::MultiEpisode,
        AnimeBatchKind::SeasonPack => ReleaseKind::SeasonPack,
        AnimeBatchKind::MultiSeasonPack => ReleaseKind::MultiSeasonPack,
        AnimeBatchKind::CompleteSeries => ReleaseKind::SeriesPack,
        AnimeBatchKind::UnknownBatch => ReleaseKind::Unknown,
    }
}

fn anime_coverage_kind(release_kind: ReleaseKind) -> ReleaseCoverageKind {
    match release_kind {
        ReleaseKind::Single => ReleaseCoverageKind::SingleEpisode,
        ReleaseKind::MultiEpisode => ReleaseCoverageKind::MultiEpisodeRange,
        ReleaseKind::SeasonPack => ReleaseCoverageKind::SeasonPack,
        ReleaseKind::MultiSeasonPack => ReleaseCoverageKind::MultiSeasonPack,
        ReleaseKind::SeriesPack => ReleaseCoverageKind::SeriesPack,
        ReleaseKind::Unknown => ReleaseCoverageKind::ManualOverride,
    }
}

fn anime_file_coverage_plan(
    release_kind: ReleaseKind,
    confidence: ReleaseConfidence,
    requires_file_list: bool,
    entries: Vec<AnimeFileCoverageEntry>,
    mut review_reasons: Vec<String>,
    mut rejection_reasons: Vec<String>,
) -> AnimeFileCoveragePlan {
    review_reasons.sort();
    review_reasons.dedup();
    rejection_reasons.sort();
    rejection_reasons.dedup();
    let selected_file_keys = entries
        .iter()
        .filter_map(|entry| entry.release_file_key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    AnimeFileCoveragePlan {
        resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
        resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        release_kind,
        confidence,
        requires_file_list,
        selected_file_keys,
        entries,
        review_reasons,
        rejection_reasons,
    }
}

fn expected_anime_pack_targets(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    release_kind: ReleaseKind,
) -> BTreeSet<String> {
    let mut expected = BTreeSet::new();
    match release_kind {
        ReleaseKind::SeasonPack => {
            if let Some(season) = parsed.season_number {
                expected.extend(
                    context
                        .targets
                        .iter()
                        .filter(|target| target.season_number == Some(season))
                        .map(|target| target.target_key.clone()),
                );
            } else {
                let seasons = context
                    .targets
                    .iter()
                    .filter_map(|target| target.season_number)
                    .collect::<BTreeSet<_>>();
                if seasons.len() == 1 {
                    expected.extend(
                        context
                            .targets
                            .iter()
                            .map(|target| target.target_key.clone()),
                    );
                }
            }
        }
        ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack => {
            expected.extend(
                context
                    .targets
                    .iter()
                    .map(|target| target.target_key.clone()),
            );
        }
        _ => {}
    }
    expected
}

fn is_anime_media_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    [
        ".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv", ".ts", ".m2ts", ".webm",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
}

fn is_anime_sample_or_extra_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.contains("/sample")
        || lower.contains("sample.")
        || lower.contains("/extras/")
        || lower.contains("/extra/")
        || lower.contains(" creditless ")
        || lower.contains(" ncop")
        || lower.contains(" nced")
}

fn graph_target_key(
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
) -> Option<String> {
    if let (Some(season), Some(episode)) = (season_number, episode_number)
        && season > 0
        && episode > 0
    {
        return Some(format!("S{season:02}E{episode:02}"));
    }
    absolute_episode_number
        .filter(|episode| *episode > 0)
        .map(|episode| format!("A{episode:04}"))
}

fn display_target_key(
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
) -> String {
    graph_target_key(season_number, episode_number, absolute_episode_number)
        .unwrap_or_else(|| "episode".to_string())
}

fn canonical_coverage_key(
    anilist_season_id: &str,
    tvdb_series_id: Option<&str>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    tvdb_episode_id: Option<&str>,
    anidb_episode_id: Option<&str>,
) -> String {
    if let Some(tvdb_episode_id) = tvdb_episode_id.filter(|value| !value.trim().is_empty()) {
        return format!("tvdb_episode:{}", tvdb_episode_id.trim());
    }
    if let Some(anidb_episode_id) = anidb_episode_id.filter(|value| !value.trim().is_empty()) {
        return format!("anidb_episode:{}", anidb_episode_id.trim());
    }
    if let (Some(tvdb_series_id), Some(season), Some(episode)) =
        (tvdb_series_id, season_number, episode_number)
        && !tvdb_series_id.trim().is_empty()
    {
        return format!("tvdb:{}:S{season:02}E{episode:02}", tvdb_series_id.trim());
    }
    if let (Some(season), Some(episode)) = (season_number, episode_number) {
        return format!("anilist:{anilist_season_id}:S{season:02}E{episode:02}");
    }
    if let Some(absolute) = absolute_episode_number {
        return format!("anilist:{anilist_season_id}:A{absolute:04}");
    }
    format!("anilist:{anilist_season_id}:unknown")
}

fn graph_fingerprint(
    seed_anilist_id: &str,
    root_anilist_id: &str,
    external_ids: &ExternalIds,
    targets: &[AnimeGraphTarget],
) -> String {
    let mut material = vec![
        ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
        seed_anilist_id.to_string(),
        root_anilist_id.to_string(),
        external_ids.imdb.clone().unwrap_or_default(),
        external_ids.tmdb.clone().unwrap_or_default(),
        external_ids.tvdb.clone().unwrap_or_default(),
        external_ids.tvdb_series.clone().unwrap_or_default(),
        external_ids.anilist.clone().unwrap_or_default(),
        external_ids.anidb.clone().unwrap_or_default(),
        external_ids.mal.clone().unwrap_or_default(),
        external_ids.kitsu.clone().unwrap_or_default(),
    ];
    material.extend(targets.iter().map(|target| {
        format!(
            "{}:{}:{}:{}:{}",
            target.target_key,
            target.canonical_key,
            target.anilist_season_id,
            target.tvdb_episode_id.clone().unwrap_or_default(),
            target.anidb_episode_id.clone().unwrap_or_default()
        )
    }));
    format!(
        "{}-{:016x}",
        RR3_METADATA_GRAPH_FINGERPRINT_PREFIX,
        stable_fnv1a64(&material.join("|"))
    )
}

fn stable_fnv1a64(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn normalize_series_external_ids(ids: &mut ExternalIds) {
    if ids.tvdb_series.is_none() {
        ids.tvdb_series = ids.tvdb.clone();
    }
    if ids.tvdb.is_none() {
        ids.tvdb = ids.tvdb_series.clone();
    }
}

fn primary_tvdb_series_id(ids: &ExternalIds) -> Option<String> {
    ids.tvdb_series.clone().or_else(|| ids.tvdb.clone())
}

fn insert_alias(aliases: &mut BTreeSet<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        aliases.insert(trimmed.to_string());
    }
}

fn next_search_after_for_air_time(
    air_time: Option<DateTime<Utc>>,
    release_delay_seconds: i64,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    match air_time {
        Some(air_time) if air_time + chrono::Duration::seconds(release_delay_seconds) > now => {
            air_time + chrono::Duration::seconds(release_delay_seconds)
        }
        _ => now,
    }
}

fn extract_air_date(raw: &JsonValue) -> Option<String> {
    for key in [
        "airDate",
        "air_date",
        "firstAired",
        "first_aired",
        "aired",
        "releaseDate",
    ] {
        if let Some(value) = raw.get(key).and_then(JsonValue::as_str) {
            if let Some(date) = normalize_air_date(value) {
                return Some(date);
            }
        }
    }
    extract_air_timestamp(raw).map(|value| value.date_naive().to_string())
}

fn extract_air_timestamp(raw: &JsonValue) -> Option<DateTime<Utc>> {
    for key in ["airingAt", "airing_at", "timestamp"] {
        if let Some(timestamp) = raw.get(key).and_then(JsonValue::as_i64)
            && let Some(value) = Utc.timestamp_opt(timestamp, 0).single()
        {
            return Some(value);
        }
    }
    None
}

fn normalize_air_date(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if DateTime::parse_from_rfc3339(trimmed).is_ok() {
        return DateTime::parse_from_rfc3339(trimmed)
            .ok()
            .map(|value| value.with_timezone(&Utc).date_naive().to_string());
    }
    if trimmed.len() >= 10 {
        return Some(trimmed[..10].to_string());
    }
    None
}

fn parse_air_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn parse_air_date(value: &str) -> Option<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(value.get(0..10)?, "%Y-%m-%d").ok()?;
    Some(DateTime::from_naive_utc_and_offset(
        date.and_hms_opt(12, 0, 0)?,
        Utc,
    ))
}

fn anime_classifier_hints(input: &str) -> Vec<elixir_classifier::hint::ClassificationHint> {
    let parser = AnimeParserAdapter;
    let mut file_input = ClassifierFileInput::new(input);
    file_input.file_name = Some(input.to_string());
    file_input.library_type_hint = Some(ClassifierLibraryType::Anime);
    parser.parse(&file_input)
}

fn parse_anime_release_group(input: &str) -> Option<String> {
    let captures = LEADING_RELEASE_GROUP_RE.captures(input)?;
    let raw = captures
        .name("square")
        .or_else(|| captures.name("wide"))?
        .as_str();
    normalize_release_group(raw)
}

fn normalize_release_group(raw: &str) -> Option<String> {
    let group = cleanup_anime_title(raw);
    if group.is_empty() || is_metadata_segment(&group) || is_episode_segment(&group) {
        return None;
    }
    if group.starts_with("OPFans") {
        return Some("OPFans".to_string());
    }
    Some(group)
}

fn extract_bracket_segments(input: &str) -> Vec<String> {
    BRACKET_SEGMENT_RE
        .captures_iter(input)
        .filter_map(|captures| {
            captures
                .name("square")
                .or_else(|| captures.name("wide"))
                .map(|value| cleanup_anime_title(value.as_str()))
        })
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn extract_anime_series_title(input: &str, bracket_segments: &[String]) -> Option<String> {
    let mut candidates: Vec<(i32, usize, String)> = Vec::new();
    for (idx, segment) in bracket_segments.iter().enumerate().skip(1) {
        if let Some(candidate) = title_candidate_from_segment(segment) {
            candidates.push((score_title_candidate(&candidate), idx, candidate));
        }
    }
    if let Some(candidate) = title_candidate_from_unbracketed(input) {
        candidates.push((score_title_candidate(&candidate) + 1, 0, candidate));
    }
    candidates
        .into_iter()
        .filter(|(score, _, _)| *score > 0)
        .max_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)))
        .map(|(_, _, title)| title)
}

fn title_candidate_from_unbracketed(input: &str) -> Option<String> {
    let mut value = LEADING_RELEASE_GROUP_RE.replace(input, "").to_string();
    value = strip_promo_prefixes(&value);
    if value.trim_start().starts_with(['[', '【']) {
        return None;
    }
    let first_episode_or_metadata = BRACKET_SEGMENT_RE
        .find_iter(&value)
        .find(|matched| {
            let inner = matched.as_str().trim_matches(['[', ']', '【', '】']).trim();
            is_episode_segment(inner) || is_metadata_segment(inner)
        })
        .map(|matched| matched.start());
    if let Some(index) = first_episode_or_metadata {
        value.truncate(index);
    }
    if let Some(captures) = DASH_EPISODE_RE.captures(&value)
        && let Some(start) = captures.get(0).map(|matched| matched.start())
        && start > 0
    {
        value.truncate(start);
    }
    title_candidate_from_segment(&value)
}

fn title_candidate_from_segment(segment: &str) -> Option<String> {
    let raw_normalized = normalize_fullwidth_digits(segment);
    if raw_normalized.chars().any(is_cjk)
        && raw_normalized.chars().any(|ch| ch.is_ascii_alphabetic())
        && raw_normalized.contains(['_', '＿'])
    {
        let split_parts = raw_normalized
            .split(['_', '＿'])
            .map(cleanup_title_candidate)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if let Some(candidate) = split_parts
            .into_iter()
            .max_by_key(|part| split_title_candidate_rank(part))
            .filter(|part| score_title_candidate(part) > 0)
        {
            return Some(candidate);
        }
    }
    let cleaned = cleanup_title_candidate(segment);
    if cleaned.is_empty()
        || is_metadata_segment(&cleaned)
        || is_episode_segment(&cleaned)
        || looks_like_year(&cleaned)
    {
        return None;
    }
    let split_parts = cleaned
        .split(['/', '／', '|', '·', '・'])
        .map(cleanup_title_candidate)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if split_parts.len() > 1 {
        return split_parts
            .into_iter()
            .max_by_key(|part| split_title_candidate_rank(part))
            .filter(|part| score_title_candidate(part) > 0);
    }
    let cleaned = strip_leading_cjk_before_ascii_suffix(&cleaned);
    Some(strip_trailing_cjk_after_ascii_prefix(&cleaned))
}

fn cleanup_title_candidate(value: &str) -> String {
    let mut cleaned = normalize_fullwidth_digits(value);
    cleaned = cleaned.replace('－', "-");
    cleaned = cleaned.replace(['[', ']', '【', '】'], " ");
    cleaned = cleaned.replace(['_', '＿'], " ");
    cleaned = if let Some(captures) = TITLE_TRAILING_EPISODE_RE.captures(&cleaned) {
        captures
            .name("title")
            .map(|title| title.as_str().to_string())
            .unwrap_or(cleaned)
    } else {
        TRAILING_EPISODE_SUFFIX_RE.replace(&cleaned, "").to_string()
    };
    cleaned = TITLE_TRAILING_BATCH_MARKER_RE
        .replace(&cleaned, "")
        .to_string();
    cleaned = TITLE_SEASON_CODE_RE
        .replace_all(&cleaned, |captures: &regex::Captures<'_>| {
            captures
                .name("season")
                .map(|value| format!("S{}", value.as_str()))
                .unwrap_or_else(|| captures[0].to_string())
        })
        .to_string();
    cleaned = cleaned.trim_matches(['-', '_', '.', ' ', '　']).to_string();
    cleanup_anime_title(&cleaned)
}

fn cleanup_anime_title(value: &str) -> String {
    let mut output = String::new();
    let mut previous_space = false;
    for ch in normalize_fullwidth_digits(value).chars() {
        let mapped = match ch {
            '\u{3000}' | '\t' | '\n' | '\r' => ' ',
            '＿' => '_',
            '／' => '/',
            '－' => '-',
            _ => ch,
        };
        if mapped.is_whitespace() {
            if !previous_space {
                output.push(' ');
            }
            previous_space = true;
        } else {
            output.push(mapped);
            previous_space = false;
        }
    }
    output
        .trim()
        .trim_matches(['-', '_', '.', ' '])
        .trim()
        .to_string()
}

fn strip_promo_prefixes(value: &str) -> String {
    let mut output = value.trim().to_string();
    loop {
        let trimmed = output.trim_start();
        let without_star = trimmed.strip_prefix('★').and_then(|tail| {
            tail.find('★')
                .map(|end| tail[end + '★'.len_utf8()..].to_string())
        });
        if let Some(next) = without_star {
            output = next;
            continue;
        }
        return trimmed.to_string();
    }
}

fn score_title_candidate(value: &str) -> i32 {
    let title = cleanup_anime_title(value);
    if title.len() < 2 || is_metadata_segment(&title) || is_episode_segment(&title) {
        return 0;
    }
    let mut score = 1;
    if title.chars().any(|ch| ch.is_ascii_alphabetic()) {
        score += 12;
    }
    if title.chars().any(is_cjk) {
        score += 4;
    }
    if title.contains(' ') || title.contains('-') {
        score += 2;
    }
    let lower = title.to_ascii_lowercase();
    if lower.contains("anime") || lower.contains("series") || lower.contains("title") {
        score += 8;
    }
    if lower == "baha" || lower == "viutv" || lower == "b-global" {
        return 0;
    }
    score
}

fn split_title_candidate_rank(value: &str) -> (bool, i32) {
    let title = cleanup_anime_title(value);
    let clean_latin_alias =
        title.chars().any(|ch| ch.is_ascii_alphabetic()) && !title.chars().any(is_cjk);
    (clean_latin_alias, score_title_candidate(&title))
}

fn strip_trailing_cjk_after_ascii_prefix(value: &str) -> String {
    let trimmed = value.trim();
    if !trimmed
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic())
    {
        return trimmed.to_string();
    }
    let mut output = String::new();
    for ch in trimmed.chars() {
        if is_cjk(ch) {
            break;
        }
        output.push(ch);
    }
    cleanup_anime_title(&output)
}

fn strip_leading_cjk_before_ascii_suffix(value: &str) -> String {
    let trimmed = value.trim();
    if !trimmed.chars().any(is_cjk) {
        return trimmed.to_string();
    }
    let Some(first_ascii) = trimmed.find(|ch: char| ch.is_ascii_alphabetic()) else {
        return trimmed.to_string();
    };
    if first_ascii == 0 {
        return trimmed.to_string();
    }
    let suffix = cleanup_anime_title(&trimmed[first_ascii..]);
    if score_title_candidate(&suffix) > 0 {
        suffix
    } else {
        trimmed.to_string()
    }
}

fn parse_sxxeyy_numbers(input: &str) -> Option<(i32, i32, Option<i32>)> {
    let captures = SXXEYY_RE.captures(input)?;
    Some((
        parse_capture_i32(&captures, "season")?,
        parse_capture_i32(&captures, "episode")?,
        parse_capture_i32(&captures, "end"),
    ))
}

fn parse_season_dash_episode(input: &str) -> Option<(i32, i32)> {
    let captures = SEASON_DASH_EPISODE_RE.captures(input)?;
    Some((
        parse_capture_i32(&captures, "season")?,
        parse_capture_i32(&captures, "episode")?,
    ))
}

fn parse_season_number(input: &str) -> Option<i32> {
    let captures = SEASON_WORD_RE.captures(input)?;
    ["s", "season", "ordinal", "part", "cour"]
        .into_iter()
        .find_map(|name| parse_capture_i32(&captures, name))
}

fn parse_absolute_episode_numbers(input: &str, bracket_segments: &[String]) -> Vec<i32> {
    let mut candidates = Vec::new();
    for segment in bracket_segments.iter().skip(1) {
        if let Some((start, end)) = parse_episode_segment_numbers(segment) {
            candidates.extend(expand_episode_numbers(start, end, 200));
            if !candidates.is_empty() {
                return candidates;
            }
        }
    }
    if let Some((_, episode)) = parse_season_dash_episode(input) {
        return vec![episode];
    }
    if let Some(captures) = CHINESE_EPISODE_RE.captures(input)
        && let Some(episode) = parse_capture_i32(&captures, "episode")
    {
        return vec![episode];
    }
    if let Some(captures) = BATCH_EPISODE_RANGE_RE.captures(input)
        && let Some(start) = parse_capture_i32(&captures, "start")
    {
        let end = parse_capture_i32(&captures, "end").unwrap_or(start);
        return expand_episode_numbers(start, end, 200);
    }
    if let Some(captures) = DASH_EPISODE_RE.captures(input)
        && let Some(start) = parse_capture_i32(&captures, "start")
    {
        let end = parse_capture_i32(&captures, "end").unwrap_or(start);
        return expand_episode_numbers(start, end, 200);
    }
    Vec::new()
}

fn parse_episode_segment_numbers(segment: &str) -> Option<(i32, i32)> {
    let normalized = normalize_fullwidth_digits(segment);
    if is_metadata_segment(&normalized) || looks_like_year(&normalized) {
        return None;
    }
    if let Some(captures) = CHINESE_EPISODE_RE.captures(&normalized)
        && let Some(episode) = parse_capture_i32(&captures, "episode")
    {
        return Some((episode, episode));
    }
    let captures = BRACKET_EPISODE_RE.captures(normalized.trim())?;
    let start = parse_capture_i32(&captures, "start")?;
    if looks_like_resolution_number(start, normalized.trim()) {
        return None;
    }
    let end = parse_capture_i32(&captures, "end").unwrap_or(start);
    Some((start, end))
}

fn expand_episode_numbers(start: i32, end: i32, max_span: i32) -> Vec<i32> {
    if start <= 0 || end <= 0 {
        return Vec::new();
    }
    if end < start || end - start > max_span {
        return vec![start];
    }
    (start..=end).collect()
}

fn parse_anime_episode_type(input: &str) -> AnimeEpisodeType {
    let lower = input.to_ascii_lowercase();
    if lower.contains("ncop")
        || lower.contains("nced")
        || lower.contains("creditless")
        || lower.contains("clean op")
        || lower.contains("clean ed")
    {
        return AnimeEpisodeType::Credits;
    }
    if lower.contains("trailer") || lower.contains("[pv") || lower.contains(" pv ") {
        return AnimeEpisodeType::Trailer;
    }
    if lower.contains("movie") {
        return AnimeEpisodeType::Movie;
    }
    if lower.contains("ova")
        || lower.contains("oad")
        || lower.contains("ona")
        || lower.contains("special")
        || lower.contains(" sp ")
    {
        return AnimeEpisodeType::Special;
    }
    AnimeEpisodeType::Normal
}

fn parse_anime_batch_kind(
    input: &str,
    episode_type: AnimeEpisodeType,
    episode_numbers: &[i32],
    absolute_episode_numbers: &[i32],
) -> AnimeBatchKind {
    let lower = input.to_ascii_lowercase();
    if episode_type == AnimeEpisodeType::Movie {
        return AnimeBatchKind::Movie;
    }
    if lower.contains("complete series")
        || lower.contains("complete collection")
        || lower.contains("全季")
    {
        return AnimeBatchKind::CompleteSeries;
    }
    if lower.contains("multi-season") {
        return AnimeBatchKind::MultiSeasonPack;
    }
    if lower.contains("season complete") || lower.contains("bd box") || lower.contains("batch") {
        return AnimeBatchKind::SeasonPack;
    }
    if episode_numbers.len() > 1 || absolute_episode_numbers.len() > 1 {
        return AnimeBatchKind::Range;
    }
    if lower.contains("complete") || lower.contains("合集") {
        return AnimeBatchKind::UnknownBatch;
    }
    AnimeBatchKind::Single
}

fn parse_anime_version(input: &str) -> Option<u8> {
    VERSION_RE.captures(input).and_then(|captures| {
        ["v", "bracket", "episode_v", "repack", "rerip"]
            .into_iter()
            .filter_map(|name| captures.name(name).map(|value| value.as_str()))
            .find_map(|value| {
                if value.is_empty() {
                    Some(2)
                } else {
                    value.parse::<u8>().ok()
                }
            })
    })
}

fn parse_crc32(input: &str) -> Option<String> {
    CRC32_RE
        .captures_iter(input)
        .filter_map(|captures| {
            captures
                .name("crc")
                .map(|value| value.as_str().to_uppercase())
        })
        .last()
}

fn parse_anime_quality(input: &str) -> AnimeParsedQuality {
    let lower = input.to_ascii_lowercase();
    let resolution = RESOLUTION_RE.captures(input).and_then(|captures| {
        captures
            .name("resolution")
            .map(|value| normalize_resolution(value.as_str()))
    });
    let source = if lower.contains("web-dl") || lower.contains("webdl") {
        Some("web_dl".to_string())
    } else if lower.contains("webrip") || lower.contains("web-rip") {
        Some("web_rip".to_string())
    } else if lower.contains("bluray") || lower.contains("blu-ray") || lower.contains("bdrip") {
        Some("blu_ray".to_string())
    } else if lower.contains("hdtv") {
        Some("hdtv".to_string())
    } else if lower.contains("dvd") {
        Some("dvd".to_string())
    } else {
        None
    };
    AnimeParsedQuality {
        resolution,
        source,
        video_codec: CODEC_RE.captures(input).and_then(|captures| {
            captures
                .name("codec")
                .map(|value| normalize_codec(value.as_str()))
        }),
        audio_codec: AUDIO_CODEC_RE.captures(input).and_then(|captures| {
            captures
                .name("audio")
                .map(|value| value.as_str().to_uppercase())
        }),
        dual_audio: lower.contains("dual audio") || lower.contains("dual-audio"),
        multi_sub: lower.contains("multisub")
            || lower.contains("multi-sub")
            || lower.contains("multi subs")
            || lower.contains("multiple subtitle")
            || lower.contains("简繁")
            || lower.contains("雙語")
            || lower.contains("双语"),
    }
}

fn parse_anime_languages(input: &str) -> (Vec<String>, Vec<String>) {
    let mut audio = BTreeSet::new();
    let mut subtitles = BTreeSet::new();
    let upper = input.to_uppercase();
    for (token, language) in [
        ("JPN", "JPN"),
        ("JAP", "JPN"),
        ("ENG", "ENG"),
        ("ENGLISH", "ENG"),
        ("CHS", "CHS"),
        ("GB", "CHS"),
        ("CHT", "CHT"),
        ("BIG5", "CHT"),
    ] {
        if upper.contains(token) {
            if matches!(language, "JPN" | "ENG") {
                audio.insert(language.to_string());
            } else {
                subtitles.insert(language.to_string());
            }
        }
    }
    if input.contains("简体") || input.contains("簡體") || input.contains("繁中") {
        subtitles.insert("CHS".to_string());
    }
    if input.contains("繁体") || input.contains("繁體") || input.contains("繁中") {
        subtitles.insert("CHT".to_string());
    }
    if input.contains("简繁") || input.contains("簡繁") {
        subtitles.insert("CHS".to_string());
        subtitles.insert("CHT".to_string());
    }
    if input.to_ascii_lowercase().contains("multi-sub")
        || input.to_ascii_lowercase().contains("multisub")
    {
        subtitles.insert("MULTI".to_string());
    }
    (audio.into_iter().collect(), subtitles.into_iter().collect())
}

fn normalize_resolution(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "1920x1080" | "1080p10" => "1080p".to_string(),
        "1280x720" => "720p".to_string(),
        "4k" | "uhd" => "2160p".to_string(),
        other => other.to_string(),
    }
}

fn normalize_codec(value: &str) -> String {
    match value.to_ascii_lowercase().replace('.', "").as_str() {
        "h265" | "x265" | "hevc" => "HEVC".to_string(),
        "h264" | "x264" | "avc" => "H264".to_string(),
        "av1" => "AV1".to_string(),
        "vp9" => "VP9".to_string(),
        _ => value.to_uppercase(),
    }
}

fn is_episode_segment(segment: &str) -> bool {
    parse_episode_segment_numbers(segment).is_some()
}

fn is_metadata_segment(segment: &str) -> bool {
    let value = cleanup_anime_title(segment);
    let lower = value.to_ascii_lowercase();
    if value.is_empty() || looks_like_year(&value) {
        return true;
    }
    if lower.contains("新番")
        || lower.contains("招募")
        || lower == "国漫"
        || lower == "baha"
        || lower == "b-global"
        || lower == "viutv"
        || lower == "mp4"
        || lower == "mkv"
        || lower == "ass"
        || lower.contains("正式版本")
    {
        return true;
    }
    RESOLUTION_RE.is_match(&value)
        || CODEC_RE.is_match(&value)
        || AUDIO_CODEC_RE.is_match(&value)
        || CRC32_RE.is_match(&format!("[{value}]"))
        || lower.contains("web-dl")
        || lower.contains("webrip")
        || lower.contains("multi")
        || lower.contains("subtitle")
        || lower.contains("gb")
        || lower.contains("big5")
        || lower.contains("cht")
        || lower.contains("chs")
        || lower.contains("繁")
        || lower.contains("简")
}

fn looks_like_year(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() == 4
        && trimmed.chars().all(|ch| ch.is_ascii_digit())
        && trimmed
            .parse::<i32>()
            .is_ok_and(|year| (1950..=2100).contains(&year))
}

fn looks_like_resolution_number(number: i32, raw: &str) -> bool {
    matches!(number, 360 | 480 | 540 | 576 | 720 | 1080 | 2160) && RESOLUTION_RE.is_match(raw)
}

fn parse_capture_i32(captures: &regex::Captures<'_>, name: &str) -> Option<i32> {
    captures
        .name(name)
        .and_then(|value| value.as_str().trim_start_matches('0').parse::<i32>().ok())
}

fn normalize_anime_title(value: &str) -> String {
    cleanup_anime_title(value)
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_alphanumeric())
        .collect()
}

fn normalize_anime_alias(value: &str) -> String {
    anime_alias_tokens(value).join("")
}

fn anime_alias_tokens(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in cleanup_anime_title(value)
        .chars()
        .flat_map(char::to_lowercase)
    {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn roman_numeral_value(value: &str) -> Option<u32> {
    let trimmed = value.trim().to_ascii_lowercase();
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .all(|ch| matches!(ch, 'i' | 'v' | 'x' | 'l' | 'c' | 'd' | 'm'))
    {
        return None;
    }

    let mut total = 0_u32;
    let mut previous = 0_u32;
    for value in trimmed.chars().rev().map(|ch| match ch {
        'i' => 1,
        'v' => 5,
        'x' => 10,
        'l' => 50,
        'c' => 100,
        'd' => 500,
        'm' => 1000,
        _ => 0,
    }) {
        if value < previous {
            total = total.saturating_sub(value);
        } else {
            total += value;
            previous = value;
        }
    }
    (total > 0).then_some(total)
}

fn normalize_fullwidth_digits(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '０'..='９' => char::from_u32('0' as u32 + (ch as u32 - '０' as u32)).unwrap_or(ch),
            _ => ch,
        })
        .collect()
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff | 0x3040..=0x30ff | 0xac00..=0xd7af
    )
}

fn parse_i64(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn parse_i64_option(value: Option<&String>) -> Option<i64> {
    value.and_then(|value| parse_i64(value))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use serde::Deserialize;
    use serde_json::Value;

    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ShokoInventory {
        fixture_set: String,
        resolver: String,
        shoko_repository: String,
        shoko_commit: String,
        inspected_source_files: Vec<ShokoSourceFile>,
        fixture_schemas: Vec<FixtureSchema>,
        anidb_safety: AnidbSafety,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ShokoSourceFile {
        path: String,
        phase: String,
        coverage: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FixtureSchema {
        name: String,
        phase: String,
        required_fields: Vec<String>,
        allowed_classifications: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnidbSafety {
        direct_provider_default_enabled: bool,
        requires_explicit_capability_gate: bool,
        short_delay_seconds: u64,
        sustained_delay_seconds: u64,
        sustained_activation_seconds: u64,
        idle_reset_seconds: u64,
        padding_millis: u64,
        minimum_protocol_delay_seconds: u64,
        minimum_sustained_delay_seconds: u64,
        udp_ban_cooldown_minutes: u64,
        http_ban_cooldown_hours: u64,
        negative_cache_ttl_days: u64,
        required_gates: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeSeedSet {
        fixture_set: String,
        source_fixture_set: String,
        source_commit: String,
        source_file: String,
        classification: String,
        counts: AnimeSeedCounts,
        cases: Vec<AnimeSeedCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeSeedCounts {
        total: u64,
        release_group: u64,
        unicode_title: u64,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeSeedCase {
        id: String,
        source_fixture: String,
        source_method: String,
        source_line: u64,
        input: String,
        test_kind: String,
        fixture_group: String,
        classification: String,
        source_classification: String,
        source_skip_reason: String,
        expected: Value,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenSet {
        fixture_set: String,
        resolver: String,
        classification: String,
        cases: Vec<AnimeGraphGoldenCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenCase {
        id: String,
        classification: String,
        input: AnimeGraphGoldenInput,
        expected: AnimeGraphGoldenExpected,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenInput {
        title: String,
        year: Option<i32>,
        seed_anilist_id: String,
        seed_season_number: i32,
        release_delay_seconds: i64,
        external_ids: ExternalIds,
        season_chain: Vec<AnimeGraphGoldenSeason>,
        mappings: Vec<AnimeGraphGoldenMapping>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenSeason {
        season_number: i32,
        anilist_id: String,
        title: String,
        format: Option<String>,
        season_year: Option<i32>,
        start_year: Option<i32>,
        status: Option<String>,
        episodes: Option<i32>,
        next_airing_episode: Option<i32>,
        next_airing_at: Option<i64>,
        confidence: f32,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenMapping {
        anilist_id: String,
        ids: ExternalIds,
        #[serde(default)]
        titles: BTreeMap<String, String>,
        #[serde(default)]
        episodes: Vec<AnimeGraphGoldenEpisode>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenEpisode {
        season_number: Option<i32>,
        episode_number: Option<i32>,
        absolute_episode_number: Option<i32>,
        title: Option<String>,
        tvdb_id: Option<String>,
        anidb_eid: Option<String>,
        #[serde(default)]
        raw: Value,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeGraphGoldenExpected {
        root_anilist_id: String,
        tvdb_series_id: String,
        anidb_anime_id: String,
        target_keys: Vec<String>,
        target_sources: BTreeMap<String, String>,
        aliases: Vec<String>,
        strongest_target_title: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeParserGoldenSet {
        fixture_set: String,
        resolver: String,
        classification: String,
        cases: Vec<AnimeParserGoldenCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeParserGoldenCase {
        id: String,
        classification: String,
        input: String,
        expected: AnimeParserExpected,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeParserExpected {
        series_title: Option<String>,
        release_group: Option<Option<String>>,
        season_number: Option<i32>,
        episodes: Option<Vec<i32>>,
        absolute_episodes: Option<Vec<i32>>,
        episode_type: Option<String>,
        batch_kind: Option<String>,
        version: Option<u8>,
        crc32: Option<String>,
        resolution: Option<String>,
        source: Option<String>,
        video_codec: Option<String>,
        audio_codec: Option<String>,
        dual_audio: Option<bool>,
        multi_sub: Option<bool>,
        audio_languages: Option<Vec<String>>,
        subtitle_languages: Option<Vec<String>>,
        review_reasons: Option<Vec<String>>,
    }

    fn load_shoko_inventory() -> ShokoInventory {
        serde_json::from_str(include_str!("fixtures/anime_rr3_shoko_inventory.json"))
            .expect("valid RR-3 Shoko inventory fixture")
    }

    fn load_anime_seed_set() -> AnimeSeedSet {
        serde_json::from_str(include_str!("fixtures/anime_rr3_sonarr_seed_fixtures.json"))
            .expect("valid RR-3 Sonarr anime seed fixture")
    }

    fn load_anime_graph_goldens() -> AnimeGraphGoldenSet {
        serde_json::from_str(include_str!(
            "fixtures/anime_rr3_metadata_graph_goldens.json"
        ))
        .expect("valid RR-3 metadata graph golden fixture")
    }

    fn load_anime_parser_goldens() -> AnimeParserGoldenSet {
        serde_json::from_str(include_str!("fixtures/anime_rr3_parser_goldens.json"))
            .expect("valid RR-3 anime parser golden fixture")
    }

    fn load_sonarr_generated_payload() -> Value {
        serde_json::from_str(include_str!(
            "fixtures/sonarr_rr2_conventional_tv_generated.json"
        ))
        .expect("valid generated Sonarr fixture inventory")
    }

    fn assert_rr3_production_classification(id: &str, classification: &str) {
        let allowed = [
            "rr3_asserted",
            "rr3d_asserted",
            "unsupported_by_product_policy",
        ];
        assert!(
            allowed.contains(&classification),
            "{id} has non-production RR-3 classification {classification}"
        );
    }

    fn rr3e_scoring_context() -> AnimeCandidateScoringContext {
        AnimeCandidateScoringContext {
            graph_fingerprint: Some("rr3e-test-graph".to_string()),
            aliases: vec![
                "Example Title".to_string(),
                "Example Title Alternative".to_string(),
                "例題".to_string(),
            ],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E01".to_string(),
                    canonical_key: Some("tvdb:100:S01E01".to_string()),
                    title: "Episode One".to_string(),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    tvdb_episode_id: Some("1001".to_string()),
                    anidb_episode_id: Some("2001".to_string()),
                },
                AnimeCandidateTarget {
                    target_key: "S01E02".to_string(),
                    canonical_key: Some("tvdb:100:S01E02".to_string()),
                    title: "Episode Two".to_string(),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    tvdb_episode_id: Some("1002".to_string()),
                    anidb_episode_id: Some("2002".to_string()),
                },
            ],
        }
    }

    fn rr3e_candidate(title: &str) -> AnimeCandidateInput {
        AnimeCandidateInput {
            title: title.to_string(),
            source_kind: "magnet".to_string(),
            quality: Some("1080p".to_string()),
            cached_debrid: Some(true),
            seeders: Some(50),
            rank: Some(1),
            supported_routes: vec![
                "acquisition.debrid.default".to_string(),
                "acquisition.torrent.default".to_string(),
            ],
            ..Default::default()
        }
    }

    impl AnimeGraphGoldenSeason {
        fn to_chain_entry(&self) -> AniListSeasonChainEntry {
            AniListSeasonChainEntry {
                season_number: self.season_number,
                anilist_id: self.anilist_id.clone(),
                title: self.title.clone(),
                format: self.format.clone(),
                season_year: self.season_year,
                start_year: self.start_year,
                status: self.status.clone(),
                episodes: self.episodes,
                next_airing_episode: self.next_airing_episode,
                next_airing_at: self.next_airing_at,
                confidence: self.confidence,
            }
        }
    }

    impl AnimeGraphGoldenMapping {
        fn to_anizip_mapping(&self) -> AniZipMapping {
            AniZipMapping {
                ids: self.ids.clone(),
                episodes: self
                    .episodes
                    .iter()
                    .map(|episode| crate::library::AniZipEpisodeRecord {
                        season_number: episode.season_number,
                        episode_number: episode.episode_number,
                        absolute_episode_number: episode.absolute_episode_number,
                        title: episode.title.clone(),
                        overview: None,
                        runtime_minutes: None,
                        image: None,
                        tvdb_id: episode.tvdb_id.clone(),
                        anidb_eid: episode.anidb_eid.clone(),
                        raw: episode.raw.clone(),
                    })
                    .collect(),
                images: Vec::new(),
                titles: self
                    .titles
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            }
        }
    }

    fn build_graph_from_golden(case: &AnimeGraphGoldenCase) -> AnimeMetadataGraph {
        let mut mappings_by_anilist = case
            .input
            .mappings
            .iter()
            .map(|mapping| (mapping.anilist_id.clone(), mapping.to_anizip_mapping()))
            .collect::<HashMap<_, _>>();
        let seasons = case
            .input
            .season_chain
            .iter()
            .map(|season| AnimeSeasonMapping {
                season: season.to_chain_entry(),
                mapping: mappings_by_anilist.remove(&season.anilist_id),
            })
            .collect();

        build_anime_metadata_graph(AnimeMetadataGraphInput {
            title: case.input.title.clone(),
            year: case.input.year,
            seed_anilist_id: case.input.seed_anilist_id.clone(),
            seed_season_number: case.input.seed_season_number,
            external_ids: case.input.external_ids.clone(),
            seasons,
        })
    }

    fn assert_parser_expected(
        id: &str,
        parsed: &AnimeParsedRelease,
        expected: &AnimeParserExpected,
    ) {
        if let Some(series_title) = expected.series_title.as_deref() {
            assert_eq!(
                parsed.series_title.as_deref(),
                Some(series_title),
                "{id} series title"
            );
        }
        if let Some(release_group) = expected.release_group.as_ref() {
            assert_eq!(
                parsed.release_group.as_deref(),
                release_group.as_deref(),
                "{id} release group"
            );
        }
        if let Some(season_number) = expected.season_number {
            assert_eq!(parsed.season_number, Some(season_number), "{id} season");
        }
        if let Some(episodes) = expected.episodes.as_ref() {
            assert_eq!(&parsed.episode_numbers, episodes, "{id} episodes");
        }
        if let Some(absolute_episodes) = expected.absolute_episodes.as_ref() {
            assert_eq!(
                &parsed.absolute_episode_numbers, absolute_episodes,
                "{id} absolute episodes"
            );
        }
        if let Some(episode_type) = expected.episode_type.as_deref() {
            assert_eq!(
                serde_json::to_value(parsed.episode_type)
                    .expect("episode type serializes")
                    .as_str(),
                Some(episode_type),
                "{id} episode type"
            );
        }
        if let Some(batch_kind) = expected.batch_kind.as_deref() {
            assert_eq!(
                serde_json::to_value(parsed.batch_kind)
                    .expect("batch kind serializes")
                    .as_str(),
                Some(batch_kind),
                "{id} batch kind"
            );
        }
        if let Some(version) = expected.version {
            assert_eq!(parsed.version, Some(version), "{id} version");
        }
        if let Some(crc32) = expected.crc32.as_deref() {
            assert_eq!(parsed.crc32.as_deref(), Some(crc32), "{id} crc32");
        }
        if let Some(resolution) = expected.resolution.as_deref() {
            assert_eq!(
                parsed.quality.resolution.as_deref(),
                Some(resolution),
                "{id} resolution"
            );
        }
        if let Some(source) = expected.source.as_deref() {
            assert_eq!(
                parsed.quality.source.as_deref(),
                Some(source),
                "{id} source"
            );
        }
        if let Some(video_codec) = expected.video_codec.as_deref() {
            assert_eq!(
                parsed.quality.video_codec.as_deref(),
                Some(video_codec),
                "{id} video codec"
            );
        }
        if let Some(audio_codec) = expected.audio_codec.as_deref() {
            assert_eq!(
                parsed.quality.audio_codec.as_deref(),
                Some(audio_codec),
                "{id} audio codec"
            );
        }
        if let Some(dual_audio) = expected.dual_audio {
            assert_eq!(parsed.quality.dual_audio, dual_audio, "{id} dual audio");
        }
        if let Some(multi_sub) = expected.multi_sub {
            assert_eq!(parsed.quality.multi_sub, multi_sub, "{id} multi sub");
        }
        if let Some(audio_languages) = expected.audio_languages.as_ref() {
            assert_eq!(
                &parsed.audio_languages, audio_languages,
                "{id} audio languages"
            );
        }
        if let Some(subtitle_languages) = expected.subtitle_languages.as_ref() {
            for language in subtitle_languages {
                assert!(
                    parsed.subtitle_languages.contains(language),
                    "{id} missing subtitle language {language:?}: {:?}",
                    parsed.subtitle_languages
                );
            }
        }
        if let Some(review_reasons) = expected.review_reasons.as_ref() {
            assert_eq!(
                &parsed.review_reasons, review_reasons,
                "{id} review reasons"
            );
        }
    }

    #[test]
    fn rr3_shoko_source_inventory_is_complete() {
        let inventory = load_shoko_inventory();

        assert_eq!(inventory.fixture_set, "rr3-shoko-anime-resolver-inventory");
        assert_eq!(inventory.resolver, "anime_shoko_style");
        assert_eq!(inventory.shoko_repository, SHOKO_REFERENCE_REPOSITORY);
        assert_eq!(inventory.shoko_commit, SHOKO_REFERENCE_COMMIT);

        let expected_paths = [
            "Shoko.Server/Services/VideoHashingService.cs",
            "Shoko.Server/Scheduling/Jobs/Shoko/HashFileJob.cs",
            "Shoko.Server/Scheduling/Jobs/Shoko/ProcessFileJob.cs",
            "Shoko.Server/Services/VideoReleaseService.cs",
            "Shoko.Server/Providers/AniDB/Release/AnidbReleaseProvider.cs",
            "Shoko.Server/Providers/AniDB/UDP/Info/RequestGetFile.cs",
            "Shoko.Server/Providers/AniDB/UDP/Info/ResponseGetFile.cs",
            "Shoko.Server/Models/CrossReference/CrossRef_File_Episode.cs",
            "Shoko.Server/Models/AniDB/AniDB_Episode.cs",
            "Shoko.Server/Providers/AniDB/HTTP/HttpAnimeParser.cs",
            "Shoko.Server/Services/AnimeSeriesService.cs",
            "Shoko.Server/Tasks/AnimeGroupCreator.cs",
            "Shoko.Server/Providers/AniDB/UDP/UDPRateLimiter.cs",
            "Shoko.Server/Providers/AniDB/HTTP/HttpRateLimiter.cs",
            "Shoko.Server/Settings/AnidbRateLimitSettings.cs",
            "Shoko.Server/Providers/AniDB/ConnectionHandler.cs",
            "Shoko.Server/Providers/AniDB/UDP/AniDBUDPConnectionHandler.cs",
            "Shoko.Server/Providers/AniDB/HTTP/AniDBHttpConnectionHandler.cs",
        ];
        let expected_paths = expected_paths.into_iter().collect::<BTreeSet<_>>();
        let actual_paths = inventory
            .inspected_source_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(actual_paths, expected_paths);

        for file in &inventory.inspected_source_files {
            assert!(
                file.phase.starts_with("rr3"),
                "{} has non-RR-3 phase {}",
                file.path,
                file.phase
            );
            assert!(
                !file.coverage.is_empty(),
                "{} missing coverage notes",
                file.path
            );
        }
    }

    #[test]
    fn rr3_fixture_schema_inventory_is_complete() {
        let inventory = load_shoko_inventory();
        let schemas = inventory
            .fixture_schemas
            .iter()
            .map(|schema| (schema.name.as_str(), schema))
            .collect::<BTreeMap<_, _>>();

        let expected_schema_names = [
            "anime_title_parser",
            "metadata_graph",
            "pack_file_list",
            "anidb_file_response",
            "reconciliation",
        ];

        for name in expected_schema_names {
            let schema = schemas
                .get(name)
                .unwrap_or_else(|| panic!("missing RR-3 fixture schema {name}"));
            assert!(
                schema.phase.starts_with("rr3"),
                "{name} has non-RR-3 phase {}",
                schema.phase
            );
            assert!(
                schema.required_fields.iter().any(|field| field == "id"),
                "{name} schema must require id"
            );
            assert!(
                schema
                    .required_fields
                    .iter()
                    .any(|field| field == "classification"),
                "{name} schema must require classification"
            );
            assert_eq!(
                schema
                    .allowed_classifications
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
                [
                    "rr3_asserted",
                    "rr3d_asserted",
                    "unsupported_by_product_policy"
                ]
                .into_iter()
                .collect()
            );
        }
    }

    #[test]
    fn rr3_anidb_safety_gate_is_fail_closed() {
        let inventory = load_shoko_inventory();
        let safety = inventory.anidb_safety;

        assert!(!safety.direct_provider_default_enabled);
        assert!(safety.requires_explicit_capability_gate);
        assert_eq!(safety.short_delay_seconds, 2);
        assert_eq!(safety.sustained_delay_seconds, 6);
        assert_eq!(safety.sustained_activation_seconds, 10);
        assert_eq!(safety.idle_reset_seconds, 120);
        assert_eq!(safety.padding_millis, 50);
        assert_eq!(safety.minimum_protocol_delay_seconds, 2);
        assert_eq!(safety.minimum_sustained_delay_seconds, 4);
        assert_eq!(safety.udp_ban_cooldown_minutes, 90);
        assert_eq!(safety.http_ban_cooldown_hours, 12);
        assert_eq!(safety.negative_cache_ttl_days, 7);

        let gates = safety
            .required_gates
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            gates,
            [
                "global_udp_limiter",
                "global_http_limiter_when_http_enabled",
                "unsafe_configuration_rejection",
                "duplicate_ed2k_size_coalescing",
                "positive_cache",
                "negative_cache",
                "separate_udp_http_ban_state",
                "ban_backoff_pause",
                "no_live_anidb_unit_tests",
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn rr3_sonarr_anime_seed_inventory_is_classified() {
        let seed = load_anime_seed_set();
        let source = load_sonarr_generated_payload();

        assert_eq!(seed.fixture_set, "rr3-sonarr-anime-seed-fixtures");
        assert_eq!(
            seed.source_fixture_set,
            "rr2-sonarr-conventional-tv-exhaustive"
        );
        assert_eq!(seed.source_commit, "bf5d48c");
        assert_eq!(
            seed.source_file,
            "sonarr_rr2_conventional_tv_generated.json"
        );
        assert_eq!(seed.classification, "anime_rr3");
        assert_eq!(seed.counts.total, seed.cases.len() as u64);
        assert_eq!(seed.counts.total, 60);
        assert_eq!(seed.counts.release_group, 6);
        assert_eq!(seed.counts.unicode_title, 54);

        let source_cases = source["cases"].as_array().expect("source cases array");
        let source_anime_ids = source_cases
            .iter()
            .filter(|case| case["classification"].as_str() == Some("anime_rr3"))
            .map(|case| case["id"].as_str().expect("source case id"))
            .collect::<BTreeSet<_>>();
        let seed_ids = seed
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(seed_ids, source_anime_ids);

        let allowed_groups = [
            "anime_release_group",
            "anime_unicode_title",
            "anime_season_episode_title",
            "anime_multi_episode_title",
            "anime_false_positive_title",
            "anime_unicode_group_title",
            "anime_unicode_digits_title",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let mut seen_groups = BTreeMap::<String, u64>::new();
        let mut seen_ids = BTreeSet::new();

        for case in &seed.cases {
            assert!(seen_ids.insert(&case.id), "duplicate RR-3 seed {}", case.id);
            assert!(
                !case.source_fixture.trim().is_empty(),
                "{} missing fixture",
                case.id
            );
            assert!(
                !case.source_method.trim().is_empty(),
                "{} missing method",
                case.id
            );
            assert!(case.source_line > 0, "{} missing source line", case.id);
            assert!(!case.input.trim().is_empty(), "{} missing input", case.id);
            assert!(
                matches!(case.test_kind.as_str(), "release_group" | "unicode_title"),
                "{} has unexpected test kind {}",
                case.id,
                case.test_kind
            );
            assert!(
                allowed_groups.contains(case.fixture_group.as_str()),
                "{} has unexpected fixture group {}",
                case.id,
                case.fixture_group
            );
            assert_eq!(case.classification, "rr3d_asserted", "{}", case.id);
            assert_eq!(case.source_classification, "anime_rr3", "{}", case.id);
            assert_eq!(case.source_skip_reason, "anime_rr3", "{}", case.id);
            assert!(
                case.expected.is_object(),
                "{} missing expected object",
                case.id
            );
            *seen_groups.entry(case.fixture_group.clone()).or_default() += 1;
        }

        assert_eq!(seen_groups["anime_release_group"], 6);
        assert_eq!(seen_groups["anime_unicode_title"], 25);
        assert_eq!(seen_groups["anime_season_episode_title"], 18);
        assert_eq!(seen_groups["anime_multi_episode_title"], 2);
        assert_eq!(seen_groups["anime_false_positive_title"], 1);
        assert_eq!(seen_groups["anime_unicode_group_title"], 7);
        assert_eq!(seen_groups["anime_unicode_digits_title"], 1);
    }

    #[test]
    fn rr3_anime_parser_goldens_pass() {
        let goldens = load_anime_parser_goldens();
        assert_eq!(goldens.fixture_set, "rr3-anime-parser-goldens");
        assert_eq!(goldens.resolver, "anime_shoko_style");
        assert_eq!(goldens.classification, "rr3d_asserted");

        for case in &goldens.cases {
            assert_eq!(case.classification, "rr3d_asserted", "{}", case.id);
            let parsed = parse_anime_release_title(&case.input);
            assert_parser_expected(&case.id, &parsed, &case.expected);
            assert_eq!(parsed.parser_version, ANIME_PRE_DOWNLOAD_PARSER_VERSION);
        }
    }

    #[test]
    fn rr3l_production_parity_gate_has_no_pending_rows() {
        let inventory = load_shoko_inventory();
        let seed = load_anime_seed_set();
        let parser_goldens = load_anime_parser_goldens();
        let graph_goldens = load_anime_graph_goldens();
        let source = load_sonarr_generated_payload();

        for schema in &inventory.fixture_schemas {
            assert!(
                !schema
                    .allowed_classifications
                    .iter()
                    .any(|classification| classification == "rr3_pending"
                        || classification == "known_parity_gap"),
                "{} still permits pending/parity-gap rows",
                schema.name
            );
        }

        assert_rr3_production_classification("parser fixture set", &parser_goldens.classification);
        for case in &parser_goldens.cases {
            assert_rr3_production_classification(&case.id, &case.classification);
        }

        assert_rr3_production_classification(
            "metadata graph fixture set",
            &graph_goldens.classification,
        );
        for case in &graph_goldens.cases {
            assert_rr3_production_classification(&case.id, &case.classification);
        }

        for case in &seed.cases {
            assert_rr3_production_classification(&case.id, &case.classification);
            assert_eq!(case.source_classification, "anime_rr3", "{}", case.id);
            assert_eq!(case.source_skip_reason, "anime_rr3", "{}", case.id);
        }

        let source_cases = source["cases"].as_array().expect("source cases array");
        let pending_source_rows = source_cases
            .iter()
            .filter(|case| {
                matches!(
                    case["classification"].as_str(),
                    None | Some("") | Some("known_parity_gap")
                )
            })
            .collect::<Vec<_>>();
        assert!(
            pending_source_rows.is_empty(),
            "Sonarr source fixture still has unclassified/parity-gap rows: {}",
            pending_source_rows.len()
        );

        let source_anime_ids = source_cases
            .iter()
            .filter(|case| case["classification"].as_str() == Some("anime_rr3"))
            .map(|case| case["id"].as_str().expect("source case id"))
            .collect::<BTreeSet<_>>();
        let seed_ids = seed
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            seed_ids, source_anime_ids,
            "RR-3 seed fixtures must cover every Sonarr anime/absolute handoff row"
        );
    }

    #[test]
    fn rr3_sonarr_anime_seed_rows_parse_with_rr3d_parser() {
        let seed = load_anime_seed_set();
        for case in &seed.cases {
            let parsed = parse_anime_release_title(&case.input);
            let expected = &case.expected;

            if expected.get("releaseGroup").is_some() {
                let expected_group = expected
                    .get("releaseGroup")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                assert_eq!(
                    parsed.release_group, expected_group,
                    "{} release group for {}",
                    case.id, case.input
                );
            }
            if let Some(series_title) = expected.get("seriesTitle").and_then(Value::as_str) {
                assert_eq!(
                    parsed.series_title.as_deref(),
                    Some(series_title),
                    "{} series title for {}",
                    case.id,
                    case.input
                );
            }
            if let Some(absolute) = expected.get("absoluteEpisodes") {
                let expected_episodes = if let Some(value) = absolute.as_i64() {
                    vec![value as i32]
                } else {
                    absolute
                        .as_array()
                        .expect("absoluteEpisodes array")
                        .iter()
                        .map(|value| value.as_i64().expect("episode integer") as i32)
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    parsed.absolute_episode_numbers, expected_episodes,
                    "{} absolute episodes for {}",
                    case.id, case.input
                );
            }
        }
    }

    #[test]
    fn rr3_metadata_graph_goldens_expand_expected_targets() {
        let goldens = load_anime_graph_goldens();
        assert_eq!(goldens.fixture_set, "rr3-anime-metadata-graph-goldens");
        assert_eq!(goldens.resolver, "anime_shoko_style");
        assert_eq!(goldens.classification, "rr3_asserted");

        let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("valid fixed test time")
            .with_timezone(&Utc);

        for case in &goldens.cases {
            assert_eq!(case.classification, "rr3_asserted", "{}", case.id);
            let graph = build_graph_from_golden(case);

            assert_eq!(
                graph.root_anilist_id, case.expected.root_anilist_id,
                "{} root AniList id",
                case.id
            );
            assert_eq!(
                graph.external_ids.tvdb_series.as_deref(),
                Some(case.expected.tvdb_series_id.as_str()),
                "{} TVDB series id",
                case.id
            );
            assert_eq!(
                graph.external_ids.anidb.as_deref(),
                Some(case.expected.anidb_anime_id.as_str()),
                "{} AniDB anime id",
                case.id
            );

            let target_keys = graph
                .targets
                .iter()
                .map(|target| target.target_key.clone())
                .collect::<Vec<_>>();
            assert_eq!(
                target_keys, case.expected.target_keys,
                "{} target expansion",
                case.id
            );

            let target_sources = graph
                .targets
                .iter()
                .map(|target| {
                    (
                        target.target_key.clone(),
                        target.source.as_str().to_string(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            for (key, source) in &case.expected.target_sources {
                assert_eq!(
                    target_sources.get(key),
                    Some(source),
                    "{} source for {}",
                    case.id,
                    key
                );
            }

            for alias in &case.expected.aliases {
                assert!(
                    graph.aliases.iter().any(|value| value == alias),
                    "{} missing alias {}",
                    case.id,
                    alias
                );
            }

            if let Some(title) = case.expected.strongest_target_title.as_deref() {
                let target = graph
                    .targets
                    .iter()
                    .find(|target| target.target_key == "S01E01")
                    .unwrap_or_else(|| panic!("{} missing S01E01", case.id));
                assert_eq!(target.title, title, "{} strongest duplicate", case.id);
                assert_eq!(
                    target.tvdb_episode_id.as_deref(),
                    Some("15001"),
                    "{} strongest TVDB identity",
                    case.id
                );
                assert_eq!(
                    target.anidb_episode_id.as_deref(),
                    Some("25001"),
                    "{} strongest AniDB identity",
                    case.id
                );
            }

            let targets = graph.to_new_acquisition_targets(case.input.release_delay_seconds, now);
            assert_eq!(
                targets.len(),
                graph.targets.len(),
                "{} new target conversion",
                case.id
            );
            assert!(
                targets
                    .iter()
                    .all(|target| target.media_type == Some(MediaType::Anime)),
                "{} converted targets must remain anime",
                case.id
            );

            let snapshot = graph.to_graph_snapshot_input(None, "default");
            assert_eq!(snapshot.fingerprint, graph.fingerprint, "{}", case.id);
            assert_eq!(snapshot.media_type, MediaType::Anime, "{}", case.id);
            assert!(
                snapshot.graph.is_object(),
                "{} snapshot graph must be structured",
                case.id
            );
        }
    }

    #[test]
    fn rr3e_exact_alias_episode_match_is_high_confidence() {
        let score = score_anime_candidate(
            &rr3e_scoring_context(),
            &rr3e_candidate("[SubsPlease] Example Title - 01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(score.confidence, ReleaseConfidence::High);
        assert_eq!(
            score.alias_matches.first().map(|item| item.kind),
            Some(AnimeAliasMatchKind::Exact)
        );
        assert_eq!(
            score
                .target_matches
                .first()
                .map(|item| item.target_key.as_str()),
            Some("S01E01")
        );
        assert!(score.review_reasons.is_empty());
        assert!(score.rejection_reasons.is_empty());
    }

    #[test]
    fn rr3e_prefix_alias_with_episode_is_medium_not_high() {
        let score = score_anime_candidate(
            &rr3e_scoring_context(),
            &rr3e_candidate("[SubsPlease] Example Title Final - 02 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(score.confidence, ReleaseConfidence::Medium);
        assert_eq!(
            score.alias_matches.first().map(|item| item.kind),
            Some(AnimeAliasMatchKind::Prefix)
        );
        assert_eq!(
            score
                .target_matches
                .first()
                .map(|item| item.target_key.as_str()),
            Some("S01E02")
        );
    }

    #[test]
    fn rr3e_ambiguous_alias_requires_review() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("rr3e-ambiguous".to_string()),
            aliases: vec![
                "Example Title Final".to_string(),
                "Example Title Special".to_string(),
            ],
            targets: rr3e_scoring_context().targets,
        };
        let score = score_anime_candidate(
            &context,
            &rr3e_candidate("[SubsPlease] Example Title - 01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Deferred);
        assert_eq!(score.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            score
                .review_reasons
                .iter()
                .any(|reason| reason == "ambiguous_alias_match")
        );
        assert!(score.rejection_reasons.is_empty());
    }

    #[test]
    fn rr3e_wrong_alias_is_rejected_even_when_episode_number_matches() {
        let score = score_anime_candidate(
            &rr3e_scoring_context(),
            &rr3e_candidate("[SubsPlease] Different Title - 01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Rejected);
        assert_eq!(score.confidence, ReleaseConfidence::Low);
        assert!(
            score
                .rejection_reasons
                .iter()
                .any(|reason| reason == "no_graph_alias_match")
        );
    }

    #[test]
    fn rr3f_season_pack_file_list_maps_all_wanted_targets() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title S01 Batch [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "1".to_string(),
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "2".to_string(),
                file_id: Some("2".to_string()),
                file_index: Some(2),
                path: "Example Title - 02 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage(&context, &candidate, &files);

        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["1", "2"]);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01", "S01E02"]
        );
        assert!(plan.review_reasons.is_empty());
        assert!(plan.rejection_reasons.is_empty());
    }

    #[test]
    fn rr3f_pack_with_unmapped_media_file_requires_review() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title S01 Batch [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "1".to_string(),
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "bonus".to_string(),
                file_id: Some("bonus".to_string()),
                file_index: Some(2),
                path: "Bonus Feature [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage(&context, &candidate, &files);

        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            plan.review_reasons
                .iter()
                .any(|reason| reason == "unmapped_media_files")
        );
    }
}
