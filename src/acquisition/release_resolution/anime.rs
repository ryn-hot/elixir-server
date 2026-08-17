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
        release_resolution::tv::{
            TvParsedRelease, TvQuality, TvReleaseSource, TvResolution, TvSonarrStyleResolver,
        },
        subscriptions::{AcquisitionTargetState, NewAcquisitionTarget},
    },
    db::models::MediaType,
    extensions::ExternalIds,
    library::{
        AniListSeasonChainEntry, AniZipMapping, anizip_prefers_mainline_numbering,
        resolve_anizip_target_numbers,
    },
};

pub const ANIME_SHOKO_STYLE_RESOLVER_VERSION: &str = "rr3-anime-shoko-style-v0";
pub const SHOKO_REFERENCE_COMMIT: &str = "74a673ed57daef76ac6ac1c745728bebcfbd870b";
pub const SHOKO_REFERENCE_REPOSITORY: &str = "https://github.com/ShokoAnime/ShokoServer";
pub const ANIME_PRE_DOWNLOAD_PARSER_VERSION: &str = "rr3d-anime-pre-download-parser-v0";
pub const ANIME_SONARR_ADAPTER_VERSION: &str = "rr3p-anime-sonarr-adapter-v0";
pub const ANIME_PARSER_PROVENANCE_SCHEMA_VERSION: u32 = 1;

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
static FILE_SIZE_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:^|[^\p{L}\p{N}])0*(?P<size>\d{1,5})(?:\.\d+)?\s*(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB)(?:$|[^\p{L}\p{N}])")
        .expect("valid anime file size token regex")
});
static RESOLUTION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?P<resolution>2160p|2160i|1440p|1080p10|1080p|1080i|720p|720i|576p|576i|540p|480p|480i|360p|4096x2160|3840x2160|1920x1080|1280x720|640x480|848x480|960p|4kto1080p|BluRay1080p|BD1080p|BluRay720p|BD720p|4k|uhd|fhd)")
        .expect("valid anime resolution regex")
});
static CODEC_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(?P<codec>HEVC|H\.?265|x265|H\.?264|x264|AVC|AV1|VP9|XviD|DivX|MPEG[-_. ]?2)\b",
    )
    .expect("valid anime codec regex")
});
static AUDIO_CODEC_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?P<audio>AAC|FLAC|OPUS|EAC3|AC3|DTS(?:[-_. ]?HD)?|TrueHD|DDP?|Dolby[-_. ]?Digital)\b")
        .expect("valid anime audio codec regex")
});
static WEB_DL_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bWEB[-_. ]?DL\b").expect("valid web-dl source regex"));
static WEB_RIP_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bWEB[-_. ]?Rip\b").expect("valid web-rip source regex"));
static BLURAY_SOURCE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:Blu[-_. ]?Ray|BDRip|BRRip|BD[-_. ]?Remux|BD[-_. ]?Box|Remux)\b")
        .expect("valid bluray source regex")
});
static HDTV_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bHDTV\b").expect("valid hdtv source regex"));
static DVD_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:DVD|DVDRip)\b").expect("valid dvd source regex"));
static RAW_HD_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bRaw[-_. ]?HD\b").expect("valid raw-hd source regex"));
static PDTV_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bPDTV\b").expect("valid pdtv source regex"));
static DSR_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDSR\b").expect("valid dsr source regex"));
static SDTV_SOURCE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:SDTV|TVRip)\b").expect("valid sdtv source regex"));
static DUAL_AUDIO_SIGNAL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:(?:dual|2)[-_. ]?audio|dual[-_. ]?dub|multi[-_. ]?audio|english[-_. ]?dub|eng[-_. ]?dub|dubbed)\b")
        .expect("valid anime dual-audio signal regex")
});
static ENGLISH_DUB_SIGNAL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:english[-_. ]?dub|eng[-_. ]?dub|dubbed)\b")
        .expect("valid anime English dub signal regex")
});
static MULTI_SUB_SIGNAL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(?:multi[-_. ]?subs?|multisub|multiple[-_. ]?subtitles?)\b|简繁|簡繁|雙語|双语",
    )
    .expect("valid anime multi-sub signal regex")
});
static ANIME_LANGUAGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:TRUEFRENCH|SUBFRENCH|VOSTFR|VF2?|VFQ|VFF|VFI|ENGLISH|ENG|FRENCH|FRE|FRA|FR|GERMAN|SWISSGERMAN|GER|ITALIAN|ITALY|ITA|SPANISH|ESPA(?:Ñ|N)OL|CASTELLANO|SPA|ESP|CZECH|CZE|JAPANESE|JPN|JAP|JA|CHINESE|CANTONESE|MANDARIN|CHI|CHS|CHT|BIG5|GB|KOREAN|KOR|LATVIAN|LAT|LAV|LV|RUSSIAN|RUS|RU|POLISH|PL(?:LEK|DUB)?|DUBPL|LEKPL|DANISH|DAN|DUTCH|FLEMISH|PORTUGUESE|POR|MULTI[-_. ]?SUBS?|MULTISUB|MULTI|SUBS?|SUBBED|DUAL(?:[-_. ]AUDIO)?)\b")
        .expect("valid anime language regex")
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSonarrParseFacts {
    pub parser_version: String,
    pub original_title: String,
    pub series_title: Option<String>,
    pub title_without_year: Option<String>,
    pub title_year: Option<i32>,
    pub all_titles: Vec<String>,
    pub season_number: Option<i32>,
    pub season_end_number: Option<i32>,
    pub episode_numbers: Vec<i32>,
    pub absolute_episode_numbers: Vec<i32>,
    pub special_absolute_episode_numbers: Vec<String>,
    pub episode_start_number: Option<i32>,
    pub episode_end_number: Option<i32>,
    pub release_kind: ReleaseKind,
    pub batch_kind: AnimeBatchKind,
    pub full_season: bool,
    pub full_series: bool,
    pub is_partial_season: bool,
    pub is_multi_season: bool,
    pub is_season_extra: bool,
    pub season_part: Option<i32>,
    pub daily_part: Option<i32>,
    pub is_mini_series: bool,
    pub special: bool,
    pub is_split_episode: bool,
    pub release_group: Option<String>,
    pub release_hash: Option<String>,
    pub release_tokens: Option<String>,
    pub quality: AnimeParsedQuality,
    pub audio_languages: Vec<String>,
    pub raw: Option<TvParsedRelease>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSignalFacts {
    pub parser_version: String,
    pub classifier_hints: Vec<AnimeClassifierSignal>,
    pub title_candidates: Vec<String>,
    pub normalized_title_candidates: Vec<String>,
    pub title_season_alias_candidates: Vec<String>,
    pub fallback_absolute_episode_hypotheses: Vec<i32>,
    pub fallback_season_one_episode_hypotheses: Vec<i32>,
    pub bounded_explicit_ranges: Vec<AnimeExplicitRange>,
    pub dual_audio: bool,
    pub english_dub: bool,
    pub multi_sub: bool,
    pub subtitle_languages: Vec<String>,
    pub leading_bracket_release_group: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeClassifierSignal {
    pub parser: String,
    pub title: String,
    pub alt_titles: Vec<String>,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub absolute_episode: Option<i32>,
    pub parser_confidence_basis_points: u16,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeExplicitRange {
    pub start: i32,
    pub end: i32,
    pub source: String,
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
    #[serde(default)]
    pub release_hash: Option<String>,
    pub quality: AnimeParsedQuality,
    pub audio_languages: Vec<String>,
    pub subtitle_languages: Vec<String>,
    #[serde(default)]
    pub sonarr_facts: AnimeSonarrParseFacts,
    #[serde(default)]
    pub anime_signal_facts: AnimeSignalFacts,
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
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub anilist_season_id: Option<String>,
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
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub anilist_season_id: Option<String>,
    pub kind: AnimeAliasMatchKind,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeScopedAlias {
    pub display: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub anilist_season_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCandidateTarget {
    pub target_key: String,
    pub canonical_key: Option<String>,
    pub title: String,
    pub season_number: Option<i32>,
    #[serde(default)]
    pub anilist_season_id: Option<String>,
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
    #[serde(default)]
    pub scoped_aliases: Vec<AnimeScopedAlias>,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnimeCoverageOptions {
    pub file_selection_supported: bool,
}

/// Canonical, server-authored interpretation selected by the local model.
/// This is evidence for the existing resolver, not a model-authored match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeSemanticCandidateEvidence {
    pub season_number: i32,
    pub release_season_numbers: Vec<i32>,
    pub episode_number_offset: i32,
    pub anilist_season_id: Option<String>,
    pub aliases: Vec<String>,
    pub numbering: AnimeSemanticNumberingEvidence,
    pub media_kind: AnimeSemanticMediaKindEvidence,
    pub episode_numbers: Vec<i32>,
    pub absolute_episode_numbers: Vec<i32>,
    pub target_keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimeSemanticNumberingEvidence {
    Seasonal,
    Absolute,
    EntityOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimeSemanticMediaKindEvidence {
    Episode,
    Range,
    SeasonPack,
    SeriesPack,
    Movie,
    Special,
    Ova,
}

/// Post-inference identity strength used only to arbitrate candidates that
/// already selected the same server-authored target. This is deliberately not
/// part of the model contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnimeSemanticCanonicalIdentity {
    Exact,
    SubstantiveExtension,
    Other,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeReconciliationOutcome {
    Agreement,
    Translation,
    Augmentation,
    BenignMismatch,
    TrueContradiction,
    Unexplainable,
}

impl Default for AnimeReconciliationOutcome {
    fn default() -> Self {
        Self::Unexplainable
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeGraphReconciliation {
    pub outcome: AnimeReconciliationOutcome,
    pub graph_fingerprint: Option<String>,
    pub identity_agreed: bool,
    pub alias_best_score: Option<f64>,
    pub alias_margin: Option<f64>,
    pub target_matches: Vec<AnimeCandidateTargetMatch>,
    pub sonarr_target_matches: Vec<AnimeCandidateTargetMatch>,
    pub anime_signal_target_matches: Vec<AnimeCandidateTargetMatch>,
    pub agreed_target_keys: Vec<String>,
    pub augmented_target_keys: Vec<String>,
    pub contradiction_reasons: Vec<String>,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
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
    pub reconciliation: AnimeGraphReconciliation,
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
    #[serde(default)]
    pub requires_file_selection: bool,
    pub selected_file_keys: Vec<String>,
    pub entries: Vec<AnimeFileCoverageEntry>,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeParserDiagnostics {
    pub parser_provenance: AnimeParserProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeParserProvenance {
    pub schema_version: u32,
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub parser_version: String,
    pub sonarr_adapter_version: String,
    pub parsed: AnimeParsedReleaseProvenance,
    pub sonarr: AnimeSonarrParserProvenance,
    pub anime_signals: AnimeSignalParserProvenance,
    pub graph: AnimeGraphMappingProvenance,
    pub reconciliation: AnimeReconciliationProvenance,
    pub outcome: AnimeMatchOutcome,
    pub confidence: ReleaseConfidence,
    pub score: f64,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
    pub coverage: Option<AnimeCoverageProvenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeParsedReleaseProvenance {
    pub original_title: String,
    pub normalized_title: Option<String>,
    pub series_title: Option<String>,
    pub season_number: Option<i32>,
    pub episode_numbers: Vec<i32>,
    pub absolute_episode_numbers: Vec<i32>,
    pub episode_type: AnimeEpisodeType,
    pub batch_kind: AnimeBatchKind,
    pub release_group: Option<String>,
    pub release_hash: Option<String>,
    pub quality: AnimeParsedQuality,
    pub confidence: ReleaseConfidence,
    pub review_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSonarrParserProvenance {
    pub parser_version: String,
    pub matched_pattern_id: Option<String>,
    pub matched_pattern_id_source: String,
    pub original_title: String,
    pub parsed_title: Option<String>,
    pub title_without_year: Option<String>,
    pub title_year: Option<i32>,
    pub season_number: Option<i32>,
    pub season_end_number: Option<i32>,
    pub episode_numbers: Vec<i32>,
    pub absolute_episode_numbers: Vec<i32>,
    pub special_absolute_episode_numbers: Vec<String>,
    pub release_kind: ReleaseKind,
    pub batch_kind: AnimeBatchKind,
    pub full_season: bool,
    pub full_series: bool,
    pub is_partial_season: bool,
    pub is_multi_season: bool,
    pub special: bool,
    pub is_split_episode: bool,
    pub release_group: Option<String>,
    pub release_hash: Option<String>,
    pub release_tokens: Option<String>,
    pub quality: AnimeParsedQuality,
    pub audio_languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeSignalParserProvenance {
    pub parser_version: String,
    pub classifier_hints: Vec<AnimeClassifierSignal>,
    pub title_candidates: Vec<String>,
    pub normalized_title_candidates: Vec<String>,
    pub title_season_alias_candidates: Vec<String>,
    pub fallback_absolute_episode_hypotheses: Vec<i32>,
    pub fallback_season_one_episode_hypotheses: Vec<i32>,
    pub bounded_explicit_ranges: Vec<AnimeExplicitRange>,
    pub dual_audio: bool,
    pub english_dub: bool,
    pub multi_sub: bool,
    pub subtitle_languages: Vec<String>,
    pub leading_bracket_release_group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeGraphMappingProvenance {
    pub graph_fingerprint: Option<String>,
    pub alias_count: usize,
    pub target_count: usize,
    pub alias_matches: Vec<AnimeAliasMatch>,
    pub target_matches: Vec<AnimeCandidateTargetMatch>,
    pub sonarr_target_matches: Vec<AnimeCandidateTargetMatch>,
    pub anime_signal_target_matches: Vec<AnimeCandidateTargetMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeReconciliationProvenance {
    pub outcome: AnimeReconciliationOutcome,
    pub identity_agreed: bool,
    pub alias_best_score: Option<f64>,
    pub alias_margin: Option<f64>,
    pub agreed_target_keys: Vec<String>,
    pub augmented_target_keys: Vec<String>,
    pub contradiction_reasons: Vec<String>,
    pub review_reasons: Vec<String>,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeCoverageProvenance {
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub release_kind: ReleaseKind,
    pub confidence: ReleaseConfidence,
    pub requires_file_list: bool,
    pub requires_file_selection: bool,
    pub selected_file_keys: Vec<String>,
    pub entry_count: usize,
    pub covered_target_keys: Vec<String>,
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
            anilist_season_id: Some(target.anilist_season_id.clone()),
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
            scoped_aliases: graph.scoped_aliases.clone(),
            targets: graph
                .targets
                .iter()
                .map(AnimeCandidateTarget::from_graph_target)
                .collect(),
        }
    }
}

pub fn parse_anime_sonarr_adapter_facts(input: &str) -> AnimeSonarrParseFacts {
    let resolver = TvSonarrStyleResolver;
    let coordinate_input = strip_anime_crc32_tokens(input);
    sonarr_facts_from_tv_parse(resolver.parse_title(&coordinate_input))
}

fn strip_anime_crc32_tokens(input: &str) -> String {
    CRC32_RE.replace_all(input, " ").into_owned()
}

fn sonarr_facts_from_tv_parse(parsed: TvParsedRelease) -> AnimeSonarrParseFacts {
    let mut all_titles = parsed.series_title_info.all_titles.clone();
    if let Some(title) = parsed.normalized_series_title.as_deref() {
        all_titles.push(title.to_string());
    }
    if let Some(title) = parsed.series_title_info.title_without_year.as_deref() {
        all_titles.push(title.to_string());
    }
    let all_titles = dedup_clean_strings(all_titles);

    let mut episode_numbers = parsed.episode_numbers.clone();
    episode_numbers.sort_unstable();
    episode_numbers.dedup();

    let mut absolute_episode_numbers = parsed
        .anime_absolute_hints
        .iter()
        .copied()
        .filter(|episode| !sonarr_absolute_hint_looks_like_year(&parsed.original_title, *episode))
        .filter(|episode| !number_is_file_size_token(&parsed.original_title, *episode))
        .collect::<Vec<_>>();
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

    let special_absolute_episode_numbers = if parsed.special && parsed.season_number == Some(0) {
        episode_numbers
            .iter()
            .map(|episode| format!("S00E{episode:02}"))
            .collect()
    } else if parsed.special {
        absolute_episode_numbers
            .iter()
            .map(|episode| format!("S{episode:04}"))
            .collect()
    } else {
        Vec::new()
    };

    let season_end_number = parsed.season_end_number;
    let is_multi_season = season_end_number
        .zip(parsed.season_number)
        .is_some_and(|(end, start)| end > start)
        || parsed.release_kind == ReleaseKind::MultiSeasonPack;

    AnimeSonarrParseFacts {
        parser_version: ANIME_SONARR_ADAPTER_VERSION.to_string(),
        original_title: parsed.original_title.clone(),
        series_title: parsed.normalized_series_title.clone(),
        title_without_year: parsed.series_title_info.title_without_year.clone(),
        title_year: parsed.series_title_info.year,
        all_titles,
        season_number: parsed.season_number,
        season_end_number,
        episode_numbers,
        absolute_episode_numbers,
        special_absolute_episode_numbers,
        episode_start_number,
        episode_end_number,
        release_kind: parsed.release_kind,
        batch_kind: anime_batch_kind_from_release_kind(parsed.release_kind),
        full_season: parsed.full_season,
        full_series: parsed.full_series,
        is_partial_season: parsed.is_partial_season,
        is_multi_season,
        is_season_extra: parsed.is_season_extra,
        season_part: parsed.season_part,
        daily_part: parsed.daily_part,
        is_mini_series: parsed.is_mini_series,
        special: parsed.special,
        is_split_episode: parsed.is_split_episode,
        release_group: parsed.release_group.clone(),
        release_hash: parsed.release_hash.clone(),
        release_tokens: parsed.release_tokens.clone(),
        quality: anime_quality_from_tv_quality(&parsed.quality, &parsed.original_title),
        audio_languages: normalize_sonarr_languages(&parsed.modifiers.languages),
        raw: Some(parsed),
    }
}

fn anime_batch_kind_from_release_kind(release_kind: ReleaseKind) -> AnimeBatchKind {
    match release_kind {
        ReleaseKind::Single => AnimeBatchKind::Single,
        ReleaseKind::MultiEpisode => AnimeBatchKind::Range,
        ReleaseKind::SeasonPack => AnimeBatchKind::SeasonPack,
        ReleaseKind::MultiSeasonPack => AnimeBatchKind::MultiSeasonPack,
        ReleaseKind::SeriesPack => AnimeBatchKind::CompleteSeries,
        ReleaseKind::Unknown => AnimeBatchKind::UnknownBatch,
    }
}

fn sonarr_absolute_hint_looks_like_year(title: &str, episode: i32) -> bool {
    if !(1950..=2100).contains(&episode) {
        return false;
    }
    let token = episode.to_string();
    extract_bracket_segments(title)
        .iter()
        .any(|segment| segment.trim() == token)
}

fn anime_quality_from_tv_quality(quality: &TvQuality, title: &str) -> AnimeParsedQuality {
    let normalized = title.replace(['.', '_', '-'], " ");
    AnimeParsedQuality {
        resolution: quality.resolution.map(tv_resolution_label),
        source: quality.source.map(tv_source_label),
        video_codec: quality.codec.as_deref().map(normalize_codec),
        audio_codec: None,
        dual_audio: DUAL_AUDIO_SIGNAL_RE.is_match(&normalized),
        multi_sub: MULTI_SUB_SIGNAL_RE.is_match(&normalized),
    }
}

fn tv_resolution_label(resolution: TvResolution) -> String {
    match resolution {
        TvResolution::R360p => "360p",
        TvResolution::R480p => "480p",
        TvResolution::R540p => "540p",
        TvResolution::R576p => "576p",
        TvResolution::R720p => "720p",
        TvResolution::R1080p => "1080p",
        TvResolution::R2160p => "2160p",
    }
    .to_string()
}

fn tv_source_label(source: TvReleaseSource) -> String {
    match source {
        TvReleaseSource::BluRay | TvReleaseSource::BdRip | TvReleaseSource::BrRip => "blu_ray",
        TvReleaseSource::WebDl => "web_dl",
        TvReleaseSource::WebRip => "web_rip",
        TvReleaseSource::Hdtv => "hdtv",
        TvReleaseSource::Dvd => "dvd",
        TvReleaseSource::Dsr => "dsr",
        TvReleaseSource::Pdtv => "pdtv",
        TvReleaseSource::Sdtv | TvReleaseSource::TvRip => "sdtv",
        TvReleaseSource::RawHd => "raw_hd",
    }
    .to_string()
}

fn normalize_sonarr_languages(languages: &[String]) -> Vec<String> {
    let mut normalized = BTreeSet::new();
    for language in languages {
        let token = language
            .replace(['-', '_', '.', '/'], " ")
            .replace(['ñ', 'Ñ'], "N")
            .to_ascii_uppercase();
        let token = token.trim();
        if let Some(language) = normalize_anime_language_token(token) {
            normalized.insert(language.to_string());
        }
    }
    normalized.into_iter().collect()
}

fn build_anime_signal_facts(
    input: &str,
    classifier_hints: &[elixir_classifier::hint::ClassificationHint],
    bracket_segments: &[String],
    leading_bracket_release_group: Option<String>,
    quality: &AnimeParsedQuality,
    subtitle_languages: &[String],
) -> AnimeSignalFacts {
    let mut classifier_signals = Vec::new();
    let mut title_candidates = BTreeSet::new();
    let mut title_season_alias_candidates = BTreeSet::new();
    let mut fallback_absolute_episode_hypotheses = BTreeSet::new();
    let mut fallback_season_one_episode_hypotheses = BTreeSet::new();

    for hint in classifier_hints {
        let cleaned_title = cleanup_anime_title(&hint.title);
        if !cleaned_title.is_empty() {
            title_candidates.insert(cleaned_title.clone());
            if let Some(season) = hint.season.filter(|season| *season > 0) {
                title_season_alias_candidates.insert(format!("{cleaned_title} S{season:02}"));
                title_season_alias_candidates.insert(format!("{cleaned_title} Season {season}"));
            }
        }

        let alt_titles = hint
            .alt_titles
            .iter()
            .map(|title| cleanup_anime_title(title))
            .filter(|title| !title.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for title in &alt_titles {
            title_candidates.insert(title.clone());
            if let Some(season) = hint.season.filter(|season| *season > 0) {
                title_season_alias_candidates.insert(format!("{title} S{season:02}"));
                title_season_alias_candidates.insert(format!("{title} Season {season}"));
            }
        }

        if let Some(absolute) = hint.absolute_episode.filter(|episode| *episode > 0) {
            fallback_absolute_episode_hypotheses.insert(absolute);
        }
        if hint.season.unwrap_or(1) == 1
            && let Some(episode) = hint.episode.filter(|episode| *episode > 0)
        {
            fallback_season_one_episode_hypotheses.insert(episode);
        }

        classifier_signals.push(AnimeClassifierSignal {
            parser: hint.parser.to_string(),
            title: cleaned_title,
            alt_titles,
            year: hint.year,
            season: hint.season,
            episode: hint.episode,
            absolute_episode: hint.absolute_episode,
            parser_confidence_basis_points: parser_confidence_basis_points(hint.parser_confidence),
        });
    }

    for episode in parse_absolute_episode_numbers(input, bracket_segments) {
        fallback_absolute_episode_hypotheses.insert(episode);
    }

    let normalized_title_candidates = title_candidates
        .iter()
        .map(|title| normalize_anime_title(title))
        .filter(|title| !title.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let normalized = input.replace(['.', '_', '-'], " ");
    AnimeSignalFacts {
        parser_version: ANIME_PRE_DOWNLOAD_PARSER_VERSION.to_string(),
        classifier_hints: classifier_signals,
        title_candidates: title_candidates.into_iter().collect(),
        normalized_title_candidates,
        title_season_alias_candidates: title_season_alias_candidates.into_iter().collect(),
        fallback_absolute_episode_hypotheses: fallback_absolute_episode_hypotheses
            .into_iter()
            .collect(),
        fallback_season_one_episode_hypotheses: fallback_season_one_episode_hypotheses
            .into_iter()
            .collect(),
        bounded_explicit_ranges: parse_bounded_explicit_ranges(input, bracket_segments),
        dual_audio: quality.dual_audio,
        english_dub: ENGLISH_DUB_SIGNAL_RE.is_match(&normalized),
        multi_sub: quality.multi_sub,
        subtitle_languages: subtitle_languages.to_vec(),
        leading_bracket_release_group,
    }
}

fn parser_confidence_basis_points(confidence: f32) -> u16 {
    (confidence.clamp(0.0, 1.0) * 10_000.0).round() as u16
}

fn parse_bounded_explicit_ranges(
    input: &str,
    bracket_segments: &[String],
) -> Vec<AnimeExplicitRange> {
    let mut ranges = BTreeMap::<(i32, i32), String>::new();
    if let Some((_, start, Some(end))) = parse_sxxeyy_numbers(input) {
        push_explicit_range(&mut ranges, start, end, "sxxeyy");
    }
    for captures in BATCH_EPISODE_RANGE_RE.captures_iter(input) {
        if let Some(start) = parse_capture_i32(&captures, "start") {
            let end = parse_capture_i32(&captures, "end").unwrap_or(start);
            push_explicit_range(&mut ranges, start, end, "batch");
        }
    }
    for captures in DASH_EPISODE_RE.captures_iter(input) {
        if let Some(start) = parse_capture_i32(&captures, "start") {
            let end = parse_capture_i32(&captures, "end").unwrap_or(start);
            push_explicit_range(&mut ranges, start, end, "dash");
        }
    }
    for segment in bracket_segments.iter().skip(1) {
        if let Some((start, end)) = parse_episode_segment_numbers(segment) {
            push_explicit_range(&mut ranges, start, end, "bracket");
        }
    }
    ranges
        .into_iter()
        .map(|((start, end), source)| AnimeExplicitRange { start, end, source })
        .collect()
}

fn push_explicit_range(
    ranges: &mut BTreeMap<(i32, i32), String>,
    start: i32,
    end: i32,
    source: &str,
) {
    if start <= 0 || end <= start || end - start > 200 {
        return;
    }
    ranges
        .entry((start, end))
        .or_insert_with(|| source.to_string());
}

fn merge_anime_quality(
    sonarr_quality: &AnimeParsedQuality,
    anime_quality: AnimeParsedQuality,
) -> AnimeParsedQuality {
    AnimeParsedQuality {
        resolution: anime_quality
            .resolution
            .or_else(|| sonarr_quality.resolution.clone()),
        source: anime_quality
            .source
            .or_else(|| sonarr_quality.source.clone()),
        video_codec: anime_quality
            .video_codec
            .or_else(|| sonarr_quality.video_codec.clone()),
        audio_codec: anime_quality.audio_codec,
        dual_audio: anime_quality.dual_audio || sonarr_quality.dual_audio,
        multi_sub: anime_quality.multi_sub || sonarr_quality.multi_sub,
    }
}

fn merge_i32_values(
    left: impl IntoIterator<Item = i32>,
    right: impl IntoIterator<Item = i32>,
) -> Vec<i32> {
    left.into_iter()
        .chain(right)
        .filter(|value| *value > 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn dedup_clean_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| cleanup_anime_title(&value))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn sorted_unique_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn parse_anime_release_title(input: &str) -> AnimeParsedRelease {
    let original_title = input.trim().to_string();
    let normalized_input = normalize_fullwidth_digits(&original_title);
    let leading_bracket_release_group = parse_anime_release_group(&normalized_input);
    let sonarr_facts = parse_anime_sonarr_adapter_facts(&original_title);
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
        .or_else(|| {
            sonarr_facts
                .series_title
                .clone()
                .map(|title| cleanup_anime_title(&title))
        })
        .filter(|title| !title.trim().is_empty());
    let normalized_title = series_title.as_deref().map(normalize_anime_title);
    let mut alt_title_candidates = classifier_hint
        .map(|hint| hint.alt_titles.clone())
        .unwrap_or_default();
    alt_title_candidates.extend(sonarr_facts.all_titles.clone());
    let alt_titles = dedup_clean_strings(alt_title_candidates);

    let sxxeyy = parse_sxxeyy_numbers(&normalized_input);
    let season_number = sxxeyy
        .as_ref()
        .map(|parsed| parsed.0)
        .or_else(|| parse_season_dash_episode(&normalized_input).map(|parsed| parsed.0))
        .or_else(|| parse_season_number(&normalized_input))
        .or(sonarr_facts.season_number)
        .or_else(|| classifier_hint.and_then(|hint| hint.season));
    let mut episode_numbers = sxxeyy
        .as_ref()
        .map(|parsed| expand_episode_numbers(parsed.1, parsed.2.unwrap_or(parsed.1), 200))
        .or_else(|| {
            parse_season_dash_episode(&normalized_input)
                .map(|parsed| expand_episode_numbers(parsed.1, parsed.1, 200))
        })
        .unwrap_or_default();
    episode_numbers = merge_i32_values(episode_numbers, sonarr_facts.episode_numbers.clone());

    let mut absolute_episode_numbers =
        parse_absolute_episode_numbers(&normalized_input, &bracket_segments);
    if absolute_episode_numbers.is_empty()
        && episode_numbers.is_empty()
        && let Some(hint) = classifier_hint
        && let Some(absolute) = hint.absolute_episode
    {
        absolute_episode_numbers.push(absolute);
    }
    absolute_episode_numbers = merge_i32_values(
        absolute_episode_numbers,
        sonarr_facts.absolute_episode_numbers.clone(),
    );

    let episode_start_number = episode_numbers
        .first()
        .copied()
        .or_else(|| absolute_episode_numbers.first().copied());
    let episode_end_number = episode_numbers
        .last()
        .copied()
        .or_else(|| absolute_episode_numbers.last().copied());
    let mut episode_type = parse_anime_episode_type(&normalized_input);
    if sonarr_facts.special && episode_type == AnimeEpisodeType::Normal {
        episode_type = AnimeEpisodeType::Special;
    }
    let mut batch_kind = parse_anime_batch_kind(
        &normalized_input,
        episode_type,
        &episode_numbers,
        &absolute_episode_numbers,
    );
    if matches!(
        sonarr_facts.batch_kind,
        AnimeBatchKind::CompleteSeries
            | AnimeBatchKind::SeasonPack
            | AnimeBatchKind::MultiSeasonPack
    ) && !matches!(
        batch_kind,
        AnimeBatchKind::CompleteSeries
            | AnimeBatchKind::SeasonPack
            | AnimeBatchKind::MultiSeasonPack
    ) && episode_numbers.is_empty()
        && absolute_episode_numbers.is_empty()
    {
        batch_kind = sonarr_facts.batch_kind;
    } else if sonarr_facts.batch_kind == AnimeBatchKind::Range
        && batch_kind == AnimeBatchKind::Single
    {
        batch_kind = AnimeBatchKind::Range;
    }
    let version = parse_anime_version(&normalized_input);
    let crc32 = parse_crc32(&normalized_input);
    let quality = merge_anime_quality(
        &sonarr_facts.quality,
        parse_anime_quality(&normalized_input),
    );
    let (audio_languages, subtitle_languages) = parse_anime_languages(&normalized_input);
    let audio_languages = dedup_clean_strings(
        audio_languages
            .into_iter()
            .chain(sonarr_facts.audio_languages.clone()),
    );
    let release_group = leading_bracket_release_group
        .clone()
        .or_else(|| sonarr_facts.release_group.clone());
    let release_hash = sonarr_facts.release_hash.clone().or_else(|| crc32.clone());
    let anime_signal_facts = build_anime_signal_facts(
        &normalized_input,
        &classifier_hints,
        &bracket_segments,
        leading_bracket_release_group,
        &quality,
        &subtitle_languages,
    );

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
        release_hash,
        quality,
        audio_languages,
        subtitle_languages,
        sonarr_facts,
        anime_signal_facts,
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
    #[serde(default)]
    pub scoped_aliases: Vec<AnimeScopedAlias>,
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
        let episode_number_offsets = self
            .seasons
            .iter()
            .filter_map(|season| {
                let offset = crate::anime_matching::complete_episode_number_offset(
                    season.episodes,
                    self.targets
                        .iter()
                        .filter(|target| target.anilist_season_id == season.anilist_id)
                        .filter_map(|target| {
                            target.episode_number.or(target.absolute_episode_number)
                        }),
                );
                (offset > 0).then(|| (season.anilist_id.as_str(), offset))
            })
            .collect::<BTreeMap<_, _>>();
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
                    "scopedAliases": self.scoped_aliases,
                    "anilistRootId": self.root_anilist_id,
                    "anilistSeasonId": target.anilist_season_id,
                    "episodeNumberOffset": episode_number_offsets
                        .get(target.anilist_season_id.as_str())
                        .copied()
                        .unwrap_or(0),
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
    let mut scoped_aliases = BTreeMap::<String, AnimeScopedAlias>::new();
    insert_alias(&mut aliases, &input.title);
    let mut targets_by_key: BTreeMap<String, AnimeGraphTarget> = BTreeMap::new();
    let mut mapped_counts_by_season = HashMap::<String, usize>::new();

    for season_input in &season_inputs {
        insert_alias(&mut aliases, &season_input.season.title);
        insert_scoped_alias(
            &mut scoped_aliases,
            &season_input.season.title,
            "anilist_season_title",
            &season_input.season,
        );
        insert_generated_season_aliases(&mut scoped_aliases, &input.title, &season_input.season);
        if let Some(mapping) = season_input.mapping.as_ref() {
            let mut localized_titles = mapping.titles.iter().collect::<Vec<_>>();
            localized_titles.sort_by(|left, right| left.0.cmp(right.0));
            for (language, title) in localized_titles {
                insert_alias(&mut aliases, title);
                insert_scoped_alias_with_language(
                    &mut scoped_aliases,
                    title,
                    "anizip_title",
                    Some(language),
                    &season_input.season,
                );
            }
            let prefer_mainline_numbering = anizip_prefers_mainline_numbering(mapping);
            for episode in &mapping.episodes {
                let Some(target) = graph_target_from_anizip(
                    &input,
                    &external_ids,
                    &season_input.season,
                    mapping,
                    episode,
                    prefer_mainline_numbering,
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
            if !target_absolute_episode_already_mapped(&targets_by_key, &target) {
                targets_by_key
                    .entry(target.target_key.clone())
                    .or_insert(target);
            }
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
    let scoped_aliases = scoped_aliases.into_values().collect::<Vec<_>>();
    let fingerprint = graph_fingerprint(
        &input.seed_anilist_id,
        &root_anilist_id,
        &external_ids,
        &targets,
        &scoped_aliases,
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
        scoped_aliases,
        fingerprint,
    }
}

pub fn build_anime_alias_table(context: &AnimeCandidateScoringContext) -> AnimeAliasTable {
    let mut entries_by_key = BTreeMap::<String, AnimeAliasEntry>::new();
    let scoped_normalized = context
        .scoped_aliases
        .iter()
        .map(|alias| normalize_anime_alias(&alias.display))
        .filter(|alias| !alias.is_empty())
        .collect::<BTreeSet<_>>();
    for alias in &context.aliases {
        let normalized = normalize_anime_alias(alias);
        if !scoped_normalized.contains(&normalized) {
            insert_alias_entry(&mut entries_by_key, alias, "graph_alias", 50, None, None);
        }
    }
    for alias in &context.scoped_aliases {
        insert_alias_entry(
            &mut entries_by_key,
            &alias.display,
            &alias.source,
            60,
            alias.season_number,
            alias.anilist_season_id.clone(),
        );
    }
    for target in &context.targets {
        insert_alias_entry(
            &mut entries_by_key,
            &target.title,
            "target_title",
            10,
            target.season_number,
            target.anilist_season_id.clone(),
        );
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

pub fn reconcile_anime_graph(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    alias_matches: &[AnimeAliasMatch],
) -> AnimeGraphReconciliation {
    let sonarr_structured_matches = match_targets_by_season_episode(
        context,
        parsed.sonarr_facts.season_number,
        &parsed.sonarr_facts.episode_numbers,
        "sonarr_season_episode",
        112.0,
    );
    let sonarr_absolute_matches = match_targets_by_absolute_episode(
        context,
        &parsed.sonarr_facts.absolute_episode_numbers,
        "sonarr_absolute_episode",
        108.0,
    );
    let sonarr_target_matches = dedup_target_matches(
        sonarr_structured_matches
            .iter()
            .cloned()
            .chain(sonarr_absolute_matches.iter().cloned()),
    );
    let alias_scoped_target_matches = match_targets_by_alias_scope(context, parsed, alias_matches);
    let anime_signal_target_matches = match_anime_signal_targets(context, parsed);
    let direct_target_matches = match_candidate_targets(context, parsed);

    let has_sonarr_structured_facts = parsed.sonarr_facts.season_number.is_some()
        && !parsed.sonarr_facts.episode_numbers.is_empty();
    let has_sonarr_absolute_facts = !parsed.sonarr_facts.absolute_episode_numbers.is_empty();
    let sonarr_structured_keys = target_identity_keys(&sonarr_structured_matches);
    let sonarr_absolute_keys = target_identity_keys(&sonarr_absolute_matches);
    let sonarr_agreement_keys = sonarr_structured_keys
        .intersection(&sonarr_absolute_keys)
        .cloned()
        .collect::<BTreeSet<_>>();
    let exact_scoped_alias_match = alias_matches.iter().any(|alias| {
        alias.kind == AnimeAliasMatchKind::Exact
            && (alias.season_number.is_some() || alias.anilist_season_id.is_some())
    });
    let exact_scoped_alias_season_conflict =
        exact_scoped_alias_conflicts_with_structured_season(context, parsed, alias_matches);

    let mut contradiction_reasons = Vec::new();
    let mut review_reasons = Vec::new();
    let mut rejection_reasons = Vec::new();
    let mut outcome = AnimeReconciliationOutcome::Unexplainable;
    let mut target_matches = Vec::new();

    if exact_scoped_alias_season_conflict {
        outcome = AnimeReconciliationOutcome::TrueContradiction;
        contradiction_reasons.push("exact_scoped_alias_and_sxxeyy_season_disagree".to_string());
        review_reasons.push("graph_reconciliation_true_contradiction".to_string());
    } else if exact_scoped_alias_match && !alias_scoped_target_matches.is_empty() {
        outcome = AnimeReconciliationOutcome::Translation;
        target_matches = alias_scoped_target_matches.clone();
    } else if has_sonarr_structured_facts
        && has_sonarr_absolute_facts
        && !sonarr_structured_matches.is_empty()
        && !sonarr_absolute_matches.is_empty()
    {
        if sonarr_agreement_keys.is_empty() {
            outcome = AnimeReconciliationOutcome::TrueContradiction;
            contradiction_reasons.push("sonarr_absolute_and_sxxeyy_disagree".to_string());
            review_reasons.push("graph_reconciliation_true_contradiction".to_string());
            target_matches = dedup_target_matches(
                sonarr_structured_matches
                    .iter()
                    .cloned()
                    .chain(sonarr_absolute_matches.iter().cloned()),
            );
        } else {
            outcome = AnimeReconciliationOutcome::Translation;
            target_matches = dedup_target_matches(
                sonarr_structured_matches
                    .iter()
                    .cloned()
                    .chain(sonarr_absolute_matches.iter().cloned())
                    .filter(|item| {
                        target_identity_key(item)
                            .is_some_and(|key| sonarr_agreement_keys.contains(&key))
                    }),
            );
        }
    } else if has_sonarr_structured_facts
        && has_sonarr_absolute_facts
        && (!sonarr_structured_matches.is_empty() || !sonarr_absolute_matches.is_empty())
    {
        outcome = AnimeReconciliationOutcome::BenignMismatch;
        review_reasons.push("sonarr_mixed_numbering_partially_unmapped".to_string());
        target_matches = if !sonarr_target_matches.is_empty() {
            sonarr_target_matches.clone()
        } else {
            direct_target_matches.clone()
        };
    } else if !sonarr_target_matches.is_empty() {
        let sonarr_keys = target_identity_keys(&sonarr_target_matches);
        let additive_matches = anime_signal_target_matches
            .iter()
            .filter(|item| match target_identity_key(item) {
                Some(key) => !sonarr_keys.contains(&key),
                None => true,
            })
            .cloned()
            .collect::<Vec<_>>();
        if additive_matches.is_empty() {
            outcome = AnimeReconciliationOutcome::Agreement;
            target_matches = sonarr_target_matches.clone();
        } else {
            outcome = AnimeReconciliationOutcome::Augmentation;
            target_matches = dedup_target_matches(
                sonarr_target_matches
                    .iter()
                    .cloned()
                    .chain(additive_matches),
            );
        }
    } else if !alias_scoped_target_matches.is_empty() {
        outcome = AnimeReconciliationOutcome::Augmentation;
        target_matches = alias_scoped_target_matches.clone();
    } else if !anime_signal_target_matches.is_empty() {
        outcome = AnimeReconciliationOutcome::Augmentation;
        target_matches = anime_signal_target_matches.clone();
    } else if !direct_target_matches.is_empty() {
        outcome = AnimeReconciliationOutcome::Augmentation;
        target_matches = direct_target_matches.clone();
    }

    if let Some(alias) = most_specific_exact_scoped_alias(alias_matches)
        && !target_matches.is_empty()
    {
        let owned_target_keys = context
            .targets
            .iter()
            .filter(|target| anime_alias_scope_matches_target(alias, target))
            .map(|target| target.target_key.as_str())
            .collect::<BTreeSet<_>>();
        target_matches.retain(|target| owned_target_keys.contains(target.target_key.as_str()));
        if target_matches.is_empty() {
            // A specific named sequel/season alias must not fall through to a
            // coincident absolute number owned by the shorter franchise alias.
            outcome = AnimeReconciliationOutcome::Unexplainable;
            rejection_reasons.push("exact_scoped_alias_target_unmapped".to_string());
        }
    }

    if target_matches.is_empty()
        && (!parsed.episode_numbers.is_empty() || !parsed.absolute_episode_numbers.is_empty())
        && outcome != AnimeReconciliationOutcome::TrueContradiction
    {
        outcome = AnimeReconciliationOutcome::Unexplainable;
        rejection_reasons.push("graph_reconciliation_unexplainable".to_string());
    }
    if sonarr_target_matches.is_empty()
        && parsed.absolute_episode_numbers.is_empty()
        && anime_signal_target_matches
            .iter()
            .any(|item| item.match_reason == "anime_signal_season_one_hypothesis")
    {
        review_reasons.push("season_one_inference_requires_review".to_string());
    }

    let alias_best_score = alias_matches.first().map(|item| item.score);
    let alias_margin = alias_match_margin(alias_matches);
    let identity_agreed = alias_matches.first().is_some_and(|best| {
        best.kind == AnimeAliasMatchKind::Exact
            || (best.score >= 86.0 && alias_margin.unwrap_or(100.0) > 8.0)
    });
    if !identity_agreed && !alias_matches.is_empty() {
        review_reasons.push("weak_alias_margin".to_string());
    }

    let sonarr_keys = target_identity_keys(&sonarr_target_matches);
    let target_keys = target_identity_keys(&target_matches);
    let agreed_target_keys = sonarr_keys
        .intersection(&target_keys)
        .cloned()
        .collect::<Vec<_>>();
    let augmented_target_keys = target_keys
        .difference(&sonarr_keys)
        .cloned()
        .collect::<Vec<_>>();

    review_reasons.sort();
    review_reasons.dedup();
    rejection_reasons.sort();
    rejection_reasons.dedup();
    contradiction_reasons.sort();
    contradiction_reasons.dedup();

    AnimeGraphReconciliation {
        outcome,
        graph_fingerprint: context.graph_fingerprint.clone(),
        identity_agreed,
        alias_best_score,
        alias_margin,
        target_matches,
        sonarr_target_matches,
        anime_signal_target_matches,
        agreed_target_keys,
        augmented_target_keys,
        contradiction_reasons,
        review_reasons,
        rejection_reasons,
    }
}

pub fn score_anime_candidate(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
) -> AnimeCandidateScore {
    let parsed = parse_anime_release_title(&candidate.title);
    let alias_table = build_anime_alias_table(context);
    let alias_matches = match_anime_aliases(&alias_table, &parsed);
    score_anime_candidate_from_parsed(context, candidate, parsed, alias_matches)
}

/// Re-run the normal resolver with one bounded semantic interpretation. Any
/// invalid interpretation is ignored by returning `None`; callers retain the
/// exact deterministic result they already had.
pub fn score_anime_candidate_with_semantic_evidence(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeCandidateScore> {
    score_anime_candidate_with_semantic_evidence_mode(
        context,
        candidate,
        evidence,
        SemanticScoringMode::Strict,
    )
}

/// Recreate the semantic score only after the coverage planner has proven the
/// selected target through real provider files. This is the narrow bridge for
/// parent torrent names that identify an anime entity but serialize their
/// episode coordinates only in the contained filenames.
pub fn score_anime_candidate_with_verified_semantic_plan(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
    plan: &AnimeFileCoveragePlan,
) -> Option<AnimeCandidateScore> {
    if let Some(score) = score_anime_candidate_with_semantic_evidence(context, candidate, evidence)
    {
        return Some(score);
    }
    if !semantic_bridge_plan_is_file_corroborated(plan, evidence) {
        return None;
    }
    score_anime_candidate_with_semantic_evidence_mode(
        context,
        candidate,
        evidence,
        SemanticScoringMode::CoverageWithFiles,
    )
}

/// Score a plan admitted by complete-batch weak-alias arbitration. The
/// identity exception remains private to this post-selection path and does not
/// alter ordinary semantic scoring.
pub(crate) fn score_anime_candidate_with_batch_unique_semantic_plan(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeCandidateScore> {
    score_anime_candidate_with_semantic_evidence_mode(
        context,
        candidate,
        evidence,
        SemanticScoringMode::BatchUniqueAliasPrefix,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticScoringMode {
    Strict,
    CoverageWithFiles,
    BatchUniqueAliasPrefix,
}

fn score_anime_candidate_with_semantic_evidence_mode(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
    mode: SemanticScoringMode,
) -> Option<AnimeCandidateScore> {
    let (scoped_context, parsed, alias_matches) =
        semantic_scoring_inputs(context, candidate, evidence, mode)?;
    Some(score_anime_candidate_from_parsed(
        &scoped_context,
        candidate,
        parsed,
        alias_matches,
    ))
}

fn score_anime_candidate_from_parsed(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    parsed: AnimeParsedRelease,
    alias_matches: Vec<AnimeAliasMatch>,
) -> AnimeCandidateScore {
    let reconciliation = reconcile_anime_graph(context, &parsed, &alias_matches);
    let target_matches = reconciliation.target_matches.clone();

    let mut review_reasons = parsed.review_reasons.clone();
    let mut rejection_reasons = reconciliation.rejection_reasons.clone();

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

    review_reasons.extend(reconciliation.review_reasons.iter().cloned());

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
        reconciliation,
        outcome,
        confidence,
        score: breakdown.total,
        breakdown,
        review_reasons,
        rejection_reasons,
    }
}

fn semantic_scoring_inputs(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
    mode: SemanticScoringMode,
) -> Option<(
    AnimeCandidateScoringContext,
    AnimeParsedRelease,
    Vec<AnimeAliasMatch>,
)> {
    if evidence.season_number < 0 || evidence.aliases.iter().all(|alias| alias.trim().is_empty()) {
        return None;
    }

    let selected_keys = evidence
        .target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if selected_keys.len() != evidence.target_keys.len() {
        return None;
    }
    if !selected_keys.is_empty()
        && selected_keys.iter().any(|key| {
            !context
                .targets
                .iter()
                .any(|target| target.target_key == *key)
        })
    {
        return None;
    }

    let mut parsed = parse_anime_release_title(&candidate.title);
    // Semantic identity is useful only when the untouched release text supports
    // at least one alias owned by the selected canonical entity. Validate that
    // relationship before adding semantic aliases to the parsed release;
    // otherwise the injected alias would manufacture its own exact match.
    let supplemental_identity = match mode {
        SemanticScoringMode::Strict => false,
        SemanticScoringMode::CoverageWithFiles => {
            semantic_identity_corroborated_by_unique_coordinate(context, &parsed, evidence)
        }
        SemanticScoringMode::BatchUniqueAliasPrefix => {
            model_selected_title_has_batch_unique_alias_prefix(context, &parsed, evidence)
        }
    };
    if !semantic_identity_supported_by_release(context, &parsed, evidence) && !supplemental_identity
    {
        return None;
    }
    let explicit_coordinate =
        parse_sxxeyy_numbers(&normalize_fullwidth_digits(&parsed.original_title));
    let mut authorized_release_season = None;
    if let Some((explicit_season, explicit_start, explicit_end)) = explicit_coordinate {
        let release_seasons = evidence
            .release_season_numbers
            .iter()
            .copied()
            .chain(std::iter::once(evidence.season_number))
            .collect::<BTreeSet<_>>();
        // A provider can serialize the same entity under a different season
        // number, but only an explicit alias owned by that entity may authorize
        // the translation. The model cannot waive an arbitrary Sxx conflict.
        if !release_seasons.contains(&explicit_season) {
            return None;
        }
        authorized_release_season =
            (explicit_season != evidence.season_number).then_some(explicit_season);
        let explicit_end = explicit_end.unwrap_or(explicit_start);
        let selected = evidence
            .episode_numbers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        match evidence.numbering {
            AnimeSemanticNumberingEvidence::Seasonal => {
                if !(explicit_start..=explicit_end).all(|episode| selected.contains(&episode)) {
                    return None;
                }
            }
            AnimeSemanticNumberingEvidence::Absolute
            | AnimeSemanticNumberingEvidence::EntityOnly => {}
        }
    }

    let mut translated_entity_episode_numbers = None;
    // A semantic hypothesis may recover a missing interpretation, but it may
    // not replace a different, clearly serialized episode in the release. The
    // selected entity and target own both seasonal and absolute coordinates,
    // so either is sufficient. A provider-season translation can change the
    // season label; it does not waive the raw episode coordinate.
    {
        let mut observed = semantic_explicit_release_episode_numbers(&parsed.original_title);
        // Reuse the production parser's coordinates as well as the narrow raw
        // syntax probe. The latter intentionally understands only a few clear
        // layouts and can miss forms such as `2014 OVA - 02`, while the main
        // parser has already extracted episode 2. A four-digit production year
        // is context, not an episode coordinate.
        observed.extend(
            parsed
                .episode_numbers
                .iter()
                .chain(&parsed.absolute_episode_numbers)
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number)),
        );
        let mut allowed = evidence
            .episode_numbers
            .iter()
            .chain(&evidence.absolute_episode_numbers)
            .copied()
            .filter(|number| *number > 0)
            .collect::<BTreeSet<_>>();
        for target in context
            .targets
            .iter()
            .filter(|target| selected_keys.contains(target.target_key.as_str()))
        {
            allowed.extend(target.episode_number.filter(|number| *number > 0));
            allowed.extend(target.absolute_episode_number.filter(|number| *number > 0));
        }
        // Entity-only evidence deliberately leaves target keys empty so the
        // deterministic parser owns numbering. It still may not turn a clear
        // raw episode into a different episode merely because only one target
        // from the selected entity is present in the scoped context.
        if evidence.numbering == AnimeSemanticNumberingEvidence::EntityOnly
            && selected_keys.is_empty()
        {
            for target in context.targets.iter().filter(|target| {
                evidence.anilist_season_id.as_deref().is_some_and(|id| {
                    target
                        .anilist_season_id
                        .as_deref()
                        .is_some_and(|target_id| target_id.eq_ignore_ascii_case(id))
                }) || (evidence.anilist_season_id.is_none()
                    && target.season_number == Some(evidence.season_number))
            }) {
                allowed.extend(target.episode_number.filter(|number| *number > 0));
                allowed.extend(target.absolute_episode_number.filter(|number| *number > 0));
            }
        }
        let selected_target_has_coordinate = context.targets.iter().any(|target| {
            selected_keys.contains(target.target_key.as_str())
                && (target.episode_number.is_some() || target.absolute_episode_number.is_some())
        });
        if mode != SemanticScoringMode::CoverageWithFiles
            && evidence.numbering == AnimeSemanticNumberingEvidence::EntityOnly
            && matches!(
                evidence.media_kind,
                AnimeSemanticMediaKindEvidence::Episode | AnimeSemanticMediaKindEvidence::Range
            )
            && selected_target_has_coordinate
            && observed.is_empty()
        {
            // Entity-only evidence confirms title identity; it never invents
            // the coordinate for an episode/range whose release contains no
            // parseable number. The deterministic resolver must decline it.
            return None;
        }
        if !observed.is_empty() && !allowed.is_empty() && observed.is_disjoint(&allowed) {
            let release_season = explicit_coordinate
                .map(|(season, _, _)| season)
                .or(parsed.season_number);
            let translated = release_season
                .filter(|season| evidence.release_season_numbers.contains(season))
                .filter(|_| evidence.episode_number_offset > 0)
                .map(|_| {
                    parsed
                        .episode_numbers
                        .iter()
                        .filter_map(|number| number.checked_add(evidence.episode_number_offset))
                        .collect::<BTreeSet<_>>()
                })
                .unwrap_or_default();
            if evidence.numbering != AnimeSemanticNumberingEvidence::EntityOnly
                || translated.is_empty()
                || translated.is_disjoint(&allowed)
            {
                return None;
            }
            translated_entity_episode_numbers = Some(translated.into_iter().collect::<Vec<_>>());
        }
    }

    match evidence.numbering {
        AnimeSemanticNumberingEvidence::Seasonal => {
            if evidence.episode_numbers.is_empty() {
                return None;
            }
            parsed.season_number = Some(evidence.season_number);
            parsed.episode_numbers = positive_sorted_numbers(&evidence.episode_numbers);
            parsed.absolute_episode_numbers.clear();
            parsed.sonarr_facts.season_number = Some(evidence.season_number);
            parsed.sonarr_facts.episode_numbers = parsed.episode_numbers.clone();
            parsed.sonarr_facts.absolute_episode_numbers.clear();
        }
        AnimeSemanticNumberingEvidence::Absolute => {
            if evidence.absolute_episode_numbers.is_empty() {
                return None;
            }
            parsed.season_number = Some(evidence.season_number);
            parsed.episode_numbers.clear();
            parsed.absolute_episode_numbers =
                positive_sorted_numbers(&evidence.absolute_episode_numbers);
            parsed.sonarr_facts.season_number = None;
            parsed.sonarr_facts.episode_numbers.clear();
            parsed.sonarr_facts.absolute_episode_numbers = parsed.absolute_episode_numbers.clone();
        }
        AnimeSemanticNumberingEvidence::EntityOnly => {
            parsed.season_number = Some(evidence.season_number);
            parsed.sonarr_facts.season_number = Some(evidence.season_number);
            if let Some(translated) = translated_entity_episode_numbers {
                parsed.episode_numbers = translated.clone();
                parsed.absolute_episode_numbers.clear();
                parsed.sonarr_facts.episode_numbers = translated;
                parsed.sonarr_facts.absolute_episode_numbers.clear();
            }
        }
    }

    match evidence.media_kind {
        AnimeSemanticMediaKindEvidence::Episode => {}
        AnimeSemanticMediaKindEvidence::Range => {
            parsed.batch_kind = AnimeBatchKind::Range;
            parsed.sonarr_facts.batch_kind = AnimeBatchKind::Range;
            parsed.sonarr_facts.release_kind = ReleaseKind::MultiEpisode;
        }
        AnimeSemanticMediaKindEvidence::SeasonPack => {
            parsed.batch_kind = AnimeBatchKind::SeasonPack;
            parsed.sonarr_facts.batch_kind = AnimeBatchKind::SeasonPack;
            parsed.sonarr_facts.release_kind = ReleaseKind::SeasonPack;
        }
        AnimeSemanticMediaKindEvidence::SeriesPack => {
            parsed.batch_kind = AnimeBatchKind::CompleteSeries;
            parsed.sonarr_facts.batch_kind = AnimeBatchKind::CompleteSeries;
            parsed.sonarr_facts.release_kind = ReleaseKind::SeriesPack;
        }
        AnimeSemanticMediaKindEvidence::Movie => {
            parsed.batch_kind = AnimeBatchKind::Movie;
            parsed.episode_type = AnimeEpisodeType::Movie;
        }
        AnimeSemanticMediaKindEvidence::Special | AnimeSemanticMediaKindEvidence::Ova => {
            parsed.episode_type = AnimeEpisodeType::Special;
            parsed.sonarr_facts.special = true;
        }
    }

    if matches!(
        evidence.numbering,
        AnimeSemanticNumberingEvidence::Seasonal | AnimeSemanticNumberingEvidence::Absolute
    ) {
        parsed
            .review_reasons
            .retain(|reason| reason != "missing_episode_number");
    }
    parsed
        .review_reasons
        .retain(|reason| reason != "missing_series_title");
    parsed.confidence = if parsed.review_reasons.is_empty() {
        ReleaseConfidence::High
    } else {
        ReleaseConfidence::ReviewRequired
    };

    let mut scoped_context = context.clone();
    if evidence.media_kind != AnimeSemanticMediaKindEvidence::SeriesPack {
        scoped_context.targets.retain(|target| {
            if !selected_keys.is_empty() {
                return selected_keys.contains(target.target_key.as_str());
            }
            evidence
                .anilist_season_id
                .as_ref()
                .zip(target.anilist_season_id.as_ref())
                .is_some_and(|(selected, target)| selected == target)
                || target.season_number == Some(evidence.season_number)
        });
    }
    if scoped_context.targets.is_empty() {
        return None;
    }
    scoped_context.scoped_aliases.retain(|alias| {
        evidence
            .anilist_season_id
            .as_ref()
            .zip(alias.anilist_season_id.as_ref())
            .is_some_and(|(selected, alias)| selected == alias)
            || alias.season_number == Some(evidence.season_number)
    });
    for alias in &evidence.aliases {
        let alias = alias.trim();
        if alias.is_empty() {
            continue;
        }
        scoped_context.scoped_aliases.push(AnimeScopedAlias {
            display: alias.to_string(),
            source: authorized_release_season
                .map(|_| "semantic_evidence_provider_season")
                .unwrap_or("semantic_evidence")
                .to_string(),
            language: None,
            season_number: authorized_release_season.or(Some(evidence.season_number)),
            anilist_season_id: evidence.anilist_season_id.clone(),
        });
    }

    let selected_alias = evidence
        .aliases
        .iter()
        .map(|alias| alias.trim())
        .find(|alias| !alias.is_empty())?;
    parsed.alt_titles.push(selected_alias.to_string());
    let alias_table = build_anime_alias_table(&scoped_context);
    let mut alias_matches = match_anime_aliases(&alias_table, &parsed);
    alias_matches.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Some((scoped_context, parsed, alias_matches))
}

fn semantic_identity_supported_by_release(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let mut selected_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .collect::<BTreeSet<_>>();
    let competing_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| !semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .collect::<BTreeSet<_>>();
    let generic_aliases = context
        .aliases
        .iter()
        .map(|alias| normalize_anime_alias(alias))
        .chain(
            competing_aliases
                .iter()
                .map(|alias| normalize_anime_alias(alias)),
        )
        .filter(|alias| !alias.is_empty())
        .collect::<BTreeSet<_>>();
    // The evidence object is constructed from the selected metadata entity.
    // Keep useful entity-owned shorthand that is not present in the scoring
    // graph (for example "Root A"), but discard global/shared franchise names
    // because they cannot distinguish this entity from its neighbours.
    selected_aliases.extend(evidence.aliases.iter().map(String::as_str).filter(|alias| {
        let normalized = normalize_anime_alias(alias);
        !normalized.is_empty() && !generic_aliases.contains(&normalized)
    }));
    if selected_aliases.is_empty() {
        return false;
    }

    let parsed_titles = semantic_parsed_title_candidates(parsed);

    let selected_score = parsed_titles
        .iter()
        .flat_map(|title| {
            selected_aliases
                .iter()
                .filter_map(move |alias| semantic_identity_alias_score(title, alias))
        })
        .max();
    let Some(selected_score) = selected_score.filter(|score| *score >= 82) else {
        return false;
    };
    let competing_score = parsed_titles
        .iter()
        .flat_map(|title| {
            competing_aliases
                .iter()
                .filter_map(move |alias| semantic_identity_alias_score(title, alias))
        })
        .max();

    // A selected entity must normally explain the release better than adjacent
    // graph entities. Movies have no episode coordinate with which to break an
    // exact metadata-alias tie, so an exact raw alias plus the model-selected
    // movie hypothesis is the bounded tie-breaker. Fuzzy/shared franchise
    // names remain insufficient.
    (selected_score == 100 && evidence.media_kind == AnimeSemanticMediaKindEvidence::Movie)
        || competing_score.is_none_or(|competing| {
            (selected_score >= 98 && competing < 98) || selected_score >= competing + 8
        })
}

/// Permit an adjacent-entity alias tie only when the raw release coordinate
/// identifies exactly the selected server-owned target. Similarity thresholds
/// are not lowered: the coordinate resolves ambiguity; it never supplies title
/// identity by itself.
fn semantic_identity_corroborated_by_unique_coordinate(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    if !semantic_coordinate_identifies_selected_targets(context, parsed, evidence) {
        return false;
    }

    let parsed_titles = semantic_parsed_title_candidates(parsed);
    let generic_aliases = context
        .aliases
        .iter()
        .map(|alias| normalize_anime_alias(alias))
        .filter(|alias| !alias.is_empty())
        .collect::<BTreeSet<_>>();
    let selected_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .chain(evidence.aliases.iter().map(String::as_str).filter(|alias| {
            let normalized = normalize_anime_alias(alias);
            !normalized.is_empty() && !generic_aliases.contains(&normalized)
        }));
    parsed_titles.iter().any(|title| {
        selected_aliases.clone().any(|alias| {
            semantic_identity_alias_score(title, alias).is_some_and(|score| score >= 82)
                || semantic_title_is_substantial_alias_prefix(title, alias)
        })
    })
}

fn semantic_coordinate_identifies_selected_targets(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    if evidence.target_keys.is_empty()
        || matches!(
            evidence.media_kind,
            AnimeSemanticMediaKindEvidence::SeasonPack
                | AnimeSemanticMediaKindEvidence::SeriesPack
                | AnimeSemanticMediaKindEvidence::Movie
        )
    {
        return false;
    }

    let selected_keys = evidence
        .target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if selected_keys.len() != evidence.target_keys.len() {
        return false;
    }
    let coordinate_key_sets = match evidence.numbering {
        AnimeSemanticNumberingEvidence::Seasonal => {
            // Anime releases normally omit S01. An explicit season remains
            // authoritative; otherwise the model-selected entity supplies the
            // season while the raw episode coordinate still has to identify
            // exactly the selected server target.
            let season = explicit_structured_season(parsed).unwrap_or(evidence.season_number);
            let observed = parsed
                .episode_numbers
                .iter()
                .copied()
                .filter(|number| *number > 0)
                .collect::<BTreeSet<_>>();
            if observed.is_empty() {
                return false;
            }
            vec![
                context
                    .targets
                    .iter()
                    .filter(|target| {
                        target.season_number == Some(season)
                            && target
                                .episode_number
                                .is_some_and(|episode| observed.contains(&episode))
                    })
                    .map(|target| target.target_key.as_str())
                    .collect::<BTreeSet<_>>(),
            ]
        }
        AnimeSemanticNumberingEvidence::Absolute => {
            let observed = parsed
                .absolute_episode_numbers
                .iter()
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number))
                .collect::<BTreeSet<_>>();
            if observed.is_empty() {
                return false;
            }
            vec![
                context
                    .targets
                    .iter()
                    .filter(|target| {
                        target
                            .absolute_episode_number
                            .is_some_and(|episode| observed.contains(&episode))
                    })
                    .map(|target| target.target_key.as_str())
                    .collect::<BTreeSet<_>>(),
            ]
        }
        AnimeSemanticNumberingEvidence::EntityOnly => {
            // The current selector intentionally returns entity identity only.
            // Raw coordinates remain parser-owned. Consider their seasonal and
            // absolute interpretations independently and proceed only when one
            // interpretation identifies exactly the selected server targets.
            let observed_seasonal = parsed
                .episode_numbers
                .iter()
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number))
                .collect::<BTreeSet<_>>();
            let observed_absolute = parsed
                .absolute_episode_numbers
                .iter()
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number))
                .collect::<BTreeSet<_>>();
            let structured_season = explicit_structured_season(parsed);
            let mut interpretations = Vec::new();
            if !observed_seasonal.is_empty() {
                interpretations.push(
                    context
                        .targets
                        .iter()
                        .filter(|target| {
                            structured_season
                                .is_none_or(|season| target.season_number == Some(season))
                                && target
                                    .episode_number
                                    .is_some_and(|episode| observed_seasonal.contains(&episode))
                        })
                        .map(|target| target.target_key.as_str())
                        .collect::<BTreeSet<_>>(),
                );
            }
            if !observed_absolute.is_empty() {
                interpretations.push(
                    context
                        .targets
                        .iter()
                        .filter(|target| {
                            target
                                .absolute_episode_number
                                .is_some_and(|episode| observed_absolute.contains(&episode))
                        })
                        .map(|target| target.target_key.as_str())
                        .collect::<BTreeSet<_>>(),
                );
            }
            interpretations
        }
    };
    coordinate_key_sets
        .iter()
        .any(|coordinate_keys| coordinate_keys == &selected_keys)
}

fn semantic_title_is_substantial_alias_prefix(title: &str, alias: &str) -> bool {
    let title_tokens = semantic_identity_tokens(title);
    let alias_tokens = semantic_identity_tokens(alias);
    title_tokens.len() >= 2
        && title_tokens.len() < alias_tokens.len()
        && title_tokens.concat().len() >= 10
        && alias_tokens.starts_with(&title_tokens)
}

/// Independently corroborate the one provider basename already owned by a
/// definitive semantic parent plan. Identity must still be present in the raw
/// basename and its explicit coordinate must agree with the exact planned
/// server target; absence or contradiction remains a rejection.
pub(crate) fn semantic_provider_file_corroborates_target(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    evidence: &AnimeSemanticCandidateEvidence,
    target_key: &str,
) -> bool {
    let parsed = parse_anime_release_title(&candidate.title);
    if !semantic_identity_supported_by_release(context, &parsed, evidence) {
        return false;
    }
    let Some(target) = context
        .targets
        .iter()
        .find(|target| target.target_key == target_key)
    else {
        return false;
    };
    if let Some((season, _, _)) =
        parse_sxxeyy_numbers(&normalize_fullwidth_digits(&parsed.original_title))
        && season != evidence.season_number
        && !evidence.release_season_numbers.contains(&season)
    {
        return false;
    }
    let observed = semantic_explicit_release_episode_numbers(&parsed.original_title);
    let allowed = target
        .episode_number
        .into_iter()
        .chain(target.absolute_episode_number)
        .filter(|number| *number > 0)
        .collect::<BTreeSet<_>>();
    !observed.is_empty()
        && !allowed.is_empty()
        && observed.iter().all(|number| allowed.contains(number))
}

fn semantic_parsed_title_candidates(parsed: &AnimeParsedRelease) -> BTreeSet<String> {
    let mut parsed_titles = parsed
        .series_title
        .iter()
        .chain(&parsed.alt_titles)
        .chain(&parsed.sonarr_facts.all_titles)
        .chain(&parsed.anime_signal_facts.title_candidates)
        .chain(&parsed.anime_signal_facts.title_season_alias_candidates)
        .map(|title| title.as_str().to_string())
        .filter(|title| !title.trim().is_empty())
        .collect::<BTreeSet<_>>();
    // Alternate releases commonly join equivalent English and romaji names
    // with a slash or pipe. Treat each side as raw release evidence; do not add
    // aliases supplied by the semantic hypothesis itself.
    let alternate_segments = parsed_titles
        .iter()
        .flat_map(|title| title.split(['/', '|']))
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    parsed_titles.extend(alternate_segments);
    parsed_titles
}

/// Classify how the untouched release title relates to the requested
/// canonical title. Batch planning uses this only as a dominance rule between
/// model-selected candidates for the same target: an exact canonical release
/// may displace a substantive longer-title collision, while unrelated aliases
/// and harmless packaging remain untouched.
pub(crate) fn anime_semantic_canonical_identity(
    candidate: &AnimeCandidateInput,
    canonical_title: &str,
) -> AnimeSemanticCanonicalIdentity {
    let canonical_tokens = semantic_identity_tokens(canonical_title);
    if canonical_tokens.is_empty() {
        return AnimeSemanticCanonicalIdentity::Other;
    }
    let parsed = parse_anime_release_title(&candidate.title);
    let title_tokens = semantic_parsed_title_candidates(&parsed)
        .into_iter()
        .map(|title| semantic_identity_tokens(&title))
        .filter(|tokens| !tokens.is_empty())
        .collect::<Vec<_>>();
    let canonical_joined = canonical_tokens.concat();
    if title_tokens
        .iter()
        .any(|tokens| tokens.concat() == canonical_joined)
    {
        return AnimeSemanticCanonicalIdentity::Exact;
    }
    if title_tokens.iter().any(|tokens| {
        tokens.len() > canonical_tokens.len()
            && tokens.starts_with(&canonical_tokens)
            && tokens[canonical_tokens.len()..]
                .iter()
                .any(|token| semantic_canonical_extension_token_is_substantive(token))
    }) {
        return AnimeSemanticCanonicalIdentity::SubstantiveExtension;
    }
    AnimeSemanticCanonicalIdentity::Other
}

fn semantic_canonical_extension_token_is_substantive(token: &str) -> bool {
    !matches!(
        token,
        "the"
            | "animation"
            | "dual"
            | "audio"
            | "dub"
            | "dubbed"
            | "multi"
            | "sub"
            | "subs"
            | "subbed"
            | "bd"
            | "dvd"
            | "pal"
            | "ntsc"
            | "bluray"
            | "bdrip"
            | "webrip"
            | "web"
            | "remux"
            | "rip"
            | "movie"
    ) && !token
        .parse::<i32>()
        .is_ok_and(|number| (1900..=2099).contains(&number))
}

fn semantic_alias_scope_is_selected(
    alias: &AnimeScopedAlias,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    evidence.anilist_season_id.as_deref().map_or_else(
        || alias.season_number == Some(evidence.season_number),
        |selected| {
            alias
                .anilist_season_id
                .as_deref()
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(selected))
        },
    )
}

fn semantic_identity_alias_score(title: &str, alias: &str) -> Option<u16> {
    let title_tokens = semantic_identity_tokens(title);
    let alias_tokens = semantic_identity_tokens(alias);
    if title_tokens.is_empty() || alias_tokens.is_empty() {
        return None;
    }
    // Anime release names frequently join or split romanized words differently
    // (`Kumamiko`/`Kuma Miko`, `Mahoutsukai`/`Mahou Tsukai`). Once punctuation
    // and token boundaries are removed, equality is still exact identity—not
    // fuzzy similarity.
    if title_tokens == alias_tokens || title_tokens.concat() == alias_tokens.concat() {
        return Some(100);
    }
    if title_tokens.len() > alias_tokens.len()
        && title_tokens.starts_with(&alias_tokens)
        && alias_remainder_is_release_context(&title_tokens[alias_tokens.len()..])
    {
        return Some(94);
    }
    if title_tokens.len() >= 2
        && title_tokens.len() < alias_tokens.len()
        && alias_tokens.ends_with(&title_tokens)
        && !alias_remainder_is_release_context(&title_tokens)
    {
        // Fansub releases often use only the entity-specific sequel/arc suffix
        // ("Root A", "Future Arc"). It is affirmative only when it is the
        // canonical alias suffix itself, not a generic season/pack marker.
        return Some(92);
    }
    // Some release groups shorten a long canonical subtitle while preserving
    // the franchise prefix and the distinguishing arc/part suffix, e.g.
    // "Danganronpa 3 - Future Arc". The anchors keep a bare franchise title
    // from becoming proof for a longer sequel name.
    if title_tokens.len() >= 3
        && title_tokens.len() < alias_tokens.len()
        && title_tokens[..title_tokens.len().min(2)] == alias_tokens[..title_tokens.len().min(2)]
        && title_tokens.last() == alias_tokens.last()
        && ordered_token_subsequence(&title_tokens, &alias_tokens)
    {
        return Some(90);
    }
    let overlap = token_overlap_score(&title_tokens, &alias_tokens)?;
    // High bag-of-words overlap is not enough for sequel identity: long anime
    // franchise titles often differ by only one arc/movie phrase. Permit a
    // fuzzy relation only when one form is an ordered abbreviation of the
    // other, never when substantive identity tokens were replaced or shuffled.
    (overlap >= 0.72
        && (ordered_token_subsequence(&title_tokens, &alias_tokens)
            || ordered_token_subsequence(&alias_tokens, &title_tokens)))
    .then_some((60.0 + overlap * 35.0).round() as u16)
}

fn semantic_identity_tokens(value: &str) -> Vec<String> {
    anime_alias_tokens(value)
        .into_iter()
        .map(|token| {
            ["st", "nd", "rd", "th"]
                .into_iter()
                .find_map(|suffix| {
                    token.strip_suffix(suffix).filter(|number| {
                        !number.is_empty() && number.chars().all(|ch| ch.is_ascii_digit())
                    })
                })
                .map(str::to_string)
                .unwrap_or(token)
        })
        .collect()
}

fn ordered_token_subsequence(needle: &[String], haystack: &[String]) -> bool {
    let mut cursor = 0;
    for token in haystack {
        if needle.get(cursor) == Some(token) {
            cursor += 1;
            if cursor == needle.len() {
                return true;
            }
        }
    }
    false
}

fn semantic_explicit_release_episode_numbers(input: &str) -> BTreeSet<i32> {
    let normalized = normalize_fullwidth_digits(input);
    let mut numbers = if let Some((_, start, end)) = parse_sxxeyy_numbers(&normalized) {
        expand_episode_numbers(start, end.unwrap_or(start), 200)
    } else if let Some((_, episode)) = parse_season_dash_episode(&normalized) {
        vec![episode]
    } else {
        let bracket_segments = extract_bracket_segments(&normalized);
        parse_absolute_episode_numbers(&normalized, &bracket_segments)
    };
    // A bare four-digit production year is not affirmative episode evidence.
    // Explicit SxxEyy values were returned above and are never discarded here.
    if parse_sxxeyy_numbers(&normalized).is_none() {
        numbers.retain(|number| !(1900..=2099).contains(number));
    }
    numbers.into_iter().filter(|number| *number > 0).collect()
}

fn positive_sorted_numbers(numbers: &[i32]) -> Vec<i32> {
    numbers
        .iter()
        .copied()
        .filter(|number| *number > 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn anime_parser_diagnostics(
    context: &AnimeCandidateScoringContext,
    score: &AnimeCandidateScore,
    coverage_plan: Option<&AnimeFileCoveragePlan>,
) -> JsonValue {
    json!(AnimeParserDiagnostics {
        parser_provenance: anime_parser_provenance(context, score, coverage_plan),
    })
}

pub fn anime_parser_provenance(
    context: &AnimeCandidateScoringContext,
    score: &AnimeCandidateScore,
    coverage_plan: Option<&AnimeFileCoveragePlan>,
) -> AnimeParserProvenance {
    let parsed = &score.parsed;
    let sonarr = &parsed.sonarr_facts;
    let signals = &parsed.anime_signal_facts;
    let final_confidence = coverage_plan
        .map(|plan| plan.confidence)
        .unwrap_or(score.confidence);
    let final_review_reasons = coverage_plan
        .map(|plan| {
            sorted_unique_strings(
                plan.review_reasons
                    .iter()
                    .cloned()
                    .chain(score.review_reasons.iter().cloned()),
            )
        })
        .unwrap_or_else(|| score.review_reasons.clone());
    let final_rejection_reasons = coverage_plan
        .map(|plan| {
            sorted_unique_strings(
                plan.rejection_reasons
                    .iter()
                    .cloned()
                    .chain(score.rejection_reasons.iter().cloned()),
            )
        })
        .unwrap_or_else(|| score.rejection_reasons.clone());

    AnimeParserProvenance {
        schema_version: ANIME_PARSER_PROVENANCE_SCHEMA_VERSION,
        resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
        resolver_version: score.resolver_version.clone(),
        parser_version: parsed.parser_version.clone(),
        sonarr_adapter_version: sonarr.parser_version.clone(),
        parsed: AnimeParsedReleaseProvenance {
            original_title: parsed.original_title.clone(),
            normalized_title: parsed.normalized_title.clone(),
            series_title: parsed.series_title.clone(),
            season_number: parsed.season_number,
            episode_numbers: parsed.episode_numbers.clone(),
            absolute_episode_numbers: parsed.absolute_episode_numbers.clone(),
            episode_type: parsed.episode_type,
            batch_kind: parsed.batch_kind,
            release_group: parsed.release_group.clone(),
            release_hash: parsed.release_hash.clone(),
            quality: parsed.quality.clone(),
            confidence: parsed.confidence,
            review_reasons: parsed.review_reasons.clone(),
        },
        sonarr: AnimeSonarrParserProvenance {
            parser_version: sonarr.parser_version.clone(),
            matched_pattern_id: None,
            matched_pattern_id_source: "rr2_public_parser_does_not_expose_regex_id".to_string(),
            original_title: sonarr.original_title.clone(),
            parsed_title: sonarr
                .series_title
                .clone()
                .or_else(|| sonarr.title_without_year.clone()),
            title_without_year: sonarr.title_without_year.clone(),
            title_year: sonarr.title_year,
            season_number: sonarr.season_number,
            season_end_number: sonarr.season_end_number,
            episode_numbers: sonarr.episode_numbers.clone(),
            absolute_episode_numbers: sonarr.absolute_episode_numbers.clone(),
            special_absolute_episode_numbers: sonarr.special_absolute_episode_numbers.clone(),
            release_kind: sonarr.release_kind,
            batch_kind: sonarr.batch_kind,
            full_season: sonarr.full_season,
            full_series: sonarr.full_series,
            is_partial_season: sonarr.is_partial_season,
            is_multi_season: sonarr.is_multi_season,
            special: sonarr.special,
            is_split_episode: sonarr.is_split_episode,
            release_group: sonarr.release_group.clone(),
            release_hash: sonarr.release_hash.clone(),
            release_tokens: sonarr.release_tokens.clone(),
            quality: sonarr.quality.clone(),
            audio_languages: sonarr.audio_languages.clone(),
        },
        anime_signals: AnimeSignalParserProvenance {
            parser_version: signals.parser_version.clone(),
            classifier_hints: signals.classifier_hints.clone(),
            title_candidates: signals.title_candidates.clone(),
            normalized_title_candidates: signals.normalized_title_candidates.clone(),
            title_season_alias_candidates: signals.title_season_alias_candidates.clone(),
            fallback_absolute_episode_hypotheses: signals
                .fallback_absolute_episode_hypotheses
                .clone(),
            fallback_season_one_episode_hypotheses: signals
                .fallback_season_one_episode_hypotheses
                .clone(),
            bounded_explicit_ranges: signals.bounded_explicit_ranges.clone(),
            dual_audio: signals.dual_audio,
            english_dub: signals.english_dub,
            multi_sub: signals.multi_sub,
            subtitle_languages: signals.subtitle_languages.clone(),
            leading_bracket_release_group: signals.leading_bracket_release_group.clone(),
        },
        graph: AnimeGraphMappingProvenance {
            graph_fingerprint: score.reconciliation.graph_fingerprint.clone(),
            alias_count: context.aliases.len(),
            target_count: context.targets.len(),
            alias_matches: score.alias_matches.clone(),
            target_matches: score.target_matches.clone(),
            sonarr_target_matches: score.reconciliation.sonarr_target_matches.clone(),
            anime_signal_target_matches: score.reconciliation.anime_signal_target_matches.clone(),
        },
        reconciliation: AnimeReconciliationProvenance {
            outcome: score.reconciliation.outcome,
            identity_agreed: score.reconciliation.identity_agreed,
            alias_best_score: score.reconciliation.alias_best_score,
            alias_margin: score.reconciliation.alias_margin,
            agreed_target_keys: score.reconciliation.agreed_target_keys.clone(),
            augmented_target_keys: score.reconciliation.augmented_target_keys.clone(),
            contradiction_reasons: score.reconciliation.contradiction_reasons.clone(),
            review_reasons: score.reconciliation.review_reasons.clone(),
            rejection_reasons: score.reconciliation.rejection_reasons.clone(),
        },
        outcome: score.outcome,
        confidence: final_confidence,
        score: score.score,
        review_reasons: final_review_reasons,
        rejection_reasons: final_rejection_reasons,
        coverage: coverage_plan.map(anime_coverage_provenance),
    }
}

fn anime_coverage_provenance(plan: &AnimeFileCoveragePlan) -> AnimeCoverageProvenance {
    let covered_target_keys = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    AnimeCoverageProvenance {
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        release_kind: plan.release_kind,
        confidence: plan.confidence,
        requires_file_list: plan.requires_file_list,
        requires_file_selection: plan.requires_file_selection,
        selected_file_keys: plan.selected_file_keys.clone(),
        entry_count: plan.entries.len(),
        covered_target_keys,
        review_reasons: plan.review_reasons.clone(),
        rejection_reasons: plan.rejection_reasons.clone(),
    }
}

pub fn plan_anime_file_coverage(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
) -> AnimeFileCoveragePlan {
    plan_anime_file_coverage_with_options(
        context,
        candidate,
        files,
        AnimeCoverageOptions::default(),
    )
}

pub fn plan_anime_file_coverage_with_options(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
) -> AnimeFileCoveragePlan {
    plan_anime_file_coverage_internal(context, candidate, files, options, None)
        .expect("deterministic anime coverage does not use semantic evidence")
}

pub fn plan_anime_file_coverage_with_semantic_evidence(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeFileCoveragePlan> {
    let full = plan_anime_semantic_evidence_attempt(
        context, candidate, files, options, evidence, evidence,
    );
    if full.as_ref().is_some_and(semantic_plan_is_definitive) {
        return full;
    }

    // Number interpretation is secondary evidence and may be dropped when the
    // untouched release carries a better deterministic coordinate. Canonical
    // wanted-target binding is not secondary: preserve it through the retry so
    // an adjacent episode or entity can never satisfy the current request.
    if evidence.numbering == AnimeSemanticNumberingEvidence::EntityOnly {
        // A legacy review/deferred plan is not stronger than a validated model
        // selection that passes every contradiction check below. Prefer the
        // definitive single-target plan; retain the legacy plan as fallback.
        return plan_model_selected_single_target_coverage(context, candidate, files, evidence)
            .or(full);
    }

    let mut without_target_binding = evidence.clone();
    without_target_binding.target_keys.clear();
    let without_target_binding = plan_anime_semantic_evidence_attempt(
        context,
        candidate,
        files,
        options,
        &without_target_binding,
        evidence,
    );
    if without_target_binding
        .as_ref()
        .is_some_and(semantic_plan_is_definitive)
    {
        return without_target_binding;
    }

    let mut identity_only = evidence.clone();
    identity_only.numbering = AnimeSemanticNumberingEvidence::EntityOnly;
    identity_only.episode_numbers.clear();
    identity_only.absolute_episode_numbers.clear();
    let identity_only = plan_anime_semantic_evidence_attempt(
        context,
        candidate,
        files,
        options,
        &identity_only,
        &identity_only,
    );
    if identity_only
        .as_ref()
        .is_some_and(semantic_plan_is_definitive)
    {
        return identity_only;
    }

    // The attempts above have already had every opportunity to return a
    // definitive deterministic plan. Do not let one of their legacy
    // review/deferred results shadow a validated model-selected single target.
    // The direct plan still has to pass all identity, coordinate, year, media
    // boundary, and provider-file contradiction checks below.
    plan_model_selected_single_target_coverage(context, candidate, files, evidence)
        .or(full)
        .or(without_target_binding)
        .or(identity_only)
}

/// Convert a validated semantic selection into coverage when the normal anime
/// resolver cannot reconstruct the same single-target plan. The model still
/// selects only a server-authored entity/media-kind hypothesis; target and file
/// ownership remain deterministic. This fallback intentionally excludes packs
/// and ranges, where every file must continue to prove exact coverage.
fn plan_model_selected_single_target_coverage(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeFileCoveragePlan> {
    try_plan_model_selected_single_target_coverage(context, candidate, files, evidence).ok()
}

/// Build the same single-target plan using only a shortened selected-entity
/// alias. Callers must arbitrate this evidence across the complete candidate
/// batch and accept it only when exactly one still-needed candidate qualifies.
/// Keeping this entry point separate prevents a weak alias from changing any
/// single-candidate, library, download-broker, or deterministic fallback path.
pub(crate) fn plan_anime_file_coverage_with_batch_unique_semantic_evidence(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeFileCoveragePlan> {
    try_plan_model_selected_single_target_coverage_with_identity(
        context,
        candidate,
        files,
        evidence,
        ModelSelectedIdentityRequirement::BatchUniqueAliasPrefix,
    )
    .ok()
}

/// Explain why the validated model-selected single-target path could not
/// author coverage. This is consumed by the qualification harness so replay
/// failures identify the exact deterministic safeguard involved.
pub(crate) fn model_selected_single_target_coverage_rejection_reason(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<&'static str> {
    try_plan_model_selected_single_target_coverage(context, candidate, files, evidence).err()
}

fn try_plan_model_selected_single_target_coverage(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
) -> Result<AnimeFileCoveragePlan, &'static str> {
    try_plan_model_selected_single_target_coverage_with_identity(
        context,
        candidate,
        files,
        evidence,
        ModelSelectedIdentityRequirement::Strict,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelSelectedIdentityRequirement {
    Strict,
    BatchUniqueAliasPrefix,
}

fn try_plan_model_selected_single_target_coverage_with_identity(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
    identity_requirement: ModelSelectedIdentityRequirement,
) -> Result<AnimeFileCoveragePlan, &'static str> {
    if evidence.target_keys.len() != 1
        || matches!(
            evidence.media_kind,
            AnimeSemanticMediaKindEvidence::Range
                | AnimeSemanticMediaKindEvidence::SeasonPack
                | AnimeSemanticMediaKindEvidence::SeriesPack
        )
    {
        return Err("unsupported_target_cardinality_or_pack");
    }

    let target_key = evidence
        .target_keys
        .first()
        .ok_or("selected_target_key_missing")?;
    let target = context
        .targets
        .iter()
        .find(|target| target.target_key == *target_key)
        .ok_or("selected_target_not_found")?;
    let parsed = parse_anime_release_title(&candidate.title);
    let identity_supported = match identity_requirement {
        ModelSelectedIdentityRequirement::Strict => {
            semantic_identity_supported_by_release(context, &parsed, evidence)
                || semantic_identity_corroborated_by_unique_coordinate(context, &parsed, evidence)
                || model_selected_entity_has_exact_release_identity(context, &parsed, evidence)
        }
        ModelSelectedIdentityRequirement::BatchUniqueAliasPrefix => {
            model_selected_title_has_batch_unique_alias_prefix(context, &parsed, evidence)
        }
    };
    if !identity_supported {
        return Err("release_identity_not_supported");
    }
    if semantic_release_coordinate_contradicts_target(&parsed, target, evidence) {
        return Err("release_coordinate_contradiction");
    }
    if semantic_release_year_contradicts_selected_entity(context, &parsed, evidence) {
        return Err("release_year_contradiction");
    }
    if semantic_special_boundary_contradicts_selected_target(context, &parsed, target, evidence) {
        return Err("release_media_boundary_contradiction");
    }

    let media_files = files
        .iter()
        .filter(|file| {
            is_anime_media_file(&file.path) && !is_anime_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();
    let bound_file = match media_files.as_slice() {
        [] => None,
        [file] => {
            let file_parsed = parse_anime_release_title(&file.path);
            if semantic_release_coordinate_contradicts_target(&file_parsed, target, evidence) {
                return Err("provider_file_coordinate_contradiction");
            }
            if semantic_release_year_contradicts_selected_entity(context, &file_parsed, evidence) {
                return Err("provider_file_year_contradiction");
            }
            if semantic_special_boundary_contradicts_selected_target(
                context,
                &file_parsed,
                target,
                evidence,
            ) {
                return Err("provider_file_media_boundary_contradiction");
            }
            Some(*file)
        }
        _ => return Err("multiple_provider_media_files_require_exact_coverage"),
    };

    let entry = AnimeFileCoverageEntry {
        target_key: target.target_key.clone(),
        canonical_key: target.canonical_key.clone(),
        release_file_key: bound_file.map(|file| file.file_key.clone()),
        file_id: bound_file.and_then(|file| file.file_id.clone()),
        file_index: bound_file.and_then(|file| file.file_index),
        path: bound_file.map(|file| file.path.clone()),
        coverage_kind: anime_coverage_kind(ReleaseKind::Single),
        confidence: ReleaseConfidence::High,
        score: Some(100.0),
        reason: match identity_requirement {
            ModelSelectedIdentityRequirement::Strict => {
                "model_selected_entity_without_deterministic_contradiction"
            }
            ModelSelectedIdentityRequirement::BatchUniqueAliasPrefix => {
                "batch_unique_model_selected_alias_prefix_without_deterministic_contradiction"
            }
        }
        .to_string(),
        state: ReleaseCoverageState::Planned,
    };
    Ok(anime_file_coverage_plan(
        ReleaseKind::Single,
        ReleaseConfidence::High,
        false,
        false,
        vec![entry],
        Vec::new(),
        Vec::new(),
    ))
}

/// The selector has already compared the raw release with the bounded,
/// server-authored entity hypotheses. At this final single-target boundary an
/// exact normalized match to any alias owned by the selected entity is enough
/// positive identity evidence, even when graph projection also copied that
/// canonical alias onto an adjacent entity. Competing entities remain useful
/// to the ordinary semantic scorer, but an artificial alias tie must not veto
/// a correct model selection. Hard coordinate, year, media-boundary, and file
/// contradictions are checked immediately after this function returns.
fn model_selected_entity_has_exact_release_identity(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let selected_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .chain(evidence.aliases.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let coordinate_identifies_selected_targets =
        semantic_coordinate_identifies_selected_targets(context, parsed, evidence);
    let parsed_titles = semantic_parsed_title_candidates(parsed);
    parsed_titles.iter().any(|title| {
        selected_aliases
            .iter()
            .any(|alias| model_selected_title_matches_owned_alias(title, alias, evidence))
            || model_selected_title_is_unambiguous_alias_abbreviation(
                context,
                title,
                &selected_aliases,
                evidence,
            )
            || model_selected_compound_title_has_owned_identity(
                title,
                &selected_aliases,
                parsed.release_group.as_deref(),
                evidence,
                coordinate_identifies_selected_targets,
            )
    })
}

/// A release may omit an entity subtitle (`Time Stranger Kyoko` versus
/// `Time Stranger Kyouko: Chocola ni Omakase!`). This predicate identifies
/// that narrow shape without promoting a title that is itself the exact alias
/// of an adjacent entity. It is intentionally insufficient on its own: only a
/// complete-batch caller may use the resulting plan, and only when one
/// still-needed candidate qualifies.
fn model_selected_title_has_batch_unique_alias_prefix(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let selected_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .chain(evidence.aliases.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    if selected_aliases.is_empty() {
        return false;
    }
    let competing_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| !semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .collect::<BTreeSet<_>>();

    semantic_parsed_title_candidates(parsed)
        .iter()
        .any(|title| {
            !competing_aliases.iter().any(|alias| {
                semantic_identity_alias_score(title, alias).is_some_and(|score| score == 100)
            }) && selected_aliases
                .iter()
                .any(|alias| semantic_title_is_substantial_alias_prefix(title, alias))
        })
}

fn model_selected_title_matches_owned_alias(
    title: &str,
    alias: &str,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    semantic_identity_alias_score(title, alias).is_some_and(|score| score == 100)
        || model_selected_alias_with_release_context(title, alias, evidence)
        || model_selected_title_matches_named_alias_segment(title, alias)
}

/// Metadata punctuation and filler words can make a release title a contracted
/// form of the selected alias. Require four retained identity tokens and at
/// least 60% token coverage; short franchise prefixes remain ambiguous and
/// fall back instead of being promoted to a sequel, movie, or OVA.
fn model_selected_title_is_unambiguous_alias_abbreviation(
    context: &AnimeCandidateScoringContext,
    title: &str,
    selected_aliases: &BTreeSet<&str>,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let title_tokens = semantic_identity_tokens(title);
    if title_tokens.len() < 4 || title_tokens.concat().len() < 16 {
        return false;
    }
    let selected_completion = selected_aliases.iter().any(|alias| {
        let alias_tokens = semantic_identity_tokens(alias);
        title_tokens.len() < alias_tokens.len()
            && title_tokens.len() * 5 >= alias_tokens.len() * 3
            && title_tokens.first() == alias_tokens.first()
            && title_tokens
                .iter()
                .try_fold(0usize, |offset, token| {
                    alias_tokens[offset..]
                        .iter()
                        .position(|candidate| candidate == token)
                        .map(|position| offset + position + 1)
                })
                .is_some()
    });
    if !selected_completion {
        return false;
    }

    !context
        .scoped_aliases
        .iter()
        .filter(|alias| !semantic_alias_scope_is_selected(alias, evidence))
        .any(|alias| {
            semantic_identity_alias_score(title, &alias.display).is_some_and(|score| score == 100)
        })
}

/// Some metadata aliases contain a second complete title after a colon. Match
/// that named title exactly (with harmless token-boundary differences) without
/// accepting arbitrary franchise substrings.
fn model_selected_title_matches_named_alias_segment(title: &str, alias: &str) -> bool {
    let title_tokens = semantic_identity_tokens(title);
    title_tokens.len() >= 3
        && title_tokens.concat().len() >= 12
        && alias
            .split([':', '：', '|'])
            .skip(1)
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .any(|segment| semantic_identity_tokens(segment).concat() == title_tokens.concat())
}

/// Accept a selected-entity alias followed only by technical packaging or a
/// media-boundary marker that agrees with the selected hypothesis. This also
/// handles harmless token-boundary differences such as
/// `Amefuri Kozou`/`Amefurikozou`; substantive sequel or episode-title text is
/// never discarded.
fn model_selected_alias_with_release_context(
    title: &str,
    alias: &str,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let title_tokens = semantic_identity_tokens(title);
    let alias_tokens = semantic_identity_tokens(alias);
    if title_tokens.is_empty() || alias_tokens.is_empty() {
        return false;
    }
    let alias_joined = alias_tokens.concat();
    if alias_joined.len() < 5 {
        return false;
    }
    (1..title_tokens.len()).any(|prefix_len| {
        title_tokens[..prefix_len].concat() == alias_joined
            && model_selected_release_context_is_safe(&title_tokens[prefix_len..], evidence)
    })
}

/// Fansub names often place two equivalent owned aliases around ` - `, for
/// example `Harmagedon - Genma Taisen` or
/// `Sen to Chihiro no Kamikakushi - Spirited Away`. Every substantive segment
/// must be owned by the selected entity; one matching franchise prefix cannot
/// hide a different OVA, sequel, or episode subtitle in another segment.
fn model_selected_compound_title_has_owned_identity(
    title: &str,
    selected_aliases: &BTreeSet<&str>,
    release_group: Option<&str>,
    evidence: &AnimeSemanticCandidateEvidence,
    coordinate_identifies_selected_targets: bool,
) -> bool {
    let segments = title
        .split(" -")
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() < 2 {
        return false;
    }

    let mut matched_owned_alias = false;
    for segment in segments {
        if selected_aliases
            .iter()
            .any(|alias| model_selected_title_matches_owned_alias(segment, alias, evidence))
        {
            matched_owned_alias = true;
            continue;
        }
        if release_group
            .is_some_and(|group| normalize_anime_alias(group) == normalize_anime_alias(segment))
        {
            continue;
        }
        let tokens = semantic_identity_tokens(segment);
        if model_selected_compound_segment_is_release_context(
            &tokens,
            evidence,
            coordinate_identifies_selected_targets,
        ) {
            continue;
        }
        return false;
    }
    matched_owned_alias
}

fn model_selected_compound_segment_is_release_context(
    tokens: &[String],
    evidence: &AnimeSemanticCandidateEvidence,
    coordinate_identifies_selected_targets: bool,
) -> bool {
    if model_selected_release_context_is_safe(tokens, evidence) {
        return true;
    }
    if coordinate_identifies_selected_targets
        && tokens.len() == 1
        && tokens[0]
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return true;
    }
    let Some((first, remainder)) = tokens.split_first() else {
        return false;
    };
    matches!(first.as_str(), "episode" | "ep")
        && remainder.first().is_some_and(|number| {
            !number.is_empty() && number.chars().all(|ch| ch.is_ascii_digit())
        })
        && remainder[1..].iter().all(|token| {
            matches!(
                token.as_str(),
                "eng" | "english" | "sub" | "subs" | "subbed" | "dub" | "dubbed"
            )
        })
}

fn model_selected_release_context_is_safe(
    tokens: &[String],
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    !tokens.is_empty()
        && tokens.iter().all(|token| {
            let technical = matches!(
                token.as_str(),
                "the"
                    | "animation"
                    | "dual"
                    | "audio"
                    | "dub"
                    | "dubbed"
                    | "multi"
                    | "sub"
                    | "subs"
                    | "subbed"
                    | "bd"
                    | "dvd"
                    | "pal"
                    | "ntsc"
                    | "bluray"
                    | "bdrip"
                    | "webrip"
                    | "web"
                    | "remux"
                    | "rip"
            ) || token
                .parse::<i32>()
                .is_ok_and(|number| (1900..=2099).contains(&number));
            if technical {
                return true;
            }
            match token.as_str() {
                "movie" => evidence.media_kind == AnimeSemanticMediaKindEvidence::Movie,
                "ova" | "oad" | "ona" => matches!(
                    evidence.media_kind,
                    AnimeSemanticMediaKindEvidence::Ova | AnimeSemanticMediaKindEvidence::Special
                ),
                "special" | "specials" => matches!(
                    evidence.media_kind,
                    AnimeSemanticMediaKindEvidence::Special | AnimeSemanticMediaKindEvidence::Ova
                ),
                _ => false,
            }
        })
}

fn semantic_release_coordinate_contradicts_target(
    parsed: &AnimeParsedRelease,
    target: &AnimeCandidateTarget,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    if let Some((season, _, _)) =
        parse_sxxeyy_numbers(&normalize_fullwidth_digits(&parsed.original_title))
        && season != evidence.season_number
        && !evidence.release_season_numbers.contains(&season)
    {
        return true;
    }

    // A movie is one canonical media target. Years, dimensions, and numerals
    // in its title are not episode coordinates. Explicit SxxEyy remains a hard
    // contradiction above; unstructured numeric parser guesses do not veto a
    // model-selected movie entity.
    if evidence.media_kind == AnimeSemanticMediaKindEvidence::Movie {
        return false;
    }

    let observed = semantic_explicit_release_episode_numbers(&parsed.original_title)
        .into_iter()
        .chain(
            parsed
                .episode_numbers
                .iter()
                .chain(&parsed.absolute_episode_numbers)
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number)),
        )
        .collect::<BTreeSet<_>>();
    let allowed = target
        .episode_number
        .into_iter()
        .chain(target.absolute_episode_number)
        .filter(|number| *number > 0)
        .collect::<BTreeSet<_>>();
    if observed.is_empty() || allowed.is_empty() || !observed.is_disjoint(&allowed) {
        return false;
    }

    evidence.episode_number_offset <= 0
        || observed
            .iter()
            .filter_map(|number| number.checked_add(evidence.episode_number_offset))
            .all(|number| !allowed.contains(&number))
}

fn semantic_release_year_contradicts_selected_entity(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let release_years = semantic_identity_tokens(&parsed.original_title)
        .into_iter()
        .filter_map(|token| token.parse::<i32>().ok())
        .filter(|year| (1950..=2100).contains(year))
        .collect::<BTreeSet<_>>();
    if release_years.is_empty() {
        return false;
    }
    let entity_years = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .chain(evidence.aliases.iter().map(String::as_str))
        .flat_map(semantic_identity_tokens)
        .filter_map(|token| token.parse::<i32>().ok())
        .filter(|year| (1950..=2100).contains(year))
        .collect::<BTreeSet<_>>();
    !entity_years.is_empty() && release_years.is_disjoint(&entity_years)
}

fn semantic_special_boundary_contradicts_selected_target(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    target: &AnimeCandidateTarget,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    let boundary_tokens = ["movie", "ova", "oad", "ona", "special", "specials"];
    let parsed_titles = semantic_parsed_title_candidates(parsed);
    let raw_without_release_group = LEADING_RELEASE_GROUP_RE
        .replace(&parsed.original_title, "")
        .into_owned();
    let release_boundaries = std::iter::once(raw_without_release_group.as_str())
        .chain(parsed_titles.iter().map(String::as_str))
        .flat_map(|title| semantic_identity_tokens(title))
        .filter(|token| boundary_tokens.contains(&token.as_str()))
        .collect::<BTreeSet<_>>();
    if release_boundaries.is_empty() {
        return false;
    }

    // An ordinary television episode cannot be replaced by a movie/OVA/special
    // merely because the franchise title is shared. Preserve legitimate words
    // such as "Special" when they are part of the selected entity's own title.
    let selected_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .chain(evidence.aliases.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let selected_boundaries = selected_aliases
        .iter()
        .copied()
        .flat_map(semantic_identity_tokens)
        .filter(|token| boundary_tokens.contains(&token.as_str()))
        .collect::<BTreeSet<_>>();
    if target.season_number.is_some()
        && target.episode_number.is_some()
        && !release_boundaries.is_subset(&selected_boundaries)
    {
        return true;
    }

    // For special-like releases, remove only the generic boundary word and
    // check whether the remaining franchise name also identifies an adjacent
    // metadata entity. In that case the model has not selected an entity-
    // specific title, so the generic marker cannot resolve the ambiguity.
    let competing_aliases = context
        .scoped_aliases
        .iter()
        .filter(|alias| !semantic_alias_scope_is_selected(alias, evidence))
        .map(|alias| alias.display.as_str())
        .collect::<Vec<_>>();
    let has_entity_specific_compound_title = parsed_titles.iter().any(|title| {
        let substantive_segments = title
            .split(" -")
            .map(str::trim)
            .filter(|segment| {
                semantic_identity_tokens(segment)
                    .iter()
                    .any(|token| !boundary_tokens.contains(&token.as_str()))
            })
            .collect::<Vec<_>>();
        substantive_segments.len() >= 2
            && substantive_segments.iter().all(|segment| {
                selected_aliases
                    .iter()
                    .any(|alias| model_selected_title_matches_owned_alias(segment, alias, evidence))
            })
    });
    if has_entity_specific_compound_title {
        return false;
    }
    parsed_titles.iter().any(|title| {
        let base = semantic_identity_tokens(title)
            .into_iter()
            .filter(|token| !boundary_tokens.contains(&token.as_str()))
            .collect::<Vec<_>>()
            .join(" ");
        !base.is_empty()
            && competing_aliases.iter().any(|alias| {
                semantic_identity_alias_score(&base, alias).is_some_and(|score| score >= 82)
            })
    })
}

fn plan_anime_semantic_evidence_attempt(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
    scoring_evidence: &AnimeSemanticCandidateEvidence,
    required_evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeFileCoveragePlan> {
    let uses_bridge =
        score_anime_candidate_with_semantic_evidence(context, candidate, scoring_evidence)
            .is_none();
    let plan = plan_anime_file_coverage_internal(
        context,
        candidate,
        files,
        options,
        Some(scoring_evidence),
    )
    .filter(semantic_plan_is_definitive)
    .or_else(|| {
        plan_entity_only_with_file_corroboration(
            context,
            candidate,
            files,
            options,
            scoring_evidence,
        )
    })?;
    let plan = enforce_semantic_target_coverage(plan, required_evidence);
    Some(if uses_bridge {
        enforce_semantic_bridge_file_corroboration(
            plan,
            context,
            candidate,
            files,
            scoring_evidence,
            required_evidence,
        )
    } else {
        plan
    })
}

fn plan_entity_only_with_file_corroboration(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
    evidence: &AnimeSemanticCandidateEvidence,
) -> Option<AnimeFileCoveragePlan> {
    let parsed = parse_anime_release_title(&candidate.title);
    if evidence.numbering != AnimeSemanticNumberingEvidence::EntityOnly
        || evidence.target_keys.is_empty()
        || !semantic_identity_supported_by_release(context, &parsed, evidence)
    {
        return None;
    }
    let observed_parent_numbers = semantic_explicit_release_episode_numbers(&parsed.original_title)
        .into_iter()
        .chain(
            parsed
                .episode_numbers
                .iter()
                .chain(&parsed.absolute_episode_numbers)
                .copied()
                .filter(|number| *number > 0 && !(1900..=2099).contains(number)),
        )
        .collect::<BTreeSet<_>>();
    if !observed_parent_numbers.is_empty() {
        return None;
    }
    if parsed.season_number.is_some_and(|season| {
        season != evidence.season_number && !evidence.release_season_numbers.contains(&season)
    }) {
        return None;
    }

    let required = evidence
        .target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if required.len() != evidence.target_keys.len() {
        return None;
    }
    let targets = context
        .targets
        .iter()
        .filter(|target| required.contains(target.target_key.as_str()))
        .collect::<Vec<_>>();
    if targets.len() != required.len() {
        return None;
    }
    let media_files = files
        .iter()
        .filter(|file| {
            is_anime_media_file(&file.path) && !is_anime_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();
    if media_files.is_empty() {
        return None;
    }

    let release_kind = match evidence.media_kind {
        AnimeSemanticMediaKindEvidence::SeriesPack => ReleaseKind::SeriesPack,
        AnimeSemanticMediaKindEvidence::SeasonPack => ReleaseKind::SeasonPack,
        AnimeSemanticMediaKindEvidence::Range => ReleaseKind::MultiEpisode,
        _ if targets.len() > 1 => ReleaseKind::MultiEpisode,
        _ => ReleaseKind::Single,
    };
    let coverage_kind = anime_coverage_kind(release_kind);
    let mut entries_by_target = BTreeMap::new();
    let mut selected_file_keys = BTreeSet::new();
    for file in &media_files {
        if !file.selectable {
            continue;
        }
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
        let Some(score) =
            score_anime_candidate_with_semantic_evidence(context, &file_candidate, evidence)
        else {
            continue;
        };
        if !matches!(
            score.confidence,
            ReleaseConfidence::High | ReleaseConfidence::Medium
        ) || !score.review_reasons.is_empty()
            || !score.rejection_reasons.is_empty()
        {
            continue;
        }
        let file_targets = score
            .target_matches
            .iter()
            .filter(|target| required.contains(target.target_key.as_str()))
            .collect::<Vec<_>>();
        if file_targets.is_empty() || file_targets.len() != score.target_matches.len() {
            continue;
        }
        for target in file_targets {
            if entries_by_target.contains_key(&target.target_key) {
                return None;
            }
            entries_by_target.insert(
                target.target_key.clone(),
                AnimeFileCoverageEntry {
                    target_key: target.target_key.clone(),
                    canonical_key: target.canonical_key.clone(),
                    release_file_key: Some(file.file_key.clone()),
                    file_id: file.file_id.clone(),
                    file_index: file.file_index,
                    path: Some(file.path.clone()),
                    coverage_kind,
                    confidence: ReleaseConfidence::High,
                    score: Some(target.score),
                    reason: "semantic_parent_identity_and_deterministic_file_coordinate"
                        .to_string(),
                    state: ReleaseCoverageState::Planned,
                },
            );
            selected_file_keys.insert(file.file_key.clone());
        }
    }
    if entries_by_target
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != required
    {
        return None;
    }

    let requires_file_selection = selected_file_keys.len() < media_files.len();
    if requires_file_selection
        && (!options.file_selection_supported
            || media_files
                .iter()
                .any(|file| !anime_file_has_safe_selection_id(file, options)))
    {
        return None;
    }
    let entries = evidence
        .target_keys
        .iter()
        .filter_map(|target_key| entries_by_target.remove(target_key))
        .collect::<Vec<_>>();
    if entries.len() != required.len() {
        return None;
    }
    Some(anime_file_coverage_plan(
        release_kind,
        ReleaseConfidence::High,
        false,
        requires_file_selection,
        entries,
        Vec::new(),
        Vec::new(),
    ))
}

fn enforce_semantic_target_coverage(
    mut plan: AnimeFileCoveragePlan,
    evidence: &AnimeSemanticCandidateEvidence,
) -> AnimeFileCoveragePlan {
    let required = evidence
        .target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if required.is_empty() {
        return plan;
    }
    let covered = plan
        .entries
        .iter()
        .filter(|entry| {
            !matches!(
                entry.state,
                ReleaseCoverageState::ReviewRequired | ReleaseCoverageState::Rejected
            )
        })
        .map(|entry| entry.target_key.as_str())
        .collect::<BTreeSet<_>>();
    if !required.is_subset(&covered) {
        plan.confidence = ReleaseConfidence::ReviewRequired;
        plan.review_reasons
            .push("missing_semantic_target_coverage".to_string());
        plan.review_reasons.sort();
        plan.review_reasons.dedup();
    }
    plan
}

fn semantic_plan_is_definitive(plan: &AnimeFileCoveragePlan) -> bool {
    matches!(
        plan.confidence,
        ReleaseConfidence::High | ReleaseConfidence::Medium
    ) && !plan.entries.is_empty()
        && plan.review_reasons.is_empty()
        && plan.rejection_reasons.is_empty()
}

fn semantic_bridge_plan_is_file_corroborated(
    plan: &AnimeFileCoveragePlan,
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    if !semantic_plan_is_definitive(plan) || evidence.target_keys.is_empty() {
        return false;
    }
    let required = evidence
        .target_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let covered = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.as_str())
        .collect::<BTreeSet<_>>();
    required == covered
        && plan.entries.len() == covered.len()
        && plan.entries.iter().all(|entry| {
            entry
                .release_file_key
                .as_deref()
                .is_some_and(|key| !key.trim().is_empty())
                && entry
                    .path
                    .as_deref()
                    .is_some_and(|path| !path.trim().is_empty())
        })
        && !plan.selected_file_keys.is_empty()
}

fn enforce_semantic_bridge_file_corroboration(
    mut plan: AnimeFileCoveragePlan,
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    scoring_evidence: &AnimeSemanticCandidateEvidence,
    required_evidence: &AnimeSemanticCandidateEvidence,
) -> AnimeFileCoveragePlan {
    let single_file_verified = plan.release_kind != ReleaseKind::Single
        || semantic_single_file_corroborates_plan(
            &plan,
            context,
            candidate,
            files,
            scoring_evidence,
        );
    if !semantic_bridge_plan_is_file_corroborated(&plan, required_evidence) || !single_file_verified
    {
        plan.confidence = ReleaseConfidence::ReviewRequired;
        plan.review_reasons
            .push("semantic_bridge_file_corroboration_failed".to_string());
        plan.review_reasons.sort();
        plan.review_reasons.dedup();
    }
    plan
}

fn semantic_single_file_corroborates_plan(
    plan: &AnimeFileCoveragePlan,
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    evidence: &AnimeSemanticCandidateEvidence,
) -> bool {
    if plan.entries.len() != 1 {
        return false;
    }
    let media_files = files
        .iter()
        .filter(|file| {
            file.selectable
                && is_anime_media_file(&file.path)
                && !is_anime_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();
    let [file] = media_files.as_slice() else {
        return false;
    };
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
    let wanted = plan.entries[0].target_key.as_str();
    std::iter::once(score_anime_candidate(context, &file_candidate))
        .chain(score_anime_candidate_with_semantic_evidence(
            context,
            &file_candidate,
            evidence,
        ))
        .chain(score_anime_candidate_with_semantic_evidence_mode(
            context,
            &file_candidate,
            evidence,
            SemanticScoringMode::CoverageWithFiles,
        ))
        .any(|score| {
            score.confidence == ReleaseConfidence::High
                && score.target_matches.len() == 1
                && score.review_reasons.is_empty()
                && score.rejection_reasons.is_empty()
                && score.target_matches[0].target_key == wanted
        })
}

fn plan_anime_file_coverage_internal(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
    semantic_evidence: Option<&AnimeSemanticCandidateEvidence>,
) -> Option<AnimeFileCoveragePlan> {
    let candidate_score = match semantic_evidence {
        Some(evidence) if !files.is_empty() => score_anime_candidate_with_semantic_evidence_mode(
            context,
            candidate,
            evidence,
            SemanticScoringMode::CoverageWithFiles,
        )?,
        Some(evidence) => {
            score_anime_candidate_with_semantic_evidence(context, candidate, evidence)?
        }
        None => score_anime_candidate(context, candidate),
    };
    let release_kind = anime_release_kind_for_coverage(&candidate_score.parsed);
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
    if !files.is_empty() && pack_requires_file_coverage {
        rejection_reasons.retain(|reason| {
            !matches!(
                reason.as_str(),
                "no_graph_alias_match"
                    | "no_graph_target_coverage"
                    | "graph_reconciliation_unexplainable"
            )
        });
    }

    if !rejection_reasons.is_empty() {
        return Some(anime_file_coverage_plan(
            release_kind,
            ReleaseConfidence::Low,
            false,
            false,
            Vec::new(),
            review_reasons,
            rejection_reasons,
        ));
    }

    if matches!(
        release_kind,
        ReleaseKind::Single | ReleaseKind::MultiEpisode
    ) {
        let mut entries = candidate_score
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
        let mut requires_file_selection = false;
        if !files.is_empty() && !entries.is_empty() {
            match bind_non_pack_file_coverage(context, candidate, files, options, &mut entries) {
                Some(selection_required) => requires_file_selection = selection_required,
                None => {
                    // A release may be semantically identified while its
                    // provider files remain ambiguous. Never treat that as a
                    // definitive automatic plan: file ownership is still a
                    // deterministic responsibility, especially when a batch
                    // was mislabeled as a single release upstream.
                    review_reasons.push("file_list_does_not_cover_expected_targets".to_string());
                }
            }
        }
        let confidence = if review_reasons.is_empty() && !entries.is_empty() {
            candidate_score.confidence
        } else {
            ReleaseConfidence::ReviewRequired
        };
        return Some(anime_file_coverage_plan(
            release_kind,
            confidence,
            false,
            requires_file_selection,
            entries,
            review_reasons,
            rejection_reasons,
        ));
    }

    if files.is_empty() {
        review_reasons.push("file_list_required".to_string());
        if matches!(
            release_kind,
            ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack
        ) {
            review_reasons.push("file_selection_required".to_string());
        }
        return Some(anime_file_coverage_plan(
            release_kind,
            ReleaseConfidence::ReviewRequired,
            true,
            matches!(
                release_kind,
                ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack
            ),
            Vec::new(),
            review_reasons,
            rejection_reasons,
        ));
    }

    let expected_targets =
        expected_anime_pack_targets(context, &candidate_score.parsed, release_kind);
    let mut entries = Vec::new();
    let mut selected_file_keys = BTreeSet::new();
    let mut covered_targets = BTreeSet::new();
    let mut duplicate_targets = BTreeSet::new();
    let mut unmapped_media_files = Vec::new();
    let mut unsafe_overfetch_files = Vec::new();
    let mut skipped_overfetch_file_keys = BTreeSet::new();
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
        let file_score = match semantic_evidence {
            Some(evidence) if evidence.media_kind == AnimeSemanticMediaKindEvidence::SeriesPack => {
                score_anime_candidate(context, &file_candidate)
            }
            Some(evidence) => {
                let mut entity_evidence = evidence.clone();
                entity_evidence.numbering = AnimeSemanticNumberingEvidence::EntityOnly;
                entity_evidence.media_kind = AnimeSemanticMediaKindEvidence::Episode;
                entity_evidence.episode_numbers.clear();
                entity_evidence.absolute_episode_numbers.clear();
                entity_evidence.target_keys.clear();
                match score_anime_candidate_with_semantic_evidence(
                    context,
                    &file_candidate,
                    &entity_evidence,
                ) {
                    Some(score) => score,
                    None => {
                        unmapped_media_files.push(file.path.clone());
                        continue;
                    }
                }
            }
            None => score_anime_candidate(context, &file_candidate),
        };
        if file_score.outcome == AnimeMatchOutcome::Rejected
            || file_score.confidence == ReleaseConfidence::ReviewRequired
            || file_score.target_matches.is_empty()
        {
            if pack_requires_file_coverage
                && anime_file_score_looks_like_scoped_overfetch(&file_score)
            {
                if anime_file_has_safe_selection_id(file, options) {
                    skipped_overfetch_file_keys.insert(file.file_key.clone());
                } else {
                    unsafe_overfetch_files.push(file.path.clone());
                }
            } else {
                unmapped_media_files.push(file.path.clone());
            }
            continue;
        }
        for target in file_score.target_matches {
            if !expected_targets.is_empty() && !expected_targets.contains(&target.target_key) {
                if anime_file_has_safe_selection_id(file, options) {
                    skipped_overfetch_file_keys.insert(file.file_key.clone());
                } else {
                    unsafe_overfetch_files.push(file.path.clone());
                }
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
    if !unsafe_overfetch_files.is_empty() {
        review_reasons.push("pack_overfetch_without_safe_file_selection".to_string());
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
        !skipped_overfetch_file_keys.is_empty(),
        entries,
        review_reasons,
        rejection_reasons,
    );
    plan.selected_file_keys = selected_file_keys.into_iter().collect();
    Some(plan)
}

#[derive(Debug)]
struct AnimeNonPackFileMatch<'a> {
    file: &'a AnimeReleaseFileInput,
    targets: BTreeMap<String, (f64, String)>,
}

/// Bind server-inventoried files for single and multi-episode releases. The
/// already-definitive parent release establishes identity. A sole media file is
/// therefore the owned payload; multi-file releases still require each basename
/// to establish a unique target/file relation. This is entirely deterministic
/// and never reuses model-authored facts.
fn bind_non_pack_file_coverage(
    context: &AnimeCandidateScoringContext,
    candidate: &AnimeCandidateInput,
    files: &[AnimeReleaseFileInput],
    options: AnimeCoverageOptions,
    entries: &mut [AnimeFileCoverageEntry],
) -> Option<bool> {
    let media_files = files
        .iter()
        .filter(|file| {
            is_anime_media_file(&file.path) && !is_anime_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();
    if media_files.is_empty() {
        return None;
    }

    let planned_targets = entries
        .iter()
        .map(|entry| entry.target_key.clone())
        .collect::<BTreeSet<_>>();
    if planned_targets.len() != entries.len() {
        return None;
    }

    // A definitive single/multi-episode release with one media payload has no
    // file-ownership ambiguity. Re-parsing that basename as a second release
    // identity is both redundant and harmful when indexer and payload names use
    // different aliases or provider-season numbering.
    if media_files.len() == 1 && media_files[0].selectable {
        let file = media_files[0];
        for entry in entries {
            entry.release_file_key = Some(file.file_key.clone());
            entry.file_id = file.file_id.clone();
            entry.file_index = file.file_index;
            entry.path = Some(file.path.clone());
            entry.confidence = ReleaseConfidence::High;
            entry.reason = "parent_release_identity_and_sole_media_file".to_string();
        }
        return Some(false);
    }

    let mut strict_file_matches = Vec::new();
    let mut file_matches = Vec::new();
    for file in &media_files {
        if !file.selectable {
            continue;
        }
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
        let score = score_anime_candidate(context, &file_candidate);
        if matches!(
            score.confidence,
            ReleaseConfidence::High | ReleaseConfidence::Medium
        ) && score.review_reasons.is_empty()
            && score.rejection_reasons.is_empty()
            && !score.target_matches.is_empty()
        {
            let strict_target_keys = score
                .target_matches
                .iter()
                .map(|target| target.target_key.clone())
                .collect::<BTreeSet<_>>();
            if strict_target_keys.is_subset(&planned_targets) {
                strict_file_matches.push(AnimeNonPackFileMatch {
                    file: *file,
                    targets: score
                        .target_matches
                        .iter()
                        .map(|target| {
                            (
                                target.target_key.clone(),
                                (target.score, target.match_reason.clone()),
                            )
                        })
                        .collect(),
                });
            }
        }
        if !score.reconciliation.contradiction_reasons.is_empty()
            || score.rejection_reasons.iter().any(|reason| {
                !matches!(
                    reason.as_str(),
                    "no_graph_alias_match"
                        | "no_graph_target_coverage"
                        | "graph_reconciliation_unexplainable"
                        | "exact_scoped_alias_target_unmapped"
                )
            })
        {
            continue;
        }
        let mut targets = score
            .target_matches
            .iter()
            .filter(|target| planned_targets.contains(&target.target_key))
            .map(|target| {
                (
                    target.target_key.clone(),
                    (target.score, target.match_reason.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if targets.is_empty() {
            for alias in score.alias_matches.iter().filter(|alias| {
                alias.kind == AnimeAliasMatchKind::Exact
                    && (alias.season_number.is_some() || alias.anilist_season_id.is_some())
            }) {
                for target in context.targets.iter().filter(|target| {
                    planned_targets.contains(&target.target_key)
                        && anime_alias_scope_matches_target(alias, target)
                }) {
                    targets.insert(
                        target.target_key.clone(),
                        (
                            100.0 + alias.score / 100.0,
                            format!(
                                "parent_release_identity_and_exact_file_alias:{}",
                                alias.source
                            ),
                        ),
                    );
                }
            }
        }
        if targets.is_empty() {
            continue;
        }
        file_matches.push(AnimeNonPackFileMatch {
            file: *file,
            targets,
        });
    }

    let (file_matches, selected) = if let Some(selected) =
        select_non_pack_file_matches(entries, &strict_file_matches, &planned_targets)
    {
        (&strict_file_matches, selected)
    } else {
        (
            &file_matches,
            select_non_pack_file_matches(entries, &file_matches, &planned_targets)?,
        )
    };

    let selected_file_indexes = selected.values().copied().collect::<BTreeSet<_>>();
    let requires_selection = selected_file_indexes.len() < media_files.len();
    if requires_selection
        && (!options.file_selection_supported
            || media_files
                .iter()
                .any(|file| !anime_file_has_safe_selection_id(file, options)))
    {
        return None;
    }

    for entry in entries {
        let file_match = &file_matches[*selected.get(&entry.target_key)?];
        let (score, reason) = file_match.targets.get(&entry.target_key)?;
        entry.release_file_key = Some(file_match.file.file_key.clone());
        entry.file_id = file_match.file.file_id.clone();
        entry.file_index = file_match.file.file_index;
        entry.path = Some(file_match.file.path.clone());
        entry.confidence = ReleaseConfidence::High;
        entry.score = Some(*score);
        entry.reason = reason.clone();
    }
    Some(requires_selection)
}

fn select_non_pack_file_matches(
    entries: &[AnimeFileCoverageEntry],
    file_matches: &[AnimeNonPackFileMatch<'_>],
    planned_targets: &BTreeSet<String>,
) -> Option<BTreeMap<String, usize>> {
    if file_matches.len() == 1
        && file_matches[0]
            .targets
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == *planned_targets
    {
        return Some(
            entries
                .iter()
                .map(|entry| (entry.target_key.clone(), 0_usize))
                .collect(),
        );
    }

    let mut selected = BTreeMap::new();
    let mut selected_file_indexes = BTreeSet::new();
    for entry in entries {
        let candidates = file_matches
            .iter()
            .enumerate()
            .filter(|(_, file_match)| {
                file_match.targets.len() == 1 && file_match.targets.contains_key(&entry.target_key)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if candidates.len() != 1 || !selected_file_indexes.insert(candidates[0]) {
            return None;
        }
        selected.insert(entry.target_key.clone(), candidates[0]);
    }
    Some(selected)
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
    let mut structured_count = 0_usize;
    let mut absolute_only_count = 0_usize;
    for episode in &mapping.episodes {
        if episode.season_number.is_some() || episode.episode_number.is_some() {
            structured_count += 1;
            if let Some(season) = episode.season_number.filter(|season| *season > 0) {
                *counts.entry(season).or_default() += 1;
            }
        } else if episode.mainline_episode_number.is_some() {
            absolute_only_count += 1;
        }
    }
    if absolute_only_count > structured_count {
        return Some(1);
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
    prefer_mainline_numbering: bool,
) -> Option<AnimeGraphTarget> {
    let (season_number, episode_number, absolute_episode_number) =
        resolve_anizip_target_numbers(season.season_number, prefer_mainline_numbering, episode);
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

fn target_absolute_episode_already_mapped(
    targets_by_key: &BTreeMap<String, AnimeGraphTarget>,
    candidate: &AnimeGraphTarget,
) -> bool {
    let Some(absolute_episode_number) = candidate
        .absolute_episode_number
        .or(candidate.episode_number)
        .filter(|episode| *episode > 0)
    else {
        return false;
    };

    targets_by_key.values().any(|target| {
        target.anilist_season_id == candidate.anilist_season_id
            && target
                .absolute_episode_number
                .or(target.episode_number)
                .is_some_and(|episode| episode == absolute_episode_number)
    })
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
    season_number: Option<i32>,
    anilist_season_id: Option<String>,
) {
    let cleaned = cleanup_anime_title(display);
    let normalized = normalize_anime_alias(&cleaned);
    let tokens = anime_alias_tokens(&cleaned);
    if normalized.is_empty() || tokens.is_empty() || is_metadata_segment(&cleaned) {
        return;
    }
    let key = format!(
        "{}:{}:{}:{}",
        normalized,
        source,
        season_number
            .map(|season| season.to_string())
            .unwrap_or_default(),
        anilist_season_id.clone().unwrap_or_default()
    );
    let entry = AnimeAliasEntry {
        display: cleaned,
        normalized,
        tokens,
        source: source.to_string(),
        season_number,
        anilist_season_id,
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
    for title in &parsed.anime_signal_facts.title_season_alias_candidates {
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
                    season_number: alias.season_number,
                    anilist_season_id: alias.anilist_season_id.clone(),
                    kind,
                    score: score + f64::from(alias.priority) / 1000.0,
                });
            }
        }
    }
    let original_tokens = anime_alias_tokens(&parsed.original_title);
    if !original_tokens.is_empty() {
        for alias in &alias_table.entries {
            if alias.season_number.is_none() && alias.anilist_season_id.is_none() {
                continue;
            }
            if alias.tokens.is_empty() || alias.tokens.len() > original_tokens.len() {
                continue;
            }
            if !token_sequence_contains(&original_tokens, &alias.tokens) {
                continue;
            }
            matches.push(AnimeAliasMatch {
                display: alias.display.clone(),
                normalized: alias.normalized.clone(),
                source: alias.source.clone(),
                season_number: alias.season_number,
                anilist_season_id: alias.anilist_season_id.clone(),
                kind: AnimeAliasMatchKind::Exact,
                score: 101.0
                    + (alias.tokens.len() as f64 / 100.0)
                    + f64::from(alias.priority) / 1000.0,
            });
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
        .filter(|item| {
            seen.insert((
                item.normalized.clone(),
                item.kind,
                item.season_number,
                item.anilist_season_id.clone(),
            ))
        })
        .take(5)
        .collect()
}

fn token_sequence_contains(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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

fn alias_match_margin(matches: &[AnimeAliasMatch]) -> Option<f64> {
    let best = matches.first()?;
    matches
        .iter()
        .skip(1)
        .find(|candidate| candidate.normalized != best.normalized)
        .map(|candidate| best.score - candidate.score)
}

fn most_specific_exact_scoped_alias(matches: &[AnimeAliasMatch]) -> Option<&AnimeAliasMatch> {
    let exact_scoped = matches
        .iter()
        .filter(|alias| {
            alias.kind == AnimeAliasMatchKind::Exact
                && (alias.season_number.is_some() || alias.anilist_season_id.is_some())
        })
        .collect::<Vec<_>>();
    exact_scoped.iter().copied().find(|candidate| {
        exact_scoped.iter().any(|other| {
            candidate.normalized != other.normalized
                && candidate.normalized.len() > other.normalized.len()
                && (candidate.normalized.starts_with(&other.normalized)
                    || candidate.normalized.ends_with(&other.normalized))
        })
    })
}

fn match_targets_by_alias_scope(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    alias_matches: &[AnimeAliasMatch],
) -> Vec<AnimeCandidateTargetMatch> {
    let scoped_matches = alias_matches
        .iter()
        .filter(|item| item.season_number.is_some() || item.anilist_season_id.is_some())
        .filter(|item| item.kind >= AnimeAliasMatchKind::Suffix)
        .collect::<Vec<_>>();
    if scoped_matches.is_empty() {
        return Vec::new();
    }
    let structured_season = explicit_structured_season(parsed);
    let selected_scope = scoped_matches
        .iter()
        .find(|item| {
            item.kind == AnimeAliasMatchKind::Exact
                && structured_season.is_some_and(|season| {
                    item.season_number == Some(season)
                        || context.targets.iter().any(|target| {
                            target.season_number == Some(season)
                                && anime_alias_scope_matches_target(item, target)
                        })
                })
        })
        .or_else(|| {
            scoped_matches
                .iter()
                .find(|item| item.kind == AnimeAliasMatchKind::Exact)
        })
        .copied()
        .or_else(|| scoped_matches.first().copied());
    let scoped_matches = selected_scope
        .map(|best| {
            scoped_matches
                .into_iter()
                .filter(|item| anime_alias_scopes_agree(best, item))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if scoped_matches.is_empty() {
        return Vec::new();
    }

    let mut episode_numbers = parsed
        .sonarr_facts
        .episode_numbers
        .iter()
        .copied()
        .chain(parsed.sonarr_facts.absolute_episode_numbers.iter().copied())
        .chain(parsed.episode_numbers.iter().copied())
        .chain(parsed.absolute_episode_numbers.iter().copied())
        .filter(|episode| *episode > 0)
        .collect::<BTreeSet<_>>();
    episode_numbers.extend(
        parsed
            .anime_signal_facts
            .fallback_season_one_episode_hypotheses
            .iter()
            .copied()
            .filter(|episode| *episode > 0),
    );
    if episode_numbers.is_empty() {
        return Vec::new();
    }

    let mut matches = Vec::new();
    for alias in scoped_matches {
        for target in &context.targets {
            if !anime_alias_scope_matches_target(alias, target) {
                continue;
            }
            if structured_season.zip(target.season_number).is_some_and(
                |(parsed_season, target_season)| {
                    parsed_season != target_season
                        && !anime_alias_provider_season_matches_target(alias, target, parsed_season)
                },
            ) {
                continue;
            }
            if !target
                .episode_number
                .is_some_and(|episode| episode_numbers.contains(&episode))
            {
                continue;
            }
            let mut matched = candidate_target_match(
                target,
                "scoped_alias_season_episode",
                104.0 + alias.score / 100.0 + target_identity_bonus(target),
            );
            matched.match_reason = format!("scoped_alias_season_episode:{}", alias.source);
            matches.push(matched);
        }
    }

    dedup_target_matches(matches)
}

fn exact_scoped_alias_conflicts_with_structured_season(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
    alias_matches: &[AnimeAliasMatch],
) -> bool {
    let Some(structured_season) = explicit_structured_season(parsed) else {
        return false;
    };
    let exact_scoped = alias_matches
        .iter()
        .filter(|alias| {
            alias.kind == AnimeAliasMatchKind::Exact
                && (alias.season_number.is_some() || alias.anilist_season_id.is_some())
        })
        .collect::<Vec<_>>();
    if exact_scoped.is_empty() {
        return false;
    }

    let scoped_target_seasons = context
        .targets
        .iter()
        .filter(|target| {
            exact_scoped
                .iter()
                .any(|alias| anime_alias_scope_matches_target(alias, target))
        })
        .filter_map(|target| target.season_number)
        .collect::<BTreeSet<_>>();
    if !scoped_target_seasons.is_empty() {
        return !scoped_target_seasons.contains(&structured_season)
            && !exact_scoped.iter().any(|alias| {
                context.targets.iter().any(|target| {
                    anime_alias_provider_season_matches_target(alias, target, structured_season)
                })
            });
    }

    !exact_scoped
        .iter()
        .any(|alias| alias.season_number == Some(structured_season))
}

fn anime_alias_provider_season_matches_target(
    alias: &AnimeAliasMatch,
    target: &AnimeCandidateTarget,
    release_season: i32,
) -> bool {
    alias.source == "semantic_evidence_provider_season"
        && alias.season_number == Some(release_season)
        && alias
            .anilist_season_id
            .as_ref()
            .zip(target.anilist_season_id.as_ref())
            .is_some_and(|(alias_id, target_id)| alias_id == target_id)
}

fn explicit_structured_season(parsed: &AnimeParsedRelease) -> Option<i32> {
    [
        parsed.original_title.as_str(),
        parsed.sonarr_facts.original_title.as_str(),
    ]
    .into_iter()
    .find_map(|title| {
        let title = normalize_fullwidth_digits(title);
        parse_sxxeyy_numbers(&title)
            .map(|(season, _, _)| season)
            .or_else(|| parse_season_dash_episode(&title).map(|(season, _)| season))
    })
}

fn anime_alias_scope_matches_target(
    alias: &AnimeAliasMatch,
    target: &AnimeCandidateTarget,
) -> bool {
    alias
        .anilist_season_id
        .as_ref()
        .zip(target.anilist_season_id.as_ref())
        .is_some_and(|(alias_id, target_id)| alias_id == target_id)
        || alias
            .season_number
            .zip(target.season_number)
            .is_some_and(|(alias_season, target_season)| alias_season == target_season)
}

fn anime_alias_scopes_agree(left: &AnimeAliasMatch, right: &AnimeAliasMatch) -> bool {
    if let (Some(left_id), Some(right_id)) = (
        left.anilist_season_id.as_deref(),
        right.anilist_season_id.as_deref(),
    ) && left_id != right_id
    {
        return false;
    }
    if let (Some(left_season), Some(right_season)) = (left.season_number, right.season_number)
        && left_season != right_season
    {
        return false;
    }
    (left.anilist_season_id.is_some() || left.season_number.is_some())
        && (right.anilist_season_id.is_some() || right.season_number.is_some())
}

fn match_anime_signal_targets(
    context: &AnimeCandidateScoringContext,
    parsed: &AnimeParsedRelease,
) -> Vec<AnimeCandidateTargetMatch> {
    let sonarr_episodes = parsed
        .sonarr_facts
        .episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let signal_episodes = parsed
        .episode_numbers
        .iter()
        .copied()
        .filter(|episode| !sonarr_episodes.contains(episode))
        .collect::<Vec<_>>();

    let sonarr_absolute = parsed
        .sonarr_facts
        .absolute_episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut signal_absolute = parsed
        .absolute_episode_numbers
        .iter()
        .copied()
        .filter(|episode| !sonarr_absolute.contains(episode))
        .collect::<BTreeSet<_>>();
    signal_absolute.extend(
        parsed
            .anime_signal_facts
            .fallback_absolute_episode_hypotheses
            .iter()
            .copied()
            .filter(|episode| !sonarr_absolute.contains(episode)),
    );

    let mut matches = Vec::new();
    matches.extend(match_targets_by_season_episode(
        context,
        parsed.season_number,
        &signal_episodes,
        "anime_signal_season_episode",
        88.0,
    ));
    matches.extend(match_targets_by_absolute_episode(
        context,
        &signal_absolute.into_iter().collect::<Vec<_>>(),
        "anime_signal_absolute_episode",
        86.0,
    ));

    let fallback_season_one = parsed
        .anime_signal_facts
        .fallback_season_one_episode_hypotheses
        .iter()
        .copied()
        .filter(|episode| !sonarr_episodes.contains(episode))
        .collect::<Vec<_>>();
    matches.extend(match_targets_by_season_episode(
        context,
        Some(1),
        &fallback_season_one,
        "anime_signal_season_one_hypothesis",
        72.0,
    ));

    dedup_target_matches(matches)
}

fn match_targets_by_season_episode(
    context: &AnimeCandidateScoringContext,
    season_number: Option<i32>,
    episode_numbers: &[i32],
    reason: &str,
    base_score: f64,
) -> Vec<AnimeCandidateTargetMatch> {
    let Some(parsed_season) = season_number else {
        return Vec::new();
    };
    let episode_numbers = episode_numbers.iter().copied().collect::<BTreeSet<_>>();
    if episode_numbers.is_empty() {
        return Vec::new();
    }
    context
        .targets
        .iter()
        .filter(|target| {
            target.season_number == Some(parsed_season)
                && target
                    .episode_number
                    .is_some_and(|episode| episode_numbers.contains(&episode))
        })
        .map(|target| {
            candidate_target_match(target, reason, base_score + target_identity_bonus(target))
        })
        .collect()
}

fn match_targets_by_absolute_episode(
    context: &AnimeCandidateScoringContext,
    absolute_episode_numbers: &[i32],
    reason: &str,
    base_score: f64,
) -> Vec<AnimeCandidateTargetMatch> {
    let absolute_episode_numbers = absolute_episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if absolute_episode_numbers.is_empty() {
        return Vec::new();
    }
    context
        .targets
        .iter()
        .filter(|target| {
            target
                .absolute_episode_number
                .is_some_and(|episode| absolute_episode_numbers.contains(&episode))
        })
        .map(|target| {
            candidate_target_match(target, reason, base_score + target_identity_bonus(target))
        })
        .collect()
}

fn dedup_target_matches(
    matches: impl IntoIterator<Item = AnimeCandidateTargetMatch>,
) -> Vec<AnimeCandidateTargetMatch> {
    let mut by_key = BTreeMap::<String, AnimeCandidateTargetMatch>::new();
    for target_match in matches {
        match by_key.get(&target_match.target_key) {
            Some(existing) if existing.score >= target_match.score => {}
            _ => {
                by_key.insert(target_match.target_key.clone(), target_match);
            }
        }
    }
    let mut matches = by_key.into_values().collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.target_key.cmp(&right.target_key))
    });
    matches
}

fn target_identity_key(target_match: &AnimeCandidateTargetMatch) -> Option<String> {
    target_match
        .canonical_key
        .clone()
        .or_else(|| Some(target_match.target_key.clone()))
}

fn target_identity_keys(matches: &[AnimeCandidateTargetMatch]) -> BTreeSet<String> {
    matches.iter().filter_map(target_identity_key).collect()
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

pub(crate) fn anime_release_kind_for_coverage(parsed: &AnimeParsedRelease) -> ReleaseKind {
    let parsed_kind = anime_release_kind(parsed);
    let sonarr_kind = parsed.sonarr_facts.release_kind;
    match (sonarr_kind, parsed_kind) {
        (
            ReleaseKind::SeasonPack | ReleaseKind::MultiSeasonPack | ReleaseKind::SeriesPack,
            ReleaseKind::Single | ReleaseKind::MultiEpisode | ReleaseKind::Unknown,
        ) if parsed.episode_numbers.is_empty() && parsed.absolute_episode_numbers.is_empty() => {
            sonarr_kind
        }
        (ReleaseKind::MultiEpisode, ReleaseKind::Single | ReleaseKind::Unknown) => sonarr_kind,
        (kind, ReleaseKind::Unknown) if kind != ReleaseKind::Unknown => kind,
        (_, kind) => kind,
    }
}

pub(crate) fn anime_coverage_kind(release_kind: ReleaseKind) -> ReleaseCoverageKind {
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
    requires_file_selection: bool,
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
        requires_file_selection,
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
                if expected.is_empty()
                    && context.targets.iter().all(|target| {
                        target.season_number.is_none() && target.absolute_episode_number.is_some()
                    })
                {
                    expected.extend(
                        context
                            .targets
                            .iter()
                            .map(|target| target.target_key.clone()),
                    );
                }
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

fn anime_file_has_safe_selection_id(
    file: &AnimeReleaseFileInput,
    options: AnimeCoverageOptions,
) -> bool {
    options.file_selection_supported
        && file.selectable
        && file
            .file_id
            .as_deref()
            .is_some_and(|file_id| !file_id.trim().is_empty())
}

fn anime_file_score_looks_like_scoped_overfetch(score: &AnimeCandidateScore) -> bool {
    !score.alias_matches.is_empty()
        && !score
            .rejection_reasons
            .iter()
            .any(|reason| reason == "no_graph_alias_match")
        && (!score.parsed.episode_numbers.is_empty()
            || !score.parsed.absolute_episode_numbers.is_empty()
            || !score.parsed.sonarr_facts.episode_numbers.is_empty()
            || !score
                .parsed
                .sonarr_facts
                .absolute_episode_numbers
                .is_empty())
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
    let parent_is_bonus_material = lower.rsplit_once('/').is_some_and(|(parent, _)| {
        parent.split('/').any(|segment| {
            let segment = segment.trim();
            segment == "sample"
                || segment == "samples"
                || segment == "extra"
                || segment == "extras"
                || segment == "featurette"
                || segment == "featurettes"
                || segment == "related video clips"
                || segment.starts_with("bonus ")
                || segment == "bonus"
        })
    });
    parent_is_bonus_material
        || lower.contains("/sample")
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
    scoped_aliases: &[AnimeScopedAlias],
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
    material.extend(scoped_aliases.iter().map(|alias| {
        format!(
            "alias:{}:{}:{}:{}:{}",
            alias.display,
            alias.source,
            alias.language.clone().unwrap_or_default(),
            alias
                .season_number
                .map(|season| season.to_string())
                .unwrap_or_default(),
            alias.anilist_season_id.clone().unwrap_or_default()
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

fn insert_generated_season_aliases(
    scoped_aliases: &mut BTreeMap<String, AnimeScopedAlias>,
    base_title: &str,
    season: &AniListSeasonChainEntry,
) {
    if season.season_number <= 1 {
        return;
    }
    insert_scoped_alias(
        scoped_aliases,
        &format!("{} Season {}", base_title.trim(), season.season_number),
        "generated_season_ordinal",
        season,
    );
    insert_scoped_alias(
        scoped_aliases,
        &format!("{} S{}", base_title.trim(), season.season_number),
        "generated_season_short",
        season,
    );
    insert_scoped_alias(
        scoped_aliases,
        &format!("{} S{:02}", base_title.trim(), season.season_number),
        "generated_season_short",
        season,
    );
}

fn insert_scoped_alias(
    scoped_aliases: &mut BTreeMap<String, AnimeScopedAlias>,
    value: &str,
    source: &str,
    season: &AniListSeasonChainEntry,
) {
    insert_scoped_alias_with_language(scoped_aliases, value, source, None, season);
}

fn insert_scoped_alias_with_language(
    scoped_aliases: &mut BTreeMap<String, AnimeScopedAlias>,
    value: &str,
    source: &str,
    language: Option<&str>,
    season: &AniListSeasonChainEntry,
) {
    let display = cleanup_anime_title(value);
    if display.is_empty() || is_metadata_segment(&display) {
        return;
    }
    let normalized = normalize_anime_alias(&display);
    if normalized.is_empty() {
        return;
    }
    let key = format!(
        "{}:{}:{}:{}",
        normalized, source, season.season_number, season.anilist_id
    );
    scoped_aliases
        .entry(key)
        .or_insert_with(|| AnimeScopedAlias {
            display,
            source: source.to_string(),
            language: language
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            season_number: Some(season.season_number),
            anilist_season_id: Some(season.anilist_id.clone()),
        });
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
        "airdate",
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
    let coordinate_input = strip_anime_crc32_tokens(input);
    let mut file_input = ClassifierFileInput::new(&coordinate_input);
    file_input.file_name = Some(coordinate_input);
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
        if number_is_file_size_token(input, start) {
            return Vec::new();
        }
        let end = parse_capture_i32(&captures, "end").unwrap_or(start);
        return expand_episode_numbers(start, end, 200);
    }
    if let Some(captures) = DASH_EPISODE_RE.captures(input)
        && let Some(start) = parse_capture_i32(&captures, "start")
    {
        if number_is_file_size_token(input, start) {
            return Vec::new();
        }
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
    if number_is_file_size_token(&normalized, start) {
        return None;
    }
    let end = parse_capture_i32(&captures, "end").unwrap_or(start);
    Some((start, end))
}

fn number_is_file_size_token(input: &str, number: i32) -> bool {
    FILE_SIZE_TOKEN_RE.captures_iter(input).any(|captures| {
        captures
            .name("size")
            .and_then(|value| value.as_str().parse::<i32>().ok())
            .is_some_and(|size| size == number)
    })
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
    let normalized = input.replace('_', " ");
    let resolution = RESOLUTION_RE.captures(input).and_then(|captures| {
        captures
            .name("resolution")
            .map(|value| normalize_resolution(value.as_str()))
    });
    let source = if RAW_HD_SOURCE_RE.is_match(&normalized) {
        Some("raw_hd".to_string())
    } else if WEB_DL_SOURCE_RE.is_match(&normalized) {
        Some("web_dl".to_string())
    } else if WEB_RIP_SOURCE_RE.is_match(&normalized) {
        Some("web_rip".to_string())
    } else if BLURAY_SOURCE_RE.is_match(&normalized) {
        Some("blu_ray".to_string())
    } else if HDTV_SOURCE_RE.is_match(&normalized) {
        Some("hdtv".to_string())
    } else if DVD_SOURCE_RE.is_match(&normalized) {
        Some("dvd".to_string())
    } else if PDTV_SOURCE_RE.is_match(&normalized) {
        Some("pdtv".to_string())
    } else if DSR_SOURCE_RE.is_match(&normalized) {
        Some("dsr".to_string())
    } else if SDTV_SOURCE_RE.is_match(&normalized) {
        Some("sdtv".to_string())
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
        dual_audio: DUAL_AUDIO_SIGNAL_RE.is_match(&normalized),
        multi_sub: MULTI_SUB_SIGNAL_RE.is_match(&normalized),
    }
}

fn parse_anime_languages(input: &str) -> (Vec<String>, Vec<String>) {
    let mut audio = BTreeSet::new();
    let mut subtitles = BTreeSet::new();

    let normalized = normalize_language_signal_input(input);
    let normalized_lower = normalized.to_ascii_lowercase();
    let subtitle_only_context = (normalized_lower.contains(" sub")
        || normalized_lower.contains(" subtitle"))
        && !normalized_lower.contains(" dub")
        && !normalized_lower.contains(" audio");

    for captures in ANIME_LANGUAGE_RE.captures_iter(&normalized) {
        let Some(raw) = captures.get(0).map(|value| value.as_str()) else {
            continue;
        };
        let token = raw
            .replace(['-', '_', '.'], " ")
            .replace(['ñ', 'Ñ'], "N")
            .to_ascii_uppercase();
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        if matches!(token, "DUAL" | "DUAL AUDIO" | "MULTI") {
            continue;
        }
        if matches!(token, "MULTI SUB" | "MULTI SUBS" | "MULTISUB") {
            subtitles.insert("MULTI".to_string());
            continue;
        }
        if matches!(token, "SUB" | "SUBS" | "SUBBED") {
            continue;
        }

        let Some(language) = normalize_anime_language_token(token) else {
            continue;
        };
        if is_subtitle_language_token(token, language) || subtitle_only_context {
            subtitles.insert(language.to_string());
        } else {
            audio.insert(language.to_string());
        }
    }

    if ENGLISH_DUB_SIGNAL_RE.is_match(&normalized) {
        audio.insert("ENG".to_string());
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
        || MULTI_SUB_SIGNAL_RE.is_match(&normalized)
    {
        subtitles.insert("MULTI".to_string());
    }
    (audio.into_iter().collect(), subtitles.into_iter().collect())
}

fn normalize_language_signal_input(input: &str) -> String {
    input
        .replace('\u{3000}', " ")
        .replace(['.', '_', '-', '/', '[', ']', '(', ')'], " ")
}

fn normalize_anime_language_token(token: &str) -> Option<&'static str> {
    match token {
        "ENG" | "ENGLISH" => Some("ENG"),
        "TRUEFRENCH" | "FRENCH" | "FRE" | "FRA" | "FR" | "SUBFRENCH" | "VOSTFR" | "VF" | "VF2"
        | "VFQ" | "VFF" | "VFI" => Some("FRE"),
        "GERMAN" | "SWISSGERMAN" | "GER" => Some("GER"),
        "ITALIAN" | "ITALY" | "ITA" => Some("ITA"),
        "SPANISH" | "ESPAÑOL" | "ESPANOL" | "CASTELLANO" | "SPA" | "ESP" => Some("SPA"),
        "CZECH" | "CZE" => Some("CZE"),
        "JAPANESE" | "JPN" | "JAP" | "JA" => Some("JPN"),
        "CHINESE" | "CANTONESE" | "MANDARIN" | "CHI" => Some("CHI"),
        "CHS" | "GB" => Some("CHS"),
        "CHT" | "BIG5" => Some("CHT"),
        "KOREAN" | "KOR" => Some("KOR"),
        "LATVIAN" | "LAT" | "LAV" | "LV" => Some("LV"),
        "RUSSIAN" | "RUS" | "RU" => Some("RUS"),
        "POLISH" | "PL" | "PLDUB" | "DUBPL" | "PLLEK" | "LEKPL" => Some("POL"),
        "DANISH" | "DAN" => Some("DAN"),
        "DUTCH" | "FLEMISH" => Some("DUT"),
        "PORTUGUESE" | "POR" => Some("POR"),
        _ => None,
    }
}

fn is_subtitle_language_token(token: &str, language: &str) -> bool {
    matches!(token, "SUBFRENCH" | "VOSTFR")
        || matches!(language, "CHS" | "CHT")
        || token.contains("SUB")
}

fn normalize_resolution(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "1920x1080" | "1080p10" | "1080i" | "1440p" | "fhd" | "4kto1080p" | "bluray1080p"
        | "bd1080p" => "1080p".to_string(),
        "1280x720" | "720i" | "960p" | "bluray720p" | "bd720p" => "720p".to_string(),
        "576i" => "576p".to_string(),
        "480i" | "640x480" | "848x480" => "480p".to_string(),
        "4096x2160" | "3840x2160" | "4k" | "uhd" | "2160i" => "2160p".to_string(),
        other => other.to_string(),
    }
}

fn normalize_codec(value: &str) -> String {
    match value.to_ascii_lowercase().replace('.', "").as_str() {
        "h265" | "x265" | "hevc" => "HEVC".to_string(),
        "h264" | "x264" | "avc" => "H264".to_string(),
        "av1" => "AV1".to_string(),
        "vp9" => "VP9".to_string(),
        "xvid" => "XVID".to_string(),
        "divx" => "DIVX".to_string(),
        "mpeg2" | "mpeg-2" => "MPEG2".to_string(),
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
        || WEB_DL_SOURCE_RE.is_match(&value)
        || WEB_RIP_SOURCE_RE.is_match(&value)
        || BLURAY_SOURCE_RE.is_match(&value)
        || HDTV_SOURCE_RE.is_match(&value)
        || DVD_SOURCE_RE.is_match(&value)
        || RAW_HD_SOURCE_RE.is_match(&value)
        || DUAL_AUDIO_SIGNAL_RE.is_match(&value)
        || MULTI_SUB_SIGNAL_RE.is_match(&value)
        || ANIME_LANGUAGE_RE.is_match(&value)
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
    struct AnimeReconciliationGoldenSet {
        fixture_set: String,
        resolver: String,
        classification: String,
        cases: Vec<AnimeReconciliationGoldenCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeReconciliationGoldenCase {
        id: String,
        classification: String,
        input: String,
        aliases: Vec<String>,
        targets: Vec<AnimeReconciliationTarget>,
        expected: AnimeReconciliationExpected,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeReconciliationTarget {
        target_key: String,
        canonical_key: Option<String>,
        title: String,
        season_number: Option<i32>,
        episode_number: Option<i32>,
        absolute_episode_number: Option<i32>,
        tvdb_episode_id: Option<String>,
        anidb_episode_id: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeReconciliationExpected {
        outcome: String,
        target_keys: Vec<String>,
        review_reasons: Vec<String>,
        rejection_reasons: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeParserGoldenSet {
        fixture_set: String,
        resolver: String,
        classification: String,
        #[serde(default)]
        sonarr_commit: Option<String>,
        #[serde(default)]
        source_fixture_set: Option<String>,
        cases: Vec<AnimeParserGoldenCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AnimeParserGoldenCase {
        id: String,
        classification: String,
        #[serde(default)]
        source_fixture: Option<String>,
        #[serde(default)]
        source_method: Option<String>,
        #[serde(default)]
        source_line: Option<u64>,
        #[serde(default)]
        source_classification: Option<String>,
        #[serde(default)]
        origin: Option<String>,
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

    fn load_anime_reconciliation_goldens() -> AnimeReconciliationGoldenSet {
        serde_json::from_str(include_str!(
            "fixtures/anime_rr3_reconciliation_goldens.json"
        ))
        .expect("valid RR-3Q reconciliation golden fixture")
    }

    fn load_anime_parser_goldens() -> AnimeParserGoldenSet {
        serde_json::from_str(include_str!("fixtures/anime_rr3_parser_goldens.json"))
            .expect("valid RR-3 anime parser golden fixture")
    }

    fn load_anime_quality_goldens() -> AnimeParserGoldenSet {
        serde_json::from_str(include_str!(
            "fixtures/sonarr_rr3_anime_quality_goldens.json"
        ))
        .expect("valid RR-3O anime quality golden fixture")
    }

    fn load_anime_language_goldens() -> AnimeParserGoldenSet {
        serde_json::from_str(include_str!(
            "fixtures/sonarr_rr3_anime_language_goldens.json"
        ))
        .expect("valid RR-3O anime language golden fixture")
    }

    fn load_anime_release_group_goldens() -> AnimeParserGoldenSet {
        serde_json::from_str(include_str!(
            "fixtures/sonarr_rr3_anime_release_group_goldens.json"
        ))
        .expect("valid RR-3O anime release group golden fixture")
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
            "rr3q_asserted",
            "unsupported_by_product_policy",
        ];
        assert!(
            allowed.contains(&classification),
            "{id} has non-production RR-3 classification {classification}"
        );
    }

    fn rr3_fixture_payloads() -> Vec<(&'static str, JsonValue)> {
        vec![
            (
                "anime_rr3_shoko_inventory.json",
                serde_json::from_str(include_str!("fixtures/anime_rr3_shoko_inventory.json"))
                    .expect("valid RR-3 Shoko inventory fixture"),
            ),
            (
                "anime_rr3_sonarr_seed_fixtures.json",
                serde_json::from_str(include_str!("fixtures/anime_rr3_sonarr_seed_fixtures.json"))
                    .expect("valid RR-3 Sonarr anime seed fixture"),
            ),
            (
                "anime_rr3_parser_goldens.json",
                serde_json::from_str(include_str!("fixtures/anime_rr3_parser_goldens.json"))
                    .expect("valid RR-3 anime parser golden fixture"),
            ),
            (
                "sonarr_rr3_anime_quality_goldens.json",
                serde_json::from_str(include_str!(
                    "fixtures/sonarr_rr3_anime_quality_goldens.json"
                ))
                .expect("valid RR-3O anime quality golden fixture"),
            ),
            (
                "sonarr_rr3_anime_language_goldens.json",
                serde_json::from_str(include_str!(
                    "fixtures/sonarr_rr3_anime_language_goldens.json"
                ))
                .expect("valid RR-3O anime language golden fixture"),
            ),
            (
                "sonarr_rr3_anime_release_group_goldens.json",
                serde_json::from_str(include_str!(
                    "fixtures/sonarr_rr3_anime_release_group_goldens.json"
                ))
                .expect("valid RR-3O anime release group golden fixture"),
            ),
            (
                "anime_rr3_metadata_graph_goldens.json",
                serde_json::from_str(include_str!(
                    "fixtures/anime_rr3_metadata_graph_goldens.json"
                ))
                .expect("valid RR-3 metadata graph golden fixture"),
            ),
            (
                "anime_rr3_reconciliation_goldens.json",
                serde_json::from_str(include_str!(
                    "fixtures/anime_rr3_reconciliation_goldens.json"
                ))
                .expect("valid RR-3Q reconciliation golden fixture"),
            ),
        ]
    }

    fn collect_pending_rr3_fixture_values(
        fixture: &str,
        path: &str,
        value: &JsonValue,
        failures: &mut Vec<String>,
    ) {
        match value {
            JsonValue::String(text) => {
                let normalized = text.to_ascii_lowercase();
                if normalized.contains("pending")
                    || normalized == "known_parity_gap"
                    || normalized == "parity_gap_pending"
                    || normalized == "rr3_pending"
                    || normalized == "anime_rr3_pending"
                {
                    failures.push(format!("{fixture}:{path} = {text:?}"));
                }
            }
            JsonValue::Array(values) => {
                for (index, item) in values.iter().enumerate() {
                    collect_pending_rr3_fixture_values(
                        fixture,
                        &format!("{path}[{index}]"),
                        item,
                        failures,
                    );
                }
            }
            JsonValue::Object(values) => {
                for (key, item) in values {
                    let child_path = if path.is_empty() {
                        key.to_string()
                    } else {
                        format!("{path}.{key}")
                    };
                    collect_pending_rr3_fixture_values(fixture, &child_path, item, failures);
                }
            }
            _ => {}
        }
    }

    fn assert_rr3_fixture_gate_counts_are_frozen() {
        let inventory = load_shoko_inventory();
        let seed = load_anime_seed_set();
        let parser_goldens = load_anime_parser_goldens();
        let quality_goldens = load_anime_quality_goldens();
        let language_goldens = load_anime_language_goldens();
        let release_group_goldens = load_anime_release_group_goldens();
        let graph_goldens = load_anime_graph_goldens();
        let reconciliation_goldens = load_anime_reconciliation_goldens();
        let source = load_sonarr_generated_payload();

        assert_eq!(inventory.fixture_set, "rr3-shoko-anime-resolver-inventory");
        assert_eq!(inventory.inspected_source_files.len(), 18);
        assert_eq!(inventory.fixture_schemas.len(), 5);

        assert_eq!(seed.fixture_set, "rr3-sonarr-anime-seed-fixtures");
        assert_eq!(seed.counts.total, 60);
        assert_eq!(seed.counts.release_group, 6);
        assert_eq!(seed.counts.unicode_title, 54);
        assert_eq!(seed.cases.len(), 60);

        assert_eq!(parser_goldens.fixture_set, "rr3-anime-parser-goldens");
        assert_eq!(parser_goldens.cases.len(), 8);
        assert_eq!(quality_goldens.fixture_set, "rr3o-sonarr-anime-quality");
        assert_eq!(quality_goldens.cases.len(), 7);
        assert_eq!(language_goldens.fixture_set, "rr3o-sonarr-anime-language");
        assert_eq!(language_goldens.cases.len(), 8);
        assert_eq!(
            release_group_goldens.fixture_set,
            "rr3o-sonarr-anime-release-group"
        );
        assert_eq!(release_group_goldens.cases.len(), 7);
        assert_eq!(
            graph_goldens.fixture_set,
            "rr3-anime-metadata-graph-goldens"
        );
        assert_eq!(graph_goldens.cases.len(), 4);
        assert_eq!(
            reconciliation_goldens.fixture_set,
            "rr3q-anime-reconciliation-goldens"
        );
        assert_eq!(reconciliation_goldens.cases.len(), 7);

        let source_cases = source["cases"].as_array().expect("source cases array");
        assert_eq!(source_cases.len(), 1192);
        assert_eq!(
            source_cases
                .iter()
                .filter(|case| case["classification"].as_str() == Some("anime_rr3"))
                .count(),
            60
        );
        assert_eq!(
            source_cases
                .iter()
                .filter(|case| {
                    case["classification"].as_str() == Some("unsupported_by_product_policy")
                })
                .count(),
            13
        );
        assert_eq!(
            source_cases
                .iter()
                .filter(|case| case["skipReason"].as_str() == Some("known_parity_gap"))
                .count(),
            0
        );

        let mut pending = Vec::new();
        for (fixture, payload) in rr3_fixture_payloads() {
            collect_pending_rr3_fixture_values(fixture, "", &payload, &mut pending);
        }
        assert!(
            pending.is_empty(),
            "RR-3 production fixtures still contain pending/parity-gap rows:\n{}",
            pending.join("\n")
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
            scoped_aliases: vec![],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E01".to_string(),
                    canonical_key: Some("tvdb:100:S01E01".to_string()),
                    title: "Episode One".to_string(),
                    season_number: Some(1),
                    anilist_season_id: Some("100".to_string()),
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
                    anilist_season_id: Some("100".to_string()),
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
                        episode_label: None,
                        mainline_episode_number: None,
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

    impl AnimeReconciliationTarget {
        fn to_candidate_target(&self) -> AnimeCandidateTarget {
            AnimeCandidateTarget {
                target_key: self.target_key.clone(),
                canonical_key: self.canonical_key.clone(),
                title: self.title.clone(),
                season_number: self.season_number,
                anilist_season_id: None,
                episode_number: self.episode_number,
                absolute_episode_number: self.absolute_episode_number,
                tvdb_episode_id: self.tvdb_episode_id.clone(),
                anidb_episode_id: self.anidb_episode_id.clone(),
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

    fn assert_anime_parser_golden_set(
        goldens: &AnimeParserGoldenSet,
        expected_fixture_set: &str,
        expected_case_count: usize,
    ) {
        assert_eq!(goldens.fixture_set, expected_fixture_set);
        assert_eq!(goldens.resolver, "anime_shoko_style");
        assert_eq!(goldens.classification, "rr3d_asserted");
        if let Some(sonarr_commit) = goldens.sonarr_commit.as_deref() {
            assert_eq!(sonarr_commit, "bf5d48c", "{expected_fixture_set} commit");
        }
        if let Some(source_fixture_set) = goldens.source_fixture_set.as_deref() {
            assert_eq!(
                source_fixture_set, "rr2-sonarr-conventional-tv-exhaustive",
                "{expected_fixture_set} source fixture set"
            );
        }
        assert_eq!(
            goldens.cases.len(),
            expected_case_count,
            "{expected_fixture_set} case count"
        );

        let mut seen = BTreeSet::new();
        for case in &goldens.cases {
            assert!(
                seen.insert(case.id.as_str()),
                "duplicate golden id {}",
                case.id
            );
            assert_eq!(case.classification, "rr3d_asserted", "{}", case.id);
            if goldens.source_fixture_set.as_deref()
                == Some("rr2-sonarr-conventional-tv-exhaustive")
                && case.origin.as_deref() != Some("elixir_rr3o_enrichment")
            {
                assert!(
                    case.source_fixture
                        .as_deref()
                        .is_some_and(|value| !value.is_empty()),
                    "{} missing source fixture",
                    case.id
                );
                assert!(
                    case.source_method
                        .as_deref()
                        .is_some_and(|value| !value.is_empty()),
                    "{} missing source method",
                    case.id
                );
                assert!(
                    case.source_line.is_some_and(|value| value > 0),
                    "{} missing source line",
                    case.id
                );
                assert!(
                    matches!(
                        case.source_classification.as_deref(),
                        Some("tv_rr2" | "anime_rr3" | "unsupported_by_product_policy")
                    ),
                    "{} source classification missing",
                    case.id
                );
            }
            let parsed = parse_anime_release_title(&case.input);
            assert_parser_expected(&case.id, &parsed, &case.expected);
            assert_eq!(parsed.parser_version, ANIME_PRE_DOWNLOAD_PARSER_VERSION);
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
        assert_anime_parser_golden_set(&goldens, "rr3-anime-parser-goldens", 8);
    }

    #[test]
    fn rr3o_anime_quality_goldens_pass() {
        let goldens = load_anime_quality_goldens();
        assert_anime_parser_golden_set(&goldens, "rr3o-sonarr-anime-quality", 7);
    }

    #[test]
    fn rr3o_anime_language_goldens_pass() {
        let goldens = load_anime_language_goldens();
        assert_anime_parser_golden_set(&goldens, "rr3o-sonarr-anime-language", 8);
    }

    #[test]
    fn rr3o_anime_release_group_goldens_pass() {
        let goldens = load_anime_release_group_goldens();
        assert_anime_parser_golden_set(&goldens, "rr3o-sonarr-anime-release-group", 7);
    }

    #[test]
    fn rr3p_anime_adapter_maps_absolute_only_title() {
        let parsed = parse_anime_release_title("[SubsPlease] One Piece - 1149 (1080p) [ABCDEF12]");

        assert_eq!(parsed.sonarr_facts.absolute_episode_numbers, vec![1149]);
        assert_eq!(parsed.absolute_episode_numbers, vec![1149]);
        assert_eq!(parsed.release_group.as_deref(), Some("SubsPlease"));
        assert_eq!(parsed.release_hash.as_deref(), Some("ABCDEF12"));
        assert_eq!(parsed.crc32.as_deref(), Some("ABCDEF12"));
        assert_eq!(
            parsed.sonarr_facts.quality.resolution.as_deref(),
            Some("1080p")
        );
    }

    #[test]
    fn anime_crc32_with_episode_shaped_hex_is_not_a_coordinate() {
        let parsed = parse_anime_release_title(
            "[cbm]_FLCL_Alternative_06_(English_Dub)_[HDTV_720p_8bit]_[E5E7481E].mkv",
        );

        let coordinates = parsed
            .episode_numbers
            .iter()
            .chain(&parsed.absolute_episode_numbers)
            .chain(&parsed.sonarr_facts.episode_numbers)
            .chain(&parsed.sonarr_facts.absolute_episode_numbers)
            .copied()
            .collect::<BTreeSet<_>>();
        assert!(coordinates.contains(&6));
        assert!(!coordinates.contains(&7481));
        assert!(coordinates.len() <= 2);
        assert_eq!(parsed.crc32.as_deref(), Some("E5E7481E"));
    }

    #[test]
    fn rr3p_anime_adapter_maps_sxxeyy_title() {
        let parsed =
            parse_anime_release_title("[EMBER] Solo Leveling S02E02 1080p WEB-DL AAC2.0 H.264");

        assert_eq!(parsed.sonarr_facts.season_number, Some(2));
        assert_eq!(parsed.sonarr_facts.episode_numbers, vec![2]);
        assert_eq!(parsed.season_number, Some(2));
        assert_eq!(parsed.episode_numbers, vec![2]);
        assert_eq!(parsed.sonarr_facts.release_kind, ReleaseKind::Single);
        assert_eq!(
            parsed.sonarr_facts.quality.source.as_deref(),
            Some("web_dl")
        );
        assert_eq!(
            parsed.sonarr_facts.quality.video_codec.as_deref(),
            Some("H264")
        );
    }

    #[test]
    fn rr3p_anime_adapter_preserves_mixed_absolute_and_sxxeyy_evidence() {
        let parsed =
            parse_anime_release_title("[Group] Example Anime S02E03 [027] [1080p] [ABCDEF12]");

        assert_eq!(parsed.sonarr_facts.season_number, Some(2));
        assert_eq!(parsed.sonarr_facts.episode_numbers, vec![3]);
        assert_eq!(parsed.episode_numbers, vec![3]);
        assert_eq!(parsed.absolute_episode_numbers, vec![27]);
        assert_eq!(
            parsed
                .anime_signal_facts
                .fallback_absolute_episode_hypotheses,
            vec![27]
        );
        assert!(
            parsed
                .anime_signal_facts
                .normalized_title_candidates
                .iter()
                .any(|title| title == "exampleanime")
        );
    }

    #[test]
    fn rr3p_anime_adapter_maps_special_facts() {
        let parsed = parse_anime_release_title("Example.Anime.S00E01.Special.1080p.WEB-DL-GRP");

        assert_eq!(parsed.sonarr_facts.season_number, Some(0));
        assert_eq!(parsed.sonarr_facts.episode_numbers, vec![1]);
        assert!(parsed.sonarr_facts.special);
        assert_eq!(
            parsed.sonarr_facts.special_absolute_episode_numbers,
            vec!["S00E01"]
        );
        assert_eq!(parsed.episode_type, AnimeEpisodeType::Special);
    }

    #[test]
    fn rr3p_anime_adapter_maps_ranges() {
        let parsed = parse_anime_release_title("Example.Anime.S01E01-E03.1080p.WEB-DL-GRP");

        assert_eq!(parsed.sonarr_facts.release_kind, ReleaseKind::MultiEpisode);
        assert_eq!(parsed.sonarr_facts.batch_kind, AnimeBatchKind::Range);
        assert_eq!(parsed.sonarr_facts.episode_numbers, vec![1, 2, 3]);
        assert_eq!(parsed.episode_numbers, vec![1, 2, 3]);
        assert!(
            parsed
                .anime_signal_facts
                .bounded_explicit_ranges
                .iter()
                .any(|range| range.start == 1 && range.end == 3)
        );
    }

    #[test]
    fn rr3p_anime_adapter_maps_season_pack() {
        let parsed = parse_anime_release_title("Example.Anime.S01.1080p.WEB-DL.H264-GRP");

        assert_eq!(parsed.sonarr_facts.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(parsed.sonarr_facts.batch_kind, AnimeBatchKind::SeasonPack);
        assert_eq!(parsed.sonarr_facts.season_number, Some(1));
        assert!(parsed.sonarr_facts.full_season);
        assert_eq!(parsed.batch_kind, AnimeBatchKind::SeasonPack);
        assert!(
            parsed
                .review_reasons
                .iter()
                .any(|reason| reason == "file_list_required_for_pack")
        );
    }

    #[test]
    fn rr3p_anime_adapter_maps_multi_season_pack() {
        let parsed = parse_anime_release_title("Example.Anime.S01-S02.1080p.WEB-DL.H264-GRP");

        assert_eq!(
            parsed.sonarr_facts.release_kind,
            ReleaseKind::MultiSeasonPack
        );
        assert_eq!(
            parsed.sonarr_facts.batch_kind,
            AnimeBatchKind::MultiSeasonPack
        );
        assert_eq!(parsed.sonarr_facts.season_number, Some(1));
        assert_eq!(parsed.sonarr_facts.season_end_number, Some(2));
        assert!(parsed.sonarr_facts.is_multi_season);
        assert_eq!(parsed.batch_kind, AnimeBatchKind::MultiSeasonPack);
    }

    #[test]
    fn rr3p_anime_adapter_maps_mini_series_flag_without_identity_decision() {
        let parsed = parse_anime_release_title("Example Anime - E01-E03 1080p WEB-DL-GRP");

        assert!(parsed.sonarr_facts.is_mini_series);
        assert_eq!(parsed.sonarr_facts.season_number, Some(1));
        assert_eq!(parsed.sonarr_facts.episode_numbers, vec![1, 2, 3]);
        assert!(
            !parsed
                .sonarr_facts
                .all_titles
                .iter()
                .any(|title| title.is_empty())
        );
        assert!(parsed.sonarr_facts.raw.is_some());
    }

    #[test]
    fn rr3l_production_parity_gate_has_no_pending_rows() {
        let inventory = load_shoko_inventory();
        let seed = load_anime_seed_set();
        let parser_goldens = load_anime_parser_goldens();
        let quality_goldens = load_anime_quality_goldens();
        let language_goldens = load_anime_language_goldens();
        let release_group_goldens = load_anime_release_group_goldens();
        let graph_goldens = load_anime_graph_goldens();
        let reconciliation_goldens = load_anime_reconciliation_goldens();
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
            "quality fixture set",
            &quality_goldens.classification,
        );
        assert_rr3_production_classification(
            "language fixture set",
            &language_goldens.classification,
        );
        assert_rr3_production_classification(
            "release group fixture set",
            &release_group_goldens.classification,
        );
        for case in quality_goldens
            .cases
            .iter()
            .chain(language_goldens.cases.iter())
            .chain(release_group_goldens.cases.iter())
        {
            assert_rr3_production_classification(&case.id, &case.classification);
        }

        assert_rr3_production_classification(
            "metadata graph fixture set",
            &graph_goldens.classification,
        );
        for case in &graph_goldens.cases {
            assert_rr3_production_classification(&case.id, &case.classification);
        }
        assert_rr3_production_classification(
            "reconciliation fixture set",
            &reconciliation_goldens.classification,
        );
        for case in &reconciliation_goldens.cases {
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
    fn rr3t_production_gate_rejects_pending_anime_fixture_rows() {
        let mut failures = Vec::new();

        for (fixture, payload) in rr3_fixture_payloads() {
            collect_pending_rr3_fixture_values(fixture, "", &payload, &mut failures);
        }

        assert!(
            failures.is_empty(),
            "RR-3 production fixtures still contain pending/parity-gap rows:\n{}",
            failures.join("\n")
        );
    }

    #[test]
    fn rrmt_anime_fixture_gate_counts_are_frozen() {
        assert_rr3_fixture_gate_counts_are_frozen();
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
    fn metadata_graph_normalizes_tokyo_ghoul_relation_and_provider_numbering() {
        let season = |season_number, anilist_id: &str, title: &str| AniListSeasonChainEntry {
            season_number,
            anilist_id: anilist_id.to_string(),
            title: title.to_string(),
            format: Some("TV".to_string()),
            season_year: None,
            start_year: None,
            status: Some("FINISHED".to_string()),
            episodes: Some(12),
            next_airing_episode: None,
            next_airing_at: None,
            confidence: 1.0,
        };
        let mapping = |anilist_id: &str,
                       provider_season: i32,
                       provider_episode: i32,
                       local_episode: i32,
                       absolute_episode: i32,
                       tvdb_episode_id: &str| AniZipMapping {
            ids: ExternalIds {
                anilist: Some(anilist_id.to_string()),
                tvdb_series: Some("305014".to_string()),
                ..Default::default()
            },
            episodes: vec![crate::library::AniZipEpisodeRecord {
                season_number: Some(provider_season),
                episode_number: Some(provider_episode),
                absolute_episode_number: Some(absolute_episode),
                episode_label: Some(local_episode.to_string()),
                mainline_episode_number: Some(local_episode),
                tvdb_id: Some(tvdb_episode_id.to_string()),
                raw: serde_json::json!({
                    "seasonNumber": provider_season,
                    "episodeNumber": provider_episode,
                    "absoluteEpisodeNumber": absolute_episode,
                    "episode": local_episode,
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let graph = build_anime_metadata_graph(AnimeMetadataGraphInput {
            title: "Tokyo Ghoul".to_string(),
            year: Some(2014),
            seed_anilist_id: "20605".to_string(),
            seed_season_number: 1,
            external_ids: ExternalIds::default(),
            seasons: vec![
                AnimeSeasonMapping {
                    season: season(2, "20850", "Tokyo Ghoul Root A"),
                    mapping: Some(mapping("20850", 2, 1, 1, 13, "root-a-tvdb-1")),
                },
                AnimeSeasonMapping {
                    season: season(4, "102351", "Tokyo Ghoul:re 2nd Season"),
                    mapping: Some(mapping("102351", 3, 16, 4, 40, "re-2-tvdb-16")),
                },
            ],
        });

        let root_a = graph
            .targets
            .iter()
            .find(|target| target.anilist_season_id == "20850")
            .expect("Root A target");
        assert_eq!(root_a.target_key, "S02E01");
        assert_eq!(root_a.absolute_episode_number, Some(13));

        let re_second = graph
            .targets
            .iter()
            .find(|target| target.anilist_season_id == "102351")
            .expect("Tokyo Ghoul:re 2 target");
        assert_eq!(re_second.season.season_number, 4);
        assert_eq!(re_second.target_key, "S04E04");
        assert_eq!(re_second.season_number, Some(4));
        assert_eq!(re_second.episode_number, Some(4));
        assert_eq!(re_second.absolute_episode_number, Some(40));
        assert_eq!(re_second.tvdb_episode_id.as_deref(), Some("re-2-tvdb-16"));
        assert_eq!(re_second.raw["seasonNumber"], 3);
        assert_eq!(re_second.raw["episodeNumber"], 16);
    }

    #[test]
    fn alm3_metadata_graph_keeps_localized_titles_with_language_scope() {
        let graph = build_anime_metadata_graph(AnimeMetadataGraphInput {
            title: "Long Running Anime".to_string(),
            year: Some(1999),
            seed_anilist_id: "21".to_string(),
            seed_season_number: 1,
            external_ids: ExternalIds {
                anilist: Some("21".to_string()),
                ..ExternalIds::default()
            },
            seasons: vec![AnimeSeasonMapping {
                season: AniListSeasonChainEntry {
                    season_number: 1,
                    anilist_id: "21".to_string(),
                    title: "Long Running Anime".to_string(),
                    format: Some("TV".to_string()),
                    season_year: Some(1999),
                    start_year: Some(1999),
                    status: Some("RELEASING".to_string()),
                    episodes: None,
                    next_airing_episode: Some(23),
                    next_airing_at: None,
                    confidence: 1.0,
                },
                mapping: Some(AniZipMapping {
                    ids: ExternalIds {
                        anilist: Some("21".to_string()),
                        tvdb_series: Some("81797".to_string()),
                        ..ExternalIds::default()
                    },
                    episodes: vec![
                        crate::library::AniZipEpisodeRecord {
                            season_number: Some(1),
                            episode_number: Some(1),
                            absolute_episode_number: Some(1),
                            episode_label: Some("1".to_string()),
                            mainline_episode_number: Some(1),
                            title: Some("I'm Luffy!".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: Some("1001".to_string()),
                            anidb_eid: None,
                            raw: serde_json::json!({ "episode": "1" }),
                        },
                        crate::library::AniZipEpisodeRecord {
                            season_number: Some(2),
                            episode_number: Some(9),
                            absolute_episode_number: Some(17),
                            episode_label: Some("9".to_string()),
                            mainline_episode_number: Some(9),
                            title: Some("Captain Usopp!".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: None,
                            anidb_eid: None,
                            raw: serde_json::json!({
                                "episode": "9",
                                "airdate": "2000-01-12"
                            }),
                        },
                        crate::library::AniZipEpisodeRecord {
                            season_number: None,
                            episode_number: None,
                            absolute_episode_number: Some(23),
                            episode_label: Some("23".to_string()),
                            mainline_episode_number: Some(23),
                            title: Some("Protect Baratie!".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: None,
                            anidb_eid: None,
                            raw: serde_json::json!({
                                "episode": "23",
                                "airdate": "2000-05-03"
                            }),
                        },
                        crate::library::AniZipEpisodeRecord {
                            season_number: None,
                            episode_number: None,
                            absolute_episode_number: Some(24),
                            episode_label: Some("24".to_string()),
                            mainline_episode_number: Some(24),
                            title: Some("Episode 24".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: None,
                            anidb_eid: None,
                            raw: serde_json::json!({
                                "episode": "24",
                                "airdate": "2000-05-10"
                            }),
                        },
                        crate::library::AniZipEpisodeRecord {
                            season_number: None,
                            episode_number: None,
                            absolute_episode_number: Some(25),
                            episode_label: Some("25".to_string()),
                            mainline_episode_number: Some(25),
                            title: Some("Episode 25".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: None,
                            anidb_eid: None,
                            raw: serde_json::json!({
                                "episode": "25",
                                "airdate": "2000-05-17"
                            }),
                        },
                        crate::library::AniZipEpisodeRecord {
                            season_number: None,
                            episode_number: None,
                            absolute_episode_number: None,
                            episode_label: Some("S4".to_string()),
                            mainline_episode_number: None,
                            title: Some("Special".to_string()),
                            overview: None,
                            runtime_minutes: None,
                            image: None,
                            tvdb_id: None,
                            anidb_eid: None,
                            raw: serde_json::json!({ "episode": "S4" }),
                        },
                    ],
                    images: Vec::new(),
                    titles: HashMap::from([
                        ("en".to_string(), "Long Running Anime".to_string()),
                        ("x-jat".to_string(), "Long Running Anime Romaji".to_string()),
                        ("ja".to_string(), "長編アニメ".to_string()),
                    ]),
                }),
            }],
        });

        assert!(graph.scoped_aliases.iter().any(|alias| {
            alias.display == "Long Running Anime Romaji"
                && alias.language.as_deref() == Some("x-jat")
        }));
        assert!(graph.scoped_aliases.iter().any(|alias| {
            alias.display == "長編アニメ" && alias.language.as_deref() == Some("ja")
        }));

        let target_keys = graph
            .targets
            .iter()
            .map(|target| target.target_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            target_keys,
            vec!["A0001", "A0009", "A0023", "A0024", "A0025"]
        );

        let single_digit = graph
            .targets
            .iter()
            .find(|target| target.target_key == "A0009")
            .expect("single digit numeric ani.zip episode target");
        assert_eq!(single_digit.absolute_episode_number, Some(9));
        assert_eq!(single_digit.season_number, None);
        assert_eq!(single_digit.episode_number, None);
        assert_eq!(single_digit.air_date.as_deref(), Some("2000-01-12"));
        assert!(
            !graph
                .targets
                .iter()
                .any(|target| target.target_key == "S02E09"),
            "majority absolute-only ani.zip mappings must not drop monolith rows into stray TVDB seasons"
        );

        let absolute = graph
            .targets
            .iter()
            .find(|target| target.target_key == "A0023")
            .expect("numeric ani.zip episode target");
        assert_eq!(absolute.source, AnimeGraphTargetSource::AniZip);
        assert_eq!(absolute.season_number, None);
        assert_eq!(absolute.episode_number, None);
        assert_eq!(absolute.absolute_episode_number, Some(23));
        assert_eq!(absolute.title, "Protect Baratie!");
        assert_eq!(absolute.air_date.as_deref(), Some("2000-05-03"));

        assert!(
            !graph.targets.iter().any(|target| target.title == "Special"),
            "prefixed ani.zip specials must not become normal mainline targets"
        );
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
    fn alm9_complete_absolute_entity_persists_release_episode_offset() {
        let mapping = AniZipMapping {
            episodes: (13..=24)
                .map(|episode| crate::library::AniZipEpisodeRecord {
                    absolute_episode_number: Some(episode),
                    episode_label: Some(episode.to_string()),
                    mainline_episode_number: Some(episode),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let graph = build_anime_metadata_graph(AnimeMetadataGraphInput {
            title: "Komi Can't Communicate".to_string(),
            year: Some(2022),
            seed_anilist_id: "142984".to_string(),
            seed_season_number: 1,
            external_ids: ExternalIds::default(),
            seasons: vec![AnimeSeasonMapping {
                season: AniListSeasonChainEntry {
                    season_number: 1,
                    anilist_id: "142984".to_string(),
                    title: "Komi Can't Communicate Season 2".to_string(),
                    format: Some("TV".to_string()),
                    season_year: Some(2022),
                    start_year: Some(2022),
                    status: Some("FINISHED".to_string()),
                    episodes: Some(12),
                    next_airing_episode: None,
                    next_airing_at: None,
                    confidence: 1.0,
                },
                mapping: Some(mapping),
            }],
        });
        let targets = graph.to_new_acquisition_targets(0, Utc::now());

        assert_eq!(targets.len(), 12);
        assert!(targets.iter().all(|target| {
            target
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("episodeNumberOffset"))
                .and_then(JsonValue::as_i64)
                == Some(12)
        }));
    }

    #[test]
    fn rr3q_reconciliation_goldens_cover_expected_outcomes() {
        let goldens = load_anime_reconciliation_goldens();
        assert_eq!(goldens.fixture_set, "rr3q-anime-reconciliation-goldens");
        assert_eq!(goldens.resolver, "anime_shoko_style");
        assert_eq!(goldens.classification, "rr3q_asserted");
        assert_eq!(goldens.cases.len(), 7);

        let mut outcomes = BTreeSet::new();
        for case in &goldens.cases {
            assert_eq!(case.classification, "rr3q_asserted", "{}", case.id);
            let context = AnimeCandidateScoringContext {
                graph_fingerprint: Some(format!("rr3q:{}", case.id)),
                aliases: case.aliases.clone(),
                scoped_aliases: vec![],
                targets: case
                    .targets
                    .iter()
                    .map(AnimeReconciliationTarget::to_candidate_target)
                    .collect(),
            };
            let candidate = AnimeCandidateInput {
                title: case.input.clone(),
                source_kind: "golden".to_string(),
                quality: None,
                size_bytes: None,
                seeders: None,
                cached_debrid: Some(true),
                rank: None,
                source_score: None,
                supported_routes: vec!["debrid".to_string()],
                default_route: Some("debrid".to_string()),
            };
            let score = score_anime_candidate(&context, &candidate);
            let reconciliation = &score.reconciliation;
            let outcome = serde_json::to_value(reconciliation.outcome)
                .expect("outcome serializes")
                .as_str()
                .expect("outcome string")
                .to_string();
            outcomes.insert(outcome.clone());
            assert_eq!(outcome, case.expected.outcome, "{}", case.id);
            assert_eq!(
                reconciliation
                    .target_matches
                    .iter()
                    .map(|target| target.target_key.clone())
                    .collect::<Vec<_>>(),
                case.expected.target_keys,
                "{} target keys",
                case.id
            );
            assert_eq!(
                reconciliation.review_reasons, case.expected.review_reasons,
                "{} review reasons",
                case.id
            );
            assert_eq!(
                reconciliation.rejection_reasons, case.expected.rejection_reasons,
                "{} rejection reasons",
                case.id
            );

            if case.id == "one_piece_absolute_only_graph_target_remains_mappable" {
                assert_eq!(
                    reconciliation
                        .target_matches
                        .first()
                        .map(|target| target.target_key.as_str()),
                    Some("A0009"),
                    "mostly absolute-only ani.zip graph targets must stay selectable"
                );
            }
        }

        assert_eq!(
            outcomes,
            [
                "agreement",
                "translation",
                "augmentation",
                "benign_mismatch",
                "true_contradiction",
                "unexplainable",
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>()
        );
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
            scoped_aliases: vec![],
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

    fn tokyo_ghoul_scoped_context() -> AnimeCandidateScoringContext {
        AnimeCandidateScoringContext {
            graph_fingerprint: Some("rr3-scoped-tokyo-ghoul".to_string()),
            aliases: vec![
                "Tokyo Ghoul".to_string(),
                "Tokyo Ghoul Root A".to_string(),
                "Tokyo Ghoul:re".to_string(),
            ],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Tokyo Ghoul".to_string(),
                    source: "anilist_season_title".to_string(),
                    language: None,
                    season_number: Some(1),
                    anilist_season_id: Some("1001".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul Root A".to_string(),
                    source: "anilist_season_title".to_string(),
                    language: None,
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul Season 2".to_string(),
                    source: "generated_season_ordinal".to_string(),
                    language: None,
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                },
            ],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E01".to_string(),
                    canonical_key: Some("anilist:1001:S01E01".to_string()),
                    title: "Tragedy".to_string(),
                    season_number: Some(1),
                    anilist_season_id: Some("1001".to_string()),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    tvdb_episode_id: Some("2001".to_string()),
                    anidb_episode_id: None,
                },
                AnimeCandidateTarget {
                    target_key: "S02E01".to_string(),
                    canonical_key: Some("anilist:1002:S02E01".to_string()),
                    title: "New Surge".to_string(),
                    season_number: Some(2),
                    anilist_season_id: Some("1002".to_string()),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    tvdb_episode_id: Some("2013".to_string()),
                    anidb_episode_id: None,
                },
            ],
        }
    }

    #[test]
    fn rr3_scoped_anilist_season_alias_maps_to_matching_season() {
        let score = score_anime_candidate(
            &tokyo_ghoul_scoped_context(),
            &rr3e_candidate("[SubsPlease] Tokyo Ghoul Root A - 01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(
            score
                .target_matches
                .first()
                .map(|target| target.target_key.as_str()),
            Some("S02E01")
        );
        assert!(
            score
                .target_matches
                .iter()
                .all(|target| target.target_key != "S01E01")
        );
        assert_eq!(
            score
                .alias_matches
                .first()
                .and_then(|alias| alias.season_number),
            Some(2)
        );
    }

    #[test]
    fn rr3_exact_scoped_alias_cannot_override_conflicting_sxxeyy_season() {
        let score = score_anime_candidate(
            &tokyo_ghoul_scoped_context(),
            &rr3e_candidate("[SubsPlease] Tokyo Ghoul Root A S03E01 [1080p]"),
        );

        assert_eq!(score.parsed.sonarr_facts.season_number, Some(3));
        assert_eq!(
            score
                .alias_matches
                .first()
                .and_then(|alias| alias.season_number),
            Some(2)
        );
        assert_eq!(
            score.alias_matches.first().map(|alias| alias.kind),
            Some(AnimeAliasMatchKind::Exact),
            "explicit scoped alias fixture must exercise contradiction handling: {:?}",
            score.alias_matches
        );
        assert!(
            score.target_matches.is_empty(),
            "a season-two alias must not turn explicit S03E01 into definitive coverage"
        );
        assert_eq!(
            score.reconciliation.outcome,
            AnimeReconciliationOutcome::TrueContradiction
        );
        assert!(
            score
                .reconciliation
                .contradiction_reasons
                .iter()
                .any(|reason| reason == "exact_scoped_alias_and_sxxeyy_season_disagree")
        );
        assert_eq!(score.outcome, AnimeMatchOutcome::Rejected);
        assert!(
            score
                .rejection_reasons
                .iter()
                .any(|reason| reason == "no_graph_target_coverage")
        );
    }

    #[test]
    fn rr3_exact_scoped_alias_accepts_agreeing_sxxeyy_season() {
        let score = score_anime_candidate(
            &tokyo_ghoul_scoped_context(),
            &rr3e_candidate("[SubsPlease] Tokyo Ghoul Root A S02E01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(
            score
                .target_matches
                .first()
                .map(|target| target.target_key.as_str()),
            Some("S02E01")
        );
        assert!(score.reconciliation.contradiction_reasons.is_empty());
    }

    #[test]
    fn alm9_exact_scoped_alias_cannot_fall_through_to_a_sibling_absolute_number() {
        let mut context = tokyo_ghoul_scoped_context();
        context.targets.push(AnimeCandidateTarget {
            target_key: "S01E10".to_string(),
            canonical_key: Some("anilist:1001:S01E10".to_string()),
            title: "Season One Episode Ten".to_string(),
            season_number: Some(1),
            anilist_season_id: Some("1001".to_string()),
            episode_number: Some(10),
            absolute_episode_number: Some(10),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        });

        let score = score_anime_candidate(
            &context,
            &rr3e_candidate("[BakedFish] Tokyo Ghoul Root A - 10 [1080p]"),
        );

        assert!(score.target_matches.is_empty());
        assert_eq!(score.outcome, AnimeMatchOutcome::Rejected);
        assert!(
            score
                .reconciliation
                .rejection_reasons
                .iter()
                .any(|reason| reason == "exact_scoped_alias_target_unmapped")
        );
    }

    #[test]
    fn alm9_season_dash_episode_prefers_the_matching_shared_season_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("alm9-shared-season-alias".to_string()),
            aliases: vec!["Shared Anime".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Shared Anime".to_string(),
                    source: "canonical_title".to_string(),
                    language: None,
                    season_number: Some(1),
                    anilist_season_id: Some("season-1".to_string()),
                },
                AnimeScopedAlias {
                    display: "Shared Anime".to_string(),
                    source: "canonical_title".to_string(),
                    language: None,
                    season_number: Some(2),
                    anilist_season_id: Some("season-2".to_string()),
                },
            ],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E12".to_string(),
                    canonical_key: None,
                    title: "Season One Finale".to_string(),
                    season_number: Some(1),
                    anilist_season_id: Some("season-1".to_string()),
                    episode_number: Some(12),
                    absolute_episode_number: Some(12),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                },
                AnimeCandidateTarget {
                    target_key: "S02E12".to_string(),
                    canonical_key: None,
                    title: "Season Two Finale".to_string(),
                    season_number: Some(2),
                    anilist_season_id: Some("season-2".to_string()),
                    episode_number: Some(12),
                    absolute_episode_number: Some(24),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                },
            ],
        };

        let score = score_anime_candidate(
            &context,
            &rr3e_candidate("[EMBER] Shared Anime S2 - 12 [1080p].mkv"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(
            score
                .target_matches
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E12"]
        );
        assert!(score.reconciliation.contradiction_reasons.is_empty());
    }

    #[test]
    fn alm9_semantic_evidence_enriches_existing_resolver_without_authoring_a_plan() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Root A - 01 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        let score = score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence)
            .expect("server-authored evidence must validate");
        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(score.confidence, ReleaseConfidence::High);
        assert_eq!(
            score
                .target_matches
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E01"]
        );
    }

    #[test]
    fn alm9_entity_only_translates_release_season_episode_with_server_offset() {
        let mut context = tokyo_ghoul_scoped_context();
        let target = context
            .targets
            .iter_mut()
            .find(|target| target.target_key == "S02E01")
            .unwrap();
        target.episode_number = Some(13);
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A S02E01 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 12,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        let score = score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence)
            .expect("server-owned offset should translate release-local episode 1 to canonical 13");
        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(score.target_matches[0].target_key, "S02E01");
        assert_eq!(score.target_matches[0].episode_number, Some(13));
    }

    #[test]
    fn alm9_semantic_parent_coordinate_can_be_proven_by_exact_provider_file() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: "file-1".to_string(),
            file_id: Some("1".to_string()),
            file_index: Some(1),
            path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
        }];

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none(),
            "the parent title alone must not invent an episode coordinate"
        );
        let plan = plan_anime_file_coverage_with_semantic_evidence(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
            &evidence,
        )
        .expect("the provider filename should corroborate the target");

        assert!(semantic_plan_is_definitive(&plan), "{plan:#?}");
        assert_eq!(plan.selected_file_keys, vec!["file-1"]);
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].target_key, "S02E01");
        assert_eq!(
            plan.entries[0].path.as_deref(),
            Some(files[0].path.as_str())
        );
        assert!(
            score_anime_candidate_with_verified_semantic_plan(
                &context, &candidate, &evidence, &plan,
            )
            .is_some()
        );
    }

    #[test]
    fn alm9_semantic_parent_coordinate_rejects_wrong_provider_file() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: "file-2".to_string(),
            file_id: Some("2".to_string()),
            file_index: Some(2),
            path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
        }];

        let plan = plan_anime_file_coverage_with_semantic_evidence(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
            &evidence,
        );
        assert!(plan.is_none_or(|plan| !semantic_plan_is_definitive(&plan)));
    }

    #[test]
    fn alm9_semantic_provider_files_cannot_override_parent_episode_conflict() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A - 02 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: "file-1".to_string(),
            file_id: Some("1".to_string()),
            file_index: Some(1),
            path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
        }];

        assert!(
            plan_anime_file_coverage_with_semantic_evidence(
                &context,
                &candidate,
                &files,
                AnimeCoverageOptions {
                    file_selection_supported: true,
                },
                &evidence,
            )
            .is_none_or(|plan| !semantic_plan_is_definitive(&plan))
        );
    }

    #[test]
    fn alm9_semantic_entity_only_pack_requires_exact_per_file_coverage() {
        let mut context = tokyo_ghoul_scoped_context();
        context.targets.push(AnimeCandidateTarget {
            target_key: "S02E02".to_string(),
            canonical_key: Some("anilist:1002:S02E02".to_string()),
            title: "Old Guard".to_string(),
            season_number: Some(2),
            anilist_season_id: Some("1002".to_string()),
            episode_number: Some(2),
            absolute_episode_number: Some(14),
            tvdb_episode_id: Some("2014".to_string()),
            anidb_episode_id: None,
        });
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A Complete [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::SeasonPack,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string(), "S02E02".to_string()],
        };
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "file-1".to_string(),
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                size_bytes: Some(1_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "file-2".to_string(),
                file_id: Some("2".to_string()),
                file_index: Some(2),
                path: "Tokyo Ghoul Root A - 02.mkv".to_string(),
                size_bytes: Some(1_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage_with_semantic_evidence(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
            &evidence,
        )
        .expect("every pack target has one independently parsed provider file");

        assert!(semantic_plan_is_definitive(&plan), "{plan:#?}");
        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.selected_file_keys, vec!["file-1", "file-2"]);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E01", "S02E02"]
        );
    }

    #[test]
    fn alm9_model_selected_coverage_rejects_explicit_entity_year_conflict() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-year-conflict".to_string()),
            aliases: vec![
                "Vampire Hunter D".to_string(),
                "Vampire Hunter D: Bloodlust".to_string(),
            ],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Vampire Hunter D (1985)".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("1985".to_string()),
                },
                AnimeScopedAlias {
                    display: "Vampire Hunter D: Bloodlust (2000)".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("bloodlust".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("bloodlust".to_string()),
            aliases: vec!["Vampire Hunter D: Bloodlust (2000)".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["movie:bloodlust".to_string()],
        };

        assert!(semantic_release_year_contradicts_selected_entity(
            &context,
            &parse_anime_release_title("[naiyas] Vampire Hunter D (1985) [BD1080p]"),
            &evidence,
        ));
        assert!(!semantic_release_year_contradicts_selected_entity(
            &context,
            &parse_anime_release_title("Vampire Hunter D: Bloodlust [1080p]"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_tv_episode_rejects_ova_boundary() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-tv-ova-conflict".to_string()),
            aliases: vec!["Love Stage!!".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Love Stage!!".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("tv".to_string()),
                },
                AnimeScopedAlias {
                    display: "Love Stage!! OVA".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("ova".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "S01E05".to_string(),
                canonical_key: None,
                title: "But I Do Like You".to_string(),
                season_number: Some(1),
                anilist_season_id: Some("tv".to_string()),
                episode_number: Some(5),
                absolute_episode_number: Some(5),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("tv".to_string()),
            aliases: vec!["Love Stage!!".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![5],
            absolute_episode_numbers: vec![5],
            target_keys: vec!["S01E05".to_string()],
        };

        assert!(semantic_special_boundary_contradicts_selected_target(
            &context,
            &parse_anime_release_title("[SS] Love Stage!! OVA"),
            &context.targets[0],
            &evidence,
        ));
        assert!(!semantic_special_boundary_contradicts_selected_target(
            &context,
            &parse_anime_release_title("[HorribleSubs] Love Stage!! - 05 [480p].mkv"),
            &context.targets[0],
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_special_rejects_generic_adjacent_entity_boundary() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-special-boundary".to_string()),
            aliases: vec!["Owari no Seraph".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Owari no Seraph".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("tv".to_string()),
                },
                AnimeScopedAlias {
                    display: "Owari no Seraph".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("specials".to_string()),
                },
                AnimeScopedAlias {
                    display: "Owaranai Seraph".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("specials".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "S00E09".to_string(),
                canonical_key: None,
                title: "Owaranai Seraph 09".to_string(),
                season_number: None,
                anilist_season_id: Some("specials".to_string()),
                episode_number: Some(9),
                absolute_episode_number: None,
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("specials".to_string()),
            aliases: vec!["Owari no Seraph".to_string(), "Owaranai Seraph".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Special,
            episode_numbers: vec![9],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S00E09".to_string()],
        };

        assert!(semantic_special_boundary_contradicts_selected_target(
            &context,
            &parse_anime_release_title("Owari no Seraph - OAD [DVD]"),
            &context.targets[0],
            &evidence,
        ));
        assert!(!semantic_special_boundary_contradicts_selected_target(
            &context,
            &parse_anime_release_title("Owari no Seraph Specials - Owaranai Seraph - 09 [1080p]"),
            &context.targets[0],
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_ova_keeps_entity_specific_boundary() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-entity-specific-ova".to_string()),
            aliases: vec!["Amefuri Kozou".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Amefuri Kozou".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("rain-boy".to_string()),
                },
                AnimeScopedAlias {
                    display: "Black Jack".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("adjacent".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "S00E01".to_string(),
                canonical_key: None,
                title: "Rain Boy".to_string(),
                season_number: None,
                anilist_season_id: Some("rain-boy".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("rain-boy".to_string()),
            aliases: vec!["Amefuri Kozou".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Ova,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S00E01".to_string()],
        };

        assert!(!semantic_special_boundary_contradicts_selected_target(
            &context,
            &parse_anime_release_title("Amefuri Kozou OVA [DVD]"),
            &context.targets[0],
            &evidence,
        ));
    }

    #[test]
    fn alm9_entity_only_selection_uses_unique_raw_coordinate_to_break_alias_tie() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-coordinate-tie".to_string()),
            aliases: vec!["Shared Franchise Arc".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Shared Franchise Arc".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("season-1".to_string()),
                },
                AnimeScopedAlias {
                    display: "Shared Franchise Arc".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("season-2".to_string()),
                },
            ],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E01".to_string(),
                    canonical_key: None,
                    title: "Season One".to_string(),
                    season_number: Some(1),
                    anilist_season_id: Some("season-1".to_string()),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                },
                AnimeCandidateTarget {
                    target_key: "S02E01".to_string(),
                    canonical_key: None,
                    title: "Season Two".to_string(),
                    season_number: Some(2),
                    anilist_season_id: Some("season-2".to_string()),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                },
            ],
        };
        let candidate = rr3e_candidate("Shared Franchise Arc S02E01 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("season-2".to_string()),
            aliases: vec!["Shared Franchise Arc".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: "file-1".to_string(),
            file_id: Some("1".to_string()),
            file_index: Some(1),
            path: "Shared Franchise Arc S02E01.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
        }];

        assert!(!semantic_identity_supported_by_release(
            &context,
            &parse_anime_release_title(&candidate.title),
            &evidence,
        ));
        let plan = plan_anime_file_coverage_with_semantic_evidence(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
            &evidence,
        )
        .expect("the server-owned S02E01 coordinate should break the exact alias tie");
        assert!(semantic_plan_is_definitive(&plan));
        assert_eq!(plan.entries[0].target_key, "S02E01");
    }

    #[test]
    fn alm9_movie_selection_breaks_only_an_exact_adjacent_alias_tie() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-movie-alias-tie".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Shared Movie Title".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("movie-1".to_string()),
                },
                AnimeScopedAlias {
                    display: "Shared Movie Title".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(2),
                    anilist_season_id: Some("movie-2".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title("[Group] Shared Movie Title [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("movie-1".to_string()),
            aliases: vec!["Shared Movie Title".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
        let mut episode_evidence = evidence;
        episode_evidence.media_kind = AnimeSemanticMediaKindEvidence::Episode;
        assert!(!semantic_identity_supported_by_release(
            &context,
            &parsed,
            &episode_evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_accepts_exact_joined_entity_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-exact-joined-alias".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Mahou Tsukai no Yakusoku".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("wizard".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("wizard".to_string()),
            aliases: vec!["Mahou Tsukai no Yakusoku".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Mahoutsukai no Yakusoku - 10"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_uses_all_raw_parser_title_candidates() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-full-title-evidence".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Highlander: The Search for Vengeance".to_string(),
                source: "anilist_english".to_string(),
                language: Some("en".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("highlander".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("highlander".to_string()),
            aliases: vec!["Highlander: The Search for Vengeance".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title(
                "[Tech-Mod] Highlander - The Search for Vengeance [Director's Cut].mkv",
            ),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_accepts_only_fully_owned_compound_titles() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-owned-compound-title".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Harmagedon".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("harmagedon".to_string()),
                },
                AnimeScopedAlias {
                    display: "Genma Taisen".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("harmagedon".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("harmagedon".to_string()),
            aliases: vec!["Harmagedon".to_string(), "Genma Taisen".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Harmagedon - Genma Taisen [BD 720p]"),
            &evidence,
        ));
        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Harmagedon - A Different Movie [BD 720p]"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_compound_identity_rejects_adjacent_special_subtitles() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-compound-adjacent-special".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Sora no Method".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("sora-tv".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("sora-tv".to_string()),
            aliases: vec!["Sora no Method".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title(
                "[Butter] Sora no Method - Mou Hitotsu no Negai [WEB 1080p AAC]",
            ),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_accepts_a_complete_named_alias_segment() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-named-alias-segment".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Senki Zesshou Symphogear G: Senki Zesshou Shinai Symphogear".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(2),
                anilist_season_id: Some("symphogear-g".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("symphogear-g".to_string()),
            aliases: vec![
                "Senki Zesshou Symphogear G: Senki Zesshou Shinai Symphogear".to_string(),
            ],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![2],
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Senki Zesshoushinai Symphogear - 02"),
            &evidence,
        ));
        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Senki Zesshou Symphogear GX - 11"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_movie_identity_rejects_plain_episode_suffixes() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-movie-episode-suffix".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Kaijuu Girls".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("kaijuu-movie".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("kaijuu-movie".to_string()),
            aliases: vec!["Kaijuu Girls".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("[HorribleSubs] Kaijuu Girls - 07 [480p].mkv"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_rejects_adjacent_owned_prefixes() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-adjacent-owned-prefix".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Baja no Studio: Baja no Mita Umi".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("baja-second".to_string()),
                },
                AnimeScopedAlias {
                    display: "Baja no Studio".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("baja-first".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("baja-second".to_string()),
            aliases: vec!["Baja no Studio: Baja no Mita Umi".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Special,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("[PAS] Baja no Studio [BD 1080p qAAC]"),
            &evidence,
        ));
        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("[PAS] Baja no Studio - v2 [BD 1080p AAC]"),
            &evidence,
        ));
        let mut incomplete_context = context;
        incomplete_context
            .scoped_aliases
            .retain(|alias| alias.anilist_season_id.as_deref() == Some("baja-second"));
        assert!(!model_selected_entity_has_exact_release_identity(
            &incomplete_context,
            &parse_anime_release_title("[PAS] Baja no Studio [BD 1080p qAAC]"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_accepts_substantial_alias_contractions() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-alias-contraction".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Jungle Emperor: The Symphonic Poem Film".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("jungle-special".to_string()),
                },
                AnimeScopedAlias {
                    display: "The Returnee Noble Lady Attacks His Majesty the Dragon Emperor"
                        .to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("adjacent".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("jungle-special".to_string()),
            aliases: vec!["Jungle Emperor: The Symphonic Poem Film".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Special,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("Jungle Emperor - Symphonic Poem [Blu-Flash]"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_batch_unique_prefix_recovers_shortened_special_without_weakening_strict_identity() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-batch-prefix".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Time Stranger Kyouko: Chocola ni Omakase!".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("time-special".to_string()),
                },
                // A duplicated graph alias prevents ordinary identity from
                // manufacturing uniqueness. The batch tier may still use the
                // shorter raw title because it is not itself an adjacent
                // entity's exact title.
                AnimeScopedAlias {
                    display: "Time Stranger Kyouko: Chocola ni Omakase!".to_string(),
                    source: "graph_neighbor".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("adjacent".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "special:time".to_string(),
                canonical_key: None,
                title: "Time Stranger Kyouko: Chocola ni Omakase!".to_string(),
                season_number: Some(0),
                anilist_season_id: Some("time-special".to_string()),
                episode_number: None,
                absolute_episode_number: Some(1),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("time-special".to_string()),
            aliases: vec!["Time Stranger Kyoko: Chocola ni Omakase!".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Special,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["special:time".to_string()],
        };
        let candidate = rr3e_candidate("[CF] Time Stranger Kyoko [VHS] [45724292].avi");

        assert!(
            plan_model_selected_single_target_coverage(&context, &candidate, &[], &evidence)
                .is_none()
        );
        assert!(
            plan_anime_file_coverage_with_batch_unique_semantic_evidence(
                &context,
                &candidate,
                &[],
                &evidence,
            )
            .is_some()
        );
    }

    #[test]
    fn alm9_batch_unique_prefix_rejects_an_exact_adjacent_entity_title() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-batch-prefix-adjacent".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Baja no Studio: Baja no Mita Umi".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("baja-second".to_string()),
                },
                AnimeScopedAlias {
                    display: "Baja no Studio".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("ja-Latn".to_string()),
                    season_number: Some(0),
                    anilist_season_id: Some("baja-first".to_string()),
                },
            ],
            targets: vec![AnimeCandidateTarget {
                target_key: "special:baja-second".to_string(),
                canonical_key: None,
                title: "Baja no Studio: Baja no Mita Umi".to_string(),
                season_number: Some(0),
                anilist_season_id: Some("baja-second".to_string()),
                episode_number: None,
                absolute_episode_number: Some(1),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("baja-second".to_string()),
            aliases: vec!["Baja no Studio: Baja no Mita Umi".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Special,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["special:baja-second".to_string()],
        };

        assert!(
            plan_anime_file_coverage_with_batch_unique_semantic_evidence(
                &context,
                &rr3e_candidate("[PAS] Baja no Studio [BD 1080p qAAC]"),
                &[],
                &evidence,
            )
            .is_none()
        );
    }

    #[test]
    fn alm9_canonical_identity_distinguishes_exact_title_from_substantive_extension() {
        assert_eq!(
            anime_semantic_canonical_identity(
                &rr3e_candidate("cencoroll.2009.dvdrip.x264.ac3.[daifuku]"),
                "Cencoroll",
            ),
            AnimeSemanticCanonicalIdentity::Exact
        );
        assert_eq!(
            anime_semantic_canonical_identity(
                &rr3e_candidate("Cencoroll.Connect.1080p.WEB.x264-WKN.mp4"),
                "Cencoroll",
            ),
            AnimeSemanticCanonicalIdentity::SubstantiveExtension
        );
    }

    #[test]
    fn alm9_model_selected_identity_ignores_the_parsed_trailing_release_group() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-trailing-release-group".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Golgo 13".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("golgo-movie".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("golgo-movie".to_string()),
            aliases: vec!["Golgo 13".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };
        let parsed = parse_anime_release_title("Golgo.13.1983.576p.BluRay.x264-HANDJOB.mkv");

        assert_eq!(parsed.release_group.as_deref(), Some("HANDJOB"));
        assert!(model_selected_entity_has_exact_release_identity(
            &context, &parsed, &evidence,
        ));
    }

    #[test]
    fn alm9_model_selected_identity_accepts_only_compatible_release_context() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-release-context".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Amefurikozou".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(0),
                anilist_season_id: Some("rain-boy".to_string()),
            }],
            targets: Vec::new(),
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 0,
            release_season_numbers: vec![0],
            episode_number_offset: 0,
            anilist_season_id: Some("rain-boy".to_string()),
            aliases: vec!["Amefurikozou".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Ova,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("[ARR] Amefuri Kozou OVA"),
            &evidence,
        ));
        let mut episode = evidence;
        episode.media_kind = AnimeSemanticMediaKindEvidence::Episode;
        assert!(!model_selected_entity_has_exact_release_identity(
            &context,
            &parse_anime_release_title("[ARR] Amefuri Kozou OVA"),
            &episode,
        ));
    }

    #[test]
    fn alm9_unique_coordinate_accepts_a_substantial_owned_title_prefix() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-coordinate-prefix".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Seisen Cerberus: Ryuukoku no Fatalite".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("cerberus".to_string()),
            }],
            targets: vec![AnimeCandidateTarget {
                target_key: "S01E13".to_string(),
                canonical_key: None,
                title: "Episode 13".to_string(),
                season_number: Some(1),
                anilist_season_id: Some("cerberus".to_string()),
                episode_number: Some(13),
                absolute_episode_number: Some(13),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("cerberus".to_string()),
            aliases: vec!["Seisen Cerberus: Ryuukoku no Fatalite".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![13],
            absolute_episode_numbers: vec![13],
            target_keys: vec!["S01E13".to_string()],
        };

        assert!(semantic_identity_corroborated_by_unique_coordinate(
            &context,
            &parse_anime_release_title("Seisen Cerberus - 13"),
            &evidence,
        ));
        assert!(!semantic_identity_corroborated_by_unique_coordinate(
            &context,
            &parse_anime_release_title("Seisen Cerberus - 12"),
            &evidence,
        ));
    }

    #[test]
    fn alm9_exact_model_identity_cannot_override_wrong_episode() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-exact-wrong-coordinate".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Kumamiko".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("ja-Latn".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("kuma".to_string()),
            }],
            targets: vec![AnimeCandidateTarget {
                target_key: "S01E09".to_string(),
                canonical_key: None,
                title: "Episode 9".to_string(),
                season_number: Some(1),
                anilist_season_id: Some("kuma".to_string()),
                episode_number: Some(9),
                absolute_episode_number: Some(9),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("kuma".to_string()),
            aliases: vec!["Kumamiko".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S01E09".to_string()],
        };

        assert!(
            plan_model_selected_single_target_coverage(
                &context,
                &rr3e_candidate("Kuma Miko - 08"),
                &[],
                &evidence,
            )
            .is_none()
        );
    }

    #[test]
    fn alm9_model_selected_movie_ignores_unstructured_numeric_parser_noise() {
        let target = AnimeCandidateTarget {
            target_key: "movie:palme".to_string(),
            canonical_key: None,
            title: "A Tree of Palme".to_string(),
            season_number: Some(1),
            anilist_season_id: Some("palme".to_string()),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            tvdb_episode_id: None,
            anidb_episode_id: None,
        };
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("palme".to_string()),
            aliases: vec!["A Tree of Palme".to_string(), "Palme no Ki".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["movie:palme".to_string()],
        };

        assert!(!semantic_release_coordinate_contradicts_target(
            &parse_anime_release_title("Palme no Ki (DVD 676x444 h264)"),
            &target,
            &evidence,
        ));
        assert!(semantic_release_coordinate_contradicts_target(
            &parse_anime_release_title("Palme no Ki S02E01"),
            &target,
            &evidence,
        ));
    }

    #[test]
    fn alm9_provider_basename_corroboration_requires_identity_and_exact_coordinate() {
        let context = tokyo_ghoul_scoped_context();
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(semantic_provider_file_corroborates_target(
            &context,
            &rr3e_candidate("Tokyo Ghoul Root A - 01.mkv"),
            &evidence,
            "S02E01",
        ));
        assert!(!semantic_provider_file_corroborates_target(
            &context,
            &rr3e_candidate("Tokyo Ghoul Root A - 02.mkv"),
            &evidence,
            "S02E01",
        ));
        assert!(!semantic_provider_file_corroborates_target(
            &context,
            &rr3e_candidate("Unrelated Show - 01.mkv"),
            &evidence,
            "S02E01",
        ));
    }

    #[test]
    fn alm9_semantic_unique_coordinate_cannot_override_adjacent_season() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul S01E01 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: vec![13],
            target_keys: vec!["S02E01".to_string()],
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: "file-1".to_string(),
            file_id: Some("1".to_string()),
            file_index: Some(1),
            path: "Tokyo Ghoul S01E01.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
        }];

        assert!(
            plan_anime_file_coverage_with_semantic_evidence(
                &context,
                &candidate,
                &files,
                AnimeCoverageOptions {
                    file_selection_supported: true,
                },
                &evidence,
            )
            .is_none_or(|plan| !semantic_plan_is_definitive(&plan))
        );
    }

    #[test]
    fn alm9_semantic_planner_retains_valid_identity_when_numbering_is_unsupported() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Root A - 01 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            // The selected identity is useful, but this secondary coordinate
            // is unsupported by the raw release and must not erase it.
            episode_numbers: vec![2],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        let plan = plan_anime_file_coverage_with_semantic_evidence(
            &context,
            &candidate,
            &[],
            AnimeCoverageOptions::default(),
            &evidence,
        )
        .expect("identity-only projection should still produce a plan");

        assert!(semantic_plan_is_definitive(&plan));
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].target_key, "S02E01");
    }

    #[test]
    fn alm9_semantic_evidence_cannot_create_its_own_raw_identity_match() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Josee to Tora to Sakana-tachi - 01");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none()
        );
    }

    #[test]
    fn alm9_semantic_franchise_root_does_not_explain_a_distinct_ova() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul - Pinto OVA [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("1001".to_string()),
            aliases: vec!["Tokyo Ghoul".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none()
        );
    }

    #[test]
    fn alm9_semantic_identity_accepts_entity_owned_ordinal_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-ordinal-alias".to_string()),
            aliases: vec!["Tokyo Ghoul".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Tokyo Ghoul:re".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(3),
                    anilist_season_id: Some("100240".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul:re 2".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(4),
                    anilist_season_id: Some("102351".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title(
            "[AU] Tokyo Ghoul Re 2nd Season - 04 [BS11 720p x264 AAC].mkv",
        );
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 4,
            release_season_numbers: vec![2, 4],
            episode_number_offset: 0,
            anilist_season_id: Some("102351".to_string()),
            aliases: vec!["Tokyo Ghoul:re 2".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![4],
            absolute_episode_numbers: vec![40],
            target_keys: Vec::new(),
        };

        assert!(semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_identity_accepts_abbreviated_owned_arc_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-arc-alias".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Danganronpa 3: The End of Hope's Peak High School - Future Arc"
                    .to_string(),
                source: "anilist_english".to_string(),
                language: Some("en".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("21509".to_string()),
            }],
            targets: Vec::new(),
        };
        let parsed =
            parse_anime_release_title("[HorribleSubs] Danganronpa 3 - Future Arc - 12 [1080p].mkv");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("21509".to_string()),
            aliases: vec![
                "Danganronpa 3: The End of Hope's Peak High School - Future Arc".to_string(),
            ],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![12],
            absolute_episode_numbers: vec![25],
            target_keys: Vec::new(),
        };

        assert!(semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_identity_accepts_slash_joined_owned_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-joined-alias".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "BELLE".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("127271".to_string()),
                },
                AnimeScopedAlias {
                    display: "Ryuu to Sobakasu no Hime".to_string(),
                    source: "anilist_romaji".to_string(),
                    language: Some("x-jat".to_string()),
                    season_number: Some(1),
                    anilist_season_id: Some("127271".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title(
            "Ryuu.to.Sobakasu.no.Hime/Belle.2021.1080p.BDRip.AAC5.1.10bits.x265-Rapta",
        );
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("127271".to_string()),
            aliases: vec!["BELLE".to_string(), "Ryuu to Sobakasu no Hime".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_identity_rejects_adjacent_exact_entity() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-adjacent-alias".to_string()),
            aliases: vec!["Tokyo Ghoul".to_string()],
            scoped_aliases: vec![
                AnimeScopedAlias {
                    display: "Tokyo Ghoul:re".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(3),
                    anilist_season_id: Some("100240".to_string()),
                },
                AnimeScopedAlias {
                    display: "Tokyo Ghoul:re 2".to_string(),
                    source: "anilist_english".to_string(),
                    language: Some("en".to_string()),
                    season_number: Some(4),
                    anilist_season_id: Some("102351".to_string()),
                },
            ],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title("[Group] Tokyo Ghoul:re - 04 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 4,
            release_season_numbers: vec![4],
            episode_number_offset: 0,
            anilist_season_id: Some("102351".to_string()),
            aliases: vec!["Tokyo Ghoul:re 2".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_identity_rejects_replaced_sequel_tokens() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-distinct-movie".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Seishun Buta Yarou wa Randoseru Girl no Yume wo Minai".to_string(),
                source: "anilist_romaji".to_string(),
                language: Some("x-jat".to_string()),
                season_number: Some(1),
                anilist_season_id: Some("161474".to_string()),
            }],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title(
            "Seishun Buta Yarou wa Odekake Sister no Yume wo Minai | \
             Rascal Does Not Dream of a Sister Venturing Out",
        );
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("161474".to_string()),
            aliases: vec!["Seishun Buta Yarou wa Randoseru Girl no Yume wo Minai".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Movie,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_identity_rejects_unordered_special_overlap() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("semantic-distinct-ova".to_string()),
            aliases: Vec::new(),
            scoped_aliases: vec![AnimeScopedAlias {
                display: "OVA Tokyo Ghoul".to_string(),
                source: "animap_title".to_string(),
                language: None,
                season_number: Some(1),
                anilist_season_id: Some("21132".to_string()),
            }],
            targets: Vec::new(),
        };
        let parsed = parse_anime_release_title("Tokyo Ghoul - Pinto OVA");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 1,
            release_season_numbers: vec![1],
            episode_number_offset: 0,
            anilist_season_id: Some("21132".to_string()),
            aliases: vec!["OVA Tokyo Ghoul".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Ova,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: Vec::new(),
        };

        assert!(!semantic_identity_supported_by_release(
            &context, &parsed, &evidence
        ));
    }

    #[test]
    fn alm9_semantic_numbering_cannot_replace_a_different_raw_episode() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A - 10 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none()
        );
    }

    #[test]
    fn alm9_semantic_entity_only_cannot_replace_a_different_raw_episode() {
        let mut context = tokyo_ghoul_scoped_context();
        context.targets.push(AnimeCandidateTarget {
            target_key: "S02E02".to_string(),
            canonical_key: Some("anilist:1002:S02E02".to_string()),
            title: "Old Guard".to_string(),
            season_number: Some(2),
            anilist_season_id: Some("1002".to_string()),
            episode_number: Some(2),
            absolute_episode_number: Some(14),
            tvdb_episode_id: Some("2014".to_string()),
            anidb_episode_id: None,
        });
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A - 02 [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none()
        );
    }

    #[test]
    fn alm9_semantic_entity_only_cannot_invent_a_missing_episode_coordinate() {
        let context = tokyo_ghoul_scoped_context();
        let candidate = rr3e_candidate("[Group] Tokyo Ghoul Root A [1080p]");
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string(), "Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::EntityOnly,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: Vec::new(),
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(&context, &candidate, &evidence).is_none()
        );
    }

    #[test]
    fn alm9_named_second_season_episode_is_not_promoted_to_a_pack() {
        let parsed = parse_anime_release_title(
            "[AU] Tokyo Ghoul Re 2nd Season - 04 [BS11 720p x264 AAC].mkv",
        );

        assert_eq!(parsed.absolute_episode_numbers, vec![4]);
        assert_eq!(
            anime_release_kind_for_coverage(&parsed),
            ReleaseKind::Single
        );
    }

    #[test]
    fn alm9_semantic_evidence_cannot_override_explicit_sxxeyy_contradiction() {
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec!["Tokyo Ghoul Root A".to_string()],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        assert!(
            score_anime_candidate_with_semantic_evidence(
                &tokyo_ghoul_scoped_context(),
                &rr3e_candidate("[Group] Tokyo Ghoul Root A S03E01 [1080p]"),
                &evidence,
            )
            .is_none()
        );
    }

    #[test]
    fn alm9_semantic_evidence_allows_entity_owned_provider_season_translation() {
        let evidence = AnimeSemanticCandidateEvidence {
            season_number: 2,
            release_season_numbers: vec![2, 3],
            episode_number_offset: 0,
            anilist_season_id: Some("1002".to_string()),
            aliases: vec![
                "Tokyo Ghoul Root A".to_string(),
                "Tokyo Ghoul Root A Season 3".to_string(),
            ],
            numbering: AnimeSemanticNumberingEvidence::Seasonal,
            media_kind: AnimeSemanticMediaKindEvidence::Episode,
            episode_numbers: vec![1],
            absolute_episode_numbers: Vec::new(),
            target_keys: vec!["S02E01".to_string()],
        };

        let score = score_anime_candidate_with_semantic_evidence(
            &tokyo_ghoul_scoped_context(),
            &rr3e_candidate("[Group] Tokyo Ghoul Root A S03E01 [1080p]"),
            &evidence,
        )
        .expect("entity-owned provider season should translate");
        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(score.target_matches[0].target_key, "S02E01");
    }

    #[test]
    fn rr3_generated_season_alias_maps_to_matching_season() {
        let score = score_anime_candidate(
            &tokyo_ghoul_scoped_context(),
            &rr3e_candidate("[SubsPlease] Tokyo Ghoul Season 2 - 01 [1080p]"),
        );

        assert_eq!(score.outcome, AnimeMatchOutcome::Planned);
        assert_eq!(
            score
                .target_matches
                .first()
                .map(|target| target.target_key.as_str()),
            Some("S02E01")
        );
        assert_eq!(
            score
                .alias_matches
                .first()
                .map(|alias| alias.source.as_str()),
            Some("generated_season_ordinal")
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
    fn alm9_single_release_binds_its_independently_parsed_media_file() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title - 01 [1080p]");
        let files = vec![AnimeReleaseFileInput {
            file_key: "episode-1".to_string(),
            file_id: Some("episode-1".to_string()),
            file_index: Some(0),
            path: "Provider Payload Alias - 01 [1080p].mkv".to_string(),
            size_bytes: Some(1_000_000),
            selectable: true,
        }];

        let plan = plan_anime_file_coverage(&context, &candidate, &files);

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["episode-1"]);
        assert_eq!(plan.entries[0].target_key, "S01E01");
        assert_eq!(
            plan.entries[0].release_file_key.as_deref(),
            Some("episode-1")
        );
        assert!(plan.review_reasons.is_empty());
    }

    #[test]
    fn alm9_multi_file_release_uses_unique_exact_scoped_file_alias() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("alm9-ova-file-binding".to_string()),
            aliases: vec!["Example".to_string()],
            scoped_aliases: vec![AnimeScopedAlias {
                display: "Example Jack".to_string(),
                source: "anilist_romaji".to_string(),
                language: None,
                season_number: Some(1),
                anilist_season_id: Some("ova-jack".to_string()),
            }],
            targets: vec![AnimeCandidateTarget {
                target_key: "OVA-JACK".to_string(),
                canonical_key: None,
                title: "Example Jack".to_string(),
                season_number: Some(1),
                anilist_season_id: Some("ova-jack".to_string()),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let candidate = rr3e_candidate("[Group] Example Jack - 01 [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "jack".to_string(),
                file_id: Some("jack".to_string()),
                file_index: Some(0),
                path: "Example Jack.mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "pinto".to_string(),
                file_id: Some("pinto".to_string()),
                file_index: Some(1),
                path: "Example Pinto.mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage_with_options(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["jack"]);
        assert_eq!(plan.entries[0].release_file_key.as_deref(), Some("jack"));
        assert!(plan.review_reasons.is_empty());
    }

    #[test]
    fn alm9_bonus_directories_are_not_media_payloads() {
        assert!(is_anime_sample_or_extra_file(
            "bonus goodies/related video clips/promo.mp4"
        ));
        assert!(is_anime_sample_or_extra_file(
            "Release/Featurettes/interview.mkv"
        ));
        assert!(!is_anime_sample_or_extra_file(
            "Release/Example Anime - 01.mkv"
        ));
    }

    #[test]
    fn alm9_multi_episode_release_binds_each_unique_media_basename() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title - 01-02 [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "episode-1".to_string(),
                file_id: Some("episode-1".to_string()),
                file_index: Some(0),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "episode-2".to_string(),
                file_id: Some("episode-2".to_string()),
                file_index: Some(1),
                path: "Example Title - 02 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage(&context, &candidate, &files);

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["episode-1", "episode-2"]);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| (
                    entry.target_key.as_str(),
                    entry.release_file_key.as_deref().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![("S01E01", "episode-1"), ("S01E02", "episode-2")]
        );
        assert!(plan.review_reasons.is_empty());
    }

    #[test]
    fn alm9_non_pack_duplicate_file_matches_require_review() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title - 01 [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "episode-1-a".to_string(),
                file_id: Some("episode-1-a".to_string()),
                file_index: Some(0),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "episode-1-b".to_string(),
                file_id: Some("episode-1-b".to_string()),
                file_index: Some(1),
                path: "Example Title - 01 v2 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage(&context, &candidate, &files);

        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert!(plan.selected_file_keys.is_empty());
        assert!(plan.entries[0].release_file_key.is_none());
        assert!(
            plan.review_reasons
                .iter()
                .any(|reason| reason == "file_list_does_not_cover_expected_targets")
        );
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

    #[test]
    fn rr3r_scoped_pack_overfetch_with_safe_file_selection_selects_only_wanted_targets() {
        let mut context = rr3e_scoring_context();
        context.targets.truncate(1);
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

        let plan = plan_anime_file_coverage_with_options(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );

        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["1"]);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01"]
        );
        assert!(plan.requires_file_selection);
        assert!(plan.review_reasons.is_empty());
        assert!(plan.rejection_reasons.is_empty());
    }

    #[test]
    fn rr3r_scoped_pack_overfetch_without_safe_file_selection_requires_review() {
        let mut context = rr3e_scoring_context();
        context.targets.truncate(1);
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
                file_id: None,
                file_index: Some(2),
                path: "Example Title - 02 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage_with_options(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );

        assert_eq!(plan.confidence, ReleaseConfidence::ReviewRequired);
        assert_eq!(plan.selected_file_keys, vec!["1"]);
        assert!(
            plan.review_reasons
                .iter()
                .any(|reason| reason == "pack_overfetch_without_safe_file_selection")
        );
    }

    #[test]
    fn rr3r_pack_file_list_preserves_exact_file_episode_set() {
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
                file_key: "3".to_string(),
                file_id: Some("3".to_string()),
                file_index: Some(3),
                path: "Example Title - 03 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage_with_options(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01"]
        );
        assert!(
            plan.review_reasons
                .iter()
                .any(|reason| reason == "file_list_does_not_cover_expected_targets")
        );
    }

    #[test]
    fn rr3r_absolute_only_arc_scope_uses_requested_graph_targets() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("rr3r-absolute-arc".to_string()),
            aliases: vec!["Long Running Anime".to_string()],
            scoped_aliases: vec![],
            targets: vec![AnimeCandidateTarget {
                target_key: "A0009".to_string(),
                canonical_key: Some("anilist:21:A0009".to_string()),
                title: "Long Running Anime A0009".to_string(),
                season_number: None,
                anilist_season_id: None,
                episode_number: None,
                absolute_episode_number: Some(9),
                tvdb_episode_id: None,
                anidb_episode_id: None,
            }],
        };
        let candidate = rr3e_candidate("[SubsPlease] Long Running Anime S01 Batch [1080p]");
        let files = vec![
            AnimeReleaseFileInput {
                file_key: "9".to_string(),
                file_id: Some("9".to_string()),
                file_index: Some(9),
                path: "Long Running Anime - 0009 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
            AnimeReleaseFileInput {
                file_key: "10".to_string(),
                file_id: Some("10".to_string()),
                file_index: Some(10),
                path: "Long Running Anime - 0010 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: true,
            },
        ];

        let plan = plan_anime_file_coverage_with_options(
            &context,
            &candidate,
            &files,
            AnimeCoverageOptions {
                file_selection_supported: true,
            },
        );

        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.selected_file_keys, vec!["9"]);
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["A0009"]
        );
    }

    #[test]
    fn rr3s_parser_provenance_records_sonarr_graph_and_coverage() {
        let context = rr3e_scoring_context();
        let candidate = rr3e_candidate("[SubsPlease] Example Title S01E01 [1080p][ABCDEF12]");
        let score = score_anime_candidate(&context, &candidate);
        let plan = plan_anime_file_coverage(&context, &candidate, &[]);

        let provenance = anime_parser_provenance(&context, &score, Some(&plan));

        assert_eq!(
            provenance.schema_version,
            ANIME_PARSER_PROVENANCE_SCHEMA_VERSION
        );
        assert_eq!(
            provenance.sonarr.matched_pattern_id_source,
            "rr2_public_parser_does_not_expose_regex_id"
        );
        assert_eq!(
            provenance.sonarr.parsed_title.as_deref(),
            Some("Example Title")
        );
        assert_eq!(provenance.sonarr.season_number, Some(1));
        assert_eq!(provenance.sonarr.episode_numbers, vec![1]);
        assert_eq!(provenance.sonarr.release_hash.as_deref(), Some("ABCDEF12"));
        assert_eq!(
            provenance.parsed.release_group.as_deref(),
            Some("SubsPlease")
        );
        assert_eq!(provenance.parsed.release_hash.as_deref(), Some("ABCDEF12"));
        assert_eq!(
            provenance.graph.graph_fingerprint.as_deref(),
            Some("rr3e-test-graph")
        );
        assert_eq!(provenance.graph.alias_count, 3);
        assert_eq!(provenance.graph.target_count, 2);
        assert_eq!(
            provenance.reconciliation.outcome,
            AnimeReconciliationOutcome::Agreement
        );
        assert_eq!(
            provenance
                .graph
                .target_matches
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01"]
        );
        let coverage = provenance.coverage.expect("coverage provenance");
        assert_eq!(coverage.entry_count, 1);
        assert_eq!(coverage.covered_target_keys, vec!["S01E01"]);
        assert_eq!(coverage.confidence, ReleaseConfidence::High);

        let diagnostics = anime_parser_diagnostics(&context, &score, Some(&plan));
        assert_eq!(
            diagnostics
                .pointer("/parserProvenance/sonarr/episodeNumbers/0")
                .and_then(JsonValue::as_i64),
            Some(1)
        );
        assert_eq!(
            diagnostics
                .pointer("/parserProvenance/graph/targetMatches/0/targetKey")
                .and_then(JsonValue::as_str),
            Some("S01E01")
        );
    }
}
