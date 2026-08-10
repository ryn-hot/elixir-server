use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::{db::models::MediaType, extensions::store::ExtensionStore};

pub const LANGUAGE_PREFERENCE_SETTING_KEY: &str = "acquisition.language_preference";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LanguagePreferenceMode {
    Off,
    Prefer,
    RequireReview,
}

impl Default for LanguagePreferenceMode {
    fn default() -> Self {
        Self::Off
    }
}

impl LanguagePreferenceMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Prefer => "prefer",
            Self::RequireReview => "require_review",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnknownLanguagePolicy {
    Allow,
    AllowLowerPriority,
    RequireReview,
}

impl Default for UnknownLanguagePolicy {
    fn default() -> Self {
        Self::AllowLowerPriority
    }
}

impl UnknownLanguagePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::AllowLowerPriority => "allow_lower_priority",
            Self::RequireReview => "require_review",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LanguagePreferenceMediaRule {
    #[serde(default)]
    pub audio: Vec<String>,
    #[serde(default)]
    pub subtitles: Vec<String>,
    #[serde(default)]
    pub profiles: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionLanguagePreference {
    #[serde(default)]
    pub mode: LanguagePreferenceMode,
    #[serde(default)]
    pub movie: LanguagePreferenceMediaRule,
    #[serde(default)]
    pub tv: LanguagePreferenceMediaRule,
    #[serde(default)]
    pub anime: LanguagePreferenceMediaRule,
    #[serde(default)]
    pub unknown_language: UnknownLanguagePolicy,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeAudioPreferenceMode {
    Any,
    PreferDub,
    RequireDubReview,
}

impl Default for AnimeAudioPreferenceMode {
    fn default() -> Self {
        Self::Any
    }
}

impl AnimeAudioPreferenceMode {
    pub fn active(self) -> bool {
        !matches!(self, Self::Any)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeAudioPreference {
    #[serde(default)]
    pub mode: AnimeAudioPreferenceMode,
    #[serde(default)]
    pub language: Option<String>,
}

impl Default for AnimeAudioPreference {
    fn default() -> Self {
        Self {
            mode: AnimeAudioPreferenceMode::Any,
            language: None,
        }
    }
}

impl AnimeAudioPreference {
    pub fn normalized(&self) -> Self {
        if !self.mode.active() {
            return Self::default();
        }
        Self {
            mode: self.mode,
            language: Some(
                self.language
                    .as_deref()
                    .and_then(normalize_language_value)
                    .unwrap_or_else(|| "en".to_string()),
            ),
        }
    }

    pub fn active_for_media_type(&self, media_type: MediaType) -> bool {
        media_type == MediaType::Anime && self.normalized().mode.active()
    }

    pub fn provider_language_hints(&self, media_type: MediaType) -> Vec<String> {
        if !self.active_for_media_type(media_type) {
            return Vec::new();
        }
        self.normalized().language.into_iter().collect()
    }

    pub fn to_language_preference(
        &self,
        media_type: MediaType,
    ) -> Option<AcquisitionLanguagePreference> {
        if !self.active_for_media_type(media_type) {
            return None;
        }
        let normalized = self.normalized();
        let mut preference = AcquisitionLanguagePreference {
            mode: match normalized.mode {
                AnimeAudioPreferenceMode::Any => LanguagePreferenceMode::Off,
                AnimeAudioPreferenceMode::PreferDub => LanguagePreferenceMode::Prefer,
                AnimeAudioPreferenceMode::RequireDubReview => LanguagePreferenceMode::RequireReview,
            },
            anime: LanguagePreferenceMediaRule {
                profiles: vec![
                    "en_audio".to_string(),
                    "dual_audio".to_string(),
                    "dubbed".to_string(),
                ],
                ..LanguagePreferenceMediaRule::default()
            },
            unknown_language: UnknownLanguagePolicy::AllowLowerPriority,
            ..AcquisitionLanguagePreference::default()
        };
        if normalized.mode == AnimeAudioPreferenceMode::RequireDubReview {
            preference.anime.audio = normalized.language.into_iter().collect();
            preference.unknown_language = UnknownLanguagePolicy::RequireReview;
        }
        Some(preference.normalized())
    }
}

impl Default for AcquisitionLanguagePreference {
    fn default() -> Self {
        Self {
            mode: LanguagePreferenceMode::Off,
            movie: LanguagePreferenceMediaRule {
                audio: vec!["en".to_string()],
                ..LanguagePreferenceMediaRule::default()
            },
            tv: LanguagePreferenceMediaRule {
                audio: vec!["en".to_string()],
                ..LanguagePreferenceMediaRule::default()
            },
            anime: LanguagePreferenceMediaRule {
                profiles: vec![
                    "ja_audio_en_subs".to_string(),
                    "dual_audio".to_string(),
                    "en_audio".to_string(),
                ],
                ..LanguagePreferenceMediaRule::default()
            },
            unknown_language: UnknownLanguagePolicy::AllowLowerPriority,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidateLanguageEvidence {
    pub audio: BTreeSet<String>,
    pub subtitles: BTreeSet<String>,
    pub profiles: BTreeSet<String>,
    pub raw: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LanguagePreferenceAssessment {
    pub score_delta: f64,
    pub state: LanguagePreferenceAssessmentState,
    pub matching_audio: Vec<String>,
    pub matching_subtitles: Vec<String>,
    pub matching_profiles: Vec<String>,
    pub desired_audio: Vec<String>,
    pub desired_subtitles: Vec<String>,
    pub desired_profiles: Vec<String>,
    pub evidence_audio: Vec<String>,
    pub evidence_subtitles: Vec<String>,
    pub evidence_profiles: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanguagePreferenceAssessmentState {
    Off,
    Match,
    Mismatch,
    Unknown,
}

impl LanguagePreferenceAssessmentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Match => "match",
            Self::Mismatch => "mismatch",
            Self::Unknown => "unknown",
        }
    }
}

impl AcquisitionLanguagePreference {
    pub fn normalized(mut self) -> Self {
        self.movie = normalize_rule(self.movie);
        self.tv = normalize_rule(self.tv);
        self.anime = normalize_rule(self.anime);
        self
    }

    pub fn active(&self) -> bool {
        self.mode != LanguagePreferenceMode::Off
    }

    pub fn rule_for_media_type(&self, media_type: MediaType) -> LanguagePreferenceMediaRule {
        match media_type {
            MediaType::Movie => self.movie.clone(),
            MediaType::Series => self.tv.clone(),
            MediaType::Anime => self.anime.clone(),
        }
    }

    pub fn provider_language_hints(&self, media_type: MediaType) -> Vec<String> {
        if !self.active() {
            return Vec::new();
        }
        let rule = self.rule_for_media_type(media_type);
        let mut out = BTreeSet::new();
        for language in rule.audio.iter().chain(rule.subtitles.iter()) {
            if let Some(normalized) = normalize_language_value(language) {
                out.insert(normalized);
            }
        }
        for profile in &rule.profiles {
            match normalize_language_profile(profile).as_deref() {
                Some("ja_audio_en_subs") => {
                    out.insert("ja".to_string());
                    out.insert("en".to_string());
                }
                Some("dual_audio") => {
                    out.insert("ja".to_string());
                    out.insert("en".to_string());
                }
                Some("en_audio") => {
                    out.insert("en".to_string());
                }
                _ => {}
            }
        }
        out.into_iter().collect()
    }
}

pub fn language_preference_from_json(
    value: Option<&JsonValue>,
) -> Option<AcquisitionLanguagePreference> {
    serde_json::from_value::<AcquisitionLanguagePreference>(value?.clone())
        .ok()
        .map(AcquisitionLanguagePreference::normalized)
}

pub async fn load_saved_language_preference(
    store: &ExtensionStore<'_>,
) -> anyhow::Result<AcquisitionLanguagePreference> {
    Ok(store
        .get_extension_setting(LANGUAGE_PREFERENCE_SETTING_KEY)
        .await?
        .as_ref()
        .and_then(|value| language_preference_from_json(Some(value)))
        .unwrap_or_default()
        .normalized())
}

pub async fn save_language_preference(
    store: &ExtensionStore<'_>,
    preference: &AcquisitionLanguagePreference,
) -> anyhow::Result<()> {
    let normalized = preference.clone().normalized();
    if normalized == AcquisitionLanguagePreference::default() {
        store
            .delete_extension_setting(LANGUAGE_PREFERENCE_SETTING_KEY)
            .await?;
    } else {
        let value = language_preference_to_value(&normalized);
        store
            .upsert_extension_setting(LANGUAGE_PREFERENCE_SETTING_KEY, &value)
            .await?;
    }
    Ok(())
}

pub fn language_preference_from_quality_profile(
    profile: Option<&JsonValue>,
    media_type: MediaType,
) -> AcquisitionLanguagePreference {
    if let Some(preference) = profile
        .and_then(|value| value.get("languagePreference"))
        .or_else(|| profile.and_then(|value| value.get("language_preference")))
        .and_then(|value| language_preference_from_json(Some(value)))
    {
        return preference;
    }

    let required = json_language_values_from_paths(
        profile,
        &[
            &["requiredLanguages"][..],
            &["required_languages"][..],
            &["audioLanguages"][..],
            &["audio_languages"][..],
            &["audio", "requiredLanguages"][..],
            &["audio", "required_languages"][..],
        ],
    );
    if required.is_empty() {
        return AcquisitionLanguagePreference::default();
    }

    let mut preference = AcquisitionLanguagePreference {
        mode: LanguagePreferenceMode::Prefer,
        ..AcquisitionLanguagePreference::default()
    };
    let normalized = normalize_language_list(required);
    match media_type {
        MediaType::Movie => preference.movie.audio = normalized,
        MediaType::Series => preference.tv.audio = normalized,
        MediaType::Anime => preference.anime.audio = normalized,
    }
    preference.normalized()
}

pub fn quality_profile_with_language_preference(
    profile: Option<JsonValue>,
    media_type: MediaType,
    preference: &AcquisitionLanguagePreference,
) -> Option<JsonValue> {
    if !preference.active() {
        return profile;
    }
    let mut object = profile
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    if let Some(existing) = object
        .get("languagePreference")
        .or_else(|| object.get("language_preference"))
        .and_then(|value| language_preference_from_json(Some(value)))
    {
        object.insert("languagePreference".to_string(), json!(existing));
        return Some(JsonValue::Object(object));
    }
    let normalized = preference.clone().normalized();
    object.insert("languagePreference".to_string(), json!(normalized));

    let existing_required = json_language_values_from_paths(
        Some(&JsonValue::Object(object.clone())),
        &[
            &["requiredLanguages"][..],
            &["required_languages"][..],
            &["audioLanguages"][..],
            &["audio_languages"][..],
        ],
    );
    if existing_required.is_empty() {
        let hints = normalized.provider_language_hints(media_type);
        if !hints.is_empty() {
            object.insert("requiredLanguages".to_string(), json!(hints));
        }
    }
    Some(JsonValue::Object(object))
}

pub fn quality_profile_with_anime_audio_preference(
    profile: Option<JsonValue>,
    media_type: MediaType,
    preference: Option<&AnimeAudioPreference>,
) -> Option<JsonValue> {
    let Some(preference) = preference else {
        return profile;
    };
    if !preference.active_for_media_type(media_type) {
        return profile;
    }
    let normalized = preference.normalized();
    let Some(language_preference) = normalized.to_language_preference(media_type) else {
        return profile;
    };
    let mut object = profile
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    object.insert("animeAudioPreference".to_string(), json!(normalized));
    object.insert("languagePreference".to_string(), json!(language_preference));

    let provider_hints = preference.provider_language_hints(media_type);
    if !provider_hints.is_empty() {
        object.insert("providerLanguageHints".to_string(), json!(provider_hints));
    }

    if normalized.mode == AnimeAudioPreferenceMode::RequireDubReview {
        let required = preference.provider_language_hints(media_type);
        if !required.is_empty() {
            object.insert("requiredAudioLanguages".to_string(), json!(required));
        }
    }

    Some(JsonValue::Object(object))
}

pub fn add_language_evidence_value(evidence: &mut CandidateLanguageEvidence, value: &JsonValue) {
    for raw in json_language_values(value) {
        add_language_evidence_text(evidence, &raw);
    }
}

pub fn add_language_evidence_text(evidence: &mut CandidateLanguageEvidence, raw: &str) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }
    evidence.raw.insert(trimmed.to_string());
    if let Some(profile) = normalize_language_profile(trimmed) {
        evidence.profiles.insert(profile);
    }
    if let Some(language) = normalize_language_value(trimmed) {
        evidence.audio.insert(language);
        return;
    }
    let signal_tokens = language_signal_tokens(trimmed);
    if signal_tokens.windows(2).any(|tokens| {
        matches!(tokens[0].to_ascii_lowercase().as_str(), "dual" | "multi")
            && tokens[1].eq_ignore_ascii_case("audio")
    }) {
        evidence.profiles.insert("dual_audio".to_string());
    }
    for token in signal_tokens {
        if let Some(profile) = normalize_language_profile(&token) {
            evidence.profiles.insert(profile);
        }
        if let Some(language) = normalize_language_value(&token) {
            evidence.audio.insert(language);
        }
    }
}

pub fn add_subtitle_language_evidence_value(
    evidence: &mut CandidateLanguageEvidence,
    value: &JsonValue,
) {
    for raw in json_language_values(value) {
        add_subtitle_language_evidence_text(evidence, &raw);
    }
}

pub fn add_subtitle_language_evidence_text(evidence: &mut CandidateLanguageEvidence, raw: &str) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }
    evidence.raw.insert(trimmed.to_string());
    if let Some(language) = normalize_language_value(trimmed) {
        evidence.subtitles.insert(language);
        return;
    }
    let tokens = language_signal_tokens(trimmed);
    if tokens
        .windows(2)
        .any(|pair| pair[0].eq_ignore_ascii_case("multi") && pair[1].eq_ignore_ascii_case("audio"))
    {
        evidence.profiles.insert("dual_audio".to_string());
    }
    for token in tokens {
        if let Some(language) = normalize_language_value(&token) {
            evidence.subtitles.insert(language);
        }
    }
}

pub fn assess_language_preference(
    preference: &AcquisitionLanguagePreference,
    media_type: MediaType,
    evidence: &CandidateLanguageEvidence,
) -> LanguagePreferenceAssessment {
    if !preference.active() {
        return assessment(
            0.0,
            LanguagePreferenceAssessmentState::Off,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            evidence,
        );
    }

    let rule = normalize_rule(preference.rule_for_media_type(media_type));
    let desired_audio = rule.audio;
    let desired_subtitles = rule.subtitles;
    let desired_profiles = rule.profiles;

    if desired_audio.is_empty() && desired_subtitles.is_empty() && desired_profiles.is_empty() {
        return assessment(
            0.0,
            LanguagePreferenceAssessmentState::Unknown,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            desired_audio,
            desired_subtitles,
            desired_profiles,
            evidence,
        );
    }

    let matching_audio = intersect_sorted(&evidence.audio, &desired_audio);
    let matching_subtitles = intersect_sorted(&evidence.subtitles, &desired_subtitles);
    let matching_profiles = desired_profiles
        .iter()
        .filter(|profile| anime_profile_matches(profile, evidence))
        .cloned()
        .collect::<Vec<_>>();

    let has_match = !matching_audio.is_empty()
        || !matching_subtitles.is_empty()
        || !matching_profiles.is_empty();
    let has_explicit_audio = !evidence.audio.is_empty();
    let has_explicit_subtitle = !evidence.subtitles.is_empty();
    let has_explicit_profile = !evidence.profiles.is_empty();
    let has_any_evidence = has_explicit_audio || has_explicit_subtitle || has_explicit_profile;

    let desired_audio_mismatch =
        !desired_audio.is_empty() && has_explicit_audio && matching_audio.is_empty();
    let desired_subtitle_mismatch =
        !desired_subtitles.is_empty() && has_explicit_subtitle && matching_subtitles.is_empty();
    let profile_mismatch =
        !desired_profiles.is_empty() && has_any_evidence && matching_profiles.is_empty();

    let state = if has_match {
        LanguagePreferenceAssessmentState::Match
    } else if desired_audio_mismatch || desired_subtitle_mismatch || profile_mismatch {
        LanguagePreferenceAssessmentState::Mismatch
    } else {
        LanguagePreferenceAssessmentState::Unknown
    };

    let mut score_delta: f64 = match state {
        LanguagePreferenceAssessmentState::Match => 0.08,
        LanguagePreferenceAssessmentState::Mismatch => -0.06,
        LanguagePreferenceAssessmentState::Unknown => match preference.unknown_language {
            UnknownLanguagePolicy::Allow => 0.0,
            UnknownLanguagePolicy::AllowLowerPriority | UnknownLanguagePolicy::RequireReview => {
                -0.01
            }
        },
        LanguagePreferenceAssessmentState::Off => 0.0,
    };
    if !matching_profiles.is_empty() {
        score_delta += 0.04;
    }
    if !matching_audio.is_empty() && !matching_subtitles.is_empty() {
        score_delta += 0.02;
    }
    score_delta = score_delta.clamp(-0.08, 0.14);

    assessment(
        score_delta,
        state,
        matching_audio,
        matching_subtitles,
        matching_profiles,
        desired_audio,
        desired_subtitles,
        desired_profiles,
        evidence,
    )
}

pub fn normalize_language_value(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.replace('_', "-");
    let first = normalized
        .split('-')
        .find(|part| !part.trim().is_empty())?
        .trim()
        .to_ascii_lowercase();
    normalize_language_token(&first)
}

pub fn normalize_language_profile(raw: &str) -> Option<String> {
    let token = raw
        .trim()
        .to_ascii_lowercase()
        .replace(['-', '.', ' '], "_");
    match token.as_str() {
        "dual" | "dual_audio" | "dual_audio_audio" | "multi_audio" | "multiaudio" => {
            Some("dual_audio".to_string())
        }
        "sub" | "subs" | "subbed" | "subtitle" | "subtitles" => Some("subbed".to_string()),
        "dub" | "dubbed" | "dub_audio" => Some("dubbed".to_string()),
        "ja_audio_en_subs" | "japanese_audio_english_subs" | "jpn_audio_eng_subs" => {
            Some("ja_audio_en_subs".to_string())
        }
        "en_audio" | "english_audio" | "eng_audio" => Some("en_audio".to_string()),
        value if value.ends_with("_dub") => Some("dubbed".to_string()),
        value if value.ends_with("_sub") || value.ends_with("_subs") => Some("subbed".to_string()),
        _ => None,
    }
}

pub fn json_language_values(value: &JsonValue) -> Vec<String> {
    if let Some(raw) = value.as_str() {
        return split_language_list(raw);
    }
    if let Some(values) = value.as_array() {
        return values.iter().flat_map(json_language_values).collect();
    }
    if let Some(object) = value.as_object() {
        return ["language", "lang", "name", "value", "code", "id"]
            .iter()
            .filter_map(|key| object.get(*key))
            .flat_map(json_language_values)
            .collect();
    }
    Vec::new()
}

fn assessment(
    score_delta: f64,
    state: LanguagePreferenceAssessmentState,
    matching_audio: Vec<String>,
    matching_subtitles: Vec<String>,
    matching_profiles: Vec<String>,
    desired_audio: Vec<String>,
    desired_subtitles: Vec<String>,
    desired_profiles: Vec<String>,
    evidence: &CandidateLanguageEvidence,
) -> LanguagePreferenceAssessment {
    LanguagePreferenceAssessment {
        score_delta,
        state,
        matching_audio,
        matching_subtitles,
        matching_profiles,
        desired_audio,
        desired_subtitles,
        desired_profiles,
        evidence_audio: evidence.audio.iter().cloned().collect(),
        evidence_subtitles: evidence.subtitles.iter().cloned().collect(),
        evidence_profiles: evidence.profiles.iter().cloned().collect(),
    }
}

fn normalize_rule(rule: LanguagePreferenceMediaRule) -> LanguagePreferenceMediaRule {
    LanguagePreferenceMediaRule {
        audio: normalize_language_list(rule.audio),
        subtitles: normalize_language_list(rule.subtitles),
        profiles: normalize_profile_list(rule.profiles),
    }
}

fn normalize_language_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| normalize_language_value(&value))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalize_profile_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| normalize_language_profile(&value).or_else(|| Some(value)))
        .map(|value| {
            value
                .trim()
                .to_ascii_lowercase()
                .replace(['-', '.', ' '], "_")
        })
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn json_language_values_from_paths(root: Option<&JsonValue>, paths: &[&[&str]]) -> Vec<String> {
    let mut out = Vec::new();
    for path in paths {
        if let Some(value) = json_value_path(root, path) {
            out.extend(json_language_values(value));
        }
    }
    out
}

fn json_value_path<'a>(root: Option<&'a JsonValue>, path: &[&str]) -> Option<&'a JsonValue> {
    let mut cursor = root?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    Some(cursor)
}

fn split_language_list(raw: &str) -> Vec<String> {
    raw.split(|ch: char| matches!(ch, ',' | ';' | '/' | '|'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn language_signal_tokens(raw: &str) -> Vec<String> {
    raw.split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn normalize_language_token(token: &str) -> Option<String> {
    let token = token.trim().to_ascii_lowercase();
    if matches!(
        token.as_str(),
        "" | "und"
            | "undefined"
            | "unknown"
            | "unk"
            | "mul"
            | "multi"
            | "dual"
            | "original"
            | "sub"
            | "subs"
            | "subtitle"
            | "subbed"
            | "dub"
            | "dubbed"
    ) {
        return None;
    }
    if token.len() == 2 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return Some(token);
    }
    if token.len() == 3 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return map_three_letter_language(&token).map(ToString::to_string);
    }
    map_language_name(&token).map(ToString::to_string)
}

fn map_three_letter_language(token: &str) -> Option<&'static str> {
    match token {
        "eng" => Some("en"),
        "jpn" | "jpa" => Some("ja"),
        "jap" => Some("ja"),
        "rus" => Some("ru"),
        "spa" | "esp" => Some("es"),
        "fre" | "fra" => Some("fr"),
        "ger" | "deu" => Some("de"),
        "ita" => Some("it"),
        "por" => Some("pt"),
        "chi" | "zho" => Some("zh"),
        "kor" => Some("ko"),
        "hin" => Some("hi"),
        "ara" => Some("ar"),
        _ => None,
    }
}

fn map_language_name(token: &str) -> Option<&'static str> {
    match token {
        "english" => Some("en"),
        "japanese" | "nihongo" => Some("ja"),
        "russian" => Some("ru"),
        "spanish" | "espanol" | "castilian" => Some("es"),
        "french" => Some("fr"),
        "german" | "deutsch" => Some("de"),
        "italian" => Some("it"),
        "portuguese" => Some("pt"),
        "chinese" | "mandarin" | "cantonese" => Some("zh"),
        "korean" => Some("ko"),
        "hindi" => Some("hi"),
        "arabic" => Some("ar"),
        _ => None,
    }
}

fn anime_profile_matches(profile: &str, evidence: &CandidateLanguageEvidence) -> bool {
    match profile {
        "ja_audio_en_subs" => {
            evidence.audio.contains("ja")
                && (evidence.subtitles.contains("en") || evidence.profiles.contains("subbed"))
        }
        "dual_audio" => {
            evidence.profiles.contains("dual_audio")
                || (evidence.audio.contains("ja") && evidence.audio.contains("en"))
        }
        "en_audio" => evidence.audio.contains("en") || evidence.profiles.contains("dubbed"),
        "subbed" => evidence.profiles.contains("subbed") || !evidence.subtitles.is_empty(),
        "dubbed" => evidence.profiles.contains("dubbed"),
        other => evidence.profiles.contains(other),
    }
}

fn intersect_sorted(left: &BTreeSet<String>, right: &[String]) -> Vec<String> {
    right
        .iter()
        .filter(|value| left.contains(value.as_str()))
        .cloned()
        .collect()
}

pub fn language_preference_to_value(preference: &AcquisitionLanguagePreference) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert("mode".to_string(), json!(preference.mode));
    object.insert("movie".to_string(), json!(preference.movie));
    object.insert("tv".to_string(), json!(preference.tv));
    object.insert("anime".to_string(), json!(preference.anime));
    object.insert(
        "unknownLanguage".to_string(),
        json!(preference.unknown_language),
    );
    JsonValue::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lp1_normalizes_common_language_values() {
        assert_eq!(normalize_language_value("English").as_deref(), Some("en"));
        assert_eq!(normalize_language_value("eng").as_deref(), Some("en"));
        assert_eq!(normalize_language_value("jpn").as_deref(), Some("ja"));
        assert_eq!(normalize_language_value("Russian").as_deref(), Some("ru"));
        assert_eq!(normalize_language_value("unknown"), None);
    }

    #[test]
    fn lp1_normalizes_anime_language_profiles() {
        assert_eq!(
            normalize_language_profile("Dual Audio").as_deref(),
            Some("dual_audio")
        );
        assert_eq!(
            normalize_language_profile("multi audio").as_deref(),
            Some("dual_audio")
        );
        assert_eq!(
            normalize_language_profile("subbed").as_deref(),
            Some("subbed")
        );
        assert_eq!(normalize_language_profile("dub").as_deref(), Some("dubbed"));
    }

    #[test]
    fn lp3_anime_dual_audio_matches_dual_profile() {
        let preference = AcquisitionLanguagePreference {
            mode: LanguagePreferenceMode::Prefer,
            anime: LanguagePreferenceMediaRule {
                profiles: vec!["dual_audio".to_string()],
                ..LanguagePreferenceMediaRule::default()
            },
            ..AcquisitionLanguagePreference::default()
        }
        .normalized();
        let mut evidence = CandidateLanguageEvidence::default();
        add_language_evidence_text(&mut evidence, "Dual Audio");

        let assessment = assess_language_preference(&preference, MediaType::Anime, &evidence);

        assert_eq!(assessment.state, LanguagePreferenceAssessmentState::Match);
        assert!(assessment.score_delta > 0.0);
    }

    #[test]
    fn alm9_noisy_release_tokens_do_not_manufacture_dual_audio() {
        let preference = AcquisitionLanguagePreference {
            mode: LanguagePreferenceMode::Prefer,
            anime: LanguagePreferenceMediaRule {
                profiles: vec!["dual_audio".to_string()],
                ..LanguagePreferenceMediaRule::default()
            },
            ..AcquisitionLanguagePreference::default()
        }
        .normalized();
        let mut noisy_english_dub = CandidateLanguageEvidence::default();
        add_language_evidence_text(
            &mut noisy_english_dub,
            "[Yameii] Example Anime - 01 [English Dub] [CR WEB-DL]",
        );

        let noisy_assessment =
            assess_language_preference(&preference, MediaType::Anime, &noisy_english_dub);

        assert_eq!(
            noisy_assessment.state,
            LanguagePreferenceAssessmentState::Mismatch
        );
        assert!(noisy_assessment.matching_profiles.is_empty());

        for release in [
            "Example Anime - 01 [Dual Audio]",
            "Example Anime - 01 [Multi Audio]",
        ] {
            let mut explicit_profile = CandidateLanguageEvidence::default();
            add_language_evidence_text(&mut explicit_profile, release);

            let assessment =
                assess_language_preference(&preference, MediaType::Anime, &explicit_profile);

            assert_eq!(
                assessment.state,
                LanguagePreferenceAssessmentState::Match,
                "{release}"
            );
            assert_eq!(assessment.matching_profiles, vec!["dual_audio"]);
        }

        let mut explicit_japanese_and_english = CandidateLanguageEvidence::default();
        add_language_evidence_text(&mut explicit_japanese_and_english, "Audio: JA + EN");
        let assessment = assess_language_preference(
            &preference,
            MediaType::Anime,
            &explicit_japanese_and_english,
        );
        assert_eq!(assessment.state, LanguagePreferenceAssessmentState::Match);
        assert_eq!(assessment.matching_profiles, vec!["dual_audio"]);
    }

    #[test]
    fn lp3_anime_audio_preference_builds_request_scoped_dub_profile() {
        let preference = AnimeAudioPreference {
            mode: AnimeAudioPreferenceMode::PreferDub,
            language: None,
        };

        let profile =
            quality_profile_with_anime_audio_preference(None, MediaType::Anime, Some(&preference))
                .expect("quality profile");

        assert_eq!(
            profile.pointer("/animeAudioPreference/mode"),
            Some(&json!("prefer_dub"))
        );
        assert_eq!(
            profile.pointer("/animeAudioPreference/language"),
            Some(&json!("en"))
        );
        assert_eq!(
            profile.pointer("/languagePreference/mode"),
            Some(&json!("prefer"))
        );
        assert_eq!(
            profile.pointer("/providerLanguageHints"),
            Some(&json!(["en"]))
        );
        assert!(profile.pointer("/requiredLanguages").is_none());
        assert!(profile.pointer("/requiredAudioLanguages").is_none());

        let saved = AcquisitionLanguagePreference {
            mode: LanguagePreferenceMode::Prefer,
            anime: LanguagePreferenceMediaRule {
                profiles: vec!["ja_audio_en_subs".to_string()],
                ..LanguagePreferenceMediaRule::default()
            },
            ..AcquisitionLanguagePreference::default()
        };
        let merged = quality_profile_with_language_preference(
            Some(profile.clone()),
            MediaType::Anime,
            &saved,
        )
        .expect("merged profile");
        assert_eq!(
            merged.pointer("/languagePreference/anime/profiles"),
            Some(&json!(["dual_audio", "dubbed", "en_audio"]))
        );
    }

    #[test]
    fn lp3_anime_audio_preference_ignores_non_anime_requests() {
        let preference = AnimeAudioPreference {
            mode: AnimeAudioPreferenceMode::PreferDub,
            language: Some("English".to_string()),
        };

        let profile = quality_profile_with_anime_audio_preference(
            Some(json!({ "allowedQualities": ["1080p"] })),
            MediaType::Series,
            Some(&preference),
        )
        .expect("quality profile");

        assert_eq!(
            profile.pointer("/allowedQualities"),
            Some(&json!(["1080p"]))
        );
        assert!(profile.pointer("/animeAudioPreference").is_none());
        assert!(profile.pointer("/languagePreference").is_none());
    }
}
