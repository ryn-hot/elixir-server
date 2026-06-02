use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    acquisition::release_resolution::movie_radarr_parser::{clean_movie_title, normalize_imdb_id},
    extensions::ExternalIds,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieIdentityGraphInput {
    pub target_title: String,
    pub target_year: Option<i32>,
    #[serde(default)]
    pub target_external_ids: ExternalIds,
    #[serde(default)]
    pub tvdb_movie: Option<JsonValue>,
    #[serde(default)]
    pub candidate_external_ids: Vec<MovieSourceExternalIds>,
}

impl MovieIdentityGraphInput {
    pub fn new(
        target_title: impl Into<String>,
        target_year: Option<i32>,
        target_external_ids: ExternalIds,
        tvdb_movie: Option<JsonValue>,
    ) -> Self {
        Self {
            target_title: target_title.into(),
            target_year,
            target_external_ids,
            tvdb_movie,
            candidate_external_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieSourceExternalIds {
    pub source: String,
    pub external_ids: ExternalIds,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieIdentityGraph {
    pub target_title: String,
    pub target_year: Option<i32>,
    pub canonical_title: Option<String>,
    pub canonical_year: Option<i32>,
    pub runtime_seconds: Option<i32>,
    pub external_ids: ExternalIds,
    pub titles: Vec<MovieTitleEvidence>,
    pub years: Vec<MovieYearEvidence>,
    pub remote_ids: Vec<MovieRemoteIdEvidence>,
    pub id_conflicts: Vec<MovieExternalIdConflict>,
    pub diagnostics: Vec<String>,
}

impl MovieIdentityGraph {
    pub fn from_input(input: MovieIdentityGraphInput) -> Self {
        let mut builder = MovieIdentityGraphBuilder::new(&input);
        builder.add_target(&input);

        if let Some(tvdb_movie) = input.tvdb_movie.as_ref() {
            builder.add_tvdb_movie(tvdb_movie);
        }

        for source in &input.candidate_external_ids {
            builder.add_candidate_external_ids(source);
        }

        builder.finish()
    }

    pub fn normalized_title_set(&self) -> BTreeSet<String> {
        self.titles
            .iter()
            .filter_map(|entry| entry.normalized.clone())
            .collect()
    }

    pub fn has_identity_external_id(&self, provider: MovieExternalIdProvider, value: &str) -> bool {
        let Some(normalized) = normalize_external_id(provider, value) else {
            return false;
        };
        identity_external_id(&self.external_ids, provider) == Some(&normalized)
    }
}

pub fn build_movie_identity_graph(input: MovieIdentityGraphInput) -> MovieIdentityGraph {
    MovieIdentityGraph::from_input(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieTitleEvidenceKind {
    Target,
    TvdbCanonical,
    TvdbAlias,
    TvdbTranslation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieTitleEvidence {
    pub title: String,
    pub normalized: Option<String>,
    pub kind: MovieTitleEvidenceKind,
    pub source: String,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieYearEvidenceKind {
    Target,
    TvdbYear,
    TvdbReleaseDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieYearEvidence {
    pub year: i32,
    pub kind: MovieYearEvidenceKind,
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieExternalIdProvider {
    Tvdb,
    TvdbMovie,
    Imdb,
    Tmdb,
    Eidr,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieExternalIdEvidenceKind {
    Target,
    TvdbRemoteId,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieRemoteIdEvidence {
    pub provider: MovieExternalIdProvider,
    pub id: String,
    pub normalized_id: String,
    pub source: String,
    pub source_name: Option<String>,
    pub kind: MovieExternalIdEvidenceKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieExternalIdConflict {
    pub provider: MovieExternalIdProvider,
    pub kept: String,
    pub rejected: String,
    pub kept_source: String,
    pub rejected_source: String,
}

struct MovieIdentityGraphBuilder {
    graph: MovieIdentityGraph,
    title_keys: BTreeSet<(String, MovieTitleEvidenceKind, String, String)>,
    year_keys: BTreeSet<(i32, MovieYearEvidenceKind, String)>,
    remote_id_keys: BTreeSet<(
        MovieExternalIdEvidenceKind,
        MovieExternalIdProvider,
        String,
        String,
    )>,
    identity_id_sources: BTreeMap<MovieExternalIdProvider, String>,
}

impl MovieIdentityGraphBuilder {
    fn new(input: &MovieIdentityGraphInput) -> Self {
        Self {
            graph: MovieIdentityGraph {
                target_title: input.target_title.clone(),
                target_year: input.target_year,
                canonical_title: None,
                canonical_year: None,
                runtime_seconds: None,
                external_ids: ExternalIds::default(),
                titles: Vec::new(),
                years: Vec::new(),
                remote_ids: Vec::new(),
                id_conflicts: Vec::new(),
                diagnostics: Vec::new(),
            },
            title_keys: BTreeSet::new(),
            year_keys: BTreeSet::new(),
            remote_id_keys: BTreeSet::new(),
            identity_id_sources: BTreeMap::new(),
        }
    }

    fn add_target(&mut self, input: &MovieIdentityGraphInput) {
        self.add_title(
            &input.target_title,
            MovieTitleEvidenceKind::Target,
            "target_metadata",
            None,
        );
        if let Some(year) = input.target_year {
            self.add_year(year, MovieYearEvidenceKind::Target, "target_metadata");
        }

        let ids = normalize_movie_external_ids(&input.target_external_ids);
        self.add_identity_external_ids(
            &ids,
            MovieExternalIdEvidenceKind::Target,
            "target_metadata",
        );
    }

    fn add_tvdb_movie(&mut self, tvdb_movie: &JsonValue) {
        let meta = tvdb_movie_payload(tvdb_movie);

        if let Some(tvdb_id) = extract_tvdb_movie_id(meta) {
            self.add_remote_id(
                MovieExternalIdProvider::TvdbMovie,
                &tvdb_id,
                MovieExternalIdEvidenceKind::TvdbRemoteId,
                "tvdb_movie_metadata",
                Some("tvdb_movie_id"),
                true,
            );
            self.add_remote_id(
                MovieExternalIdProvider::Tvdb,
                &tvdb_id,
                MovieExternalIdEvidenceKind::TvdbRemoteId,
                "tvdb_movie_metadata",
                Some("tvdb_id"),
                true,
            );
        } else {
            self.graph
                .diagnostics
                .push("tvdb_movie_metadata_missing_id".to_string());
        }

        for key in [
            "name",
            "movieName",
            "movie_name",
            "title",
            "originalTitle",
            "original_title",
        ] {
            if let Some(title) = json_string(meta.get(key)) {
                if self.graph.canonical_title.is_none() {
                    self.graph.canonical_title = Some(title.clone());
                }
                self.add_title(
                    &title,
                    MovieTitleEvidenceKind::TvdbCanonical,
                    format!("tvdb_movie_metadata.{key}"),
                    None,
                );
            }
        }

        self.add_tvdb_aliases(meta);
        self.add_tvdb_translations(meta);

        if let Some(year) = extract_tvdb_year(meta) {
            self.add_year(
                year,
                MovieYearEvidenceKind::TvdbYear,
                "tvdb_movie_metadata.year",
            );
            if self.graph.canonical_year.is_none() {
                self.graph.canonical_year = Some(year);
            }
        }

        for (year, source) in extract_tvdb_release_years(meta) {
            self.add_year(year, MovieYearEvidenceKind::TvdbReleaseDate, source);
            if self.graph.canonical_year.is_none() {
                self.graph.canonical_year = Some(year);
            }
        }

        if self.graph.canonical_year.is_none() {
            self.graph.canonical_year = self.graph.target_year;
        }

        self.graph.runtime_seconds = extract_tvdb_runtime_seconds(meta);
        self.add_tvdb_remote_ids(meta);
    }

    fn add_candidate_external_ids(&mut self, source: &MovieSourceExternalIds) {
        let ids = normalize_movie_external_ids(&source.external_ids);
        for (provider, value, source_name) in external_id_pairs(&ids) {
            self.add_remote_id(
                provider,
                &value,
                MovieExternalIdEvidenceKind::Candidate,
                source.source.clone(),
                Some(source_name),
                false,
            );
        }
    }

    fn add_tvdb_aliases(&mut self, meta: &JsonValue) {
        for key in [
            "aliases",
            "aka",
            "alternativeTitles",
            "alternative_titles",
            "alternateTitles",
            "alternate_titles",
            "alsoKnownAs",
            "also_known_as",
        ] {
            if let Some(value) = meta.get(key) {
                self.add_alias_value(value, format!("tvdb_movie_metadata.{key}"), None);
            }
        }

        if let Some(translations) = meta.get("translations") {
            for key in ["aliases", "aliasTranslations", "alias_translations"] {
                if let Some(value) = translations.get(key) {
                    self.add_alias_value(
                        value,
                        format!("tvdb_movie_metadata.translations.{key}"),
                        None,
                    );
                }
            }
        }
    }

    fn add_alias_value(
        &mut self,
        value: &JsonValue,
        source: impl Into<String> + Clone,
        language: Option<String>,
    ) {
        if let Some(text) = tvdb_text_value(value) {
            self.add_title(
                &text,
                MovieTitleEvidenceKind::TvdbAlias,
                source.into(),
                language,
            );
            return;
        }

        if let Some(array) = value.as_array() {
            for entry in array {
                let language = language.clone().or_else(|| tvdb_language(entry));
                if let Some(text) = tvdb_text_value(entry) {
                    self.add_title(
                        &text,
                        MovieTitleEvidenceKind::TvdbAlias,
                        source.clone().into(),
                        language,
                    );
                }
            }
            return;
        }

        if let Some(object) = value.as_object() {
            for (key, entry) in object {
                let language = language.clone().or_else(|| Some(key.clone()));
                self.add_alias_value(entry, source.clone(), language);
            }
        }
    }

    fn add_tvdb_translations(&mut self, meta: &JsonValue) {
        for key in ["nameTranslations", "name_translations"] {
            if let Some(value) = meta.get(key) {
                self.add_translation_value(value, format!("tvdb_movie_metadata.{key}"), None);
            }
        }

        if let Some(translations) = meta.get("translations") {
            for key in [
                "nameTranslations",
                "name_translations",
                "titles",
                "titleTranslations",
                "title_translations",
            ] {
                if let Some(value) = translations.get(key) {
                    self.add_translation_value(
                        value,
                        format!("tvdb_movie_metadata.translations.{key}"),
                        None,
                    );
                }
            }
        }
    }

    fn add_translation_value(
        &mut self,
        value: &JsonValue,
        source: impl Into<String> + Clone,
        language: Option<String>,
    ) {
        if let Some(text) = tvdb_text_value(value) {
            if !looks_like_language_code(&text) {
                self.add_title(
                    &text,
                    MovieTitleEvidenceKind::TvdbTranslation,
                    source.into(),
                    language,
                );
            }
            return;
        }

        if let Some(array) = value.as_array() {
            for entry in array {
                let language = language.clone().or_else(|| tvdb_language(entry));
                if let Some(text) = tvdb_text_value(entry)
                    && !looks_like_language_code(&text)
                {
                    self.add_title(
                        &text,
                        MovieTitleEvidenceKind::TvdbTranslation,
                        source.clone().into(),
                        language,
                    );
                }
            }
            return;
        }

        if let Some(object) = value.as_object() {
            for (key, entry) in object {
                let language = language.clone().or_else(|| Some(key.clone()));
                self.add_translation_value(entry, source.clone(), language);
            }
        }
    }

    fn add_tvdb_remote_ids(&mut self, meta: &JsonValue) {
        for key in ["remoteIds", "remote_ids"] {
            let Some(values) = meta.get(key).and_then(JsonValue::as_array) else {
                continue;
            };
            for entry in values {
                let Some(raw_id) = json_string(
                    entry
                        .get("id")
                        .or_else(|| entry.get("remoteId"))
                        .or_else(|| entry.get("remote_id")),
                ) else {
                    continue;
                };
                let source_name = entry
                    .get("sourceName")
                    .or_else(|| entry.get("source_name"))
                    .or_else(|| entry.get("source"))
                    .or_else(|| entry.get("type"))
                    .and_then(JsonValue::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
                let provider = classify_remote_id(source_name.as_deref(), &raw_id);
                self.add_remote_id(
                    provider,
                    &raw_id,
                    MovieExternalIdEvidenceKind::TvdbRemoteId,
                    format!("tvdb_movie_metadata.{key}"),
                    source_name,
                    true,
                );
            }
        }
    }

    fn add_identity_external_ids(
        &mut self,
        ids: &ExternalIds,
        kind: MovieExternalIdEvidenceKind,
        source: impl Into<String> + Clone,
    ) {
        for (provider, value, source_name) in external_id_pairs(ids) {
            self.add_remote_id(
                provider,
                &value,
                kind,
                source.clone(),
                Some(source_name),
                true,
            );
        }
    }

    fn add_title(
        &mut self,
        title: &str,
        kind: MovieTitleEvidenceKind,
        source: impl Into<String>,
        language: Option<String>,
    ) {
        let title = title.trim();
        if title.is_empty() || looks_like_language_code(title) {
            return;
        }
        let normalized = normalize_movie_title(title);
        let source = source.into();
        let language_key = language.clone().unwrap_or_default();
        let key = (title.to_string(), kind, source.clone(), language_key);
        if !self.title_keys.insert(key) {
            return;
        }
        self.graph.titles.push(MovieTitleEvidence {
            title: title.to_string(),
            normalized,
            kind,
            source,
            language,
        });
    }

    fn add_year(&mut self, year: i32, kind: MovieYearEvidenceKind, source: impl Into<String>) {
        if !(1800..=2200).contains(&year) {
            return;
        }
        let source = source.into();
        if !self.year_keys.insert((year, kind, source.clone())) {
            return;
        }
        self.graph
            .years
            .push(MovieYearEvidence { year, kind, source });
    }

    fn add_remote_id(
        &mut self,
        provider: MovieExternalIdProvider,
        id: &str,
        kind: MovieExternalIdEvidenceKind,
        source: impl Into<String>,
        source_name: Option<impl Into<String>>,
        contributes_to_identity: bool,
    ) {
        let Some(normalized_id) = normalize_external_id(provider, id) else {
            return;
        };
        let source = source.into();
        let source_name = source_name.map(Into::into);
        if !self
            .remote_id_keys
            .insert((kind, provider, normalized_id.clone(), source.clone()))
        {
            return;
        }

        if contributes_to_identity {
            self.merge_identity_id(provider, &normalized_id, &source);
        }

        self.graph.remote_ids.push(MovieRemoteIdEvidence {
            provider,
            id: id.trim().to_string(),
            normalized_id,
            source,
            source_name,
            kind,
        });
    }

    fn merge_identity_id(&mut self, provider: MovieExternalIdProvider, value: &str, source: &str) {
        if !identity_provider(provider) {
            return;
        }

        let existing = identity_external_id(&self.graph.external_ids, provider).cloned();
        match existing {
            None => {
                set_identity_external_id(&mut self.graph.external_ids, provider, value.to_string());
                self.identity_id_sources
                    .insert(provider, source.to_string());
            }
            Some(current) if current == value => {}
            Some(current) => {
                let kept_source = self
                    .identity_id_sources
                    .get(&provider)
                    .cloned()
                    .unwrap_or_else(|| "identity_graph".to_string());
                self.graph.id_conflicts.push(MovieExternalIdConflict {
                    provider,
                    kept: current,
                    rejected: value.to_string(),
                    kept_source,
                    rejected_source: source.to_string(),
                });
            }
        }
    }

    fn finish(mut self) -> MovieIdentityGraph {
        if self.graph.canonical_title.is_none() {
            self.graph.canonical_title = self
                .graph
                .titles
                .iter()
                .find(|entry| entry.kind == MovieTitleEvidenceKind::Target)
                .map(|entry| entry.title.clone());
        }
        if self.graph.canonical_year.is_none() {
            self.graph.canonical_year = self.graph.target_year;
        }
        self.graph
    }
}

pub fn normalize_movie_title(title: &str) -> Option<String> {
    let normalized = clean_movie_title(title);
    let normalized = normalized.trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn normalize_movie_external_ids(ids: &ExternalIds) -> ExternalIds {
    let tvdb = ids.tvdb.as_deref().and_then(normalize_string_id);
    let tvdb_movie = ids
        .tvdb_movie
        .as_deref()
        .and_then(normalize_string_id)
        .or_else(|| tvdb.clone());

    ExternalIds {
        imdb: ids.imdb.as_deref().and_then(normalize_imdb_external_id),
        tmdb: ids.tmdb.as_deref().and_then(normalize_numeric_string_id),
        tvdb: tvdb.or_else(|| tvdb_movie.clone()),
        tvdb_series: ids.tvdb_series.as_deref().and_then(normalize_string_id),
        tvdb_movie,
        anilist: ids.anilist.as_deref().and_then(normalize_string_id),
        anidb: ids.anidb.as_deref().and_then(normalize_string_id),
        mal: ids.mal.as_deref().and_then(normalize_string_id),
        kitsu: ids.kitsu.as_deref().and_then(normalize_string_id),
    }
}

fn external_id_pairs(ids: &ExternalIds) -> Vec<(MovieExternalIdProvider, String, &'static str)> {
    let mut pairs = Vec::new();
    if let Some(value) = ids.tvdb.as_ref() {
        pairs.push((MovieExternalIdProvider::Tvdb, value.clone(), "tvdb"));
    }
    if let Some(value) = ids.tvdb_movie.as_ref() {
        pairs.push((
            MovieExternalIdProvider::TvdbMovie,
            value.clone(),
            "tvdb_movie",
        ));
    }
    if let Some(value) = ids.imdb.as_ref() {
        pairs.push((MovieExternalIdProvider::Imdb, value.clone(), "imdb"));
    }
    if let Some(value) = ids.tmdb.as_ref() {
        pairs.push((MovieExternalIdProvider::Tmdb, value.clone(), "tmdb"));
    }
    pairs
}

fn identity_provider(provider: MovieExternalIdProvider) -> bool {
    matches!(
        provider,
        MovieExternalIdProvider::Tvdb
            | MovieExternalIdProvider::TvdbMovie
            | MovieExternalIdProvider::Imdb
            | MovieExternalIdProvider::Tmdb
    )
}

fn identity_external_id(ids: &ExternalIds, provider: MovieExternalIdProvider) -> Option<&String> {
    match provider {
        MovieExternalIdProvider::Tvdb => ids.tvdb.as_ref(),
        MovieExternalIdProvider::TvdbMovie => ids.tvdb_movie.as_ref(),
        MovieExternalIdProvider::Imdb => ids.imdb.as_ref(),
        MovieExternalIdProvider::Tmdb => ids.tmdb.as_ref(),
        MovieExternalIdProvider::Eidr | MovieExternalIdProvider::Other => None,
    }
}

fn set_identity_external_id(
    ids: &mut ExternalIds,
    provider: MovieExternalIdProvider,
    value: String,
) {
    match provider {
        MovieExternalIdProvider::Tvdb => ids.tvdb = Some(value),
        MovieExternalIdProvider::TvdbMovie => ids.tvdb_movie = Some(value),
        MovieExternalIdProvider::Imdb => ids.imdb = Some(value),
        MovieExternalIdProvider::Tmdb => ids.tmdb = Some(value),
        MovieExternalIdProvider::Eidr | MovieExternalIdProvider::Other => {}
    }
}

fn normalize_external_id(provider: MovieExternalIdProvider, value: &str) -> Option<String> {
    match provider {
        MovieExternalIdProvider::Imdb => normalize_imdb_external_id(value),
        MovieExternalIdProvider::Tmdb => normalize_numeric_string_id(value),
        MovieExternalIdProvider::Tvdb | MovieExternalIdProvider::TvdbMovie => {
            normalize_string_id(value)
        }
        MovieExternalIdProvider::Eidr | MovieExternalIdProvider::Other => {
            normalize_string_id(value)
        }
    }
}

fn normalize_imdb_external_id(value: &str) -> Option<String> {
    let normalized_case = value.trim().to_ascii_lowercase();
    normalize_imdb_id(&normalized_case)
}

fn normalize_string_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_numeric_string_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(trimmed.to_string());
    }
    trimmed
        .rsplit(|ch: char| !ch.is_ascii_digit())
        .find(|token| !token.is_empty())
        .map(str::to_string)
}

fn tvdb_movie_payload(value: &JsonValue) -> &JsonValue {
    value.get("data").unwrap_or(value)
}

fn extract_tvdb_movie_id(value: &JsonValue) -> Option<String> {
    json_string(
        value
            .get("id")
            .or_else(|| value.get("tvdbId"))
            .or_else(|| value.get("tvdb_id")),
    )
}

fn extract_tvdb_year(value: &JsonValue) -> Option<i32> {
    for key in ["year", "releaseYear", "release_year"] {
        if let Some(year) = json_i32(value.get(key)) {
            return Some(year);
        }
        if let Some(year) = value
            .get(key)
            .and_then(JsonValue::as_str)
            .and_then(parse_year_str)
        {
            return Some(year);
        }
    }

    for key in [
        "releaseDate",
        "release_date",
        "released",
        "firstAired",
        "first_air_time",
        "first_air_date",
        "premiereDate",
        "premiere_date",
        "startDate",
        "start_date",
    ] {
        if let Some(year) = value
            .get(key)
            .and_then(JsonValue::as_str)
            .and_then(parse_year_str)
        {
            return Some(year);
        }
    }
    None
}

fn extract_tvdb_release_years(value: &JsonValue) -> Vec<(i32, String)> {
    let mut years = Vec::new();
    for key in [
        "releaseDate",
        "release_date",
        "released",
        "firstAired",
        "first_air_time",
        "first_air_date",
        "premiereDate",
        "premiere_date",
        "startDate",
        "start_date",
    ] {
        if let Some(year) = value
            .get(key)
            .and_then(JsonValue::as_str)
            .and_then(parse_year_str)
        {
            years.push((year, format!("tvdb_movie_metadata.{key}")));
        }
    }

    for key in ["releases", "releaseDates", "release_dates"] {
        let Some(entries) = value.get(key).and_then(JsonValue::as_array) else {
            continue;
        };
        for entry in entries {
            for date_key in [
                "date",
                "releaseDate",
                "release_date",
                "released",
                "dateString",
                "date_string",
            ] {
                if let Some(year) = entry
                    .get(date_key)
                    .and_then(JsonValue::as_str)
                    .and_then(parse_year_str)
                {
                    years.push((year, format!("tvdb_movie_metadata.{key}.{date_key}")));
                }
            }
        }
    }
    years
}

fn extract_tvdb_runtime_seconds(value: &JsonValue) -> Option<i32> {
    for key in ["runtimeSeconds", "runtime_seconds"] {
        if let Some(seconds) = json_i32(value.get(key)) {
            return Some(seconds);
        }
    }

    for key in ["runtime", "runtimeMinutes", "runtime_minutes", "length"] {
        if let Some(minutes) = json_i32(value.get(key)) {
            return minutes.checked_mul(60);
        }
    }
    None
}

fn add_field_text<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    fields: &[&str],
) -> Option<&'a str> {
    fields
        .iter()
        .filter_map(|key| object.get(*key))
        .find_map(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn tvdb_text_value(value: &JsonValue) -> Option<String> {
    if let Some(text) = value.as_str() {
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }

    let object = value.as_object()?;
    add_field_text(
        object,
        &[
            "name",
            "title",
            "movieName",
            "movie_name",
            "alias",
            "text",
            "translation",
            "value",
        ],
    )
    .map(str::to_string)
}

fn tvdb_language(value: &JsonValue) -> Option<String> {
    let object = value.as_object()?;
    add_field_text(
        object,
        &[
            "language",
            "languageCode",
            "language_code",
            "lang",
            "iso_639_1",
            "iso6391",
            "iso_639_2",
            "iso6392",
        ],
    )
    .map(str::to_string)
}

fn classify_remote_id(source_name: Option<&str>, id: &str) -> MovieExternalIdProvider {
    let source = source_name
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let id_lower = id.trim().to_ascii_lowercase();

    if id_lower.starts_with("tt") || source.contains("imdb") {
        return MovieExternalIdProvider::Imdb;
    }
    if source.contains("tmdb")
        || source.contains("themoviedb")
        || source.contains("the movie database")
    {
        return MovieExternalIdProvider::Tmdb;
    }
    if source.contains("eidr") {
        return MovieExternalIdProvider::Eidr;
    }
    MovieExternalIdProvider::Other
}

fn json_string(value: Option<&JsonValue>) -> Option<String> {
    let value = value?;
    if let Some(text) = value.as_str() {
        return normalize_string_id(text);
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_u64() {
        return Some(number.to_string());
    }
    None
}

fn json_i32(value: Option<&JsonValue>) -> Option<i32> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    if let Some(number) = value.as_u64() {
        return i32::try_from(number).ok();
    }
    value.as_str()?.trim().parse::<i32>().ok()
}

fn parse_year_str(value: &str) -> Option<i32> {
    let trimmed = value.trim();
    let prefix = trimmed.get(0..4)?;
    if prefix.chars().all(|ch| ch.is_ascii_digit()) {
        prefix.parse::<i32>().ok()
    } else {
        None
    }
}

fn looks_like_language_code(value: &str) -> bool {
    let trimmed = value.trim();
    matches!(trimmed.len(), 2 | 3) && trimmed.chars().all(|ch| ch.is_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn target_ids() -> ExternalIds {
        ExternalIds {
            tvdb_movie: Some("170".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn movie_identity_graph_builds_tvdb_only_identity() {
        let graph = build_movie_identity_graph(MovieIdentityGraphInput::new(
            "The Matrix",
            Some(1999),
            target_ids(),
            Some(json!({
                "id": 170,
                "name": "The Matrix",
                "year": "1999",
                "runtime": 136,
                "aliases": ["Matrix"],
                "releaseDate": "1999-03-31",
            })),
        ));

        assert_eq!(graph.canonical_title.as_deref(), Some("The Matrix"));
        assert_eq!(graph.canonical_year, Some(1999));
        assert_eq!(graph.runtime_seconds, Some(8160));
        assert_eq!(graph.external_ids.tvdb_movie.as_deref(), Some("170"));
        assert_eq!(graph.external_ids.tvdb.as_deref(), Some("170"));
        assert!(graph.id_conflicts.is_empty());
        assert!(graph.normalized_title_set().contains("thematrix"));
        assert!(graph.normalized_title_set().contains("matrix"));
        assert!(graph.years.iter().any(|year| {
            year.year == 1999 && year.kind == MovieYearEvidenceKind::TvdbReleaseDate
        }));
    }

    #[test]
    fn movie_identity_graph_maps_tvdb_remote_ids() {
        let graph = build_movie_identity_graph(MovieIdentityGraphInput::new(
            "The Matrix",
            Some(1999),
            target_ids(),
            Some(json!({
                "data": {
                    "id": "170",
                    "name": "The Matrix",
                    "remoteIds": [
                        {"sourceName": "IMDB", "id": "tt0133093"},
                        {"sourceName": "TheMovieDB.com", "id": 603},
                        {"sourceName": "EIDR", "id": "10.5240/1820-2C7E-73D1-6A31-0866-F"}
                    ]
                }
            })),
        ));

        assert_eq!(graph.external_ids.imdb.as_deref(), Some("tt0133093"));
        assert_eq!(graph.external_ids.tmdb.as_deref(), Some("603"));
        assert!(graph.remote_ids.iter().any(|entry| {
            entry.provider == MovieExternalIdProvider::Eidr
                && entry.normalized_id == "10.5240/1820-2C7E-73D1-6A31-0866-F"
        }));
        assert!(graph.has_identity_external_id(MovieExternalIdProvider::Imdb, "0133093"));
        assert!(graph.has_identity_external_id(MovieExternalIdProvider::Imdb, "TT0133093"));
    }

    #[test]
    fn movie_identity_graph_preserves_aliases_and_translations() {
        let graph = build_movie_identity_graph(MovieIdentityGraphInput::new(
            "Spirited Away",
            Some(2001),
            ExternalIds {
                tvdb_movie: Some("118".to_string()),
                ..Default::default()
            },
            Some(json!({
                "id": "118",
                "name": "Spirited Away",
                "aliases": [
                    {"name": "Sen and Chihiro's Spiriting Away", "language": "eng"},
                    {"title": "千と千尋の神隠し", "languageCode": "jpn"}
                ],
                "translations": {
                    "nameTranslations": [
                        {"name": "El viaje de Chihiro", "language": "spa"},
                        {"name": "Le voyage de Chihiro", "languageCode": "fra"}
                    ]
                }
            })),
        ));

        assert!(graph.titles.iter().any(|entry| {
            entry.kind == MovieTitleEvidenceKind::TvdbAlias
                && entry.title == "Sen and Chihiro's Spiriting Away"
                && entry.language.as_deref() == Some("eng")
        }));
        assert!(graph.titles.iter().any(|entry| {
            entry.kind == MovieTitleEvidenceKind::TvdbTranslation
                && entry.title == "El viaje de Chihiro"
                && entry.language.as_deref() == Some("spa")
        }));
        assert!(graph.normalized_title_set().contains("elviajedechihiro"));
    }

    #[test]
    fn movie_identity_graph_records_remote_id_conflicts_without_candidate_poisoning() {
        let graph = build_movie_identity_graph(MovieIdentityGraphInput {
            target_title: "The Matrix".to_string(),
            target_year: Some(1999),
            target_external_ids: ExternalIds {
                imdb: Some("tt0133093".to_string()),
                tvdb_movie: Some("170".to_string()),
                ..Default::default()
            },
            tvdb_movie: Some(json!({
                "id": "170",
                "name": "The Matrix",
                "remoteIds": [
                    {"sourceName": "IMDB", "id": "tt9999999"}
                ]
            })),
            candidate_external_ids: vec![MovieSourceExternalIds {
                source: "source_candidate".to_string(),
                external_ids: ExternalIds {
                    imdb: Some("tt1111111".to_string()),
                    tmdb: Some("603".to_string()),
                    ..Default::default()
                },
            }],
        });

        assert_eq!(graph.external_ids.imdb.as_deref(), Some("tt0133093"));
        assert_eq!(graph.external_ids.tmdb, None);
        assert_eq!(graph.id_conflicts.len(), 1);
        assert_eq!(
            graph.id_conflicts[0].provider,
            MovieExternalIdProvider::Imdb
        );
        assert_eq!(graph.id_conflicts[0].kept, "tt0133093");
        assert_eq!(graph.id_conflicts[0].rejected, "tt9999999");
        assert!(graph.remote_ids.iter().any(|entry| {
            entry.kind == MovieExternalIdEvidenceKind::Candidate
                && entry.provider == MovieExternalIdProvider::Tmdb
                && entry.normalized_id == "603"
        }));
    }

    #[test]
    fn movie_identity_graph_falls_back_without_tvdb_metadata() {
        let graph = build_movie_identity_graph(MovieIdentityGraphInput::new(
            "Primer",
            Some(2004),
            ExternalIds::default(),
            None,
        ));

        assert_eq!(graph.canonical_title.as_deref(), Some("Primer"));
        assert_eq!(graph.canonical_year, Some(2004));
        assert_eq!(graph.runtime_seconds, None);
        assert_eq!(
            graph.normalized_title_set(),
            BTreeSet::from(["primer".to_string()])
        );
        assert!(graph.remote_ids.is_empty());
    }
}
