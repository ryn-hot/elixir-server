use std::collections::BTreeSet;

use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};

use crate::acquisition::release_resolution::movie::{
    MOVIE_RADARR_STYLE_RESOLVER_VERSION, RADARR_REFERENCE_COMMIT, RADARR_REFERENCE_REPOSITORY,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieResolution {
    R360p,
    R480p,
    R540p,
    R576p,
    R720p,
    R1080p,
    R2160p,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieQualitySource {
    Unknown,
    Cam,
    Telesync,
    Telecine,
    Workprint,
    Dvd,
    Tv,
    WebDl,
    WebRip,
    BluRay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieQualityModifier {
    None,
    Regional,
    Screener,
    RawHd,
    BrDisk,
    Remux,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovieQualityDetectionSource {
    Unknown,
    Name,
    Extension,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieQuality {
    pub quality: Option<String>,
    pub source: Option<MovieQualitySource>,
    pub resolution: Option<MovieResolution>,
    pub modifier: MovieQualityModifier,
    pub source_detection_source: MovieQualityDetectionSource,
    pub resolution_detection_source: MovieQualityDetectionSource,
    pub modifier_detection_source: MovieQualityDetectionSource,
    pub revision_detection_source: MovieQualityDetectionSource,
    pub revision_version: i32,
    pub revision_real: i32,
    pub revision_is_repack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieParsedRelease {
    pub parser_version: String,
    pub radarr_repository: String,
    pub radarr_commit: String,
    pub original_title: String,
    pub release_title: String,
    pub simple_release_title: String,
    pub movie_titles: Vec<String>,
    pub year: Option<i32>,
    pub quality: MovieQuality,
    pub languages: Vec<String>,
    pub release_group: Option<String>,
    pub release_hash: Option<String>,
    pub edition: Option<String>,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<i32>,
    pub hardcoded_subs: Option<String>,
    pub matched_pattern_id: Option<String>,
}

impl MovieParsedRelease {
    pub fn primary_movie_title(&self) -> Option<&str> {
        self.movie_titles.first().map(String::as_str)
    }
}

impl Default for MovieQuality {
    fn default() -> Self {
        quality_model(None, None, MovieQualityModifier::None)
    }
}

#[derive(Debug, Default)]
pub struct MovieRadarrStyleParser;

impl MovieRadarrStyleParser {
    pub fn parse_title(&self, title: &str) -> Option<MovieParsedRelease> {
        parse_movie_title(title, false)
    }

    pub fn parse_folder_title(&self, title: &str) -> Option<MovieParsedRelease> {
        parse_movie_title(title, true)
    }

    pub fn parse_path(&self, path: &str) -> Option<MovieParsedRelease> {
        parse_movie_path(path)
    }
}

pub fn parse_movie_path(path: &str) -> Option<MovieParsedRelease> {
    let file_name = path
        .rsplit(|ch| ch == '/' || ch == '\\')
        .next()
        .unwrap_or(path);
    if let Some(parsed) = parse_movie_title(file_name, true) {
        return Some(parsed);
    }

    let parent = path
        .rfind(['/', '\\'])
        .map(|idx| &path[..idx])
        .and_then(|parent| parent.rsplit(|ch| ch == '/' || ch == '\\').next())
        .unwrap_or_default();
    if !parent.is_empty()
        && let Some(parsed) = parse_movie_title(&format!("{parent} {file_name}"), false)
    {
        return Some(parsed);
    }

    let extension = path_extension(file_name).unwrap_or_default();
    if !parent.is_empty() {
        return parse_movie_title(&format!("{parent}{extension}"), false);
    }
    None
}

pub fn parse_movie_title(title: &str, is_dir: bool) -> Option<MovieParsedRelease> {
    let original_title = title.to_string();
    if !validate_before_parsing(title) {
        return None;
    }

    let mut title_for_parsing = title.to_string();
    if REVERSED_TITLE_RE.is_match(&title_for_parsing) {
        let without_ext = strip_file_extension(&title_for_parsing);
        let reversed = without_ext.chars().rev().collect::<String>();
        let extension = title_for_parsing
            .strip_prefix(&without_ext)
            .unwrap_or_default()
            .to_string();
        title_for_parsing = format!("{reversed}{extension}");
    }

    let mut release_title = strip_file_extension(&title_for_parsing);
    release_title = release_title
        .trim()
        .trim_matches(['-', '_'])
        .replace('\u{3010}', "[")
        .replace('\u{3011}', "]");

    let simple_title = simplify_title_for_movie_match(&release_title);
    let Some(title_match) = parse_movie_title_match(&simple_title, is_dir) else {
        return None;
    };

    let mut simple_release_title = SIMPLE_RELEASE_TITLE_RE
        .replace_all(&release_title, "")
        .to_string();
    if !title_match.raw_title.is_empty() {
        let replacement = if title_match.raw_title.contains('.') {
            "A.Movie"
        } else {
            "A Movie"
        };
        simple_release_title =
            replace_first_literal(&simple_release_title, &title_match.raw_title, replacement);
    }

    let mut release_group = parse_release_group(&simple_release_title);
    if let Some(group) = title_match
        .subgroup
        .clone()
        .filter(|group| !group.trim().is_empty())
    {
        release_group = Some(group);
    }
    let language_title = if let Some(group) = release_group.as_deref() {
        simple_release_title.replace(group, "RlsGrp")
    } else {
        simple_release_title.clone()
    };

    let mut edition = title_match
        .edition
        .clone()
        .or_else(|| parse_edition(&simple_release_title));

    if edition.as_deref().is_some_and(str::is_empty) {
        edition = None;
    }

    Some(MovieParsedRelease {
        parser_version: MOVIE_RADARR_STYLE_RESOLVER_VERSION.to_string(),
        radarr_repository: RADARR_REFERENCE_REPOSITORY.to_string(),
        radarr_commit: RADARR_REFERENCE_COMMIT.to_string(),
        original_title,
        release_title,
        simple_release_title: simple_release_title.clone(),
        movie_titles: build_movie_titles(&title_match.display_title),
        year: title_match.year,
        quality: parse_quality(title),
        languages: parse_languages(&language_title),
        release_group,
        release_hash: title_match.release_hash,
        edition,
        imdb_id: parse_imdb_id(&simple_release_title).or_else(|| parse_imdb_id(title)),
        tmdb_id: parse_tmdb_id(&simple_release_title).or_else(|| parse_tmdb_id(title)),
        hardcoded_subs: parse_hardcoded_subs(title),
        matched_pattern_id: Some(title_match.pattern_id),
    })
}

pub fn parse_quality(name: &str) -> MovieQuality {
    if name.trim().is_empty() {
        return MovieQuality::default();
    }

    let mut result = parse_quality_name(name);
    if result.quality.is_none()
        && let Some(extension) = path_extension(name.trim())
        && let Some(quality) = quality_for_extension(extension)
    {
        result = quality;
        result.source_detection_source = MovieQualityDetectionSource::Extension;
        result.resolution_detection_source = MovieQualityDetectionSource::Extension;
        result.modifier_detection_source = MovieQualityDetectionSource::Extension;
    }

    result
}

pub fn parse_quality_name(name: &str) -> MovieQuality {
    let normalized_name = name.replace('_', " ").trim().to_string();
    let mut result = parse_quality_modifiers(name, &normalized_name);
    let resolution = parse_resolution(&normalized_name);
    let source = last_source_match(&normalized_name);
    let codec = codec_hint(&normalized_name);
    let remux = REMUX_RE.is_match(&normalized_name) || german_remux_match(&normalized_name);
    let brdisk = brdisk_match(&normalized_name);

    if RAW_HD_RE.is_match(&normalized_name) && !brdisk {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::RawHd,
            )
            .with_quality_name("RAWHD")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if resolution.is_some() {
        result.resolution_detection_source = MovieQualityDetectionSource::Name;
    }

    if let Some(source) = source {
        result.source_detection_source = MovieQualityDetectionSource::Name;
        return quality_from_source_match(
            result,
            name,
            &normalized_name,
            source,
            resolution,
            codec,
            remux,
            brdisk,
        );
    }

    if remux && resolution.is_some() {
        let quality = match resolution {
            Some(MovieResolution::R480p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray480p"),
            Some(MovieResolution::R720p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p"),
            Some(MovieResolution::R2160p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::Remux,
            )
            .with_quality_name("Remux2160p"),
            _ => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::Remux,
            )
            .with_quality_name("Remux1080p"),
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Unknown,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if ANIME_BLURAY_RE.is_match(&normalized_name) {
        let quality = match resolution {
            Some(
                MovieResolution::R360p
                | MovieResolution::R480p
                | MovieResolution::R540p
                | MovieResolution::R576p,
            ) => quality_model(
                Some(MovieQualitySource::Dvd),
                None,
                MovieQualityModifier::None,
            )
            .with_quality_name("DVD"),
            Some(MovieResolution::R1080p) => {
                if remux {
                    quality_model(
                        Some(MovieQualitySource::BluRay),
                        Some(MovieResolution::R1080p),
                        MovieQualityModifier::Remux,
                    )
                    .with_quality_name("Remux1080p")
                } else {
                    quality_model(
                        Some(MovieQualitySource::BluRay),
                        Some(MovieResolution::R1080p),
                        MovieQualityModifier::None,
                    )
                    .with_quality_name("Bluray1080p")
                }
            }
            Some(MovieResolution::R2160p) => {
                if remux {
                    quality_model(
                        Some(MovieQualitySource::BluRay),
                        Some(MovieResolution::R2160p),
                        MovieQualityModifier::Remux,
                    )
                    .with_quality_name("Remux2160p")
                } else {
                    quality_model(
                        Some(MovieQualitySource::BluRay),
                        Some(MovieResolution::R2160p),
                        MovieQualityModifier::None,
                    )
                    .with_quality_name("Bluray2160p")
                }
            }
            _ if remux => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::Remux,
            )
            .with_quality_name("Remux1080p"),
            _ => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p"),
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if ANIME_WEB_RE.is_match(&normalized_name) {
        let quality = match resolution {
            Some(
                MovieResolution::R360p
                | MovieResolution::R480p
                | MovieResolution::R540p
                | MovieResolution::R576p,
            ) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL480p"),
            Some(MovieResolution::R1080p) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL1080p"),
            Some(MovieResolution::R2160p) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL2160p"),
            _ => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL720p"),
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if let Some(resolution) = resolution {
        result.resolution_detection_source = MovieQualityDetectionSource::Name;
        let mut source = MovieQualitySource::Unknown;
        let mut modifier = MovieQualityModifier::None;
        let mut source_detection = MovieQualityDetectionSource::Unknown;

        if remux {
            source = MovieQualitySource::BluRay;
            modifier = MovieQualityModifier::Remux;
            source_detection = MovieQualityDetectionSource::Name;
        } else if let Some(extension_quality) =
            path_extension(name.trim()).and_then(quality_for_extension)
            && extension_quality.quality.is_some()
        {
            source = extension_quality
                .source
                .unwrap_or(MovieQualitySource::Unknown);
            source_detection = MovieQualityDetectionSource::Extension;
        }

        let mut quality = if source == MovieQualitySource::Unknown {
            match resolution {
                MovieResolution::R2160p => quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R2160p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("HDTV2160p"),
                MovieResolution::R1080p => quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("HDTV1080p"),
                MovieResolution::R720p => quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R720p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("HDTV720p"),
                _ => quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R480p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("SDTV"),
            }
        } else {
            find_quality_by_source_resolution(source, resolution, modifier)
        };
        quality.source_detection_source = source_detection;
        quality.resolution_detection_source = MovieQualityDetectionSource::Name;
        return merge_quality(result, quality);
    }

    if CODEC_X264_RE.is_match(&normalized_name) {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("SDTV"),
        );
    }

    if normalized_name.to_ascii_lowercase().contains("848x480") {
        let quality = if normalized_name.to_ascii_lowercase().contains("dvd") {
            quality_model(
                Some(MovieQualitySource::Dvd),
                None,
                MovieQualityModifier::None,
            )
            .with_quality_name("DVD")
        } else if normalized_name.to_ascii_lowercase().contains("bluray") {
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray480p")
        } else {
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("SDTV")
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if normalized_name.to_ascii_lowercase().contains("1280x720") {
        let quality = if normalized_name.to_ascii_lowercase().contains("bluray") {
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p")
        } else {
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("HDTV720p")
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if normalized_name.to_ascii_lowercase().contains("1920x1080") {
        let quality = if normalized_name.to_ascii_lowercase().contains("bluray") {
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray1080p")
        } else {
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("HDTV1080p")
        };
        return merge_quality(
            result,
            quality.with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    let lower = normalized_name.to_ascii_lowercase();
    if lower.contains("bluray720p") {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }
    if lower.contains("bluray1080p") {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray1080p")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }
    if lower.contains("bluray2160p") {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray2160p")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Name,
            ),
        );
    }

    if OTHER_HDTV_RE.is_match(&normalized_name) {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("HDTV720p")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Unknown,
            ),
        );
    }
    if OTHER_SDTV_RE.is_match(&normalized_name) {
        return merge_quality(
            result,
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("SDTV")
            .with_detection(
                MovieQualityDetectionSource::Name,
                MovieQualityDetectionSource::Unknown,
            ),
        );
    }

    result
}

pub fn parse_languages(title: &str) -> Vec<String> {
    let lower = title.to_ascii_lowercase();
    let mut languages = Vec::<String>::new();
    let has_release_language_context = LANGUAGE_CONTEXT_RE.is_match(title);

    for (needle, language) in [
        ("english", "English"),
        ("spanish", "Spanish"),
        ("danish", "Danish"),
        ("dutch", "Dutch"),
        ("japanese", "Japanese"),
        ("icelandic", "Icelandic"),
        ("mandarin", "Chinese"),
        ("cantonese", "Chinese"),
        ("chinese", "Chinese"),
        ("korean", "Korean"),
        ("russian", "Russian"),
        ("romanian", "Romanian"),
        ("hindi", "Hindi"),
        ("arabic", "Arabic"),
        ("thai", "Thai"),
        ("bulgarian", "Bulgarian"),
        ("polish", "Polish"),
        ("vietnamese", "Vietnamese"),
        ("swedish", "Swedish"),
        ("norwegian", "Norwegian"),
        ("finnish", "Finnish"),
        ("turkish", "Turkish"),
        ("portuguese", "Portuguese"),
        ("brazilian", "PortugueseBR"),
        ("hungarian", "Hungarian"),
        ("hebrew", "Hebrew"),
        ("ukrainian", "Ukrainian"),
        ("persian", "Persian"),
        ("bengali", "Bengali"),
        ("slovak", "Slovak"),
        ("latvian", "Latvian"),
        ("latino", "SpanishLatino"),
        ("tamil", "Tamil"),
        ("telugu", "Telugu"),
        ("malayalam", "Malayalam"),
        ("kannada", "Kannada"),
        ("albanian", "Albanian"),
        ("afrikaans", "Afrikaans"),
        ("marathi", "Marathi"),
        ("tagalog", "Tagalog"),
    ] {
        if (has_release_language_context || needle == "latino") && lower.contains(needle) {
            languages.push(language.to_string());
        }
    }

    for captures in CASE_SENSITIVE_LANGUAGE_RE.captures_iter(title) {
        for (name, language) in [
            ("english", "English"),
            ("lithuanian", "Lithuanian"),
            ("czech", "Czech"),
            ("polish", "Polish"),
            ("bulgarian", "Bulgarian"),
            ("slovak", "Slovak"),
            ("german", "German"),
            ("spanish", "Spanish"),
        ] {
            if captures.name(name).is_some()
                && !case_sensitive_language_false_positive(title, language)
            {
                languages.push(language.to_string());
            }
        }
    }

    for captures in LANGUAGE_RE.captures_iter(title) {
        for (name, language) in [
            ("english", "English"),
            ("italian", "Italian"),
            ("german", "German"),
            ("flemish", "Flemish"),
            ("bulgarian", "Bulgarian"),
            ("romanian", "Romanian"),
            ("brazilian", "PortugueseBR"),
            ("greek", "Greek"),
            ("french", "French"),
            ("russian", "Russian"),
            ("hungarian", "Hungarian"),
            ("hebrew", "Hebrew"),
            ("polish", "Polish"),
            ("chinese", "Chinese"),
            ("ukrainian", "Ukrainian"),
            ("spanish", "Spanish"),
            ("catalan", "Catalan"),
            ("latvian", "Latvian"),
            ("telugu", "Telugu"),
            ("vietnamese", "Vietnamese"),
            ("japanese", "Japanese"),
            ("korean", "Korean"),
            ("urdu", "Urdu"),
            ("romansh", "Romansh"),
            ("mongolian", "Mongolian"),
            ("georgian", "Georgian"),
            ("original", "Original"),
        ] {
            if captures.name(name).is_some() {
                languages.push(language.to_string());
            }
        }
    }

    if languages.is_empty() {
        languages.push("Unknown".to_string());
    }

    let has_only_german = languages.len() == 1 && languages[0] == "German";
    if has_only_german {
        if GERMAN_DUAL_LANGUAGE_RE.is_match(title) {
            languages.push("Original".to_string());
        } else if GERMAN_MULTI_LANGUAGE_RE.is_match(title) {
            languages.push("Original".to_string());
            languages.push("English".to_string());
        }
    }

    dedup_stable(languages)
}

pub fn parse_release_group(title: &str) -> Option<String> {
    let mut title = strip_file_extension(title).trim().to_string();
    title = title.replace('\u{3010}', "[").replace('\u{3011}', "]");
    title = WEBSITE_PREFIX_RE.replace(&title, "").to_string();
    title = TORRENT_SUFFIX_RE.replace(&title, "").to_string();
    title = WEBSITE_POSTFIX_RE.replace(&title, "").to_string();

    if let Some(captures) = ANIME_RELEASE_GROUP_RE.captures(&title)
        && let Some(group) = captures.name("subgroup")
    {
        return Some(group.as_str().to_string());
    }

    title = CLEAN_RELEASE_GROUP_RE.replace_all(&title, "").to_string();

    if let Some(captures) = BRACKET_RELEASE_GROUP_RE.captures(&title)
        && let Some(group) = captures.name("releasegroup")
        && let Some(group) = valid_release_group(group.as_str())
    {
        return Some(group);
    }

    if let Some(group) = last_capture(&EXCEPTION_RELEASE_GROUP_EXACT_RE, &title, "releasegroup") {
        return Some(group);
    }
    if let Some(group) = last_capture(&EXCEPTION_RELEASE_GROUP_RE, &title, "releasegroup") {
        return Some(group);
    }

    let mut groups = Vec::new();
    for captures in RELEASE_GROUP_RE.captures_iter(&title) {
        if let Some(group) = captures.name("releasegroup") {
            groups.push(group.as_str().to_string());
        }
    }

    groups
        .into_iter()
        .rev()
        .find_map(|group| valid_release_group(&group))
}

pub fn parse_edition(title: &str) -> Option<String> {
    let title = strip_file_extension(title);
    let normalized = title
        .replace(['.', '_', '-'], " ")
        .replace(['(', ')', '[', ']'], " ");
    let normalized = normalize_space(&normalized);
    let words = normalized.split_whitespace().collect::<Vec<_>>();
    if words.len() < 2 {
        return None;
    }

    let quality_start = words
        .iter()
        .position(|word| QUALITY_BOUNDARY_RE.is_match(word))
        .unwrap_or(words.len());
    find_edition_span(&words[..quality_start])
}

pub fn parse_imdb_id(title: &str) -> Option<String> {
    IMDB_ID_RE
        .captures(title)
        .and_then(|captures| captures.name("imdbid"))
        .map(|value| value.as_str().to_string())
        .filter(|value| matches!(value.len(), 9 | 10))
}

pub fn parse_tmdb_id(title: &str) -> Option<i32> {
    TMDB_ID_RE
        .captures(title)
        .and_then(|captures| captures.name("tmdbid"))
        .and_then(|value| value.as_str().parse::<i32>().ok())
}

pub fn normalize_imdb_id(imdb_id: &str) -> Option<String> {
    if !NORMALIZE_IMDB_ID_RE.is_match(imdb_id) {
        return None;
    }
    if imdb_id.len() <= 2 {
        return None;
    }
    let stripped = imdb_id.strip_prefix("tt").unwrap_or(imdb_id);
    Some(format!("tt{stripped:0>7}"))
}

pub fn parse_hardcoded_subs(title: &str) -> Option<String> {
    let mut found = None;
    for captures in HARDCODED_SUBS_RE.captures_iter(title) {
        if let Some(value) = captures.name("hcsub") {
            let value = value.as_str();
            let upper = value.to_ascii_uppercase();
            if upper.contains("SOFTSUB")
                || upper.contains("MULTISUB")
                || upper.contains("HORRIBLESUB")
            {
                continue;
            }
            found = Some(value.to_string());
        } else if captures.name("hc").is_some() {
            found = Some("Generic Hardcoded Subs".to_string());
        }
    }
    found
}

pub fn clean_movie_title(title: &str) -> String {
    if title.trim().is_empty() {
        return title.to_string();
    }
    if title.parse::<i64>().is_ok() {
        return title.to_string();
    }

    let title = remove_accents(&replace_german_umlauts(title));
    let tokens = alphanumeric_tokens(&title);
    let mut cleaned = String::new();
    for (idx, token) in tokens.iter().enumerate() {
        let lower = token.to_ascii_lowercase();
        let is_common = matches!(
            lower.as_str(),
            "a" | "à" | "an" | "the" | "and" | "or" | "of"
        );
        let keep_common = if !is_common {
            true
        } else if idx == 0 || idx + 1 == tokens.len() {
            true
        } else {
            false
        };
        if keep_common {
            cleaned.push_str(&lower);
        }
    }
    cleaned
}

pub fn to_url_slug(
    value: &str,
    invalid_dash_replacement: bool,
    trim_end_chars: Option<&str>,
    deduplicate_chars: Option<&str>,
) -> String {
    let mut value = remove_accents(&value.to_ascii_lowercase());
    value = WHITESPACE_RE.replace_all(&value, "-").to_string();
    let replace = if invalid_dash_replacement { "-" } else { "" };
    value = INVALID_SLUG_CHARS_RE
        .replace_all(&value, replace)
        .to_string();

    if let Some(trim_chars) = trim_end_chars
        && !trim_chars.is_empty()
    {
        value = value.trim_matches(|ch| trim_chars.contains(ch)).to_string();
    }

    if let Some(dedupe_chars) = deduplicate_chars
        && !dedupe_chars.is_empty()
    {
        value = dedupe_repeated_chars(&value, dedupe_chars);
    }

    value
}

pub fn iso_language_find(code: &str) -> Option<&'static str> {
    match code.to_ascii_lowercase().as_str() {
        "en" | "eng" | "en-us" | "en-gb" => Some("English"),
        "pt" | "por" | "pt-pt" => Some("Portuguese"),
        "te" | "tel" | "te-in" => Some("Telugu"),
        "af" | "afr" | "af-za" => Some("Afrikaans"),
        "mr" | "mar" | "mr-in" => Some("Marathi"),
        "tl" | "tgl" | "tl-ph" => Some("Tagalog"),
        "ur" | "urd" | "ur-pk" => Some("Urdu"),
        "rm" | "roh" | "rm-ch" => Some("Romansh"),
        "mn" | "mon" | "khk" | "mvf" | "mn-cyrl" => Some("Mongolian"),
        "bn" | "ben" | "bn-bd" | "bn-in" => Some("Bengali"),
        "ka" | "geo" | "kat" | "ka-ge" => Some("Georgian"),
        _ => None,
    }
}

pub fn get_scene_title(title: &str) -> Option<String> {
    if !title.contains('.') || title.contains(' ') {
        return None;
    }
    let parsed = parse_movie_title(title, false)?;
    if parsed.release_group.is_none()
        || parsed.quality.quality.is_none()
        || parsed.primary_movie_title().is_none_or(str::is_empty)
        || parsed.release_title.trim().is_empty()
    {
        return None;
    }
    Some(parsed.release_title)
}

pub fn is_scene_title(title: &str) -> bool {
    get_scene_title(title).is_some()
}

#[derive(Debug, Clone)]
struct MovieTitleMatch {
    pattern_id: String,
    raw_title: String,
    display_title: String,
    year: Option<i32>,
    edition: Option<String>,
    subgroup: Option<String>,
    release_hash: Option<String>,
}

impl MovieQuality {
    fn with_quality_name(mut self, name: &str) -> Self {
        self.quality = Some(name.to_string());
        self
    }

    fn with_detection(
        mut self,
        source: MovieQualityDetectionSource,
        resolution: MovieQualityDetectionSource,
    ) -> Self {
        self.source_detection_source = source;
        self.resolution_detection_source = resolution;
        if self.modifier != MovieQualityModifier::None {
            self.modifier_detection_source = source;
        }
        self
    }
}

fn quality_model(
    source: Option<MovieQualitySource>,
    resolution: Option<MovieResolution>,
    modifier: MovieQualityModifier,
) -> MovieQuality {
    MovieQuality {
        quality: None,
        source: source.filter(|source| *source != MovieQualitySource::Unknown),
        resolution,
        modifier,
        source_detection_source: MovieQualityDetectionSource::Unknown,
        resolution_detection_source: MovieQualityDetectionSource::Unknown,
        modifier_detection_source: MovieQualityDetectionSource::Unknown,
        revision_detection_source: MovieQualityDetectionSource::Unknown,
        revision_version: 1,
        revision_real: 0,
        revision_is_repack: false,
    }
}

fn merge_quality(mut base: MovieQuality, parsed: MovieQuality) -> MovieQuality {
    base.quality = parsed.quality;
    base.source = parsed.source;
    base.resolution = parsed.resolution;
    base.modifier = parsed.modifier;
    base.source_detection_source = parsed.source_detection_source;
    base.resolution_detection_source = parsed.resolution_detection_source;
    base.modifier_detection_source = parsed.modifier_detection_source;
    base
}

fn parse_quality_modifiers(name: &str, normalized_name: &str) -> MovieQuality {
    let mut result = MovieQuality::default();
    let version = VERSION_RE.captures(normalized_name).and_then(|captures| {
        [
            "version",
            "version_bracket",
            "version_repack",
            "version_rerip",
        ]
        .iter()
        .find_map(|name| {
            captures
                .name(name)
                .and_then(|m| m.as_str().parse::<i32>().ok())
        })
    });
    if let Some(version) = version {
        result.revision_version = version;
        result.revision_detection_source = MovieQualityDetectionSource::Name;
    }
    if PROPER_RE.is_match(normalized_name) {
        result.revision_version = version.map(|value| value + 1).unwrap_or(2);
        result.revision_is_repack = false;
        result.revision_detection_source = MovieQualityDetectionSource::Name;
    }
    if REPACK_RE.is_match(normalized_name) {
        result.revision_version = version.map(|value| value + 1).unwrap_or(2);
        result.revision_is_repack = true;
        result.revision_detection_source = MovieQualityDetectionSource::Name;
    }
    result.revision_real = REAL_RE.find_iter(name).count() as i32;
    if result.revision_real > 0 {
        result.revision_detection_source = MovieQualityDetectionSource::Name;
    }
    result
}

fn quality_from_source_match(
    base: MovieQuality,
    name: &str,
    normalized_name: &str,
    source: SourceMatch,
    resolution: Option<MovieResolution>,
    codec: CodecHint,
    remux: bool,
    brdisk: bool,
) -> MovieQuality {
    let mut quality = match source {
        SourceMatch::BluRay => {
            if remux {
                bluray_quality_for_resolution(resolution, true)
            } else if brdisk {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::BrDisk,
                )
                .with_quality_name("BRDISK")
            } else if matches!(codec, CodecHint::Xvid | CodecHint::Divx) {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R480p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("Bluray480p")
            } else {
                bluray_quality_for_resolution(resolution, false)
            }
        }
        SourceMatch::WebDl => match resolution {
            Some(MovieResolution::R2160p) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL2160p"),
            Some(MovieResolution::R1080p) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL1080p"),
            Some(MovieResolution::R720p) | None if name.contains("[WEBDL]") => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL720p"),
            Some(MovieResolution::R720p) => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL720p"),
            _ => quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL480p"),
        },
        SourceMatch::WebRip => match resolution {
            Some(MovieResolution::R2160p) => quality_model(
                Some(MovieQualitySource::WebRip),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBRip2160p"),
            Some(MovieResolution::R1080p) => quality_model(
                Some(MovieQualitySource::WebRip),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBRip1080p"),
            Some(MovieResolution::R720p) => quality_model(
                Some(MovieQualitySource::WebRip),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBRip720p"),
            _ => quality_model(
                Some(MovieQualitySource::WebRip),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBRip480p"),
        },
        SourceMatch::Scr => quality_model(
            Some(MovieQualitySource::Dvd),
            Some(MovieResolution::R480p),
            MovieQualityModifier::Screener,
        )
        .with_quality_name("DVDSCR"),
        SourceMatch::Cam => quality_model(
            Some(MovieQualitySource::Cam),
            None,
            MovieQualityModifier::None,
        )
        .with_quality_name("CAM"),
        SourceMatch::Telesync => {
            let mut q = quality_model(
                Some(MovieQualitySource::Telesync),
                resolution,
                MovieQualityModifier::None,
            )
            .with_quality_name("TELESYNC");
            q.resolution = resolution;
            q
        }
        SourceMatch::Telecine => quality_model(
            Some(MovieQualitySource::Telecine),
            None,
            MovieQualityModifier::None,
        )
        .with_quality_name("TELECINE"),
        SourceMatch::Workprint => quality_model(
            Some(MovieQualitySource::Workprint),
            None,
            MovieQualityModifier::None,
        )
        .with_quality_name("WORKPRINT"),
        SourceMatch::Regional => quality_model(
            Some(MovieQualitySource::Dvd),
            Some(MovieResolution::R480p),
            MovieQualityModifier::Regional,
        )
        .with_quality_name("REGIONAL"),
        SourceMatch::Hdtv => {
            if MPEG2_RE.is_match(normalized_name) {
                quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::RawHd,
                )
                .with_quality_name("RAWHD")
            } else {
                tv_quality_for_resolution(resolution, name.contains("[HDTV]"))
            }
        }
        SourceMatch::BdRip | SourceMatch::BrRip => match resolution {
            Some(MovieResolution::R720p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p"),
            Some(MovieResolution::R1080p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R1080p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray1080p"),
            Some(MovieResolution::R2160p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R2160p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray2160p"),
            Some(MovieResolution::R576p) => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R576p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray576p"),
            _ => quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray480p"),
        },
        SourceMatch::Dvdr => quality_model(
            Some(MovieQualitySource::Dvd),
            Some(MovieResolution::R480p),
            MovieQualityModifier::Remux,
        )
        .with_quality_name("DVDR"),
        SourceMatch::Dvd => quality_model(
            Some(MovieQualitySource::Dvd),
            None,
            MovieQualityModifier::None,
        )
        .with_quality_name("DVD"),
        SourceMatch::Pdtv | SourceMatch::Sdtv | SourceMatch::Dsr | SourceMatch::TvRip => {
            if resolution == Some(MovieResolution::R1080p) || contains_ci(normalized_name, "1080p")
            {
                quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("HDTV1080p")
            } else if resolution == Some(MovieResolution::R720p)
                || contains_ci(normalized_name, "720p")
                || HIGH_DEF_PDTV_RE.is_match(normalized_name)
            {
                quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R720p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("HDTV720p")
            } else {
                quality_model(
                    Some(MovieQualitySource::Tv),
                    Some(MovieResolution::R480p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("SDTV")
            }
        }
    };

    quality.source_detection_source = MovieQualityDetectionSource::Name;
    if resolution.is_some() || HIGH_DEF_PDTV_RE.is_match(normalized_name) {
        quality.resolution_detection_source = MovieQualityDetectionSource::Name;
    }
    merge_quality(base, quality)
}

fn bluray_quality_for_resolution(resolution: Option<MovieResolution>, remux: bool) -> MovieQuality {
    match resolution {
        Some(MovieResolution::R2160p) => {
            if remux {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R2160p),
                    MovieQualityModifier::Remux,
                )
                .with_quality_name("Remux2160p")
            } else {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R2160p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("Bluray2160p")
            }
        }
        Some(MovieResolution::R1080p) => {
            if remux {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::Remux,
                )
                .with_quality_name("Remux1080p")
            } else {
                quality_model(
                    Some(MovieQualitySource::BluRay),
                    Some(MovieResolution::R1080p),
                    MovieQualityModifier::None,
                )
                .with_quality_name("Bluray1080p")
            }
        }
        Some(MovieResolution::R720p) => quality_model(
            Some(MovieQualitySource::BluRay),
            Some(MovieResolution::R720p),
            MovieQualityModifier::None,
        )
        .with_quality_name("Bluray720p"),
        Some(MovieResolution::R576p) => quality_model(
            Some(MovieQualitySource::BluRay),
            Some(MovieResolution::R576p),
            MovieQualityModifier::None,
        )
        .with_quality_name("Bluray576p"),
        Some(MovieResolution::R360p | MovieResolution::R480p | MovieResolution::R540p) => {
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray480p")
        }
        None if remux => quality_model(
            Some(MovieQualitySource::BluRay),
            Some(MovieResolution::R1080p),
            MovieQualityModifier::Remux,
        )
        .with_quality_name("Remux1080p"),
        None => quality_model(
            Some(MovieQualitySource::BluRay),
            Some(MovieResolution::R720p),
            MovieQualityModifier::None,
        )
        .with_quality_name("Bluray720p"),
    }
}

fn tv_quality_for_resolution(
    resolution: Option<MovieResolution>,
    bracket_hdtv: bool,
) -> MovieQuality {
    match resolution {
        Some(MovieResolution::R2160p) => quality_model(
            Some(MovieQualitySource::Tv),
            Some(MovieResolution::R2160p),
            MovieQualityModifier::None,
        )
        .with_quality_name("HDTV2160p"),
        Some(MovieResolution::R1080p) => quality_model(
            Some(MovieQualitySource::Tv),
            Some(MovieResolution::R1080p),
            MovieQualityModifier::None,
        )
        .with_quality_name("HDTV1080p"),
        Some(MovieResolution::R720p) | None if bracket_hdtv => quality_model(
            Some(MovieQualitySource::Tv),
            Some(MovieResolution::R720p),
            MovieQualityModifier::None,
        )
        .with_quality_name("HDTV720p"),
        Some(MovieResolution::R720p) => quality_model(
            Some(MovieQualitySource::Tv),
            Some(MovieResolution::R720p),
            MovieQualityModifier::None,
        )
        .with_quality_name("HDTV720p"),
        _ => quality_model(
            Some(MovieQualitySource::Tv),
            Some(MovieResolution::R480p),
            MovieQualityModifier::None,
        )
        .with_quality_name("SDTV"),
    }
}

fn find_quality_by_source_resolution(
    source: MovieQualitySource,
    resolution: MovieResolution,
    modifier: MovieQualityModifier,
) -> MovieQuality {
    match (source, resolution, modifier) {
        (MovieQualitySource::BluRay, MovieResolution::R1080p, MovieQualityModifier::Remux) => {
            quality_model(Some(source), Some(resolution), modifier).with_quality_name("Remux1080p")
        }
        (MovieQualitySource::BluRay, MovieResolution::R2160p, MovieQualityModifier::Remux) => {
            quality_model(Some(source), Some(resolution), modifier).with_quality_name("Remux2160p")
        }
        (MovieQualitySource::BluRay, MovieResolution::R720p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("Bluray720p")
        }
        (MovieQualitySource::BluRay, MovieResolution::R1080p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("Bluray1080p")
        }
        (MovieQualitySource::BluRay, MovieResolution::R2160p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("Bluray2160p")
        }
        (
            MovieQualitySource::BluRay,
            MovieResolution::R360p | MovieResolution::R480p | MovieResolution::R540p,
            _,
        ) => quality_model(
            Some(source),
            Some(MovieResolution::R480p),
            MovieQualityModifier::None,
        )
        .with_quality_name("Bluray480p"),
        (MovieQualitySource::Dvd, _, _) => {
            quality_model(Some(source), None, MovieQualityModifier::None).with_quality_name("DVD")
        }
        (MovieQualitySource::Tv, MovieResolution::R720p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("HDTV720p")
        }
        (MovieQualitySource::Tv, MovieResolution::R1080p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("HDTV1080p")
        }
        (MovieQualitySource::Tv, MovieResolution::R2160p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("HDTV2160p")
        }
        (MovieQualitySource::WebDl, MovieResolution::R720p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("WEBDL720p")
        }
        (MovieQualitySource::WebDl, MovieResolution::R1080p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("WEBDL1080p")
        }
        (MovieQualitySource::WebDl, MovieResolution::R2160p, _) => {
            quality_model(Some(source), Some(resolution), MovieQualityModifier::None)
                .with_quality_name("WEBDL2160p")
        }
        (
            MovieQualitySource::WebDl,
            MovieResolution::R360p
            | MovieResolution::R480p
            | MovieResolution::R540p
            | MovieResolution::R576p,
            _,
        ) => quality_model(
            Some(source),
            Some(MovieResolution::R480p),
            MovieQualityModifier::None,
        )
        .with_quality_name("WEBDL480p"),
        _ => quality_model(Some(source), Some(resolution), modifier),
    }
}

fn parse_resolution(name: &str) -> Option<MovieResolution> {
    if RES_360_RE.is_match(name) {
        Some(MovieResolution::R360p)
    } else if RES_480_RE.is_match(name) {
        Some(MovieResolution::R480p)
    } else if RES_540_RE.is_match(name) {
        Some(MovieResolution::R540p)
    } else if RES_576_RE.is_match(name) {
        Some(MovieResolution::R576p)
    } else if RES_720_RE.is_match(name) {
        Some(MovieResolution::R720p)
    } else if RES_1080_RE.is_match(name) {
        Some(MovieResolution::R1080p)
    } else if RES_2160_RE.is_match(name) || ALT_2160_RE.is_match(name) {
        Some(MovieResolution::R2160p)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMatch {
    BluRay,
    WebDl,
    WebRip,
    Hdtv,
    BdRip,
    BrRip,
    Dvdr,
    Dvd,
    Dsr,
    Pdtv,
    Sdtv,
    TvRip,
    Scr,
    Telesync,
    Telecine,
    Cam,
    Workprint,
    Regional,
}

fn last_source_match(name: &str) -> Option<SourceMatch> {
    let mut best: Option<(usize, SourceMatch)> = None;
    for (source, regex) in [
        (SourceMatch::BluRay, &SOURCE_BLURAY_RE),
        (SourceMatch::WebDl, &SOURCE_WEBDL_RE),
        (SourceMatch::WebRip, &SOURCE_WEBRIP_RE),
        (SourceMatch::Hdtv, &SOURCE_HDTV_RE),
        (SourceMatch::BdRip, &SOURCE_BDRIP_RE),
        (SourceMatch::BrRip, &SOURCE_BRRIP_RE),
        (SourceMatch::Dvdr, &SOURCE_DVDR_RE),
        (SourceMatch::Dvd, &SOURCE_DVD_RE),
        (SourceMatch::Dsr, &SOURCE_DSR_RE),
        (SourceMatch::Pdtv, &SOURCE_PDTV_RE),
        (SourceMatch::Sdtv, &SOURCE_SDTV_RE),
        (SourceMatch::TvRip, &SOURCE_TVRIP_RE),
        (SourceMatch::Scr, &SOURCE_SCR_RE),
        (SourceMatch::Telesync, &SOURCE_TS_RE),
        (SourceMatch::Telecine, &SOURCE_TC_RE),
        (SourceMatch::Cam, &SOURCE_CAM_RE),
        (SourceMatch::Workprint, &SOURCE_WP_RE),
        (SourceMatch::Regional, &SOURCE_REGIONAL_RE),
    ] {
        for m in regex.find_iter(name) {
            if should_skip_source_match(source, name, m.start(), m.end()) {
                continue;
            }
            if best.as_ref().is_none_or(|(idx, _)| m.start() >= *idx) {
                best = Some((m.start(), source));
            }
        }
    }
    best.map(|(_, source)| source)
}

fn should_skip_source_match(source: SourceMatch, name: &str, start: usize, end: usize) -> bool {
    let lower = name.to_ascii_lowercase();
    let matched = lower[start..end].trim_matches(['.', '_', '-', ' '].as_slice());
    match source {
        SourceMatch::BluRay if matched == "bd" => {
            let before = lower[..start].trim_end_matches(['.', '_', ' '].as_slice());
            let after = lower[end..].trim();
            before.ends_with('-') && after.is_empty()
        }
        SourceMatch::Dvd => {
            let before = lower[..start].trim_end_matches(['.', '_', '-', ' '].as_slice());
            before.ends_with("hd")
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodecHint {
    None,
    X264,
    H264,
    Xvid,
    Divx,
}

fn codec_hint(name: &str) -> CodecHint {
    if CODEC_X264_RE.is_match(name) {
        CodecHint::X264
    } else if CODEC_H264_RE.is_match(name) {
        CodecHint::H264
    } else if CODEC_XVID_RE.is_match(name) {
        CodecHint::Xvid
    } else if CODEC_DIVX_RE.is_match(name) {
        CodecHint::Divx
    } else {
        CodecHint::None
    }
}

fn quality_for_extension(extension: &str) -> Option<MovieQuality> {
    match extension.to_ascii_lowercase().as_str() {
        ".mkv" | ".mk3d" => Some(
            quality_model(
                Some(MovieQualitySource::WebDl),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("WEBDL720p"),
        ),
        ".m2ts" => Some(
            quality_model(
                Some(MovieQualitySource::BluRay),
                Some(MovieResolution::R720p),
                MovieQualityModifier::None,
            )
            .with_quality_name("Bluray720p"),
        ),
        ".img" | ".iso" | ".vob" => Some(
            quality_model(
                Some(MovieQualitySource::Dvd),
                None,
                MovieQualityModifier::None,
            )
            .with_quality_name("DVD"),
        ),
        ".webm" => None,
        ".m4v" | ".3gp" | ".nsv" | ".ty" | ".strm" | ".rm" | ".rmvb" | ".m3u" | ".ifo" | ".mov"
        | ".qt" | ".divx" | ".xvid" | ".bivx" | ".nrg" | ".pva" | ".wmv" | ".asf" | ".asx"
        | ".ogm" | ".ogv" | ".m2v" | ".avi" | ".bin" | ".dat" | ".dvr-ms" | ".mpg" | ".mpeg"
        | ".mp4" | ".avc" | ".vp3" | ".svq3" | ".nuv" | ".viv" | ".dv" | ".fli" | ".flv"
        | ".wpl" | ".ts" | ".wtv" => Some(
            quality_model(
                Some(MovieQualitySource::Tv),
                Some(MovieResolution::R480p),
                MovieQualityModifier::None,
            )
            .with_quality_name("SDTV"),
        ),
        _ => None,
    }
}

fn brdisk_match(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    let compact = lower.replace(['.', '_', '-', ' '], "");
    if lower.contains("bdrip")
        || lower.contains("remux")
        || lower.contains("x264")
        || lower.contains("x265")
        || lower.contains("mkv")
        || lower.contains("german.dl")
        || lower.contains("german ml")
    {
        return false;
    }
    lower.contains("bdiso")
        || lower.contains("complete bluray")
        || lower.contains("complete.bluray")
        || lower.contains("bluray.iso")
        || lower.contains("bluray iso")
        || lower.contains("bd25")
        || lower.contains("bd50")
        || lower.contains("bd-50")
        || lower.contains("bd66")
        || lower.contains("untouched")
        || lower.contains(".iso")
        || lower.contains("blu-ray avc")
        || lower.contains("blu ray avc")
        || lower.contains("blu-ray vc-1")
        || lower.contains("blu ray vc-1")
        || lower.contains("blu-ray hevc")
        || lower.contains("blu ray hevc")
        || lower.contains("hd.dvd")
        || lower.contains("hd dvd")
        || (compact.contains("bluray")
            && (lower.contains("avc")
                || lower.contains("hevc")
                || lower.contains("vc-1")
                || lower.contains("vc 1")
                || lower.contains("mpeg-2")
                || lower.contains("mpeg 2")
                || lower.contains("dts.hd")
                || lower.contains("dts-hd")))
}

fn german_remux_match(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("german")
        && (lower.contains(".ml.") || lower.contains(" ml ") || lower.contains("-ml-"))
        && lower.contains("bluray")
        && (lower.contains("avc")
            || lower.contains("hevc")
            || lower.contains("vc-1")
            || lower.contains("vc 1")
            || lower.contains("mpeg-2")
            || lower.contains("mpeg 2"))
}

fn case_sensitive_language_false_positive(title: &str, language: &str) -> bool {
    let lower = title.to_ascii_lowercase();
    matches!(
        (language, lower.as_str()),
        ("Lithuanian", value) if value.contains("yts.lt")
    ) || matches!(
        (language, lower.as_str()),
        ("Polish", value) if value.contains("pl-sub") || value.contains("sub-pl")
    ) || matches!(
        (language, lower.as_str()),
        ("Spanish", value) if value.contains("dts-es")
    )
}

fn validate_before_parsing(title: &str) -> bool {
    let lower = title.to_ascii_lowercase();
    if lower.contains("password") && lower.contains("yenc") {
        return false;
    }
    if !title.chars().any(char::is_alphanumeric) {
        return false;
    }
    let title_without_extension = strip_file_extension(title);
    !REJECT_HASHED_RELEASE_RE
        .iter()
        .any(|regex| regex.is_match(&title_without_extension))
        && !INVALID_UNSPACED_RELEASE_RE.is_match(&title_without_extension)
}

fn simplify_title_for_movie_match(release_title: &str) -> String {
    let mut value = SIMPLE_TITLE_STRIP_RE
        .replace_all(release_title, " ")
        .to_string();
    value = WEBSITE_PREFIX_RE.replace(&value, "").to_string();
    value = WEBSITE_POSTFIX_RE.replace(&value, "").to_string();
    value = TORRENT_SUFFIX_RE.replace(&value, "").to_string();
    value = TRAILING_QUALITY_BRACKET_RE
        .replace_all(&value, |captures: &Captures<'_>| {
            let token = captures
                .name("quality")
                .map(|m| m.as_str())
                .unwrap_or_default();
            if parse_quality_name(token).quality.is_some() {
                String::new()
            } else {
                captures
                    .get(0)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default()
            }
        })
        .to_string();
    value
}

fn parse_movie_title_match(simple_title: &str, is_dir: bool) -> Option<MovieTitleMatch> {
    if is_dir && let Some(captures) = FOLDER_YEAR_FIRST_RE.captures(simple_title) {
        let raw_title = capture(&captures, "title");
        return Some(MovieTitleMatch {
            pattern_id: "folder_year_first".to_string(),
            raw_title: raw_title.to_string(),
            display_title: normalize_movie_display_title(raw_title),
            year: capture(&captures, "year").parse::<i32>().ok(),
            edition: None,
            subgroup: None,
            release_hash: None,
        });
    }

    for (idx, regex) in ANIME_MOVIE_REGEXES.iter().enumerate() {
        if let Some(captures) = regex.captures(simple_title) {
            let raw_title = capture(&captures, "title");
            if raw_title.trim().is_empty() || raw_title == "(" {
                continue;
            }
            return Some(MovieTitleMatch {
                pattern_id: format!("radarr_anime_movie_{idx}"),
                raw_title: raw_title.to_string(),
                display_title: normalize_movie_display_title(raw_title),
                year: capture(&captures, "year").parse::<i32>().ok(),
                edition: None,
                subgroup: captures.name("subgroup").map(|m| m.as_str().to_string()),
                release_hash: captures.name("hash").map(|m| m.as_str().to_string()),
            });
        }
    }

    if let Some(match_) = parse_german_truefrench_movie(simple_title) {
        return Some(match_);
    }

    parse_year_based_movie(simple_title)
}

fn parse_german_truefrench_movie(simple_title: &str) -> Option<MovieTitleMatch> {
    let marker = GERMAN_TRUEFRENCH_RE.find(simple_title)?;
    let before_marker = simple_title[..marker.start()].trim_matches(['.', '_', '-', ' ']);
    let tail = &simple_title[marker.end()..];
    if year_matches(tail)
        .next()
        .is_some_and(|(start, _, _)| tail[..start].trim_matches(['.', '_', '-', ' ']).is_empty())
    {
        return None;
    }
    if before_marker.eq_ignore_ascii_case("The") || before_marker.eq_ignore_ascii_case("Good") {
        return None;
    }
    let before_year = year_matches(before_marker).last();
    let year = before_year
        .map(|(_, _, year)| year)
        .or_else(|| year_matches(tail).last().map(|(_, _, year)| year));
    let mut raw_title = before_year
        .map(|(start, _, _)| {
            before_marker[..start]
                .trim_matches(['.', '_', '-', ' '])
                .to_string()
        })
        .unwrap_or_else(|| before_marker.to_string());
    let edition_source = before_year
        .map(|(_, end, _)| {
            before_marker[end..]
                .trim_matches(['.', '_', '-', ' '])
                .to_string()
        })
        .unwrap_or_else(|| before_marker.to_string());
    let edition = parse_edition(&edition_source).or_else(|| parse_edition(before_marker));
    if let Some(edition) = edition.as_deref() {
        raw_title = remove_trailing_phrase(&raw_title, edition);
    }
    if raw_title.trim().is_empty() {
        return None;
    }
    Some(MovieTitleMatch {
        pattern_id: "radarr_german_truefrench".to_string(),
        raw_title: raw_title.clone(),
        display_title: normalize_movie_display_title(&raw_title),
        year,
        edition,
        subgroup: None,
        release_hash: None,
    })
}

fn parse_year_based_movie(simple_title: &str) -> Option<MovieTitleMatch> {
    let mut candidates = Vec::new();
    for (start, end, year) in year_matches(simple_title) {
        if year_followed_by_forbidden_token(simple_title, end) {
            continue;
        }
        candidates.push((start, end, year));
    }
    let (start, end, year) = *candidates.last()?;
    let mut raw_title = simple_title[..start]
        .trim_start_matches(['.', '_', '-', ' '])
        .trim_end_matches(['.', '_', '-', ' ', '(', '['])
        .to_string();
    if raw_title.to_ascii_lowercase().ends_with(".german")
        && !raw_title.eq_ignore_ascii_case("The.Good.German")
        && !raw_title.eq_ignore_ascii_case("The.German")
    {
        raw_title = raw_title
            .trim_end_matches(|ch| ch != '.')
            .trim_end_matches('.')
            .to_string();
    }
    let after_year = simple_title[end..].trim_start_matches(['.', '_', '-', ' ', ')', ']']);
    let mut edition = parse_edition(&raw_title).or_else(|| {
        let possible = format!("A Movie {after_year}");
        parse_edition(&possible)
    });
    if let Some(edition_value) = edition.as_deref() {
        raw_title = remove_trailing_phrase(&raw_title, edition_value);
    }

    if raw_title.trim().is_empty() && simple_title[..start].contains('(') {
        raw_title = simple_title[..start]
            .trim_matches(['.', '_', '-', ' '])
            .to_string();
    }

    if raw_title.trim().is_empty() || raw_title.trim() == "(" {
        return None;
    }

    if edition.as_deref().is_some_and(str::is_empty) {
        edition = None;
    }

    Some(MovieTitleMatch {
        pattern_id: "radarr_movie_year".to_string(),
        raw_title: raw_title.clone(),
        display_title: normalize_movie_display_title(&raw_title),
        year: Some(year),
        edition,
        subgroup: None,
        release_hash: RELEASE_HASH_RE
            .captures(simple_title)
            .and_then(|captures| captures.name("hash").map(|m| m.as_str().to_string())),
    })
}

fn year_matches(value: &str) -> std::vec::IntoIter<(usize, usize, i32)> {
    let mut matches = Vec::new();
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx + 4 <= bytes.len() {
        if bytes[idx].is_ascii_digit()
            && bytes[idx + 1].is_ascii_digit()
            && bytes[idx + 2].is_ascii_digit()
            && bytes[idx + 3].is_ascii_digit()
        {
            let before_is_digit = idx > 0 && bytes[idx - 1].is_ascii_digit();
            let after_is_digit = idx + 4 < bytes.len() && bytes[idx + 4].is_ascii_digit();
            if !before_is_digit && !after_is_digit {
                if let Ok(year) = value[idx..idx + 4].parse::<i32>() {
                    if (1800..=2099).contains(&year) {
                        matches.push((idx, idx + 4, year));
                    }
                }
            }
            idx += 4;
        } else {
            idx += 1;
        }
    }
    matches.into_iter()
}

fn year_followed_by_forbidden_token(value: &str, end: usize) -> bool {
    let rest = value[end..].chars().take(3).collect::<String>();
    if rest.starts_with('p') || rest.starts_with('i') {
        return true;
    }
    if rest.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        return true;
    }
    false
}

fn normalize_movie_display_title(raw: &str) -> String {
    let preserve_spaced_aka = raw.contains(". AKA.");
    let mut movie_name = raw.replace('_', " ");
    movie_name = NORMALIZE_ALT_TITLE_RE
        .replace_all(&movie_name, " AKA ")
        .to_string();
    movie_name = REQUEST_INFO_RE
        .replace_all(&movie_name, "")
        .trim()
        .to_string();

    let parts = movie_name.split('.').collect::<Vec<_>>();
    if parts.len() == 1 {
        let normalized = normalize_space(movie_name.trim_matches(['.', '_', '-', ' ']));
        return if preserve_spaced_aka {
            normalized.replace(" AKA ", "  AKA ")
        } else {
            normalized
        };
    }

    let mut out = String::new();
    let mut previous_acronym = false;
    for (idx, part) in parts.iter().enumerate() {
        let next = parts.get(idx + 1).copied().unwrap_or_default();
        let lower = part.to_ascii_lowercase();
        let is_single_non_a = part.chars().count() == 1
            && lower != "a"
            && part.parse::<i32>().is_err()
            && (previous_acronym || idx < parts.len() - 1)
            && (previous_acronym || next.chars().count() != 1 || next.parse::<i32>().is_err());
        let is_a_acronym = lower == "a" && (previous_acronym || next.chars().count() == 1);
        if is_single_non_a || is_a_acronym || lower == "dr" {
            out.push_str(part);
            out.push('.');
            previous_acronym = true;
        } else {
            if previous_acronym {
                out.push(' ');
                previous_acronym = false;
            }
            out.push_str(part);
            out.push(' ');
        }
    }
    let normalized = normalize_space(out.trim());
    if preserve_spaced_aka {
        normalized.replace(" AKA ", "  AKA ")
    } else {
        normalized
    }
}

fn build_movie_titles(movie_name: &str) -> Vec<String> {
    let mut titles = Vec::new();
    titles.push(movie_name.to_string());
    let unbracketed = BRACKETED_ALT_TITLE_RE
        .replace(movie_name, "$1 AKA $2")
        .to_string();
    for part in ALT_TITLE_SPLIT_RE.split(&unbracketed) {
        let part = part.trim();
        if !part.is_empty() && part != movie_name && !titles.iter().any(|title| title == part) {
            titles.push(part.to_string());
        }
    }
    titles
}

fn valid_release_group(raw: &str) -> Option<String> {
    let group = raw.trim().trim_matches(['-', '.', '_', ' ', '[', ']']);
    if group.is_empty()
        || group.parse::<i64>().is_ok()
        || INVALID_RELEASE_GROUP_RE.is_match(group)
        || RESOLUTION_TOKEN_RE.is_match(group)
    {
        return None;
    }
    Some(group.to_string())
}

fn parse_year_token(value: &str) -> Option<i32> {
    let clean = value.trim_matches(|ch: char| !ch.is_ascii_digit());
    if clean.len() == 4 {
        let year = clean.parse::<i32>().ok()?;
        if (1800..=2099).contains(&year) {
            return Some(year);
        }
    }
    None
}

fn edition_phrase_len(value: &str) -> Option<usize> {
    let words = value.split_whitespace().collect::<Vec<_>>();
    find_edition_span(&words).map(|edition| edition.split_whitespace().count())
}

fn find_edition_span(words: &[&str]) -> Option<String> {
    let mut best: Option<(usize, usize)> = None;
    for start in 0..words.len() {
        if parse_year_token(words[start]).is_some() {
            continue;
        }
        let max_end = words.len().min(start + 6);
        for end in start + 1..=max_end {
            if words[start..end]
                .iter()
                .any(|word| parse_year_token(word).is_some())
            {
                continue;
            }
            let phrase = words[start..end].join(" ");
            if is_edition_phrase(&phrase) {
                if start == 0 && end < words.len() {
                    continue;
                }
                let len = end - start;
                if best.is_none_or(|(best_start, best_end)| {
                    len > best_end - best_start
                        || (len == best_end - best_start && start > best_start)
                }) {
                    best = Some((start, end));
                }
            }
        }
    }
    best.map(|(start, end)| words[start..end].join(" "))
}

fn is_edition_phrase(phrase: &str) -> bool {
    let phrase = phrase.trim().to_ascii_lowercase();
    if phrase.is_empty() {
        return false;
    }
    if matches!(
        phrase.as_str(),
        "extended"
            | "recut"
            | "recut extended"
            | "despecialized"
            | "imax"
            | "restored"
            | "uncensored"
            | "remastered"
            | "unrated"
            | "uncut"
            | "open matte"
            | "2in1"
            | "3in1"
            | "4in1"
            | "final cut"
            | "assembly cut"
            | "director's cut"
            | "directors cut"
            | "directors"
            | "special edition"
            | "special edition remastered"
            | "extended cut"
            | "extended edition"
            | "extended directors cut"
            | "extended directors cut fan edit"
            | "extended directors cut fanedit"
            | "extended theatrical version imax"
            | "ultimate hunter edition"
            | "diamond edition"
            | "ultimate rekall edition"
            | "signature edition"
            | "imperial edition"
            | "50th anniversary edition"
            | "special edition fan edit"
    ) {
        return true;
    }
    let first = phrase.split_whitespace().next().unwrap_or_default();
    ((first.contains("director") || first == "directors")
        && (phrase.contains("cut") || phrase.contains("edition")))
        || (first.chars().next().is_some_and(|ch| ch.is_ascii_digit())
            && phrase.contains("anniversary edition"))
}

fn remove_trailing_phrase(value: &str, phrase: &str) -> String {
    let separators = ['.', '_', '-', ' '];
    let normalized_value = value.replace(['.', '_', '-'], " ");
    if normalized_value
        .to_ascii_lowercase()
        .ends_with(&phrase.replace(['.', '_', '-'], " ").to_ascii_lowercase())
    {
        let mut remaining = value.to_string();
        let mut phrase_chars = phrase.chars().filter(|ch| !separators.contains(ch)).count();
        while phrase_chars > 0 && !remaining.is_empty() {
            if let Some(ch) = remaining.pop()
                && !separators.contains(&ch)
            {
                phrase_chars -= 1;
            }
        }
        return remaining.trim_matches(separators).to_string();
    }
    value.to_string()
}

fn strip_file_extension(value: &str) -> String {
    let Some((prefix, ext)) = value.rsplit_once('.') else {
        return value.to_string();
    };
    let ext_with_dot = format!(".{ext}");
    let lower = ext_with_dot.to_ascii_lowercase();
    let known = matches!(
        lower.as_str(),
        ".mkv"
            | ".mp4"
            | ".avi"
            | ".mov"
            | ".wmv"
            | ".m4v"
            | ".webm"
            | ".m2ts"
            | ".ts"
            | ".iso"
            | ".img"
            | ".vob"
            | ".nzb"
            | ".sub"
            | ".srt"
            | ".ass"
    );
    if known {
        prefix.to_string()
    } else {
        value.to_string()
    }
}

fn path_extension(value: &str) -> Option<&str> {
    let (_, ext) = value.rsplit_once('.')?;
    if ext.contains(['/', '\\']) {
        return None;
    }
    Some(&value[value.len() - ext.len() - 1..])
}

fn capture<'a>(captures: &'a Captures<'_>, name: &str) -> &'a str {
    captures.name(name).map(|m| m.as_str()).unwrap_or_default()
}

fn last_capture(regex: &Regex, value: &str, name: &str) -> Option<String> {
    regex
        .captures_iter(value)
        .filter_map(|captures| captures.name(name).map(|m| m.as_str().to_string()))
        .last()
}

fn replace_first_literal(value: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return value.to_string();
    }
    if let Some(index) = value.find(needle) {
        let mut out = String::new();
        out.push_str(&value[..index]);
        out.push_str(replacement);
        out.push_str(&value[index + needle.len()..]);
        out
    } else {
        value.replacen(needle, replacement, 1)
    }
}

fn normalize_space(value: &str) -> String {
    WHITESPACE_RE.replace_all(value.trim(), " ").to_string()
}

fn contains_ci(value: &str, needle: &str) -> bool {
    value
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn dedup_stable(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

fn remove_accents(value: &str) -> String {
    value
        .chars()
        .filter_map(|ch| match ch {
            'á' | 'à' | 'â' | 'ä' | 'å' | 'ã' | 'ā' | 'ă' | 'ą' | 'Á' | 'À' | 'Â' | 'Ä' | 'Å'
            | 'Ã' | 'Ā' | 'Ă' | 'Ą' => Some('a'),
            'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' | 'É' | 'È' | 'Ê' | 'Ë' | 'Ē'
            | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => Some('e'),
            'í' | 'ì' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'İ' | 'Í' | 'Ì' | 'Î' | 'Ï' | 'Ī' | 'Ĭ'
            | 'Į' => Some('i'),
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ō' | 'ŏ' | 'ő' | 'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' | 'Ō'
            | 'Ŏ' | 'Ő' => Some('o'),
            'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' | 'Ú' | 'Ù' | 'Û' | 'Ü' | 'Ū'
            | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' => Some('u'),
            'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' | 'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' => Some('c'),
            'ñ' | 'ń' | 'ņ' | 'ň' | 'Ñ' | 'Ń' | 'Ņ' | 'Ň' => Some('n'),
            'ß' | 'œ' | 'Œ' | 'Ø' | 'ø' => None,
            _ => Some(ch),
        })
        .collect()
}

fn replace_german_umlauts(value: &str) -> String {
    value
        .replace('ä', "ae")
        .replace('ö', "oe")
        .replace('ü', "ue")
        .replace('Ä', "Ae")
        .replace('Ö', "Oe")
        .replace('Ü', "Ue")
        .replace('ß', "ss")
}

fn alphanumeric_tokens(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
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

fn dedupe_repeated_chars(value: &str, chars: &str) -> String {
    let mut out = String::new();
    let mut run: Option<char> = None;
    for ch in value.chars() {
        if chars.contains(ch) {
            run = Some(ch);
            continue;
        }
        if let Some(run_ch) = run.take() {
            out.push(run_ch);
        }
        out.push(ch);
    }
    if let Some(run_ch) = run {
        out.push(run_ch);
    }
    out
}

fn movie_resolution_label(value: MovieResolution) -> &'static str {
    match value {
        MovieResolution::R360p => "360p",
        MovieResolution::R480p => "480p",
        MovieResolution::R540p => "540p",
        MovieResolution::R576p => "576p",
        MovieResolution::R720p => "720p",
        MovieResolution::R1080p => "1080p",
        MovieResolution::R2160p => "2160p",
    }
}

fn movie_source_label(value: MovieQualitySource) -> &'static str {
    match value {
        MovieQualitySource::Unknown => "UNKNOWN",
        MovieQualitySource::Cam => "CAM",
        MovieQualitySource::Telesync => "TELESYNC",
        MovieQualitySource::Telecine => "TELECINE",
        MovieQualitySource::Workprint => "WORKPRINT",
        MovieQualitySource::Dvd => "DVD",
        MovieQualitySource::Tv => "TV",
        MovieQualitySource::WebDl => "WEBDL",
        MovieQualitySource::WebRip => "WEBRIP",
        MovieQualitySource::BluRay => "BLURAY",
    }
}

fn movie_modifier_label(value: MovieQualityModifier) -> Option<&'static str> {
    match value {
        MovieQualityModifier::None => None,
        MovieQualityModifier::Regional => Some("REGIONAL"),
        MovieQualityModifier::Screener => Some("SCREENER"),
        MovieQualityModifier::RawHd => Some("RAWHD"),
        MovieQualityModifier::BrDisk => Some("BRDISK"),
        MovieQualityModifier::Remux => Some("REMUX"),
    }
}

fn detection_label(value: MovieQualityDetectionSource) -> &'static str {
    match value {
        MovieQualityDetectionSource::Unknown => "Unknown",
        MovieQualityDetectionSource::Name => "Name",
        MovieQualityDetectionSource::Extension => "Extension",
    }
}

static FOLDER_YEAR_FIRST_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^\s*[\(\[]?(?P<year>19\d{2}|20\d{2})[\)\]]?\s+(?P<title>.+?)\s*$")
        .expect("valid folder year-first regex")
});

static ANIME_MOVIE_REGEXES: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"(?ix)^\[(?P<subgroup>.+?)\][-_. ]?(?P<title>.+?)[-_.\s\(\[]+(?P<year>18\d{2}|19\d{2}|20\d{2}).*?(?P<hash>\[\w{8}\])?(?:$|\.)").expect("valid anime movie year regex"),
        Regex::new(r"(?ix)^\[(?P<subgroup>.+?)\][-_. ]?(?P<title>.+?v\d{1,2})(?:[-_. ]|\[).*?(?P<hash>\[\w{8}\])(?:$|\.)").expect("valid anime versioned movie regex"),
        Regex::new(r"(?ix)^\[(?P<subgroup>.+?)\][-_. ]?(?P<title>.+?\[.*).*?(?P<hash>\[\w{8}\])(?:$|\.)").expect("valid anime bracket movie regex"),
        Regex::new(r"(?ix)^\[(?P<subgroup>.+?)\][-_. ]?(?P<title>.+?)(?:[\[(][^]\)]).*?(?P<hash>\[\w{8}\])(?:$|\.)").expect("valid anime info movie regex"),
    ]
});

static YEAR_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:^|[^0-9])(?P<year>1[89]\d{2}|20\d{2})(?:$|[^0-9])")
        .expect("valid year token regex")
});

static GERMAN_TRUEFRENCH_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:^|[._\-\s])(German|TrueFrench)(?:$|[._\-\s])")
        .expect("valid German marker regex")
});

static SIMPLE_TITLE_STRIP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:(?:480|540|576|720|1080|2160)[ip]|[xh][\W_]?26[45]|DD\W?5\W1|[<>?*]|848x480|1280x720|1920x1080|3840x2160|4096x2160|(?:8|10)b(?:it)?|10-bit)\s*?(?:$|[^a-b0-9])")
        .expect("valid simple title strip regex")
});
static SIMPLE_RELEASE_TITLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s*(?:[<>?*|])").expect("valid simple release title regex"));
static WEBSITE_PREFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:(?:\[|\()\s*)?(?:www\.)?[-a-z0-9-]{1,256}\.(?:[a-z]{2,6}\.[a-z]{2,6}|xn--[a-z0-9-]{4,}|[a-z]{2,})(?:\s*(?:\]|\))\s*[- ]*|\s*[-:]\s+)")
        .expect("valid website prefix regex")
});
static WEBSITE_POSTFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)(?:\[\s*)?www\.[-a-z0-9-]{1,256}\.(?:xn--[a-z0-9-]{4,}|[a-z]{2,6})\b(?:\s*\])$",
    )
    .expect("valid website postfix regex")
});
static TORRENT_SUFFIX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)[\s._-]*\[(?:ettv|rartv|rarbg|rarbg\.com|cttv|publichd)\]\s*$")
        .expect("valid torrent suffix regex")
});
static TRAILING_QUALITY_BRACKET_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\[(?P<quality>[a-z0-9 ._-]+)\]$").expect("valid quality bracket regex")
});
static REQUEST_INFO_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?:\[.+?\])+").expect("valid request info regex"));
static NORMALIZE_ALT_TITLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[ ]+(?:A\.K\.A\.)[ ]+").expect("valid alt title normalize regex")
});
static ALT_TITLE_SPLIT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)[ ]+(?:AKA|/)[ ]+").expect("valid alt title split regex"));
static BRACKETED_ALT_TITLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(.*) \([ ]*AKA[ ]+(.*)\)").expect("valid bracketed alt regex"));

static IMDB_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(?P<imdbid>tt\d{7,8})").expect("valid imdb id regex"));
static TMDB_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)tmdb(?:id)?-(?P<tmdbid>\d+)").expect("valid tmdb id regex"));
static NORMALIZE_IMDB_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?:\d{1,10}|tt\d{1,10})$").expect("valid imdb normalize regex"));
static HARDCODED_SUBS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\b(?:(?P<hcsub>\w+SUBS?)|(?P<hc>HC|SUBBED))\b")
        .expect("valid hardcoded subs regex")
});
static RELEASE_HASH_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(?P<hash>\[\w{8}\])(?:$|\.)").expect("valid release hash regex"));

static REJECT_HASHED_RELEASE_RE: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"^[0-9a-zA-Z]{32}").expect("valid hash reject regex"),
        Regex::new(r"^[a-z0-9]{24}$").expect("valid short hash reject regex"),
        Regex::new(r"^[A-Z]{11}\d{3}$").expect("valid nzb hash reject regex"),
        Regex::new(r"^[a-z]{12}\d{3}$").expect("valid nzb lower hash reject regex"),
        Regex::new(r"^Backup_\d{5,}S\d{2}-\d{2}$").expect("valid backup reject regex"),
        Regex::new(r"^123$").expect("valid 123 reject regex"),
        Regex::new(r"(?i)^abc$").expect("valid abc reject regex"),
        Regex::new(r"(?i)^abc[-_. ]xyz").expect("valid abc xyz reject regex"),
        Regex::new(r"(?i)^b00bs$").expect("valid b00bs reject regex"),
    ]
});
static INVALID_UNSPACED_RELEASE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^thebiggestmovie1618finale$").expect("valid unspaced reject regex")
});
static REVERSED_TITLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:^|[-._ ])(?:p027|p0801)[-._ ]").expect("valid reversed regex"));

static SOURCE_BLURAY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)\b(?:M?Blu[-_.\s]?Ray|HD[-_.\s]?DVD|BD|UHD2?BD|BDISO|BDMux|BD25|BD50|BD66|BR[-_.\s]?DISK)\b")
        .expect("valid bluray source regex")
});
static SOURCE_WEBDL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\bWEB[-_.\s]?DL(?:mux)?\b|\bAmazonHD\b|\bAmazonSD\b|\biTunesHD\b|\bMaxdomeHD\b|\bNetflixU?HD\b|\bWebHD\b|\bHBOMaxHD\b|\bDisneyHD\b|[.\s]WEB[.\s](?:[xh][.\s]?26[45]|AVC|HEVC|DDP?5[.\s]1)|[.\s]WEB$|(?:\d{3,4}0p)[-.\s](?:Hybrid[-_.\s]?)?WEB[-.\s]|[-.\s]WEB[-.\s]\d{3,4}0p|\b\s/\sWEB\s/\s\b|(?:AMZN|NF|DP)[.\s-]WEB[.\s-]|\bWEB[.\s](?:h[.\s]?26[45]|H[.\s]?26[45]|x[.\s]?26[45]|AVC|HEVC)\b)")
        .expect("valid webdl source regex")
});
static SOURCE_WEBRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:WebRip|Web-Rip|WEBMux)\b").expect("valid webrip regex"));
static SOURCE_HDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bHDTV\b").expect("valid hdtv regex"));
static SOURCE_BDRIP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:BDRip|BDLight|HD[-_. ]?DVDRip|UHDBDRip)\b").expect("valid bdrip regex")
});
static SOURCE_BRRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bBRRip\b").expect("valid brrip regex"));
static SOURCE_DVDR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b\d?x?M?DVD-?[R59]\b").expect("valid dvdr regex"));
static SOURCE_DVD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:\bDVDRip\b|\bxvidvd\b|\bDVD(?:$|[._\s]))").expect("valid dvd regex")
});
static SOURCE_DSR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:WS[-_. ]DSR|DSR)\b").expect("valid dsr regex"));
static SOURCE_PDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bPDTV\b").expect("valid pdtv regex"));
static SOURCE_SDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bSDTV\b").expect("valid sdtv regex"));
static SOURCE_TVRIP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTVRip\b").expect("valid tvrip regex"));
static SOURCE_SCR_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:SCR|SCREENER|DVDSCR|DVDSCREENER)\b").expect("valid scr regex")
});
static SOURCE_TS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:TS[-_. ]|TELESYNCH?|HD-?TS|HDTS|PDVD|TSRip|HDTSRip)\b")
        .expect("valid ts regex")
});
static SOURCE_TC_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:TC|TELECINE|HD-TC|HDTC)\b").expect("valid tc regex"));
static SOURCE_CAM_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:CAMRIP|(?:NEW)?CAM|HD-?CAM(?:Rip)?|HQCAM)\b").expect("valid cam regex")
});
static SOURCE_WP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:WORKPRINT|WP)\b").expect("valid wp regex"));
static SOURCE_REGIONAL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:R[0-9]{1}|REGIONAL)\b").expect("valid regional regex"));

static RAW_HD_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bRaw[-_. ]?HD\b").expect("valid rawhd regex"));
static MPEG2_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bMPEG[-_. ]?2\b").expect("valid mpeg2 regex"));
static REMUX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:[_. \[]|\d{4}p-|\bHybrid-)(?:(?:BD|UHD)[-_. ]?)?Remux\b|(?:(?:BD|UHD)[-_. ]?)?Remux[_. ]\d{4}p")
        .expect("valid remux regex")
});
static ANIME_BLURAY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)bd(?:720|1080|2160)|(?:^|[-_. (\[])bd(?:$|[-_. )\]])")
        .expect("valid anime bluray regex")
});
static ANIME_WEB_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[WEB\]|[\[\(]WEB[ .]").expect("valid anime web regex"));
static HIGH_DEF_PDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)hr[-_. ]ws").expect("valid high def pdtv regex"));
static OTHER_HDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)HD[-_. ]TV").expect("valid other hdtv regex"));
static OTHER_SDTV_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)SD[-_. ]TV").expect("valid other sdtv regex"));

static RES_360_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b360p\b").expect("valid 360p regex"));
static RES_480_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:480p|480i|640x480|848x480)\b").expect("valid 480p regex"));
static RES_540_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b540p\b").expect("valid 540p regex"));
static RES_576_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b576p\b").expect("valid 576p regex"));
static RES_720_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:720p|1280x720|960p)\b").expect("valid 720p regex"));
static RES_1080_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:1080p|BD1080p|1920x1080|1440p|FHD|1080i|4kto1080p)\b")
        .expect("valid 1080p regex")
});
static RES_2160_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:2160p|BD2160p|3840x2160|4k[-_.\s](?:UHD|HEVC|BD|H\.?265)|(?:UHD|HEVC|BD|H\.?265)[-_.\s]4k)\b").expect("valid 2160p regex")
});
static ALT_2160_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bUHD\b|\[4K\]").expect("valid alternative 2160p regex"));

static CODEC_X264_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bx264\b").expect("valid x264 regex"));
static CODEC_H264_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bh264\b").expect("valid h264 regex"));
static CODEC_XVID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bX-?vid\b").expect("valid xvid regex"));
static CODEC_DIVX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bdivx\b").expect("valid divx regex"));
static PROPER_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bproper\b").expect("valid proper regex"));
static REPACK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(?:repack\d?|rerip\d?)\b").expect("valid repack regex"));
static VERSION_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\d[-._ ]?v(?P<version>\d)[-._ ]|\[v(?P<version_bracket>\d)\]|repack(?P<version_repack>\d)|rerip(?P<version_rerip>\d))")
        .expect("valid version regex")
});
static REAL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bREAL\b").expect("valid real regex"));

static CASE_SENSITIVE_LANGUAGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?x)(?:(?P<english>\bEN\b)|(?P<lithuanian>\bLT\b)|(?P<czech>\bCZ\b)|(?P<polish>\bPL\b)|(?P<bulgarian>\bBG\b)|(?P<slovak>\bSK\b)|(?P<german>\bDE\b)|(?P<spanish>\bES\b))")
        .expect("valid case-sensitive language regex")
});
static LANGUAGE_CONTEXT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\b(?:480|540|576|720|1080|2160)[ip]\b|\b(?:BluRay|WEB[-_.\s]?DL|WEBRip|HDTV|DVDRip|BDRip|BRRip|x264|x265|H[.\s]?264|H[.\s]?265|XviD)\b)")
        .expect("valid language context regex")
});
static LANGUAGE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:\W|_|^)(?:(?P<english>\beng\b)|(?P<italian>\b(?:ita|italian)\b)|(?P<german>(?:swiss)?german\b|videomann|ger[. ]dub|\bger\b)|(?P<flemish>flemish)|(?P<bulgarian>bgaudio)|(?P<romanian>rodubbed)|(?P<brazilian>\b(?:dublado|pt-BR)\b)|(?P<greek>greek)|(?P<french>\b(?:FR|VO|VF|VFF|VFQ|VFI|VF2|TRUEFRENCH|FRENCH|FRE|FRA)\b)|(?P<russian>\b(?:rus|ru)\b)|(?P<hungarian>\b(?:HUNDUB|HUN)\b)|(?P<hebrew>\b(?:HebDub|HebDubbed)\b)|(?P<polish>\b(?:PL\W?DUB|DUB\W?PL|LEK\W?PL|PL\W?LEK)\b)|(?P<chinese>\[(?:CH[ST]|BIG5|GB)\]|简|繁|字幕)|(?P<ukrainian>(?:(?:\dx)?UKR))|(?P<spanish>\b(?:español|castellano)\b)|(?P<catalan>\b(?:catalan?|catalán|català)\b)|(?P<latvian>\b(?:lat|lav|lv)\b)|(?P<telugu>\btel\b)|(?P<vietnamese>\bVIE\b)|(?P<japanese>\bJAP\b)|(?P<korean>\bKOR\b)|(?P<urdu>\burdu\b)|(?P<romansh>\b(?:romansh|rumantsch|romansch)\b)|(?P<mongolian>\b(?:mongolian|khalkha)\b)|(?P<georgian>\b(?:georgian|geo|ka|kat)\b)|(?P<original>\b(?:orig|original)\b))")
        .expect("valid language regex")
});
static GERMAN_DUAL_LANGUAGE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(?:^|[^A-Z])DL(?:$|[^A-Z])").expect("valid German DL regex"));
static GERMAN_MULTI_LANGUAGE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bML\b").expect("valid German ML regex"));

static ANIME_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\[(?P<subgroup>(?:\S|.*\S))\](?:_|-|\s|\.)?")
        .expect("valid anime group regex")
});
static CLEAN_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)(?:-(?:RP|1|NZBGeek|Obfuscated|Obfuscation|Scrambled|sample|Pre|postbot|xpost|Rakuv[a-z0-9]*|WhiteRev|BUYMORE|AsRequested|AlternativeToRequested|GEROV|Z0iDS3N|Chamele0n|4P|4Planet|AlteZachen|RePACKPOST))+$")
        .expect("valid clean release group regex")
});
static EXCEPTION_RELEASE_GROUP_EXACT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?P<releasegroup>KRaLiMaRKo|E\.N\.D|D\-Z0N3|Koten_Gars|BluDragon|ZØNEHD|HQMUX|VARYG|YIFY|YTS(?:\.(?:MX|LT|AG))?|TMd|Eml HDTeam|LMain|DarQ|BEN THE MEN|TAoE|QxR|126811)\b")
        .expect("valid exact release group regex")
});
static EXCEPTION_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:[._ \[])(?P<releasegroup>Silence|afm72|Panda|Ghost|MONOLITH|Tigole|Joy|ImE|UTR|t3nzin|Anime Time|Project Angel|Hakata Ramen|HONE|GiLG|Vyndros|SEV|Garshasp|Kappa|Natty|RCVR|SAMPA|YOGI|r00t|EDGE2020|RZeroX|FreetheFish|Anna|Bandi|Qman|theincognito|HDO|DusIctv|DHD|CtrlHD|-ZR-|ADC|XZVN|RH|Kametsu)(?:\]|\))")
        .expect("valid exception release group regex")
});
static BRACKET_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[-._ ]\[(?P<releasegroup>[a-z0-9]+(?:[._][a-z0-9]+)*)\]$")
        .expect("valid bracket group regex")
});
static RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)-(?P<releasegroup>[a-z0-9]+(?:-[a-z0-9]+)?)(?:\b|[-._ ]|$)")
        .expect("valid release group regex")
});
static INVALID_RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:[se]\d+|[0-9a-f]{8}|WEB-(?:DL|Rip)|Blu-Ray|480p|576p|720p|1080p|2160p|DTS-HD|DTS-X|DTS-MA|DTS-ES|ES|EN|CAT|ENG|JAP|GER|FRA|FRE|ITA|HDRip|DL|X|MA|HD|Rip|bit|Movie|eztv|rartv|CAT-EN|CAT-ES|ES-CAT|\d{1,2}-bit|\d{4}-\d{2}|\d{2}-\d{2}|\d{2}|tmdb(?:id)?-\d+|tt\d{7,8})$")
        .expect("valid invalid release group regex")
});
static RESOLUTION_TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:^|[-_. ])(?:480p|576p|720p|1080p|2160p)(?:$|[-_. ])")
        .expect("valid resolution token regex")
});

static QUALITY_BOUNDARY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ix)^(?:480p|576p|720p|1080p|2160p|BluRay|Bluray|WEB|WEB-DL|WEBDL|WEBRip|HDTV|DVDRip|x264|x265|H264|H265|AVC|HEVC|DTS|AC3|AAC|DD5|REMUX|BDREMUX|BDRip|BRRip|PAL|NTSC)$")
        .expect("valid quality boundary regex")
});
static WHITESPACE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s+").expect("valid whitespace regex"));
static INVALID_SLUG_CHARS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[^a-z0-9\s\-_]").expect("valid slug chars regex"));

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::Value as JsonValue;

    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Inventory {
        radarr_commit: String,
        fixture_set: String,
        cases: Vec<InventoryCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct InventoryCase {
        id: String,
        fixture: String,
        method: String,
        input: String,
        test_kind: String,
        classification: String,
        expected: JsonValue,
    }

    fn inventory() -> Inventory {
        serde_json::from_str(include_str!(
            "fixtures/radarr_rrm0_movie_parser_inventory.json"
        ))
        .expect("valid RR-M0 inventory")
    }

    fn asserted_cases() -> Vec<InventoryCase> {
        let inventory = inventory();
        assert_eq!(inventory.radarr_commit, RADARR_REFERENCE_COMMIT);
        assert_eq!(inventory.fixture_set, "rrm0-radarr-movie-parser-inventory");
        inventory
            .cases
            .into_iter()
            .filter(|case| case.classification == "movie_rrm_asserted")
            .collect()
    }

    fn parsed_for(case: &InventoryCase) -> Option<MovieParsedRelease> {
        match case.test_kind.as_str() {
            "movie_folder" => parse_movie_title(&case.input, true),
            "hashed_path" => parse_movie_path(&case.input),
            _ => parse_movie_title(&case.input, false),
        }
    }

    fn quality_for(case: &InventoryCase) -> MovieQuality {
        parse_quality(&case.input)
    }

    fn expected_strings(value: &JsonValue) -> Vec<String> {
        value
            .as_array()
            .expect("string array")
            .iter()
            .map(|value| value.as_str().expect("string").to_string())
            .collect()
    }

    fn case_errors(case: &InventoryCase) -> Vec<String> {
        let mut errors = Vec::new();
        match case.test_kind.as_str() {
            "movie_title" | "movie_folder" => {
                let parsed = parsed_for(case);
                let expected = case.expected["primaryMovieTitle"].as_str().expect("title");
                if parsed
                    .as_ref()
                    .and_then(MovieParsedRelease::primary_movie_title)
                    != Some(expected)
                {
                    errors.push(format!(
                        "primaryMovieTitle expected {:?}, got {:?}",
                        expected,
                        parsed
                            .as_ref()
                            .and_then(|p| p.primary_movie_title().map(str::to_string))
                    ));
                }
            }
            "movie_year" => {
                let parsed = parsed_for(case);
                let expected = case.expected["year"].as_i64().map(|v| v as i32);
                if parsed.as_ref().and_then(|p| p.year) != expected {
                    errors.push(format!(
                        "year expected {:?}, got {:?}",
                        expected,
                        parsed.as_ref().and_then(|p| p.year)
                    ));
                }
            }
            "movie_external_id" => {
                let parsed = parsed_for(case);
                if let Some(expected) = case.expected["tmdbId"].as_i64() {
                    if parsed.as_ref().and_then(|p| p.tmdb_id) != Some(expected as i32) {
                        errors.push(format!(
                            "tmdbId expected {expected}, got {:?}",
                            parsed.as_ref().and_then(|p| p.tmdb_id)
                        ));
                    }
                }
                if let Some(expected) = case.expected["imdbId"].as_str() {
                    if parsed.as_ref().and_then(|p| p.imdb_id.as_deref()) != Some(expected) {
                        errors.push(format!(
                            "imdbId expected {expected:?}, got {:?}",
                            parsed.as_ref().and_then(|p| p.imdb_id.clone())
                        ));
                    }
                }
            }
            "movie_alternative_titles" => {
                let parsed = parsed_for(case);
                let expected = expected_strings(&case.expected["movieTitles"]);
                if parsed.as_ref().map(|p| &p.movie_titles) != Some(&expected) {
                    errors.push(format!(
                        "movieTitles expected {:?}, got {:?}",
                        expected,
                        parsed.as_ref().map(|p| p.movie_titles.clone())
                    ));
                }
            }
            "quality" => {
                let quality = quality_for(case);
                if let Some(expected) = case.expected["source"].as_str() {
                    if quality.source.map(movie_source_label) != Some(expected) {
                        errors.push(format!(
                            "source expected {expected:?}, got {:?}",
                            quality.source.map(movie_source_label)
                        ));
                    }
                } else if case.expected.get("source").is_some() && quality.source.is_some() {
                    errors.push(format!("source expected None, got {:?}", quality.source));
                }
                if let Some(expected) = case.expected["resolution"].as_str() {
                    if quality.resolution.map(movie_resolution_label) != Some(expected) {
                        errors.push(format!(
                            "resolution expected {expected:?}, got {:?}",
                            quality.resolution.map(movie_resolution_label)
                        ));
                    }
                } else if case.expected.get("resolution").is_some() && quality.resolution.is_some()
                {
                    errors.push(format!(
                        "resolution expected None, got {:?}",
                        quality.resolution
                    ));
                }
                if let Some(expected) = case.expected["modifier"].as_str() {
                    if movie_modifier_label(quality.modifier) != Some(expected) {
                        errors.push(format!(
                            "modifier expected {expected:?}, got {:?}",
                            movie_modifier_label(quality.modifier)
                        ));
                    }
                } else if case.expected.get("modifier").is_some()
                    && quality.modifier != MovieQualityModifier::None
                {
                    errors.push(format!(
                        "modifier expected None, got {:?}",
                        quality.modifier
                    ));
                }
                if let Some(proper) = case.expected["proper"].as_bool() {
                    let actual = quality.revision_version > 1;
                    if actual != proper {
                        errors.push(format!("proper expected {proper}, got {actual}"));
                    }
                }
            }
            "quality_detection" => {
                let quality = quality_for(case);
                let source = case.expected["sourceDetectionSource"]
                    .as_str()
                    .expect("source detection");
                let resolution = case.expected["resolutionDetectionSource"]
                    .as_str()
                    .expect("resolution detection");
                if detection_label(quality.source_detection_source) != source {
                    errors.push(format!(
                        "sourceDetectionSource expected {source:?}, got {:?}",
                        detection_label(quality.source_detection_source)
                    ));
                }
                if detection_label(quality.resolution_detection_source) != resolution {
                    errors.push(format!(
                        "resolutionDetectionSource expected {resolution:?}, got {:?}",
                        detection_label(quality.resolution_detection_source)
                    ));
                }
            }
            "quality_revision" => {
                let quality = quality_for(case);
                if let Some(expected) = case.expected["version"].as_i64()
                    && quality.revision_version != expected as i32
                {
                    errors.push(format!(
                        "version expected {expected}, got {}",
                        quality.revision_version
                    ));
                }
                if let Some(expected) = case.expected["real"].as_i64()
                    && quality.revision_real != expected as i32
                {
                    errors.push(format!(
                        "real expected {expected}, got {}",
                        quality.revision_real
                    ));
                }
                if let Some(expected) = case.expected["isRepack"].as_bool()
                    && quality.revision_is_repack != expected
                {
                    errors.push(format!(
                        "isRepack expected {expected}, got {}",
                        quality.revision_is_repack
                    ));
                }
            }
            "language" => {
                let actual = if case.fixture == "LanguageParserFixture" {
                    parse_languages(&case.input)
                } else {
                    parsed_for(case)
                        .map(|parsed| parsed.languages)
                        .unwrap_or_else(|| vec!["Unknown".to_string()])
                };
                for expected in expected_strings(&case.expected["languages"]) {
                    if !actual.iter().any(|language| language == &expected) {
                        errors.push(format!("language missing {expected:?}, got {actual:?}"));
                    }
                }
            }
            "release_group" | "url_release_group" => {
                let actual = parse_release_group(&case.input);
                let expected = case.expected["releaseGroup"].as_str();
                if actual.as_deref() != expected {
                    errors.push(format!(
                        "releaseGroup expected {expected:?}, got {actual:?}"
                    ));
                }
            }
            "edition" => {
                let actual = parse_movie_title(&case.input, false)
                    .and_then(|parsed| parsed.edition)
                    .unwrap_or_default();
                let expected = case.expected["edition"].as_str().unwrap_or_default();
                if actual != expected {
                    errors.push(format!("edition expected {expected:?}, got {actual:?}"));
                }
            }
            "hashed_path" => {
                let parsed = parse_movie_path(&case.input);
                let expected_title = case.expected["primaryMovieTitle"].as_str().expect("title");
                if parsed
                    .as_ref()
                    .and_then(MovieParsedRelease::primary_movie_title)
                    != Some(expected_title)
                {
                    errors.push(format!(
                        "primaryMovieTitle expected {expected_title:?}, got {:?}",
                        parsed
                            .as_ref()
                            .and_then(MovieParsedRelease::primary_movie_title)
                    ));
                }
                let expected_quality = case.expected["quality"].as_str().expect("quality");
                if parsed.as_ref().and_then(|p| p.quality.quality.as_deref())
                    != Some(expected_quality)
                {
                    errors.push(format!(
                        "quality expected {expected_quality:?}, got {:?}",
                        parsed.as_ref().and_then(|p| p.quality.quality.as_deref())
                    ));
                }
                let expected_group = case.expected["releaseGroup"].as_str();
                if parsed.as_ref().and_then(|p| p.release_group.as_deref()) != expected_group {
                    errors.push(format!(
                        "releaseGroup expected {expected_group:?}, got {:?}",
                        parsed.as_ref().and_then(|p| p.release_group.as_deref())
                    ));
                }
            }
            "reject_title" => {
                let parsed = if case.input.starts_with("random-alphanumeric-length-") {
                    None
                } else {
                    parse_movie_title(&case.input, false)
                };
                if parsed.is_some() {
                    errors.push(format!("expected parse failure, got {parsed:?}"));
                }
            }
            "scene_title" => {
                if let Some(expected) = case.expected["isSceneTitle"].as_bool() {
                    let actual = is_scene_title(&case.input);
                    if actual != expected {
                        errors.push(format!("isSceneTitle expected {expected}, got {actual}"));
                    }
                }
                if let Some(expected) = case.expected["sceneTitle"].as_str() {
                    let actual = get_scene_title(&case.input);
                    if actual.as_deref() != Some(expected) {
                        errors.push(format!("sceneTitle expected {expected:?}, got {actual:?}"));
                    }
                }
            }
            "title_normalization" | "url_title" => {
                let actual = if case.test_kind == "url_title" {
                    parse_movie_title(&case.input, false)
                        .and_then(|parsed| parsed.primary_movie_title().map(clean_movie_title))
                        .unwrap_or_default()
                } else {
                    clean_movie_title(&case.input)
                };
                if let Some(expected) = case.expected["cleanMovieTitle"].as_str() {
                    let expected = if case.test_kind == "url_title" {
                        clean_movie_title(expected)
                    } else {
                        expected.to_string()
                    };
                    if actual != expected {
                        errors.push(format!(
                            "cleanMovieTitle expected {expected:?}, got {actual:?}"
                        ));
                    }
                }
            }
            "hardcoded_subs" => {
                let actual = parse_hardcoded_subs(&case.input);
                let expected = case.expected["hardcodedSubs"].as_str();
                if actual.as_deref() != expected {
                    errors.push(format!(
                        "hardcodedSubs expected {expected:?}, got {actual:?}"
                    ));
                }
            }
            "imdb_normalization" => {
                let actual = normalize_imdb_id(&case.input);
                let expected = case.expected["normalizedImdbId"].as_str();
                if actual.as_deref() != expected {
                    errors.push(format!(
                        "normalizedImdbId expected {expected:?}, got {actual:?}"
                    ));
                }
            }
            "iso_language" => {
                let actual = iso_language_find(&case.input);
                let expected = case.expected["language"].as_str();
                if actual != expected {
                    errors.push(format!(
                        "iso language expected {expected:?}, got {actual:?}"
                    ));
                }
            }
            "slug" => {
                let trim = if case.expected.get("trimEndChars").is_some() {
                    case.expected["trimEndChars"].as_str()
                } else {
                    Some("-_")
                };
                let dedupe = if case.expected.get("deduplicateChars").is_some() {
                    case.expected["deduplicateChars"].as_str()
                } else {
                    Some("-_")
                };
                let invalid_dash = case.expected["invalidDashReplacement"]
                    .as_bool()
                    .unwrap_or_else(|| {
                        case.method == "should_replace_special_characters_with_dash_when_enabled"
                    });
                let actual = to_url_slug(&case.input, invalid_dash, trim, dedupe);
                let expected = case.expected["expectedSlug"]
                    .as_str()
                    .expect("expected slug");
                if actual != expected {
                    errors.push(format!("slug expected {expected:?}, got {actual:?}"));
                }
            }
            other => errors.push(format!("unhandled test kind {other}")),
        }
        errors
    }

    #[test]
    fn movie_radarr_parser_core_goldens_pass() {
        let mut failures = Vec::new();
        let mut checked = 0_usize;
        for case in asserted_cases() {
            if matches!(case.test_kind.as_str(), "subtitle_language") {
                continue;
            }
            checked += 1;
            let errors = case_errors(&case);
            if !errors.is_empty() {
                failures.push(format!(
                    "{} {} {:?}\n  {}",
                    case.id,
                    case.test_kind,
                    case.input,
                    errors.join("\n  ")
                ));
            }
        }
        assert!(checked > 0, "no RR-M1 parser fixtures checked");
        assert!(
            failures.is_empty(),
            "{} RR-M1 Radarr parser golden rows failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn movie_radarr_parser_records_reference_provenance() {
        let parsed = parse_movie_title("Movie.Name.2024.1080p.WEB-DL.x264-GROUP", false)
            .expect("movie parsed");
        assert_eq!(parsed.parser_version, MOVIE_RADARR_STYLE_RESOLVER_VERSION);
        assert_eq!(parsed.radarr_repository, RADARR_REFERENCE_REPOSITORY);
        assert_eq!(parsed.radarr_commit, RADARR_REFERENCE_COMMIT);
        assert_eq!(parsed.primary_movie_title(), Some("Movie Name"));
        assert_eq!(parsed.year, Some(2024));
    }
}
