use std::collections::HashSet;

use serde_json::Value;

use crate::db::models::MediaType;
use crate::drivers::AddMediaRequest;

pub const MANAGED_MEDIA_TAG: &str = "elixir";

pub fn lookup_terms_for_add_request(
    request: &AddMediaRequest,
    implementation: &str,
) -> Vec<String> {
    let mut terms = Vec::new();
    let mut seen = HashSet::new();

    if let Some(ids) = request.external_ids.as_ref() {
        match implementation {
            "radarr" => {
                push_prefixed_lookup_term(&mut terms, &mut seen, "tmdb", ids.tmdb.as_deref());
                push_prefixed_lookup_term(
                    &mut terms,
                    &mut seen,
                    "tvdb",
                    ids.tvdb_movie.as_deref().or(ids.tvdb.as_deref()),
                );
                push_prefixed_lookup_term(&mut terms, &mut seen, "imdb", ids.imdb.as_deref());
            }
            "sonarr" => {
                if request.media_type == MediaType::Movie {
                    push_prefixed_lookup_term(
                        &mut terms,
                        &mut seen,
                        "tvdb",
                        ids.tvdb_movie.as_deref().or(ids.tvdb.as_deref()),
                    );
                } else {
                    push_prefixed_lookup_term(
                        &mut terms,
                        &mut seen,
                        "tvdb",
                        ids.tvdb_series.as_deref().or(ids.tvdb.as_deref()),
                    );
                }
                push_prefixed_lookup_term(&mut terms, &mut seen, "imdb", ids.imdb.as_deref());
                push_prefixed_lookup_term(&mut terms, &mut seen, "tmdb", ids.tmdb.as_deref());
            }
            _ => {}
        }

        push_lookup_term(&mut terms, &mut seen, ids.tvdb_movie.as_deref());
        push_lookup_term(&mut terms, &mut seen, ids.tvdb_series.as_deref());
        push_lookup_term(&mut terms, &mut seen, ids.tvdb.as_deref());
        push_lookup_term(&mut terms, &mut seen, ids.tmdb.as_deref());
        push_lookup_term(&mut terms, &mut seen, ids.imdb.as_deref());
        push_lookup_term(&mut terms, &mut seen, ids.anilist.as_deref());
    }

    if let Some(year) = request.year {
        push_lookup_term(
            &mut terms,
            &mut seen,
            Some(&format!("{} {}", request.title.trim(), year)),
        );
    }
    push_lookup_term(&mut terms, &mut seen, Some(request.title.as_str()));

    terms
}

pub fn select_lookup_item(items: &[Value], request: &AddMediaRequest) -> Option<Value> {
    for value in items {
        if lookup_item_matches(value, request) {
            return Some(value.clone());
        }
    }
    items.first().cloned()
}

fn lookup_item_matches(value: &Value, request: &AddMediaRequest) -> bool {
    let Some(external_ids) = request.external_ids.as_ref() else {
        return lookup_title_year_matches(value, request);
    };

    if let Some(tvdb) = external_ids.tvdb_series.as_deref() {
        if value
            .get("tvdbId")
            .and_then(as_id_string)
            .map(|id| id == tvdb.trim())
            .unwrap_or(false)
        {
            return true;
        }
    }
    if request.media_type == MediaType::Movie {
        if let Some(tvdb) = external_ids
            .tvdb_movie
            .as_deref()
            .or(external_ids.tvdb.as_deref())
            && value
                .get("tvdbId")
                .and_then(as_id_string)
                .map(|id| id == tvdb.trim())
                .unwrap_or(false)
        {
            return true;
        }
    } else if let Some(tvdb) = external_ids.tvdb.as_deref()
        && value
            .get("tvdbId")
            .and_then(as_id_string)
            .map(|id| id == tvdb.trim())
            .unwrap_or(false)
    {
        return true;
    }
    if let Some(tmdb) = external_ids.tmdb.as_deref()
        && value
            .get("tmdbId")
            .and_then(as_id_string)
            .map(|id| id == tmdb.trim())
            .unwrap_or(false)
    {
        return true;
    }
    if let Some(imdb) = external_ids.imdb.as_deref()
        && value
            .get("imdbId")
            .and_then(as_id_string)
            .map(|id| id.eq_ignore_ascii_case(imdb.trim()))
            .unwrap_or(false)
    {
        return true;
    }
    if request.media_type == MediaType::Anime
        && let Some(anilist) = external_ids.anilist.as_deref()
        && value
            .get("tvdbId")
            .and_then(as_id_string)
            .map(|id| id == anilist.trim())
            .unwrap_or(false)
    {
        return true;
    }

    lookup_title_year_matches(value, request)
}

fn lookup_title_year_matches(value: &Value, request: &AddMediaRequest) -> bool {
    let title_match = value
        .get("title")
        .and_then(Value::as_str)
        .map(|value| normalize_name(value) == normalize_name(&request.title))
        .unwrap_or(false);
    if !title_match {
        return false;
    }
    match (
        value
            .get("year")
            .and_then(Value::as_i64)
            .map(|value| value as i32),
        request.year,
    ) {
        (_, None) => true,
        (Some(left), Some(right)) => left == right,
        (None, Some(_)) => true,
    }
}

fn push_lookup_term(terms: &mut Vec<String>, seen: &mut HashSet<String>, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    let normalized = value.to_ascii_lowercase();
    if seen.insert(normalized) {
        terms.push(value.to_string());
    }
}

fn push_prefixed_lookup_term(
    terms: &mut Vec<String>,
    seen: &mut HashSet<String>,
    prefix: &str,
    value: Option<&str>,
) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    let term = format!("{prefix}:{value}");
    push_lookup_term(terms, seen, Some(&term));
}

fn as_id_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn normalize_name(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_', ':'], "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::AddMediaOptions;
    use crate::extensions::ExternalIds;

    #[test]
    fn movie_add_lookup_terms_use_prefixed_external_ids_before_raw_values() {
        let request = AddMediaRequest {
            media_type: MediaType::Movie,
            title: "Scream".to_string(),
            year: Some(1996),
            external_ids: Some(ExternalIds {
                imdb: Some("tt0117571".to_string()),
                tmdb: Some("4232".to_string()),
                ..Default::default()
            }),
            options: AddMediaOptions {
                monitor: true,
                search: false,
                root_folder_path: None,
                quality_profile_id: None,
            },
        };

        let terms = lookup_terms_for_add_request(&request, "radarr");

        assert_eq!(terms[0], "tmdb:4232");
        assert!(terms.iter().any(|term| term == "imdb:tt0117571"));
        assert!(terms.iter().any(|term| term == "Scream 1996"));
        assert!(terms.iter().any(|term| term == "Scream"));
    }

    #[test]
    fn series_add_lookup_terms_use_prefixed_tvdb_before_fallback_title() {
        let request = AddMediaRequest {
            media_type: MediaType::Series,
            title: "Blocked Show".to_string(),
            year: Some(2024),
            external_ids: Some(ExternalIds {
                tvdb_series: Some("321".to_string()),
                tvdb: Some("321".to_string()),
                ..Default::default()
            }),
            options: AddMediaOptions {
                monitor: true,
                search: false,
                root_folder_path: None,
                quality_profile_id: None,
            },
        };

        let terms = lookup_terms_for_add_request(&request, "sonarr");

        assert_eq!(terms[0], "tvdb:321");
        assert!(terms.iter().any(|term| term == "Blocked Show 2024"));
        assert!(terms.iter().any(|term| term == "Blocked Show"));
    }
}
