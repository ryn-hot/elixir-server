use anyhow::Result;
use reqwest::Client;
use serde::Serialize;

use crate::{
    db::models::MediaType,
    extensions::{ExternalIds, MediaIdentity},
    metadata::provider_types::{DiscoveryResult, MetadataResult},
};

pub mod cinemeta {
    use super::*;
    use serde::{Deserialize, Deserializer, de};

    #[derive(Debug, Deserialize)]
    struct CineMetaResponse {
        meta: Option<CineMetaItem>,
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct CineMetaItem {
        #[serde(rename = "imdb_id")]
        pub imdb_id: Option<String>,
        #[serde(default, deserialize_with = "deserialize_runtime_minutes")]
        pub runtime: Option<i32>, // minutes
        pub description: Option<String>,
        #[serde(default, deserialize_with = "deserialize_string_vec")]
        pub genres: Option<Vec<String>>,
        #[serde(
            default,
            rename = "genre",
            deserialize_with = "deserialize_string_vec",
            skip_serializing
        )]
        genre_alias: Option<Vec<String>>,
        #[serde(default, deserialize_with = "deserialize_optional_i32")]
        pub year: Option<i32>,
        #[serde(flatten)]
        pub rest: serde_json::Value,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
        base_url: &str,
    ) -> Result<Option<MetadataResult>> {
        let media_kind = match identity.r#type {
            MediaType::Movie => "movie",
            MediaType::Series | MediaType::Anime => "series",
        };
        let base_url = normalize_base_url(base_url);
        if base_url.is_empty() {
            return Ok(None);
        }

        // Try by imdb id if present.
        if let Some(imdb) = identity.external_ids.imdb.as_ref() {
            let url = format!("{}/meta/{}/{}.json", base_url, media_kind, imdb);
            let res = client.get(&url).send().await?;
            if !res.status().is_success() {
                return Ok(None);
            }
            let body = res.json::<CineMetaResponse>().await?;
            if let Some(meta) = body.meta {
                let runtime_seconds = meta.runtime.map(|m| m * 60);
                let mut external_ids = ExternalIds::default();
                external_ids.imdb = meta.imdb_id.clone();
                let genres = meta.genres();
                let json = serde_json::to_value(&meta)?;
                return Ok(Some(MetadataResult {
                    metadata_json: json,
                    runtime_seconds,
                    external_ids: Some(external_ids),
                    description: meta.description.clone(),
                    genres,
                }));
            }
            return Ok(None);
        }

        // Fallback to search by title.
        let query = urlencoding::encode(&identity.title);
        let url = format!("{}/meta/{}/search={}.json", base_url, media_kind, query);
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(None);
        }
        #[derive(Debug, Deserialize)]
        struct SearchResp {
            metas: Option<Vec<CineMetaItem>>,
        }
        let body: SearchResp = res.json().await?;
        if let Some(first) = body.metas.and_then(|mut m| m.pop()) {
            let runtime_seconds = first.runtime.map(|m| m * 60);
            let genres = first.genres();
            let json = serde_json::to_value(&first)?;
            let external_ids = first.imdb_id.clone().map(|imdb| ExternalIds {
                imdb: Some(imdb),
                ..Default::default()
            });
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds,
                external_ids,
                description: first.description.clone(),
                genres,
            }));
        }
        Ok(None)
    }

    pub async fn search(
        client: &Client,
        query: &str,
        media_kind: &str,
        base_url: &str,
    ) -> Result<Vec<CineMetaItem>> {
        #[derive(Debug, Deserialize)]
        struct SearchResp {
            metas: Option<Vec<CineMetaItem>>,
        }
        let base_url = normalize_base_url(base_url);
        if base_url.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = urlencoding::encode(query);
        let url = format!("{}/meta/{}/search={}.json", base_url, media_kind, encoded);
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(Vec::new());
        }
        let body: SearchResp = res.json().await?;
        Ok(body.metas.unwrap_or_default())
    }

    fn normalize_base_url(base_url: &str) -> String {
        base_url.trim().trim_end_matches('/').to_string()
    }

    impl CineMetaItem {
        fn genres(&self) -> Option<Vec<String>> {
            self.genres
                .clone()
                .filter(|values| !values.is_empty())
                .or_else(|| self.genre_alias.clone().filter(|values| !values.is_empty()))
        }
    }

    fn deserialize_runtime_minutes<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<serde_json::Value>::deserialize(deserializer)?;
        Ok(value.as_ref().and_then(parse_runtime_minutes))
    }

    fn parse_runtime_minutes(value: &serde_json::Value) -> Option<i32> {
        match value {
            serde_json::Value::Null => None,
            serde_json::Value::Number(number) => number
                .as_i64()
                .and_then(|minutes| positive_i64_to_i32(minutes)),
            serde_json::Value::String(value) => parse_runtime_minutes_str(value),
            _ => None,
        }
    }

    fn parse_runtime_minutes_str(value: &str) -> Option<i32> {
        let value = value.trim();
        if value.is_empty() {
            return None;
        }
        if let Ok(minutes) = value.parse::<i64>() {
            return positive_i64_to_i32(minutes);
        }
        if value.contains(':') {
            return parse_colon_duration_minutes(value);
        }

        let lower = value.to_ascii_lowercase();
        let tokens: Vec<&str> = lower.split_whitespace().collect();
        let mut total_minutes = 0i64;
        let mut matched_unit = false;
        for (index, token) in tokens.iter().enumerate() {
            let Some((amount, suffix)) = parse_numeric_token(token) else {
                continue;
            };
            let unit = if suffix.is_empty() {
                tokens.get(index + 1).copied().unwrap_or_default()
            } else {
                suffix
            };
            if unit.starts_with('h') {
                total_minutes += amount * 60;
                matched_unit = true;
            } else if unit.starts_with('m') {
                total_minutes += amount;
                matched_unit = true;
            } else if unit.starts_with('s') {
                total_minutes += (amount + 59) / 60;
                matched_unit = true;
            }
        }
        if matched_unit {
            return positive_i64_to_i32(total_minutes);
        }

        lower
            .split(|ch: char| !ch.is_ascii_digit())
            .find_map(|part| {
                if part.is_empty() {
                    None
                } else {
                    part.parse::<i64>().ok().and_then(positive_i64_to_i32)
                }
            })
    }

    fn parse_colon_duration_minutes(value: &str) -> Option<i32> {
        let parts: Vec<i64> = value
            .split(':')
            .map(str::trim)
            .map(str::parse::<i64>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;
        let seconds = match parts.as_slice() {
            [minutes, seconds] => minutes.checked_mul(60)?.checked_add(*seconds)?,
            [hours, minutes, seconds] => hours
                .checked_mul(3600)?
                .checked_add(minutes.checked_mul(60)?)?
                .checked_add(*seconds)?,
            _ => return None,
        };
        positive_i64_to_i32((seconds + 30) / 60)
    }

    fn parse_numeric_token(token: &str) -> Option<(i64, &str)> {
        let digit_len = token
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_digit())
            .last()
            .map(|(index, ch)| index + ch.len_utf8())
            .unwrap_or(0);
        if digit_len == 0 {
            return None;
        }
        let amount = token[..digit_len].parse::<i64>().ok()?;
        Some((amount, &token[digit_len..]))
    }

    fn positive_i64_to_i32(value: i64) -> Option<i32> {
        if value > 0 && value <= i32::MAX as i64 {
            Some(value as i32)
        } else {
            None
        }
    }

    fn deserialize_string_vec<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<Vec<String>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<serde_json::Value>::deserialize(deserializer)?;
        match value {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(value)) => {
                let values: Vec<String> = value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
                    .collect();
                Ok((!values.is_empty()).then_some(values))
            }
            Some(serde_json::Value::Array(values)) => {
                let values: Vec<String> = values
                    .into_iter()
                    .filter_map(|value| match value {
                        serde_json::Value::String(value) => {
                            let value = value.trim().to_string();
                            (!value.is_empty()).then_some(value)
                        }
                        _ => None,
                    })
                    .collect();
                Ok((!values.is_empty()).then_some(values))
            }
            Some(other) => Err(de::Error::custom(format!(
                "expected string or string array, got {other}"
            ))),
        }
    }

    fn deserialize_optional_i32<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<serde_json::Value>::deserialize(deserializer)?;
        Ok(value.as_ref().and_then(parse_optional_i32))
    }

    fn parse_optional_i32(value: &serde_json::Value) -> Option<i32> {
        match value {
            serde_json::Value::Null => None,
            serde_json::Value::Number(number) => number
                .as_i64()
                .filter(|value| *value >= i32::MIN as i64 && *value <= i32::MAX as i64)
                .map(|value| value as i32),
            serde_json::Value::String(value) => value.trim().parse::<i32>().ok(),
            _ => None,
        }
    }

    pub fn extract_poster_url(rest: &serde_json::Value) -> Option<String> {
        rest.get("poster")
            .or_else(|| rest.get("posterUrl"))
            .or_else(|| rest.get("image"))
            .or_else(|| rest.get("thumbnail"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
    }

    pub fn extract_popularity_score(rest: &serde_json::Value) -> Option<f64> {
        value_to_score(rest.get("popularity"))
            .or_else(|| value_to_score(rest.get("imdbRating")))
            .or_else(|| value_to_score(rest.get("rank")))
            .or_else(|| value_to_score(rest.get("votes")))
    }

    fn value_to_score(value: Option<&serde_json::Value>) -> Option<f64> {
        let value = value?;
        if let Some(number) = value.as_f64() {
            return Some(number);
        }
        value.as_str()?.trim().parse::<f64>().ok()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[test]
        fn deserializes_real_cinemeta_runtime_and_genres() {
            let response: CineMetaResponse = serde_json::from_value(json!({
                "meta": {
                    "imdb_id": "tt0381061",
                    "runtime": "144 min",
                    "genre": ["Action", "Adventure", "Thriller"],
                    "genres": ["Action", "Adventure", "Thriller"],
                    "year": "2006",
                    "poster": "https://images.metahub.space/poster/small/tt0381061/img",
                    "background": "https://images.metahub.space/background/medium/tt0381061/img",
                    "description": "Casino Royale metadata"
                }
            }))
            .expect("cinemeta response");

            let meta = response.meta.expect("meta");
            assert_eq!(meta.runtime, Some(144));
            assert_eq!(meta.year, Some(2006));
            assert_eq!(
                meta.genres(),
                Some(vec![
                    "Action".to_string(),
                    "Adventure".to_string(),
                    "Thriller".to_string()
                ])
            );

            let serialized = serde_json::to_value(&meta).expect("serialized meta");
            assert_eq!(
                serialized.get("poster").and_then(serde_json::Value::as_str),
                Some("https://images.metahub.space/poster/small/tt0381061/img")
            );
            assert!(serialized.get("genre").is_none());
        }

        #[test]
        fn deserializes_runtime_variants() {
            assert_eq!(parse_runtime_minutes_str("2h 24m"), Some(144));
            assert_eq!(parse_runtime_minutes_str("02:24:00"), Some(144));
            assert_eq!(parse_runtime_minutes_str("144"), Some(144));
        }
    }
}

pub mod tvdb {
    use super::*;
    use serde_json::Value;

    pub fn map_search_results(
        items: &[Value],
        fallback_query: &str,
        media_type: MediaType,
    ) -> Vec<DiscoveryResult> {
        let mut results = Vec::new();
        for item in items {
            if !tvdb_result_matches_media_type(item, media_type) {
                continue;
            }
            let Some(tvdb_id) = extract_tvdb_id(item) else {
                continue;
            };

            let title = select_title(item, fallback_query);
            let year = extract_year(item);
            let description = item
                .get("overview")
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            let imdb = extract_remote_id(item, &["imdb"], true);
            let tmdb = extract_remote_id(item, &["tmdb", "themoviedb"], false);

            let (tvdb_series, tvdb_movie) = match media_type {
                MediaType::Movie => (None, Some(tvdb_id.clone())),
                MediaType::Series | MediaType::Anime => (Some(tvdb_id.clone()), None),
            };

            results.push(DiscoveryResult {
                title,
                r#type: media_type,
                year,
                external_ids: Some(ExternalIds {
                    imdb,
                    tmdb,
                    tvdb: Some(tvdb_id),
                    tvdb_series,
                    tvdb_movie,
                    ..Default::default()
                }),
                description,
                poster_url: extract_poster_url(item),
                popularity_score: extract_popularity_score(item),
            });
        }
        results
    }

    fn tvdb_result_matches_media_type(value: &Value, media_type: MediaType) -> bool {
        let expected = match media_type {
            MediaType::Movie => "movie",
            MediaType::Series | MediaType::Anime => "series",
        };
        let actual = value
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value.trim().to_ascii_lowercase());
        if let Some(actual) = actual {
            if expected == "movie" {
                return actual.contains("movie");
            }
            if actual.contains("series") || actual.contains("show") || actual.contains("tv") {
                return true;
            }
            return false;
        }
        if expected == "movie" {
            return value
                .get("isMovie")
                .and_then(Value::as_i64)
                .map(|flag| flag == 1)
                .unwrap_or(true);
        }
        true
    }

    fn extract_tvdb_id(value: &Value) -> Option<String> {
        value
            .get("tvdb_id")
            .or_else(|| value.get("tvdbId"))
            .or_else(|| value.get("id"))
            .and_then(as_string)
    }

    fn select_title(value: &Value, fallback_query: &str) -> String {
        let title = value
            .get("name")
            .or_else(|| value.get("title"))
            .or_else(|| value.get("name_translated"))
            .or_else(|| value.get("seriesName"))
            .or_else(|| value.get("movieName"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback_query);
        title.to_string()
    }

    fn extract_year(value: &Value) -> Option<i32> {
        if let Some(year) = value.get("year").and_then(Value::as_i64) {
            return Some(year as i32);
        }
        if let Some(year) = value.get("year").and_then(Value::as_str) {
            return parse_year_text(year);
        }
        if let Some(first_air_time) = value.get("first_air_time").and_then(Value::as_str) {
            return parse_year_text(first_air_time);
        }
        if let Some(first_aired) = value.get("firstAired").and_then(Value::as_str) {
            return parse_year_text(first_aired);
        }
        None
    }

    fn extract_poster_url(value: &Value) -> Option<String> {
        value
            .get("image_url")
            .or_else(|| value.get("image"))
            .or_else(|| value.get("poster"))
            .or_else(|| value.get("thumbnail"))
            .and_then(as_string)
    }

    fn extract_popularity_score(value: &Value) -> Option<f64> {
        value_to_score(value.get("score"))
            .or_else(|| value_to_score(value.get("popularity")))
            .or_else(|| value_to_score(value.get("siteRating")))
    }

    fn parse_year_text(value: &str) -> Option<i32> {
        let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() < 4 {
            return None;
        }
        digits.get(0..4)?.parse::<i32>().ok()
    }

    fn extract_remote_id(
        value: &Value,
        sources: &[&str],
        allow_prefix_match: bool,
    ) -> Option<String> {
        let array = value
            .get("remote_ids")
            .or_else(|| value.get("remoteIds"))
            .and_then(Value::as_array)?;
        for item in array {
            let source = item
                .get("sourceName")
                .or_else(|| item.get("source_name"))
                .or_else(|| item.get("source"))
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            let id = item.get("id").and_then(as_string)?;
            let lower_id = id.to_ascii_lowercase();
            if allow_prefix_match && lower_id.starts_with("tt") {
                return Some(id);
            }
            if sources.iter().any(|expected| source.contains(expected)) {
                return Some(id);
            }
        }
        None
    }

    fn as_string(value: &Value) -> Option<String> {
        if let Some(text) = value.as_str() {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return None;
            }
            return Some(trimmed.to_string());
        }
        if let Some(number) = value.as_i64() {
            return Some(number.to_string());
        }
        if let Some(number) = value.as_u64() {
            return Some(number.to_string());
        }
        None
    }

    fn value_to_score(value: Option<&Value>) -> Option<f64> {
        let value = value?;
        if let Some(number) = value.as_f64() {
            return Some(number);
        }
        value.as_str()?.trim().parse::<f64>().ok()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[test]
        fn maps_tvdb_movie_search_result() {
            let items = vec![json!({
                "id": "550",
                "type": "movie",
                "name": "Fight Club",
                "overview": "Desc",
                "year": "1999",
                "remote_ids": [
                    { "sourceName": "IMDB", "id": "tt0137523" },
                    { "sourceName": "TheMovieDB.com", "id": "550" }
                ]
            })];
            let mapped = map_search_results(&items, "fight club", MediaType::Movie);
            assert_eq!(mapped.len(), 1);
            let result = &mapped[0];
            assert_eq!(result.title, "Fight Club");
            assert_eq!(result.year, Some(1999));
            let ids = result.external_ids.as_ref().expect("external ids");
            assert_eq!(ids.imdb.as_deref(), Some("tt0137523"));
            assert_eq!(ids.tmdb.as_deref(), Some("550"));
            assert_eq!(ids.tvdb_movie.as_deref(), Some("550"));
        }

        #[test]
        fn ignores_non_series_results_for_series_search() {
            let items = vec![
                json!({
                    "id": "123",
                    "type": "movie",
                    "name": "Movie Result"
                }),
                json!({
                    "id": "456",
                    "type": "series",
                    "name": "Series Result"
                }),
            ];
            let mapped = map_search_results(&items, "query", MediaType::Series);
            assert_eq!(mapped.len(), 1);
            assert_eq!(mapped[0].title, "Series Result");
            let ids = mapped[0].external_ids.as_ref().expect("external ids");
            assert_eq!(ids.tvdb_series.as_deref(), Some("456"));
        }
    }
}

pub mod anilist {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, Serialize)]
    pub struct MediaItem {
        pub id: i32,
        pub title: Option<serde_json::Value>,
        pub episodes: Option<i32>,
        pub duration: Option<i32>,
        pub format: Option<String>,
        pub status: Option<String>,
        #[serde(rename = "nextAiringEpisode")]
        pub next_airing_episode: Option<serde_json::Value>,
        pub description: Option<String>,
        pub genres: Option<Vec<String>>,
        pub startDate: Option<serde_json::Value>,
        pub coverImage: Option<serde_json::Value>,
        pub bannerImage: Option<String>,
        pub popularity: Option<i32>,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        if let Some(anilist_id) = identity.external_ids.anilist.as_ref() {
            if let Ok(parsed) = anilist_id.parse::<i32>() {
                return fetch_by_id(client, parsed).await;
            }
            return Ok(None);
        }

        if let Some(meta) = fetch_by_search(client, &identity.title).await? {
            return Ok(Some(meta));
        }

        if let Some(season) = identity.season {
            if season > 1 {
                let season_title = format!("{} Season {}", identity.title, season);
                if let Some(meta) = fetch_by_search(client, &season_title).await? {
                    return Ok(Some(meta));
                }
            }
        }

        Ok(None)
    }

    pub async fn search(client: &Client, query: &str) -> Result<Vec<MediaItem>> {
        let gql = r#"
            query ($search: String) {
              Page(perPage: 5) {
                media(search: $search, type: ANIME) {
                  id
                  title { romaji english native }
                  episodes
                  duration
                  format
                  status
                  nextAiringEpisode { episode airingAt }
                  description
                  genres
                  startDate { year }
                  coverImage { extraLarge large medium }
                  popularity
                }
              }
            }
        "#;

        #[derive(Serialize)]
        struct Variables<'a> {
            search: &'a str,
        }
        #[derive(Deserialize)]
        struct PageData {
            media: Option<Vec<MediaItem>>,
        }
        #[derive(Deserialize)]
        struct Resp {
            data: Option<Data>,
        }
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "Page")]
            page: Option<PageData>,
        }

        let res = client
            .post("https://graphql.anilist.co")
            .json(&serde_json::json!({ "query": gql, "variables": Variables { search: query } }))
            .send()
            .await?;

        if !res.status().is_success() {
            return Ok(Vec::new());
        }
        let body: Resp = res.json().await?;
        Ok(body
            .data
            .and_then(|d| d.page)
            .and_then(|p| p.media)
            .unwrap_or_default())
    }

    async fn fetch_by_search(client: &Client, search: &str) -> Result<Option<MetadataResult>> {
        let query = r#"
            query ($search: String) {
              Media(search: $search, type: ANIME) {
                id
                title { romaji english native }
                episodes
                duration
                format
                status
                nextAiringEpisode { episode airingAt }
                description
                genres
                coverImage { extraLarge large medium }
                bannerImage
              }
            }
        "#;

        #[derive(Serialize)]
        struct Variables<'a> {
            search: &'a str,
        }

        let res = client
            .post("https://graphql.anilist.co")
            .json(&serde_json::json!({ "query": query, "variables": Variables { search } }))
            .send()
            .await?;

        if !res.status().is_success() {
            return Ok(None);
        }
        #[derive(Deserialize)]
        struct MediaResp {
            data: Option<MediaData>,
        }
        #[derive(Deserialize)]
        struct MediaData {
            #[serde(rename = "Media")]
            media: Option<MediaItem>,
        }

        let body: MediaResp = res.json().await?;
        if let Some(media) = body.data.and_then(|d| d.media) {
            return Ok(Some(metadata_from_media(&media)?));
        }
        Ok(None)
    }

    async fn fetch_by_id(client: &Client, id: i32) -> Result<Option<MetadataResult>> {
        let query = r#"
            query ($id: Int) {
              Media(id: $id, type: ANIME) {
                id
                title { romaji english native }
                episodes
                duration
                format
                status
                nextAiringEpisode { episode airingAt }
                description
                genres
                coverImage { extraLarge large medium }
                bannerImage
              }
            }
        "#;

        #[derive(Serialize)]
        struct Variables {
            id: i32,
        }

        let res = client
            .post("https://graphql.anilist.co")
            .json(&serde_json::json!({ "query": query, "variables": Variables { id } }))
            .send()
            .await?;

        if !res.status().is_success() {
            return Ok(None);
        }
        #[derive(Deserialize)]
        struct MediaResp {
            data: Option<MediaData>,
        }
        #[derive(Deserialize)]
        struct MediaData {
            #[serde(rename = "Media")]
            media: Option<MediaItem>,
        }

        let body: MediaResp = res.json().await?;
        if let Some(media) = body.data.and_then(|d| d.media) {
            return Ok(Some(metadata_from_media(&media)?));
        }
        Ok(None)
    }

    fn metadata_from_media(media: &MediaItem) -> Result<MetadataResult> {
        let runtime_seconds = media
            .duration
            .and_then(|d| media.episodes.map(|e| d * 60 * e).or(Some(d * 60)));
        let json = serde_json::to_value(media)?;
        Ok(MetadataResult {
            metadata_json: json,
            runtime_seconds,
            external_ids: Some(ExternalIds {
                anilist: Some(media.id.to_string()),
                ..Default::default()
            }),
            description: media.description.clone(),
            genres: media.genres.clone(),
        })
    }

    pub fn extract_cover_image_url(media: &MediaItem) -> Option<String> {
        let Some(value) = media.coverImage.as_ref() else {
            return None;
        };
        value
            .get("extraLarge")
            .or_else(|| value.get("large"))
            .or_else(|| value.get("medium"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(|url| url.to_string())
    }
}

pub mod aniapi {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Serialize)]
    struct AniApiAnime {
        id: i64,
        titles: serde_json::Value,
        episodes_count: Option<i32>,
        episode_duration: Option<i32>,
        description: Option<String>,
        genres: Option<Vec<String>>,
    }

    #[derive(Deserialize)]
    struct AniApiResponse {
        data: Option<Vec<AniApiAnime>>,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        let url = format!(
            "https://api.aniapi.com/v1/anime?title={}",
            urlencoding::encode(&identity.title)
        );
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(None);
        }
        let body: AniApiResponse = res.json().await?;
        if let Some(first) = body.data.and_then(|mut d| d.pop()) {
            let runtime_seconds = first.episode_duration.and_then(|dur| {
                first
                    .episodes_count
                    .map(|e| dur * 60 * e)
                    .or(Some(dur * 60))
            });
            let json = serde_json::to_value(&first)?;
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds,
                external_ids: None,
                description: first.description.clone(),
                genres: first.genres.clone(),
            }));
        }
        Ok(None)
    }
}

pub mod consumet {
    use super::*;

    pub async fn fetch(
        _client: &Client,
        _identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        // Placeholder: Consumet has multiple providers; skip actual call to avoid instability.
        Ok(None)
    }
}
