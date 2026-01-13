use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;

use crate::{
    extensions::ExternalIds, http::error::ApiResult, library::run_full_scan_with_metadata_and_linkers,
    state::AppState,
};

#[derive(Serialize)]
pub struct LibraryItemResponse {
    pub id: String,
    pub title: String,
    pub r#type: String,
    pub year: Option<i32>,
    pub updated_at: String,
    pub runtime_seconds: Option<i32>,
    pub metadata: Option<serde_json::Value>,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
    pub backdrop_url: Option<String>,
}

pub async fn list_items(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<LibraryItemResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let rows = sqlx::query("SELECT id, title, 'movie' as type, year, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies UNION ALL SELECT id, title, library_type as type, year, CAST(NULL AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series ORDER BY updated_at DESC LIMIT 200")
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let mut movie_ids = Vec::new();
    let mut series_ids = Vec::new();
    let mut anime_ids = Vec::new();
    for row in &rows {
        let id = row.get::<String, _>("id");
        let item_type = row.get::<String, _>("type");
        match item_type.as_str() {
            "movie" => movie_ids.push(id),
            "anime" => anime_ids.push(id),
            _ => series_ids.push(id),
        }
    }

    let movie_posters = load_primary_artwork(
        &state.db_pool,
        "movie",
        &movie_ids,
        "poster",
        &preferred_languages,
        &["cinemeta", "tvdb"],
    )
    .await?;
    let movie_backdrops = load_primary_artwork(
        &state.db_pool,
        "movie",
        &movie_ids,
        "backdrop",
        &preferred_languages,
        &["cinemeta", "tvdb"],
    )
    .await?;
    let series_posters = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "poster",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let series_banners = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;
    let series_backdrops = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "backdrop",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let anime_posters = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "poster",
        &preferred_languages,
        &["anilist", "tvdb", "cinemeta"],
    )
    .await?;
    let anime_banners = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "banner",
        &preferred_languages,
        &["anilist", "tvdb"],
    )
    .await?;
    let anime_backdrops = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "backdrop",
        &preferred_languages,
        &["anilist", "tvdb", "cinemeta"],
    )
    .await?;

    let items = rows
        .into_iter()
        .map(|row| LibraryItemResponse {
            id: row.get::<String, _>("id"),
            title: row.get::<String, _>("title"),
            r#type: row.get::<String, _>("type"),
            year: row.try_get::<i64, _>("year").ok().map(|v| v as i32),
            updated_at: row.get::<String, _>("updated_at"),
            runtime_seconds: row
                .try_get::<String, _>("runtime_seconds")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v as i32),
            metadata: row
                .try_get::<String, _>("metadata_json")
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok()),
            poster_url: {
                let id = row.get::<String, _>("id");
                match row.get::<String, _>("type").as_str() {
                    "movie" => movie_posters.get(&id).cloned(),
                    "anime" => anime_posters.get(&id).cloned(),
                    _ => series_posters.get(&id).cloned(),
                }
            },
            banner_url: {
                let id = row.get::<String, _>("id");
                match row.get::<String, _>("type").as_str() {
                    "anime" => anime_banners.get(&id).cloned(),
                    "movie" => None,
                    _ => series_banners.get(&id).cloned(),
                }
            },
            backdrop_url: {
                let id = row.get::<String, _>("id");
                match row.get::<String, _>("type").as_str() {
                    "movie" => movie_backdrops.get(&id).cloned(),
                    "anime" => anime_backdrops.get(&id).cloned(),
                    _ => series_backdrops.get(&id).cloned(),
                }
            },
        })
        .collect();

    Ok(Json(items))
}

#[derive(Debug, Deserialize)]
pub struct ScanQuery {
    #[serde(default)]
    pub force_metadata: bool,
    #[serde(default)]
    pub force_reclassify: bool,
}

pub async fn scan(
    State(state): State<AppState>,
    Query(params): Query<ScanQuery>,
) -> ApiResult<Json<&'static str>> {
    let candidates = state
        .extensions
        .scan_all_with_db(&state.db_pool, &state.settings.library.sonarr, None)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    run_full_scan_with_metadata_and_linkers(
        &state.db_pool,
        Some(&state.metadata),
        Some(&state.linkers),
        Some(&state.settings.classifier),
        Some(&state.artwork),
        candidates,
        params.force_metadata,
        params.force_reclassify,
        state.settings.library.hash_dedupe_enabled,
    )
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    Ok(Json("ok"))
}

#[derive(Serialize)]
pub struct LibraryDetailResponse {
    pub id: String,
    pub title: String,
    pub r#type: String,
    pub year: Option<i32>,
    pub runtime_seconds: Option<i32>,
    pub external_ids: ExternalIds,
    pub metadata: Option<serde_json::Value>,
    pub description: Option<String>,
    pub genres: Vec<String>,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
    pub backdrop_url: Option<String>,
    pub files: Vec<LibraryFileResponse>,
}

#[derive(Serialize)]
pub struct SeasonResponse {
    pub id: String,
    pub season_number: i32,
    pub title: Option<String>,
    pub episode_count: i32,
    pub has_files: bool,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
}

#[derive(Serialize)]
pub struct SeasonDetailResponse {
    pub id: String,
    pub series_id: String,
    pub season_number: i32,
    pub title: Option<String>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
}

#[derive(Serialize)]
pub struct EpisodeResponse {
    pub id: String,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
    pub title: Option<String>,
    pub runtime_seconds: Option<i32>,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub has_file: bool,
}

#[derive(Serialize)]
pub struct LibraryFileResponse {
    pub id: String,
    pub path: String,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub size_bytes: Option<i64>,
    pub scan_state: String,
    pub source_config_id: Option<String>,
    pub extension_metadata: Option<serde_json::Value>,
    pub tracks: Vec<MediaTrackResponse>,
    pub external_subtitles: Vec<ExternalSubtitleResponse>,
}

#[derive(Serialize)]
pub struct MediaTrackResponse {
    pub id: String,
    pub track_type: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub codec: Option<String>,
    pub channels: Option<i32>,
    pub is_default: bool,
    pub is_forced: bool,
    pub stream_index: Option<i32>,
}

#[derive(Serialize)]
pub struct ExternalSubtitleResponse {
    pub id: String,
    pub path: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub format: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
}

pub async fn list_seasons(
    State(state): State<AppState>,
    Path(series_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<SeasonResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let exists: Option<String> = sqlx::query_scalar(
        "SELECT id FROM series WHERE id = ? LIMIT 1",
    )
    .bind(&series_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    if exists.is_none() {
        return Err(crate::http::error::ApiError::not_found("series not found"));
    }

    let rows = sqlx::query(
        "SELECT s.id, s.season_number, s.title, COUNT(e.id) as episode_count, COALESCE(SUM(CASE WHEN e.has_file THEN 1 ELSE 0 END), 0) as file_count FROM seasons s LEFT JOIN episodes e ON e.season_id = s.id WHERE s.series_id = ? GROUP BY s.id ORDER BY s.season_number",
    )
    .bind(&series_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let season_ids: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect();

    let posters = load_primary_artwork(
        &state.db_pool,
        "season",
        &season_ids,
        "poster",
        &preferred_languages,
        &["tvdb", "anilist"],
    )
    .await?;
    let banners = load_primary_artwork(
        &state.db_pool,
        "season",
        &season_ids,
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;

    let seasons = rows
        .into_iter()
        .map(|row| {
            let id = row.get::<String, _>("id");
            let banner_url = banners.get(&id).cloned();
            let poster_url = posters.get(&id).cloned().or_else(|| banner_url.clone());
            SeasonResponse {
                id,
                season_number: row
                    .try_get::<i64, _>("season_number")
                    .ok()
                    .unwrap_or_default() as i32,
                title: row.try_get::<String, _>("title").ok(),
                episode_count: row
                    .try_get::<i64, _>("episode_count")
                    .ok()
                    .unwrap_or_default() as i32,
                has_files: row
                    .try_get::<i64, _>("file_count")
                    .ok()
                    .unwrap_or_default()
                    > 0,
                poster_url,
                banner_url,
            }
        })
        .collect();

    Ok(Json(seasons))
}

pub async fn list_episodes(
    State(state): State<AppState>,
    Path(season_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<EpisodeResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let exists: Option<String> = sqlx::query_scalar(
        "SELECT id FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(&season_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    if exists.is_none() {
        return Err(crate::http::error::ApiError::not_found("season not found"));
    }

    let rows = sqlx::query(
        "SELECT e.id, e.season_number, e.episode_number, e.absolute_episode_number, e.title, e.runtime_seconds, CAST(e.has_file AS INTEGER) AS has_file, e.metadata_json, aem.title as anime_title, aem.duration_seconds as anime_duration, aem.snapshot_url FROM episodes e LEFT JOIN anime_episode_meta aem ON aem.season_id = e.season_id AND aem.episode_number = e.episode_number WHERE e.season_id = ? ORDER BY e.episode_number",
    )
    .bind(&season_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let episode_ids: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect();
    let thumbnails = load_primary_artwork(
        &state.db_pool,
        "episode",
        &episode_ids,
        "thumbnail",
        &preferred_languages,
        &["tvdb", "anizip"],
    )
    .await?;

    let episodes = rows
        .into_iter()
        .map(|row| {
            let id = row.get::<String, _>("id");
            let metadata_json: Option<Value> = row
                .try_get::<String, _>("metadata_json")
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok());
            let description = extract_episode_description(metadata_json.as_ref());
            let anime_title: Option<String> = row.try_get("anime_title").ok();
            let raw_title: Option<String> = row.try_get("title").ok();
            let title = match raw_title.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
                Some(_) => raw_title,
                None => anime_title,
            };
            let runtime_seconds = row
                .try_get::<i64, _>("runtime_seconds")
                .ok()
                .map(|v| v as i32)
                .or_else(|| {
                    row.try_get::<i64, _>("anime_duration")
                        .ok()
                        .map(|v| v as i32)
                });
            let snapshot_url: Option<String> = row.try_get("snapshot_url").ok();
            let thumbnail_url = thumbnails
                .get(&id)
                .cloned()
                .or_else(|| snapshot_url)
                .or_else(|| extract_episode_thumbnail(metadata_json.as_ref()));

            EpisodeResponse {
                id,
                season_number: row
                    .try_get::<i64, _>("season_number")
                    .ok()
                    .unwrap_or_default() as i32,
                episode_number: row
                    .try_get::<i64, _>("episode_number")
                    .ok()
                    .unwrap_or_default() as i32,
                absolute_episode_number: row
                    .try_get::<i64, _>("absolute_episode_number")
                    .ok()
                    .map(|v| v as i32),
                title,
                runtime_seconds,
                description,
                thumbnail_url,
                has_file: row
                    .try_get::<i64, _>("has_file")
                    .ok()
                    .map(|v| v != 0)
                    .unwrap_or(false),
            }
        })
        .collect();

    Ok(Json(episodes))
}

pub async fn season_detail(
    State(state): State<AppState>,
    Path(season_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<SeasonDetailResponse>> {
    let preferred_languages = parse_language_header(&headers);
    let row = sqlx::query(
        "SELECT id, series_id, season_number, title, metadata_json FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(&season_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let Some(row) = row else {
        return Err(crate::http::error::ApiError::not_found("season not found"));
    };

    let metadata_json: Option<Value> = row
        .try_get::<String, _>("metadata_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let description = extract_season_description(metadata_json.as_ref());

    let posters = load_primary_artwork(
        &state.db_pool,
        "season",
        &[season_id.clone()],
        "poster",
        &preferred_languages,
        &["tvdb", "anilist"],
    )
    .await?;
    let banners = load_primary_artwork(
        &state.db_pool,
        "season",
        &[season_id.clone()],
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;

    let poster_url = posters
        .get(&season_id)
        .cloned()
        .or_else(|| banners.get(&season_id).cloned());

    let response = SeasonDetailResponse {
        id: row.get::<String, _>("id"),
        series_id: row.get::<String, _>("series_id"),
        season_number: row
            .try_get::<i64, _>("season_number")
            .ok()
            .unwrap_or_default() as i32,
        title: row.try_get::<String, _>("title").ok(),
        description,
        poster_url,
        banner_url: banners.get(&season_id).cloned(),
    };

    Ok(Json(response))
}

pub async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<LibraryDetailResponse>> {
    let preferred_languages = parse_language_header(&headers);
    let movie = sqlx::query(
        "SELECT id, title, year, external_imdb, external_tmdb, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json FROM movies WHERE id = ? LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let (item_type, title, year, runtime_seconds, metadata_json, external_ids) =
        if let Some(row) = movie {
        let external_ids = ExternalIds {
            imdb: row.try_get::<String, _>("external_imdb").ok(),
            tmdb: row.try_get::<String, _>("external_tmdb").ok(),
            ..Default::default()
        };
        let metadata_json: Option<Value> = row
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        (
            "movie".to_string(),
            row.get::<String, _>("title"),
            row.try_get::<i64, _>("year").ok().map(|v| v as i32),
            row.try_get::<String, _>("runtime_seconds")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v as i32),
            metadata_json,
            external_ids,
        )
    } else {
        let series = sqlx::query(
            "SELECT id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json FROM series WHERE id = ? LIMIT 1",
        )
        .bind(&id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

        let series = series.ok_or_else(|| crate::http::error::ApiError::not_found("item not found"))?;
        let external_tvdb: Option<String> = series.try_get("external_tvdb_series").ok();
        let external_ids = ExternalIds {
            imdb: series.try_get::<String, _>("external_imdb").ok(),
            tvdb: external_tvdb.clone(),
            tvdb_series: external_tvdb,
            anilist: series.try_get::<String, _>("external_anilist").ok(),
            ..Default::default()
        };
        let metadata_json: Option<Value> = series
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        (
            series.get::<String, _>("library_type"),
            series.get::<String, _>("title"),
            series.try_get::<i64, _>("year").ok().map(|v| v as i32),
            None,
            metadata_json,
            external_ids,
        )
    };

    let files = if item_type == "movie" {
        sqlx::query("SELECT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, mf.size_bytes, mf.scan_state, mf.source_config_id, mf.extension_metadata FROM media_files mf JOIN movie_files mlf ON mlf.media_file_id = mf.id WHERE mlf.movie_id = ?")
            .bind(&id)
            .fetch_all(&state.db_pool)
            .await
            .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?
    } else {
        sqlx::query("SELECT DISTINCT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, mf.size_bytes, mf.scan_state, mf.source_config_id, mf.extension_metadata FROM media_files mf JOIN episode_files ef ON ef.media_file_id = mf.id JOIN episodes e ON e.id = ef.episode_id WHERE e.series_id = ?")
            .bind(&id)
            .fetch_all(&state.db_pool)
            .await
            .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?
    };

    let file_ids: Vec<String> = files
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect();

    let mut tracks_by_file = load_media_tracks(&state.db_pool, &file_ids).await?;
    let mut subtitles_by_file = load_external_subtitles(&state.db_pool, &file_ids).await?;

    let files = files
        .into_iter()
        .map(|row| {
            let file_id = row.get::<String, _>("id");
            let tracks = tracks_by_file.remove(&file_id).unwrap_or_default();
            let external_subtitles = subtitles_by_file.remove(&file_id).unwrap_or_default();
            LibraryFileResponse {
                id: file_id,
                path: row.get::<String, _>("path"),
                container: row.try_get::<String, _>("container").ok(),
                video_codec: row.try_get::<String, _>("video_codec").ok(),
                audio_codec: row.try_get::<String, _>("audio_codec").ok(),
                size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
                scan_state: row.get::<String, _>("scan_state"),
                source_config_id: row.try_get("source_config_id").ok(),
                extension_metadata: row
                    .try_get::<String, _>("extension_metadata")
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok()),
                tracks,
                external_subtitles,
            }
        })
        .collect();

    let (description, genres) = extract_metadata_fields(metadata_json.as_ref());
    let owner_type = if item_type == "movie" { "movie" } else { "series" };
    let provider_priority: &[&str] = if item_type == "anime" {
        &["anilist", "tvdb", "cinemeta"]
    } else if item_type == "movie" {
        &["cinemeta", "tvdb"]
    } else {
        &["tvdb", "cinemeta"]
    };
    let poster_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "poster",
        &preferred_languages,
        provider_priority,
    )
    .await?;
    let banner_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "banner",
        &preferred_languages,
        provider_priority,
    )
    .await?;
    let backdrop_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "backdrop",
        &preferred_languages,
        provider_priority,
    )
    .await?;

    let poster_url = poster_map.get(&id).cloned();
    let banner_url = banner_map.get(&id).cloned();
    let backdrop_url = backdrop_map.get(&id).cloned();

    let response = LibraryDetailResponse {
        id,
        title,
        r#type: item_type,
        year,
        runtime_seconds,
        external_ids,
        metadata: metadata_json,
        description,
        genres,
        poster_url,
        banner_url,
        backdrop_url,
        files,
    };

    Ok(Json(response))
}

async fn load_media_tracks(
    pool: &sqlx::AnyPool,
    file_ids: &[String],
) -> ApiResult<HashMap<String, Vec<MediaTrackResponse>>> {
    let mut by_file: HashMap<String, Vec<MediaTrackResponse>> = HashMap::new();
    if file_ids.is_empty() {
        return Ok(by_file);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT id, media_file_id, track_type, language, title, codec, channels, CAST(is_default AS INTEGER) AS is_default, CAST(is_forced AS INTEGER) AS is_forced, stream_index FROM media_tracks WHERE media_file_id IN (",
    );
    let mut separated = builder.separated(", ");
    for id in file_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(") ORDER BY media_file_id, track_type, stream_index");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    for row in rows {
        let media_file_id = row.get::<String, _>("media_file_id");
        let entry = by_file.entry(media_file_id).or_default();
        let is_default = row
            .try_get::<i64, _>("is_default")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        let is_forced = row
            .try_get::<i64, _>("is_forced")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        entry.push(MediaTrackResponse {
            id: row.get::<String, _>("id"),
            track_type: row.get::<String, _>("track_type"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            codec: row.try_get::<String, _>("codec").ok(),
            channels: row.try_get::<i64, _>("channels").ok().map(|v| v as i32),
            is_default,
            is_forced,
            stream_index: row.try_get::<i64, _>("stream_index").ok().map(|v| v as i32),
        });
    }

    Ok(by_file)
}

async fn load_external_subtitles(
    pool: &sqlx::AnyPool,
    file_ids: &[String],
) -> ApiResult<HashMap<String, Vec<ExternalSubtitleResponse>>> {
    let mut by_file: HashMap<String, Vec<ExternalSubtitleResponse>> = HashMap::new();
    if file_ids.is_empty() {
        return Ok(by_file);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT id, media_file_id, path, language, title, format, CAST(is_default AS INTEGER) AS is_default, CAST(is_forced AS INTEGER) AS is_forced FROM external_subtitles WHERE media_file_id IN (",
    );
    let mut separated = builder.separated(", ");
    for id in file_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(") ORDER BY media_file_id, path");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    for row in rows {
        let media_file_id = row.get::<String, _>("media_file_id");
        let entry = by_file.entry(media_file_id).or_default();
        let is_default = row
            .try_get::<i64, _>("is_default")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        let is_forced = row
            .try_get::<i64, _>("is_forced")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        entry.push(ExternalSubtitleResponse {
            id: row.get::<String, _>("id"),
            path: row.get::<String, _>("path"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            format: row.try_get::<String, _>("format").ok(),
            is_default,
            is_forced,
        });
    }

    Ok(by_file)
}

struct ArtworkRow {
    owner_id: String,
    id: String,
    url: String,
    language: Option<String>,
    provider: Option<String>,
    score: Option<f32>,
    width: Option<i32>,
    height: Option<i32>,
}

async fn load_primary_artwork(
    pool: &sqlx::AnyPool,
    owner_type: &str,
    owner_ids: &[String],
    kind: &str,
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> ApiResult<HashMap<String, String>> {
    let mut by_owner: HashMap<String, String> = HashMap::new();
    if owner_ids.is_empty() {
        return Ok(by_owner);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT owner_id, id, url, language, provider, score, width, height FROM artwork_refs WHERE owner_type = ",
    );
    builder.push_bind(owner_type);
    builder.push(" AND kind = ");
    builder.push_bind(kind);
    builder.push(" AND owner_id IN (");
    let mut separated = builder.separated(", ");
    for id in owner_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let mut grouped: HashMap<String, Vec<ArtworkRow>> = HashMap::new();
    for row in rows {
        let owner_id = row.get::<String, _>("owner_id");
        grouped.entry(owner_id.clone()).or_default().push(ArtworkRow {
            owner_id,
            id: row.get::<String, _>("id"),
            url: row.get::<String, _>("url"),
            language: row.try_get::<String, _>("language").ok(),
            provider: row.try_get::<String, _>("provider").ok(),
            score: row.try_get::<f64, _>("score").ok().map(|v| v as f32),
            width: row.try_get::<i64, _>("width").ok().map(|v| v as i32),
            height: row.try_get::<i64, _>("height").ok().map(|v| v as i32),
        });
    }

    for (owner_id, candidates) in grouped {
        if let Some(selected) =
            select_primary_artwork(&candidates, preferred_languages, provider_priority)
        {
            let url = format!("/api/v1/artwork/{}", selected.id);
            by_owner.insert(owner_id, url);
        }
    }

    Ok(by_owner)
}

fn extract_metadata_fields(meta: Option<&Value>) -> (Option<String>, Vec<String>) {
    let mut description = None;
    let mut genres = Vec::new();

    if let Some(value) = meta {
        let desc_candidate = value
            .get("description")
            .and_then(Value::as_str)
            .or_else(|| value.get("overview").and_then(Value::as_str))
            .or_else(|| value.get("plot").and_then(Value::as_str))
            .or_else(|| value.get("summary").and_then(Value::as_str));

        if let Some(desc) = desc_candidate {
            let cleaned = clean_description(desc);
            if !cleaned.is_empty() {
                description = Some(cleaned);
            }
        }

        if let Some(arr) = value.get("genres").and_then(Value::as_array) {
            for g in arr {
                if let Some(s) = g.as_str() {
                    genres.push(s.to_string());
                } else if let Some(s) = g
                    .as_object()
                    .and_then(|o| o.get("name"))
                    .and_then(Value::as_str)
                {
                    genres.push(s.to_string());
                }
            }
        }

        if genres.is_empty() {
            if let Some(single) = value.get("genre").and_then(Value::as_str) {
                genres.push(single.to_string());
            }
        }
    }

    (description, genres)
}

fn parse_language_header(headers: &HeaderMap) -> Vec<String> {
    let mut languages = Vec::new();
    if let Some(value) = headers.get(axum::http::header::ACCEPT_LANGUAGE) {
        if let Ok(raw) = value.to_str() {
            for part in raw.split(',') {
                let tag = part.split(';').next().unwrap_or("").trim();
                if tag.is_empty() {
                    continue;
                }
                let normalized = normalize_language(tag);
                if !languages.contains(&normalized) {
                    languages.push(normalized.clone());
                }
                if let Some(base) = normalized.split('-').next() {
                    let base = base.to_string();
                    if !languages.contains(&base) {
                        languages.push(base);
                    }
                }
            }
        }
    }
    if !languages.iter().any(|lang| lang == "en") {
        languages.push("en".to_string());
    }
    languages
}

fn select_primary_artwork<'a>(
    candidates: &'a [ArtworkRow],
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> Option<&'a ArtworkRow> {
    let mut best: Option<&ArtworkRow> = None;
    for candidate in candidates {
        if best.is_none() {
            best = Some(candidate);
            continue;
        }
        let current = best.unwrap();
        if artwork_cmp(candidate, current, preferred_languages, provider_priority).is_lt() {
            best = Some(candidate);
        }
    }
    best
}

fn artwork_cmp(
    left: &ArtworkRow,
    right: &ArtworkRow,
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> std::cmp::Ordering {
    let left_lang = language_rank(left.language.as_deref(), preferred_languages);
    let right_lang = language_rank(right.language.as_deref(), preferred_languages);
    if left_lang != right_lang {
        return left_lang.cmp(&right_lang);
    }

    let left_provider = provider_rank(left.provider.as_deref(), provider_priority);
    let right_provider = provider_rank(right.provider.as_deref(), provider_priority);
    if left_provider != right_provider {
        return left_provider.cmp(&right_provider);
    }

    let left_score = left.score.unwrap_or(0.0);
    let right_score = right.score.unwrap_or(0.0);
    if (left_score - right_score).abs() > f32::EPSILON {
        return right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal);
    }

    let left_area = left.width.unwrap_or(0) * left.height.unwrap_or(0);
    let right_area = right.width.unwrap_or(0) * right.height.unwrap_or(0);
    right_area.cmp(&left_area)
}

fn language_rank(language: Option<&str>, preferred_languages: &[String]) -> usize {
    let Some(language) = language else {
        return preferred_languages.len() + 2;
    };
    let normalized = normalize_language(language);
    for (idx, pref) in preferred_languages.iter().enumerate() {
        if language_matches(&normalized, pref) {
            return idx;
        }
    }
    preferred_languages.len() + 1
}

fn language_matches(candidate: &str, preferred: &str) -> bool {
    if candidate == preferred {
        return true;
    }
    candidate.starts_with(&format!("{}-", preferred))
        || preferred.starts_with(&format!("{}-", candidate))
}

fn normalize_language(value: &str) -> String {
    value.trim().replace('_', "-").to_lowercase()
}

fn provider_rank(provider: Option<&str>, priority: &[&str]) -> usize {
    let Some(provider) = provider else {
        return priority.len();
    };
    priority
        .iter()
        .position(|p| p.eq_ignore_ascii_case(provider))
        .unwrap_or(priority.len())
}

fn extract_episode_description(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    extract_description_from_meta(meta)
}

fn extract_episode_thumbnail(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    meta.get("image")
        .or_else(|| meta.get("imageUrl"))
        .or_else(|| meta.get("thumbnail"))
        .and_then(Value::as_str)
        .map(|value| value.to_string())
}

fn extract_season_description(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    if let Some(desc) = extract_description_from_meta(meta) {
        return Some(desc);
    }
    if let Some(tvdb) = meta.get("tvdb") {
        return extract_description_from_meta(tvdb);
    }
    None
}

fn extract_description_from_meta(meta: &Value) -> Option<String> {
    let desc = meta
        .get("overview")
        .and_then(Value::as_str)
        .or_else(|| meta.get("description").and_then(Value::as_str))
        .or_else(|| meta.get("summary").and_then(Value::as_str));
    desc.map(clean_description).filter(|d| !d.is_empty())
}

fn clean_description(raw: &str) -> String {
    let normalized = raw
        .replace("<br>", "\n")
        .replace("<br />", "\n")
        .replace("<br/>", "\n");
    let mut cleaned = String::with_capacity(normalized.len());
    let mut in_tag = false;
    for ch in normalized.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => cleaned.push(ch),
            _ => {}
        }
    }

    let decoded = html_escape::decode_html_entities(&cleaned);
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}
