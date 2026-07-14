use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use image::GenericImageView;
use serde::Deserialize;
use sqlx::Row;
use std::{
    path::{Path as StdPath, PathBuf},
    time::SystemTime,
};
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    artwork::ArtworkKind,
    http::error::{ApiError, ApiResult},
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct ArtworkQuery {
    pub w: Option<u32>,
    pub h: Option<u32>,
}

pub async fn get_artwork(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ArtworkQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let row = sqlx::query(
        "SELECT ar.url, ar.kind, ac.local_path FROM artwork_refs ar LEFT JOIN artwork_cache ac ON ac.artwork_id = ar.id WHERE ar.id = $1 LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let Some(row) = row else {
        return Err(ApiError::not_found("artwork not found"));
    };

    let url: Option<String> = row.try_get("url").ok();
    let kind: Option<String> = row.try_get("kind").ok();
    let mut local_path: Option<String> = row.try_get("local_path").ok();

    if let Some(path) = local_path.as_ref() {
        if tokio::fs::metadata(path).await.is_err() {
            local_path = None;
        }
    }

    if local_path.is_none() {
        if let (Some(url), Some(kind)) = (url.as_deref(), kind.as_deref()) {
            if let Ok(artwork_id) = Uuid::parse_str(&id) {
                let parsed_kind = ArtworkKind::from_str(kind).unwrap_or(ArtworkKind::Poster);
                if let Ok(path) = state
                    .artwork
                    .ensure_cached(&state.db_pool, artwork_id, parsed_kind, url)
                    .await
                {
                    local_path = path;
                }
            }
        }
    }

    if let Some(path) = local_path {
        let resolved = resolve_variant_path(&state, &id, &path, &query).await?;
        return serve_file(&resolved, &headers).await;
    }

    if let Some(url) = url {
        let response = axum::response::Redirect::temporary(&url).into_response();
        return Ok(response);
    }

    Ok(StatusCode::NOT_FOUND.into_response())
}

async fn resolve_variant_path(
    state: &AppState,
    artwork_id: &str,
    local_path: &str,
    query: &ArtworkQuery,
) -> ApiResult<PathBuf> {
    if query.w.is_none() && query.h.is_none() {
        return Ok(PathBuf::from(local_path));
    }
    ensure_resized_variant(state, artwork_id, local_path, query.w, query.h).await
}

async fn ensure_resized_variant(
    state: &AppState,
    artwork_id: &str,
    local_path: &str,
    width: Option<u32>,
    height: Option<u32>,
) -> ApiResult<PathBuf> {
    let original_path = PathBuf::from(local_path);
    let width = width.filter(|value| *value > 0);
    let height = height.filter(|value| *value > 0);
    if width.is_none() && height.is_none() {
        return Ok(original_path);
    }
    let (orig_w, orig_h) = image_dimensions(&original_path).await?;

    let (mut target_w, mut target_h) = if let Some(w) = width {
        if let Some(h) = height {
            (w, h)
        } else {
            let ratio = orig_h as f32 / orig_w as f32;
            let computed_h = ((w as f32) * ratio).round().max(1.0) as u32;
            (w, computed_h)
        }
    } else if let Some(h) = height {
        let ratio = orig_w as f32 / orig_h as f32;
        let computed_w = ((h as f32) * ratio).round().max(1.0) as u32;
        (computed_w, h)
    } else {
        return Ok(original_path);
    };

    if target_w > orig_w {
        target_w = orig_w;
    }
    if target_h > orig_h {
        target_h = orig_h;
    }

    if target_w == orig_w && target_h == orig_h {
        return Ok(original_path);
    }

    let ext = file_extension_or_default(&original_path);
    let variant_dir = state.artwork.cache_dir().join("variants");
    tokio::fs::create_dir_all(&variant_dir)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let variant_path =
        variant_dir.join(format!("{}_{}x{}.{}", artwork_id, target_w, target_h, ext));

    if tokio::fs::metadata(&variant_path).await.is_ok() {
        return Ok(variant_path);
    }

    let original_path_clone = original_path.clone();
    let variant_path_clone = variant_path.clone();
    let format = image_format_from_extension(&ext).unwrap_or(image::ImageFormat::Jpeg);
    tokio::task::spawn_blocking(move || -> Result<(), image::ImageError> {
        let image = image::open(&original_path_clone)?;
        let resized = image.resize(target_w, target_h, image::imageops::FilterType::Triangle);
        resized.save_with_format(&variant_path_clone, format)?;
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(variant_path)
}

async fn image_dimensions(path: &StdPath) -> ApiResult<(u32, u32)> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(u32, u32), image::ImageError> {
        let image = image::open(path)?;
        Ok(image.dimensions())
    })
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .map_err(|e| ApiError::internal(e.to_string()))
}

async fn serve_file(path: &StdPath, headers: &HeaderMap) -> ApiResult<Response> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let etag = format!("W/\"{}-{}\"", metadata.len(), system_time_seconds(modified));

    if let Some(value) = headers.get(header::IF_NONE_MATCH) {
        if value.to_str().ok().map(|s| s.trim()) == Some(etag.as_str()) {
            let mut response = StatusCode::NOT_MODIFIED.into_response();
            set_cache_headers(&mut response, &etag, modified, metadata.len(), path);
            return Ok(response);
        }
    }

    if let Some(value) = headers.get(header::IF_MODIFIED_SINCE) {
        if let Ok(value) = value.to_str() {
            if let Ok(since) = httpdate::parse_http_date(value) {
                if modified <= since {
                    let mut response = StatusCode::NOT_MODIFIED.into_response();
                    set_cache_headers(&mut response, &etag, modified, metadata.len(), path);
                    return Ok(response);
                }
            }
        }
    }

    let file = File::open(path)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let content_type = content_type_for_path(&path.to_string_lossy());
    let stream = ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);
    let mut response = body.into_response();
    if let Some(content_type) = content_type {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            content_type
                .parse()
                .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream")),
        );
    }
    set_cache_headers(&mut response, &etag, modified, metadata.len(), path);
    Ok(response)
}

fn set_cache_headers(
    response: &mut Response,
    etag: &str,
    modified: SystemTime,
    size: u64,
    path: &StdPath,
) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=604800"),
    );
    if let Ok(value) = header::HeaderValue::from_str(etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&httpdate::fmt_http_date(modified)) {
        response.headers_mut().insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&size.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    if let Some(content_type) = content_type_for_path(&path.to_string_lossy()) {
        if let Ok(value) = header::HeaderValue::from_str(content_type) {
            response.headers_mut().insert(header::CONTENT_TYPE, value);
        }
    }
}

fn system_time_seconds(value: SystemTime) -> u64 {
    value
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .unwrap_or_default()
}

fn file_extension_or_default(path: &StdPath) -> String {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .filter(|ext| !ext.is_empty())
        .unwrap_or_else(|| "jpg".to_string())
}

fn image_format_from_extension(ext: &str) -> Option<image::ImageFormat> {
    match ext {
        "jpg" | "jpeg" => Some(image::ImageFormat::Jpeg),
        "png" => Some(image::ImageFormat::Png),
        "webp" => Some(image::ImageFormat::WebP),
        _ => None,
    }
}

fn content_type_for_path(path: &str) -> Option<&'static str> {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "webp" => Some("image/webp"),
        "gif" => Some("image/gif"),
        _ => None,
    }
}
