use crate::db::models::MediaType;
use crate::extensions::ExternalIds;

#[derive(Debug, Clone)]
pub struct MetadataResult {
    pub metadata_json: serde_json::Value,
    pub runtime_seconds: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub description: Option<String>,
    pub genres: Option<Vec<String>>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryResult {
    pub title: String,
    pub r#type: MediaType,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub description: Option<String>,
}
