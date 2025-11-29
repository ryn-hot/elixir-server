use crate::extensions::ExternalIds;

#[derive(Debug, Clone)]
pub struct MetadataResult {
    pub metadata_json: serde_json::Value,
    pub runtime_seconds: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub description: Option<String>,
    pub genres: Option<Vec<String>>,
}
