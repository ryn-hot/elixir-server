//! Small process-local cache of content-addressed image configuration only.
//! No tags, repository manifests, negative results or native runtime state.
use super::*;
use std::collections::VecDeque;

const MAX_IMAGES: usize = 4;
const MAX_LABELS: usize = 128;
const MAX_LABEL_BYTES: usize = 16 * 1024;

pub(super) fn immutable_reference(image: &str) -> bool {
    image.strip_prefix("sha256:").is_some_and(immutable_id)
}
pub(super) fn require_identity(image: &str, actual: &Value) -> Result<()> {
    if !immutable_reference(image) || actual.get("Id").and_then(Value::as_str) != Some(image) {
        bail!("immutable image label inspection returned another image");
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct Cache(VecDeque<(String, HashMap<String, String>)>);
impl Cache {
    pub(super) fn get(&self, image: &str) -> Option<HashMap<String, String>> {
        if !immutable_reference(image) {
            return None;
        }
        self.0
            .iter()
            .find(|(id, _)| id == image)
            .map(|(_, labels)| labels.clone())
    }
    pub(super) fn insert(&mut self, image: &str, labels: &HashMap<String, String>) {
        if !immutable_reference(image)
            || labels.len() > MAX_LABELS
            || labels
                .iter()
                .try_fold(0usize, |n, (key, value)| {
                    n.checked_add(key.len())?.checked_add(value.len())
                })
                .is_none_or(|bytes| bytes > MAX_LABEL_BYTES)
        {
            return;
        }
        // First successfully inspected value wins for this immutable ID.
        // Bounded FIFO eviction needs no per-request timestamp/history.
        if self.0.iter().any(|(id, _)| id == image) {
            return;
        }
        if self.0.len() == MAX_IMAGES {
            self.0.pop_front();
        }
        self.0.push_back((image.to_owned(), labels.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: usize) -> String {
        format!("sha256:{n:064x}")
    }
    fn labels() -> HashMap<String, String> {
        HashMap::from([(
            "elixir.marketplace.worker.sdk_http_stream".into(),
            "1".into(),
        )])
    }
    #[test]
    fn image_label_cache_rejects_mutable_references_and_mismatched_native_identity() {
        let mut cache = Cache::default();
        for reference in [
            "worker:latest",
            "worker@sha256:bad",
            "sha256:short",
            &"a".repeat(64),
            &format!("sha256:{}", "A".repeat(64)),
        ] {
            cache.insert(reference, &labels());
            assert!(cache.get(reference).is_none());
            assert!(require_identity(reference, &serde_json::json!({"Id":reference})).is_err());
        }
        assert!(cache.0.is_empty());
        assert!(require_identity(&id(1), &serde_json::json!({"Id":id(1)})).is_ok());
        for actual in [
            serde_json::json!({"Id":id(2)}),
            serde_json::json!({}),
            serde_json::json!({"Id":null}),
        ] {
            assert!(require_identity(&id(1), &actual).is_err());
        }
    }
    #[test]
    fn image_label_cache_is_small_fifo_and_returns_independent_metadata() {
        let mut cache = Cache::default();
        let original = labels();
        for n in 0..MAX_IMAGES {
            cache.insert(&id(n), &original);
        }
        let mut copied = cache.get(&id(0)).unwrap();
        copied.clear();
        assert_eq!(cache.get(&id(0)).unwrap(), original);
        cache.insert(&id(0), &HashMap::new());
        assert_eq!(cache.get(&id(0)).unwrap(), original);
        cache.insert(&id(MAX_IMAGES), &original);
        assert_eq!(cache.0.len(), MAX_IMAGES);
        assert!(cache.get(&id(0)).is_none());
        assert_eq!(cache.get(&id(MAX_IMAGES)).unwrap(), original);
        for n in 5..100 {
            cache.insert(&id(n), &original);
        }
        assert_eq!(cache.0.len(), MAX_IMAGES);
    }
    #[test]
    fn oversized_metadata_is_not_retained_and_cannot_evict_valid_entries() {
        let mut cache = Cache::default();
        cache.insert(&id(1), &labels());
        let oversized = HashMap::from([("x".into(), "x".repeat(MAX_LABEL_BYTES))]);
        cache.insert(&id(2), &oversized);
        let many = (0..=MAX_LABELS)
            .map(|n| (n.to_string(), "x".into()))
            .collect();
        cache.insert(&id(3), &many);
        assert_eq!(cache.0.len(), 1);
        assert!(cache.get(&id(2)).is_none());
        assert!(cache.get(&id(3)).is_none());
        assert_eq!(cache.get(&id(1)).unwrap(), labels());
    }
    #[tokio::test]
    async fn cached_image_metadata_never_substitutes_for_live_native_inspection_or_tags()
    -> Result<()> {
        // Deliberately unavailable CLI: only the known immutable metadata hit
        // can succeed. This tests wiring, not an actual engine/profile/health.
        let runtime = DockerRuntimeManager::new(Some("/must-not-run-image-cache-fixture".into()));
        runtime
            .immutable_image_labels
            .lock()
            .await
            .insert(&id(1), &labels());
        assert_eq!(runtime.image_labels(&id(1)).await?, labels());
        assert!(runtime.image_labels("worker:latest").await.is_err());
        assert!(runtime.image_labels(&id(2)).await.is_err());
        let handle = ContainerHandle {
            id: "a".repeat(64),
            name: "a".repeat(64),
        };
        assert!(runtime.inspect(&handle).await.is_err());
        let cache = runtime.immutable_image_labels.lock().await;
        assert_eq!(
            cache.0.len(),
            1,
            "failures must not be retained or replace valid metadata"
        );
        assert!(cache.get(&id(2)).is_none());
        Ok(())
    }
}
