//! Deterministic scheduler/SQL tests only, not native or VPN qualification.
use super::*;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn records() -> Result<SecurityRecords> {
    let value: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    Ok(SecurityRecords {
        schema_version: 1,
        extension_id: serde_json::from_value(value["extension_id"].clone())?,
        publisher: serde_json::from_value(value["publisher"].clone())?,
        head: None,
        revocations: vec![],
    })
}
struct Mock {
    records: BTreeMap<ExtensionId, SecurityRecords>,
    needed: AtomicBool,
    fail_first: AtomicBool,
    unavailable: AtomicBool,
    heads: AtomicUsize,
    verified: AtomicUsize,
}
impl Mock {
    fn new() -> Result<Self> {
        let base = records()?;
        let mut values = BTreeMap::new();
        for name in ["a", "b"] {
            let mut record = base.clone();
            record.extension_id = ExtensionId::new(&base.publisher.payload.root_key, name)?;
            values.insert(record.extension_id.clone(), record);
        }
        Ok(Self {
            records: values,
            needed: AtomicBool::new(true),
            fail_first: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            heads: AtomicUsize::new(0),
            verified: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl Backend for Mock {
    async fn next(&self, after: Option<&ExtensionId>) -> Result<Option<ExtensionId>> {
        Ok(self
            .records
            .keys()
            .find(|id| after.is_none_or(|after| *id > after))
            .cloned())
    }
    async fn head(&self, id: &ExtensionId) -> Result<Option<SecurityRecords>> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        Ok((!self.unavailable.load(Ordering::SeqCst)).then(|| self.records[id].clone()))
    }
    async fn needed(&self, _: &SecurityRecords) -> Result<bool> {
        Ok(self.needed.load(Ordering::SeqCst))
    }
    async fn verify_and_retain(&self, value: &SecurityRecords) -> Result<()> {
        self.verified.fetch_add(1, Ordering::SeqCst);
        if self.fail_first.load(Ordering::SeqCst)
            && value.extension_id == *self.records.keys().next().unwrap()
        {
            bail!("synthetic positively unsent busy verifier");
        }
        Ok(())
    }
}
#[tokio::test]
async fn duplicate_and_absent_local_heads_do_not_launch_a_helper_or_grow_state() -> Result<()> {
    let backend = Mock::new()?;
    let mut scan = Scan::default();
    backend.needed.store(false, Ordering::SeqCst);
    for _ in 0..100 {
        scan.poll(&backend).await?;
        assert!(scan.pending.is_none());
        assert_eq!(scan.failures, 0);
    }
    assert_eq!(backend.verified.load(Ordering::SeqCst), 0);
    backend.unavailable.store(true, Ordering::SeqCst);
    scan.poll(&backend).await?;
    assert!(scan.pending.is_none());
    assert_eq!(backend.verified.load(Ordering::SeqCst), 0);
    Ok(())
}
#[tokio::test]
async fn busy_identity_keeps_one_input_then_advances_fairly_without_refetching_each_retry()
-> Result<()> {
    let backend = Mock::new()?;
    backend.fail_first.store(true, Ordering::SeqCst);
    let mut scan = Scan::default();
    for attempt in 1..=3 {
        assert!(scan.poll(&backend).await.is_err());
        assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
        assert_eq!(scan.pending.is_some(), attempt < 3);
    }
    scan.poll(&backend).await?;
    assert!(scan.pending.is_none());
    assert_eq!(backend.heads.load(Ordering::SeqCst), 2);
    assert_eq!(backend.verified.load(Ordering::SeqCst), 4);
    assert_eq!(scan.cursor.as_ref(), backend.records.keys().last());
    Ok(())
}
#[tokio::test]
async fn mismatched_pending_scope_is_not_verified() -> Result<()> {
    let backend = Mock::new()?;
    let mut scan = Scan::default();
    let mut values = backend.records.values();
    let first = values.next().unwrap();
    let second = values.next().unwrap();
    scan.pending = Some(Pending {
        extension: first.extension_id.clone(),
        records: Some(second.clone()),
    });
    assert!(scan.poll(&backend).await.is_err());
    assert_eq!(backend.verified.load(Ordering::SeqCst), 0);
    Ok(())
}
#[tokio::test]
async fn installed_selector_is_independent_of_auto_update_and_enable_flags_and_uses_a_keyset()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&crate::config::DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("selector.sqlite").display()
        ),
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await?;
    // Selector-only schema. No fake ACTIVE journal/permission or code execution.
    for sql in [
        "CREATE TABLE marketplace_installations(extension_id TEXT NOT NULL,instance_id TEXT NOT NULL,PRIMARY KEY(extension_id,instance_id))",
        "CREATE TABLE marketplace_extension_security(extension_id TEXT PRIMARY KEY)",
    ] {
        sqlx::query(sql).execute(&database.pool).await?;
    }
    let backend = Mock::new()?;
    let ids = backend.records.keys().cloned().collect::<Vec<_>>();
    for id in &ids {
        sqlx::query("INSERT INTO marketplace_extension_security VALUES($1)")
            .bind(id.as_str())
            .execute(&database.pool)
            .await?;
        for instance in ["first", "second"] {
            sqlx::query("INSERT INTO marketplace_installations VALUES($1,$2)")
                .bind(id.as_str())
                .bind(instance)
                .execute(&database.pool)
                .await?;
        }
    }
    assert_eq!(next_installed(&database, None).await?, Some(ids[0].clone()));
    assert_eq!(
        next_installed(&database, Some(&ids[0])).await?,
        Some(ids[1].clone())
    );
    assert_eq!(next_installed(&database, Some(&ids[1])).await?, None);
    database.pool.close().await;
    Ok(())
}
