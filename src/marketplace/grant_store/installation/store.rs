use super::*;
use record::OperationalMetadata;

// Read one character beyond every valid field length so corrupt oversized DB
// values are rejected without unbounded allocation. Manifest strings are at
// most 64 KiB (therefore at most 65536 characters).
pub(super) const EXTENSION_METADATA: &str = "SELECT extension_id,SUBSTR(name,1,129) AS name,SUBSTR(version,1,65) AS version,SUBSTR(kind,1,16) AS kind,SUBSTR(publisher_name,1,129) AS publisher_name,SUBSTR(signing_key_id,1,129) AS signing_key_id,SUBSTR(trust_level,1,16) AS trust_level,SUBSTR(manifest_json,1,65537) AS manifest_json,SUBSTR(package_hash,1,72) AS package_hash,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled FROM extensions WHERE extension_id=$1";

const OPERATIONAL_INSTANCE_REFERENCES: &[(&str, &str)] = &[
    ("providers", "instance_id"),
    ("runtime_logs", "instance_id"),
    ("debrid_download_jobs", "instance_id"),
    ("extension_source_registries", "instance_id"),
    ("extension_source_modules", "instance_id"),
    ("extension_source_module_certifications", "instance_id"),
    ("extension_source_module_quarantines", "instance_id"),
    ("extension_source_certification_jobs", "instance_id"),
    ("media_segment_provider_certifications", "instance_id"),
    ("media_ownerships", "owner_instance_id"),
];

pub(super) struct Ledger {
    revision: i64,
    entries: i64,
    bytes: i64,
}
// At most 4096 journals exist. Reserve one final writer revision per journal
// so unrelated normal mutations cannot exhaust pre-migration abort capacity.
const RESUME_RESERVE: i64 = 4096;
impl Ledger {
    pub(super) fn require_normal_capacity(&self) -> Result<()> {
        if self.revision >= MAX_REVISION - RESUME_RESERVE {
            bail!("installation revision capacity reserved for safe resume");
        }
        Ok(())
    }
}

/// Cleanup/status uses only the home and revision binding, not enabled status
/// or grant-review JSON. Corrupt installation data must never block revocation.
pub(super) async fn lock_grant_binding(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    home: Uuid,
    id: &ExtensionId,
) -> Result<()> {
    let query = "SELECT home_id,revision,revocation_revision FROM marketplace_extension_grants WHERE extension_id=$1";
    let query = if driver == DatabaseDriver::Postgres {
        format!("{query} FOR SHARE")
    } else {
        query.into()
    };
    let grant = sqlx::query(&query)
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .context("installation grant binding is missing; no implicit repair")?;
    let revision: i64 = grant.try_get("revision")?;
    let revoked: i64 = grant.try_get("revocation_revision")?;
    if grant.try_get::<String, _>("home_id")? != home.to_string()
        || !(1..=MAX_REVISION).contains(&revision)
        || !(0..=revision).contains(&revoked)
    {
        bail!("installation grant home or revision differs");
    }
    Ok(())
}
pub(super) async fn lock_budget(
    tx: &mut Transaction<'_, Any>,
    limit: InstallBudget,
) -> Result<Ledger> {
    // Taking the writer position first also serializes SQLite's absent-row case.
    // SQLite RETURNING may expose a computed integer as 32-bit through Any.
    // Round-trip this 53-bit counter as decimal text, never truncate it.
    let row=sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1 RETURNING CAST(revision AS TEXT) AS revision,entries,encoded_bytes")
        .fetch_optional(&mut **tx).await?.context("installation budget is missing; no implicit reset")?;
    let value = Ledger {
        revision: row.try_get::<String, _>("revision")?.parse()?,
        entries: row.try_get("entries")?,
        bytes: row.try_get("encoded_bytes")?,
    };
    let actual:(i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT) FROM marketplace_installations").fetch_one(&mut **tx).await?;
    if !(0..=MAX_REVISION).contains(&value.revision)
        || value.entries < 0
        || value.bytes < 0
        || actual != (value.entries, value.bytes)
        || value.entries > limit.entries as i64
        || value.bytes > limit.bytes as i64
    {
        bail!("installation budget is corrupt or exceeds the owner allocation");
    }
    Ok(value)
}
pub(super) async fn load(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    id: &ExtensionId,
) -> Result<Option<Stored>> {
    load_locked(tx, driver, id, None, true).await
}
pub(super) async fn load_configured(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    id: &ExtensionId,
    instance: Option<Uuid>,
) -> Result<Option<Stored>> {
    load_locked(tx, driver, id, instance, true).await
}
pub(super) async fn load_shared(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    id: &ExtensionId,
) -> Result<Option<Stored>> {
    load_locked(tx, driver, id, None, false).await
}
pub(super) async fn load_configured_shared(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    id: &ExtensionId,
    instance: Option<Uuid>,
) -> Result<Option<Stored>> {
    load_locked(tx, driver, id, instance, false).await
}
async fn load_locked(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    id: &ExtensionId,
    instance: Option<Uuid>,
    writing: bool,
) -> Result<Option<Stored>> {
    // The grant is already share-locked by the caller. Derive the revocation
    // boundary from that authority, never from mutable/corrupt journal data.
    let scope = if instance.is_some() {
        "i.instance_id=$2"
    } else {
        "i.instance_id IS NULL"
    };
    let base = format!(
        "SELECT i.extension_id,SUBSTR(i.instance_id,1,37) AS instance_id,i.home_id,i.revision,g.revision AS grant_revision,g.revocation_revision,SUBSTR(i.record_json,1,409601) AS record_json,i.record_bytes,i.record_sha256,i.encoded_bytes FROM marketplace_installations i JOIN marketplace_extension_grants g ON g.extension_id=i.extension_id WHERE i.extension_id=$1 AND {scope}"
    );
    let query = if driver == DatabaseDriver::Postgres {
        format!(
            "{base} FOR {} OF i",
            if writing { "UPDATE" } else { "SHARE" }
        )
    } else {
        base.to_owned()
    };
    let mut query = sqlx::query(&query).bind(id.as_str());
    if let Some(instance) = instance {
        query = query.bind(instance.to_string());
    }
    query
        .fetch_optional(&mut **tx)
        .await?
        .map(Stored::parse)
        .transpose()
}

/// Effects share the ledger with each other, but block installation writers.
/// SQLite callers have already acquired the home-binding writer position.
pub(super) async fn read_budget(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    limit: InstallBudget,
) -> Result<()> {
    limit.validate()?;
    let base = "SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget WHERE singleton=1";
    let query = if driver == DatabaseDriver::Postgres {
        format!("{base} FOR SHARE")
    } else {
        base.to_owned()
    };
    let row = sqlx::query(&query)
        .fetch_optional(&mut **tx)
        .await?
        .context("installed authority budget is missing; no implicit reset")?;
    let revision: i64 = row.try_get("revision")?;
    let entries: i64 = row.try_get("entries")?;
    let bytes: i64 = row.try_get("encoded_bytes")?;
    let actual: (i64, i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT) FROM marketplace_installations")
        .fetch_one(&mut **tx).await?;
    if !(0..=MAX_REVISION).contains(&revision)
        || !(1..=limit.entries as i64).contains(&entries)
        || bytes <= 0
        || bytes > limit.bytes as i64
        || actual != (entries, bytes)
    {
        bail!("installed authority accounting is corrupt or outside its allocation");
    }
    Ok(())
}

pub(super) async fn require_active_metadata(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    if journal.configured_instance.is_some() {
        return configured::require_active_metadata(tx, driver, journal).await;
    }
    let active = journal
        .active
        .as_ref()
        .context("no active installed source")?;
    let query = if driver == DatabaseDriver::Postgres {
        format!("{EXTENSION_METADATA} FOR SHARE")
    } else {
        EXTENSION_METADATA.to_owned()
    };
    let row = sqlx::query(&query)
        .bind(journal.extension.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .context("active extension metadata is missing")?;
    if row.try_get::<i64, _>("enabled")? != 1
        || metadata_from_row(&row)?.id()? != active.metadata_id()?
    {
        bail!("active extension metadata or enabled binding differs");
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1")
            .bind(journal.extension.as_str())
            .fetch_one(&mut **tx)
            .await?;
    if count != journal.slots().count() as i64 {
        bail!("active extension has missing or unrecorded generations");
    }
    Ok(())
}
pub(super) async fn prepare_extension(
    tx: &mut Transaction<'_, Any>,
    material: &Material,
    previous: Option<&Stored>,
) -> Result<()> {
    let id = material.review.scope().extension_id();
    let existing = sqlx::query(EXTENSION_METADATA)
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    match (previous, existing) {
        (None, None) => {
            write_disabled_metadata(tx, material, true).await?;
        }
        (Some(previous), Some(row)) => {
            if metadata_from_row(&row)?.id()? != metadata_id(&previous.journal)? {
                bail!("extension metadata differs from its installation record");
            }
            if let Some(active) = &previous.journal.active {
                if active.facts()?.release().release_id() == material.review.release_id() {
                    bail!("chosen release is already active");
                }
            } else {
                if previous.journal.cancelled.is_none()
                    || previous.journal.pending.is_some()
                    || row.try_get::<i64, _>("enabled")? != 0
                {
                    bail!("existing installation has no active or cancelled generation");
                }
                // This is the known disabled identity, not an adopted legacy
                // row. The new material has independently passed every gate.
                write_disabled_metadata(tx, material, false).await?;
            }
        }
        _ => {
            bail!("extension metadata exists without its journal or is missing; no adoption/repair")
        }
    }
    Ok(())
}

async fn write_disabled_metadata(
    tx: &mut Transaction<'_, Any>,
    material: &Material,
    insert: bool,
) -> Result<()> {
    write_metadata(tx, material, insert, true).await
}
pub(super) async fn write_metadata(
    tx: &mut Transaction<'_, Any>,
    material: &Material,
    insert: bool,
    disabled: bool,
) -> Result<()> {
    let release = material.authority.verify()?;
    let metadata = OperationalMetadata::from_release(release.release(), &material.manifest)?;
    let query = if insert {
        "INSERT INTO extensions (extension_id,name,version,kind,publisher_name,signing_key_id,trust_level,manifest_json,package_hash,enabled) VALUES ($1,$2,$3,$4,$5,$6,'community',$7,$8,FALSE)"
    } else if disabled {
        "UPDATE extensions SET name=$2,version=$3,kind=$4,publisher_name=$5,signing_key_id=$6,trust_level='community',manifest_json=$7,package_hash=$8 WHERE extension_id=$1 AND enabled=FALSE"
    } else {
        "UPDATE extensions SET name=$2,version=$3,kind=$4,publisher_name=$5,signing_key_id=$6,trust_level='community',manifest_json=$7,package_hash=$8 WHERE extension_id=$1"
    };
    let changed = sqlx::query(query)
        .bind(metadata.extension.as_str())
        .bind(&metadata.name)
        .bind(&metadata.version)
        .bind(&metadata.kind)
        .bind(&metadata.publisher)
        .bind(&metadata.key)
        .bind(&metadata.manifest)
        .bind(metadata.package.to_string())
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if changed != 1 {
        bail!("disabled extension metadata changed");
    }
    Ok(())
}

fn metadata_id(journal: &Journal) -> Result<Digest> {
    if let Some(capsule) = journal.active.as_ref().or(journal.pending.as_ref()) {
        capsule.metadata_id()
    } else {
        let cancelled = journal
            .cancelled
            .as_ref()
            .context("installation source metadata missing")?;
        Ok(cancelled.metadata)
    }
}
pub(super) fn metadata_from_row(row: &AnyRow) -> Result<OperationalMetadata> {
    Ok(OperationalMetadata {
        extension: row.try_get::<String, _>("extension_id")?.parse()?,
        name: row.try_get("name")?,
        version: row.try_get("version")?,
        kind: row.try_get("kind")?,
        publisher: row.try_get("publisher_name")?,
        key: row.try_get("signing_key_id")?,
        trust: row.try_get("trust_level")?,
        manifest: row.try_get("manifest_json")?,
        package: row.try_get::<String, _>("package_hash")?.parse()?,
    })
}
pub(super) async fn verify_instances(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    if journal.configured_instance.is_some() {
        return configured::verify(tx, driver, journal).await;
    }
    let ext_query = EXTENSION_METADATA;
    let ext_query = if driver == DatabaseDriver::Postgres {
        format!("{ext_query} FOR SHARE")
    } else {
        ext_query.to_owned()
    };
    let ext = sqlx::query(&ext_query)
        .bind(journal.extension.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .context("installed extension metadata is missing")?;
    if metadata_from_row(&ext)?.id()? != metadata_id(journal)?
        || (journal.active.is_none() && ext.try_get::<i64, _>("enabled")? != 0)
    {
        bail!("installed extension metadata or inactive state differs");
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1")
            .bind(journal.extension.as_str())
            .fetch_one(&mut **tx)
            .await?;
    if count != journal.slots().count() as i64 {
        bail!("installation has unrecorded or missing instances");
    }
    if let Some(cancelled) = &journal.cancelled {
        let present: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM extension_instances WHERE instance_id=$1) OR EXISTS(SELECT 1 FROM marketplace_instance_sdk_state WHERE instance_id=$1) OR EXISTS(SELECT 1 FROM marketplace_sdk_namespaces WHERE namespace_id=$1) THEN 1 ELSE 0 END AS BIGINT)")
            .bind(cancelled.instance.to_string()).fetch_one(&mut **tx).await?;
        if present != 0 {
            bail!("cancelled installation instance/state has reappeared");
        }
    }
    let base = "SELECT i.extension_id,i.instance_name,i.config_json,CAST(CASE WHEN i.enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,s.extension_id AS state_extension,s.home_id,s.revision,s.key_count,s.encoded_bytes,s.purpose FROM extension_instances i JOIN marketplace_instance_sdk_state s ON s.instance_id=i.instance_id WHERE i.instance_id=$1";
    let query = if driver == DatabaseDriver::Postgres {
        format!("{base} FOR UPDATE OF i,s")
    } else {
        base.to_owned()
    };
    for capsule in journal.slots() {
        super::super::sdk::namespaces::require_identity(
            tx,
            journal.home,
            &journal.extension,
            capsule.instance,
            super::super::sdk::namespaces::Purpose::Installed,
        )
        .await?;
        let row = sqlx::query(&query)
            .bind(capsule.instance.to_string())
            .fetch_optional(&mut **tx)
            .await?
            .context("installed instance or state head is missing")?;
        if row.try_get::<String, _>("extension_id")? != journal.extension.as_str()
            || row.try_get::<String, _>("state_extension")? != journal.extension.as_str()
            || row.try_get::<String, _>("home_id")? != journal.home.to_string()
            || row.try_get::<String, _>("purpose")? != "installed"
            || (journal
                .pending
                .as_ref()
                .is_some_and(|p| p.instance == capsule.instance)
                && row.try_get::<i64, _>("enabled")? != 0)
            || (journal
                .previous
                .as_ref()
                .is_some_and(|p| p.instance == capsule.instance)
                && row.try_get::<i64, _>("enabled")? != 0)
        {
            bail!("installed instance/state scope or inactive generation differs");
        }
        if journal
            .pending
            .as_ref()
            .is_some_and(|p| p.instance == capsule.instance)
        {
            let keys: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1",
            )
            .bind(capsule.instance.to_string())
            .fetch_one(&mut **tx)
            .await?;
            let copied = journal.activation.as_ref().is_some_and(|f| {
                matches!(
                    f.phase,
                    record::ActivationPhase::StateCopied
                        | record::ActivationPhase::CandidateStarting
                )
            });
            if (!copied
                && (row.try_get::<i64, _>("revision")? != 1
                    || row.try_get::<i64, _>("key_count")? != 0
                    || row.try_get::<i64, _>("encoded_bytes")? != 0
                    || keys != 0))
                || row.try_get::<Option<String>, _>("config_json")?.is_some()
                || row.try_get::<String, _>("instance_name")?
                    != format!("marketplace-{}", capsule.instance)
            {
                bail!("inactive installation has unexpected mutable state");
            }
            require_no_operational_dependants(tx, capsule.instance).await?;
            if !copied {
                require_no_secrets(tx, capsule.instance).await?;
            }
        }
    }
    if journal
        .activation
        .as_ref()
        .is_some_and(|f| f.phase == record::ActivationPhase::StateCopied)
    {
        activation::state_copy::verify(tx, driver, journal).await?;
    }
    if journal
        .activation
        .as_ref()
        .is_some_and(|f| f.phase == record::ActivationPhase::CandidateStarting)
    {
        activation::state_copy::verify_candidate(tx, driver, journal).await?;
    }
    Ok(())
}

pub(super) async fn require_no_operational_dependants(
    tx: &mut Transaction<'_, Any>,
    instance: Uuid,
) -> Result<()> {
    // Fixed schema names only, never peer input. Pending candidates cannot have
    // providers, logs or scraper/download/media work. Copied secrets have their
    // own immutable seal; untouched candidates must have none. Hold the parent
    // instance FOR UPDATE (SQLite writer position) before these child checks.
    // Keep this list aligned with operational instance FKs, not SDK-state FKs.
    for (table, column) in OPERATIONAL_INSTANCE_REFERENCES {
        let present: i64 = sqlx::query_scalar(&format!(
            "SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM {table} WHERE {column}=$1) THEN 1 ELSE 0 END AS BIGINT)"
        ))
        .bind(instance.to_string())
        .fetch_one(&mut **tx)
        .await?;
        if present != 0 {
            bail!("unstarted installation has operational dependants");
        }
    }
    Ok(())
}
pub(super) async fn require_no_secrets(
    tx: &mut Transaction<'_, Any>,
    instance: Uuid,
) -> Result<()> {
    let secrets: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM secrets WHERE scope='instance' AND scope_id=$1) THEN 1 ELSE 0 END AS BIGINT)")
        .bind(instance.to_string()).fetch_one(&mut **tx).await?;
    if secrets != 0 {
        bail!("unstarted installation has unexpected secrets");
    }
    Ok(())
}

pub(super) async fn write(
    tx: &mut Transaction<'_, Any>,
    ledger: Ledger,
    previous: Option<&Stored>,
    journal: Journal,
    floor: i64,
    budget: InstallBudget,
) -> Result<Stored> {
    write_inner(tx, ledger, previous, journal, floor, budget, false).await
}

/// Only original joined retirement can use the reserved final revision for
/// phase-six cleanup. This cannot alter sources, instances or enabled flags.
pub(super) async fn write_retired_cleanup(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    ledger: Ledger,
    old: &Stored,
    journal: Journal,
    retired: &runtime::RetiredCandidate,
) -> Result<Stored> {
    retired.require_owner(tx, driver).await?;
    retired.binding().require_cleanup_journal(old)?;
    let mut expected = serde_json::to_value(&old.journal)?;
    expected["revision"] = serde_json::json!(
        old.journal
            .revision
            .checked_add(1)
            .context("cleanup revision exhausted")?
    );
    expected["mutation"] = serde_json::json!(journal.mutation);
    expected["request"] = serde_json::json!(journal.request);
    expected["approved_by"] = serde_json::json!(journal.approved_by);
    expected
        .as_object_mut()
        .context("cleanup journal missing")?
        .remove("activation");
    if journal.activation.is_some() || expected != serde_json::to_value(&journal)? {
        bail!("retired cleanup cannot change installation generations or source metadata");
    }
    write_inner(
        tx,
        ledger,
        Some(old),
        journal,
        old.floor,
        retired.binding().budget(),
        true,
    )
    .await
}

/// Closed, non-growing cancellation under ORIGINAL retirement or an original
/// never-launched claim permanently consumed into its cleanup-only owner.
/// Only its exact authenticated tombstone may consume the final reserved write.
pub(super) async fn write_candidate_cancellation(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    ledger: Ledger,
    old: &Stored,
    journal: Journal,
    closed: &runtime::ClosedCandidate<'_>,
    intent: &activation::retirement::OwnerCandidateCancelIntent,
) -> Result<Stored> {
    closed.require_owner(tx, driver).await?;
    closed.binding().require_cleanup_journal(old)?;
    intent.require_result(closed, &journal)?;
    let pending = old.journal.pending.as_ref().context("candidate missing")?;
    if old.journal.configured_instance.is_none()
        || canonical::to_vec(pending.as_ref())? != canonical::to_vec(closed.capsule())?
    {
        bail!("candidate cancellation requires its exact configured candidate");
    }
    let mut expected = serde_json::to_value(&old.journal)?;
    expected["revision"] = serde_json::json!(
        old.journal
            .revision
            .checked_add(1)
            .context("cancellation revision exhausted")?
    );
    expected["mutation"] = serde_json::json!(journal.mutation);
    expected["request"] = serde_json::json!(journal.request);
    expected["approved_by"] = serde_json::json!(journal.approved_by);
    expected["cancelled"] =
        serde_json::to_value(record::CancelledCandidate::from_capsule(pending)?)?;
    let object = expected
        .as_object_mut()
        .context("cancellation journal missing")?;
    object.remove("activation");
    object.insert("pending".into(), serde_json::Value::Null);
    if expected != serde_json::to_value(&journal)?
        || canonical::to_vec(&journal)?.len() > canonical::to_vec(&old.journal)?.len()
    {
        bail!("candidate cancellation cannot change generations or grow metadata");
    }
    write_inner(
        tx,
        ledger,
        Some(old),
        journal,
        old.floor,
        closed.binding().budget(),
        true,
    )
    .await
}

/// One non-growing cancellation after an ORIGINAL probe has joined disposal,
/// or its ORIGINAL one-use start was permanently sealed without a launch.
/// An original acknowledged pre-claim quiescence/copy receipt also qualifies;
/// its exact journal and untouched copy are checked in the same transaction.
/// This may use its reserved final revision, never create runtime authority.
pub(super) async fn write_closed_source_cancellation(
    tx: &mut Transaction<'_, Any>,
    ledger: Ledger,
    old: &Stored,
    journal: Journal,
    intent: &cancellation::OwnerCancelIntent,
    budget: InstallBudget,
) -> Result<Stored> {
    intent.require_closed_source(old)?;
    intent.require_cancelled_record(&journal)?;
    let pending = old
        .journal
        .pending
        .as_ref()
        .context("source candidate missing")?;
    let mut expected = serde_json::to_value(&old.journal)?;
    expected["revision"] = serde_json::json!(
        old.journal
            .revision
            .checked_add(1)
            .context("cancellation revision exhausted")?
    );
    expected["mutation"] = serde_json::json!(journal.mutation);
    expected["request"] = serde_json::json!(journal.request);
    expected["approved_by"] = serde_json::json!(journal.approved_by);
    expected["cancelled"] =
        serde_json::to_value(record::CancelledCandidate::from_capsule(pending)?)?;
    let object = expected
        .as_object_mut()
        .context("cancellation journal missing")?;
    object.remove("activation");
    object.insert("pending".into(), serde_json::Value::Null);
    if expected != serde_json::to_value(&journal)?
        || canonical::to_vec(&journal)?.len() > canonical::to_vec(&old.journal)?.len()
    {
        bail!("closed-source cancellation cannot change generations or grow metadata");
    }
    write_inner(tx, ledger, Some(old), journal, old.floor, budget, true).await
}

async fn write_inner(
    tx: &mut Transaction<'_, Any>,
    ledger: Ledger,
    previous: Option<&Stored>,
    journal: Journal,
    floor: i64,
    budget: InstallBudget,
    retired_cleanup: bool,
) -> Result<Stored> {
    journal.validate()?;
    if previous.is_some_and(|old| {
        old.journal.extension != journal.extension
            || old.journal.home != journal.home
            || old.journal.configured_instance != journal.configured_instance
    }) {
        bail!("installation journal cannot change its configured owner");
    }
    let safe_resume = previous.is_some_and(|old| {
        old.journal.activation.as_ref().is_some_and(|f| {
            matches!(
                f.phase,
                record::ActivationPhase::Quiescing | record::ActivationPhase::StateCopied
            )
        })
    }) && journal.activation.is_none();
    if !safe_resume && !retired_cleanup {
        ledger.require_normal_capacity()?;
    }
    if ledger.revision >= MAX_REVISION {
        bail!("installation revision exhausted");
    }
    let json = String::from_utf8(canonical::to_vec(&journal)?)?;
    let bytes = json.len() as i64;
    let encoded = bytes + ROW_BYTES;
    let entries = ledger.entries + i64::from(previous.is_none());
    let total = ledger
        .bytes
        .checked_sub(previous.map_or(0, |p| p.encoded))
        .and_then(|n| n.checked_add(encoded))
        .context("installation accounting overflow")?;
    if entries > budget.entries as i64 || total > budget.bytes as i64 {
        bail!("owner installation metadata allocation exhausted");
    }
    let changed = if let Some(old) = previous {
        let predicate = if journal.configured_instance.is_some() {
            "instance_id=$9"
        } else {
            "instance_id IS NULL"
        };
        let query = format!(
            "UPDATE marketplace_installations SET revision=$1,record_json=$2,record_bytes=$3,record_sha256=$4,encoded_bytes=$5,updated_at=CURRENT_TIMESTAMP WHERE extension_id=$6 AND home_id=$7 AND revision=$8 AND {predicate}"
        );
        let mut query = sqlx::query(&query)
            .bind(journal.revision)
            .bind(&json)
            .bind(bytes)
            .bind(Digest::of(json.as_bytes()).to_string())
            .bind(encoded)
            .bind(journal.extension.as_str())
            .bind(journal.home.to_string())
            .bind(old.journal.revision);
        if let Some(owner) = journal.configured_instance {
            query = query.bind(owner.to_string());
        }
        query.execute(&mut **tx).await?.rows_affected()
    } else {
        sqlx::query("INSERT INTO marketplace_installations (extension_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes,instance_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
            .bind(journal.extension.as_str()).bind(journal.home.to_string()).bind(journal.revision).bind(&json).bind(bytes).bind(Digest::of(json.as_bytes()).to_string()).bind(encoded).bind(journal.configured_instance.map(|id|id.to_string())).execute(&mut **tx).await?.rows_affected()
    };
    if changed != 1 {
        bail!("installation revision changed");
    }
    // Derived hints change atomically with the actual ACTIVE journal. They
    // grant nothing; every requested body still requires fresh installed/source
    // checks. Pending/previous capsules never enter this index.
    sharing::hints::write(tx, &journal).await?;
    let changed=sqlx::query("UPDATE marketplace_installation_budget SET revision=revision+1,entries=$1,encoded_bytes=$2 WHERE singleton=1 AND revision=$3")
        .bind(entries).bind(total).bind(ledger.revision).execute(&mut **tx).await?.rows_affected();
    if changed != 1 {
        bail!("installation budget revision changed");
    }
    Ok(Stored {
        journal,
        floor,
        encoded,
    })
}

/// Closed, non-allocating promotion under the ORIGINAL joined candidate owner.
/// The caller separately authenticates fresh intent, grants and full data audit.
pub(super) async fn write_retired_activation(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    ledger: Ledger,
    old: &Stored,
    journal: Journal,
    retired: &runtime::RetiredCandidate,
) -> Result<Stored> {
    retired.require_owner(tx, driver).await?;
    retired.binding().require_cleanup_journal(old)?;
    if old.journal.configured_instance.is_none() || retired.migration_result().is_err() {
        bail!("configured activation requires successful original native readiness");
    }
    let mut expected = serde_json::to_value(&old.journal)?;
    expected["revision"] = serde_json::json!(
        old.journal
            .revision
            .checked_add(1)
            .context("activation revision exhausted")?
    );
    expected["mutation"] = serde_json::json!(journal.mutation);
    expected["request"] = serde_json::json!(journal.request);
    expected["approved_by"] = serde_json::json!(journal.approved_by);
    expected["active"] = serde_json::to_value(&old.journal.pending)?;
    expected["previous"] = serde_json::to_value(&old.journal.active)?;
    expected["pending"] = serde_json::Value::Null;
    expected
        .as_object_mut()
        .context("activation journal missing")?
        .remove("activation");
    if expected != serde_json::to_value(&journal)?
        || canonical::to_vec(&journal)?.len() as i64 + ROW_BYTES > old.encoded
    {
        bail!("retired activation cannot alter sources or allocate journal capacity");
    }
    write_inner(
        tx,
        ledger,
        Some(old),
        journal,
        old.floor,
        retired.binding().budget(),
        true,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn pending_dependency_guards_cover_every_operational_instance_fk_and_use_indexes()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let db =
            crate::marketplace::grant_store::tests::connect(&root.path().join("schema.sqlite"))
                .await?;
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_all(&db.pool)
        .await?;
        let mut actual = BTreeSet::new();
        let mut choice_owner = false;
        for table in tables {
            assert!(table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
            for row in sqlx::query(&format!("PRAGMA foreign_key_list({table})"))
                .fetch_all(&db.pool)
                .await?
            {
                if row.try_get::<String, _>("table")? == "extension_instances"
                    && matches!(
                        table.as_str(),
                        "marketplace_installed_blob_hints" | "marketplace_publisher_seeding"
                    )
                {
                    assert_eq!(row.try_get::<String, _>("from")?, "instance_id");
                    assert_eq!(row.try_get::<String, _>("to")?, "instance_id");
                    assert_eq!(row.try_get::<String, _>("on_delete")?, "CASCADE");
                    continue;
                }
                if row.try_get::<String, _>("table")? == "extension_instances"
                    && table == "marketplace_instance_choices"
                {
                    // A fixed private choice counter is not operational work.
                    // Validate this exact exception, rather than ignoring all
                    // marketplace-prefixed tables or blocking every candidate.
                    assert_eq!(row.try_get::<String, _>("from")?, "instance_id");
                    assert_eq!(row.try_get::<String, _>("to")?, "instance_id");
                    assert_eq!(row.try_get::<String, _>("on_delete")?, "CASCADE");
                    choice_owner = true;
                    continue;
                }
                if row.try_get::<String, _>("table")? == "extension_instances"
                    && table != "marketplace_sdk_namespaces"
                    // These two private ownership relations are checked by
                    // namespace/journal validation, not operational work GC.
                    && table != "marketplace_installations"
                {
                    actual.insert((table.clone(), row.try_get::<String, _>("from")?));
                }
            }
        }
        assert!(
            choice_owner,
            "bounded choice counter requires its exact stable owner FK"
        );
        let guarded: BTreeSet<_> = OPERATIONAL_INSTANCE_REFERENCES
            .iter()
            .map(|(t, c)| (t.to_string(), c.to_string()))
            .collect();
        assert_eq!(
            actual, guarded,
            "new operational instance references must not be silently cascade-deleted"
        );
        for &(table, column) in OPERATIONAL_INSTANCE_REFERENCES
            .iter()
            .chain([("marketplace_instance_choices", "instance_id")].iter())
        {
            let mut indexed = false;
            for row in sqlx::query(&format!("PRAGMA index_list({table})"))
                .fetch_all(&db.pool)
                .await?
            {
                let name: String = row.try_get("name")?;
                assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
                let columns = sqlx::query(&format!("PRAGMA index_info({name})"))
                    .fetch_all(&db.pool)
                    .await?;
                indexed |= columns.iter().any(|r| {
                    r.try_get::<i64, _>("seqno").ok() == Some(0)
                        && r.try_get::<String, _>("name").ok().as_deref() == Some(column)
                });
            }
            assert!(
                indexed,
                "{table}.{column}: cancellation must not scan a large operational table"
            );
        }
        Ok(())
    }
}
