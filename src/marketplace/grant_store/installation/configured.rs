//! Per-configured-instance journal data and explicit disabled staging. No
//! namespace/marker/receipt is execution, retirement or activation authority.
use super::super::sdk::namespaces;
use super::*;

#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(in crate::marketplace) enum InstallTarget {
    New {
        name: String,
    },
    Existing {
        #[serde(rename = "instance_id")]
        instance: Uuid,
    },
}
pub(super) struct Target {
    instance: Uuid,
    name: Option<String>,
}
impl Target {
    pub(super) fn new(
        intent: &OwnerInstallIntent,
        input: InstallTarget,
        extension: &ExtensionId,
    ) -> Result<Self> {
        let (instance, name) = match input {
            InstallTarget::Existing { instance } => (instance, None),
            InstallTarget::New { name } => {
                if name.is_empty()
                    || name.len() > 128
                    || name.chars().any(char::is_control)
                    || intent.expected_revision != 0
                {
                    bail!("invalid new configured instance");
                }
                // Stable retry coordinates, NOT authentication. Reconstructing
                // one owner action after an unknown commit cannot allocate a
                // second configured instance. Name changes reuse this ID but
                // change request_id, so the mutation is rejected, not adopted.
                let digest = Digest::of(canonical::to_vec(&serde_json::json!({
                    "domain":"elixir-marketplace/configured-instance/v1",
                    "user":intent.actor.user,"home":intent.actor.home,
                    "extension":extension,"mutation":intent.mutation,
                }))?);
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&digest.as_bytes()[..16]);
                bytes[6] = (bytes[6] & 0x0f) | 0x80; // UUIDv8: domain-separated hash data.
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                (Uuid::from_bytes(bytes), Some(name))
            }
        };
        if instance.is_nil() {
            bail!("invalid configured instance");
        }
        Ok(Self { instance, name })
    }
    pub(super) fn instance(&self) -> Uuid {
        self.instance
    }
    pub(super) fn existing_instance(&self) -> Option<Uuid> {
        self.name.is_none().then_some(self.instance)
    }
    pub(super) fn request_id(
        &self,
        intent: &OwnerInstallIntent,
        material: &Material,
    ) -> Result<Digest> {
        let mut value = serde_json::json!({
            "domain":"elixir-marketplace/stage-install/v2", "user":intent.actor.user,
            "home":intent.actor.home,"review":intent.review,"release":intent.release,
            "grant_revision":intent.grant_revision,"expected_revision":intent.expected_revision,
            "mutation":intent.mutation,"source":material.source.id()?,
            "configured_instance":self.instance,"new_name":self.name,
        });
        if let Some(expected) = intent.expected_controls {
            value["domain"] = serde_json::json!("elixir-marketplace/stage-install/v3");
            value["expected_controls"] = serde_json::to_value(expected)?;
        }
        Ok(Digest::of(canonical::to_vec(&value)?))
    }
}

// Stage/cancel already hold home -> grant -> global budget. Metadata is only a
// desired product/UI view; an instance's actual source always comes from its
// own capsule. No whole-extension generation count/version equality is used.
async fn desired(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    home: Uuid,
    extension: &ExtensionId,
    mode: super::super::sdk::RowLock,
) -> Result<bool> {
    let query = super::super::sdk::locked(store::EXTENSION_METADATA, driver, mode);
    let row = sqlx::query(&query)
        .bind(extension.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    let marker: Option<(String,String)> = sqlx::query_as("SELECT SUBSTR(home_id,1,37),SUBSTR(metadata_sha256,1,72) FROM marketplace_installation_metadata WHERE extension_id=$1")
        .bind(extension.as_str()).fetch_optional(&mut **tx).await?;
    let (legacy, configured): (i64,i64) = sqlx::query_as("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_installations WHERE extension_id=$1 AND instance_id IS NULL) THEN 1 ELSE 0 END AS BIGINT),CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_installations WHERE extension_id=$1 AND instance_id IS NOT NULL) THEN 1 ELSE 0 END AS BIGINT)")
        .bind(extension.as_str()).fetch_one(&mut **tx).await?;
    if legacy != 0 {
        bail!("legacy installation requires explicit conversion; no implicit adoption");
    }
    match (row, marker, configured) {
        (None, None, 0) => Ok(false),
        (Some(row), Some((bound, digest)), 1)
            if bound == home.to_string()
                && digest == store::metadata_from_row(&row)?.id()?.to_string() =>
        {
            Ok(true)
        }
        _ => bail!("desired installation metadata is missing, changed or unmanaged"),
    }
}

pub(super) async fn prepare(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    intent: &OwnerInstallIntent,
    material: &Material,
    previous: Option<&Stored>,
    target: &Target,
) -> Result<()> {
    if target.name.is_some() && previous.is_some() {
        bail!("configured creation is no longer an exact retry");
    }
    if target.name.is_none() && previous.is_none() {
        bail!("configured journal missing; existing operational instances are not adopted");
    }
    let extension = material.review.scope().extension_id();
    let exists = desired(
        tx,
        driver,
        intent.actor.home,
        extension,
        super::super::sdk::RowLock::Exclusive,
    )
    .await?;
    store::write_metadata(tx, material, !exists, false).await?;
    let facts = material.authority.verify()?;
    let digest =
        record::OperationalMetadata::from_release(facts.release(), &material.manifest)?.id()?;
    let query = if exists {
        "UPDATE marketplace_installation_metadata SET metadata_sha256=$3 WHERE extension_id=$1 AND home_id=$2"
    } else {
        "INSERT INTO marketplace_installation_metadata(extension_id,home_id,metadata_sha256) VALUES($1,$2,$3)"
    };
    if sqlx::query(query)
        .bind(extension.as_str())
        .bind(intent.actor.home.to_string())
        .bind(digest.to_string())
        .execute(&mut **tx)
        .await?
        .rows_affected()
        != 1
    {
        bail!("desired metadata changed");
    }
    if let Some(name) = &target.name {
        let used: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_sdk_namespaces WHERE namespace_id=$1) OR EXISTS(SELECT 1 FROM marketplace_instance_sdk_state WHERE instance_id=$1) THEN 1 ELSE 0 END AS BIGINT)")
            .bind(target.instance.to_string()).fetch_one(&mut **tx).await?;
        if used != 0 {
            bail!("configured identity aliases an existing namespace");
        }
        store::require_no_secrets(tx, target.instance).await?;
        sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,$3,FALSE)")
            .bind(target.instance.to_string()).bind(extension.as_str()).bind(name).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn verify(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    let owner = journal
        .configured_instance
        .context("configured owner missing")?;
    journal.validate()?;
    if !desired(
        tx,
        driver,
        journal.home,
        &journal.extension,
        super::super::sdk::RowLock::Shared,
    )
    .await?
    {
        bail!("configured desired metadata missing");
    }
    let query = super::super::sdk::locked(
        "SELECT extension_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled FROM extension_instances WHERE instance_id=$1",
        driver,
        super::super::sdk::RowLock::Exclusive,
    );
    let row = sqlx::query(&query)
        .bind(owner.to_string())
        .fetch_optional(&mut **tx)
        .await?
        .context("configured instance missing; no implicit repair")?;
    if row.try_get::<String, _>("extension_id")? != journal.extension.as_str()
        || (journal.active.is_none() && row.try_get::<i64, _>("enabled")? != 0)
    {
        bail!("configured instance scope or inactive state differs");
    }
    if journal.active.is_none() {
        store::require_no_operational_dependants(tx, owner).await?;
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE instance_id=$1")
            .bind(owner.to_string())
            .fetch_one(&mut **tx)
            .await?;
    if count != journal.slots().count() as i64 {
        bail!("configured instance has unrecorded or missing generations");
    }
    if let Some(cancelled) = &journal.cancelled {
        let present: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM extension_instances WHERE instance_id=$1) OR EXISTS(SELECT 1 FROM marketplace_sdk_namespaces WHERE namespace_id=$1) OR EXISTS(SELECT 1 FROM marketplace_instance_sdk_state WHERE instance_id=$1) THEN 1 ELSE 0 END AS BIGINT)")
            .bind(cancelled.instance.to_string()).fetch_one(&mut **tx).await?;
        if present != 0 {
            bail!("cancelled namespace has reappeared");
        }
    }
    for capsule in journal.slots() {
        namespaces::require(
            tx,
            namespaces::Coordinates {
                home: journal.home,
                extension: &journal.extension,
                instance: owner,
                namespace: capsule.instance,
            },
            namespaces::Purpose::Installed,
        )
        .await?;
        if journal
            .pending
            .as_ref()
            .is_some_and(|p| p.instance == capsule.instance)
        {
            let copied = journal.activation.as_ref().is_some_and(|f| {
                matches!(
                    f.phase,
                    record::ActivationPhase::StateCopied
                        | record::ActivationPhase::CandidateStarting
                )
            });
            if !copied {
                let head: (i64,i64,i64) = sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
                    .bind(capsule.instance.to_string()).fetch_one(&mut **tx).await?;
                let values: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1",
                )
                .bind(capsule.instance.to_string())
                .fetch_one(&mut **tx)
                .await?;
                let secrets: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1",
                )
                .bind(capsule.instance.to_string())
                .fetch_one(&mut **tx)
                .await?;
                if head != (1, 0, 0) || values != 0 || secrets != 0 {
                    bail!("unstarted namespace has unexpected private data");
                }
            }
        }
    }
    // The original activation controller must still verify the actual bound
    // namespace and retirement owner before allowing these transitions.
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

pub(super) async fn require_active_metadata(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    if journal.active.is_none() {
        bail!("no active configured source");
    }
    verify(tx, driver, journal).await?;
    // desired() retained a share lock. Its version is NOT this instance's
    // running version; only the separately checked active capsule is source
    // authority. Global/instance disable remain independently enforced.
    let enabled: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extensions WHERE extension_id=$1")
        .bind(journal.extension.as_str()).fetch_one(&mut **tx).await?;
    if enabled != 1 {
        bail!("configured extension is disabled");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
