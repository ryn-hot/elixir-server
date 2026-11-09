//! Source provider registration/snapshot DATA. No native owner or readiness is
//! constructed from these rows. Calls independently require the held controller.
use super::*;
use crate::extensions::manifest::{LIVE_CATALOG_PROVIDER_CAPABILITY, ManifestProviderScope};
use elixir_marketplace::records::RuntimeProfile;
use serde_json::{Value, json};

pub(crate) const SOURCE_ENDPOINT: &str = "{\"type\":\"source-js-v1\"}";
const MAX_PROVIDERS: i64 = 4096;

#[derive(Clone)]
pub(crate) struct Descriptor {
    pub provider: Uuid,
    pub home: Uuid,
    pub extension: ExtensionId,
    pub instance: Uuid,
    pub version: String,
    pub implementation: String,
    pub scope: ManifestProviderScope,
    pub permissions: Vec<String>,
    pub config: Value,
    pub revision: Digest,
}
#[derive(Clone)]
pub(in crate::marketplace) struct Expectation {
    pub provider: Uuid,
    pub revision: Digest,
}
#[derive(Clone)]
pub(in crate::marketplace) struct Coordinates {
    pub provider: Uuid,
    pub home: Uuid,
    pub extension: ExtensionId,
    pub instance: Uuid,
}
fn provider_id(instance: Uuid, slot: &str) -> Result<Uuid> {
    let digest = Digest::of(canonical::to_vec(
        &json!({"domain":"elixir-marketplace/source-live-provider/v1",
        "instance":instance,"slot":slot,"capability":LIVE_CATALOG_PROVIDER_CAPABILITY}),
    )?);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

/// Called only within original retired-candidate activation. These are routing
/// metadata, deliberately UNKNOWN health and without a DriverReady receipt.
pub(super) async fn register(
    tx: &mut Transaction<'_, Any>,
    owner: Uuid,
    capsule: &Capsule,
) -> Result<()> {
    let facts = capsule.facts()?;
    let (manifest, _) = capsule.source.review(facts.release())?;
    let mut declarations = Vec::new();
    let mut charged = 0usize;
    for provide in &manifest.provides {
        if provide.capability != LIVE_CATALOG_PROVIDER_CAPABILITY {
            continue;
        }
        if facts.release().payload().runtime_profile != RuntimeProfile::SourceJs {
            continue;
        }
        if provide
            .endpoint
            .as_ref()
            .is_none_or(|e| e.r#type != "source-js-v1")
        {
            bail!("installed live provider has no source transport");
        }
        let scope = serde_json::to_string(
            provide
                .scope
                .as_ref()
                .context("source provider scope missing")?,
        )?;
        let implementation = provide
            .implementation
            .as_ref()
            .context("source provider implementation missing")?;
        charged = charged
            .checked_add(scope.len() + implementation.len() + provide.slot.len() + 2048)
            .context("provider metadata overflow")?;
        if charged > record::ROW_BYTES as usize / 2 {
            bail!("source providers exceed their existing operational metadata reservation");
        }
        declarations.push((
            provider_id(owner, &provide.slot)?,
            provide,
            scope,
            implementation,
        ));
    }
    let existing:Vec<(String,String)>=sqlx::query_as("SELECT provider_id,slot_id FROM providers WHERE instance_id=$1 AND capability=$2 AND endpoint_json=$3 ORDER BY provider_id")
        .bind(owner.to_string()).bind(LIVE_CATALOG_PROVIDER_CAPABILITY).bind(SOURCE_ENDPOINT).fetch_all(&mut **tx).await?;
    for (id, slot) in existing {
        let id: Uuid = id.parse()?;
        if id != provider_id(owner, &slot)? {
            bail!("source provider identity changed; no implicit repair");
        }
        if !declarations.iter().any(|(next, _, _, _)| *next == id) {
            let dependants:i64=sqlx::query_scalar("SELECT COUNT(*) FROM bindings WHERE consumer_provider_id=$1 OR target_provider_id=$1")
                .bind(id.to_string()).fetch_one(&mut **tx).await?;
            if dependants != 0 {
                bail!("source provider removal requires resolving its bindings first");
            }
            sqlx::query("DELETE FROM providers WHERE provider_id=$1 AND instance_id=$2 AND endpoint_json=$3")
                .bind(id.to_string()).bind(owner.to_string()).bind(SOURCE_ENDPOINT).execute(&mut **tx).await?;
        }
    }
    for (id, provide, scope, implementation) in declarations {
        let changed=sqlx::query("INSERT INTO providers(provider_id,instance_id,capability,slot_id,cardinality,implementation,scope_json,endpoint_json,health_state) VALUES($1,$2,$3,$4,'many',$5,$6,$7,'unknown') ON CONFLICT(provider_id) DO UPDATE SET implementation=excluded.implementation,scope_json=excluded.scope_json,health_state='unknown',last_healthcheck_at=NULL,updated_at=CURRENT_TIMESTAMP WHERE providers.instance_id=excluded.instance_id AND providers.capability=excluded.capability AND providers.slot_id=excluded.slot_id AND providers.endpoint_json=excluded.endpoint_json")
            .bind(id.to_string()).bind(owner.to_string()).bind(LIVE_CATALOG_PROVIDER_CAPABILITY).bind(&provide.slot)
            .bind(implementation).bind(scope).bind(SOURCE_ENDPOINT).execute(&mut **tx).await?.rows_affected();
        if changed != 1 {
            bail!("source provider row is foreign; no adoption");
        }
    }
    Ok(())
}

pub(in crate::marketplace) async fn candidates(
    database: &Database,
    id: Option<Uuid>,
) -> Result<Vec<Coordinates>> {
    let mut query = String::from(
        "SELECT p.provider_id,SUBSTR(g.home_id,1,37),SUBSTR(i.extension_id,1,129),p.instance_id FROM providers p JOIN extension_instances i ON i.instance_id=p.instance_id JOIN extensions e ON e.extension_id=i.extension_id JOIN marketplace_extension_grants g ON g.extension_id=i.extension_id WHERE p.capability=$1 AND p.endpoint_json=$2 AND i.enabled=TRUE AND e.enabled=TRUE AND g.enabled=TRUE AND i.extension_id LIKE 'elx:%'",
    );
    if id.is_some() {
        query.push_str(" AND p.provider_id=$3");
    }
    query.push_str(" ORDER BY p.provider_id LIMIT 4097");
    let mut query = sqlx::query_as::<_, (String, String, String, String)>(&query)
        .bind(LIVE_CATALOG_PROVIDER_CAPABILITY)
        .bind(SOURCE_ENDPOINT);
    if let Some(id) = id {
        query = query.bind(id.to_string());
    }
    let rows = query.fetch_all(&database.pool).await?;
    if rows.len() > MAX_PROVIDERS as usize {
        bail!("source provider discovery exceeds its bounded page");
    }
    rows.into_iter()
        .map(|(provider, home, extension, instance)| {
            Ok(Coordinates {
                provider: provider.parse()?,
                home: home.parse()?,
                extension: extension.parse()?,
                instance: instance.parse()?,
            })
        })
        .collect()
}

/// Parent extension/instance rows are already held by read_active. Do not
/// upgrade those shared locks here. Counter writers take their parents first.
pub(super) async fn describe(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
    capsule: &Capsule,
    provider: Uuid,
) -> Result<Descriptor> {
    let facts = capsule.facts()?;
    if facts.release().payload().runtime_profile != RuntimeProfile::SourceJs {
        bail!("provider requires an installed source-js generation");
    }
    let (manifest, review) = capsule.source.review(facts.release())?;
    let sql = super::super::sdk::locked(
        "SELECT SUBSTR(slot_id,1,129) AS slot_id,SUBSTR(implementation,1,129) AS implementation,SUBSTR(scope_json,1,65537) AS scope_json,cardinality FROM providers WHERE provider_id=$1 AND instance_id=$2 AND capability=$3 AND endpoint_json=$4",
        driver,
        super::super::sdk::RowLock::Shared,
    );
    let (slot, implementation, scope, cardinality): (String, String, String, String) =
        sqlx::query_as(&sql)
            .bind(provider.to_string())
            .bind(instance.to_string())
            .bind(LIVE_CATALOG_PROVIDER_CAPABILITY)
            .bind(SOURCE_ENDPOINT)
            .fetch_optional(&mut **tx)
            .await?
            .context("source provider no longer registered")?;
    if provider != provider_id(instance, &slot)? || cardinality != "many" || scope.len() > 65536 {
        bail!("source provider metadata is invalid");
    }
    let declaration = manifest
        .provides
        .iter()
        .find(|p| p.capability == LIVE_CATALOG_PROVIDER_CAPABILITY && p.slot == slot)
        .context("source provider is not in the active signed manifest")?;
    if declaration.implementation.as_deref() != Some(&implementation)
        || declaration
            .endpoint
            .as_ref()
            .is_none_or(|e| e.r#type != "source-js-v1")
    {
        bail!("source provider declaration changed");
    }
    let declared_scope = declaration
        .scope
        .as_ref()
        .context("source provider scope missing")?;
    if serde_json::from_str::<Value>(&scope)? != serde_json::to_value(declared_scope)? {
        bail!("source provider scope differs from active source");
    }
    let config:Option<String>=sqlx::query_scalar("SELECT SUBSTR(config_json,1,65537) FROM extension_instances WHERE instance_id=$1 AND extension_id=$2")
        .bind(instance.to_string()).bind(extension.as_str()).fetch_one(&mut **tx).await?;
    let mut config = match config {
        Some(text) if text.len() <= 65536 => serde_json::from_str::<Value>(&text)?,
        None => json!({}),
        _ => bail!("source provider config exceeds bound"),
    };
    if let Some(object) = config.as_object_mut() {
        object.remove("runtime");
    }
    config.sort_all_objects();
    let controls:(i64,i64)=sqlx::query_as("SELECT e.revision,i.revision FROM marketplace_extension_choices e JOIN marketplace_instance_choices i ON i.extension_id=e.extension_id WHERE e.extension_id=$1 AND i.instance_id=$2")
        .bind(extension.as_str()).bind(instance.to_string()).fetch_optional(&mut **tx).await?.context("source provider control revisions missing")?;
    if !(1..record::MAX_REVISION).contains(&controls.0)
        || !(1..record::MAX_REVISION).contains(&controls.1)
    {
        bail!("source provider counters require recovery");
    }
    let revision = Digest::of(canonical::to_vec(
        &json!({"domain":"elixir-marketplace/source-provider-snapshot/v1",
        "provider":provider,"home":home,"extension":extension,"instance":instance,
        "capsule":Digest::of(canonical::to_vec(capsule)?),"controls":[controls.0,controls.1],
        "implementation":implementation,"slot":slot,"config":serde_json::to_string(&config)?}),
    )?);
    Ok(Descriptor {
        provider,
        home,
        extension: extension.clone(),
        instance,
        version: facts.release().payload().version.clone(),
        implementation,
        scope: declared_scope.clone(),
        permissions: review.scope().permissions().to_vec(),
        config,
        revision,
    })
}
