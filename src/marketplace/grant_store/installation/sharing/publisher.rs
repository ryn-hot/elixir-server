//! Explicit standing permission to seed ONE exact verified ACTIVE capsule.
//! Not searchable admission, paid support, a new archive, or an execution grant.
use super::*;
use crate::marketplace::worker::PublicationObservation;
use elixir_marketplace::publication::{MAX_PUBLICATION_BYTES, PreparedPublication};
use serde::{Deserialize, Serialize, de::IntoDeserializer};
use sqlx::{Any, Row, Transaction};

pub(crate) const MAX_REQUEST_BYTES: usize = MAX_PUBLICATION_BYTES + 4096;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    schema_version: u32,
    instance_id: Uuid,
    extension_id: ExtensionId,
    expected_revision: i64,
    mutation_id: Uuid,
    action: Action,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Enable {
        publication: Box<PreparedPublication>,
        accepted_reference: Digest,
        accept_unqualified_seeding: bool,
    },
    Disable,
}
impl Request {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let value = canonical::parse(bytes, MAX_REQUEST_BYTES)?;
        let mut ignored = false;
        let request: Self =
            serde_ignored::deserialize(value.into_deserializer(), |_| ignored = true)?;
        if ignored
            || request.schema_version != 1
            || request.instance_id.is_nil()
            || request.mutation_id.is_nil()
            || !(0..MAX_REVISION).contains(&request.expected_revision)
        {
            bail!("invalid explicit publisher-seeding choice");
        }
        if let Action::Enable {
            publication,
            accepted_reference,
            accept_unqualified_seeding,
        } = &request.action
        {
            if !accept_unqualified_seeding
                || Digest::of(publication.to_bytes()?) != *accepted_reference
                || publication.release.payload.extension_id != request.extension_id
            {
                bail!("explicit acceptance of the exact unqualified publication is required");
            }
        }
        Ok(request)
    }
    pub(in crate::marketplace) fn publication(&self) -> Option<&PreparedPublication> {
        match &self.action {
            Action::Enable { publication, .. } => Some(publication),
            Action::Disable => None,
        }
    }
    fn fingerprint(&self, actor: &Actor) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/publisher-seeding-choice/v1",
            "home":actor.home,"user":actor.user,"profile":actor.profile,
            "authorization_revision":actor.authorization_revision,
            "instance":self.instance_id,"extension":self.extension_id,
            "expected_revision":self.expected_revision,"mutation":self.mutation_id,
            "reference":self.publication().map(|p|p.to_bytes().map(Digest::of)).transpose()?,
        }))?))
    }
}
#[derive(Debug, Serialize)]
pub(crate) struct View {
    schema_version: u32,
    instance_id: Uuid,
    pub(in crate::marketplace) revision: i64,
    pub(in crate::marketplace) enabled: bool,
    accepted_reference: Option<Digest>,
}
struct Stored {
    revision: i64,
    mutation: Uuid,
    request: Digest,
    consent: Option<Consent>,
}
/// Intentionally no Deserialize/Clone. Rows are DATA; every use checks current
/// owner/home, exact installed binding, signed security fences and row revision.
pub(in crate::marketplace) struct Consent {
    pub(in crate::marketplace) publication: PreparedPublication,
    home: Uuid,
    instance: Uuid,
    extension: ExtensionId,
    revision: i64,
    reference: Digest,
    capsule: Digest,
    user: Uuid,
    profile: Uuid,
    authorization_revision: i64,
}
const COLUMNS: &str = "revision,mutation_id,request_id,SUBSTR(publication_json,1,65537) AS publication_json,capsule_id,home_id,extension_id,approved_by_user_id,profile_id,authorization_revision";
async fn read(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    instance: Uuid,
) -> Result<Option<Stored>> {
    let lock = if driver == DatabaseDriver::Postgres {
        " FOR UPDATE"
    } else {
        ""
    };
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM marketplace_publisher_seeding WHERE instance_id=$1{lock}"
    ))
    .bind(instance.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        let revision: i64 = row.try_get("revision")?;
        if !(1..=MAX_REVISION).contains(&revision) {
            bail!("invalid publisher-seeding revision");
        }
        let publication = row.try_get::<Option<String>, _>("publication_json")?;
        let capsule = row.try_get::<Option<String>, _>("capsule_id")?;
        if publication.is_some() != capsule.is_some() {
            bail!("incomplete publisher consent");
        }
        let consent = publication
            .map(|bytes| -> Result<Consent> {
                let publication = PreparedPublication::parse(bytes.as_bytes())?;
                let reference = Digest::of(publication.to_bytes()?);
                let extension: ExtensionId = row.try_get::<String, _>("extension_id")?.parse()?;
                if publication.release.payload.extension_id != extension {
                    bail!("publisher consent identity differs");
                }
                Ok(Consent {
                    publication,
                    reference,
                    extension,
                    revision,
                    instance,
                    capsule: capsule.context("missing capsule")?.parse()?,
                    home: row.try_get::<String, _>("home_id")?.parse()?,
                    user: row.try_get::<String, _>("approved_by_user_id")?.parse()?,
                    profile: row.try_get::<String, _>("profile_id")?.parse()?,
                    authorization_revision: row.try_get("authorization_revision")?,
                })
            })
            .transpose()?;
        Ok(Stored {
            revision,
            mutation: row.try_get::<String, _>("mutation_id")?.parse()?,
            request: row.try_get::<String, _>("request_id")?.parse()?,
            consent,
        })
    })
    .transpose()
}
impl Stored {
    fn view(&self, instance: Uuid) -> View {
        View {
            schema_version: 1,
            instance_id: instance,
            revision: self.revision,
            enabled: self.consent.is_some(),
            accepted_reference: self.consent.as_ref().map(|c| c.reference),
        }
    }
}
pub(in crate::marketplace) async fn load(
    database: &Database,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
) -> Result<Option<Consent>> {
    let mut tx = database.pool.begin().await?;
    let value = read(&mut tx, database.driver, instance)
        .await?
        .and_then(|v| v.consent);
    tx.rollback().await?;
    if value
        .as_ref()
        .is_some_and(|c| c.home != home || &c.extension != extension)
    {
        bail!("publisher consent belongs to a different installation");
    }
    Ok(value)
}
impl Consent {
    pub(super) fn generation(&self, binding: &standing::InstalledBinding) -> Result<Digest> {
        let generation = binding.generation();
        if generation.capsule != self.capsule
            || generation.home != self.home
            || generation.extension != self.extension
            || generation.configured_instance != Some(self.instance)
        {
            bail!(
                "publisher consent does not follow replacement code or another configured instance"
            );
        }
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/publisher-seeding-generation/v1",
            "home":self.home,"instance":self.instance,"capsule":self.capsule,
            "revision":self.revision,"reference":self.reference,
        }))?))
    }
    pub(super) async fn current(
        &self,
        database: &Database,
        binding: &standing::InstalledBinding,
        publication: &PublicationObservation,
    ) -> Result<()> {
        if publication.summary()?.reference_id != self.reference {
            bail!("publisher seeding reference changed");
        }
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        // Standing choice, not a retained browser bearer/refresh token. Match
        // the normal owner -> home -> grant/source -> choice lock order.
        let profile: Option<String> = sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE id=$1 AND home_id=$2 AND user_id=$3 AND profile_type='account' RETURNING id")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut *tx).await?;
        let owner: Option<String> = sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
            .bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut *tx).await?;
        let revision: Option<i64> = sqlx::query_scalar("UPDATE profile_authorization_revisions SET revision=revision WHERE profile_id=$1 AND home_id=$2 AND revision=$3 RETURNING revision")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.authorization_revision).fetch_optional(&mut *tx).await?;
        if profile.is_none() || owner.is_none() || revision.is_none() {
            bail!("publisher-seeding owner authorization changed");
        }
        crate::marketplace::preferences::require_home_binding(&mut tx, self.home, database.driver)
            .await?;
        binding
            .require_in_transaction(&mut tx, database.driver)
            .await?;
        crate::marketplace::security_state::retained::require_publication_in_transaction(
            &mut tx,
            database.driver,
            publication,
        )
        .await?;
        self.generation(binding)?;
        let current = read(&mut tx, database.driver, self.instance)
            .await?
            .and_then(|v| v.consent)
            .context("publisher seeding was withdrawn")?;
        if current.revision != self.revision
            || current.reference != self.reference
            || current.capsule != self.capsule
            || current.home != self.home
            || current.extension != self.extension
            || current.user != self.user
            || current.profile != self.profile
            || current.authorization_revision != self.authorization_revision
        {
            bail!("publisher-seeding choice changed");
        }
        publication.summary()?;
        tx.commit().await?;
        Ok(())
    }
}
pub(in crate::marketplace) async fn change(
    database: &Database,
    principal: &CurrentPrincipal,
    request: Request,
    observation: Option<&PublicationObservation>,
    budget: InstallBudget,
) -> Result<View> {
    budget.validate()?;
    let actor = Actor::from_principal(principal, false)?;
    let fingerprint = request.fingerprint(&actor)?;
    // Disable must work without network, a current publication or enabled
    // instance/grant. Retiring a choice must never require starting a helper.
    let binding = if request.publication().is_some() {
        Some(
            standing::InstalledBinding::load_configured(
                database,
                actor.home,
                &request.extension_id,
                request.instance_id,
                budget,
            )
            .await?,
        )
    } else {
        None
    };
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = actor.authenticate(&mut tx, now).await?;
    crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
        .await?;
    let (publication, capsule) = match (&request.action, binding.as_ref()) {
        (
            Action::Enable {
                publication,
                accepted_reference,
                ..
            },
            Some(binding),
        ) => {
            let observation =
                observation.context("independent publication verification required")?;
            if observation.summary()?.reference_id != *accepted_reference {
                bail!("publisher verification differs from consent");
            }
            let (_, active) = binding.current_material(&mut tx, database.driver).await?;
            let facts = active.facts()?;
            if canonical::to_vec(&publication.release.payload)?
                != canonical::to_vec(facts.release().payload())?
                || publication.publisher.payload.root_key != facts.root_key()
            {
                bail!("publisher consent differs from the exact installed source");
            }
            crate::marketplace::security_state::retained::require_publication_in_transaction(
                &mut tx,
                database.driver,
                observation,
            )
            .await?;
            (
                Some(String::from_utf8(publication.to_bytes()?)?),
                Some(binding.generation().capsule.to_string()),
            )
        }
        (Action::Disable, None) => (None, None),
        _ => bail!("invalid publisher-seeding action"),
    };
    require_managed(
        &mut tx,
        actor.home,
        &request.extension_id,
        request.instance_id,
    )
    .await?;
    let previous = read(&mut tx, database.driver, request.instance_id).await?;
    if let Some(old) = previous.as_ref() {
        if old.mutation == request.mutation_id {
            if old.request != fingerprint {
                bail!("publisher mutation was reused with different inputs");
            }
            let view = old.view(request.instance_id);
            ensure_before_expiry(expires, now, started)?;
            tx.commit().await?;
            return Ok(view);
        }
    }
    if previous.as_ref().map_or(0, |v| v.revision) != request.expected_revision {
        bail!("publisher choice changed; refresh its revision");
    }
    let revision = request
        .expected_revision
        .checked_add(1)
        .context("publisher revision exhausted")?;
    sqlx::query("INSERT INTO marketplace_publisher_seeding(instance_id,home_id,extension_id,revision,mutation_id,request_id,publication_json,capsule_id,approved_by_user_id,profile_id,authorization_revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(instance_id) DO UPDATE SET home_id=excluded.home_id,extension_id=excluded.extension_id,revision=excluded.revision,mutation_id=excluded.mutation_id,request_id=excluded.request_id,publication_json=excluded.publication_json,capsule_id=excluded.capsule_id,approved_by_user_id=excluded.approved_by_user_id,profile_id=excluded.profile_id,authorization_revision=excluded.authorization_revision")
        .bind(request.instance_id.to_string()).bind(actor.home.to_string()).bind(request.extension_id.as_str()).bind(revision)
        .bind(request.mutation_id.to_string()).bind(fingerprint.to_string()).bind(publication).bind(capsule)
        .bind(actor.user.to_string()).bind(actor.profile.to_string()).bind(actor.authorization_revision).execute(&mut *tx).await?;
    let view = read(&mut tx, database.driver, request.instance_id)
        .await?
        .context("publisher choice missing")?
        .view(request.instance_id);
    if let Some(observation) = observation {
        observation.summary()?;
    }
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(view)
}

async fn require_managed(
    tx: &mut Transaction<'_, Any>,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
) -> Result<()> {
    let found: Option<String> = sqlx::query_scalar("UPDATE extension_instances SET updated_at=updated_at WHERE instance_id=$1 AND extension_id=$2 AND EXISTS(SELECT 1 FROM marketplace_installations m WHERE m.instance_id=$1 AND m.extension_id=$2 AND m.home_id=$3) RETURNING instance_id")
        .bind(instance.to_string()).bind(extension.as_str()).bind(home.to_string()).fetch_optional(&mut **tx).await?;
    if found.is_none() {
        bail!("publisher seeding requires the exact managed instance");
    }
    Ok(())
}
pub(in crate::marketplace) async fn view(
    database: &Database,
    principal: &CurrentPrincipal,
    extension: &ExtensionId,
    instance: Uuid,
) -> Result<View> {
    let actor = Actor::from_principal(principal, false)?;
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = actor.authenticate(&mut tx, now).await?;
    crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
        .await?;
    require_managed(&mut tx, actor.home, extension, instance).await?;
    let view = read(&mut tx, database.driver, instance)
        .await?
        .map(|v| v.view(instance))
        .unwrap_or(View {
            schema_version: 1,
            instance_id: instance,
            revision: 0,
            enabled: false,
            accepted_reference: None,
        });
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(view)
}

#[cfg(test)]
pub(super) mod tests;
